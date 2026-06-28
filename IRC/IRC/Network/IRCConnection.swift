import Foundation
import Network
import os.log

private let log = Logger(subsystem: "com.irc", category: "IRCConnection")

/// Actor that owns a single NWConnection to an IRC server.
/// All I/O is async; callers never block.
actor IRCConnection {

    private var connection: NWConnection?
    private let framer = LineFramer()
    private var sendQueue: [(String, CheckedContinuation<Void, Error>)] = []
    private var isSending = false

    // Token-bucket flood control: 10 tokens, refill 1/2s, burst cap 10
    private var floodTokens: Int = 10
    private var lastTokenRefill: Date = .now

    weak var delegate: (any IRCConnectionDelegate)?

    // MARK: - Connect / Disconnect

    func connect(host: String, port: UInt16, parameters: NWParameters) async throws {
        let endpoint = NWEndpoint.hostPort(
            host: NWEndpoint.Host(host),
            port: NWEndpoint.Port(rawValue: port) ?? 6667
        )
        let conn = NWConnection(to: endpoint, using: parameters)
        self.connection = conn

        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            conn.stateUpdateHandler = { [weak self] state in
                guard let self else { return }
                Task {
                    switch state {
                    case .ready:
                        await self.delegate?.connection(self, didChangeState: .connected)
                        continuation.resume()
                        await self.startReadLoop()
                    case .failed(let err):
                        await self.delegate?.connection(self, didChangeState: .failed(err))
                        continuation.resume()
                    case .cancelled:
                        await self.delegate?.connection(self, didChangeState: .disconnected(nil))
                        continuation.resume()
                    default:
                        break
                    }
                }
            }
            conn.start(queue: .global(qos: .utility))
        }
    }

    func disconnect(graceful: Bool = true) async {
        if graceful, let conn = connection {
            conn.cancel()
        }
        connection = nil
        framer.reset()
    }

    // MARK: - Send

    /// Enqueues a line for delivery. Applies basic IRC flood control.
    func send(_ line: String) async throws {
        try await withCheckedThrowingContinuation { continuation in
            sendQueue.append((line, continuation))
            Task { await drainSendQueue() }
        }
    }

    private func drainSendQueue() async {
        guard !isSending, let conn = connection else { return }
        isSending = true

        while !sendQueue.isEmpty {
            refillFloodTokens()
            if floodTokens <= 0 {
                // Wait for a token
                try? await Task.sleep(nanoseconds: 500_000_000)
                refillFloodTokens()
            }

            let (line, continuation) = sendQueue.removeFirst()
            let data = Data((line + (line.hasSuffix("\r\n") ? "" : "\r\n")).utf8)

            do {
                try await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, Error>) in
                    conn.send(content: data, completion: .contentProcessed { error in
                        if let error {
                            c.resume(throwing: error)
                        } else {
                            c.resume()
                        }
                    })
                }
                floodTokens = max(0, floodTokens - 1)
                log.debug("→ \(line.trimmingCharacters(in: .newlines))")
                continuation.resume()
            } catch {
                continuation.resume(throwing: error)
                log.error("Send failed: \(error)")
            }
        }

        isSending = false
    }

    private func refillFloodTokens() {
        let elapsed = Date.now.timeIntervalSince(lastTokenRefill)
        let newTokens = Int(elapsed / 0.5)
        if newTokens > 0 {
            floodTokens = min(10, floodTokens + newTokens)
            lastTokenRefill = .now
        }
    }

    // MARK: - Read loop

    private func startReadLoop() async {
        guard let conn = connection else { return }
        conn.receive(minimumIncompleteLength: 1, maximumLength: 8_704) { [weak self] data, _, isComplete, error in
            guard let self else { return }
            Task {
                if let data {
                    let lines = self.framer.receive(data)
                    for line in lines {
                        log.debug("← \(line)")
                        await self.delegate?.connection(self, didReceiveLine: line)
                    }
                }
                if let error {
                    await self.delegate?.connection(self, didChangeState: .failed(error))
                    return
                }
                if isComplete {
                    await self.delegate?.connection(self, didChangeState: .disconnected(nil))
                    return
                }
                // Recurse to keep reading
                await self.startReadLoop()
            }
        }
    }
}
