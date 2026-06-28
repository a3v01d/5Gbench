import Foundation

/// Manages IRCv3 capability negotiation (CAP LS 302 → REQ → ACK/NAK → END).
actor CapabilityNegotiator {

    enum State {
        case idle
        case listing        // awaiting CAP LS reply
        case requesting     // awaiting CAP ACK/NAK
        case done
    }

    private(set) var state: State = .idle
    private(set) var available: [IRCCapability: String?] = [:]  // cap → optional value
    private(set) var acknowledged: Set<IRCCapability> = []
    private(set) var denied: Set<IRCCapability> = []

    var needsSASL: Bool { acknowledged.contains(.sasl) }

    // Callback invoked when negotiation is complete (before CAP END is sent)
    var onComplete: (() async -> Void)?

    /// Kicks off negotiation. Call after TCP connect, before NICK/USER.
    func begin() -> String {
        state = .listing
        return IRCMessageEncoder.cap(subcommand: "LS", params: ["302"])
    }

    /// Process a CAP subcommand from the server.
    /// Returns any lines to send back (REQ, END, etc.)
    func handle(subcommand: String, params: [String]) async -> [String] {
        let sub = subcommand.uppercased()

        switch sub {
        case "LS":
            // params[0] = "*" if multi-line, params[1] (or params[0] if not multi-line) = caps
            let isMultiline = params.first == "*"
            let capList = isMultiline ? (params.count > 1 ? params[1] : "") : (params.first ?? "")
            parseAvailable(capList)

            if !isMultiline {
                // Last LS reply — send REQ for what we want
                state = .requesting
                let toRequest = IRCCapability.desired.filter { available[$0] != nil }
                if toRequest.isEmpty {
                    state = .done
                    await onComplete?()
                    return [IRCMessageEncoder.cap(subcommand: "END")]
                }
                let reqLine = IRCMessageEncoder.cap(
                    subcommand: "REQ",
                    params: [":" + toRequest.map(\.rawValue).joined(separator: " ")]
                )
                return [reqLine]
            }
            return []

        case "ACK":
            let capStr = params.last ?? ""
            for word in capStr.split(separator: " ") {
                let cap = IRCCapability(String(word).trimmingCharacters(in: .whitespaces))
                acknowledged.insert(cap)
            }
            state = .done
            await onComplete?()
            return []  // caller sends CAP END after SASL (if any)

        case "NAK":
            let capStr = params.last ?? ""
            for word in capStr.split(separator: " ") {
                denied.insert(IRCCapability(String(word)))
            }
            state = .done
            await onComplete?()
            return [IRCMessageEncoder.cap(subcommand: "END")]

        default:
            return []
        }
    }

    // MARK: - Private

    private func parseAvailable(_ capString: String) {
        for token in capString.split(separator: " ") {
            let t = String(token)
            if let eq = t.firstIndex(of: "=") {
                let key = IRCCapability(String(t[..<eq]))
                let val = String(t[t.index(after: eq)...])
                available[key] = val
            } else {
                available[IRCCapability(t)] = nil
            }
        }
    }
}
