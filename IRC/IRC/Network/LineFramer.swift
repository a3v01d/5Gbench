import Foundation

/// Accumulates raw bytes from a TCP stream and extracts CRLF-delimited IRC lines.
final class LineFramer: @unchecked Sendable {

    private static let maxBufferBytes = 65_536

    private var buffer = Data()

    /// Feed incoming bytes. Returns any complete lines found (without their CRLF).
    func receive(_ data: Data) -> [String] {
        buffer.append(data)

        // Guard against runaway buffer from a malicious server
        if buffer.count > LineFramer.maxBufferBytes {
            buffer.removeAll()
            return []
        }

        var lines: [String] = []
        while let range = buffer.range(of: Data([0x0D, 0x0A])) {
            let lineData = buffer[buffer.startIndex..<range.lowerBound]
            // Try UTF-8 first, fall back to Latin-1
            let line = String(data: lineData, encoding: .utf8)
                    ?? String(data: lineData, encoding: .isoLatin1)
                    ?? ""
            if !line.isEmpty { lines.append(line) }
            buffer.removeSubrange(buffer.startIndex...range.upperBound.advanced(by: -1))
        }
        return lines
    }

    func reset() { buffer.removeAll() }
}
