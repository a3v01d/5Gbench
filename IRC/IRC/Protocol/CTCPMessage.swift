import Foundation

private let ctcpDelimiter = Character("\u{0001}")
private let ctcpDelimiterStr = "\u{0001}"

/// Represents a decoded CTCP message extracted from a PRIVMSG body.
struct CTCPMessage: Sendable {
    let verb: String
    let body: String?

    // Well-known CTCP verbs
    static let ACTION  = "ACTION"
    static let VERSION = "VERSION"
    static let PING    = "PING"
    static let TIME    = "TIME"
    static let DCC     = "DCC"

    var isAction: Bool { verb == CTCPMessage.ACTION }
}

enum CTCPError: Error {
    case notCTCP
}

struct CTCP {
    /// Returns nil if the text is not a CTCP message.
    static func decode(_ text: String) -> CTCPMessage? {
        guard text.hasPrefix(ctcpDelimiterStr) && text.hasSuffix(ctcpDelimiterStr) else {
            return nil
        }
        let inner = String(text.dropFirst().dropLast())
        if let space = inner.firstIndex(of: " ") {
            let verb = String(inner[..<space]).uppercased()
            let body = String(inner[inner.index(after: space)...])
            return CTCPMessage(verb: verb, body: body.isEmpty ? nil : body)
        } else {
            return CTCPMessage(verb: inner.uppercased(), body: nil)
        }
    }

    /// Encodes a CTCP message to embed in a PRIVMSG body.
    static func encode(verb: String, body: String? = nil) -> String {
        if let b = body, !b.isEmpty {
            return "\(ctcpDelimiterStr)\(verb) \(b)\(ctcpDelimiterStr)"
        } else {
            return "\(ctcpDelimiterStr)\(verb)\(ctcpDelimiterStr)"
        }
    }

    static func action(_ text: String) -> String {
        encode(verb: CTCPMessage.ACTION, body: text)
    }

    static func versionReply(_ version: String) -> String {
        encode(verb: CTCPMessage.VERSION, body: version)
    }

    static func pingReply(_ token: String) -> String {
        encode(verb: CTCPMessage.PING, body: token)
    }
}
