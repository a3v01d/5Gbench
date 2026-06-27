import Foundation

/// A fully parsed IRC message, including optional IRCv3 message tags.
struct IRCMessage: Sendable {
    let tags: [String: String?]
    let prefix: Prefix?
    let command: IRCCommand
    let parameters: [String]
    let raw: String

    struct Prefix: Sendable {
        let nick: String?
        let user: String?
        let host: String?

        var isServer: Bool { nick == nil }

        /// Convenience: the server name when this is a server-origin prefix.
        var serverName: String? { isServer ? host : nil }

        /// Convenience: display nick (falls back to host for server prefixes).
        var displayName: String { nick ?? host ?? "" }
    }

    /// Convenience: first parameter (common target for PRIVMSG, NOTICE, etc.)
    var target: String? { parameters.first }

    /// Convenience: last parameter (the "trailing" message body).
    var text: String? { parameters.last }

    /// IRCv3 server-time tag value, if present.
    var serverTime: Date? {
        guard let raw = tags["time"] as? String ?? nil,
              let flat = raw as String? else { return nil }
        return Date.fromIRCServerTime(flat)
    }

    /// IRCv3 message-id tag, if present.
    var messageID: String? {
        guard case let .some(v) = tags["msgid"] else { return nil }
        return v
    }
}
