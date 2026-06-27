import Foundation

/// Builds raw IRC wire-format lines from structured data.
struct IRCMessageEncoder {

    // Max bytes per IRC line (classic 512, IRCv3 MAXLINE up to 8704)
    static let maxLineBytes = 512

    /// Encodes a command with optional prefix and parameters into a wire-format line.
    /// The returned string includes the trailing CRLF.
    static func encode(command: IRCCommand, parameters: [String] = [], tags: [String: String] = []) -> String {
        var line = ""

        if !tags.isEmpty {
            let tagStr = tags.map { k, v in
                v.isEmpty ? k : "\(k)=\(escapeTagValue(v))"
            }.joined(separator: ";")
            line += "@\(tagStr) "
        }

        line += command.rawValue

        for (index, param) in parameters.enumerated() {
            let isLast = index == parameters.count - 1
            let needsColon = isLast && (param.contains(" ") || param.hasPrefix(":") || param.isEmpty)
            line += needsColon ? " :\(param)" : " \(param)"
        }

        line += "\r\n"
        return line
    }

    // Convenience encoders for common commands

    static func pass(_ password: String) -> String {
        encode(command: .PASS, parameters: [password])
    }

    static func nick(_ nickname: String) -> String {
        encode(command: .NICK, parameters: [nickname])
    }

    static func user(username: String, realname: String) -> String {
        encode(command: .USER, parameters: [username, "0", "*", realname])
    }

    static func ping(server: String) -> String {
        encode(command: .PING, parameters: [server])
    }

    static func pong(token: String) -> String {
        encode(command: .PONG, parameters: [token])
    }

    static func join(channel: String, key: String? = nil) -> String {
        var params = [channel]
        if let key { params.append(key) }
        return encode(command: .JOIN, parameters: params)
    }

    static func part(channel: String, message: String? = nil) -> String {
        var params = [channel]
        if let msg = message { params.append(msg) }
        return encode(command: .PART, parameters: params)
    }

    static func quit(message: String = "Bye") -> String {
        encode(command: .QUIT, parameters: [message])
    }

    static func privmsg(target: String, text: String) -> String {
        encode(command: .PRIVMSG, parameters: [target, text])
    }

    static func notice(target: String, text: String) -> String {
        encode(command: .NOTICE, parameters: [target, text])
    }

    static func topic(channel: String, text: String) -> String {
        encode(command: .TOPIC, parameters: [channel, text])
    }

    static func kick(channel: String, nick: String, reason: String? = nil) -> String {
        var params = [channel, nick]
        if let r = reason { params.append(r) }
        return encode(command: .KICK, parameters: params)
    }

    static func mode(target: String, flags: String, params: [String] = []) -> String {
        encode(command: .MODE, parameters: [target, flags] + params)
    }

    static func whois(nick: String) -> String {
        encode(command: .WHOIS, parameters: [nick])
    }

    static func away(message: String? = nil) -> String {
        if let msg = message {
            return encode(command: .AWAY, parameters: [msg])
        }
        return encode(command: .AWAY)
    }

    static func cap(subcommand: String, params: [String] = []) -> String {
        encode(command: .CAP, parameters: [subcommand] + params)
    }

    static func authenticate(payload: String) -> String {
        encode(command: .AUTHENTICATE, parameters: [payload])
    }

    // MARK: - Tag value escaping

    private static func escapeTagValue(_ s: String) -> String {
        s.replacingOccurrences(of: "\\", with: "\\\\")
         .replacingOccurrences(of: ";",  with: "\\:")
         .replacingOccurrences(of: " ",  with: "\\s")
         .replacingOccurrences(of: "\r", with: "\\r")
         .replacingOccurrences(of: "\n", with: "\\n")
    }
}
