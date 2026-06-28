import Foundation

enum IRCParseError: Error, Equatable {
    case emptyLine
    case missingCommand
    case commandTooLong
}

/// Stateless RFC 1459 + IRCv3 message parser.
struct IRCMessageParser {

    static func parse(_ raw: String) throws -> IRCMessage {
        var line = raw
        // Strip trailing CRLF or LF
        while line.hasSuffix("\r\n") || line.hasSuffix("\n") || line.hasSuffix("\r") {
            line = String(line.dropLast())
        }
        guard !line.isEmpty else { throw IRCParseError.emptyLine }

        var remainder = line[line.startIndex...]

        // 1. IRCv3 message tags (@key=value;key2=value2 ...)
        var tags: [String: String?] = [:]
        if remainder.first == "@" {
            remainder = remainder.dropFirst()
            let end = remainder.firstIndex(of: " ") ?? remainder.endIndex
            let tagString = String(remainder[..<end])
            tags = parseTags(tagString)
            remainder = end < remainder.endIndex ? remainder[remainder.index(after: end)...] : remainder[remainder.endIndex...]
            // Skip extra spaces
            while remainder.first == " " { remainder = remainder.dropFirst() }
        }

        // 2. Prefix (:nick!user@host or :server)
        var prefix: IRCMessage.Prefix?
        if remainder.first == ":" {
            remainder = remainder.dropFirst()
            let end = remainder.firstIndex(of: " ") ?? remainder.endIndex
            let prefixString = String(remainder[..<end])
            prefix = parsePrefix(prefixString)
            remainder = end < remainder.endIndex ? remainder[remainder.index(after: end)...] : remainder[remainder.endIndex...]
            while remainder.first == " " { remainder = remainder.dropFirst() }
        }

        // 3. Command
        guard !remainder.isEmpty else { throw IRCParseError.missingCommand }
        let cmdEnd = remainder.firstIndex(of: " ") ?? remainder.endIndex
        let commandStr = String(remainder[..<cmdEnd])
        guard !commandStr.isEmpty else { throw IRCParseError.missingCommand }
        let command = IRCCommand(string: commandStr)
        remainder = cmdEnd < remainder.endIndex ? remainder[remainder.index(after: cmdEnd)...] : remainder[remainder.endIndex...]

        // 4. Parameters
        let parameters = parseParameters(remainder)

        return IRCMessage(tags: tags, prefix: prefix, command: command, parameters: parameters, raw: raw)
    }

    // MARK: - Private helpers

    private static func parseTags(_ tagString: String) -> [String: String?] {
        var result: [String: String?] = [:]
        for token in tagString.split(separator: ";", omittingEmptySubsequences: true) {
            let t = String(token)
            if let eq = t.firstIndex(of: "=") {
                let key = String(t[..<eq])
                let rawVal = String(t[t.index(after: eq)...])
                result[key] = unescapeTagValue(rawVal)
            } else {
                // Flag tag (no value)
                result[t] = nil
            }
        }
        return result
    }

    private static func unescapeTagValue(_ s: String) -> String {
        var out = ""
        var i = s.startIndex
        while i < s.endIndex {
            let c = s[i]
            if c == "\\" {
                let next = s.index(after: i)
                if next < s.endIndex {
                    switch s[next] {
                    case ":":  out.append(";")
                    case "s":  out.append(" ")
                    case "\\":out.append("\\")
                    case "r":  out.append("\r")
                    case "n":  out.append("\n")
                    default:   out.append(s[next])
                    }
                    i = s.index(after: next)
                    continue
                }
            }
            out.append(c)
            i = s.index(after: i)
        }
        return out
    }

    private static func parsePrefix(_ s: String) -> IRCMessage.Prefix {
        // nick!user@host  OR  nick@host  OR  server
        if let bang = s.firstIndex(of: "!") {
            let nick = String(s[..<bang])
            let rest = String(s[s.index(after: bang)...])
            if let at = rest.firstIndex(of: "@") {
                let user = String(rest[..<at])
                let host = String(rest[rest.index(after: at)...])
                return IRCMessage.Prefix(nick: nick, user: user, host: host)
            } else {
                return IRCMessage.Prefix(nick: nick, user: rest, host: nil)
            }
        } else if let at = s.firstIndex(of: "@") {
            let nick = String(s[..<at])
            let host = String(s[s.index(after: at)...])
            return IRCMessage.Prefix(nick: nick, user: nil, host: host)
        } else if s.contains(".") {
            // Heuristic: dots → server name
            return IRCMessage.Prefix(nick: nil, user: nil, host: s)
        } else {
            return IRCMessage.Prefix(nick: s, user: nil, host: nil)
        }
    }

    private static func parseParameters(_ remainder: Substring) -> [String] {
        var params: [String] = []
        var rest = remainder
        while !rest.isEmpty {
            while rest.first == " " { rest = rest.dropFirst() }
            guard !rest.isEmpty else { break }
            if rest.first == ":" {
                // Trailing parameter — everything after ':' is one param
                params.append(String(rest.dropFirst()))
                break
            }
            let end = rest.firstIndex(of: " ") ?? rest.endIndex
            params.append(String(rest[..<end]))
            rest = end < rest.endIndex ? rest[rest.index(after: end)...] : rest[rest.endIndex...]
        }
        return params
    }
}
