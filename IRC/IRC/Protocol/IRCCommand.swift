import Foundation

enum IRCCommand: Hashable, Sendable {
    // Connection
    case PASS, NICK, USER, QUIT, PING, PONG, ERROR
    // Channel
    case JOIN, PART, MODE, TOPIC, NAMES, LIST, INVITE, KICK
    // Messaging
    case PRIVMSG, NOTICE
    // User
    case WHOIS, WHO, WHOWAS, AWAY, ISON, USERHOST
    // IRCv3 / capability
    case CAP, AUTHENTICATE, BATCH, TAGMSG, FAIL, WARN, NOTE
    // Numeric replies
    case numeric(Int)
    // Anything not explicitly listed
    case unknown(String)

    init(string: String) {
        if let n = Int(string), string.count == 3 {
            self = .numeric(n)
            return
        }
        switch string.uppercased() {
        case "PASS":         self = .PASS
        case "NICK":         self = .NICK
        case "USER":         self = .USER
        case "QUIT":         self = .QUIT
        case "PING":         self = .PING
        case "PONG":         self = .PONG
        case "ERROR":        self = .ERROR
        case "JOIN":         self = .JOIN
        case "PART":         self = .PART
        case "MODE":         self = .MODE
        case "TOPIC":        self = .TOPIC
        case "NAMES":        self = .NAMES
        case "LIST":         self = .LIST
        case "INVITE":       self = .INVITE
        case "KICK":         self = .KICK
        case "PRIVMSG":      self = .PRIVMSG
        case "NOTICE":       self = .NOTICE
        case "WHOIS":        self = .WHOIS
        case "WHO":          self = .WHO
        case "WHOWAS":       self = .WHOWAS
        case "AWAY":         self = .AWAY
        case "ISON":         self = .ISON
        case "USERHOST":     self = .USERHOST
        case "CAP":          self = .CAP
        case "AUTHENTICATE": self = .AUTHENTICATE
        case "BATCH":        self = .BATCH
        case "TAGMSG":       self = .TAGMSG
        case "FAIL":         self = .FAIL
        case "WARN":         self = .WARN
        case "NOTE":         self = .NOTE
        default:             self = .unknown(string.uppercased())
        }
    }

    var rawValue: String {
        switch self {
        case .PASS:         return "PASS"
        case .NICK:         return "NICK"
        case .USER:         return "USER"
        case .QUIT:         return "QUIT"
        case .PING:         return "PING"
        case .PONG:         return "PONG"
        case .ERROR:        return "ERROR"
        case .JOIN:         return "JOIN"
        case .PART:         return "PART"
        case .MODE:         return "MODE"
        case .TOPIC:        return "TOPIC"
        case .NAMES:        return "NAMES"
        case .LIST:         return "LIST"
        case .INVITE:       return "INVITE"
        case .KICK:         return "KICK"
        case .PRIVMSG:      return "PRIVMSG"
        case .NOTICE:       return "NOTICE"
        case .WHOIS:        return "WHOIS"
        case .WHO:          return "WHO"
        case .WHOWAS:       return "WHOWAS"
        case .AWAY:         return "AWAY"
        case .ISON:         return "ISON"
        case .USERHOST:     return "USERHOST"
        case .CAP:          return "CAP"
        case .AUTHENTICATE: return "AUTHENTICATE"
        case .BATCH:        return "BATCH"
        case .TAGMSG:       return "TAGMSG"
        case .FAIL:         return "FAIL"
        case .WARN:         return "WARN"
        case .NOTE:         return "NOTE"
        case .numeric(let n): return String(format: "%03d", n)
        case .unknown(let s): return s
        }
    }
}
