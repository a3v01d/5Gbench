import Foundation

struct WhoisInfo: Sendable {
    var nick: String
    var username: String?
    var host: String?
    var realname: String?
    var server: String?
    var channels: [String] = []
    var idleSeconds: Int?
    var account: String?
    var isSecure: Bool = false
}

/// Per-user state, including whois cache and DM presence tracking.
actor UserSession {

    let nick: String
    private(set) var whois: WhoisInfo?
    private(set) var isAway: Bool = false
    private(set) var awayMessage: String?

    init(nick: String) { self.nick = nick }

    func updateWhois(_ info: WhoisInfo) { whois = info }

    func setAway(_ message: String?) {
        isAway = message != nil
        awayMessage = message
    }
}
