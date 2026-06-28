import Foundation

/// An IRCv3 capability identifier, strongly typed.
struct IRCCapability: RawRepresentable, Hashable, Sendable {
    let rawValue: String

    init(rawValue: String) { self.rawValue = rawValue }
    init(_ rawValue: String) { self.rawValue = rawValue }

    // Core IRCv3 capabilities we request
    static let multiPrefix      = IRCCapability("multi-prefix")
    static let serverTime       = IRCCapability("server-time")
    static let messageIDs       = IRCCapability("message-ids")
    static let sasl             = IRCCapability("sasl")
    static let awayNotify       = IRCCapability("away-notify")
    static let accountNotify    = IRCCapability("account-notify")
    static let extendedJoin     = IRCCapability("extended-join")
    static let batch            = IRCCapability("batch")
    static let labeledResponse  = IRCCapability("labeled-response")
    static let chatHistory      = IRCCapability("draft/chathistory")
    static let setname          = IRCCapability("setname")
    static let echo             = IRCCapability("echo-message")
    static let userhosts        = IRCCapability("userhost-in-names")

    /// The set we will attempt to request on every connection.
    static let desired: Set<IRCCapability> = [
        .multiPrefix, .serverTime, .messageIDs, .sasl, .awayNotify,
        .accountNotify, .extendedJoin, .batch, .labeledResponse,
        .chatHistory, .echo, .userhosts
    ]
}
