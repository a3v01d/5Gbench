import Foundation
import SwiftData

@Model
final class ServerConfig {
    var id: UUID
    var displayName: String
    var host: String
    var port: Int
    var useTLS: Bool
    var acceptInvalidCert: Bool
    var nickname: String
    var alternateNick: String
    var realname: String
    var username: String
    var useSASL: Bool
    var saslUsername: String?
    var autoConnect: Bool
    var sortOrder: Int

    @Relationship(deleteRule: .cascade, inverse: \ChannelConfig.serverConfig)
    var channels: [ChannelConfig] = []

    // MARK: - Keychain key references (actual secrets stored in Keychain)

    var keychainServerPasswordKey: String { "irc.server.\(id).pass" }
    var keychainSASLPasswordKey: String   { "irc.server.\(id).sasl" }
    var keychainNickServPasswordKey: String { "irc.server.\(id).nickserv" }

    init(
        displayName: String,
        host: String,
        port: Int = 6697,
        useTLS: Bool = true,
        acceptInvalidCert: Bool = false,
        nickname: String = "ircuser",
        alternateNick: String = "ircuser_",
        realname: String = "IRC User",
        username: String = "ircuser",
        useSASL: Bool = false,
        saslUsername: String? = nil,
        autoConnect: Bool = false,
        sortOrder: Int = 0
    ) {
        self.id = UUID()
        self.displayName = displayName
        self.host = host
        self.port = port
        self.useTLS = useTLS
        self.acceptInvalidCert = acceptInvalidCert
        self.nickname = nickname
        self.alternateNick = alternateNick
        self.realname = realname
        self.username = username
        self.useSASL = useSASL
        self.saslUsername = saslUsername
        self.autoConnect = autoConnect
        self.sortOrder = sortOrder
    }
}
