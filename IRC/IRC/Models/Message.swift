import Foundation
import SwiftData

@Model
final class Message {
    var id: String
    var serverID: UUID
    var channelName: String
    var senderNick: String
    var senderHostmask: String?
    var content: String
    var timestamp: Date
    var kindRaw: Int
    var isMention: Bool
    var isRead: Bool

    enum MessageKind: Int, Codable {
        case privmsg = 0, notice, action, join, part, quit, kick
        case mode, topic, nick, serverInfo, error
    }

    var kind: MessageKind {
        get { MessageKind(rawValue: kindRaw) ?? .privmsg }
        set { kindRaw = newValue.rawValue }
    }

    init(
        id: String = UUID().uuidString,
        serverID: UUID,
        channelName: String,
        senderNick: String,
        senderHostmask: String? = nil,
        content: String,
        timestamp: Date = .now,
        kind: MessageKind = .privmsg,
        isMention: Bool = false,
        isRead: Bool = false
    ) {
        self.id = id
        self.serverID = serverID
        self.channelName = channelName
        self.senderNick = senderNick
        self.senderHostmask = senderHostmask
        self.content = content
        self.timestamp = timestamp
        self.kindRaw = kind.rawValue
        self.isMention = isMention
        self.isRead = isRead
    }
}
