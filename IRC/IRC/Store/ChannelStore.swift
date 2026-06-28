import Foundation
import SwiftUI

/// In-memory ring-buffer of messages + live member list for one channel or DM.
@Observable
@MainActor
final class ChannelStore: Identifiable {

    static let maxMessages = 2000

    let serverID: UUID
    let channelName: String   // normalized
    let displayName: String   // raw (preserves case)

    var messages: [ChatMessage] = []
    var members: [ChannelMember] = []
    var topic: String?
    var modeString: String = ""
    var unreadCount: Int = 0
    var hasMention: Bool = false
    var isJoined: Bool = false

    var id: String { "\(serverID)/\(channelName)" }
    var isChannel: Bool { displayName.isChannelName }

    init(serverID: UUID, channelName: String, displayName: String) {
        self.serverID = serverID
        self.channelName = channelName
        self.displayName = displayName
    }

    func appendMessage(_ msg: ChatMessage) {
        messages.append(msg)
        if messages.count > ChannelStore.maxMessages {
            messages.removeFirst(messages.count - ChannelStore.maxMessages)
        }
        unreadCount += 1
        if msg.isMention { hasMention = true }
    }

    func markRead() {
        unreadCount = 0
        hasMention = false
    }

    func updateMembers(_ newMembers: [ChannelMember]) {
        members = newMembers
    }
}
