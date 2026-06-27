import Foundation
import SwiftData

/// Main-actor observable state for one IRC server.
@Observable
@MainActor
final class ServerStore: Identifiable {

    let id: UUID
    var config: ServerConfig
    var connectionState: IRCSessionState = .idle
    var channels: [ChannelStore] = []    // sorted: joined channels first, then AZ
    var dmConversations: [ChannelStore] = []

    var totalUnread: Int { channels.reduce(0) { $0 + $1.unreadCount } }
    var hasMention: Bool { channels.contains { $0.hasMention } }

    private var session: IRCSession?
    private var eventTask: Task<Void, Never>?
    private weak var appStore: AppStore?

    init(config: ServerConfig, appStore: AppStore) {
        self.id = config.id
        self.config = config
        self.appStore = appStore
    }

    // MARK: - Lifecycle

    func connect() async {
        let s = IRCSession(config: config)
        session = s
        startObservingSession(s)
        await s.connect()
    }

    func disconnect() async {
        await session?.disconnect()
        session = nil
        eventTask?.cancel()
        eventTask = nil
    }

    func join(channel: String, key: String? = nil) async {
        await session?.join(channel: channel, key: key)
    }

    func part(channel: String, message: String? = nil) async {
        await session?.part(channel: channel, message: message)
    }

    func send(privmsg text: String, to target: String) async {
        await session?.send(privmsg: text, to: target)
    }

    func send(rawLine: String) async {
        await session?.send(rawLine: rawLine)
    }

    func setAway(message: String?) async {
        await session?.setAway(message: message)
    }

    // MARK: - Session event bridge

    private func startObservingSession(_ session: IRCSession) {
        eventTask?.cancel()
        eventTask = Task { @MainActor [weak self] in
            for await event in session.events {
                guard let self else { return }
                self.handle(event: event)
            }
        }
    }

    private func handle(event: SessionEvent) {
        switch event {
        case .stateChanged(let s):
            connectionState = s

        case .messageReceived(let msg):
            let store = channelStore(for: msg.target, create: true)!
            store.appendMessage(msg)
            if msg.isMention {
                appStore?.notifications.scheduleMentionNotification(
                    from: msg.senderNick, in: msg.target, text: msg.content, serverID: id
                )
            }

        case .channelJoined(let name):
            let store = channelStore(for: name, create: true)!
            store.isJoined = true
            sortChannels()

        case .channelParted(let name):
            channelStore(for: name)?.isJoined = false
            sortChannels()

        case .membersUpdated(let channel, let members):
            channelStore(for: channel)?.updateMembers(members)

        case .memberJoined(let channel, _):
            _ = channelStore(for: channel)  // ensure exists

        case .memberLeft(let channel, _, _):
            _ = channelStore(for: channel)

        case .topicChanged(let channel, let topic):
            channelStore(for: channel)?.topic = topic

        case .nickChanged(let oldNick, let newNick, let inChannels):
            for chanName in inChannels {
                let store = channelStore(for: chanName)
                store?.members = store?.members.map { m in
                    var copy = m; if copy.nick == oldNick { copy.nick = newNick }; return copy
                } ?? []
            }

        case .modesChanged:
            break
        }
    }

    // MARK: - Channel store lookup

    func channelStore(for name: String, create: Bool = false) -> ChannelStore? {
        let normalized = name.lowercased()
        if let existing = channels.first(where: { $0.channelName == normalized }) {
            return existing
        }
        if !name.isChannelName {
            // DM
            if let dm = dmConversations.first(where: { $0.channelName == normalized }) { return dm }
            if create {
                let dm = ChannelStore(serverID: id, channelName: normalized, displayName: name)
                dmConversations.append(dm)
                return dm
            }
        }
        if create {
            let store = ChannelStore(serverID: id, channelName: normalized, displayName: name)
            channels.append(store)
            sortChannels()
            return store
        }
        return nil
    }

    private func sortChannels() {
        channels.sort {
            if $0.isJoined != $1.isJoined { return $0.isJoined }
            return $0.displayName < $1.displayName
        }
    }
}
