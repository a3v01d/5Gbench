import SwiftUI

/// Reuses ChannelView for private message conversations.
struct DMView: View {
    let server: ServerStore
    let nick: String

    var body: some View {
        if let store = server.channelStore(for: nick, create: true) {
            ChannelView(server: server, channel: store)
                .navigationTitle(nick)
        }
    }
}
