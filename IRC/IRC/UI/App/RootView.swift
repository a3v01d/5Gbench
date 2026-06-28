import SwiftUI

struct RootView: View {
    @Environment(AppStore.self) private var appStore

    var body: some View {
        @Bindable var nav = appStore.navigation
        NavigationSplitView(columnVisibility: $nav.columnVisibility) {
            SidebarView()
        } content: {
            if let serverID = nav.selectedServer,
               let server = appStore.servers.first(where: { $0.id == serverID }) {
                ServerDetailView(server: server)
            } else {
                ContentUnavailableView("Select a Server", systemImage: "antenna.radiowaves.left.and.right")
            }
        } detail: {
            detailView
        }
        .navigationSplitViewStyle(.balanced)
    }

    @ViewBuilder
    private var detailView: some View {
        if let dest = appStore.navigation.selectedDestination {
            switch dest {
            case .channel(let serverID, let channelName):
                if let server = appStore.servers.first(where: { $0.id == serverID }),
                   let chan = server.channelStore(for: channelName) {
                    ChannelView(server: server, channel: chan)
                } else {
                    ContentUnavailableView("Channel not found", systemImage: "number.circle")
                }

            case .dm(let serverID, let nick):
                if let server = appStore.servers.first(where: { $0.id == serverID }) {
                    DMView(server: server, nick: nick)
                } else {
                    ContentUnavailableView("Server not found", systemImage: "xmark.circle")
                }

            case .serverInfo(let serverID):
                if let server = appStore.servers.first(where: { $0.id == serverID }) {
                    ServerDetailView(server: server)
                }

            case .settings:
                SettingsView()
            }
        } else {
            ContentUnavailableView(
                "Welcome to IRC",
                systemImage: "bubble.left.and.bubble.right",
                description: Text("Add a server from the sidebar to get started.")
            )
        }
    }
}
