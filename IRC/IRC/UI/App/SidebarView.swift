import SwiftUI

struct SidebarView: View {
    @Environment(AppStore.self) private var appStore
    @State private var showAddServer = false
    @State private var showSettings = false

    var body: some View {
        @Bindable var nav = appStore.navigation
        List(selection: $nav.selectedDestination) {
            ForEach(appStore.servers) { server in
                Section {
                    DisclosureGroup {
                        ForEach(server.channels.filter(\.isJoined)) { chan in
                            channelRow(chan, server: server)
                        }
                        if !server.dmConversations.isEmpty {
                            Label("Direct Messages", systemImage: "message")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .listRowSeparator(.hidden)
                            ForEach(server.dmConversations) { dm in
                                channelRow(dm, server: server)
                            }
                        }
                    } label: {
                        ServerRowView(server: server)
                    }
                }
                .swipeActions(edge: .leading) {
                    connectButton(server)
                }
                .swipeActions(edge: .trailing) {
                    Button(role: .destructive) {
                        Task { await appStore.removeServer(id: server.id) }
                    } label: {
                        Label("Delete", systemImage: "trash")
                    }
                }
            }
        }
        .navigationTitle("IRC")
        .toolbar {
            ToolbarItem(placement: .topBarLeading) {
                Button { showSettings = true } label: {
                    Image(systemName: "gear")
                }
            }
            ToolbarItem(placement: .topBarTrailing) {
                Button { showAddServer = true } label: {
                    Image(systemName: "plus")
                }
            }
        }
        .sheet(isPresented: $showAddServer) { AddServerView() }
        .sheet(isPresented: $showSettings) { SettingsView() }
    }

    @ViewBuilder
    private func channelRow(_ chan: ChannelStore, server: ServerStore) -> some View {
        let dest = chan.isChannel
            ? NavigationDestination.channel(serverID: server.id, channelName: chan.displayName)
            : NavigationDestination.dm(serverID: server.id, nick: chan.displayName)

        HStack {
            Image(systemName: chan.isChannel ? "number" : "person.circle")
                .foregroundStyle(chan.isJoined ? .primary : .secondary)
                .font(.caption)
                .frame(width: 16)
            Text(chan.displayName)
                .font(.callout)
            Spacer()
            if chan.hasMention {
                Image(systemName: "at.circle.fill").foregroundStyle(.orange).font(.caption2)
            } else if chan.unreadCount > 0 {
                Text("\(chan.unreadCount)").font(.caption2.bold())
                    .padding(.horizontal, 4).padding(.vertical, 1)
                    .background(Color.accentColor).foregroundStyle(.white).clipShape(Capsule())
            }
        }
        .tag(dest)
    }

    @ViewBuilder
    private func connectButton(_ server: ServerStore) -> some View {
        switch server.connectionState {
        case .idle, .disconnected, .error:
            Button {
                Task { await appStore.connect(serverID: server.id) }
            } label: {
                Label("Connect", systemImage: "antenna.radiowaves.left.and.right")
            }
            .tint(.green)
        default:
            Button {
                Task { await appStore.disconnect(serverID: server.id) }
            } label: {
                Label("Disconnect", systemImage: "xmark.circle")
            }
            .tint(.red)
        }
    }
}
