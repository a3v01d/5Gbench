import SwiftUI

struct ServerDetailView: View {
    let server: ServerStore

    @Environment(AppStore.self) private var appStore
    @State private var joinChannelText = ""
    @State private var showJoinField = false

    var body: some View {
        List {
            Section("Channels") {
                ForEach(server.channels.filter(\.isChannel)) { chan in
                    Button {
                        appStore.navigation.select(channel: chan.displayName, on: server.id)
                    } label: {
                        HStack {
                            Image(systemName: chan.isJoined ? "number.circle.fill" : "number.circle")
                                .foregroundStyle(chan.isJoined ? .accentColor : .secondary)
                            Text(chan.displayName)
                            Spacer()
                            if chan.hasMention {
                                Image(systemName: "at.circle.fill").foregroundStyle(.orange).font(.caption)
                            } else if chan.unreadCount > 0 {
                                Text("\(chan.unreadCount)")
                                    .font(.caption.bold())
                                    .padding(.horizontal, 6).padding(.vertical, 2)
                                    .background(Color.accentColor)
                                    .foregroundStyle(.white)
                                    .clipShape(Capsule())
                            }
                        }
                    }
                    .buttonStyle(.plain)
                    .swipeActions(edge: .trailing) {
                        if chan.isJoined {
                            Button(role: .destructive) {
                                Task { await server.part(channel: chan.displayName) }
                            } label: {
                                Label("Leave", systemImage: "arrow.right.square")
                            }
                        } else {
                            Button {
                                Task { await server.join(channel: chan.displayName) }
                            } label: {
                                Label("Join", systemImage: "arrow.left.square")
                            }
                            .tint(.green)
                        }
                    }
                }
            }

            if !server.dmConversations.isEmpty {
                Section("Direct Messages") {
                    ForEach(server.dmConversations) { dm in
                        Button {
                            appStore.navigation.select(dm: dm.displayName, on: server.id)
                        } label: {
                            Label(dm.displayName, systemImage: "person.circle")
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
        }
        .navigationTitle(server.config.displayName)
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                Menu {
                    Button { showJoinField = true } label: { Label("Join Channel", systemImage: "number") }
                    Divider()
                    if server.connectionState == .idle || server.connectionState == .disconnected {
                        Button { Task { await appStore.connect(serverID: server.id) } } label: {
                            Label("Connect", systemImage: "antenna.radiowaves.left.and.right")
                        }
                    } else {
                        Button(role: .destructive) {
                            Task { await appStore.disconnect(serverID: server.id) }
                        } label: {
                            Label("Disconnect", systemImage: "xmark.circle")
                        }
                    }
                } label: {
                    Image(systemName: "ellipsis.circle")
                }
            }
        }
        .alert("Join Channel", isPresented: $showJoinField) {
            TextField("#channel", text: $joinChannelText)
                .textInputAutocapitalization(.never)
                .autocorrectionDisabled()
            Button("Join") {
                Task { await server.join(channel: joinChannelText) }
                joinChannelText = ""
            }
            Button("Cancel", role: .cancel) {}
        }
    }
}
