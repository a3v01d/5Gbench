import SwiftUI

struct DMListView: View {
    let server: ServerStore

    @Environment(AppStore.self) private var appStore

    var body: some View {
        List(server.dmConversations) { dm in
            Button {
                appStore.navigation.select(dm: dm.displayName, on: server.id)
            } label: {
                HStack {
                    Image(systemName: "person.circle")
                        .foregroundStyle(.accent)
                    VStack(alignment: .leading) {
                        Text(dm.displayName)
                            .font(.headline)
                        if let last = dm.messages.last {
                            Text(last.content)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                        }
                    }
                    Spacer()
                    if dm.unreadCount > 0 {
                        Text("\(dm.unreadCount)")
                            .font(.caption.bold())
                            .padding(.horizontal, 6)
                            .padding(.vertical, 2)
                            .background(Color.accentColor)
                            .foregroundStyle(.white)
                            .clipShape(Capsule())
                    }
                }
            }
            .buttonStyle(.plain)
        }
        .navigationTitle("Direct Messages")
    }
}
