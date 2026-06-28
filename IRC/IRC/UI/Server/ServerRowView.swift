import SwiftUI

struct ServerRowView: View {
    let server: ServerStore

    var body: some View {
        HStack {
            StatusDotView(state: server.connectionState)
            VStack(alignment: .leading, spacing: 2) {
                Text(server.config.displayName)
                    .font(.headline)
                Text(server.config.host)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            if server.hasMention {
                Image(systemName: "at.circle.fill")
                    .foregroundStyle(.orange)
                    .font(.caption)
            } else if server.totalUnread > 0 {
                Text("\(server.totalUnread)")
                    .font(.caption.bold())
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(Color.accentColor)
                    .foregroundStyle(.white)
                    .clipShape(Capsule())
            }
        }
    }
}
