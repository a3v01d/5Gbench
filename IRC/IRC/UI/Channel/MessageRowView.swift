import SwiftUI

struct MessageRowView: View, Equatable {
    let message: ChatMessage

    static func == (lhs: MessageRowView, rhs: MessageRowView) -> Bool {
        lhs.message.id == rhs.message.id
    }

    var body: some View {
        HStack(alignment: .top, spacing: 6) {
            TimestampView(date: message.timestamp)
                .frame(width: 44, alignment: .trailing)

            contentView
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 1)
        .mentionHighlight(message.isMention)
    }

    @ViewBuilder
    private var contentView: some View {
        switch message.kind {
        case .action:
            Text("* \(message.senderNick) \(message.content)")
                .italic()
                .foregroundStyle(.primary)

        case .join, .part, .quit, .kick, .nick, .mode, .topic:
            Text(message.content)
                .foregroundStyle(.secondary)
                .font(.footnote)

        case .serverInfo:
            Text(message.content)
                .foregroundStyle(.tertiary)
                .font(.caption)

        case .error:
            Text(message.content)
                .foregroundStyle(.red)
                .font(.footnote)

        default:
            HStack(alignment: .top, spacing: 4) {
                NickPillView(nick: message.senderNick, prefix: nil)
                Text(message.content)
                    .foregroundStyle(.primary)
                    .textSelection(.enabled)
            }
        }
    }
}
