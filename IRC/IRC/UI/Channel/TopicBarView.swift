import SwiftUI

struct TopicBarView: View {
    let topic: String?
    let onTap: () -> Void

    var body: some View {
        if let topic, !topic.isEmpty {
            Button(action: onTap) {
                Text(topic)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 6)
            }
            .buttonStyle(.plain)
            .background(Color(.secondarySystemBackground))
            Divider()
        }
    }
}
