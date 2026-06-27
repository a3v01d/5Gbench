import SwiftUI

struct UserListView: View {
    let members: [ChannelMember]
    let onSelectUser: (ChannelMember) -> Void
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            List(members) { member in
                Button {
                    dismiss()
                    onSelectUser(member)
                } label: {
                    HStack {
                        NickPillView(nick: member.nick, prefix: member.highestPrefix)
                        Spacer()
                        Image(systemName: "chevron.right")
                            .foregroundStyle(.tertiary)
                            .font(.caption)
                    }
                }
                .buttonStyle(.plain)
            }
            .navigationTitle("\(members.count) users")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}
