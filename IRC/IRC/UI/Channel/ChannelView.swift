import SwiftUI

struct ChannelView: View {
    let server: ServerStore
    let channel: ChannelStore

    @State private var inputText = ""
    @State private var showUserList = false
    @State private var showTopicEditor = false

    @Environment(AppStore.self) private var appStore

    var body: some View {
        VStack(spacing: 0) {
            TopicBarView(topic: channel.topic) {
                showTopicEditor = true
            }

            MessageListView(messages: channel.messages)
                .onAppear { channel.markRead() }

            Divider()

            InputBarView(
                text: $inputText,
                placeholder: channel.displayName,
                onSend: { text in
                    Task {
                        let inv = CommandParser.parse(input: text, currentTarget: channel.displayName)
                        let dispatcher = CommandDispatcher(server: server, channelStore: channel)
                        _ = await dispatcher.dispatch(inv)
                    }
                }
            )
        }
        .navigationTitle(channel.displayName)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                if channel.isChannel {
                    Button {
                        showUserList = true
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: "person.2")
                            Text("\(channel.members.count)")
                                .font(.caption)
                        }
                    }
                }
            }
        }
        .sheet(isPresented: $showUserList) {
            UserListView(members: channel.members) { member in
                // Open DM with selected user
                let dm = server.channelStore(for: member.nick, create: true)
                _ = dm
                appStore.navigation.select(dm: member.nick, on: server.id)
            }
            .presentationDetents([.medium, .large])
        }
        .sheet(isPresented: $showTopicEditor) {
            TopicEditorView(current: channel.topic ?? "") { newTopic in
                Task {
                    await server.send(rawLine: IRCMessageEncoder.topic(channel: channel.displayName, text: newTopic))
                }
            }
        }
    }
}

private struct TopicEditorView: View {
    @State var text: String
    let onSave: (String) -> Void
    @Environment(\.dismiss) private var dismiss

    init(current: String, onSave: @escaping (String) -> Void) {
        _text = State(initialValue: current)
        self.onSave = onSave
    }

    var body: some View {
        NavigationStack {
            Form {
                TextEditor(text: $text)
                    .frame(minHeight: 80)
            }
            .navigationTitle("Edit Topic")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() } }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") {
                        onSave(text)
                        dismiss()
                    }
                }
            }
        }
        .presentationDetents([.medium])
    }
}
