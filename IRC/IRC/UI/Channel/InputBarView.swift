import SwiftUI

struct InputBarView: View {
    @Binding var text: String
    let placeholder: String
    let onSend: (String) -> Void

    @State private var suggestions: [CommandDefinition] = []
    @FocusState private var focused: Bool

    var body: some View {
        VStack(spacing: 0) {
            // Command autocomplete popover
            if !suggestions.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(suggestions, id: \.name) { cmd in
                            Button(cmd.name) {
                                applySuggestion(cmd)
                            }
                            .buttonStyle(.bordered)
                            .controlSize(.small)
                        }
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 4)
                }
                .background(Color(.secondarySystemBackground))
                Divider()
            }

            HStack(spacing: 8) {
                TextField(placeholder, text: $text, axis: .vertical)
                    .textFieldStyle(.plain)
                    .lineLimit(1...5)
                    .focused($focused)
                    .padding(.vertical, 8)
                    .onChange(of: text) { _, newValue in
                        updateSuggestions(for: newValue)
                    }

                Button {
                    submitText()
                } label: {
                    Image(systemName: "arrow.up.circle.fill")
                        .font(.title2)
                }
                .disabled(text.trimmingCharacters(in: .whitespaces).isEmpty)
            }
            .padding(.horizontal, 12)
            .padding(.bottom, 8)
            .background(Color(.systemBackground))
        }
    }

    private func updateSuggestions(for input: String) {
        guard input.hasPrefix("/") else { suggestions = []; return }
        let verb = String(input.dropFirst()).split(separator: " ").first.map(String.init) ?? ""
        // Only show suggestions while the user is still typing the verb (no space yet)
        if input.contains(" ") {
            suggestions = []
        } else {
            suggestions = CommandRegistry.matching(prefix: verb)
        }
    }

    private func applySuggestion(_ cmd: CommandDefinition) {
        text = "/\(cmd.name) "
        suggestions = []
        focused = true
    }

    private func submitText() {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return }
        onSend(trimmed)
        text = ""
        suggestions = []
    }
}
