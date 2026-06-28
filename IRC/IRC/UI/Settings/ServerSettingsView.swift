import SwiftUI

struct ServerSettingsView: View {
    let server: ServerStore

    @State private var displayName: String
    @State private var nickname: String
    @State private var autoConnect: Bool

    init(server: ServerStore) {
        self.server = server
        _displayName = State(initialValue: server.config.displayName)
        _nickname = State(initialValue: server.config.nickname)
        _autoConnect = State(initialValue: server.config.autoConnect)
    }

    var body: some View {
        Form {
            Section("Display") {
                TextField("Display Name", text: $displayName)
            }
            Section("Identity") {
                TextField("Nickname", text: $nickname)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
            }
            Section {
                Toggle("Auto-connect", isOn: $autoConnect)
            }
            Section("Connection") {
                LabeledContent("Host", value: server.config.host)
                LabeledContent("Port", value: "\(server.config.port)")
                LabeledContent("TLS", value: server.config.useTLS ? "Yes" : "No")
            }
        }
        .navigationTitle(server.config.displayName)
        .onDisappear {
            // Persist changes (in a real app, use proper SwiftData context)
            server.config.displayName = displayName
            server.config.nickname = nickname
            server.config.autoConnect = autoConnect
        }
    }
}
