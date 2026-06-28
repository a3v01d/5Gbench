import SwiftUI

struct SettingsView: View {
    @Environment(AppStore.self) private var appStore
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            Form {
                Section("Notifications") {
                    NavigationLink("Notification Settings") {
                        NotificationSettingsView()
                    }
                }
                Section("Servers") {
                    ForEach(appStore.servers) { server in
                        NavigationLink(server.config.displayName) {
                            ServerSettingsView(server: server)
                        }
                    }
                }
                Section {
                    Link("IRC Protocol Reference", destination: URL(string: "https://modern.ircdocs.horse")!)
                }
            }
            .navigationTitle("Settings")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}
