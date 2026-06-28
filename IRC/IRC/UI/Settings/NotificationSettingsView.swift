import SwiftUI

struct NotificationSettingsView: View {
    @Environment(AppStore.self) private var appStore

    var body: some View {
        Form {
            Section {
                HStack {
                    Text("Status")
                    Spacer()
                    Text(appStore.notifications.isAuthorized ? "Authorized" : "Not authorized")
                        .foregroundStyle(.secondary)
                }
                if !appStore.notifications.isAuthorized {
                    Button("Request Permission") {
                        Task { await appStore.notifications.requestAuthorization() }
                    }
                }
            }
        }
        .navigationTitle("Notifications")
    }
}
