import SwiftUI
import BackgroundTasks

@main
struct IRCApp: App {

    @State private var appStore: AppStore

    private let notifHandler = PushNotificationHandler()

    init() {
        let container = try! AppSchema.makeContainer()
        let store = AppStore(modelContainer: container)
        _appStore = State(initialValue: store)
        BackgroundTaskManager.registerTasks()
    }

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(appStore)
                .task {
                    appStore.loadServers()
                    notifHandler.appStore = appStore
                    notifHandler.navigationStore = appStore.navigation
                    notifHandler.setup()
                    await appStore.notifications.requestAuthorization()

                    // Auto-connect servers
                    for server in appStore.servers where server.config.autoConnect {
                        await appStore.connect(serverID: server.id)
                    }
                }
                .onChange(of: scenePhase) { _, phase in
                    switch phase {
                    case .background:
                        Task { await appStore.handleBackground() }
                        BackgroundTaskManager.scheduleNextRefresh()
                    case .active:
                        Task { await appStore.handleForeground() }
                        appStore.notifications.clearBadge()
                    default:
                        break
                    }
                }
        }
    }

    @Environment(\.scenePhase) private var scenePhase
}
