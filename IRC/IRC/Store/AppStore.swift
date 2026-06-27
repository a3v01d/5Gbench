import Foundation
import SwiftData

/// Root application state. Owns all server stores and cross-cutting concerns.
@Observable
@MainActor
final class AppStore {

    var servers: [ServerStore] = []
    var navigation = NavigationStore()
    var notifications = NotificationStore()

    private(set) var modelContainer: ModelContainer

    init(modelContainer: ModelContainer) {
        self.modelContainer = modelContainer
    }

    // MARK: - Server management

    func loadServers() {
        let ctx = ModelContext(modelContainer)
        let configs = (try? ctx.fetch(FetchDescriptor<ServerConfig>(sortBy: [SortDescriptor(\.sortOrder)]))) ?? []
        servers = configs.map { ServerStore(config: $0, appStore: self) }
    }

    func addServer(config: ServerConfig) {
        let ctx = ModelContext(modelContainer)
        ctx.insert(config)
        try? ctx.save()
        let store = ServerStore(config: config, appStore: self)
        servers.append(store)
    }

    func removeServer(id: UUID) async {
        guard let idx = servers.firstIndex(where: { $0.id == id }) else { return }
        await servers[idx].disconnect()
        servers.remove(at: idx)

        let ctx = ModelContext(modelContainer)
        let configs = (try? ctx.fetch(FetchDescriptor<ServerConfig>())) ?? []
        if let cfg = configs.first(where: { $0.id == id }) {
            ctx.delete(cfg)
            try? ctx.save()
        }
    }

    func connect(serverID: UUID) async {
        guard let store = servers.first(where: { $0.id == serverID }) else { return }
        await store.connect()
    }

    func disconnect(serverID: UUID) async {
        guard let store = servers.first(where: { $0.id == serverID }) else { return }
        await store.disconnect()
    }

    // MARK: - Background / foreground

    func handleBackground() async {
        for server in servers {
            await server.setAway(message: "Away (backgrounded)")
        }
    }

    func handleForeground() async {
        for server in servers {
            await server.setAway(message: nil)
        }
    }
}
