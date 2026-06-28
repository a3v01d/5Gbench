import Foundation
import SwiftUI

enum NavigationDestination: Hashable, Sendable {
    case channel(serverID: UUID, channelName: String)
    case dm(serverID: UUID, nick: String)
    case serverInfo(serverID: UUID)
    case settings
}

@Observable
@MainActor
final class NavigationStore {
    var selectedServer: UUID?
    var selectedDestination: NavigationDestination?
    var columnVisibility: NavigationSplitViewVisibility = .all

    func select(channel: String, on serverID: UUID) {
        selectedServer = serverID
        selectedDestination = .channel(serverID: serverID, channelName: channel)
    }

    func select(dm nick: String, on serverID: UUID) {
        selectedServer = serverID
        selectedDestination = .dm(serverID: serverID, nick: nick)
    }

    func openSettings() {
        selectedDestination = .settings
    }
}
