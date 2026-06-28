import Foundation
import UserNotifications

@MainActor
final class PushNotificationHandler: NSObject, UNUserNotificationCenterDelegate, Sendable {

    weak var appStore: AppStore?
    weak var navigationStore: NavigationStore?

    func setup() {
        UNUserNotificationCenter.current().delegate = self
    }

    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completion: @escaping () -> Void
    ) {
        let info = response.notification.request.content.userInfo
        let serverIDStr = info["serverID"] as? String ?? ""
        let channel = info["channel"] as? String ?? ""

        if let serverID = UUID(uuidString: serverIDStr), !channel.isEmpty {
            Task { @MainActor [weak self] in
                self?.navigationStore?.select(channel: channel, on: serverID)
            }
        }
        completion()
    }

    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completion: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        // Show banner even when app is in foreground
        completion([.banner, .sound])
    }
}
