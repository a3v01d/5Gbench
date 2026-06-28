import Foundation
import UserNotifications

@Observable
@MainActor
final class NotificationStore {
    var totalMentions: Int = 0
    var isAuthorized: Bool = false

    func requestAuthorization() async {
        let center = UNUserNotificationCenter.current()
        let granted = (try? await center.requestAuthorization(options: [.alert, .badge, .sound])) ?? false
        isAuthorized = granted
    }

    func scheduleMentionNotification(from nick: String, in channel: String, text: String, serverID: UUID) {
        guard isAuthorized else { return }
        let content = UNMutableNotificationContent()
        content.title = "\(nick) in \(channel)"
        content.body = text
        content.sound = .default
        content.userInfo = [
            "serverID": serverID.uuidString,
            "channel": channel,
            "nick": nick
        ]

        let request = UNNotificationRequest(
            identifier: UUID().uuidString,
            content: content,
            trigger: nil   // deliver immediately
        )
        UNUserNotificationCenter.current().add(request)
        totalMentions += 1
        Task { HapticEngine.notification(.warning) }
    }

    func clearBadge() {
        totalMentions = 0
        UNUserNotificationCenter.current().setBadgeCount(0) { _ in }
    }
}
