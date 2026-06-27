import Foundation
import BackgroundTasks
import os.log

private let log = Logger(subsystem: "com.irc", category: "BackgroundTasks")

enum BackgroundTaskManager {

    static let refreshIdentifier   = "com.irc.refresh"
    static let processingIdentifier = "com.irc.reconnect"

    static func registerTasks() {
        BGTaskScheduler.shared.register(forTaskWithIdentifier: refreshIdentifier, using: nil) { task in
            Task { @MainActor in
                await handleRefresh(task: task as! BGAppRefreshTask)
            }
        }
    }

    static func scheduleNextRefresh() {
        let request = BGAppRefreshTaskRequest(identifier: refreshIdentifier)
        request.earliestBeginDate = Date(timeIntervalSinceNow: 15 * 60)
        try? BGTaskScheduler.shared.submit(request)
    }

    @MainActor
    private static func handleRefresh(task: BGAppRefreshTask) async {
        scheduleNextRefresh()

        // The app store is accessed through the scene delegate / environment
        // in a real app; here we just mark the task complete quickly.
        log.info("Background refresh fired")
        task.setTaskCompleted(success: true)
    }
}
