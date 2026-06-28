import os.log

extension Logger {
    static let network    = Logger(subsystem: "com.irc", category: "Network")
    static let session    = Logger(subsystem: "com.irc", category: "Session")
    static let ui         = Logger(subsystem: "com.irc", category: "UI")
    static let db         = Logger(subsystem: "com.irc", category: "Database")
    static let notif      = Logger(subsystem: "com.irc", category: "Notifications")
}
