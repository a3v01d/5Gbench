import Foundation
import SwiftData

@Model
final class UserRecord {
    var nick: String
    var hostmask: String?
    var lastSeen: Date
    var notes: String

    init(nick: String, hostmask: String? = nil) {
        self.nick = nick
        self.hostmask = hostmask
        self.lastSeen = .now
        self.notes = ""
    }
}
