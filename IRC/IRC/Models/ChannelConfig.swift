import Foundation
import SwiftData

@Model
final class ChannelConfig {
    var name: String
    var key: String?
    var autoJoin: Bool
    var lastReadMessageID: String?
    var serverConfig: ServerConfig?

    init(name: String, key: String? = nil, autoJoin: Bool = true) {
        self.name = name
        self.key = key
        self.autoJoin = autoJoin
    }
}
