import Foundation
import SwiftData

enum AppSchema {
    static let schema = Schema([
        ServerConfig.self,
        ChannelConfig.self,
        Message.self,
        UserRecord.self
    ])

    static func makeContainer() throws -> ModelContainer {
        let config = ModelConfiguration(schema: schema, isStoredInMemoryOnly: false)
        return try ModelContainer(for: schema, configurations: [config])
    }

    static func makeInMemoryContainer() throws -> ModelContainer {
        let config = ModelConfiguration(schema: schema, isStoredInMemoryOnly: true)
        return try ModelContainer(for: schema, configurations: [config])
    }
}
