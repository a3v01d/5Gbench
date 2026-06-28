import Foundation

/// Exponential back-off reconnect policy. Thread-safe via actor isolation at call-site.
struct ReconnectPolicy: Sendable {

    private static let delays: [TimeInterval] = [2, 4, 8, 16, 30]
    private static let maxDelay: TimeInterval = 30

    private(set) var attemptCount: Int = 0
    private(set) var userInitiatedDisconnect: Bool = false

    /// True if we should attempt a reconnect at all.
    var shouldReconnect: Bool { !userInitiatedDisconnect }

    /// Returns the delay before the next attempt and increments the counter.
    mutating func nextDelay() -> TimeInterval {
        let idx = min(attemptCount, ReconnectPolicy.delays.count - 1)
        let delay = ReconnectPolicy.delays[idx]
        attemptCount += 1
        return delay
    }

    /// Call on successful RPL_WELCOME — resets the back-off.
    mutating func reset() {
        attemptCount = 0
        userInitiatedDisconnect = false
    }

    /// Call when the user explicitly disconnects — suppresses automatic reconnect.
    mutating func markUserDisconnect() {
        userInitiatedDisconnect = true
    }
}
