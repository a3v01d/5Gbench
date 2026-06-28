import Foundation

enum IRCSessionState: Sendable, Equatable {
    case idle
    case connecting
    case registering          // TCP connected, performing NICK/USER/CAP/SASL
    case ready                // RPL_WELCOME received
    case away                 // ready but AWAY set
    case reconnecting(attempt: Int, delay: TimeInterval)
    case disconnected
    case error(String)
}
