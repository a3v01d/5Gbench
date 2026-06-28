import Foundation

/// Callbacks from IRCConnection into its owning IRCSession.
protocol IRCConnectionDelegate: AnyObject, Sendable {
    func connection(_ connection: IRCConnection, didReceiveLine line: String) async
    func connection(_ connection: IRCConnection, didChangeState state: IRCConnectionState) async
}

enum IRCConnectionState: Sendable {
    case idle
    case connecting
    case connected
    case disconnected(Error?)
    case failed(Error)
}
