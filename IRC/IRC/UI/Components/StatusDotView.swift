import SwiftUI

struct StatusDotView: View {
    let state: IRCSessionState

    private var color: Color {
        switch state {
        case .ready, .away:               return .green
        case .connecting, .registering,
             .reconnecting:               return .yellow
        case .idle, .disconnected, .error: return .red
        }
    }

    var body: some View {
        Circle()
            .fill(color)
            .frame(width: 8, height: 8)
            .accessibilityLabel(accessibilityText)
    }

    private var accessibilityText: String {
        switch state {
        case .ready:           return "Connected"
        case .away:            return "Away"
        case .connecting,
             .registering:     return "Connecting"
        case .reconnecting:    return "Reconnecting"
        default:               return "Disconnected"
        }
    }
}
