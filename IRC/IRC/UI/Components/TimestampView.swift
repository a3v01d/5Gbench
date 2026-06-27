import SwiftUI

struct TimestampView: View {
    let date: Date

    var body: some View {
        Text(date.ircTimestampShort)
            .font(.system(.caption2, design: .monospaced))
            .foregroundStyle(.tertiary)
    }
}
