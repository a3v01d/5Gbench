import SwiftUI

struct NickPillView: View {
    let nick: String
    let prefix: Character?   // '@', '+', nil

    private var color: Color {
        switch prefix {
        case "@": return .orange
        case "+": return .green
        default:  return .secondary
        }
    }

    var body: some View {
        Text((prefix.map(String.init) ?? "") + nick)
            .font(.system(.footnote, design: .monospaced).bold())
            .foregroundStyle(color)
    }
}
