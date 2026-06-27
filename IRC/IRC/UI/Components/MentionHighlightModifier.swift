import SwiftUI

struct MentionHighlightModifier: ViewModifier {
    let isMention: Bool

    func body(content: Content) -> some View {
        content
            .background(
                isMention
                    ? Color.yellow.opacity(0.15).cornerRadius(4)
                    : Color.clear
            )
    }
}

extension View {
    func mentionHighlight(_ isMention: Bool) -> some View {
        modifier(MentionHighlightModifier(isMention: isMention))
    }
}
