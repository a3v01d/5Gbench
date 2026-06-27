import SwiftUI

struct MessageListView: View {
    let messages: [ChatMessage]

    @State private var isAtBottom = true
    @State private var showJumpButton = false

    var body: some View {
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 0) {
                    ForEach(messages) { msg in
                        MessageRowView(message: msg)
                            .equatable()
                            .id(msg.id)
                    }
                    // Sentinel for "scroll to bottom"
                    Color.clear
                        .frame(height: 1)
                        .id("__bottom__")
                        .background(
                            GeometryReader { geo in
                                Color.clear.preference(
                                    key: BottomVisibilityKey.self,
                                    value: geo.frame(in: .global).maxY
                                )
                            }
                        )
                }
            }
            .onPreferenceChange(BottomVisibilityKey.self) { maxY in
                // If bottom sentinel is in view, we're at the bottom
                isAtBottom = maxY > 0
                showJumpButton = !isAtBottom
            }
            .onChange(of: messages.count) { _, _ in
                if isAtBottom {
                    withAnimation(.none) {
                        proxy.scrollTo("__bottom__", anchor: .bottom)
                    }
                }
            }
            .onAppear {
                proxy.scrollTo("__bottom__", anchor: .bottom)
            }
            .overlay(alignment: .bottomTrailing) {
                if showJumpButton {
                    Button {
                        isAtBottom = true
                        withAnimation {
                            proxy.scrollTo("__bottom__", anchor: .bottom)
                        }
                    } label: {
                        Image(systemName: "arrow.down.circle.fill")
                            .font(.title2)
                            .padding(12)
                    }
                    .tint(.accentColor)
                    .transition(.scale.combined(with: .opacity))
                }
            }
        }
    }
}

private struct BottomVisibilityKey: PreferenceKey {
    static var defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
        value = nextValue()
    }
}
