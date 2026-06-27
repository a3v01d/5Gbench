import Foundation

/// Stateless helper that checks whether a message constitutes a mention.
struct MentionDetector {

    let currentNick: String
    let keywords: [String]

    func isMention(in text: String) -> Bool {
        let lower = text.lowercased()
        if wordBoundary(lower, contains: currentNick.lowercased()) { return true }
        return keywords.contains { wordBoundary(lower, contains: $0.lowercased()) }
    }

    private func wordBoundary(_ text: String, contains word: String) -> Bool {
        guard !word.isEmpty else { return false }
        var search = text.startIndex..<text.endIndex
        while let range = text.range(of: word, range: search) {
            let before = range.lowerBound == text.startIndex
                      || !text[text.index(before: range.lowerBound)].isLetter
            let after  = range.upperBound == text.endIndex
                      || !text[range.upperBound].isLetter
            if before && after { return true }
            search = range.upperBound..<text.endIndex
        }
        return false
    }
}
