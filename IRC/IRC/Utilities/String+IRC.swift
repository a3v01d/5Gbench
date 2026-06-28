import Foundation

extension String {
    /// Returns true if this string looks like a channel name (starts with # or &).
    var isChannelName: Bool {
        hasPrefix("#") || hasPrefix("&") || hasPrefix("!") || hasPrefix("+")
    }

    /// Truncates to maxBytes UTF-8 bytes, preserving whole Unicode scalars.
    func truncatedToIRCBytes(_ maxBytes: Int = 510) -> String {
        guard utf8.count > maxBytes else { return self }
        var bytes = 0
        var truncated = ""
        for char in self {
            let charBytes = String(char).utf8.count
            guard bytes + charBytes <= maxBytes else { break }
            truncated.append(char)
            bytes += charBytes
        }
        return truncated
    }
}
