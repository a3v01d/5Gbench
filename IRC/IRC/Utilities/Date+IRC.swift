import Foundation

extension Date {
    /// Parses an IRCv3 server-time tag value (ISO 8601 / RFC 3339).
    /// Example: "2024-01-15T20:30:00.123Z"
    static func fromIRCServerTime(_ string: String) -> Date? {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let d = formatter.date(from: string) { return d }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: string)
    }

    var ircTimestampShort: String {
        let f = DateFormatter()
        f.dateFormat = Calendar.current.isDateInToday(self) ? "HH:mm" : "MM/dd HH:mm"
        return f.string(from: self)
    }
}
