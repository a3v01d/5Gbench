import Foundation

/// Parses and stores server feature information from RPL_ISUPPORT (005) tokens.
final class ServerInfo: @unchecked Sendable {

    enum Casemapping: String {
        case ascii       = "ascii"
        case rfc1459     = "rfc1459"
        case strictRFC   = "strict-rfc1459"
    }

    private(set) var networkName: String = ""
    private(set) var casemapping: Casemapping = .rfc1459
    private(set) var maxNickLength: Int = 9
    private(set) var chanTypes: Set<Character> = ["#", "&"]
    private(set) var prefixes: [(mode: Character, symbol: Character)] = [("o", "@"), ("v", "+")]
    private(set) var chanModeTypes: IRCMode.ChannelModeTypes = IRCMode.ChannelModeTypes()

    func apply(tokens: [String]) {
        for token in tokens where !token.hasPrefix(":") {
            let parts = token.split(separator: "=", maxSplits: 1)
            let key = String(parts[0]).uppercased()
            let value = parts.count > 1 ? String(parts[1]) : ""

            switch key {
            case "NETWORK":
                networkName = value

            case "CASEMAPPING":
                casemapping = Casemapping(rawValue: value.lowercased()) ?? .rfc1459

            case "NICKLEN", "MAXNICKLEN":
                maxNickLength = Int(value) ?? 9

            case "CHANTYPES":
                chanTypes = Set(value)

            case "PREFIX":
                // Format: (ov)@+
                if let paren = value.firstIndex(of: "("),
                   let close = value.firstIndex(of: ")") {
                    let modes = Array(value[value.index(after: paren)..<close])
                    let symbols = Array(value[value.index(after: close)...])
                    prefixes = zip(modes, symbols).map { (mode: $0, symbol: $1) }
                    chanModeTypes.prefixModes = Set(modes)
                }

            case "CHANMODES":
                // Format: A,B,C,D
                let groups = value.split(separator: ",")
                if groups.count >= 1 { chanModeTypes.listModes    = Set(groups[0]) }
                if groups.count >= 2 { chanModeTypes.paramAlways  = Set(groups[1]) }
                if groups.count >= 3 { chanModeTypes.paramWhenSet = Set(groups[2]) }
                if groups.count >= 4 { chanModeTypes.noParam      = Set(groups[3]) }

            default:
                break
            }
        }
    }

    /// Normalize a nick or channel name using the server's casemapping.
    func normalize(_ s: String) -> String {
        switch casemapping {
        case .ascii:
            return s.lowercased()
        case .rfc1459, .strictRFC:
            // In RFC 1459 casemapping: []\~ are equivalent to {}|^
            var out = ""
            for c in s {
                switch c {
                case "A"..."Z": out.append(Character(c.lowercased()))
                case "[": out.append("{")
                case "]": out.append("}")
                case "\\": out.append("|")
                case "~": out.append("^")   // rfc1459 only (strict omits ~^)
                default:  out.append(c)
                }
            }
            return out
        }
    }

    /// Returns the mode character for a given prefix symbol (@, +, etc.)
    func mode(forSymbol symbol: Character) -> Character? {
        prefixes.first { $0.symbol == symbol }?.mode
    }

    /// Returns the symbol for a given mode character
    func symbol(forMode mode: Character) -> Character? {
        prefixes.first { $0.mode == mode }?.symbol
    }
}
