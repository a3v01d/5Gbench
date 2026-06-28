import Foundation

/// Represents a single mode change applied in a MODE message.
struct ModeChange: Sendable {
    enum Direction: Sendable { case add, remove }
    let direction: Direction
    let char: Character
    let parameter: String?
}

/// Parses IRC MODE strings using the type system from ISUPPORT CHANMODES + PREFIX.
struct IRCMode {

    /// Default channel mode types when ISUPPORT is not yet available (RFC 1459 baseline).
    struct ChannelModeTypes {
        var listModes: Set<Character>       = ["b", "e", "I"]    // type A
        var paramAlways: Set<Character>     = ["k"]              // type B
        var paramWhenSet: Set<Character>    = ["l"]              // type C
        var noParam: Set<Character>         = ["m","n","t","i","s","p","r"] // type D
        var prefixModes: Set<Character>     = ["o", "v", "h"]   // from PREFIX=
    }

    /// Parse a MODE params list: modestring followed by parameters.
    /// e.g. ["+ov", "nick1", "nick2"] → [.add 'o' "nick1", .add 'v' "nick2"]
    static func parse(params: [String], types: ChannelModeTypes = ChannelModeTypes()) -> [ModeChange] {
        guard let modeString = params.first else { return [] }
        var args = Array(params.dropFirst())
        var argIndex = 0
        var changes: [ModeChange] = []
        var direction: ModeChange.Direction = .add

        for char in modeString {
            switch char {
            case "+": direction = .add
            case "-": direction = .remove
            default:
                let param: String?
                if types.prefixModes.contains(char) {
                    // always has a parameter
                    param = argIndex < args.count ? args[argIndex] : nil
                    if param != nil { argIndex += 1 }
                } else if types.listModes.contains(char) {
                    // type A: always parameter
                    param = argIndex < args.count ? args[argIndex] : nil
                    if param != nil { argIndex += 1 }
                } else if types.paramAlways.contains(char) {
                    // type B: always parameter
                    param = argIndex < args.count ? args[argIndex] : nil
                    if param != nil { argIndex += 1 }
                } else if types.paramWhenSet.contains(char) {
                    // type C: parameter only when setting (+)
                    if direction == .add {
                        param = argIndex < args.count ? args[argIndex] : nil
                        if param != nil { argIndex += 1 }
                    } else {
                        param = nil
                    }
                } else {
                    // type D: no parameter
                    param = nil
                }
                changes.append(ModeChange(direction: direction, char: char, parameter: param))
            }
        }
        return changes
    }

    /// Parse user modes (simpler — no parameter types needed for common modes).
    static func parseUserModes(_ modeString: String) -> [ModeChange] {
        var changes: [ModeChange] = []
        var direction: ModeChange.Direction = .add
        for char in modeString {
            switch char {
            case "+": direction = .add
            case "-": direction = .remove
            default:  changes.append(ModeChange(direction: direction, char: char, parameter: nil))
            }
        }
        return changes
    }
}
