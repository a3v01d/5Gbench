import Foundation

struct ChannelMember: Identifiable, Sendable, Equatable {
    var nick: String
    var prefixes: [Character]   // e.g. ['@', '+'] — highest privilege first

    var id: String { nick }
    var highestPrefix: Character? { prefixes.first }

    var sortOrder: Int {
        guard let p = highestPrefix else { return 3 }
        switch p {
        case "@": return 0
        case "%": return 1
        case "+": return 2
        default:  return 3
        }
    }
}

/// Per-channel membership and state, isolated as an actor.
actor ChannelSession {

    let name: String
    private(set) var topic: String?
    private(set) var modes: Set<Character> = []
    private(set) var modeParams: [Character: String] = [:]
    private(set) var members: [ChannelMember] = []
    private(set) var isJoined: Bool = false

    private let serverInfo: ServerInfo

    init(name: String, serverInfo: ServerInfo) {
        self.name = name
        self.serverInfo = serverInfo
    }

    // MARK: - Membership mutations

    func markJoined() { isJoined = true }
    func markParted() { isJoined = false; members = [] }

    func applyJoin(nick: String) {
        guard !members.contains(where: { serverInfo.normalize($0.nick) == serverInfo.normalize(nick) }) else { return }
        members.append(ChannelMember(nick: nick, prefixes: []))
        sortMembers()
    }

    func applyPart(nick: String) {
        let key = serverInfo.normalize(nick)
        members.removeAll { serverInfo.normalize($0.nick) == key }
    }

    func applyQuit(nick: String) { applyPart(nick: nick) }

    func applyNickChange(from oldNick: String, to newNick: String) {
        let key = serverInfo.normalize(oldNick)
        if let idx = members.firstIndex(where: { serverInfo.normalize($0.nick) == key }) {
            members[idx].nick = newNick
        }
    }

    func applyKick(nick: String) { applyPart(nick: nick) }

    func applyNamesReply(_ names: [String]) {
        for name in names {
            var prefixChars: [Character] = []
            var remaining = name
            while let first = remaining.first {
                if serverInfo.symbol(forMode: first) != nil || "@%+".contains(first) {
                    prefixChars.append(first)
                    remaining = String(remaining.dropFirst())
                } else {
                    break
                }
            }
            // Remove any already-known member
            let key = serverInfo.normalize(remaining)
            members.removeAll { serverInfo.normalize($0.nick) == key }
            members.append(ChannelMember(nick: remaining, prefixes: prefixChars))
        }
        sortMembers()
    }

    func applyModeChanges(_ changes: [ModeChange]) {
        for change in changes {
            if serverInfo.prefixes.map(\.mode).contains(change.char) {
                // Prefix mode change — update member
                guard let target = change.parameter else { continue }
                let key = serverInfo.normalize(target)
                if let idx = members.firstIndex(where: { serverInfo.normalize($0.nick) == key }),
                   let sym = serverInfo.symbol(forMode: change.char) {
                    if change.direction == .add {
                        if !members[idx].prefixes.contains(sym) {
                            members[idx].prefixes.insert(sym, at: 0)
                        }
                    } else {
                        members[idx].prefixes.removeAll { $0 == sym }
                    }
                }
            } else {
                // Channel mode
                if change.direction == .add {
                    modes.insert(change.char)
                    if let param = change.parameter { modeParams[change.char] = param }
                } else {
                    modes.remove(change.char)
                    modeParams.removeValue(forKey: change.char)
                }
            }
        }
        sortMembers()
    }

    func setTopic(_ text: String?) { topic = text }

    // MARK: - Private

    private func sortMembers() {
        members.sort { $0.sortOrder < $1.sortOrder || ($0.sortOrder == $1.sortOrder && $0.nick < $1.nick) }
    }
}
