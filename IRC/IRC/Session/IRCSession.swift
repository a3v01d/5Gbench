import Foundation
import os.log

private let log = Logger(subsystem: "com.irc", category: "IRCSession")

/// Events emitted from IRCSession to be consumed by ServerStore on the MainActor.
enum SessionEvent: Sendable {
    case stateChanged(IRCSessionState)
    case messageReceived(ChatMessage)
    case nickChanged(from: String, to: String, inChannels: [String])
    case memberJoined(channel: String, nick: String)
    case memberLeft(channel: String, nick: String, reason: String?)
    case channelJoined(String)
    case channelParted(String)
    case topicChanged(channel: String, topic: String?)
    case membersUpdated(channel: String, members: [ChannelMember])
    case modesChanged(target: String, changes: [ModeChange])
}

/// Represents a single chat message ready for display.
struct ChatMessage: Identifiable, Sendable {
    var id: String
    var serverID: UUID
    var target: String          // channel name or nick (for DMs)
    var senderNick: String
    var senderHostmask: String?
    var content: String
    var timestamp: Date
    var kind: MessageKind
    var isMention: Bool

    enum MessageKind: Sendable {
        case privmsg, notice, action, join, part, quit, kick
        case mode, topic, nick, serverInfo, error
    }
}

/// Actor that orchestrates a single IRC server connection.
actor IRCSession: Identifiable {

    let id: UUID
    let config: ServerConfig

    private(set) var state: IRCSessionState = .idle
    private(set) var currentNick: String
    private var channels: [String: ChannelSession] = [:]   // normalized name → session
    private var users: [String: UserSession] = [:]         // normalized nick → session

    private let connection: IRCConnection
    private let capNegotiator = CapabilityNegotiator()
    private let registrationFlow = RegistrationFlow()
    private let serverInfo = ServerInfo()
    private var reconnectPolicy = ReconnectPolicy()

    // Pending whois builder
    private var pendingWhois: WhoisInfo?

    // Event stream for ServerStore
    private let eventContinuation: AsyncStream<SessionEvent>.Continuation
    let events: AsyncStream<SessionEvent>

    // Highlight keywords (set from preferences)
    var highlightKeywords: [String] = []

    init(config: ServerConfig) {
        self.id = config.id
        self.config = config
        self.currentNick = config.nickname
        self.connection = IRCConnection()

        var cont: AsyncStream<SessionEvent>.Continuation!
        self.events = AsyncStream { cont = $0 }
        self.eventContinuation = cont
    }

    // MARK: - Lifecycle

    func connect() async {
        guard state == .idle || state == .disconnected else { return }
        setState(.connecting)
        await connection.connect(
            host: config.host,
            port: UInt16(config.port),
            parameters: TLSConfiguration.parameters(useTLS: config.useTLS, acceptInvalidCert: config.acceptInvalidCert)
        )
        await connection.delegate = self

        // Start registration
        capNegotiator.onComplete = { [weak self] in
            guard let self else { return }
            await self.capNegotiationComplete()
        }

        let lines = await registrationFlow.start(config: config, capNegotiator: capNegotiator)
        for line in lines { try? await connection.send(line) }
    }

    func disconnect() async {
        reconnectPolicy.markUserDisconnect()
        try? await connection.send(IRCMessageEncoder.quit(message: "Goodbye"))
        await connection.disconnect()
        setState(.disconnected)
    }

    func join(channel: String, key: String? = nil) async {
        try? await connection.send(IRCMessageEncoder.join(channel: channel, key: key))
    }

    func part(channel: String, message: String? = nil) async {
        try? await connection.send(IRCMessageEncoder.part(channel: channel, message: message))
    }

    func send(privmsg text: String, to target: String) async {
        try? await connection.send(IRCMessageEncoder.privmsg(target: target, text: text))
        // Echo back our own message
        let echo = ChatMessage(
            id: UUID().uuidString,
            serverID: id,
            target: target,
            senderNick: currentNick,
            senderHostmask: nil,
            content: text,
            timestamp: .now,
            kind: .privmsg,
            isMention: false
        )
        emit(.messageReceived(echo))
    }

    func send(rawLine: String) async {
        try? await connection.send(rawLine)
    }

    func changeNick(to nick: String) async {
        try? await connection.send(IRCMessageEncoder.nick(nick))
    }

    func setAway(message: String?) async {
        try? await connection.send(IRCMessageEncoder.away(message: message))
    }

    // MARK: - Message dispatch

    func handle(line: String) async {
        guard let msg = try? IRCMessageParser.parse(line) else { return }
        await dispatch(msg)
    }

    private func dispatch(_ msg: IRCMessage) async {
        switch msg.command {

        // MARK: Core ping/pong
        case .PING:
            let token = msg.parameters.first ?? ""
            try? await connection.send(IRCMessageEncoder.pong(token: token))

        // MARK: Registration flow
        case .CAP:
            await handleCAP(msg)

        case .AUTHENTICATE:
            if let response = await registrationFlow.handleAuthenticateChallenge() {
                try? await connection.send(response)
            }

        case .numeric(IRCNumeric.RPL_SASLSUCCESS):
            let lines = await registrationFlow.saslSuccess(config: config)
            for l in lines { try? await connection.send(l) }

        case .numeric(IRCNumeric.RPL_WELCOME):
            await handleWelcome(msg)

        case .numeric(IRCNumeric.RPL_ISUPPORT):
            serverInfo.apply(tokens: msg.parameters)

        case .numeric(IRCNumeric.ERR_NICKNAMEINUSE):
            await handleNickInUse()

        // MARK: Channel membership
        case .JOIN:
            await handleJoin(msg)

        case .PART:
            await handlePart(msg)

        case .QUIT:
            await handleQuit(msg)

        case .KICK:
            await handleKick(msg)

        case .NICK:
            await handleNick(msg)

        case .MODE:
            await handleMode(msg)

        case .TOPIC:
            await handleTopic(msg)

        case .numeric(IRCNumeric.RPL_TOPIC):
            if msg.parameters.count >= 3 {
                let chan = msg.parameters[1]
                let text = msg.parameters[2]
                let key = serverInfo.normalize(chan)
                await channels[key]?.setTopic(text)
                emit(.topicChanged(channel: chan, topic: text))
            }

        case .numeric(IRCNumeric.RPL_NAMREPLY):
            await handleNamesReply(msg)

        case .numeric(IRCNumeric.RPL_ENDOFNAMES):
            if let chan = msg.parameters.dropFirst().first {
                let key = serverInfo.normalize(String(chan))
                let members = await channels[key]?.members ?? []
                emit(.membersUpdated(channel: String(chan), members: members))
            }

        // MARK: Messaging
        case .PRIVMSG:
            await handlePrivmsg(msg)

        case .NOTICE:
            await handleNotice(msg)

        // MARK: WHOIS
        case .numeric(IRCNumeric.RPL_WHOISUSER):
            handleWhoisUser(msg)
        case .numeric(IRCNumeric.RPL_WHOISSERVER):
            handleWhoisServer(msg)
        case .numeric(IRCNumeric.RPL_WHOISCHANNELS):
            handleWhoisChannels(msg)
        case .numeric(IRCNumeric.RPL_WHOISIDLE):
            handleWhoisIdle(msg)
        case .numeric(IRCNumeric.RPL_WHOISACCOUNT):
            handleWhoisAccount(msg)
        case .numeric(IRCNumeric.RPL_ENDOFWHOIS):
            await handleEndOfWhois(msg)

        // MARK: Errors
        case .ERROR:
            let text = msg.parameters.first ?? "Unknown error"
            setState(.error(text))
            emit(.messageReceived(serverMessage(text, kind: .error)))
            await scheduleReconnect()

        // MARK: Numeric info messages (MOTD etc.)
        case .numeric(let n) where n >= 370 && n <= 376:
            let text = msg.parameters.last ?? ""
            emit(.messageReceived(serverMessage(text, kind: .serverInfo)))

        default:
            break
        }
    }

    // MARK: - Handlers

    private func handleCAP(_ msg: IRCMessage) async {
        // CAP * <subcommand> :<params>
        guard msg.parameters.count >= 2 else { return }
        let sub = msg.parameters[1]
        let rest = Array(msg.parameters.dropFirst(2))
        let lines = await capNegotiator.handle(subcommand: sub, params: rest)
        for l in lines { try? await connection.send(l) }
    }

    private func handleWelcome(_ msg: IRCMessage) async {
        currentNick = msg.parameters.first ?? currentNick
        reconnectPolicy.reset()
        await registrationFlow.welcome()
        setState(.ready)

        // Auto-join configured channels
        for chanConfig in config.channels where chanConfig.autoJoin {
            await join(channel: chanConfig.name, key: chanConfig.key)
        }

        // NickServ auth fallback if no SASL
        if !config.useSASL,
           let nsPass = Keychain.load(key: config.keychainNickServPasswordKey),
           !nsPass.isEmpty {
            try? await connection.send(
                IRCMessageEncoder.privmsg(target: "NickServ", text: "IDENTIFY \(nsPass)")
            )
        }
    }

    private func handleNickInUse() async {
        // Try alternate nick
        if currentNick == config.nickname {
            currentNick = config.alternateNick
            try? await connection.send(IRCMessageEncoder.nick(currentNick))
        } else {
            currentNick = currentNick + "_"
            try? await connection.send(IRCMessageEncoder.nick(currentNick))
        }
    }

    private func handleJoin(_ msg: IRCMessage) async {
        guard let chan = msg.parameters.first,
              let nick = msg.prefix?.nick else { return }
        let key = serverInfo.normalize(chan)
        if serverInfo.normalize(nick) == serverInfo.normalize(currentNick) {
            // We joined
            let session = ChannelSession(name: chan, serverInfo: serverInfo)
            channels[key] = session
            await session.markJoined()
            emit(.channelJoined(chan))
        } else {
            await channels[key]?.applyJoin(nick: nick)
            emit(.memberJoined(channel: chan, nick: nick))
        }
        let chatMsg = ChatMessage(
            id: UUID().uuidString, serverID: id, target: chan,
            senderNick: nick, senderHostmask: hostmask(msg),
            content: "\(nick) has joined \(chan)",
            timestamp: msgTimestamp(msg), kind: .join, isMention: false
        )
        emit(.messageReceived(chatMsg))
    }

    private func handlePart(_ msg: IRCMessage) async {
        guard let chan = msg.parameters.first,
              let nick = msg.prefix?.nick else { return }
        let key = serverInfo.normalize(chan)
        let reason = msg.parameters.count > 1 ? msg.parameters[1] : nil
        if serverInfo.normalize(nick) == serverInfo.normalize(currentNick) {
            await channels[key]?.markParted()
            emit(.channelParted(chan))
        } else {
            await channels[key]?.applyPart(nick: nick)
            emit(.memberLeft(channel: chan, nick: nick, reason: reason))
        }
        let text = reason.map { "\(nick) has left \(chan) (\($0))" } ?? "\(nick) has left \(chan)"
        emit(.messageReceived(ChatMessage(
            id: UUID().uuidString, serverID: id, target: chan,
            senderNick: nick, senderHostmask: nil, content: text,
            timestamp: msgTimestamp(msg), kind: .part, isMention: false
        )))
    }

    private func handleQuit(_ msg: IRCMessage) async {
        guard let nick = msg.prefix?.nick else { return }
        let reason = msg.parameters.first
        let normalizedNick = serverInfo.normalize(nick)
        var affectedChannels: [String] = []
        for (_, session) in channels {
            let members = await session.members
            if members.contains(where: { serverInfo.normalize($0.nick) == normalizedNick }) {
                await session.applyQuit(nick: nick)
                affectedChannels.append(session.name)
            }
        }
        let text = reason.map { "\(nick) has quit (\($0))" } ?? "\(nick) has quit"
        for chan in affectedChannels {
            emit(.messageReceived(ChatMessage(
                id: UUID().uuidString, serverID: id, target: chan,
                senderNick: nick, senderHostmask: nil, content: text,
                timestamp: msgTimestamp(msg), kind: .quit, isMention: false
            )))
        }
    }

    private func handleKick(_ msg: IRCMessage) async {
        guard msg.parameters.count >= 2,
              let by = msg.prefix?.nick else { return }
        let chan = msg.parameters[0]
        let kicked = msg.parameters[1]
        let reason = msg.parameters.count > 2 ? msg.parameters[2] : nil
        let key = serverInfo.normalize(chan)
        await channels[key]?.applyKick(nick: kicked)
        emit(.memberLeft(channel: chan, nick: kicked, reason: reason))
        let text = reason.map { "\(by) kicked \(kicked) from \(chan) (\($0))" }
                        ?? "\(by) kicked \(kicked) from \(chan)"
        emit(.messageReceived(ChatMessage(
            id: UUID().uuidString, serverID: id, target: chan,
            senderNick: by, senderHostmask: nil, content: text,
            timestamp: msgTimestamp(msg), kind: .kick, isMention: false
        )))
    }

    private func handleNick(_ msg: IRCMessage) async {
        guard let oldNick = msg.prefix?.nick,
              let newNick = msg.parameters.first else { return }
        var affected: [String] = []
        for (_, session) in channels {
            let members = await session.members
            if members.contains(where: { serverInfo.normalize($0.nick) == serverInfo.normalize(oldNick) }) {
                await session.applyNickChange(from: oldNick, to: newNick)
                affected.append(session.name)
            }
        }
        if serverInfo.normalize(oldNick) == serverInfo.normalize(currentNick) {
            currentNick = newNick
        }
        emit(.nickChanged(from: oldNick, to: newNick, inChannels: affected))
    }

    private func handleMode(_ msg: IRCMessage) async {
        guard let target = msg.parameters.first else { return }
        let modeParams = Array(msg.parameters.dropFirst())
        let changes = IRCMode.parse(params: modeParams, types: serverInfo.chanModeTypes)
        let key = serverInfo.normalize(target)
        await channels[key]?.applyModeChanges(changes)
        emit(.modesChanged(target: target, changes: changes))
    }

    private func handleTopic(_ msg: IRCMessage) async {
        guard let chan = msg.parameters.first else { return }
        let text = msg.parameters.count > 1 ? msg.parameters[1] : nil
        let key = serverInfo.normalize(chan)
        await channels[key]?.setTopic(text)
        emit(.topicChanged(channel: chan, topic: text))
    }

    private func handleNamesReply(_ msg: IRCMessage) async {
        guard msg.parameters.count >= 4 else { return }
        let chan = msg.parameters[2]
        let names = msg.parameters[3].split(separator: " ").map(String.init)
        let key = serverInfo.normalize(chan)
        await channels[key]?.applyNamesReply(names)
    }

    private func handlePrivmsg(_ msg: IRCMessage) async {
        guard let target = msg.parameters.first,
              let text = msg.parameters.last,
              let senderNick = msg.prefix?.nick else { return }

        if let ctcp = CTCP.decode(text) {
            await handleCTCP(ctcp, from: senderNick, replyTarget: target, msg: msg)
            return
        }

        let effectiveTarget = target.hasPrefix("#") || target.hasPrefix("&") ? target : senderNick
        let mention = isMention(text)
        emit(.messageReceived(ChatMessage(
            id: msg.messageID ?? UUID().uuidString,
            serverID: id, target: effectiveTarget,
            senderNick: senderNick, senderHostmask: hostmask(msg),
            content: text, timestamp: msgTimestamp(msg),
            kind: .privmsg, isMention: mention
        )))
    }

    private func handleNotice(_ msg: IRCMessage) async {
        guard let target = msg.parameters.first,
              let text = msg.parameters.last,
              let senderNick = msg.prefix?.nick ?? msg.prefix?.host else { return }
        let effectiveTarget = target.hasPrefix("#") ? target : currentNick
        emit(.messageReceived(ChatMessage(
            id: msg.messageID ?? UUID().uuidString,
            serverID: id, target: effectiveTarget,
            senderNick: senderNick, senderHostmask: hostmask(msg),
            content: text, timestamp: msgTimestamp(msg),
            kind: .notice, isMention: false
        )))
    }

    private func handleCTCP(_ ctcp: CTCPMessage, from nick: String, replyTarget: String, msg: IRCMessage) async {
        switch ctcp.verb {
        case CTCPMessage.ACTION:
            let text = ctcp.body ?? ""
            let effectiveTarget = replyTarget.hasPrefix("#") ? replyTarget : nick
            emit(.messageReceived(ChatMessage(
                id: msg.messageID ?? UUID().uuidString,
                serverID: id, target: effectiveTarget,
                senderNick: nick, senderHostmask: hostmask(msg),
                content: text, timestamp: msgTimestamp(msg),
                kind: .action, isMention: isMention(text)
            )))

        case CTCPMessage.VERSION:
            let reply = CTCP.versionReply("IRC iOS/1.0")
            try? await connection.send(IRCMessageEncoder.notice(target: nick, text: reply))

        case CTCPMessage.PING:
            if let token = ctcp.body {
                let reply = CTCP.pingReply(token)
                try? await connection.send(IRCMessageEncoder.notice(target: nick, text: reply))
            }

        default:
            break
        }
    }

    // MARK: - WHOIS assembly

    private func handleWhoisUser(_ msg: IRCMessage) {
        guard msg.parameters.count >= 5 else { return }
        pendingWhois = WhoisInfo(
            nick: msg.parameters[1],
            username: msg.parameters[2],
            host: msg.parameters[3],
            realname: msg.parameters[4]
        )
    }

    private func handleWhoisServer(_ msg: IRCMessage) {
        if msg.parameters.count >= 3 { pendingWhois?.server = msg.parameters[2] }
    }

    private func handleWhoisChannels(_ msg: IRCMessage) {
        if let chans = msg.parameters.last {
            pendingWhois?.channels = chans.split(separator: " ").map(String.init)
        }
    }

    private func handleWhoisIdle(_ msg: IRCMessage) {
        if let s = msg.parameters.dropFirst().first { pendingWhois?.idleSeconds = Int(s) }
    }

    private func handleWhoisAccount(_ msg: IRCMessage) {
        if let acc = msg.parameters.dropFirst().first { pendingWhois?.account = String(acc) }
    }

    private func handleEndOfWhois(_ msg: IRCMessage) async {
        if let info = pendingWhois {
            let key = serverInfo.normalize(info.nick)
            let session = users[key] ?? {
                let s = UserSession(nick: info.nick); users[key] = s; return s
            }()
            await session.updateWhois(info)
        }
        pendingWhois = nil
    }

    // MARK: - Cap complete

    private func capNegotiationComplete() async {
        let needsSASL = await capNegotiator.needsSASL
        let lines = await registrationFlow.capComplete(needsSASL: needsSASL, config: config)
        for l in lines { try? await connection.send(l) }
    }

    // MARK: - Reconnect

    private func scheduleReconnect() async {
        guard reconnectPolicy.shouldReconnect else { return }
        let delay = reconnectPolicy.nextDelay()
        let attempt = reconnectPolicy.attemptCount
        setState(.reconnecting(attempt: attempt, delay: delay))
        try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
        await connect()
    }

    // MARK: - Helpers

    private func setState(_ s: IRCSessionState) {
        state = s
        emit(.stateChanged(s))
    }

    private func emit(_ event: SessionEvent) {
        eventContinuation.yield(event)
    }

    private func hostmask(_ msg: IRCMessage) -> String? {
        guard let p = msg.prefix, let nick = p.nick, let host = p.host else { return nil }
        if let user = p.user { return "\(nick)!\(user)@\(host)" }
        return "\(nick)@\(host)"
    }

    private func msgTimestamp(_ msg: IRCMessage) -> Date {
        msg.serverTime ?? .now
    }

    private func isMention(_ text: String) -> Bool {
        let lowerText = text.lowercased()
        let lowerNick = currentNick.lowercased()
        if wordBoundaryContains(lowerText, word: lowerNick) { return true }
        return highlightKeywords.contains { wordBoundaryContains(lowerText, word: $0.lowercased()) }
    }

    private func wordBoundaryContains(_ text: String, word: String) -> Bool {
        guard !word.isEmpty else { return false }
        var searchRange = text.startIndex..<text.endIndex
        while let range = text.range(of: word, range: searchRange) {
            let before = range.lowerBound == text.startIndex || !text[text.index(before: range.lowerBound)].isLetter
            let after  = range.upperBound == text.endIndex   || !text[range.upperBound].isLetter
            if before && after { return true }
            searchRange = range.upperBound..<text.endIndex
        }
        return false
    }

    private func serverMessage(_ text: String, kind: ChatMessage.MessageKind) -> ChatMessage {
        ChatMessage(
            id: UUID().uuidString, serverID: id, target: "*",
            senderNick: serverInfo.networkName.isEmpty ? config.host : serverInfo.networkName,
            senderHostmask: nil, content: text, timestamp: .now,
            kind: kind, isMention: false
        )
    }
}

// MARK: - IRCConnectionDelegate conformance

extension IRCSession: IRCConnectionDelegate {
    func connection(_ connection: IRCConnection, didReceiveLine line: String) async {
        await handle(line: line)
    }

    func connection(_ connection: IRCConnection, didChangeState state: IRCConnectionState) async {
        switch state {
        case .connected:
            self.state = .registering
        case .disconnected, .failed:
            if self.state != .disconnected {
                setState(.disconnected)
                await scheduleReconnect()
            }
        default:
            break
        }
    }
}

// MARK: - Expose serverInfo properties for ChannelSession

extension IRCSession {
    var chanModeTypes: IRCMode.ChannelModeTypes { serverInfo.chanModeTypes }
}
