import Foundation

/// Sequences the IRC registration handshake: CAP → SASL (if available) → NICK → USER → CAP END.
actor RegistrationFlow {

    enum Step {
        case initial
        case capNegotiation
        case saslChallenge
        case saslComplete
        case sentNickUser
        case complete
    }

    private(set) var step: Step = .initial
    private var saslChunks: [String] = []
    private var saslChunkIndex: Int = 0

    /// Generate the initial burst of lines to send on connect.
    func start(config: ServerConfig, capNegotiator: CapabilityNegotiator) async -> [String] {
        var lines: [String] = []

        if let pass = Keychain.load(key: config.keychainServerPasswordKey), !pass.isEmpty {
            lines.append(IRCMessageEncoder.pass(pass))
        }

        lines.append(await capNegotiator.begin())
        step = .capNegotiation
        return lines
    }

    /// Called after CAP ACK is complete. Returns lines to send.
    func capComplete(needsSASL: Bool, config: ServerConfig) async -> [String] {
        if needsSASL,
           let user = config.saslUsername,
           let pass = Keychain.load(key: config.keychainSASLPasswordKey),
           !user.isEmpty, !pass.isEmpty {
            step = .saslChallenge
            saslChunks = SASLEngine.plainChunks(authcid: user, password: pass)
            saslChunkIndex = 0
            return [IRCMessageEncoder.authenticate(payload: "PLAIN")]
        } else {
            // No SASL — send NICK/USER now and end CAP
            step = .sentNickUser
            return nickUserLines(config: config) + [IRCMessageEncoder.cap(subcommand: "END")]
        }
    }

    /// Called on AUTHENTICATE + (empty challenge from server). Returns next authenticate line.
    func handleAuthenticateChallenge() -> String? {
        guard step == .saslChallenge, saslChunkIndex < saslChunks.count else { return nil }
        let chunk = saslChunks[saslChunkIndex]
        saslChunkIndex += 1
        return IRCMessageEncoder.authenticate(payload: chunk)
    }

    /// Called on RPL_SASLSUCCESS (903). Returns lines to complete registration.
    func saslSuccess(config: ServerConfig) -> [String] {
        step = .saslComplete
        return nickUserLines(config: config) + [IRCMessageEncoder.cap(subcommand: "END")]
    }

    /// Called on RPL_WELCOME (001). Registration is done.
    func welcome() {
        step = .complete
    }

    // MARK: - Private

    private func nickUserLines(config: ServerConfig) -> [String] {
        [
            IRCMessageEncoder.nick(config.nickname),
            IRCMessageEncoder.user(username: config.username, realname: config.realname)
        ]
    }
}
