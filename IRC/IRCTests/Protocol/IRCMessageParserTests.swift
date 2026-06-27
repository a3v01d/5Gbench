import Testing
@testable import IRC

struct IRCMessageParserTests {

    // MARK: - Basic parsing

    @Test func parsesSimplePing() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.ping)
        #expect(msg.command == .PING)
        #expect(msg.parameters == ["irc.libera.chat"])
        #expect(msg.prefix == nil)
    }

    @Test func parsesWelcomeNumeric() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.welcome)
        #expect(msg.command == .numeric(1))
        #expect(msg.parameters.first == "testnick")
        #expect(msg.prefix?.host == "irc.libera.chat")
        #expect(msg.prefix?.isServer == true)
    }

    @Test func parsesJoin() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.joinChannel)
        #expect(msg.command == .JOIN)
        #expect(msg.prefix?.nick == "testnick")
        #expect(msg.prefix?.user == "~user")
        #expect(msg.prefix?.host == "example.com")
        #expect(msg.parameters.first == "#test")
    }

    @Test func parsesPrivmsg() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.privmsgChannel)
        #expect(msg.command == .PRIVMSG)
        #expect(msg.prefix?.nick == "othernick")
        #expect(msg.parameters[0] == "#test")
        #expect(msg.parameters[1] == "Hello, world!")
    }

    @Test func parsesNotice() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.noticeChannel)
        #expect(msg.command == .NOTICE)
        #expect(msg.prefix?.host == "server.example")
        #expect(msg.parameters[1] == "Scheduled maintenance at midnight")
    }

    @Test func parsesNickChange() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.nickChange)
        #expect(msg.command == .NICK)
        #expect(msg.prefix?.nick == "oldnick")
        #expect(msg.parameters.first == "newnick")
    }

    @Test func parsesTopicNumeric() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.topicReply)
        #expect(msg.command == .numeric(332))
        #expect(msg.parameters[1] == "#test")
        #expect(msg.parameters[2] == "Channel topic goes here")
    }

    @Test func parsesModeOpVoice() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.modeOpVoice)
        #expect(msg.command == .MODE)
        #expect(msg.parameters[0] == "#test")
        #expect(msg.parameters[1] == "+ov")
        #expect(msg.parameters[2] == "nick1")
        #expect(msg.parameters[3] == "nick2")
    }

    @Test func parsesNamesReply() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.namesReply)
        #expect(msg.command == .numeric(353))
        #expect(msg.parameters[2] == "#test")
        #expect(msg.parameters[3] == "@op +voiced plain")
    }

    // MARK: - IRCv3 tags

    @Test func parsesTaggedPrivmsg() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.taggedPrivmsg)
        #expect(msg.command == .PRIVMSG)
        // tags
        let time = msg.tags["time"] as? String
        #expect(time == "2024-01-15T20:30:00.123Z")
        let msgid = msg.tags["msgid"] as? String
        #expect(msgid == "abc123")
        #expect(msg.parameters[0] == "#chan")
        #expect(msg.parameters[1] == "tagged message")
    }

    @Test func parsesFlagTag() throws {
        let line = "@flag :server NOTICE * :test"
        let msg = try IRCMessageParser.parse(line)
        #expect(msg.tags.keys.contains("flag"))
        // flag tag has nil value
        let val = msg.tags["flag"]
        #expect(val == Optional(Optional<String>.none))
    }

    @Test func parsesEscapedTagValues() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.escapedTags)
        let key = msg.tags["key"] as? String
        #expect(key == "value has spaces")
    }

    @Test func parsesTaggedJoinWithExtendedJoin() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.taggedJoin)
        #expect(msg.command == .JOIN)
        let account = msg.tags["account"] as? String
        #expect(account == "nick")
        #expect(msg.parameters[0] == "#chan")
    }

    // MARK: - CTCP

    @Test func parsesCTCPAction() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.ctcpAction)
        let text = msg.parameters.last ?? ""
        let ctcp = CTCP.decode(text)
        #expect(ctcp?.verb == "ACTION")
        #expect(ctcp?.body == "waves")
        #expect(ctcp?.isAction == true)
    }

    @Test func parsesCTCPVersion() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.ctcpVersion)
        let ctcp = CTCP.decode(msg.parameters.last ?? "")
        #expect(ctcp?.verb == "VERSION")
        #expect(ctcp?.body == nil)
    }

    @Test func parsesCTCPPing() throws {
        let msg = try IRCMessageParser.parse(IRCFixtures.ctcpPing)
        let ctcp = CTCP.decode(msg.parameters.last ?? "")
        #expect(ctcp?.verb == "PING")
        #expect(ctcp?.body == "1234567890")
    }

    // MARK: - Error cases

    @Test func throwsOnEmptyLine() {
        #expect(throws: IRCParseError.emptyLine) {
            try IRCMessageParser.parse("")
        }
    }

    @Test func throwsOnOnlyCRLF() {
        #expect(throws: IRCParseError.emptyLine) {
            try IRCMessageParser.parse("\r\n")
        }
    }

    @Test func throwsOnMissingCommand() {
        #expect(throws: IRCParseError.missingCommand) {
            try IRCMessageParser.parse(":prefix")
        }
    }

    // MARK: - Edge cases

    @Test func stripsTrailingCRLF() throws {
        let msg = try IRCMessageParser.parse("PING :server\r\n")
        #expect(msg.command == .PING)
    }

    @Test func stripsTrailingLF() throws {
        let msg = try IRCMessageParser.parse("PING :server\n")
        #expect(msg.command == .PING)
    }

    @Test func handlesEmptyTrailingParam() throws {
        let msg = try IRCMessageParser.parse("TOPIC #chan :")
        #expect(msg.command == .TOPIC)
        #expect(msg.parameters[1] == "")
    }

    @Test func handlesMultipleSpacesBetweenParams() throws {
        let msg = try IRCMessageParser.parse("KICK #chan  nick :reason")
        #expect(msg.command == .KICK)
        #expect(msg.parameters[0] == "#chan")
    }

    @Test func parsesUnknownCommand() throws {
        let msg = try IRCMessageParser.parse(":server FOOBAR param")
        if case .unknown(let s) = msg.command {
            #expect(s == "FOOBAR")
        } else {
            #expect(Bool(false), "Expected .unknown")
        }
    }

    @Test func parsesHighNumeric() throws {
        let msg = try IRCMessageParser.parse(":server 999 nick :error")
        #expect(msg.command == .numeric(999))
    }

    @Test func parsesServerPrefixWithDots() throws {
        let msg = try IRCMessageParser.parse(":irc.example.com NOTICE * :test")
        #expect(msg.prefix?.isServer == true)
        #expect(msg.prefix?.host == "irc.example.com")
    }

    @Test func parsesNickWithNoHostmask() throws {
        let msg = try IRCMessageParser.parse(":justnick PRIVMSG #chan :hi")
        #expect(msg.prefix?.nick == "justnick")
        #expect(msg.prefix?.host == nil)
    }

    @Test func handlesLongMessage() throws {
        let longText = String(repeating: "a", count: 490)
        let line = ":nick!u@h PRIVMSG #chan :\(longText)"
        let msg = try IRCMessageParser.parse(line)
        #expect(msg.parameters.last?.count == 490)
    }
}
