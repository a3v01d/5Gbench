import Testing
@testable import IRC

struct IRCMessageEncoderTests {

    @Test func encodesNick() {
        let line = IRCMessageEncoder.nick("testnick")
        #expect(line == "NICK testnick\r\n")
    }

    @Test func encodesUser() {
        let line = IRCMessageEncoder.user(username: "user", realname: "Real Name")
        #expect(line == "USER user 0 * :Real Name\r\n")
    }

    @Test func encodesJoinNoKey() {
        #expect(IRCMessageEncoder.join(channel: "#test") == "JOIN #test\r\n")
    }

    @Test func encodesJoinWithKey() {
        #expect(IRCMessageEncoder.join(channel: "#test", key: "secret") == "JOIN #test secret\r\n")
    }

    @Test func encodesPrivmsg() {
        let line = IRCMessageEncoder.privmsg(target: "#test", text: "Hello, world!")
        #expect(line == "PRIVMSG #test :Hello, world!\r\n")
    }

    @Test func encodesPrivmsgWithSpaces() {
        let line = IRCMessageEncoder.privmsg(target: "nick", text: "hey there")
        #expect(line == "PRIVMSG nick :hey there\r\n")
    }

    @Test func encodesPing() {
        #expect(IRCMessageEncoder.ping(server: "irc.example.com") == "PING irc.example.com\r\n")
    }

    @Test func encodesPong() {
        #expect(IRCMessageEncoder.pong(token: "irc.example.com") == "PONG :irc.example.com\r\n")
    }

    @Test func encodesQuit() {
        #expect(IRCMessageEncoder.quit(message: "Bye!") == "QUIT :Bye!\r\n")
    }

    @Test func encodesAway() {
        #expect(IRCMessageEncoder.away(message: "Gone") == "AWAY :Gone\r\n")
        #expect(IRCMessageEncoder.away() == "AWAY\r\n")
    }

    @Test func encodesCAPLS() {
        let line = IRCMessageEncoder.cap(subcommand: "LS", params: ["302"])
        #expect(line == "CAP LS 302\r\n")
    }

    @Test func encodesAuthenticate() {
        #expect(IRCMessageEncoder.authenticate(payload: "+") == "AUTHENTICATE +\r\n")
    }

    @Test func encodesKick() {
        let line = IRCMessageEncoder.kick(channel: "#test", nick: "baduser", reason: "Bye")
        #expect(line == "KICK #test baduser :Bye\r\n")
    }

    @Test func encodesMode() {
        let line = IRCMessageEncoder.mode(target: "#test", flags: "+o", params: ["nick"])
        #expect(line == "MODE #test +o nick\r\n")
    }
}
