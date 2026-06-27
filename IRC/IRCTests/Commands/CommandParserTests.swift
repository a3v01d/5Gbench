import Testing
@testable import IRC

struct CommandParserTests {

    @Test func parsesPlainText() {
        let inv = CommandParser.parse(input: "hello world", currentTarget: "#test")
        #expect(inv.isPlainText)
        #expect(inv.rawArgs == "hello world")
    }

    @Test func parsesSlashJoin() {
        let inv = CommandParser.parse(input: "/join #test", currentTarget: "")
        #expect(inv.verb == "join")
        #expect(inv.args[0] == "#test")
    }

    @Test func parsesJoinWithKey() {
        let inv = CommandParser.parse(input: "/join #test secretkey", currentTarget: "")
        #expect(inv.verb == "join")
        #expect(inv.args[0] == "#test")
        #expect(inv.args[1] == "secretkey")
    }

    @Test func parsesMsg() {
        let inv = CommandParser.parse(input: "/msg someone hello there", currentTarget: "#test")
        #expect(inv.verb == "msg")
        #expect(inv.args[0] == "someone")
        #expect(inv.rawArgs == "someone hello there")
    }

    @Test func parsesRaw() {
        let inv = CommandParser.parse(input: "/raw MODE #chan +m", currentTarget: "")
        #expect(inv.verb == "raw")
        #expect(inv.rawArgs == "MODE #chan +m")
    }

    @Test func verbIsLowercased() {
        let inv = CommandParser.parse(input: "/JOIN #chan", currentTarget: "")
        #expect(inv.verb == "join")
    }

    @Test func handlesSlashOnly() {
        let inv = CommandParser.parse(input: "/", currentTarget: "")
        #expect(inv.verb == "")
    }

    @Test func handlesEmptyInput() {
        let inv = CommandParser.parse(input: "", currentTarget: "#chan")
        #expect(inv.isPlainText)
        #expect(inv.rawArgs == "")
    }

    @Test func commandRegistryMatchesPrefix() {
        let results = CommandRegistry.matching(prefix: "jo")
        #expect(results.contains(where: { $0.name == "join" }))
        #expect(!results.contains(where: { $0.name == "quit" }))
    }

    @Test func commandRegistryEmptyPrefixReturnsAll() {
        let results = CommandRegistry.matching(prefix: "")
        #expect(results.count == CommandRegistry.all.count)
    }
}
