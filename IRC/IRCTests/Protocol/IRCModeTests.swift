import Testing
@testable import IRC

struct IRCModeTests {

    @Test func parsesOpAndVoice() {
        let changes = IRCMode.parse(params: ["+ov", "nick1", "nick2"])
        #expect(changes.count == 2)
        #expect(changes[0].char == "o")
        #expect(changes[0].direction == .add)
        #expect(changes[0].parameter == "nick1")
        #expect(changes[1].char == "v")
        #expect(changes[1].direction == .add)
        #expect(changes[1].parameter == "nick2")
    }

    @Test func parsesRemoveOp() {
        let changes = IRCMode.parse(params: ["-o", "nick1"])
        #expect(changes.count == 1)
        #expect(changes[0].char == "o")
        #expect(changes[0].direction == .remove)
        #expect(changes[0].parameter == "nick1")
    }

    @Test func parsesChannelModeNoParam() {
        let changes = IRCMode.parse(params: ["+mn"])
        #expect(changes.count == 2)
        #expect(changes[0].char == "m")
        #expect(changes[0].parameter == nil)
        #expect(changes[1].char == "n")
        #expect(changes[1].parameter == nil)
    }

    @Test func parsesKeyMode() {
        let changes = IRCMode.parse(params: ["+k", "secretkey"])
        #expect(changes.count == 1)
        #expect(changes[0].char == "k")
        #expect(changes[0].parameter == "secretkey")
    }

    @Test func parsesRemoveKeyNoParam() {
        // type B: always has param on +, but on - the server may or may not send one
        let changes = IRCMode.parse(params: ["-k", "*"])
        #expect(changes.count == 1)
        #expect(changes[0].char == "k")
        #expect(changes[0].direction == .remove)
    }

    @Test func parsesMixedDirections() {
        let changes = IRCMode.parse(params: ["+o-v", "nick1", "nick2"])
        #expect(changes.count == 2)
        #expect(changes[0].direction == .add)
        #expect(changes[0].char == "o")
        #expect(changes[1].direction == .remove)
        #expect(changes[1].char == "v")
    }

    @Test func parsesUserModes() {
        let changes = IRCMode.parseUserModes("+i-x")
        #expect(changes.count == 2)
        #expect(changes[0].char == "i"); #expect(changes[0].direction == .add)
        #expect(changes[1].char == "x"); #expect(changes[1].direction == .remove)
    }

    @Test func parsesLimitMode() {
        // type C: limit has param on + but not on -
        var types = IRCMode.ChannelModeTypes()
        types.paramWhenSet = ["l"]
        let add = IRCMode.parse(params: ["+l", "100"], types: types)
        #expect(add[0].parameter == "100")
        let remove = IRCMode.parse(params: ["-l"], types: types)
        #expect(remove[0].parameter == nil)
    }
}
