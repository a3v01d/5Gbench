import Testing
@testable import IRC

struct CTCPTests {

    @Test func decodesAction() {
        let ctcp = CTCP.decode("\u{0001}ACTION dances\u{0001}")
        #expect(ctcp?.verb == "ACTION")
        #expect(ctcp?.body == "dances")
        #expect(ctcp?.isAction == true)
    }

    @Test func decodesVersion() {
        let ctcp = CTCP.decode("\u{0001}VERSION\u{0001}")
        #expect(ctcp?.verb == "VERSION")
        #expect(ctcp?.body == nil)
    }

    @Test func decodesPing() {
        let ctcp = CTCP.decode("\u{0001}PING 1234567890\u{0001}")
        #expect(ctcp?.verb == "PING")
        #expect(ctcp?.body == "1234567890")
    }

    @Test func returnsNilForNonCTCP() {
        #expect(CTCP.decode("plain message") == nil)
        #expect(CTCP.decode("") == nil)
    }

    @Test func encodesAction() {
        let encoded = CTCP.action("waves hello")
        #expect(encoded == "\u{0001}ACTION waves hello\u{0001}")
    }

    @Test func encodesVersionReply() {
        let encoded = CTCP.versionReply("IRC iOS/1.0")
        #expect(encoded == "\u{0001}VERSION IRC iOS/1.0\u{0001}")
    }

    @Test func encodesPingReply() {
        let encoded = CTCP.pingReply("12345")
        #expect(encoded == "\u{0001}PING 12345\u{0001}")
    }

    @Test func roundTrip() {
        let original = "some multi-word action text"
        let encoded = CTCP.action(original)
        let decoded = CTCP.decode(encoded)
        #expect(decoded?.isAction == true)
        #expect(decoded?.body == original)
    }
}
