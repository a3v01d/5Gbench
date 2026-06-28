import Testing
@testable import IRC

struct CapabilityNegotiatorTests {

    @Test func beginsWithCapLS302() async {
        let neg = CapabilityNegotiator()
        let line = await neg.begin()
        #expect(line == "CAP LS 302\r\n")
    }

    @Test func requestsDesiredCapsAfterLSReply() async {
        let neg = CapabilityNegotiator()
        _ = await neg.begin()
        let lines = await neg.handle(
            subcommand: "LS",
            params: ["sasl=PLAIN multi-prefix server-time message-ids"]
        )
        // Should send a REQ line
        #expect(lines.count == 1)
        #expect(lines[0].hasPrefix("CAP REQ"))
        #expect(lines[0].contains("sasl"))
    }

    @Test func sendsEndAfterNoCommonCaps() async {
        let neg = CapabilityNegotiator()
        _ = await neg.begin()
        let lines = await neg.handle(subcommand: "LS", params: ["unknown-cap"])
        #expect(lines.contains(where: { $0.hasPrefix("CAP END") }))
    }

    @Test func tracksACKedCaps() async {
        let neg = CapabilityNegotiator()
        _ = await neg.begin()
        _ = await neg.handle(subcommand: "LS", params: ["sasl multi-prefix server-time"])
        _ = await neg.handle(subcommand: "ACK", params: ["sasl multi-prefix server-time"])
        let acked = await neg.acknowledged
        #expect(acked.contains(.sasl))
        #expect(acked.contains(.multiPrefix))
        #expect(acked.contains(.serverTime))
    }

    @Test func tracksNAKedCaps() async {
        let neg = CapabilityNegotiator()
        _ = await neg.begin()
        _ = await neg.handle(subcommand: "LS", params: ["sasl"])
        _ = await neg.handle(subcommand: "NAK", params: ["sasl"])
        let denied = await neg.denied
        #expect(denied.contains(.sasl))
    }

    @Test func needsSASLWhenAcknowledged() async {
        let neg = CapabilityNegotiator()
        _ = await neg.begin()
        _ = await neg.handle(subcommand: "LS", params: ["sasl"])
        _ = await neg.handle(subcommand: "ACK", params: ["sasl"])
        let needs = await neg.needsSASL
        #expect(needs == true)
    }

    @Test func handlesMultilineLS() async {
        let neg = CapabilityNegotiator()
        _ = await neg.begin()
        // First line: * = more coming
        let resp1 = await neg.handle(subcommand: "LS", params: ["*", "sasl multi-prefix"])
        #expect(resp1.isEmpty)  // no REQ yet
        // Second line: no * = final
        let resp2 = await neg.handle(subcommand: "LS", params: ["server-time message-ids"])
        #expect(resp2.contains(where: { $0.hasPrefix("CAP REQ") }))
    }
}
