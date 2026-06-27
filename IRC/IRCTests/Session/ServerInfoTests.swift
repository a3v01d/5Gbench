import Testing
@testable import IRC

struct ServerInfoTests {

    @Test func parsesNetworkName() {
        let info = ServerInfo()
        info.apply(tokens: ["NETWORK=Libera.Chat"])
        #expect(info.networkName == "Libera.Chat")
    }

    @Test func parsesChanTypes() {
        let info = ServerInfo()
        info.apply(tokens: ["CHANTYPES=#&"])
        #expect(info.chanTypes.contains("#"))
        #expect(info.chanTypes.contains("&"))
    }

    @Test func parsesPREFIX() {
        let info = ServerInfo()
        info.apply(tokens: ["PREFIX=(qaohv)~&@%+"])
        #expect(info.symbol(forMode: "o") == "@")
        #expect(info.symbol(forMode: "v") == "+")
        #expect(info.mode(forSymbol: "@") == "o")
    }

    @Test func parsesCHANMODES() {
        let info = ServerInfo()
        info.apply(tokens: ["CHANMODES=eIbq,k,flj,CFLMPQScgimnprstuz"])
        #expect(info.chanModeTypes.listModes.contains("b"))
        #expect(info.chanModeTypes.paramAlways.contains("k"))
        #expect(info.chanModeTypes.paramWhenSet.contains("l"))
        #expect(info.chanModeTypes.noParam.contains("m"))
    }

    @Test func normalizeRFC1459() {
        let info = ServerInfo()
        info.apply(tokens: ["CASEMAPPING=rfc1459"])
        #expect(info.normalize("NICK[name]") == "nick{name}")
        #expect(info.normalize("A\\B") == "a|b")
    }

    @Test func normalizeASCII() {
        let info = ServerInfo()
        info.apply(tokens: ["CASEMAPPING=ascii"])
        // In ASCII casemapping, [] are NOT lowercased to {}
        #expect(info.normalize("[test]") == "[test]")
        #expect(info.normalize("UPPER") == "upper")
    }

    @Test func parsesMultipleTokensAtOnce() {
        let info = ServerInfo()
        info.apply(tokens: [
            "CHANTYPES=#", "CHANMODES=eIbq,k,flj,CFLMnpst",
            "PREFIX=(ov)@+", "NETWORK=TestNet", "NICKLEN=30"
        ])
        #expect(info.networkName == "TestNet")
        #expect(info.maxNickLength == 30)
        #expect(info.symbol(forMode: "o") == "@")
    }
}
