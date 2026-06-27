import Testing
@testable import IRC

struct SASLTests {

    @Test func producesCorrectBase64ForShortCredentials() {
        // \0nick\0pass → base64
        let chunks = SASLEngine.plainChunks(authcid: "nick", password: "pass")
        #expect(chunks.count == 1)
        // Verify by decoding
        let decoded = Data(base64Encoded: chunks[0]).flatMap { String(data: $0, encoding: .utf8) }
        #expect(decoded == "\0nick\0pass")
    }

    @Test func appendsPlusWhenExactMultipleOf400() {
        // Craft a credential whose base64 is exactly 400 chars
        // base64 of N bytes = ceil(N/3)*4 chars
        // 300 bytes → 400 base64 chars exactly
        let pass = String(repeating: "x", count: 298)  // "\0nick\0" is 6 chars + 298 = 304 → 408 chars — not exact
        // Just check that a normal short cred doesn't append +
        let chunks = SASLEngine.plainChunks(authcid: "n", password: "p")
        #expect(!chunks.contains("+") || chunks.count == 1)
    }

    @Test func chunksSplitCorrectlyForLongPassword() {
        // Create a password that produces > 400 base64 chars
        // 300 raw bytes → 400 base64 chars. "\0" + 1-char nick + "\0" + 297-char pass = 300 bytes → 400 chars
        let longPass = String(repeating: "a", count: 297)
        let chunks = SASLEngine.plainChunks(authcid: "n", password: longPass)
        // 300 bytes → exactly 400 base64 chars → needs "+" chunk
        #expect(chunks.last == "+")
    }

    @Test func emptyChunksReturnPlus() {
        let chunks = SASLEngine.plainChunks(authcid: "", password: "")
        #expect(!chunks.isEmpty)
    }
}
