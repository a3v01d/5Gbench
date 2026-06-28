import Foundation

/// Produces the AUTHENTICATE payload chunks for SASL PLAIN.
struct SASLEngine {

    /// Maximum bytes per AUTHENTICATE chunk (IRC spec).
    private static let chunkSize = 400

    /// Produces base64-encoded chunks of the SASL PLAIN credential blob.
    /// Returns an array of strings to send as successive AUTHENTICATE payloads.
    /// The final chunk is either <400 bytes or the special "+" token.
    static func plainChunks(authcid: String, password: String) -> [String] {
        // Format: \0authcid\0password
        let blob = "\0\(authcid)\0\(password)"
        let data = Data(blob.utf8)
        let encoded = data.base64EncodedString()

        var chunks: [String] = []
        var idx = encoded.startIndex
        while idx < encoded.endIndex {
            let end = encoded.index(idx, offsetBy: chunkSize, limitedBy: encoded.endIndex) ?? encoded.endIndex
            chunks.append(String(encoded[idx..<end]))
            idx = end
        }

        // If the total is an exact multiple of chunkSize, we must send "+" to signal completion.
        if encoded.count % chunkSize == 0 {
            chunks.append("+")
        }

        return chunks.isEmpty ? ["+"] : chunks
    }
}
