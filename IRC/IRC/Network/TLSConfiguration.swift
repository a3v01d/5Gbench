import Foundation
import Network

enum TLSConfiguration {

    /// Builds standard CA-verified TLS parameters for a server connection.
    static func standard() -> NWProtocolTLS.Options {
        let opts = NWProtocolTLS.Options()
        // Minimum TLS 1.2
        sec_protocol_options_set_min_tls_protocol_version(opts.securityProtocolOptions, .TLSv12)
        return opts
    }

    /// Builds TLS parameters that accept any server certificate (for self-signed servers).
    /// WARNING: disables chain validation — only for explicitly user-opted servers.
    static func acceptingInvalidCerts() -> NWProtocolTLS.Options {
        let opts = NWProtocolTLS.Options()
        sec_protocol_options_set_min_tls_protocol_version(opts.securityProtocolOptions, .TLSv12)
        sec_protocol_options_set_verify_block(opts.securityProtocolOptions, { _, _, completion in
            completion(true)
        }, .global(qos: .default))
        return opts
    }

    /// Builds the NWParameters for the connection based on whether TLS is needed.
    static func parameters(useTLS: Bool, acceptInvalidCert: Bool) -> NWParameters {
        if useTLS {
            let tls = acceptInvalidCert ? acceptingInvalidCerts() : standard()
            return NWParameters(tls: tls, tcp: .init())
        } else {
            return .tcp
        }
    }
}
