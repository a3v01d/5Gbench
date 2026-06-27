import SwiftUI

struct AddServerView: View {
    @Environment(AppStore.self) private var appStore
    @Environment(\.dismiss) private var dismiss

    @State private var displayName = ""
    @State private var host = ""
    @State private var port = "6697"
    @State private var useTLS = true
    @State private var nickname = "ircuser"
    @State private var alternateNick = "ircuser_"
    @State private var realname = "IRC User"
    @State private var username = "ircuser"
    @State private var serverPassword = ""
    @State private var useSASL = false
    @State private var saslUsername = ""
    @State private var saslPassword = ""
    @State private var nickServPassword = ""
    @State private var autoConnect = false
    @State private var acceptInvalidCert = false

    var body: some View {
        NavigationStack {
            Form {
                Section("Server") {
                    TextField("Display Name", text: $displayName)
                    TextField("Hostname", text: $host)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .keyboardType(.URL)
                    TextField("Port", text: $port)
                        .keyboardType(.numberPad)
                    Toggle("Use TLS", isOn: $useTLS)
                    if useTLS {
                        Toggle("Accept invalid certificate", isOn: $acceptInvalidCert)
                    }
                    SecureField("Server password (optional)", text: $serverPassword)
                    Toggle("Auto-connect", isOn: $autoConnect)
                }

                Section("Identity") {
                    TextField("Nickname", text: $nickname)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                    TextField("Alternate nick", text: $alternateNick)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                    TextField("Username", text: $username)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                    TextField("Real name", text: $realname)
                }

                Section("Authentication") {
                    Toggle("Use SASL", isOn: $useSASL)
                    if useSASL {
                        TextField("SASL username", text: $saslUsername)
                            .textInputAutocapitalization(.never)
                        SecureField("SASL password", text: $saslPassword)
                    } else {
                        SecureField("NickServ password (optional)", text: $nickServPassword)
                    }
                }
            }
            .navigationTitle("Add Server")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Add") { saveServer() }
                        .disabled(host.isEmpty || nickname.isEmpty)
                }
            }
        }
    }

    private func saveServer() {
        let portInt = Int(port) ?? (useTLS ? 6697 : 6667)
        let config = ServerConfig(
            displayName: displayName.isEmpty ? host : displayName,
            host: host,
            port: portInt,
            useTLS: useTLS,
            acceptInvalidCert: acceptInvalidCert,
            nickname: nickname,
            alternateNick: alternateNick,
            realname: realname,
            username: username,
            useSASL: useSASL,
            saslUsername: useSASL ? saslUsername : nil,
            autoConnect: autoConnect
        )

        // Store secrets in Keychain
        if !serverPassword.isEmpty {
            Keychain.save(key: config.keychainServerPasswordKey, value: serverPassword)
        }
        if useSASL && !saslPassword.isEmpty {
            Keychain.save(key: config.keychainSASLPasswordKey, value: saslPassword)
        }
        if !useSASL && !nickServPassword.isEmpty {
            Keychain.save(key: config.keychainNickServPasswordKey, value: nickServPassword)
        }

        appStore.addServer(config: config)
        dismiss()
    }
}
