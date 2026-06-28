import Foundation

/// Raw IRC line fixtures for use in unit tests.
enum IRCFixtures {
    // Registration
    static let welcome       = ":irc.libera.chat 001 testnick :Welcome to the Libera.Chat IRC network testnick"
    static let yourHost      = ":irc.libera.chat 002 testnick :Your host is irc.libera.chat, running version solanum-1.0"
    static let isupport      = ":irc.libera.chat 005 testnick CHANTYPES=# CHANMODES=eIbq,k,flj,CFLMPQScgimnprstuz PREFIX=(qaohv)~&@%+ CASEMAPPING=rfc1459 NETWORK=Libera.Chat :are supported"

    // CAP
    static let capLS         = ":irc.libera.chat CAP * LS :sasl=PLAIN multi-prefix server-time message-ids extended-join away-notify"
    static let capACK        = ":irc.libera.chat CAP * ACK :sasl multi-prefix server-time"
    static let capNAK        = ":irc.libera.chat CAP * NAK :batch labeled-response"

    // SASL
    static let authenticate  = "AUTHENTICATE +"
    static let saslSuccess   = ":irc.libera.chat 903 testnick :SASL authentication successful"

    // JOIN / PART
    static let joinChannel   = ":testnick!~user@example.com JOIN #test"
    static let joinOther     = ":othernick!~user@host.example JOIN #test"
    static let partChannel   = ":othernick!~user@host.example PART #test :Goodbye"
    static let quitServer    = ":othernick!~user@host.example QUIT :Client quit"
    static let kickMessage   = ":op!~op@op.example KICK #test othernick :Reason"

    // PRIVMSG / NOTICE
    static let privmsgChannel   = ":othernick!~user@host.example PRIVMSG #test :Hello, world!"
    static let privmsgDM        = ":othernick!~user@host.example PRIVMSG testnick :Private hello"
    static let noticeChannel    = ":server.example NOTICE #test :Scheduled maintenance at midnight"
    static let ctcpAction       = ":othernick!~user@host.example PRIVMSG #test :\u{0001}ACTION waves\u{0001}"
    static let ctcpVersion      = ":othernick!~user@host.example PRIVMSG testnick :\u{0001}VERSION\u{0001}"
    static let ctcpPing         = ":othernick!~user@host.example PRIVMSG testnick :\u{0001}PING 1234567890\u{0001}"

    // NICK / TOPIC / MODE
    static let nickChange    = ":oldnick!~user@host.example NICK :newnick"
    static let topicChange   = ":op!~op@op.example TOPIC #test :New topic here"
    static let topicReply    = ":irc.libera.chat 332 testnick #test :Channel topic goes here"
    static let modeOpVoice   = ":op!~op@op.example MODE #test +ov nick1 nick2"
    static let modeDeop      = ":op!~op@op.example MODE #test -o nick1"
    static let modeBan       = ":op!~op@op.example MODE #test +b *!*@badhost.example"
    static let modeKey       = ":op!~op@op.example MODE #test +k secretkey"

    // NAMES
    static let namesReply    = ":irc.libera.chat 353 testnick = #test :@op +voiced plain"
    static let endOfNames    = ":irc.libera.chat 366 testnick #test :End of /NAMES list"

    // PING
    static let ping          = "PING :irc.libera.chat"
    static let pingPrefix    = ":irc.libera.chat PING irc.libera.chat"

    // Server error
    static let error         = "ERROR :Closing link: (user@host) [Killed (server)]"

    // IRCv3 tagged messages
    static let taggedPrivmsg = "@time=2024-01-15T20:30:00.123Z;msgid=abc123 :nick!user@host PRIVMSG #chan :tagged message"
    static let taggedJoin    = "@account=nick :nick!user@host JOIN #chan * :Real Name"
    static let escapedTags   = "@key=value\\shas\\sspaces;flag :server NOTICE * :test"

    // WHOIS
    static let whoisUser     = ":irc.libera.chat 311 testnick othernick ~user host.example * :Real Name"
    static let whoisServer   = ":irc.libera.chat 312 testnick othernick irc.libera.chat :Libera.Chat"
    static let whoisIdle     = ":irc.libera.chat 317 testnick othernick 42 1234567890 :seconds idle, signon time"
    static let whoisEnd      = ":irc.libera.chat 318 testnick othernick :End of /WHOIS list"

    // Errors
    static let nickInUse     = ":irc.libera.chat 433 * testnick :Nickname is already in use"
    static let notOnChannel  = ":irc.libera.chat 442 testnick #test :You're not on that channel"
    static let chanFull      = ":irc.libera.chat 471 testnick #test :Cannot join channel (+l)"
    static let bannedFromChan = ":irc.libera.chat 474 testnick #test :Cannot join channel (+b)"
}
