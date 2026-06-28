import Foundation

// Named constants for well-known IRC numeric reply codes (RFC 1459 + common extensions)
enum IRCNumeric {
    // Connection / Registration
    static let RPL_WELCOME       = 1
    static let RPL_YOURHOST      = 2
    static let RPL_CREATED       = 3
    static let RPL_MYINFO        = 4
    static let RPL_ISUPPORT      = 5
    static let RPL_BOUNCE        = 10

    // WHOIS / WHO replies
    static let RPL_WHOISUSER     = 311
    static let RPL_WHOISSERVER   = 312
    static let RPL_WHOISOPERATOR = 313
    static let RPL_WHOISIDLE     = 317
    static let RPL_ENDOFWHOIS    = 318
    static let RPL_WHOISCHANNELS = 319
    static let RPL_WHOISACCOUNT  = 330
    static let RPL_WHOISHOST     = 378
    static let RPL_WHOISMODES    = 379
    static let RPL_WHOISSECURE   = 671

    // Channel
    static let RPL_CHANNELMODEIS = 324
    static let RPL_CREATIONTIME  = 329
    static let RPL_NOTOPIC       = 331
    static let RPL_TOPIC         = 332
    static let RPL_TOPICWHOTIME  = 333
    static let RPL_INVITING      = 341
    static let RPL_NAMREPLY      = 353
    static let RPL_ENDOFNAMES    = 366
    static let RPL_BANLIST       = 367
    static let RPL_ENDOFBANLIST  = 368

    // Lists
    static let RPL_MOTDSTART     = 375
    static let RPL_MOTD          = 372
    static let RPL_ENDOFMOTD     = 376
    static let RPL_LUSERCLIENT   = 251
    static let RPL_LUSEROP       = 252
    static let RPL_LUSERUNKNOWN  = 253
    static let RPL_LUSERCHANNELS = 254
    static let RPL_LUSERME       = 255
    static let RPL_LOCALUSERS    = 265
    static let RPL_GLOBALUSERS   = 266

    // SASL (IRCv3)
    static let RPL_LOGGEDIN      = 900
    static let RPL_LOGGEDOUT     = 901
    static let RPL_NICKLOCKED    = 902
    static let RPL_SASLSUCCESS   = 903
    static let ERR_SASLFAIL      = 904
    static let ERR_SASLTOOLONG   = 905
    static let ERR_SASLABORTED   = 906
    static let ERR_SASLALREADY   = 907
    static let RPL_SASLMECHS     = 908

    // Errors
    static let ERR_NOSUCHNICK        = 401
    static let ERR_NOSUCHSERVER      = 402
    static let ERR_NOSUCHCHANNEL     = 403
    static let ERR_CANNOTSENDTOCHAN  = 404
    static let ERR_TOOMANYCHANNELS   = 405
    static let ERR_WASNOSUCHNICK     = 406
    static let ERR_NORECIPIENT       = 411
    static let ERR_NOTEXTTOSEND      = 412
    static let ERR_UNKNOWNCOMMAND    = 421
    static let ERR_NOMOTD            = 422
    static let ERR_NONICKNAMEGIVEN   = 431
    static let ERR_ERRONEUSNICKNAME  = 432
    static let ERR_NICKNAMEINUSE     = 433
    static let ERR_NICKCOLLISION     = 436
    static let ERR_USERNOTINCHANNEL  = 441
    static let ERR_NOTONCHANNEL      = 442
    static let ERR_USERONCHANNEL     = 443
    static let ERR_NOTREGISTERED     = 451
    static let ERR_NEEDMOREPARAMS    = 461
    static let ERR_ALREADYREGISTERED = 462
    static let ERR_PASSWDMISMATCH    = 464
    static let ERR_YOUREBANNEDCREEP  = 465
    static let ERR_KEYSET            = 467
    static let ERR_CHANNELISFULL     = 471
    static let ERR_UNKNOWNMODE       = 472
    static let ERR_INVITEONLYCHAN    = 473
    static let ERR_BANNEDFROMCHAN    = 474
    static let ERR_BADCHANNELKEY     = 475
    static let ERR_BADCHANMASK       = 476
    static let ERR_NOPRIVILEGES      = 481
    static let ERR_CHANOPRIVSNEEDED  = 482
    static let ERR_CANTKILLSERVER    = 483
    static let ERR_NOOPERHOST        = 491
    static let ERR_UMODEUNKNOWNFLAG  = 501
    static let ERR_USERSDONTMATCH    = 502
}
