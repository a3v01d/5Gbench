import Foundation

struct CommandDefinition {
    let name: String        // without leading /
    let usage: String       // short usage hint shown in autocomplete
    let description: String
}

enum CommandRegistry {

    static let all: [CommandDefinition] = [
        CommandDefinition(name: "join",   usage: "/join <#channel> [key]",    description: "Join a channel"),
        CommandDefinition(name: "part",   usage: "/part [message]",           description: "Leave the current channel"),
        CommandDefinition(name: "quit",   usage: "/quit [message]",           description: "Disconnect from server"),
        CommandDefinition(name: "nick",   usage: "/nick <newnick>",           description: "Change your nickname"),
        CommandDefinition(name: "msg",    usage: "/msg <nick> <text>",        description: "Send a private message"),
        CommandDefinition(name: "me",     usage: "/me <action>",              description: "Send a CTCP ACTION"),
        CommandDefinition(name: "topic",  usage: "/topic <text>",             description: "Set the channel topic"),
        CommandDefinition(name: "kick",   usage: "/kick <nick> [reason]",     description: "Kick a user"),
        CommandDefinition(name: "ban",    usage: "/ban <mask>",               description: "Ban a hostmask"),
        CommandDefinition(name: "mode",   usage: "/mode <flags> [params...]", description: "Set channel or user modes"),
        CommandDefinition(name: "whois",  usage: "/whois <nick>",             description: "Retrieve user information"),
        CommandDefinition(name: "away",   usage: "/away [message]",           description: "Set or clear away status"),
        CommandDefinition(name: "raw",    usage: "/raw <line>",               description: "Send a raw IRC line"),
        CommandDefinition(name: "clear",  usage: "/clear",                    description: "Clear the message buffer"),
        CommandDefinition(name: "ignore", usage: "/ignore <nick>",            description: "Ignore a user locally"),
        CommandDefinition(name: "names",  usage: "/names",                    description: "Refresh the user list"),
    ]

    static func matching(prefix: String) -> [CommandDefinition] {
        let lower = prefix.lowercased()
        return all.filter { $0.name.hasPrefix(lower) }
    }
}
