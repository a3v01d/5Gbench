import Foundation

struct CommandInvocation {
    let verb: String          // e.g. "join", "msg", or "privmsg" for plain text
    let rawArgs: String       // everything after the verb
    let args: [String]        // split args

    var isPlainText: Bool { verb == "_text" }
}

struct CommandParser {

    static func parse(input: String, currentTarget: String) -> CommandInvocation {
        let trimmed = input.trimmingCharacters(in: .whitespaces)

        guard trimmed.hasPrefix("/") else {
            return CommandInvocation(verb: "_text", rawArgs: trimmed, args: [trimmed])
        }

        let body = String(trimmed.dropFirst())
        let parts = body.split(separator: " ", maxSplits: 1, omittingEmptySubsequences: true)
        let verb = parts.isEmpty ? "" : String(parts[0]).lowercased()
        let rawArgs = parts.count > 1 ? String(parts[1]) : ""
        let args = rawArgs.split(separator: " ").map(String.init)

        return CommandInvocation(verb: verb, rawArgs: rawArgs, args: args)
    }
}
