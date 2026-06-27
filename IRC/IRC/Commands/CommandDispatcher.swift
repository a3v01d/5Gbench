import Foundation

/// Translates a CommandInvocation + context into session calls.
@MainActor
struct CommandDispatcher {

    let server: ServerStore
    let channelStore: ChannelStore?

    var currentTarget: String { channelStore?.displayName ?? "" }

    func dispatch(_ invocation: CommandInvocation) async -> DispatchResult {
        switch invocation.verb {

        case "_text":
            guard !currentTarget.isEmpty else { return .error("No active channel") }
            let text = invocation.rawArgs
            if text.isEmpty { return .handled }
            let ctcpAction = text.hasPrefix("/me ") ? String(text.dropFirst(4)) : nil
            let sendText = ctcpAction.map { CTCP.action($0) } ?? text
            await server.send(privmsg: sendText, to: currentTarget)
            return .handled

        case "join":
            guard let chan = invocation.args.first else { return .usage("/join <#channel> [key]") }
            let key = invocation.args.count > 1 ? invocation.args[1] : nil
            await server.join(channel: chan, key: key)
            return .handled

        case "part":
            guard !currentTarget.isEmpty else { return .error("Not in a channel") }
            let msg = invocation.rawArgs.isEmpty ? nil : invocation.rawArgs
            await server.part(channel: currentTarget, message: msg)
            return .handled

        case "quit":
            let msg = invocation.rawArgs.isEmpty ? "Goodbye" : invocation.rawArgs
            await server.send(rawLine: IRCMessageEncoder.quit(message: msg))
            return .handled

        case "nick":
            guard let newNick = invocation.args.first else { return .usage("/nick <newnick>") }
            await server.send(rawLine: IRCMessageEncoder.nick(newNick))
            return .handled

        case "msg":
            guard invocation.args.count >= 2 else { return .usage("/msg <nick> <text>") }
            let target = invocation.args[0]
            let text = invocation.args.dropFirst().joined(separator: " ")
            await server.send(privmsg: text, to: target)
            return .handled

        case "me":
            guard !currentTarget.isEmpty else { return .error("Not in a channel") }
            guard !invocation.rawArgs.isEmpty else { return .usage("/me <action>") }
            await server.send(privmsg: CTCP.action(invocation.rawArgs), to: currentTarget)
            return .handled

        case "topic":
            guard !currentTarget.isEmpty else { return .error("Not in a channel") }
            await server.send(rawLine: IRCMessageEncoder.topic(channel: currentTarget, text: invocation.rawArgs))
            return .handled

        case "kick":
            guard !currentTarget.isEmpty, let nick = invocation.args.first else {
                return .usage("/kick <nick> [reason]")
            }
            let reason = invocation.args.count > 1 ? invocation.args.dropFirst().joined(separator: " ") : nil
            await server.send(rawLine: IRCMessageEncoder.kick(channel: currentTarget, nick: nick, reason: reason))
            return .handled

        case "ban":
            guard !currentTarget.isEmpty, let mask = invocation.args.first else {
                return .usage("/ban <mask>")
            }
            await server.send(rawLine: IRCMessageEncoder.mode(target: currentTarget, flags: "+b", params: [mask]))
            return .handled

        case "mode":
            let target = invocation.args.first?.hasPrefix("#") == true ? invocation.args[0] : currentTarget
            let flags = invocation.args.count > 1 ? invocation.args[1] : (invocation.args.first ?? "")
            let params = invocation.args.count > 2 ? Array(invocation.args.dropFirst(2)) : []
            await server.send(rawLine: IRCMessageEncoder.mode(target: target, flags: flags, params: params))
            return .handled

        case "whois":
            guard let nick = invocation.args.first else { return .usage("/whois <nick>") }
            await server.send(rawLine: IRCMessageEncoder.whois(nick: nick))
            return .handled

        case "away":
            let msg = invocation.rawArgs.isEmpty ? nil : invocation.rawArgs
            await server.setAway(message: msg)
            return .handled

        case "raw":
            await server.send(rawLine: invocation.rawArgs + "\r\n")
            return .handled

        case "clear":
            channelStore?.messages.removeAll()
            return .handled

        case "names":
            if !currentTarget.isEmpty {
                await server.send(rawLine: IRCMessageEncoder.encode(command: .NAMES, parameters: [currentTarget]))
            }
            return .handled

        default:
            return .unknown(invocation.verb)
        }
    }
}

enum DispatchResult {
    case handled
    case error(String)
    case usage(String)
    case unknown(String)
}
