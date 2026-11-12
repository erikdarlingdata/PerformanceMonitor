/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7657: what the listener does with one acknowledged envelope. <see cref="Accept"/> reaches a handler; every other
/// value is a drop, counted under its name in the listener's status.</summary>
public enum SlackEnvelopeVerdict
{
    /// <summary>The payload passed every filter.</summary>
    Accept,

    /// <summary>The event has a <c>bot_id</c> or the <c>bot_message</c> subtype: the alert webhook's posts and the bot's own.</summary>
    DropBot,

    /// <summary>The payload's workspace is not the one the listener learned at connect.</summary>
    DropTeam,

    /// <summary>The payload's <c>api_app_id</c> is not the app the connection belongs to.</summary>
    DropApp,

    /// <summary>A press whose <c>user.team_id</c> differs from <c>team.id</c>, or a message or mention whose author's workspace
    /// (<c>user_team</c>, else <c>team</c>) is not the store's: a person from another workspace in a shared channel.</summary>
    DropUserTeam,

    /// <summary>The channel is not a bot channel of this store's own routes: another store's thread, or a channel nobody set.</summary>
    DropChannel,

    /// <summary>The same event, by channel and ts, was already dispatched: one of Slack's retries.</summary>
    DropDuplicate,

    /// <summary>The payload lacks a field the filters need.</summary>
    DropMalformed,

    /// <summary>An envelope type nobody subscribes to.</summary>
    DropUnhandledType,
}

/// <summary>
/// #7657: the listener's filters, pure over one envelope's type and payload. The order is the design's: the <c>bot_id</c> filter runs
/// first, before any other work; then the workspace and app checks (the security points); then the author's workspace, on a press
/// (<c>user.team_id</c>) and on a message or mention (<c>user_team</c>, else <c>team</c>), because a Slack Connect channel a route owns
/// delivers an outside organization's message under the host workspace's <c>team_id</c>; then the channel-ownership rule (a store acts
/// only on events from channels set on its own routes, and stays silent elsewhere); then, for an event, the duplicate check the caller
/// runs with <see cref="SlackEventDedupe"/>, because that one needs state.
/// </summary>
public static class SlackEnvelopeFilter
{
    /// <summary>The Socket Mode envelope type of an Events API event.</summary>
    public const string EventsApiType = "events_api";

    /// <summary>The Socket Mode envelope type of a button press.</summary>
    public const string InteractiveType = "interactive";

    /// <summary>PURE: judges one envelope. On <see cref="SlackEnvelopeVerdict.Accept"/> exactly one of <paramref name="inboundEvent"/>
    /// and <paramref name="interaction"/> is set. <paramref name="teamId"/> and <paramref name="appId"/> are the ids learned at connect;
    /// a blank one fails closed, so nothing is accepted before both are known.</summary>
    public static SlackEnvelopeVerdict Judge(
        string? envelopeType,
        JsonObject? payload,
        string? teamId,
        string? appId,
        Func<string, bool> channelOwned,
        out SlackInboundEvent? inboundEvent,
        out SlackInboundInteraction? interaction)
    {
        ArgumentNullException.ThrowIfNull(channelOwned);
        inboundEvent = null;
        interaction = null;
        if (payload is null)
        {
            return SlackEnvelopeVerdict.DropMalformed;
        }

        return envelopeType switch
        {
            EventsApiType => JudgeEvent(payload, teamId, appId, channelOwned, out inboundEvent),
            InteractiveType => JudgeInteraction(payload, teamId, appId, channelOwned, out interaction),
            _ => SlackEnvelopeVerdict.DropUnhandledType,
        };
    }

    private static SlackEnvelopeVerdict JudgeEvent(JsonObject payload, string? teamId, string? appId, Func<string, bool> channelOwned, out SlackInboundEvent? inbound)
    {
        inbound = null;
        if (payload["event"] is not JsonObject ev)
        {
            return SlackEnvelopeVerdict.DropMalformed;
        }

        /* The bot filter runs first: the alert webhook's posts (subtype bot_message, a bot_id, no user) and the bot's own posts (a
           bot_id and the app's app_id) are most of a busy channel, and none of them is a person. */
        if (ev["bot_id"] is not null || string.Equals(Text(ev, "subtype"), "bot_message", StringComparison.Ordinal))
        {
            return SlackEnvelopeVerdict.DropBot;
        }

        if (string.IsNullOrWhiteSpace(teamId) || !string.Equals(Text(payload, "team_id"), teamId, StringComparison.Ordinal))
        {
            return SlackEnvelopeVerdict.DropTeam;
        }

        if (string.IsNullOrWhiteSpace(appId) || !string.Equals(Text(payload, "api_app_id"), appId, StringComparison.Ordinal))
        {
            return SlackEnvelopeVerdict.DropApp;
        }

        var type = Text(ev, "type");
        var channel = Text(ev, "channel");
        var ts = Text(ev, "ts");
        if (string.IsNullOrWhiteSpace(type) || string.IsNullOrWhiteSpace(channel) || string.IsNullOrWhiteSpace(ts))
        {
            return SlackEnvelopeVerdict.DropMalformed;
        }

        /* Every event that reaches here is a person's: the subscriptions are message.channels, message.groups and app_mention, and the
           bot filter above took every bot post. Slack names the author's workspace in user_team when it differs from the channel's host
           (a Slack Connect member), else in team. An event that names neither is dropped: a person's workspace that cannot be checked
           is not accepted. */
        var authorTeam = Text(ev, "user_team") ?? Text(ev, "team");
        if (string.IsNullOrWhiteSpace(authorTeam))
        {
            return SlackEnvelopeVerdict.DropMalformed;
        }

        if (!string.Equals(authorTeam, teamId, StringComparison.Ordinal))
        {
            return SlackEnvelopeVerdict.DropUserTeam;
        }

        if (!channelOwned(channel))
        {
            return SlackEnvelopeVerdict.DropChannel;
        }

        inbound = new SlackInboundEvent(teamId!, appId!, type, channel, ts, Text(ev, "thread_ts"), Text(ev, "user"), ev, payload);
        return SlackEnvelopeVerdict.Accept;
    }

    private static SlackEnvelopeVerdict JudgeInteraction(JsonObject payload, string? teamId, string? appId, Func<string, bool> channelOwned, out SlackInboundInteraction? inbound)
    {
        inbound = null;
        var team = payload["team"] as JsonObject;
        var user = payload["user"] as JsonObject;
        var container = payload["container"] as JsonObject;
        var payloadTeam = team is null ? null : Text(team, "id");
        if (string.IsNullOrWhiteSpace(teamId) || !string.Equals(payloadTeam, teamId, StringComparison.Ordinal))
        {
            return SlackEnvelopeVerdict.DropTeam;
        }

        if (string.IsNullOrWhiteSpace(appId) || !string.Equals(Text(payload, "api_app_id"), appId, StringComparison.Ordinal))
        {
            return SlackEnvelopeVerdict.DropApp;
        }

        var userId = user is null ? null : Text(user, "id");
        var userTeam = user is null ? null : Text(user, "team_id");
        if (string.IsNullOrWhiteSpace(userId) || string.IsNullOrWhiteSpace(userTeam))
        {
            return SlackEnvelopeVerdict.DropMalformed;
        }

        if (!string.Equals(userTeam, teamId, StringComparison.Ordinal))
        {
            return SlackEnvelopeVerdict.DropUserTeam;
        }

        var channel = container is null ? null : Text(container, "channel_id");
        if (string.IsNullOrWhiteSpace(channel) && payload["channel"] is JsonObject channelNode)
        {
            channel = Text(channelNode, "id");
        }

        var messageTs = container is null ? null : Text(container, "message_ts");
        if (string.IsNullOrWhiteSpace(channel) || string.IsNullOrWhiteSpace(messageTs))
        {
            return SlackEnvelopeVerdict.DropMalformed;
        }

        if (!channelOwned(channel))
        {
            return SlackEnvelopeVerdict.DropChannel;
        }

        inbound = new SlackInboundInteraction(teamId!, appId!, userId, userTeam, channel, messageTs, payload);
        return SlackEnvelopeVerdict.Accept;
    }

    /// <summary>The counter name of a verdict, as the listener's status and the diagnostics bundle show it.</summary>
    public static string Name(SlackEnvelopeVerdict verdict) => verdict switch
    {
        SlackEnvelopeVerdict.Accept => "accept",
        SlackEnvelopeVerdict.DropBot => "bot",
        SlackEnvelopeVerdict.DropTeam => "team",
        SlackEnvelopeVerdict.DropApp => "app",
        SlackEnvelopeVerdict.DropUserTeam => "user_team",
        SlackEnvelopeVerdict.DropChannel => "channel",
        SlackEnvelopeVerdict.DropDuplicate => "duplicate",
        SlackEnvelopeVerdict.DropMalformed => "malformed",
        _ => "unhandled_type",
    };

    private static string? Text(JsonObject node, string name) =>
        node[name] is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;
}

/// <summary>
/// #7657: the listener's duplicate check for events. Slack retries an event it thinks unacknowledged, with the same or a new envelope
/// id and on either connection, so the envelope id is no key; an event is one by its type, channel and ts. The set holds the newest
/// <see cref="Capacity"/> keys and forgets the oldest first: a retry comes within minutes, and the bound keeps a long-lived listener
/// from growing without end. A press is not deduplicated here: its state change is <c>WHERE state = 'pending'</c>, which makes a
/// repeat harmless.
/// </summary>
public sealed class SlackEventDedupe(int capacity = SlackEventDedupe.DefaultCapacity)
{
    /// <summary>How many keys are remembered.</summary>
    public const int DefaultCapacity = 4096;

    private readonly int _capacity = capacity < 1 ? 1 : capacity;
    private readonly HashSet<string> _keys = new(StringComparer.Ordinal);
    private readonly Queue<string> _order = new();
    private readonly object _lock = new();

    /// <summary>The event's key: its type, channel and ts.</summary>
    public static string KeyOf(SlackInboundEvent inbound)
    {
        ArgumentNullException.ThrowIfNull(inbound);
        return inbound.EventType + "|" + inbound.Channel + "|" + inbound.Ts;
    }

    /// <summary>True when <paramref name="key"/> was seen before; otherwise remembers it and returns false.</summary>
    public bool Seen(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        lock (_lock)
        {
            if (!_keys.Add(key))
            {
                return true;
            }

            _order.Enqueue(key);
            while (_order.Count > _capacity)
            {
                _keys.Remove(_order.Dequeue());
            }

            return false;
        }
    }

    /// <summary>How many keys are remembered now.</summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _keys.Count;
            }
        }
    }
}
