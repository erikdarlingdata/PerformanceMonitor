/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7657: what the listener does with one acknowledged envelope.</summary>
public enum SlackEnvelopeVerdict
{
    Accept,
    DropBot,
    DropTeam,
    DropApp,
    DropUserTeam,
    DropChannel,
    DropDuplicate,
    DropMalformed,
    DropUnhandledType,
}

/// <summary>#7657: the listener's filters. Not built yet: everything is accepted and nothing is handed on.</summary>
public static class SlackEnvelopeFilter
{
    public const string EventsApiType = "events_api";
    public const string InteractiveType = "interactive";

    public static SlackEnvelopeVerdict Judge(
        string? envelopeType,
        JsonObject? payload,
        string? teamId,
        string? appId,
        Func<string, bool> channelOwned,
        out SlackInboundEvent? inboundEvent,
        out SlackInboundInteraction? interaction)
    {
        inboundEvent = null;
        interaction = null;
        return SlackEnvelopeVerdict.Accept;
    }

    public static string Name(SlackEnvelopeVerdict verdict) => "";
}

/// <summary>#7657: the duplicate check. Not built yet: nothing is remembered.</summary>
public sealed class SlackEventDedupe(int capacity = SlackEventDedupe.DefaultCapacity)
{
    public const int DefaultCapacity = 4096;

    public int Capacity { get; } = capacity;

    public static string KeyOf(SlackInboundEvent inbound) => "";

    public bool Seen(string key) => false;

    public int Count => 0;
}
