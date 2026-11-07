/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service.Chat;

public sealed record SlackListenerSnapshot(
    bool Running,
    int ConnectionsOpen,
    int ConnectionsWanted,
    bool IdsLearned,
    DateTime? LastHelloUtc,
    string? LastDisconnectReason,
    DateTime? LastDisconnectUtc,
    long Received,
    long Acked,
    long Dispatched,
    long DroppedByFilter,
    IReadOnlyDictionary<string, long> DroppedByReason,
    int QueueDepth,
    long QueueDropped,
    DateTime? LastEventUtc,
    string? LastError,
    DateTime? LastErrorUtc);

/// <summary>#7657: the listener's counters. Not built yet: nothing is counted.</summary>
public sealed class SlackListenerStatus(Func<DateTime>? nowUtc = null)
{
    private readonly Func<DateTime> _now = nowUtc ?? (static () => DateTime.UtcNow);

    public Func<int>? QueueDepthSource { get; set; }

    public DateTime Now => _now();

    public void Started(int connectionsWanted) { }
    public void Stopped() { }
    public void IdsLearned() { }
    public void ConnectionOpened() { }
    public void ConnectionClosed(string reason) { }
    public void ConnectFailed(string error) { }
    public void Received() { }
    public void Acked() { }
    public void Dropped(string reason) { }
    public void QueueDropped() { }
    public void Dispatched() { }

    public SlackListenerSnapshot Read() =>
        new(false, 0, 0, false, null, null, null, 0, 0, 0, 0, new Dictionary<string, long>(), 0, 0, null, null, null);

    public static JsonObject ToJson(SlackListenerSnapshot snapshot) => new()
    {
        ["running"] = false,
        ["connections_open"] = 0,
        ["connections_wanted"] = 0,
        ["ids_learned"] = false,
        ["last_hello_at"] = null,
        ["last_disconnect_reason"] = null,
        ["last_disconnect_at"] = null,
        ["received"] = 0L,
        ["acked"] = 0L,
        ["dispatched"] = 0L,
        ["dropped_by_filter"] = 0L,
        ["dropped_by_reason"] = new JsonObject(),
        ["queue_depth"] = 0,
        ["queue_dropped"] = 0L,
        ["last_event_at"] = null,
        ["last_error"] = null,
        ["last_error_at"] = null,
    };

    public static string Describe(SlackListenerSnapshot snapshot) => "";
}

/// <summary>#7657: the status file. Not built yet: nothing is written and nothing is found.</summary>
public static class SlackListenerStatusFile
{
    public const string FileName = "slack-listener-status.json";
    public const string SectionName = "slack_listener";

    public static string PathUnder(string directory) => Path.Combine(directory, FileName);

    public static string? Write(string directory, SlackListenerSnapshot snapshot, DateTime writtenUtc) => null;

    public static void Delete(string directory) { }

    public static JsonObject ReadSection(string directory) => new() { ["status"] = "absent" };
}
