/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7657: one coherent reading of the listener's health. Times are UTC; a null time means never. No channel id, no user id
/// and no token is in it: the counts and reasons are the whole content, so the diagnostics bundle carries it as is.</summary>
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

/// <summary>
/// #7657: the listener's counters, written by its connection and consumer tasks and read as one <see cref="SlackListenerSnapshot"/>
/// by the host's supervisor, which logs them and writes them for the diagnostics bundle. The clock is injected so a test pins the
/// times it records.
/// </summary>
public sealed class SlackListenerStatus(Func<DateTime>? nowUtc = null)
{
    private readonly Func<DateTime> _now = nowUtc ?? (static () => DateTime.UtcNow);
    private readonly object _lock = new();
    private readonly Dictionary<string, long> _drops = new(StringComparer.Ordinal);
    private bool _running;
    private int _connectionsOpen;
    private int _connectionsWanted;
    private bool _idsLearned;
    private DateTime? _lastHello;
    private string? _lastDisconnectReason;
    private DateTime? _lastDisconnect;
    private long _received;
    private long _acked;
    private long _dispatched;
    private long _droppedByFilter;
    private long _queueDropped;
    private DateTime? _lastEvent;
    private string? _lastError;
    private DateTime? _lastErrorAt;

    /// <summary>Where the queue's depth is read from; the listener sets it to its channel's count.</summary>
    public Func<int>? QueueDepthSource { get; set; }

    /// <summary>The listener started with <paramref name="connectionsWanted"/> connections to hold.</summary>
    public void Started(int connectionsWanted)
    {
        lock (_lock)
        {
            _running = true;
            _connectionsWanted = connectionsWanted;
        }
    }

    /// <summary>The listener stopped: no connection is open.</summary>
    public void Stopped()
    {
        lock (_lock)
        {
            _running = false;
            _connectionsOpen = 0;
        }
    }

    /// <summary>The workspace id (and the app id) are known, so payloads can be judged.</summary>
    public void IdsLearned()
    {
        lock (_lock)
        {
            _idsLearned = true;
        }
    }

    /// <summary>One connection received Slack's <c>hello</c>.</summary>
    public void ConnectionOpened()
    {
        lock (_lock)
        {
            _connectionsOpen++;
            _lastHello = _now();
        }
    }

    /// <summary>One connection closed, for <paramref name="reason"/> (Slack's <c>disconnect</c> reason, or what went wrong).</summary>
    public void ConnectionClosed(string reason)
    {
        lock (_lock)
        {
            if (_connectionsOpen > 0)
            {
                _connectionsOpen--;
            }

            _lastDisconnectReason = reason;
            _lastDisconnect = _now();
        }
    }

    /// <summary>A connect attempt failed with <paramref name="error"/> (never a token: the transport redacts).</summary>
    public void ConnectFailed(string error)
    {
        lock (_lock)
        {
            _lastError = error;
            _lastErrorAt = _now();
        }
    }

    /// <summary>One frame arrived.</summary>
    public void Received()
    {
        lock (_lock)
        {
            _received++;
        }
    }

    /// <summary>One acknowledgement went out.</summary>
    public void Acked()
    {
        lock (_lock)
        {
            _acked++;
        }
    }

    /// <summary>A payload was dropped by the filter named <paramref name="reason"/>.</summary>
    public void Dropped(string reason)
    {
        lock (_lock)
        {
            _droppedByFilter++;
            _drops[reason] = _drops.TryGetValue(reason, out var n) ? n + 1 : 1;
        }
    }

    /// <summary>The queue was full and an older item was dropped to make room.</summary>
    public void QueueDropped()
    {
        lock (_lock)
        {
            _queueDropped++;
        }
    }

    /// <summary>A payload passed every filter and reached a handler.</summary>
    public void Dispatched()
    {
        lock (_lock)
        {
            _dispatched++;
            _lastEvent = _now();
        }
    }

    /// <summary>One coherent reading.</summary>
    public SlackListenerSnapshot Read()
    {
        lock (_lock)
        {
            var depth = 0;
            try
            {
                depth = QueueDepthSource?.Invoke() ?? 0;
            }
            catch (Exception)
            {
                /* A channel that was completed answers nothing; the depth reads as zero. */
            }

            return new SlackListenerSnapshot(
                _running, _connectionsOpen, _connectionsWanted, _idsLearned, _lastHello, _lastDisconnectReason, _lastDisconnect,
                _received, _acked, _dispatched, _droppedByFilter, new Dictionary<string, long>(_drops, StringComparer.Ordinal),
                depth, _queueDropped, _lastEvent, _lastError, _lastErrorAt);
        }
    }

    /// <summary>PURE: the snapshot as the diagnostics bundle's block and the status file's content.</summary>
    public static JsonObject ToJson(SlackListenerSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var drops = new JsonObject();
        foreach (var pair in snapshot.DroppedByReason.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            drops[pair.Key] = pair.Value;
        }

        return new JsonObject
        {
            ["running"] = snapshot.Running,
            ["connections_open"] = snapshot.ConnectionsOpen,
            ["connections_wanted"] = snapshot.ConnectionsWanted,
            ["ids_learned"] = snapshot.IdsLearned,
            ["last_hello_at"] = Stamp(snapshot.LastHelloUtc),
            ["last_disconnect_reason"] = snapshot.LastDisconnectReason,
            ["last_disconnect_at"] = Stamp(snapshot.LastDisconnectUtc),
            ["received"] = snapshot.Received,
            ["acked"] = snapshot.Acked,
            ["dispatched"] = snapshot.Dispatched,
            ["dropped_by_filter"] = snapshot.DroppedByFilter,
            ["dropped_by_reason"] = drops,
            ["queue_depth"] = snapshot.QueueDepth,
            ["queue_dropped"] = snapshot.QueueDropped,
            ["last_event_at"] = Stamp(snapshot.LastEventUtc),
            ["last_error"] = snapshot.LastError,
            ["last_error_at"] = Stamp(snapshot.LastErrorUtc),
        };
    }

    /// <summary>PURE: one log line's worth of the snapshot.</summary>
    public static string Describe(SlackListenerSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return string.Format(
            CultureInfo.InvariantCulture,
            "connections {0}/{1}, received {2}, acked {3}, dispatched {4}, dropped by filter {5}, queue depth {6}, queue dropped {7}, last event {8}, last disconnect {9} at {10}",
            snapshot.ConnectionsOpen, snapshot.ConnectionsWanted, snapshot.Received, snapshot.Acked, snapshot.Dispatched, snapshot.DroppedByFilter,
            snapshot.QueueDepth, snapshot.QueueDropped, Stamp(snapshot.LastEventUtc) ?? "never", snapshot.LastDisconnectReason ?? "none", Stamp(snapshot.LastDisconnectUtc) ?? "never");
    }

    private static string? Stamp(DateTime? utc) => utc?.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
}

/// <summary>
/// #7657: the listener's status as a file beside the service log, so the diagnostics bundle (a separate process) reads what the
/// service holds in memory. The host writes it when the snapshot changes and once more at stop; the bundle reads it into its
/// <c>slack_listener</c> section. A missing file means the listener never ran on this host, or the service is older.
/// </summary>
public static class SlackListenerStatusFile
{
    /// <summary>The file's name under the log directory.</summary>
    public const string FileName = "slack-listener-status.json";

    /// <summary>The bundle section's name.</summary>
    public const string SectionName = "slack_listener";

    private static readonly JsonSerializerOptions s_write = new() { WriteIndented = true };

    /// <summary>The file's path under <paramref name="directory"/>.</summary>
    public static string PathUnder(string directory) => Path.Combine(directory, FileName);

    /// <summary>Writes <paramref name="snapshot"/> under <paramref name="directory"/>, whole or not at all (a temp file moved into
    /// place). Never throws: a status file nobody can write is not the listener's problem, and the caller logs the failure.</summary>
    public static string? Write(string directory, SlackListenerSnapshot snapshot, DateTime writtenUtc)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        try
        {
            Directory.CreateDirectory(directory);
            var node = SlackListenerStatus.ToJson(snapshot);
            node["written_at"] = writtenUtc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
            var path = PathUnder(directory);
            var temp = path + ".tmp";
            File.WriteAllText(temp, node.ToJsonString(s_write));
            File.Move(temp, path, overwrite: true);
            return null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or System.Security.SecurityException)
        {
            return ex.GetType().Name + ": " + ex.Message;
        }
    }

    /// <summary>Removes the file, so a bundle after an uninstall of the listener does not show a stale reading. Never throws.</summary>
    public static void Delete(string directory)
    {
        try
        {
            File.Delete(PathUnder(directory));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Nothing to do: a bundle shows the last reading with its written_at. */
        }
    }

    /// <summary>The bundle's <c>slack_listener</c> section: the file's content under <c>status: ok</c>, <c>status: absent</c> when
    /// there is no file, or <c>status: error</c> with the error's class when it cannot be read.</summary>
    public static JsonObject ReadSection(string directory)
    {
        var path = PathUnder(directory);
        if (!File.Exists(path))
        {
            return new JsonObject { ["status"] = "absent", ["note"] = "no listener status file: the Socket Mode listener has not run on this host" };
        }

        try
        {
            var node = JsonNode.Parse(File.ReadAllText(path)) as JsonObject;
            if (node is null)
            {
                return new JsonObject { ["status"] = "error", ["error_class"] = "NotAnObject" };
            }

            node["status"] = "ok";
            return node;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
        {
            return new JsonObject { ["status"] = "error", ["error_class"] = ex.GetType().Name };
        }
    }
}
