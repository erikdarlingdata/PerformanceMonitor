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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>What <see cref="PersonalTokenPendingRevokes.Add"/> did with an entry.</summary>
internal enum PersonalTokenPendingAdd
{
    /// <summary>The owner had no entry; it was queued.</summary>
    Queued,

    /// <summary>The owner already had an entry; the broader of the two stays, and at equal breadth the later reason replaces the earlier.</summary>
    Merged,

    /// <summary>The queue is full and the entry was dropped; this is the first drop, so the caller says so.</summary>
    DroppedFirst,

    /// <summary>The queue is full and the entry was dropped; the first drop was already reported.</summary>
    Dropped,
}

/// <summary>
/// The revokes a sign-in could not make (#8737): no owner connection yet, or the store write threw. They live in memory only and are drained from
/// the web supervisor tick, so a sign-in during a store outage keeps its session and the revoke lands on a later tick once the store answers. One
/// entry per owner key; an entry leaves only after its revoke commits. While an entry waits, the token check refuses that owner's tokens
/// (<see cref="Refuses"/>). With a state file, every change is saved to it (a temp file renamed over the old one) and the file is loaded when the
/// queue is built, so a restart keeps the queue. The file holds owner keys, the operator-only flag and reason codes, and no token or secret.
/// </summary>
internal sealed class PersonalTokenPendingRevokes
{
    /// <summary>The most owners queued at once. A full queue drops the newest entry, with one warning.</summary>
    internal const int MaxOwners = 10_000;

    /// <summary>One queued revoke: the owner, whether only operator tokens go, and the reason code the revoke journals.</summary>
    internal sealed record Entry(string OwnerKey, bool OperatorOnly, string Reason);

    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(System.StringComparer.Ordinal);
    private readonly int _max;
    private readonly string? _stateFile;
    private readonly ILogger? _logger;
    private bool _overflowReported;
    private bool _saveFailureLogged;

    // RED-RUN SHIMS (removed before the fix commit): the old behaviour behind the new names.
    internal const long MaxStateFileBytes = 4 * 1024 * 1024;

    internal void SaveIfDirty()
    {
    }

    internal static PersonalTokenPendingRevokes ForConfig(DarlingConfig config, string configPath, ILogger? logger) =>
        new(stateFile: Path.Combine(DarlingFileLoggerProvider.DefaultLogDirectory(), StateFileName), logger: logger);

    /// <summary>The role value of an operator token, the only role an operator-only revoke takes.</summary>
    private const string OperatorRole = "operator";

    /// <summary>The state file's name, kept beside the service logs.</summary>
    internal const string StateFileName = "personal-token-pending-revokes.json";

    /// <param name="maxOwners">The most owners queued at once.</param>
    /// <param name="stateFile">Where the queue is saved and loaded; null keeps it in memory only.</param>
    /// <param name="logger">Gets one warning for an unreadable state file, and one per run of failed saves.</param>
    internal PersonalTokenPendingRevokes(int maxOwners = MaxOwners, string? stateFile = null, ILogger? logger = null)
    {
        _max = maxOwners;
        _stateFile = stateFile;
        _logger = logger;
        Load();
    }

    /// <summary>True while the owner has a queued revoke that covers a token of <paramref name="role"/>: every role for a full revoke, only
    /// operator for an operator-only one. The token check calls this for each presented personal token.</summary>
    internal bool Refuses(string ownerKey, string role)
    {
        lock (_gate)
        {
            return _entries.TryGetValue(ownerKey, out var entry)
                && (!entry.OperatorOnly || string.Equals(role, OperatorRole, StringComparison.Ordinal));
        }
    }

    private void Load()
    {
        if (_stateFile is null)
        {
            return;
        }

        try
        {
            if (!File.Exists(_stateFile))
            {
                return;
            }

            var root = JsonNode.Parse(File.ReadAllText(_stateFile)) as JsonObject
                ?? throw new JsonException("The file is not a JSON object.");
            foreach (var item in root["entries"] as JsonArray ?? throw new JsonException("The file has no entries array."))
            {
                if (_entries.Count >= _max)
                {
                    break;
                }

                if (item is JsonObject o
                    && o["owner_key"] is JsonValue k && k.TryGetValue(out string? owner) && !string.IsNullOrEmpty(owner)
                    && o["operator_only"] is JsonValue f && f.TryGetValue(out bool operatorOnly)
                    && o["reason"] is JsonValue r && r.TryGetValue(out string? reason) && reason is not null)
                {
                    _entries[owner] = new Entry(owner, operatorOnly, reason);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or InvalidOperationException)
        {
            _entries.Clear();
            _logger?.LogWarning(ex, "The MCP personal token revoke queue file could not be read, so the queue starts empty: {File}", _stateFile);
        }
    }

    /* Called with _gate held, after a change. Writes a temp file and renames it over the old one. A failed save is a warning once per run of failures:
       the in-memory queue still holds and still refuses the tokens, only the restart copy is stale. */
    private void Save()
    {
        if (_stateFile is null)
        {
            return;
        }

        try
        {
            var dir = Path.GetDirectoryName(_stateFile);
            if (!string.IsNullOrEmpty(dir))
            {
                Directory.CreateDirectory(dir);
            }

            var array = new JsonArray();
            foreach (var e in _entries.Values)
            {
                array.Add(new JsonObject { ["owner_key"] = e.OwnerKey, ["operator_only"] = e.OperatorOnly, ["reason"] = e.Reason });
            }

            var temp = _stateFile + ".tmp";
            File.WriteAllText(temp, new JsonObject { ["entries"] = array }.ToJsonString());
            File.Move(temp, _stateFile, overwrite: true);
            _saveFailureLogged = false;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            if (!_saveFailureLogged)
            {
                _saveFailureLogged = true;
                _logger?.LogWarning(ex, "The MCP personal token revoke queue could not be saved, so a restart would lose it: {File}", _stateFile);
            }
        }
    }

    internal int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    /// <summary>Queues the revoke. For an owner that already has one, the later entry wins when it is as broad or broader (all roles over
    /// operator-only), and the reason comes from the winning entry, so at equal breadth the journal matches the sign-in that is current; a
    /// narrower later entry changes nothing.</summary>
    internal PersonalTokenPendingAdd Add(string ownerKey, bool operatorOnly, string reason)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(ownerKey, out var existing))
            {
                if (existing.OperatorOnly || !operatorOnly)
                {
                    _entries[ownerKey] = new Entry(ownerKey, operatorOnly, reason);
                    Save();
                }

                return PersonalTokenPendingAdd.Merged;
            }

            if (_entries.Count >= _max)
            {
                var first = !_overflowReported;
                _overflowReported = true;
                return first ? PersonalTokenPendingAdd.DroppedFirst : PersonalTokenPendingAdd.Dropped;
            }

            _entries[ownerKey] = new Entry(ownerKey, operatorOnly, reason);
            Save();
            return PersonalTokenPendingAdd.Queued;
        }
    }

    /// <summary>Drops the owner's entry: a later sign-in left nothing to revoke, so the queued revoke is moot.</summary>
    internal void Remove(string ownerKey)
    {
        lock (_gate)
        {
            if (_entries.Remove(ownerKey))
            {
                Save();
                if (_entries.Count < _max)
                {
                    _overflowReported = false;
                }
            }
        }
    }

    /// <summary>The queued entries now, for one drain pass.</summary>
    internal IReadOnlyList<Entry> Snapshot()
    {
        lock (_gate)
        {
            return _entries.Values.ToList();
        }
    }

    /// <summary>Removes the owner's entry after its revoke committed, but only if it is still the entry that was revoked: a broader one that
    /// replaced it meanwhile stays for the next pass.</summary>
    internal void Complete(Entry done)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(done.OwnerKey, out var current) && ReferenceEquals(current, done))
            {
                _entries.Remove(done.OwnerKey);
                Save();
                if (_entries.Count < _max)
                {
                    _overflowReported = false;
                }
            }
        }
    }
}
