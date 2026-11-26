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
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
/// The revokes a sign-in could not make (#8737): no owner connection yet, or the store write threw. They are drained from
/// the web supervisor tick, so a sign-in during a store outage keeps its session and the revoke lands on a later tick once the store answers. One
/// entry per owner key; an entry leaves only after its revoke commits. While an entry waits, the token check refuses that owner's tokens
/// (<see cref="Refuses"/>). With a state file, every change is saved to it (a flushed temp file renamed over the old one) and the file is loaded
/// when the queue is built, so a restart keeps the queue. A save that fails is tried again on the next supervisor tick (<see cref="SaveIfDirty"/>).
/// The file holds owner keys, the operator-only flag and reason codes, and no token or secret.
/// </summary>
internal sealed class PersonalTokenPendingRevokes
{
    /// <summary>The most owners queued at once. A full queue drops the newest entry, with one warning.</summary>
    internal const int MaxOwners = 10_000;

    /// <summary>One queued revoke: the owner, whether only operator tokens go, and the reason code the revoke journals.</summary>
    internal sealed record Entry(string OwnerKey, bool OperatorOnly, string Reason);

    /// <summary>Guards the entries and their version. Held only for in-memory work, never for disk I/O.</summary>
    private readonly object _gate = new();

    /// <summary>Held while the file is written, so two saves never run at once. The token check never takes it.</summary>
    private readonly object _saveGate = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly int _max;
    private readonly string? _stateFile;
    private readonly ILogger? _logger;
    private bool _overflowReported;
    private bool _saveFailureLogged;

    /// <summary>The entries as of the last change, replaced whole and never edited once published, so the token check reads it without a lock
    /// and never waits for a file write.</summary>
    private volatile IReadOnlyDictionary<string, Entry> _published = new Dictionary<string, Entry>(0, StringComparer.Ordinal);

    /// <summary>Counts the published changes. Read and written under <see cref="_gate"/>.</summary>
    private long _version;

    /// <summary>The version the file holds. Read and written under <see cref="_saveGate"/>.</summary>
    private long _savedVersion;

    /// <summary>True after a save failed and until a later one succeeds, so <see cref="SaveIfDirty"/> knows the file is behind.</summary>
    private volatile bool _dirty;

    /// <summary>The role value of an operator token, the only role an operator-only revoke takes.</summary>
    private const string OperatorRole = "operator";

    /// <summary>The state file's name, kept in the folder that holds the service's keys.</summary>
    internal const string StateFileName = "personal-token-pending-revokes.json";

    /// <summary>The most bytes a state file may hold. A full queue is about 1 MB, so a larger file is not one this service wrote.</summary>
    internal const long MaxStateFileBytes = 4 * 1024 * 1024;

    /// <param name="maxOwners">The most owners queued at once.</param>
    /// <param name="stateFile">Where the queue is saved and loaded; null keeps it in memory only.</param>
    /// <param name="logger">Gets one warning for an unreadable state file, and one per run of failed saves.</param>
    internal PersonalTokenPendingRevokes(int maxOwners = MaxOwners, string? stateFile = null, ILogger? logger = null)
    {
        _max = maxOwners;
        _stateFile = stateFile;
        _logger = logger;
        Load();
        _published = new Dictionary<string, Entry>(_entries, StringComparer.Ordinal);
    }

    /// <summary>
    /// The queue for this service: its file sits in the folder that holds the service's keys (<see cref="DarlingLogHashKeyFile.DirectoryFor"/>),
    /// which a restart keeps on every distribution and which gets the folder checks the key files get. A folder those checks refuse, or one that
    /// cannot be worked out, leaves the queue in memory only, with one warning.
    /// </summary>
    internal static PersonalTokenPendingRevokes ForConfig(DarlingConfig config, string configPath, ILogger? logger)
    {
        string? file = null;
        try
        {
            var directory = DarlingLogHashKeyFile.DirectoryFor(config, configPath);
            var problem = DarlingManagedRoles.PrepareComposeCredentialDirectory(directory, create: true, logger ?? NullLogger.Instance).Distrust;
            if (problem is null && OperatingSystem.IsWindows())
            {
                problem = DarlingFileSecurity.LogFolderProblem(directory);
            }

            if (problem is null)
            {
                file = Path.Combine(directory, StateFileName);
            }
            else
            {
                var folderNote = DarlingLogHashKeyFile.ContainerFolderNote(config);
                logger?.LogWarning(
                    "The MCP personal token revoke queue is kept in memory only, so a restart would lose it: {Folder} is not used because {Problem}.{FolderNote}",
                    directory, problem, folderNote is null ? "" : " " + folderNote);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "The MCP personal token revoke queue is kept in memory only, so a restart would lose it: its folder could not be worked out.");
        }

        return new PersonalTokenPendingRevokes(stateFile: file, logger: logger);
    }

    /// <summary>True while the owner has a queued revoke that covers a token of <paramref name="role"/>: every role for a full revoke, only
    /// operator for an operator-only one. The token check calls this for each presented personal token. It takes no lock and does no I/O.</summary>
    internal bool Refuses(string ownerKey, string role) =>
        _published.TryGetValue(ownerKey, out var entry)
            && (!entry.OperatorOnly || string.Equals(role, OperatorRole, StringComparison.Ordinal));

    private void Load()
    {
        if (_stateFile is null)
        {
            return;
        }

        try
        {
            var info = new FileInfo(_stateFile);
            if (!info.Exists)
            {
                return;
            }

            if (info.Length > MaxStateFileBytes)
            {
                throw new InvalidDataException($"The file is larger than {MaxStateFileBytes} bytes.");
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Whatever is wrong with the file (a read error, bad JSON, a repeated key, a file too large), the service still starts, with no queue. */
            _entries.Clear();
            _logger?.LogWarning(ex, "The MCP personal token revoke queue file could not be read, so the queue starts empty: {File}", _stateFile);
        }
    }

    /* Called after a change, with _gate NOT held: writes the entries as they were at `version` to a temp file, flushes it to disk, and renames it
       over the old one. Saves run one at a time, and one whose version is older than what the file already holds is skipped, so the newest
       copy always lands. A failed save is a warning once per run of failures, and sets the dirty flag so SaveIfDirty tries again: the in-memory
       queue still holds and still refuses the tokens, only the restart copy is stale. */
    private void Save(IReadOnlyDictionary<string, Entry> entries, long version)
    {
        if (_stateFile is null)
        {
            return;
        }

        lock (_saveGate)
        {
            if (version <= _savedVersion)
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
                foreach (var e in entries.Values)
                {
                    array.Add(new JsonObject { ["owner_key"] = e.OwnerKey, ["operator_only"] = e.OperatorOnly, ["reason"] = e.Reason });
                }

                var temp = _stateFile + ".tmp";
                var bytes = Encoding.UTF8.GetBytes(new JsonObject { ["entries"] = array }.ToJsonString());
                using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    stream.Write(bytes, 0, bytes.Length);
                    stream.Flush(flushToDisk: true);
                }

                File.Move(temp, _stateFile, overwrite: true);
                _savedVersion = version;
                _dirty = false;
                _saveFailureLogged = false;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _dirty = true;
                if (!_saveFailureLogged)
                {
                    _saveFailureLogged = true;
                    _logger?.LogWarning(ex, "The MCP personal token revoke queue could not be saved, so a restart would lose it: {File}", _stateFile);
                }
            }
        }
    }

    /// <summary>Called with <see cref="_gate"/> held, after a change: publishes the entries for the token check and returns the copy and its
    /// version for <see cref="Save"/>, which the caller runs after it leaves the lock.</summary>
    private (IReadOnlyDictionary<string, Entry> Entries, long Version) Publish()
    {
        var copy = new Dictionary<string, Entry>(_entries, StringComparer.Ordinal);
        _published = copy;
        return (copy, ++_version);
    }

    /// <summary>Writes the file again when an earlier save failed and none has succeeded since. The supervisor calls it on every tick, and the
    /// web host once as it stops, so a full disk that is freed, or a file that was held open, does not leave the restart copy behind.</summary>
    internal void SaveIfDirty()
    {
        if (!_dirty)
        {
            return;
        }

        IReadOnlyDictionary<string, Entry> entries;
        long version;
        lock (_gate)
        {
            entries = _published;
            version = _version;
        }

        Save(entries, version);
    }

    internal int Count => _published.Count;

    /// <summary>Queues the revoke. For an owner that already has one, the later entry wins when it is as broad or broader (all roles over
    /// operator-only), and the reason comes from the winning entry, so at equal breadth the journal matches the sign-in that is current; a
    /// narrower later entry changes nothing.</summary>
    internal PersonalTokenPendingAdd Add(string ownerKey, bool operatorOnly, string reason)
    {
        PersonalTokenPendingAdd result;
        (IReadOnlyDictionary<string, Entry> Entries, long Version)? changed = null;
        lock (_gate)
        {
            if (_entries.TryGetValue(ownerKey, out var existing))
            {
                result = PersonalTokenPendingAdd.Merged;
                if (existing.OperatorOnly || !operatorOnly)
                {
                    _entries[ownerKey] = new Entry(ownerKey, operatorOnly, reason);
                    changed = Publish();
                }
            }
            else if (_entries.Count >= _max)
            {
                var first = !_overflowReported;
                _overflowReported = true;
                result = first ? PersonalTokenPendingAdd.DroppedFirst : PersonalTokenPendingAdd.Dropped;
            }
            else
            {
                _entries[ownerKey] = new Entry(ownerKey, operatorOnly, reason);
                changed = Publish();
                result = PersonalTokenPendingAdd.Queued;
            }
        }

        if (changed is { } c)
        {
            Save(c.Entries, c.Version);
        }

        return result;
    }

    /// <summary>Drops the owner's entry: a later sign-in left nothing to revoke, so the queued revoke is moot.</summary>
    internal void Remove(string ownerKey)
    {
        (IReadOnlyDictionary<string, Entry> Entries, long Version)? changed = null;
        lock (_gate)
        {
            if (_entries.Remove(ownerKey))
            {
                changed = Publish();
                if (_entries.Count < _max)
                {
                    _overflowReported = false;
                }
            }
        }

        if (changed is { } c)
        {
            Save(c.Entries, c.Version);
        }
    }

    /// <summary>The queued entries now, for one drain pass.</summary>
    internal IReadOnlyList<Entry> Snapshot() => _published.Values.ToList();

    /// <summary>Removes the owner's entry after its revoke committed, but only if it is still the entry that was revoked: a broader one that
    /// replaced it meanwhile stays for the next pass.</summary>
    internal void Complete(Entry done)
    {
        (IReadOnlyDictionary<string, Entry> Entries, long Version)? changed = null;
        lock (_gate)
        {
            if (_entries.TryGetValue(done.OwnerKey, out var current) && ReferenceEquals(current, done))
            {
                _entries.Remove(done.OwnerKey);
                changed = Publish();
                if (_entries.Count < _max)
                {
                    _overflowReported = false;
                }
            }
        }

        if (changed is { } c)
        {
            Save(c.Entries, c.Version);
        }
    }
}
