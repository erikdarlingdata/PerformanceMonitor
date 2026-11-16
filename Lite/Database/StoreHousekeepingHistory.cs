/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>One thing Lite's housekeeping did (or failed to do) to its own store (#8788).</summary>
/// <param name="TimeUtc">When the pass ended.</param>
/// <param name="Kind">One of <see cref="StoreHousekeepingHistory.KindArchive"/>, <see cref="StoreHousekeepingHistory.KindSizeReset"/>,
/// <see cref="StoreHousekeepingHistory.KindRetention"/>.</param>
/// <param name="Outcome">One of <see cref="StoreHousekeepingHistory.OutcomeOk"/>, <see cref="StoreHousekeepingHistory.OutcomeIncomplete"/>,
/// <see cref="StoreHousekeepingHistory.OutcomeFailed"/>.</param>
/// <param name="Rows">Rows moved to the archive, when the pass moves rows.</param>
/// <param name="Files">Archive files written (archive and size reset) or deleted (retention).</param>
/// <param name="UsedMb">Data in the store when the pass started, in MB. Null when it could not be read.</param>
/// <param name="Message">The error text of a failure, or a short note on an incomplete pass.</param>
/// <param name="Count">How many identical passes this entry stands for (see <see cref="StoreHousekeepingHistory.Record"/>).</param>
public sealed record StoreHousekeepingEvent(
    DateTime TimeUtc,
    string Kind,
    string Outcome,
    long? Rows = null,
    int? Files = null,
    double? UsedMb = null,
    string? Message = null,
    int Count = 1);

/// <summary>
/// Lite's housekeeping history (#8788): the last <see cref="MaxEvents"/> timed archives, size resets and retention cleanups,
/// each with its outcome, kept in <c>store-health-history.json</c> at the data root beside <c>alert_state.json</c>.
///
/// <para><b>Why a file and not a table.</b> The 512 MB size reset deletes <c>monitor.duckdb</c> and builds a fresh one, and the
/// tables it restores afterwards are a fixed list (<c>ArchiveService.PreservedConfigTables</c>). A history kept in the store
/// would be one more table to carry through that reset, and a failed pass could not be written to a store that is the thing
/// failing. A file outside the store is not touched by the reset and needs no lock on it, so a reset's own record is written
/// the same way as any other.</para>
///
/// <para><b>Bounded.</b> A pass that did nothing, and a failure that repeats with the same message, replace the newest entry
/// of their kind and raise its <see cref="StoreHousekeepingEvent.Count"/>. Without that, the hourly archive pass that finds
/// nothing old, or a full disk that fails the size reset every 15 minutes, would push every other entry out of the 200 within
/// days.</para>
///
/// <para>Best effort: a read of a missing or damaged file is an empty history, and a write that fails is logged and dropped.
/// The history is a diagnostic aid and must never fail the pass it describes.</para>
/// </summary>
public sealed class StoreHousekeepingHistory
{
    public const string FileName = "store-health-history.json";
    public const int MaxEvents = 200;

    public const string KindArchive = "archive";
    public const string KindSizeReset = "size_reset";
    public const string KindRetention = "retention";

    public const string OutcomeOk = "ok";
    public const string OutcomeIncomplete = "incomplete";
    public const string OutcomeFailed = "failed";

    private const string LogSource = "StoreHealth";

    /* One lock for every instance: the archive service, the retention service and the window's reader each hold their own
       object over the same file. */
    private static readonly object s_fileLock = new();

    private static readonly JsonSerializerOptions s_options = new() { WriteIndented = true };

    private readonly string _filePath;

    public StoreHousekeepingHistory(string filePath)
    {
        _filePath = filePath;
    }

    /// <summary>The history file that belongs to a store: beside the database file, in the data root.</summary>
    public static string PathFor(string databasePath) =>
        Path.Combine(Path.GetDirectoryName(databasePath) ?? ".", FileName);

    public string FilePath => _filePath;

    /// <summary>Adds one entry at the head, folding it into the newest entry of its kind when it repeats that one, and trims to
    /// <see cref="MaxEvents"/>. Never throws.</summary>
    public void Record(StoreHousekeepingEvent entry)
    {
        try
        {
            lock (s_fileLock)
            {
                var events = ReadUnlocked();
                var previous = events.FindIndex(e => e.Kind == entry.Kind);
                if (previous >= 0 && Repeats(events[previous], entry))
                {
                    var folded = entry with { Count = events[previous].Count + entry.Count };
                    events.RemoveAt(previous);
                    events.Insert(0, folded);
                }
                else
                {
                    events.Insert(0, entry);
                }

                if (events.Count > MaxEvents)
                {
                    events.RemoveRange(MaxEvents, events.Count - MaxEvents);
                }

                var temp = _filePath + ".tmp";
                File.WriteAllText(temp, JsonSerializer.Serialize(events, s_options));
                File.Move(temp, _filePath, overwrite: true);
            }
        }
        catch (Exception ex)
        {
            AppLogger.Warn(LogSource, $"Could not record the housekeeping history entry: {ex.Message}");
        }
    }

    /// <summary>The history, newest first. Empty when the file is missing or cannot be read.</summary>
    public IReadOnlyList<StoreHousekeepingEvent> Read()
    {
        lock (s_fileLock)
        {
            return ReadUnlocked();
        }
    }

    private List<StoreHousekeepingEvent> ReadUnlocked()
    {
        try
        {
            if (!File.Exists(_filePath))
            {
                return [];
            }

            var events = JsonSerializer.Deserialize<List<StoreHousekeepingEvent>>(File.ReadAllText(_filePath), s_options);
            return events?
                .Where(e => e is not null && !string.IsNullOrEmpty(e.Kind))
                .OrderByDescending(e => e.TimeUtc)
                .ToList() ?? [];
        }
        catch (Exception ex)
        {
            AppLogger.Warn(LogSource, $"Could not read the housekeeping history; it is treated as empty: {ex.Message}");
            return [];
        }
    }

    /// <summary>Whether <paramref name="entry"/> says nothing the newest entry of its kind does not: two passes that did no work,
    /// or two failures with the same message.</summary>
    private static bool Repeats(StoreHousekeepingEvent previous, StoreHousekeepingEvent entry)
    {
        if (previous.Outcome != entry.Outcome)
        {
            return false;
        }

        if (entry.Outcome == OutcomeFailed)
        {
            return string.Equals(previous.Message, entry.Message, StringComparison.Ordinal);
        }

        return entry.Outcome == OutcomeOk
            && (previous.Rows ?? 0) == 0 && (previous.Files ?? 0) == 0
            && (entry.Rows ?? 0) == 0 && (entry.Files ?? 0) == 0;
    }
}
