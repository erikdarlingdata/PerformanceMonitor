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
/// <param name="Compaction">What a Parquet compaction pass did (#9422). Set only on a <see cref="StoreHousekeepingHistory.KindCompaction"/> entry.</param>
public sealed record StoreHousekeepingEvent(
    DateTime TimeUtc,
    string Kind,
    string Outcome,
    long? Rows = null,
    int? Files = null,
    double? UsedMb = null,
    string? Message = null,
    int Count = 1,
    CompactionPassDetail? Compaction = null);

/// <summary>
/// One Parquet compaction pass that merged, re-cut or failed (#9422, #9424). Every figure is for that pass alone.
/// </summary>
/// <param name="StartUtc">When the pass started. The entry's own time is when it ended.</param>
/// <param name="Seconds">How long the pass ran.</param>
/// <param name="GroupsMerged">Month or day groups whose merged files replaced their inputs.</param>
/// <param name="FilesIn">Archive files the merges read and removed.</param>
/// <param name="FilesOut">Archive files the merges wrote.</param>
/// <param name="BytesIn">Bytes of the files read.</param>
/// <param name="BytesOut">Bytes of the files written.</param>
/// <param name="SortedAtSmallGroupBatches">Batches whose sorted merge ran out of memory at the large row-group size and were sorted again in 2,048-row groups.</param>
/// <param name="UnsortedFallbackBatches">Batches that ran out of memory even then and were written in the unsorted order.</param>
/// <param name="FilesRecut">Files the one-time re-cut rewrote sorted by server.</param>
/// <param name="RecutSkippedLiveJournal">Files due for the re-cut whose swap journal was still live.</param>
/// <param name="RecutSkippedTimeBudget">Files due for the re-cut that the pass's time budget left for the next pass.</param>
/// <param name="RecutSkips">Files the re-cut left alone in this pass, counted by reason name (the values of <c>ArchiveService.RecutSkipReason</c>). A reason with no file is absent.</param>
/// <param name="ViewRebuildMs">Milliseconds the pass spent rebuilding the archive views after its swaps.</param>
/// <param name="ProcessPeakWorkingSetBytes">The highest working set of the whole Lite process, sampled every 75 ms while the pass ran. Collection and the UI share the process, so it can include their memory.</param>
/// <param name="DuckDbPeakMemoryBytes">The highest memory DuckDB reported for the merge connections (duckdb_memory(), sampled every 50 ms). Null when no merge ran or nothing was sampled.</param>
/// <param name="MemorySource">Where the two memory figures came from, in words.</param>
/// <param name="RecutSkippedTimeBudgetIsLowerBound">True when counting <paramref name="RecutSkippedTimeBudget"/> stopped at its short time cap, so the real number is at least that many.</param>
public sealed record CompactionPassDetail(
    DateTime StartUtc,
    double Seconds,
    int GroupsMerged = 0,
    int FilesIn = 0,
    int FilesOut = 0,
    long BytesIn = 0,
    long BytesOut = 0,
    int SortedAtSmallGroupBatches = 0,
    int UnsortedFallbackBatches = 0,
    int FilesRecut = 0,
    int RecutSkippedLiveJournal = 0,
    int RecutSkippedTimeBudget = 0,
    Dictionary<string, int>? RecutSkips = null,
    long ViewRebuildMs = 0,
    long? ProcessPeakWorkingSetBytes = null,
    long? DuckDbPeakMemoryBytes = null,
    string? MemorySource = null,
    bool RecutSkippedTimeBudgetIsLowerBound = false);

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
/// days. A compaction pass that only merged folds the same way into the newest routine compaction entry, which keeps the newest
/// pass's figures (#9448).</para>
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
    public const string KindCompaction = "compaction";

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

        if (entry.Kind == KindCompaction)
        {
            /* A compaction entry is written only for a pass that merged or re-cut something, so none is idle. The hourly archive
               pass that exported rows is followed by a pass that merges them into the month or day file, and without a fold that
               doubles the rows per hour and the 200 reach back half as far (#9448). A routine pass folds into the newest
               routine one. A pass with anything to read in it (a re-cut, a skip, a retry at 2,048-row groups, an unsorted
               fallback) never folds, so its own figures stay. */
            return entry.Outcome == OutcomeOk && IsRoutineCompaction(previous) && IsRoutineCompaction(entry);
        }

        return entry.Outcome == OutcomeOk
            && (previous.Rows ?? 0) == 0 && (previous.Files ?? 0) == 0
            && (entry.Rows ?? 0) == 0 && (entry.Files ?? 0) == 0;
    }

    /// <summary>A compaction pass that only merged: it re-cut nothing, skipped nothing and needed no smaller row groups or unsorted
    /// fallback (#9448).</summary>
    private static bool IsRoutineCompaction(StoreHousekeepingEvent e) =>
        e.Kind == KindCompaction
        && e.Outcome == OutcomeOk
        && e.Compaction is { } c
        && c.FilesRecut == 0
        && c.RecutSkippedLiveJournal == 0
        && c.RecutSkippedTimeBudget == 0
        && !c.RecutSkippedTimeBudgetIsLowerBound
        && (c.RecutSkips is null || c.RecutSkips.Values.All(n => n == 0))
        && c.SortedAtSmallGroupBatches == 0
        && c.UnsortedFallbackBatches == 0;
}
