/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What one <see cref="ArchiveService.CompactParquetFiles"/> pass did, collected as it goes and turned into one housekeeping
/// history entry at the end (#9422). The pass is single-threaded, but the DuckDB memory sampler reports from its own thread, so
/// that one figure is guarded.
/// </summary>
internal sealed class CompactionPassStats
{
    public const string MemorySourceText =
        "process: Process.WorkingSet64 sampled every 75 ms during the pass (the whole Lite process); " +
        "DuckDB: duckdb_memory() of each merge connection sampled every 50 ms, highest total";

    private readonly object _lock = new();
    private long _duckDbPeakBytes;

    public DateTime StartUtc { get; } = DateTime.UtcNow;
    public int GroupsMerged { get; set; }
    public int FilesIn { get; set; }
    public int FilesOut { get; set; }
    public long BytesIn { get; set; }
    public long BytesOut { get; set; }
    public int SortedAtSmallGroupBatches { get; set; }
    public int UnsortedFallbackBatches { get; set; }
    public int FilesRecut { get; set; }
    public int RecutSkippedLiveJournal { get; set; }
    public int RecutSkippedTimeBudget { get; set; }

    /// <summary>True when counting the files the time budget left stopped at its time cap (#9421), so <see cref="RecutSkippedTimeBudget"/> is a lower bound.</summary>
    public bool RecutSkippedTimeBudgetIsLowerBound { get; set; }

    /* The re-cut's skips by reason: the one place a count by ArchiveService.RecutSkipReason is kept, bumped by
       ArchiveService.NoteRecutSkip, where every skip of that kind goes. Sized by the enum, so a new reason has a slot. */
    private readonly int[] _recutSkips = new int[Enum.GetValues<ArchiveService.RecutSkipReason>().Length];

    public void NoteRecutSkip(ArchiveService.RecutSkipReason reason) => _recutSkips[(int)reason]++;

    private Dictionary<string, int> RecutSkipsByName() =>
        Enum.GetValues<ArchiveService.RecutSkipReason>()
            .Where(r => _recutSkips[(int)r] > 0)
            .ToDictionary(r => r.ToString(), r => _recutSkips[(int)r]);
    public long ViewRebuildMs { get; set; }
    public int FailedGroups { get; set; }
    public string? FirstError { get; set; }
    public bool Incomplete { get; set; }

    /// <summary>The callback the merges report each DuckDB memory sample to.</summary>
    public void NoteDuckDbMemory(long bytes)
    {
        lock (_lock)
        {
            if (bytes > _duckDbPeakBytes) _duckDbPeakBytes = bytes;
        }
    }

    public long DuckDbPeakBytes
    {
        get { lock (_lock) { return _duckDbPeakBytes; } }
    }

    /// <summary>Whether the pass did anything worth a history row: a merge, a re-cut or a failure. A pass that only found
    /// nothing to do, or only skipped files, writes none.</summary>
    public bool DidWork => GroupsMerged > 0 || FilesRecut > 0 || FailedGroups > 0 || FirstError is not null;

    public CompactionPassDetail ToDetail(double seconds, long? processPeakBytes) => new(
        StartUtc, seconds, GroupsMerged, FilesIn, FilesOut, BytesIn, BytesOut,
        SortedAtSmallGroupBatches, UnsortedFallbackBatches, FilesRecut,
        RecutSkippedLiveJournal, RecutSkippedTimeBudget, RecutSkipsByName(),
        ViewRebuildMs, processPeakBytes, DuckDbPeakBytes > 0 ? DuckDbPeakBytes : null, MemorySourceText, RecutSkippedTimeBudgetIsLowerBound);
}

/// <summary>
/// The highest working set of this process while it is alive (#9424). It is the method tools/CompactionRepro uses to measure a
/// merge: a background thread reads <c>Process.WorkingSet64</c> every 75 ms, because <c>Process.PeakWorkingSet64</c> is a
/// process-lifetime peak and says nothing about one pass.
/// </summary>
internal sealed class PeakWorkingSetSampler : IDisposable
{
    private readonly ManualResetEventSlim _stop = new(false);
    private readonly Thread _thread;
    private long _peak;

    public PeakWorkingSetSampler()
    {
        _thread = new Thread(() =>
        {
            try
            {
                using var process = Process.GetCurrentProcess();
                do
                {
                    process.Refresh();
                    Note(process.WorkingSet64);
                }
                while (!_stop.Wait(75));
            }
            catch (Exception)
            {
                /* Best effort: a process handle that cannot be read ends the sampling. */
            }
        }) { IsBackground = true, Name = "CompactionWorkingSet" };
        _thread.Start();
    }

    private void Note(long bytes)
    {
        long seen;
        while (bytes > (seen = Interlocked.Read(ref _peak)))
        {
            if (Interlocked.CompareExchange(ref _peak, bytes, seen) == seen) break;
        }
    }

    /// <summary>Stops the sampler and returns the highest working set it saw, null when it saw none.</summary>
    public long? Stop()
    {
        _stop.Set();
        _thread.Join(TimeSpan.FromSeconds(2));
        var peak = Interlocked.Read(ref _peak);
        return peak > 0 ? peak : null;
    }

    public void Dispose()
    {
        _stop.Set();
        _thread.Join(TimeSpan.FromSeconds(2));
        _stop.Dispose();
    }
}
