/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Builds the per-database RCSI inaction figures (#7448) that the DB_CONFIG drill-down writes as
/// <c>rcsi_blocking_events</c>, <c>rcsi_deadlocks</c> and <c>rcsi_reader_writer_pct</c>. Each store reads its
/// own blocked-process waits grouped by (database, lock_mode) and its own deadlock counts, and hands the rows
/// here. The classification is <see cref="RcsiLockModeClassifier"/> itself, so no store keeps a second copy of
/// the lock-mode vocabulary in SQL. A tracked database with no rows gets zeros: measured, and none found. A
/// database that was never tracked has no figures (the caller writes null for it).
/// </summary>
public sealed class RcsiFigureAccumulator
{
    /// <summary>The fewest reader and writer waits in the window that a share is worked out from. The share turns on the RCSI
    /// recommendation, a database-wide change, and one 5-second wait is 100 percent of one.</summary>
    public const int MinReaderWriterWaits = 10;

    private sealed class Counts
    {
        public long Events;
        public long Deadlocks;
        public long Readers;
        public long Writers;
    }

    private readonly Dictionary<string, Counts> _byDatabase = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Starts a database at zero, so it reads as measured with nothing found.</summary>
    public void Track(string database)
    {
        if (!string.IsNullOrEmpty(database))
            _byDatabase.TryAdd(database, new Counts());
    }

    /// <summary>Adds <paramref name="waitCount"/> blocked-process waits that held <paramref name="lockMode"/>.</summary>
    public void AddWaits(string database, string? lockMode, long waitCount)
    {
        if (!_byDatabase.TryGetValue(database, out var c)) return;
        c.Events += waitCount;
        switch (RcsiLockModeClassifier.Classify(lockMode))
        {
            case RcsiLockClass.Reader: c.Readers += waitCount; break;
            case RcsiLockClass.Writer: c.Writers += waitCount; break;
        }
    }

    /// <summary>Adds <paramref name="deadlockCount"/> deadlocks attributed to <paramref name="database"/>.</summary>
    public void AddDeadlocks(string database, long deadlockCount)
    {
        if (_byDatabase.TryGetValue(database, out var c))
            c.Deadlocks += deadlockCount;
    }

    /// <summary>The figures for a tracked database, or null when it was never tracked. The percent is the
    /// whole-number reader share of reader plus writer waits. With no reader or writer waits the share is
    /// undefined: it is 0 when the database had no blocking events and no deadlocks (measured, nothing found),
    /// and null when it had events or deadlocks that were not reader or writer waits (schema or bulk-update
    /// locks, or deadlocks alone), so a share of 0 is never printed beside counts it does not describe. It is also
    /// null with fewer than <see cref="MinReaderWriterWaits"/> reader and writer waits: too few to say what the
    /// contention is, and a null share keeps the recommendation off.</summary>
    public RcsiInactionFigures? Get(string database)
    {
        if (!_byDatabase.TryGetValue(database, out var c)) return null;
        var denominator = c.Readers + c.Writers;
        int? pct = denominator >= MinReaderWriterWaits
            ? (int)(100 * c.Readers / denominator)
            : (denominator > 0 || c.Events > 0 || c.Deadlocks > 0 ? null : 0);
        return new RcsiInactionFigures(Clamp(c.Events), Clamp(c.Deadlocks), pct);
    }

    private static int Clamp(long value) => (int)Math.Min(int.MaxValue, value);
}
