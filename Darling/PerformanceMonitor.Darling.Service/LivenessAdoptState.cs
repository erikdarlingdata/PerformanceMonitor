/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Where the retention pass's adopt step is in each liveness table, and how it is sized. The adopt step gives a row with
/// no liveness row one (<see cref="PerformanceMonitor.Darling.Storage.LivenessAdoptSlice"/>), in keyset slices of the
/// table's key, for at most <see cref="Budget"/> of slice run time per table per pass. A pass that stops on that budget
/// leaves its cursor here, and the next pass goes on from it, so the whole key is covered over several passes. A pass that
/// reaches the end of the keys clears the cursor, and the next one starts at the first key again.
///
/// <para>It is held in the service's memory: the worker owns one and hands it to every pass. A restart forgets the
/// cursors, so the first pass after it starts at the first key of every table. The adopt step is a backstop for rows
/// written with the insert trigger off, so starting over costs only the time until the cursor is back where it was.</para>
///
/// <para>A slice that runs into its command timeout halves the next slice of that table, down to
/// <see cref="SliceKeysFloor"/>, and a slice that finishes in under a quarter of the timeout doubles it, up to
/// <see cref="SliceKeys"/>. The size is kept here with the cursor, so a store that needs small slices keeps them from
/// pass to pass.</para>
/// </summary>
public sealed class LivenessAdoptState
{
    /// <summary>
    /// The keys one slice reads, and the most a slice grows back to. A slice reads this many entries of the source table's
    /// primary key and of the liveness table's, each as one range, so its cost is two index range reads of this many keys.
    /// </summary>
    internal const int DefaultSliceKeys = 50_000;

    /// <summary>The fewest keys a slice shrinks to. A slice that times out at this size is tried again at this size.</summary>
    internal const int DefaultSliceKeysFloor = 1_000;

    /// <summary>
    /// The slice run time one table's adopt may use in one pass. The time a slice's own statements take counts, and the
    /// drain's waits and pauses between slices do not.
    /// </summary>
    internal static readonly TimeSpan DefaultBudget = TimeSpan.FromSeconds(60);

    /// <summary>The command timeout of each statement of a slice, in seconds: a third of <see cref="DefaultBudget"/>.</summary>
    internal const int DefaultSliceTimeoutSeconds = 20;

    private readonly object _gate = new();
    private readonly Dictionary<string, TableState> _tables = new(StringComparer.Ordinal);
    private readonly Func<double> _secondsClock;

    public LivenessAdoptState()
        : this(DefaultSliceKeys, DefaultSliceKeysFloor, DefaultBudget, DefaultSliceTimeoutSeconds)
    {
    }

    /// <summary>
    /// The sizes and the clock are a test's to set, so a live test covers several passes and a shrunk slice with a table of
    /// a few rows. Every caller in the product uses the parameterless constructor.
    /// </summary>
    internal LivenessAdoptState(
        int sliceKeys, int sliceKeysFloor, TimeSpan budget, int sliceTimeoutSeconds, Func<double>? secondsClock = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(sliceKeysFloor, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(sliceKeys, sliceKeysFloor);
        ArgumentOutOfRangeException.ThrowIfLessThan(sliceTimeoutSeconds, 1);
        SliceKeys = sliceKeys;
        SliceKeysFloor = sliceKeysFloor;
        Budget = budget;
        SliceTimeoutSeconds = sliceTimeoutSeconds;
        _secondsClock = secondsClock ?? (() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency);
    }

    /// <summary>The keys a slice of a table reads until a timeout shrinks it, and the most it grows back to.</summary>
    internal int SliceKeys { get; }

    /// <summary>The fewest keys a slice shrinks to.</summary>
    internal int SliceKeysFloor { get; }

    /// <summary>The slice run time one table's adopt may use in one pass.</summary>
    internal TimeSpan Budget { get; }

    /// <summary>The command timeout of each statement of a slice, in seconds.</summary>
    internal int SliceTimeoutSeconds { get; }

    /// <summary>The clock a slice's run time is measured on, in seconds.</summary>
    internal double NowSeconds => _secondsClock();

    /// <summary>The upper key of the last slice of <paramref name="livenessTable"/> that ran, or null at the first key.</summary>
    internal IReadOnlyList<object>? CursorOf(string livenessTable)
    {
        lock (_gate)
        {
            return _tables.TryGetValue(livenessTable, out var table) ? table.Cursor : null;
        }
    }

    /// <summary>The keys the next slice of <paramref name="livenessTable"/> reads.</summary>
    internal int SliceKeysOf(string livenessTable)
    {
        lock (_gate)
        {
            return _tables.TryGetValue(livenessTable, out var table) ? table.SliceKeys : SliceKeys;
        }
    }

    /// <summary>
    /// Records a slice that ran: the cursor moves to <paramref name="upperKey"/>, or back to the first key when it is null
    /// (the slice reached the end of the keys), and the next slice is sized by how long this one took.
    /// </summary>
    internal void NoteSliceRan(string livenessTable, IReadOnlyList<object>? upperKey, double sliceSeconds)
    {
        lock (_gate)
        {
            var table = TableOf(livenessTable);
            table.Cursor = upperKey;
            table.SliceKeys = NextSliceKeys(table.SliceKeys, timedOut: false, sliceSeconds, SliceTimeoutSeconds, SliceKeysFloor, SliceKeys);
        }
    }

    /// <summary>Records a slice that ran into its command timeout: the cursor stays, and the next slice is half the size.</summary>
    internal void NoteSliceTimedOut(string livenessTable)
    {
        lock (_gate)
        {
            var table = TableOf(livenessTable);
            table.SliceKeys = NextSliceKeys(table.SliceKeys, timedOut: true, SliceTimeoutSeconds, SliceTimeoutSeconds, SliceKeysFloor, SliceKeys);
        }
    }

    /// <summary>
    /// The size of the slice after one of <paramref name="sliceKeys"/> keys: half after a timeout, double after a slice that
    /// took under a quarter of <paramref name="sliceTimeoutSeconds"/>, the same otherwise, within
    /// [<paramref name="floor"/>, <paramref name="ceiling"/>]. A quarter, so a slice twice the size still finishes in under
    /// half the timeout at the same rate.
    /// </summary>
    internal static int NextSliceKeys(int sliceKeys, bool timedOut, double sliceSeconds, int sliceTimeoutSeconds, int floor, int ceiling)
    {
        if (timedOut)
        {
            return Math.Clamp(sliceKeys / 2, floor, ceiling);
        }

        if (sliceSeconds < sliceTimeoutSeconds / 4.0)
        {
            return (int)Math.Clamp((long)sliceKeys * 2, floor, ceiling);
        }

        return Math.Clamp(sliceKeys, floor, ceiling);
    }

    private TableState TableOf(string livenessTable)
    {
        if (!_tables.TryGetValue(livenessTable, out var table))
        {
            table = new TableState { SliceKeys = SliceKeys };
            _tables[livenessTable] = table;
        }

        return table;
    }

    private sealed class TableState
    {
        public IReadOnlyList<object>? Cursor { get; set; }

        public int SliceKeys { get; set; }
    }
}
