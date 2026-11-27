/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// One stored <c>index_object_stats</c> row, reduced to what the object usage window needs (#8784). The caller maps its store's rows into
/// this shape, so the window has no dependency on either store or on the collector assembly. Counters are the cumulative
/// <c>sys.dm_db_index_usage_stats</c> values as collected (NULL, an index with no usage row, is 0). The <c>Last*</c> stamps are the monitored
/// server's local clock, as stored: the caller converts what it returns. <c>IsMemoryOptimized</c> marks a memory-optimized table, which that
/// DMV holds no rows for: its counters are always zero and are not evidence. It is null on a row stored before the flag was collected: such
/// a row does not say either way.
/// </summary>
public readonly record struct ObjectUsageSample(
    DateTime CollectionTime,
    DateTime? SqlServerStartTime,
    string? AnsweringServerName,
    int ObjectId,
    int IndexId,
    string SchemaName,
    string ObjectName,
    bool IsIndexedView,
    long UserSeeks,
    long UserScans,
    long UserLookups,
    long UserUpdates,
    DateTime? LastUserSeek,
    DateTime? LastUserScan,
    DateTime? LastUserLookup,
    DateTime? LastUserUpdate,
    bool? IsMemoryOptimized = false);

/// <summary>Why the window treated a snapshot as a counter reset. More than one can be true at once.</summary>
[Flags]
public enum ObjectUsageResetReasons
{
    /// <summary>No reset.</summary>
    None = 0,

    /// <summary><c>sqlserver_start_time</c> changed: the instance restarted.</summary>
    Restart = 1,

    /// <summary>An instance answered for the first time in the window (a failover, or read-only routing to another node). Its counters were
    /// counted whole unless <see cref="NodeBaseline"/> is also set. A return to an instance already seen is not a reset: its counters are
    /// measured from that instance's own last snapshot.</summary>
    NodeChanged = 2,

    /// <summary>A counter went down. The DMV was cleared without a restart: the database went offline and online (or AUTO_CLOSE closed it),
    /// was restored, or was detached and attached.</summary>
    CounterDecreased = 4,

    /// <summary>A <c>last_user_*</c> stamp that had a value is now NULL, which only a clear of the DMV does.</summary>
    LastAccessCleared = 8,

    /// <summary>With <see cref="NodeChanged"/>: the new instance had been up since before the window, so its counters may hold use from before
    /// it. Nothing of this snapshot was counted; its counters are measured from here, and its last-seen stamps count.</summary>
    NodeBaseline = 16,
}

/// <summary>A snapshot the window treated as a counter reset.</summary>
/// <param name="CollectionTime">The snapshot's collection time.</param>
/// <param name="Reasons">What gave the reset away.</param>
/// <param name="FromServerName">The answering instance of the snapshot before it, when known.</param>
/// <param name="ToServerName">The answering instance of this snapshot, when known.</param>
/// <param name="BlindFrom">For a clear of the counters (a restart, a counter that went down, a stamp that was cleared): the last snapshot the
/// same instance answered before it. Use between that snapshot and the clear itself was wiped with the counters and the stamps, so nothing
/// in the window can show it. Null where nothing was lost (an instance seen for the first time).</param>
public sealed record ObjectUsageReset(
    DateTime CollectionTime, ObjectUsageResetReasons Reasons, string? FromServerName, string? ToServerName, DateTime? BlindFrom = null)
{
    /// <summary>The longest the lost use can span: from <see cref="BlindFrom"/> to this snapshot. Zero where nothing was lost.</summary>
    public TimeSpan BlindTime => BlindFrom is { } from && CollectionTime > from ? CollectionTime - from : TimeSpan.Zero;
}

/// <summary>One table or indexed view's usage over the window.</summary>
/// <param name="ObjectId">The object id.</param>
/// <param name="SchemaName">The schema, as of the newest snapshot that holds the object.</param>
/// <param name="ObjectName">The name, as of the newest snapshot that holds the object.</param>
/// <param name="IsIndexedView">An indexed view rather than a table.</param>
/// <param name="Reads">Seeks, scans and lookups across all of the object's indexes, summed over the window's deltas.</param>
/// <param name="Writes">Updates on the heap or clustered index (index id 0 or 1), summed over the window's deltas. The base index only, so a
/// statement is not counted once per nonclustered index it maintains.</param>
/// <param name="LastRead">The newest seek, scan or lookup stamp seen in any snapshot (server-local), carried forward across resets.</param>
/// <param name="LastWrite">The newest update stamp seen in any snapshot (server-local), carried forward across resets.</param>
/// <param name="FirstSnapshot">The first snapshot the object appears in.</param>
/// <param name="LastSnapshot">The newest snapshot the object appears in. Before <see cref="ObjectUsageWindowResult.CoveredTo"/> means it was dropped.</param>
/// <param name="IndexCount">How many distinct index ids the object had across the window.</param>
/// <param name="IsMemoryOptimized">A memory-optimized table as of the newest snapshot that says: its counters are not evidence of use. Null
/// when no snapshot says (every row of the object was stored before the flag was collected).</param>
public sealed record ObjectUsageTotals(
    int ObjectId,
    string SchemaName,
    string ObjectName,
    bool IsIndexedView,
    long Reads,
    long Writes,
    DateTime? LastRead,
    DateTime? LastWrite,
    DateTime FirstSnapshot,
    DateTime LastSnapshot,
    int IndexCount,
    bool? IsMemoryOptimized = false);

/// <summary>What the window found.</summary>
/// <param name="CoveredFrom">The first snapshot read, the baseline every count is measured from. Null when there were none.</param>
/// <param name="CoveredTo">The newest snapshot read.</param>
/// <param name="SnapshotCount">How many snapshots were read, the baseline included.</param>
/// <param name="LargestGap">The longest time between two consecutive snapshots. Null with fewer than two.</param>
/// <param name="Resets">Every snapshot treated as a counter reset for the whole database, oldest first.</param>
/// <param name="AnsweringServerNames">Every answering instance named across the snapshots, in the order first seen.</param>
/// <param name="Objects">One row per table or indexed view seen in any snapshot.</param>
/// <param name="IndexResets">How many times one index's counters were found cleared on their own (the index dropped and created again, or
/// rebuilt on a version that clears its usage row) and only that index was counted whole.</param>
/// <param name="BlindTime">The time the window could not see: for each clear of the counters, from the instance's last snapshot before it to
/// the snapshot that found it, added up. An upper bound, since the clear happened somewhere in between and use after it is counted.</param>
public sealed record ObjectUsageWindowResult(
    DateTime? CoveredFrom,
    DateTime? CoveredTo,
    int SnapshotCount,
    TimeSpan? LargestGap,
    IReadOnlyList<ObjectUsageReset> Resets,
    IReadOnlyList<string> AnsweringServerNames,
    IReadOnlyList<ObjectUsageTotals> Objects,
    int IndexResets = 0,
    TimeSpan BlindTime = default);

/// <summary>
/// Usage totals over a window of daily <c>index_object_stats</c> snapshots for ONE database of ONE registration, correct across counter resets
/// (#8784). <c>sys.dm_db_index_usage_stats</c> is cumulative since it was last cleared, so the latest snapshot alone answers "how much since the
/// last restart", and a plain newest-minus-oldest is wrong as soon as a reset falls inside the window. This walks the snapshots in order and
/// adds up the delta between each one and the one before it.
///
/// <para><b>Counters are tracked per answering instance.</b> The counters are per instance, and through a listener with read-only routing the
/// node behind one registration can change from one collection to the next. Each snapshot is therefore compared with the last snapshot the
/// SAME instance answered, not with the snapshot before it: two nodes that take turns each keep their own running difference, and a return to
/// a node already seen is not a reset. A snapshot with no instance name (a row stored before the name was collected) is taken to be the
/// instance that answered before it.</para>
///
/// <para><b>A whole-database reset</b> is a clear of the DMV for the database: a restart, the database going offline and online or being
/// closed by AUTO_CLOSE, a restore, a detach and attach, a change of availability group role. It is called when the instance's
/// <c>sqlserver_start_time</c> moved by more than <see cref="StartTimeTolerance"/> (a value that only wobbles is the same start), or when at least half of the indexes that had any use before show a counter that went down or a
/// <c>last_user_*</c> stamp that went from a value to NULL (<see cref="ObjectUsageResetReasons"/>). After it every index's own value is the
/// delta, because everything since the clear happened in the window. A counter that has grown back past its old value hides the clear from
/// that index, which is why the other indexes' evidence resets it too.</para>
///
/// <para><b>One index cleared on its own</b> is not a database reset. An index that is dropped and created again under the same id, or
/// rebuilt on a version that clears its usage row, shows the same evidence on that index alone. When fewer than half of the indexes with use
/// show it, only those indexes are counted whole and the rest keep their difference
/// (<see cref="ObjectUsageWindowResult.IndexResets"/>). Treating it as a database reset would add every other table's whole cumulative value
/// again and make a table last read years ago look read.</para>
///
/// <para><b>An instance seen for the first time</b> after the baseline has counters that are cumulative since its own last clear. When its
/// <c>sqlserver_start_time</c> is more than a day before the baseline, those counters can hold use from before the window, so the snapshot
/// becomes that instance's own baseline: nothing of it is counted and it is measured from there
/// (<see cref="ObjectUsageResetReasons.NodeBaseline"/>). Otherwise (it started inside the window, or its start time is unknown) its whole
/// value is counted. The day's margin is there because the start time is the server's local clock and the collection time is UTC.</para>
///
/// <para><b>Errors lean toward "used" where the two differ.</b> A false reset over-counts and a missed one under-counts, and for a
/// decommissioning decision only the second is dangerous. An index that appears for the first time after its instance's baseline (created
/// inside the window) counts its whole value. Where a count can fall short (a hidden clear, an instance's own baseline), the last-seen stamps
/// still cover it: a clear sets them to NULL, so any stamp read after one is from inside the window.</para>
///
/// <para><b>What a clear takes with it.</b> Use after a clear is counted whole, but use between the instance's last snapshot and the clear
/// is gone: the counters and the stamps that held it were wiped before anything read them. A table read once in the morning, on an instance
/// that restarts at noon and is collected at midnight, shows nothing. The window cannot recover that, so it reports it: every such reset
/// carries <see cref="ObjectUsageReset.BlindFrom"/> and the result adds the spans up (<see cref="ObjectUsageWindowResult.BlindTime"/>), for
/// the reader to take off the covered time. With daily collection each clear hides at most a day.</para>
///
/// <para><b>Last seen is carried forward.</b> A reset sets the <c>last_user_*</c> stamps back to NULL, so the newest stamp is the newest across
/// every snapshot, not the newest snapshot's.</para>
///
/// <para><b>The baseline.</b> The first snapshot fed in is the baseline: its counters are cumulative since a clear before it, so none of it is
/// counted, and every count is measured from it (<see cref="ObjectUsageWindowResult.CoveredFrom"/>). Its stamps still count toward last seen.
/// A caller that wants the window to start at a time feeds the newest snapshot at or before that time first.</para>
///
/// <para>Feed rows in <c>CollectionTime</c> order; rows of one snapshot share their collection time and may come in any order among themselves.
/// Two rows for the same index in one snapshot keep the larger value of each field.</para>
/// </summary>
public sealed class ObjectUsageWindow
{
    private readonly List<ObjectUsageSample> _snapshot = new();
    private readonly ObjectUsageStepper _stepper = new();
    private readonly Dictionary<int, ObjectState> _objects = new();
    private readonly List<ObjectUsageReset> _resets = new();
    private readonly List<string> _servers = new();

    private DateTime? _snapshotTime;
    private int _indexResets;
    private TimeSpan? _largestGap;
    private int _snapshotCount;
    private bool _completed;

    /// <summary>Adds one row. Rows must arrive in non-decreasing <see cref="ObjectUsageSample.CollectionTime"/>.</summary>
    public void Add(ObjectUsageSample sample)
    {
        if (_completed)
        {
            throw new InvalidOperationException("The window is already complete.");
        }

        if (_snapshotTime is { } current)
        {
            if (sample.CollectionTime < current)
            {
                throw new ArgumentException(
                    $"Rows must arrive in collection time order: {sample.CollectionTime:o} came after {current:o}.", nameof(sample));
            }

            if (sample.CollectionTime != current)
            {
                Flush();
            }
        }

        _snapshotTime = sample.CollectionTime;
        _snapshot.Add(sample);
    }

    /// <summary>Closes the last snapshot and returns the totals. Objects are ordered by schema then name.</summary>
    public ObjectUsageWindowResult Complete()
    {
        if (!_completed)
        {
            Flush();
            _completed = true;
        }

        var objects = _objects.Values
            .Select(o => new ObjectUsageTotals(
                o.ObjectId, o.SchemaName, o.ObjectName, o.IsIndexedView, o.Reads, o.Writes, o.LastRead, o.LastWrite,
                o.FirstSnapshot, o.LastSnapshot, o.IndexIds.Count, o.IsMemoryOptimized))
            .OrderBy(o => o.SchemaName, StringComparer.Ordinal)
            .ThenBy(o => o.ObjectName, StringComparer.Ordinal)
            .ThenBy(o => o.ObjectId)
            .ToList();

        return new ObjectUsageWindowResult(
            _stepper.Nodes.CoveredFrom, _stepper.Nodes.PreviousTime, _snapshotCount, _largestGap, _resets.ToList(), _servers.ToList(), objects, _indexResets,
            TimeSpan.FromTicks(_resets.Sum(r => r.BlindTime.Ticks)));
    }

    private void Flush()
    {
        if (_snapshot.Count == 0 || _snapshotTime is not { } time)
        {
            return;
        }

        /* The per-snapshot rules (node, restart, evidence, whole or difference) live in ObjectUsageStepper (#9349), shared with Darling's chain
           builder and window replay. This fold only adds up what each step says. */
        var step = _stepper.Step(time, _snapshot);
        var node = step.Node;

        if (!node.IsBaseline)
        {
            _indexResets += step.IndexResets;
            if (step.Reasons != ObjectUsageResetReasons.None)
            {
                _resets.Add(new ObjectUsageReset(time, step.Reasons, node.PreviousServer, node.Server, step.BlindFrom));
            }

            var gap = time - node.PreviousTime!.Value;
            if (_largestGap is not { } largest || gap > largest)
            {
                _largestGap = gap;
            }
        }

        foreach (var row in _snapshot)
        {
            if (!_objects.TryGetValue(row.ObjectId, out var state))
            {
                state = new ObjectState(row.ObjectId, time);
                _objects.Add(row.ObjectId, state);
            }

            state.SchemaName = row.SchemaName;
            state.ObjectName = row.ObjectName;
            state.IsIndexedView = row.IsIndexedView;
            state.IsMemoryOptimized = row.IsMemoryOptimized ?? state.IsMemoryOptimized;
            state.LastSnapshot = time;
            state.IndexIds.Add(row.IndexId);
            state.LastRead = Newest(state.LastRead, Newest(row.LastUserSeek, Newest(row.LastUserScan, row.LastUserLookup)));
            state.LastWrite = Newest(state.LastWrite, row.LastUserUpdate);
        }

        foreach (var index in step.Indexes)
        {
            var state = _objects[index.ObjectId];
            state.Reads += index.Reads;
            if (index.IndexId <= 1)
            {
                state.Writes += index.Updates;
            }
        }

        if (node.Server is { } server && !_servers.Contains(server, StringComparer.OrdinalIgnoreCase))
        {
            _servers.Add(server);
        }

        _snapshotCount++;
        _snapshot.Clear();
    }

    /// <summary>How far an instance's <c>sqlserver_start_time</c> may move between two snapshots and still be the same start. The value is
    /// computed by the server, and a reading that differs by a fraction of a second would otherwise count every index whole again and make a
    /// table last read years ago look read. A real restart moves it by the downtime and the uptime before it, far more than this; one that
    /// did not is still caught by the counters going down.</summary>
    public static readonly TimeSpan StartTimeTolerance = TimeSpan.FromMinutes(1);

    /// <summary>The later of two stamps, a null being no stamp.</summary>
    internal static DateTime? Newest(DateTime? a, DateTime? b) =>
        a is null ? b : b is null ? a : (a.Value >= b.Value ? a : b);

    private sealed class ObjectState(int objectId, DateTime firstSnapshot)
    {
        public int ObjectId { get; } = objectId;
        public DateTime FirstSnapshot { get; } = firstSnapshot;
        public DateTime LastSnapshot { get; set; } = firstSnapshot;
        public string SchemaName { get; set; } = "";
        public string ObjectName { get; set; } = "";
        public bool IsIndexedView { get; set; }
        public bool? IsMemoryOptimized { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public DateTime? LastRead { get; set; }
        public DateTime? LastWrite { get; set; }
        public HashSet<int> IndexIds { get; } = new();
    }
}
