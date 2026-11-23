/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/* The per-snapshot step of the object usage window, pulled out of ObjectUsageWindow.Flush (#9349) so that one rule set serves three users: the
   full fold (ObjectUsageWindow: Lite, and Darling's fallback), the Darling chain builder, and the Darling window replay. It is internal:
   PerformanceMonitor.Common grants InternalsVisibleTo to Darling.Storage, the Darling service and the test projects.

   Two layers:
   1. ObjectUsageNodeTracker works on per-snapshot facts only (collection time, answering server name, sqlserver_start_time), never on index
      rows. It resolves the node key, the restart tolerance and the kept start, NodeChanged and NodeBaseline, and the snapshot it compares
      against. The replay runs only this layer.
   2. ObjectUsageCounterStep takes a node decision, the snapshot's rows and the compared node state's rows. It runs the evidence and half rule
      and decides per index whether its contribution is a difference or the whole value. */

/// <summary>What the node layer decided about how a snapshot's counters are counted, before any index row is looked at.</summary>
internal enum ObjectUsageNodeDecision
{
    /// <summary>Nothing is counted: the baseline, or an instance's own baseline (<see cref="ObjectUsageResetReasons.NodeBaseline"/>).</summary>
    CountNothing,

    /// <summary>Every index is counted whole: an instance seen for the first time, or a restart.</summary>
    AllWhole,

    /// <summary>Counted against the instance's last snapshot. Whether a counter reset hides in the rows is for the counter layer to say.</summary>
    Compare,
}

/// <summary>How the counter layer counted a snapshot. The numbers are the <c>counted</c> column of the persisted chain.</summary>
internal enum ObjectUsageCounted
{
    /// <summary>Nothing was counted.</summary>
    Nothing = 0,

    /// <summary>Each index's difference with the compared snapshot, the indexes that show their own clear whole.</summary>
    PerIndex = 1,

    /// <summary>Every index whole.</summary>
    AllWhole = 2,
}

/// <summary>One answering instance's last snapshot: what the next snapshot it answers is compared with. The node layer owns
/// <see cref="Start"/> and <see cref="LastSnapshot"/>. <see cref="Counters"/> is filled only by the full step, never by a facts-only replay.</summary>
internal sealed class ObjectUsageNodeState
{
    public DateTime? Start { get; set; }
    public DateTime? LastSnapshot { get; set; }
    public Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters> Counters { get; set; } = new();
}

/// <summary>One index's counters and last-use stamps in one snapshot, after the per-snapshot Max dedupe.</summary>
internal readonly record struct ObjectUsageCounters(
    long Seeks, long Scans, long Lookups, long Updates, DateTime? LastSeek, DateTime? LastScan, DateTime? LastLookup, DateTime? LastUpdate)
{
    /// <summary>Any counter above zero or any stamp set: an index a clear of the DMV would show on.</summary>
    public bool HasUse =>
        Seeks > 0 || Scans > 0 || Lookups > 0 || Updates > 0
        || LastSeek is not null || LastScan is not null || LastLookup is not null || LastUpdate is not null;

    /// <summary>The newest of the seek, scan and lookup stamps.</summary>
    public DateTime? LastRead => ObjectUsageWindow.Newest(LastSeek, ObjectUsageWindow.Newest(LastScan, LastLookup));

    public static ObjectUsageCounters From(ObjectUsageSample row) =>
        new(Math.Max(0, row.UserSeeks), Math.Max(0, row.UserScans), Math.Max(0, row.UserLookups), Math.Max(0, row.UserUpdates),
            row.LastUserSeek, row.LastUserScan, row.LastUserLookup, row.LastUserUpdate);

    public static ObjectUsageCounters Max(ObjectUsageCounters a, ObjectUsageCounters b) =>
        new(Math.Max(a.Seeks, b.Seeks), Math.Max(a.Scans, b.Scans), Math.Max(a.Lookups, b.Lookups), Math.Max(a.Updates, b.Updates),
            ObjectUsageWindow.Newest(a.LastSeek, b.LastSeek), ObjectUsageWindow.Newest(a.LastScan, b.LastScan),
            ObjectUsageWindow.Newest(a.LastLookup, b.LastLookup), ObjectUsageWindow.Newest(a.LastUpdate, b.LastUpdate));
}

/// <summary>The per-snapshot facts the node layer works on, read from a snapshot's rows the way the window always has.</summary>
/// <param name="Server">The first non-blank answering server name, or null.</param>
/// <param name="Start">The first non-null <c>sqlserver_start_time</c>, or null.</param>
internal readonly record struct ObjectUsageSnapshotFacts(string? Server, DateTime? Start)
{
    public static ObjectUsageSnapshotFacts Read(IReadOnlyList<ObjectUsageSample> rows)
    {
        DateTime? start = null;
        string? server = null;
        foreach (var row in rows)
        {
            start ??= row.SqlServerStartTime;
            server ??= string.IsNullOrWhiteSpace(row.AnsweringServerName) ? null : row.AnsweringServerName;
        }

        return new ObjectUsageSnapshotFacts(server, start);
    }
}

/// <summary>What the node layer decided for one snapshot. Everything the persisted <c>object_usage_snapshots</c> row holds about the node
/// is here: <see cref="NodeKey"/>, <see cref="ComparedWith"/>, <see cref="KeptStart"/> and the node-level <see cref="Reasons"/>.</summary>
internal sealed class ObjectUsageNodeStep
{
    internal ObjectUsageNodeStep(
        DateTime time, string? server, DateTime? start, string nodeKey, bool isBaseline, bool nodeKnown, ObjectUsageNodeState node,
        string? previousServer, DateTime? previousTime, DateTime? comparedWith, ObjectUsageResetReasons reasons, ObjectUsageNodeDecision decision,
        DateTime? keptStart)
    {
        Time = time;
        Server = server;
        Start = start;
        NodeKey = nodeKey;
        IsBaseline = isBaseline;
        NodeKnown = nodeKnown;
        Node = node;
        PreviousServer = previousServer;
        PreviousTime = previousTime;
        ComparedWith = comparedWith;
        Reasons = reasons;
        Decision = decision;
        KeptStart = keptStart;
    }

    /// <summary>The snapshot's collection time.</summary>
    public DateTime Time { get; }

    /// <summary>The answering server name as the window reads it: the first non-blank name in the snapshot, or null.</summary>
    public string? Server { get; }

    /// <summary>The snapshot's <c>sqlserver_start_time</c> as the window reads it: the first non-null value, or null.</summary>
    public DateTime? Start { get; }

    /// <summary>The node key after unnamed inheritance and the takeover: the name, else the previous snapshot's node, else "" (unnamed).</summary>
    public string NodeKey { get; }

    /// <summary>The first snapshot of the series: nothing is counted and nothing is compared.</summary>
    public bool IsBaseline { get; }

    /// <summary>The node had a state already: it answered an earlier snapshot of this fold.</summary>
    public bool NodeKnown { get; }

    /// <summary>The node's live state, a fresh empty one when <see cref="NodeKnown"/> is false. Its <c>Counters</c> are the compared rows until <see cref="ObjectUsageNodeTracker.Commit"/> replaces them.</summary>
    public ObjectUsageNodeState Node { get; }

    /// <summary>The snapshot the node's counters are compared against: the node state's last snapshot. Null when the node is new. Fixed when the step is resolved: <see cref="ObjectUsageNodeTracker.Commit"/> moves the live node state on.</summary>
    public DateTime? ComparedWith { get; }

    /// <summary>The answering instance's name for the snapshot before this one, when known (carried through unnamed snapshots).</summary>
    public string? PreviousServer { get; }

    /// <summary>The collection time of the snapshot before this one. Null for the baseline.</summary>
    public DateTime? PreviousTime { get; }

    /// <summary>The reset reasons the node layer decided on facts alone: <see cref="ObjectUsageResetReasons.NodeChanged"/>,
    /// <see cref="ObjectUsageResetReasons.NodeBaseline"/> or <see cref="ObjectUsageResetReasons.Restart"/>. The counter layer adds the evidence
    /// reasons.</summary>
    public ObjectUsageResetReasons Reasons { get; }

    /// <summary>How the snapshot is counted so far.</summary>
    public ObjectUsageNodeDecision Decision { get; }

    /// <summary>The node's <c>sqlserver_start_time</c> after the step. A start inside the tolerance of the kept one is the same start, so the
    /// first value read is kept and a slow drift cannot add up to a restart.</summary>
    public DateTime? KeptStart { get; }
}

/// <summary>The node layer: runs on per-snapshot facts only, so a replay needs no index rows.</summary>
internal sealed class ObjectUsageNodeTracker
{
    private readonly Dictionary<string, ObjectUsageNodeState> _nodes = new(StringComparer.OrdinalIgnoreCase);
    private string? _previousServer;
    private string? _previousNodeKey;

    /// <summary>The first snapshot committed.</summary>
    public DateTime? CoveredFrom { get; private set; }

    /// <summary>The newest snapshot committed.</summary>
    public DateTime? PreviousTime { get; private set; }

    /// <summary>Decides what the snapshot is. The one effect on the tracker is the takeover of an unnamed node's state by the first name that
    /// follows it. Call <see cref="Commit"/> with the result to move on to the next snapshot.</summary>
    public ObjectUsageNodeStep Resolve(DateTime time, ObjectUsageSnapshotFacts facts)
    {
        var server = facts.Server;
        var start = facts.Start;

        /* The instance this snapshot is compared against is the one that answered it. An unnamed snapshot is taken to be the instance before
           it, and the first name to follow unnamed snapshots takes their state over: a store upgraded to collect the name is one instance
           going on, not a new one appearing. */
        var nodeKey = server ?? _previousNodeKey ?? "";
        if (server is not null && _previousNodeKey is { Length: 0 } && !_nodes.ContainsKey(server) && _nodes.Remove("", out var unnamed))
        {
            _nodes[server] = unnamed;
        }

        var isBaseline = PreviousTime is null;
        var nodeKnown = _nodes.TryGetValue(nodeKey, out var node);
        node ??= new ObjectUsageNodeState();

        var reasons = ObjectUsageResetReasons.None;
        var decision = isBaseline ? ObjectUsageNodeDecision.CountNothing : ObjectUsageNodeDecision.Compare;

        if (!isBaseline)
        {
            if (!nodeKnown)
            {
                reasons |= ObjectUsageResetReasons.NodeChanged;
                if (start is { } nodeStart && CoveredFrom is { } from && nodeStart < from - StartClockMargin)
                {
                    reasons |= ObjectUsageResetReasons.NodeBaseline;
                    decision = ObjectUsageNodeDecision.CountNothing;
                }
                else
                {
                    decision = ObjectUsageNodeDecision.AllWhole;
                }
            }
            else if (start is { } s && node.Start is { } ps && (s - ps).Duration() > ObjectUsageWindow.StartTimeTolerance)
            {
                reasons |= ObjectUsageResetReasons.Restart;
                decision = ObjectUsageNodeDecision.AllWhole;
            }
        }

        var keptStart = start is { } read && node.Start is { } kept && (read - kept).Duration() <= ObjectUsageWindow.StartTimeTolerance
            ? kept
            : start ?? node.Start;

        return new ObjectUsageNodeStep(
            time, server, start, nodeKey, isBaseline, nodeKnown, node, _previousServer, PreviousTime, nodeKnown ? node.LastSnapshot : null, reasons, decision,
            keptStart);
    }

    /// <summary>Moves the tracker past the snapshot. <paramref name="counters"/> is the snapshot's deduped counters, which become the node's
    /// state for the next comparison; a facts-only replay passes null.</summary>
    public void Commit(ObjectUsageNodeStep step, Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters>? counters)
    {
        var node = step.Node;
        if (counters is not null)
        {
            node.Counters = counters;
        }

        node.LastSnapshot = step.Time;
        node.Start = step.KeptStart;
        _nodes[step.NodeKey] = node;

        CoveredFrom ??= step.Time;
        PreviousTime = step.Time;
        _previousServer = step.Server ?? _previousServer;
        _previousNodeKey = step.NodeKey;
    }

    /// <summary>Puts a tracker back where a fold left it, for the chain builder's next call: a fresh tracker is made to look as if every snapshot
    /// up to <paramref name="previousTime"/> had been committed. Each node is its last snapshot, its kept start and the deduped counters of that
    /// snapshot. The caller passes only the nodes the fold would still hold (an unnamed node is gone once a name has taken it over).</summary>
    public void Restore(
        IEnumerable<(string NodeKey, DateTime LastSnapshot, DateTime? KeptStart, Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters> Counters)> nodes,
        string? previousNodeKey, string? previousServer, DateTime coveredFrom, DateTime previousTime)
    {
        _nodes.Clear();
        foreach (var (key, last, kept, counters) in nodes)
        {
            _nodes[key] = new ObjectUsageNodeState { Start = kept, LastSnapshot = last, Counters = counters };
        }

        _previousNodeKey = previousNodeKey;
        _previousServer = previousServer;
        CoveredFrom = coveredFrom;
        PreviousTime = previousTime;
    }

    /// <summary>How far before the baseline an instance's start time has to be before its counters are taken to predate the window. The start
    /// time is the server's local clock and the collection time is UTC, so the two can differ by a time zone offset.</summary>
    private static readonly TimeSpan StartClockMargin = TimeSpan.FromDays(1);
}

/// <summary>One index's step in one snapshot.</summary>
/// <param name="ObjectId">The object id.</param>
/// <param name="IndexId">The index id.</param>
/// <param name="Now">The index's counters in this snapshot, after the Max dedupe of duplicate rows.</param>
/// <param name="HasBefore">The compared node state held this index.</param>
/// <param name="Before">The compared node state's counters for the index. Default when <see cref="HasBefore"/> is false.</param>
/// <param name="Whole">The step counted the index's whole value rather than a difference. False when nothing was counted.</param>
/// <param name="Reads">The seeks, scans and lookups the step contributed (a difference or the whole value). Zero when nothing was counted.</param>
/// <param name="Updates">The updates the step contributed. The window keeps these only for index ids 0 and 1.</param>
internal readonly record struct ObjectUsageIndexStep(
    int ObjectId,
    int IndexId,
    ObjectUsageCounters Now,
    bool HasBefore,
    ObjectUsageCounters Before,
    bool Whole,
    long Reads,
    long Updates)
{
    /// <summary>The index is worth a stored delta row: it was counted whole, it is new to the compared node state, or any of its 4 counters or
    /// 4 stamps differs from the compared row.</summary>
    public bool Moved => Whole || !HasBefore || Now != Before;
}

/// <summary>What the counter layer decided for one snapshot.</summary>
internal sealed class ObjectUsageStep
{
    internal ObjectUsageStep(
        ObjectUsageNodeStep node, ObjectUsageResetReasons reasons, ObjectUsageCounted counted, int indexResets, DateTime? blindFrom,
        Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters> current, List<ObjectUsageIndexStep> indexes)
    {
        Node = node;
        Reasons = reasons;
        Counted = counted;
        IndexResets = indexResets;
        BlindFrom = blindFrom;
        Current = current;
        Indexes = indexes;
    }

    /// <summary>The node layer's decision this step started from.</summary>
    public ObjectUsageNodeStep Node { get; }

    /// <summary>Every reset reason: the node layer's plus the evidence from the rows.</summary>
    public ObjectUsageResetReasons Reasons { get; }

    /// <summary>How the snapshot was counted.</summary>
    public ObjectUsageCounted Counted { get; }

    /// <summary>How many indexes were found cleared on their own (fewer than half of the indexes with use) and counted whole.</summary>
    public int IndexResets { get; }

    /// <summary>For a clear of the counters: the last snapshot the same instance answered before it. Null where nothing was lost.</summary>
    public DateTime? BlindFrom { get; }

    /// <summary>The snapshot's counters per index after the Max dedupe. This becomes the node's state.</summary>
    public Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters> Current { get; }

    /// <summary>One entry per index in the snapshot, with its contribution.</summary>
    public List<ObjectUsageIndexStep> Indexes { get; }
}

/// <summary>The counter layer: the evidence and half rule, and the per-index contributions.</summary>
internal static class ObjectUsageCounterStep
{
    /// <summary>One value per index: two rows for the same index in one snapshot (a retried database inside one run) keep the larger of each
    /// field, which is the later reading of a counter that only grows between clears.</summary>
    public static Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters> Dedupe(IReadOnlyList<ObjectUsageSample> rows)
    {
        var current = new Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters>();
        foreach (var row in rows)
        {
            var counters = ObjectUsageCounters.From(row);
            var key = (row.ObjectId, row.IndexId);
            current[key] = current.TryGetValue(key, out var seen) ? ObjectUsageCounters.Max(seen, counters) : counters;
        }

        return current;
    }

    /// <summary>Counts the snapshot given the node decision and the deduped rows. The compared rows are <c>node.Node.Counters</c>.</summary>
    public static ObjectUsageStep Compute(ObjectUsageNodeStep node, Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters> current)
    {
        var reasons = node.Reasons;
        var countNothing = node.Decision == ObjectUsageNodeDecision.CountNothing;
        var countAllWhole = node.Decision == ObjectUsageNodeDecision.AllWhole;
        var indexResets = 0;
        HashSet<(int ObjectId, int IndexId)>? countWhole = null;
        var compared = node.Node.Counters;

        if (node.Decision == ObjectUsageNodeDecision.Compare)
        {
            var evidence = ObjectUsageResetReasons.None;
            var active = 0;
            foreach (var (key, now) in current)
            {
                if (!compared.TryGetValue(key, out var before) || !before.HasUse)
                {
                    continue;
                }

                active++;
                var own = ObjectUsageResetReasons.None;
                if (now.Seeks < before.Seeks || now.Scans < before.Scans || now.Lookups < before.Lookups || now.Updates < before.Updates)
                {
                    own |= ObjectUsageResetReasons.CounterDecreased;
                }

                if (Cleared(before.LastSeek, now.LastSeek) || Cleared(before.LastScan, now.LastScan)
                    || Cleared(before.LastLookup, now.LastLookup) || Cleared(before.LastUpdate, now.LastUpdate))
                {
                    own |= ObjectUsageResetReasons.LastAccessCleared;
                }

                if (own != ObjectUsageResetReasons.None)
                {
                    evidence |= own;
                    (countWhole ??= new()).Add(key);
                }
            }

            if (countWhole is not null)
            {
                if (countWhole.Count * 2 >= active)
                {
                    reasons |= evidence;
                    countAllWhole = true;
                }
                else
                {
                    indexResets = countWhole.Count;
                }
            }
        }

        DateTime? blindFrom = null;
        if (!node.IsBaseline && reasons != ObjectUsageResetReasons.None)
        {
            /* A clear on an instance already being measured wiped whatever it held since that instance's last snapshot. An instance
               seen for the first time lost nothing: its counters are counted whole, or its stamps still stand (a baseline). */
            var cleared = (reasons & ~(ObjectUsageResetReasons.NodeChanged | ObjectUsageResetReasons.NodeBaseline)) != ObjectUsageResetReasons.None;
            blindFrom = cleared ? node.ComparedWith : null;
        }

        var indexes = new List<ObjectUsageIndexStep>(current.Count);
        foreach (var (key, now) in current)
        {
            var seenBefore = compared.TryGetValue(key, out var before);
            if (countNothing)
            {
                indexes.Add(new ObjectUsageIndexStep(key.ObjectId, key.IndexId, now, seenBefore, before, false, 0, 0));
                continue;
            }

            var whole = countAllWhole || !seenBefore || countWhole?.Contains(key) == true;
            var reads = whole
                ? now.Seeks + now.Scans + now.Lookups
                : Delta(now.Seeks, before.Seeks) + Delta(now.Scans, before.Scans) + Delta(now.Lookups, before.Lookups);
            var updates = whole ? now.Updates : Delta(now.Updates, before.Updates);
            indexes.Add(new ObjectUsageIndexStep(key.ObjectId, key.IndexId, now, seenBefore, before, whole, reads, updates));
        }

        var counted = countNothing ? ObjectUsageCounted.Nothing : countAllWhole ? ObjectUsageCounted.AllWhole : ObjectUsageCounted.PerIndex;
        return new ObjectUsageStep(node, reasons, counted, indexResets, blindFrom, current, indexes);
    }

    private static bool Cleared(DateTime? before, DateTime? now) => before is not null && now is null;

    private static long Delta(long now, long before) => now > before ? now - before : 0;
}

/// <summary>Both layers in one: resolve the node, count the snapshot, commit. The full fold, the chain builder and any caller that has the
/// snapshot's rows use this. A replay that has only facts uses <see cref="ObjectUsageNodeTracker"/> directly.</summary>
internal sealed class ObjectUsageStepper
{
    /// <summary>The node layer, for a caller that also needs its state (<see cref="ObjectUsageNodeTracker.CoveredFrom"/>).</summary>
    public ObjectUsageNodeTracker Nodes { get; } = new();

    /// <summary>Steps one whole snapshot: all rows share <paramref name="time"/>. The compared rows are what the previous <see cref="Step"/>
    /// for the same node left behind.</summary>
    public ObjectUsageStep Step(DateTime time, IReadOnlyList<ObjectUsageSample> rows)
    {
        var node = Nodes.Resolve(time, ObjectUsageSnapshotFacts.Read(rows));
        var step = ObjectUsageCounterStep.Compute(node, ObjectUsageCounterStep.Dedupe(rows));
        Nodes.Commit(node, step.Current);
        return step;
    }
}
