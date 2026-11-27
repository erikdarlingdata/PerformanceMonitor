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

/// <summary>One <c>collect.object_usage_snapshots</c> row, reduced to what the window replay reads (#9349).</summary>
/// <param name="CollectionTime">The snapshot's collection time, exactly as stored.</param>
/// <param name="StepVersion">The version of the chain rules the row was built with.</param>
/// <param name="AnsweringServerName">The first non-blank answering name in the snapshot, or null.</param>
/// <param name="SqlServerStartTime">The first non-null start in the snapshot, or null.</param>
/// <param name="ComparedWith">The snapshot the chain step compared against; null when the chain node was new.</param>
/// <param name="Reasons">The reset reasons the chain step decided.</param>
/// <param name="IndexResets">How many indexes the chain step counted whole on their own.</param>
/// <param name="BlindFrom">The chain step's blind-from stamp.</param>
internal readonly record struct ObjectUsageSnapshotFact(
    DateTime CollectionTime,
    short StepVersion,
    string? AnsweringServerName,
    DateTime? SqlServerStartTime,
    DateTime? ComparedWith,
    ObjectUsageResetReasons Reasons,
    int IndexResets,
    DateTime? BlindFrom);

/// <summary>A snapshot whose node the window has not seen yet. Its step is recomputed from its own raw rows instead of taken from the chain.</summary>
/// <param name="Time">The snapshot's collection time.</param>
/// <param name="CountedWhole">True when every index counts whole; false when nothing counts (the instance's own baseline).</param>
internal readonly record struct ObjectUsageBoundaryStep(DateTime Time, bool CountedWhole);

/// <summary>One index's presence run, as stored in <c>collect.object_usage_runs</c>. A null <paramref name="LastTime"/> is an open run: present
/// through the newest built snapshot.</summary>
internal readonly record struct ObjectUsageRun(
    int ObjectId, int IndexId, DateTime FirstTime, DateTime? LastTime, string SchemaName, string ObjectName, bool IsIndexedView, bool? IsMemoryOptimized);

/// <summary>One object's stored delta rows summed over the window: the persisted path's step 6.</summary>
internal readonly record struct ObjectUsageDeltaTotals(int ObjectId, long Reads, long Writes, DateTime? LastRead, DateTime? LastWrite);

/// <summary>
/// The window replay of the persisted object usage chain (#9349): what <see cref="ObjectUsageWindow"/> would have decided for each snapshot of the
/// window, worked out from per-snapshot facts only, with no index rows. It runs the node layer of the shared stepper over the stored facts and, for
/// each step after the baseline, proves that the chain's step IS the window's step (same compared snapshot, same restart decision). A step the
/// window cannot take from the chain is a boundary step, recomputed from its raw rows. Anything the replay cannot prove sets
/// <see cref="FallbackReason"/>: the caller then takes the full fold, so the answer never differs from it.
/// </summary>
internal sealed class ObjectUsageWindowReplay
{
    private readonly List<ObjectUsageReset> _resets = new();
    private readonly List<ObjectUsageBoundaryStep> _boundaries = new();
    private readonly List<string> _servers = new();

    private ObjectUsageWindowReplay(DateTime coveredFrom, DateTime coveredTo)
    {
        CoveredFrom = coveredFrom;
        CoveredTo = coveredTo;
    }

    /// <summary>Why the window cannot be answered from the chain; null when it can.</summary>
    public string? FallbackReason { get; private set; }

    /// <summary>The baseline: the snapshot every count is measured from.</summary>
    public DateTime CoveredFrom { get; }

    /// <summary>The newest raw snapshot.</summary>
    public DateTime CoveredTo { get; }

    /// <summary>How many snapshots the window holds, the baseline included.</summary>
    public int SnapshotCount { get; private set; }

    /// <summary>The longest time between two consecutive snapshots; null with fewer than two.</summary>
    public TimeSpan? LargestGap { get; private set; }

    /// <summary>The sum of the chain steps' own-index resets after the baseline.</summary>
    public int IndexResets { get; private set; }

    /// <summary>Every reset after the baseline, oldest first: the chain steps' (reasons and blind-from as stored) and the boundary steps' (node
    /// changed, blind-from null).</summary>
    public IReadOnlyList<ObjectUsageReset> Resets => _resets;

    /// <summary>The boundary steps, oldest first. Their deltas are left out of the stored sum and recomputed from their raw rows.</summary>
    public IReadOnlyList<ObjectUsageBoundaryStep> Boundaries => _boundaries;

    /// <summary>Every answering instance named, in the order first seen.</summary>
    public IReadOnlyList<string> AnsweringServerNames => _servers;

    /// <summary>The collection times to read raw: the baseline and each boundary step.</summary>
    public IReadOnlyList<DateTime> RawTimes => new[] { CoveredFrom }.Concat(_boundaries.Select(b => b.Time)).ToList();

    /// <summary>
    /// Replays the window over the stored facts.
    /// </summary>
    /// <param name="facts">The pair's facts from the baseline to the newest raw snapshot, oldest first.</param>
    /// <param name="baseline">The baseline snapshot the full fold would start from.</param>
    /// <param name="newest">The newest raw snapshot.</param>
    /// <param name="currentVersion">The chain version the reader accepts.</param>
    public static ObjectUsageWindowReplay Run(IReadOnlyList<ObjectUsageSnapshotFact> facts, DateTime baseline, DateTime newest, short currentVersion)
    {
        ArgumentNullException.ThrowIfNull(facts);

        var replay = new ObjectUsageWindowReplay(baseline, newest);
        if (facts.Count == 0 || facts[0].CollectionTime != baseline)
        {
            replay.FallbackReason = "no facts row at the baseline";
            return replay;
        }

        if (facts[^1].CollectionTime < newest)
        {
            replay.FallbackReason = "the newest facts row is before the newest snapshot";
            return replay;
        }

        if (facts.Any(f => f.StepVersion != currentVersion))
        {
            replay.FallbackReason = "a facts row has an older step version";
            return replay;
        }

        var tracker = new ObjectUsageNodeTracker();
        foreach (var fact in facts)
        {
            var step = tracker.Resolve(fact.CollectionTime, new ObjectUsageSnapshotFacts(fact.AnsweringServerName, fact.SqlServerStartTime));
            replay.SnapshotCount++;

            if (step.Server is { } server && !replay._servers.Contains(server, StringComparer.OrdinalIgnoreCase))
            {
                replay._servers.Add(server);
            }

            if (!step.IsBaseline)
            {
                var gap = fact.CollectionTime - step.PreviousTime!.Value;
                if (replay.LargestGap is not { } largest || gap > largest)
                {
                    replay.LargestGap = gap;
                }

                if (!step.NodeKnown)
                {
                    /* The window has not seen this instance: the chain's step was decided against state the window does not have. The step is
                       recomputed from the snapshot's own rows. A node first seen here counts nothing or everything, and loses nothing. */
                    replay._resets.Add(new ObjectUsageReset(fact.CollectionTime, step.Reasons, step.PreviousServer, step.Server));
                    replay._boundaries.Add(new ObjectUsageBoundaryStep(fact.CollectionTime, step.Decision == ObjectUsageNodeDecision.AllWhole));
                }
                else
                {
                    if (step.ComparedWith != fact.ComparedWith)
                    {
                        replay.FallbackReason = "the window compares a snapshot the chain step did not";
                        return replay;
                    }

                    var windowRestart = (step.Reasons & ObjectUsageResetReasons.Restart) != ObjectUsageResetReasons.None;
                    var chainRestart = (fact.Reasons & ObjectUsageResetReasons.Restart) != ObjectUsageResetReasons.None;
                    if (windowRestart != chainRestart)
                    {
                        replay.FallbackReason = "the window's restart decision differs from the chain step's";
                        return replay;
                    }

                    replay.IndexResets += fact.IndexResets;
                    if (fact.Reasons != ObjectUsageResetReasons.None)
                    {
                        replay._resets.Add(new ObjectUsageReset(fact.CollectionTime, fact.Reasons, step.PreviousServer, step.Server, fact.BlindFrom));
                    }
                }
            }

            tracker.Commit(step, null);
        }

        return replay;
    }

    /// <summary>
    /// Puts the window's answer together from the three stored sums and the raw rows of the baseline and the boundary steps. Null, with a reason,
    /// when the stored rows do not cover an object that has use.
    /// </summary>
    /// <param name="deltas">The stored deltas summed per object, boundary steps and the baseline left out.</param>
    /// <param name="runs">The pair's runs that overlap the window.</param>
    /// <param name="raw">The raw rows of <see cref="RawTimes"/>.</param>
    /// <param name="fallbackReason">Why the answer cannot be built; null when it can.</param>
    public ObjectUsageWindowResult? Assemble(
        IReadOnlyList<ObjectUsageDeltaTotals> deltas, IReadOnlyList<ObjectUsageRun> runs, IReadOnlyList<ObjectUsageSample> raw, out string? fallbackReason)
    {
        ArgumentNullException.ThrowIfNull(deltas);
        ArgumentNullException.ThrowIfNull(runs);
        ArgumentNullException.ThrowIfNull(raw);

        var objects = new Dictionary<int, Accumulator>();
        var runsByIndex = new Dictionary<(int ObjectId, int IndexId), List<ObjectUsageRun>>();
        foreach (var run in runs)
        {
            if (!runsByIndex.TryGetValue((run.ObjectId, run.IndexId), out var indexRuns))
            {
                indexRuns = [];
                runsByIndex.Add((run.ObjectId, run.IndexId), indexRuns);
            }

            indexRuns.Add(run);
            var end = run.LastTime ?? CoveredTo;
            var first = run.FirstTime > CoveredFrom ? run.FirstTime : CoveredFrom;
            if (!objects.TryGetValue(run.ObjectId, out var acc))
            {
                acc = new Accumulator { FirstSnapshot = first, LastSnapshot = end, IdentityEnd = DateTime.MinValue };
                objects.Add(run.ObjectId, acc);
            }

            acc.FirstSnapshot = first < acc.FirstSnapshot ? first : acc.FirstSnapshot;
            acc.LastSnapshot = end > acc.LastSnapshot ? end : acc.LastSnapshot;
            acc.IndexIds.Add(run.IndexId);
            if (end > acc.IdentityEnd)
            {
                acc.IdentityEnd = end;
                acc.SchemaName = run.SchemaName;
                acc.ObjectName = run.ObjectName;
                acc.IsIndexedView = run.IsIndexedView;
            }

            if (run.IsMemoryOptimized is not null && end > acc.MemoryEnd)
            {
                acc.MemoryEnd = end;
                acc.IsMemoryOptimized = run.IsMemoryOptimized;
            }
        }

        foreach (var delta in deltas)
        {
            if (!objects.TryGetValue(delta.ObjectId, out var acc))
            {
                fallbackReason = "a stored delta names an object with no run";
                return null;
            }

            acc.Reads += delta.Reads;
            acc.Writes += delta.Writes;
            acc.LastRead = ObjectUsageWindow.Newest(acc.LastRead, delta.LastRead);
            acc.LastWrite = ObjectUsageWindow.Newest(acc.LastWrite, delta.LastWrite);
        }

        var wholeTimes = _boundaries.Where(b => b.CountedWhole).Select(b => b.Time).ToHashSet();
        foreach (var time in RawTimes)
        {
            var rows = raw.Where(r => r.CollectionTime == time).ToList();
            if (rows.Count == 0)
            {
                fallbackReason = "a raw snapshot the replay needs holds no rows";
                return null;
            }

            foreach (var row in rows)
            {
                /* Per (object, index), not per object, and by cover, not overlap: an object whose clustered index has a run but whose
                   nonclustered index has none would otherwise pass, and so would an index whose only run ends before this row's time (it
                   overlaps the window and does not cover the row), and that use would be dropped from the answer. */
                if (!runsByIndex.TryGetValue((row.ObjectId, row.IndexId), out var indexRuns)
                    || !indexRuns.Any(run => run.FirstTime <= time && (run.LastTime is null || run.LastTime >= time))
                    || !objects.TryGetValue(row.ObjectId, out var acc))
                {
                    fallbackReason = "a raw row names an index with no run";
                    return null;
                }

                acc.LastRead = ObjectUsageWindow.Newest(acc.LastRead, ObjectUsageWindow.Newest(row.LastUserSeek, ObjectUsageWindow.Newest(row.LastUserScan, row.LastUserLookup)));
                acc.LastWrite = ObjectUsageWindow.Newest(acc.LastWrite, row.LastUserUpdate);
            }

            if (wholeTimes.Contains(time))
            {
                foreach (var (key, counters) in ObjectUsageCounterStep.Dedupe(rows))
                {
                    var acc = objects[key.ObjectId];
                    acc.Reads += counters.Seeks + counters.Scans + counters.Lookups;
                    if (key.IndexId <= 1)
                    {
                        acc.Writes += counters.Updates;
                    }
                }
            }
        }

        var totals = objects
            .Select(o => new ObjectUsageTotals(
                o.Key, o.Value.SchemaName, o.Value.ObjectName, o.Value.IsIndexedView, o.Value.Reads, o.Value.Writes, o.Value.LastRead, o.Value.LastWrite,
                o.Value.FirstSnapshot, o.Value.LastSnapshot, o.Value.IndexIds.Count, o.Value.IsMemoryOptimized))
            .OrderBy(o => o.SchemaName, StringComparer.Ordinal)
            .ThenBy(o => o.ObjectName, StringComparer.Ordinal)
            .ThenBy(o => o.ObjectId)
            .ToList();

        fallbackReason = null;
        return new ObjectUsageWindowResult(
            CoveredFrom, CoveredTo, SnapshotCount, LargestGap, _resets.ToList(), _servers.ToList(), totals, IndexResets,
            TimeSpan.FromTicks(_resets.Sum(r => r.BlindTime.Ticks)));
    }

    private sealed class Accumulator
    {
        public string SchemaName { get; set; } = "";
        public string ObjectName { get; set; } = "";
        public bool IsIndexedView { get; set; }
        public bool? IsMemoryOptimized { get; set; }
        public DateTime IdentityEnd { get; set; }
        public DateTime MemoryEnd { get; set; } = DateTime.MinValue;
        public DateTime FirstSnapshot { get; set; }
        public DateTime LastSnapshot { get; set; }
        public HashSet<int> IndexIds { get; } = new();
        public long Reads { get; set; }
        public long Writes { get; set; }
        public DateTime? LastRead { get; set; }
        public DateTime? LastWrite { get; set; }
    }
}
