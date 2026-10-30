/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor analysis engine.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Builds the BLOCKING_CHAIN fact from a reconstruction. Shared by Lite's and Darling's fact collectors so the
/// two cannot grade or describe the chain differently: both pick the worst chain the same way and stamp the same
/// metadata, including whether the chain was still blocking when the window ended.
/// </summary>
internal static class BlockingChainFactBuilder
{
    /// <summary>
    /// How many collection intervals old the chain's newest report or snapshot row may be and still count as
    /// "still blocking at the window end". One interval is the normal gap between two samples of a live chain;
    /// the second absorbs a late or skipped collection.
    /// </summary>
    internal const int OpenAtEndIntervals = 2;

    /// <summary>
    /// The longest blocking-collector interval, in minutes, at which "still open at the window end" is judged.
    /// Above it (an on-load or zero frequency resolves to a daily recapture, 1440) two intervals reach back days
    /// and every chain in an ordinary window would read as still open, so the fact does not say.
    /// </summary>
    internal const int MaxOpenAtEndIntervalMinutes = 60;

    /// <summary>
    /// The chains a drill-down lists, worst first by <see cref="ReconstructedChain.Grade"/> (the order the fact's
    /// pick uses), the reconstruction's own order breaking ties. Both products' drill-downs call this, so the
    /// chain the fact describes is always among the ones listed.
    /// </summary>
    internal static List<ReconstructedChain> DrillDownChains(
        BlockingReconstruction reconstruction, int count, int collectionIntervalMinutes) =>
        DrillDownChains(reconstruction.Chains, count, collectionIntervalMinutes);

    private static List<ReconstructedChain> DrillDownChains(
        IReadOnlyList<ReconstructedChain> chains, int count, int collectionIntervalMinutes)
    {
        // One episode re-fires in every scan: list it once, at its worst scan (#6931).
        var episodes = EpisodeNumbers(chains, collectionIntervalMinutes);
        return chains.OrderByDescending(c => c.Grade).DistinctBy(c => episodes[c]).Take(count).ToList();
    }

    /// <summary>
    /// A chain headed by the same apex session that appears again after more than this has passed with no scan showing
    /// it as an apex is a new episode, not the same one re-firing (#6931). Longer than the blocked-process report's
    /// re-fire interval, so one long block stays one episode. The rows carry no reliable transaction identity for the
    /// blocker (the blocking-process node omits it), so the gap is what separates a reused spid from a long block.
    /// </summary>
    internal static readonly TimeSpan EpisodeGap = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The gap used to join scans into one episode: <see cref="EpisodeGap"/>, or the span the open-at-end check
    /// allows (<see cref="OpenAtEndIntervals"/> collection intervals) when the collectors run slower than that. A
    /// DMV-only server (no blocked-process report) with a 5-minute blocking collector samples one block every 5
    /// minutes, so a fixed 2-minute gap would split it into one episode per sample. Above
    /// <see cref="MaxOpenAtEndIntervalMinutes"/> the interval is not trusted (an on-load recapture resolves to a
    /// daily one), so the fixed gap stands.
    /// </summary>
    internal static TimeSpan EffectiveEpisodeGap(int collectionIntervalMinutes) =>
        collectionIntervalMinutes > MaxOpenAtEndIntervalMinutes
            ? EpisodeGap
            : TimeSpan.FromMinutes(Math.Max(EpisodeGap.TotalMinutes, OpenAtEndIntervals * Math.Max(1, collectionIntervalMinutes)));

    /// <summary>
    /// Numbers the per-scan chains by episode: consecutive chains of one apex spid:ecid, no more than
    /// <see cref="EffectiveEpisodeGap"/> apart, share a number. Keyed by chain instance.
    /// </summary>
    internal static Dictionary<ReconstructedChain, int> EpisodeNumbers(
        IReadOnlyList<ReconstructedChain> chains, int collectionIntervalMinutes)
    {
        var gap = EffectiveEpisodeGap(collectionIntervalMinutes);
        var numbers = new Dictionary<ReconstructedChain, int>(ReferenceEqualityComparer.Instance);
        var next = 0;
        foreach (var apex in chains.GroupBy(c => (c.ApexSpid, c.ApexEcid)))
        {
            DateTime? last = null;
            var current = 0;
            foreach (var (chain, at) in apex.Select(c => (Chain: c, At: ChainTime(c))).OrderBy(x => x.At))
            {
                if (last is null || at - last.Value > gap)
                    current = next++;
                numbers[chain] = current;
                last = at;
            }
        }

        return numbers;
    }

    private static DateTime ChainTime(ReconstructedChain chain) =>
        chain.Levels.Count == 0 ? default : chain.Levels.Min(l => l.EventTime);

    /// <summary>
    /// The chain the fact describes: the worst single scan's chain (the reconstruction is per scan, #6931), the one with the highest BLOCKING_CHAIN grade, ties going to the structurally
    /// larger chain (the reconstruction's own order). A long wait behind two sessions can outgrade a short
    /// pile-up of twenty, and the fact has to describe the chain the grade came from.
    /// </summary>
    internal static ReconstructedChain PickWorst(IReadOnlyList<ReconstructedChain> chains) =>
        chains.MaxBy(c => c.Grade)!;

    /// <summary>
    /// The aggregate BLOCKING_CHAIN fact for a non-empty reconstruction. <paramref name="rows"/> are the pair rows
    /// the reconstruction came from (every sample of every pair, not just the longest-waiting one per edge);
    /// <paramref name="collectionIntervalMinutes"/> is the effective cadence of the blocking collectors on this
    /// server, which decides how recent the chain's last sample must be to count as still open.
    /// </summary>
    internal static Fact Build(
        AnalysisContext context, BlockingReconstruction reconstruction,
        IReadOnlyCollection<BlockingPairRow> rows, int collectionIntervalMinutes)
    {
        var worst = PickWorst(reconstruction.Chains);
        var episodes = EpisodeNumbers(reconstruction.Chains, collectionIntervalMinutes);

        var metadata = new Dictionary<string, double>
        {
            ["worst_chain_depth"] = worst.Depth,
            ["worst_chain_victim_count"] = worst.VictimCount,
            ["worst_apex_spid"] = worst.ApexSpid,
            ["worst_apex_sleeping"] = worst.ApexSleeping ? 1 : 0,
            ["worst_chain_max_wait_ms"] = worst.MaxWaitMs,
            // #6931: chains are per scan and one episode re-fires in every scan, so the count is of episodes: an apex
            // that keeps showing up within the episode gap of its last scan counts once, however many scans caught it.
            ["total_reconstructed_chains"] = episodes.Values.Distinct().Count(),
            ["deepest_chain_overall"] = reconstruction.Chains.Max(c => c.Depth),
            ["max_victim_count_overall"] = reconstruction.Chains.Max(c => c.VictimCount),
            ["depth_capped"] = reconstruction.DepthCapped ? 1 : 0,
            ["traversal_truncated"] = reconstruction.TraversalTruncated ? 1 : 0,
            ["cycle_detected"] = reconstruction.CycleDetected ? 1 : 0
        };

        // An anchored (as_of) window ends in the past; the advice must not tell the operator the chain may be
        // holding locks right now.
        if (context.AsOfUtc is not null)
            metadata["worst_chain_window_anchored"] = 1;

        if (NewestObservation(worst, reconstruction.Chains, episodes, rows, collectionIntervalMinutes) is DateTime newest)
        {
            var age = context.TimeRangeEnd - newest;
            metadata["worst_chain_last_seen_age_ms"] = Math.Max(0, age.TotalMilliseconds);
            if (collectionIntervalMinutes <= MaxOpenAtEndIntervalMinutes)
            {
                var open = age <= TimeSpan.FromMinutes(OpenAtEndIntervals * Math.Max(1, collectionIntervalMinutes));
                metadata["worst_chain_open_at_end"] = open ? 1 : 0;
            }
        }

        // The head's transaction start is the monitored server's own wall clock, whose offset from UTC the
        // analysis does not know, so it travels as a clock reading (seconds since 1970 on that clock), never as an age.
        if (worst.ApexTranStarted is DateTime tran)
            metadata["worst_apex_tran_started_clock_s"] = (tran - DateTime.UnixEpoch).TotalSeconds;

        return new Fact
        {
            Source = "blocking",
            Key = "BLOCKING_CHAIN",
            Value = worst.Depth,
            ServerId = context.ServerId,
            Metadata = metadata
        };
    }

    /// <summary>
    /// The newest sample time among rows where one of the worst chain's sessions blocks another of them, within the
    /// chain's own episode (#6931): from the episode's first scan to its last scan plus <see cref="EffectiveEpisodeGap"/>. A
    /// later, unrelated episode that reuses the same spids is therefore not a sighting of this chain.
    /// </summary>
    private static DateTime? NewestObservation(
        ReconstructedChain chain, IReadOnlyList<ReconstructedChain> chains,
        Dictionary<ReconstructedChain, int> episodes, IEnumerable<BlockingPairRow> rows,
        int collectionIntervalMinutes)
    {
        // The reconstruction's own session key, spid plus ecid: two parallel workers of one spid are two sessions.
        var members = new HashSet<(int Spid, int Ecid)>();
        DateTime? first = null, last = null;
        foreach (var c in chains.Where(c => episodes[c] == episodes[chain]))
        {
            members.Add((c.ApexSpid, c.ApexEcid));
            foreach (var level in c.Levels)
            {
                members.Add((level.BlockingSpid, level.BlockingEcid));
                members.Add((level.BlockedSpid, level.BlockedEcid));
                if (first is null || level.EventTime < first) first = level.EventTime;
                if (last is null || level.EventTime > last) last = level.EventTime;
            }
        }

        var until = last + EffectiveEpisodeGap(collectionIntervalMinutes);
        DateTime? newest = null;
        foreach (var row in rows)
        {
            if (members.Contains((row.BlockingSpid, row.BlockingEcid)) && members.Contains((row.BlockedSpid, row.BlockedEcid))
                && row.EventTime >= first && row.EventTime <= until
                && (newest is null || row.EventTime > newest))
                newest = row.EventTime;
        }

        return newest;
    }
}
