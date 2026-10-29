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
/// Merges blocked-process-report (BPR) and DMV-snapshot pair-rows into one list for reconstruction.
/// BPR is preferred: its rows are added first, and a DMV edge is dropped when an equivalent BPR edge
/// already exists in the same minute. Shared by the Dashboard and Lite block-chain paths (both feed the
/// same source-agnostic <see cref="BlockingChainReconstructor"/>), so the dedup rule can't drift.
///
/// The negative-vs-non-negative monitor_loop split (DMV synthesizes a negative loop, BPR loops are always
/// non-negative) already keeps the two sources in separate <see cref="SessionKey"/> spaces for the viewer's
/// per-episode (scopeByMonitorLoop:true) reconstruction, so this dedup keeps an overlapping edge from being
/// counted twice in the fact and drill-down paths. The minute bucket is intentionally coarse: a BPR event_time and a DMV collection_time for the
/// same physical block won't align to the second.
/// </summary>
internal static class BlockingPairRowMerge
{
    /// <summary>Append <paramref name="snapshots"/> (DMV) to <paramref name="primary"/> (BPR) in place,
    /// skipping any DMV edge already represented by a BPR edge in the same minute.</summary>
    internal static void MergeInto(List<BlockingPairRow> primary, IReadOnlyList<BlockingPairRow> snapshots)
    {
        if (snapshots == null || snapshots.Count == 0)
            return;

        var seen = new HashSet<(int, int, int, int, long)>();
        foreach (var r in primary)
            seen.Add(EdgeKey(r));

        foreach (var s in snapshots)
            if (seen.Add(EdgeKey(s)))
                primary.Add(s);
    }

    private static (int, int, int, int, long) EdgeKey(BlockingPairRow r) =>
        (r.BlockedSpid, r.BlockedEcid, r.BlockingSpid, r.BlockingEcid, r.EventTime.Ticks / TimeSpan.TicksPerMinute);
}
