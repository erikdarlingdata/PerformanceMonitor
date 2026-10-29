/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Merges blocked-process-report (BPR) and DMV-snapshot pair-rows into one list for reconstruction. Shared by the
/// Dashboard and Lite block-chain paths (both feed the same source-agnostic <see cref="BlockingChainReconstructor"/>).
///
/// Both sources are kept. Reconstruction is scoped per scan (#6931), and a BPR row and a DMV row never share a scan
/// (DMV synthesizes a negative monitor_loop, BPR loops are never negative), so an equivalent edge in the other source
/// is not a double count. Dropping it would split a real chain: a BPR that reports only B blocks C (B's own wait on A
/// was under the report threshold) plus a DMV snapshot of A blocks B and B blocks C would lose B to C from the
/// snapshot, and A to B to C would never be rebuilt.
/// </summary>
internal static class BlockingPairRowMerge
{
    /// <summary>Append <paramref name="snapshots"/> (DMV) to <paramref name="primary"/> (BPR) in place.</summary>
    internal static void MergeInto(List<BlockingPairRow> primary, IReadOnlyList<BlockingPairRow> snapshots)
    {
        if (snapshots == null || snapshots.Count == 0)
            return;

        primary.AddRange(snapshots);
    }
}
