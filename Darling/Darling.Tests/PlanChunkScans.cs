/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Darling.Tests;

/// <summary>
/// Reads the TimescaleDB chunk scans out of an EXPLAIN plan, for the plan-shape tests that prove chunk
/// exclusion.
///
/// <para><b>The trap this exists for.</b> TimescaleDB names a chunk's indexes after the chunk, so a bitmap plan
/// names each chunk twice: the heap node <c>Bitmap Heap Scan on _hyper_12_11_chunk</c> and its child
/// <c>Bitmap Index Scan on _hyper_12_11_chunk_file_io_stats_collection_time_idx</c>. A pattern that stops at
/// <c>_chunk</c> counts both, so the same two-chunk read counts 2 under an index-scan plan and 4 under a
/// bitmap plan, and which one the planner picks turns on the statistics every other test leaves in the shared
/// store. That is how #3968's CI run saw a 24-hour lookback "plan 4 chunks" while its plan scanned two. The
/// chunk name has to end the token.</para>
/// </summary>
internal static class PlanChunkScans
{
    private static readonly Regex ChunkScanLine = new(@"Scan\b.*\bon _hyper_\d+_\d+_chunk\b", RegexOptions.CultureInvariant);
    private static readonly Regex ChunkName = new(@"_hyper_\d+_\d+_chunk\b", RegexOptions.CultureInvariant);

    /// <summary>Whether one plan line is a scan node over a chunk: seq, index, bitmap-heap or columnar, never
    /// the index scan beneath a bitmap heap node, which reads an index named after the chunk.</summary>
    internal static bool IsChunkScan(string planLine) => ChunkScanLine.IsMatch(planLine);

    /// <summary>How many chunk scans the plan carries. A chunk excluded at plan time is simply absent. Counts
    /// every scan NODE, so a statement that scans the same physical chunk twice (a query split into two CTEs
    /// over one hypertable, each independently floor-bounded, the way #4229's job-history GROUP BY fix does)
    /// counts that chunk twice too — use <see cref="DistinctChunkCount"/> where the assertion cares about
    /// physical chunks touched, not scan nodes issued.</summary>
    internal static int Count(string plan) => plan.Split('\n').Count(IsChunkScan);

    /// <summary>How many distinct physical chunks the plan's scan nodes name — a chunk scanned twice by two
    /// separate Append branches over the same hypertable (job history's job_stats CTE plus its own base join)
    /// counts once. #4235's own live proof asserted on <see cref="Count"/> and so double-counted job history's
    /// two-scan shape against a single-scan oracle, needing a reduction the floor could never reach.</summary>
    internal static int DistinctChunkCount(string plan)
    {
        var names = new HashSet<string>();
        foreach (var line in plan.Split('\n'))
        {
            if (!IsChunkScan(line))
                continue;
            var match = ChunkName.Match(line);
            if (match.Success)
                names.Add(match.Value);
        }
        return names.Count;
    }
}
