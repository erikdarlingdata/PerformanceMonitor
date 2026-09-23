/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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

    /// <summary>Whether one plan line is a scan node over a chunk: seq, index, bitmap-heap or columnar, never
    /// the index scan beneath a bitmap heap node, which reads an index named after the chunk.</summary>
    internal static bool IsChunkScan(string planLine) => ChunkScanLine.IsMatch(planLine);

    /// <summary>How many chunk scans the plan carries. A chunk excluded at plan time is simply absent.</summary>
    internal static int Count(string plan) => plan.Split('\n').Count(IsChunkScan);
}
