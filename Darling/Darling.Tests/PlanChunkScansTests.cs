/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PlanChunkScans"/> counts one scan per chunk whichever access path the planner picks. The bitmap
/// plan is the one #3968's CI run printed when LatestValueLookbackLivePostgresTests reported "planned 4 chunks"
/// for a read that scanned two.
/// </summary>
public sealed class PlanChunkScansTests
{
    private const string BitmapPlan = """
        Aggregate
          ->  Subquery Scan on latest
                Filter: (latest.rn = 1)
                ->  WindowAgg
                      ->  Sort
                            ->  Result
                                  ->  Append
                                        ->  Bitmap Heap Scan on _hyper_12_11_chunk
                                              Recheck Cond: ((collection_time >= '2026-09-22 01:56:40'::timestamp without time zone) AND (collection_time <= '2026-09-23 01:56:40'::timestamp without time zone))
                                              Filter: ((size_mb > '0'::numeric) AND (server_id = '-389603'::integer))
                                              ->  Bitmap Index Scan on _hyper_12_11_chunk_file_io_stats_collection_time_idx
                                                    Index Cond: ((collection_time >= '2026-09-22 01:56:40'::timestamp without time zone) AND (collection_time <= '2026-09-23 01:56:40'::timestamp without time zone))
                                        ->  Bitmap Heap Scan on _hyper_12_23_chunk
                                              Recheck Cond: ((server_id = '-389603'::integer) AND (collection_time >= '2026-09-22 01:56:40'::timestamp without time zone))
                                              ->  Bitmap Index Scan on _hyper_12_23_chunk_idx_file_io_stats_time
                                                    Index Cond: ((server_id = '-389603'::integer) AND (collection_time >= '2026-09-22 01:56:40'::timestamp without time zone))
        """;

    [Fact]
    public void ABitmapPlan_CountsEachChunkOnce_NotItsIndexToo()
    {
        Assert.Equal(2, PlanChunkScans.Count(BitmapPlan));
    }

    [Theory]
    [InlineData("->  Seq Scan on _hyper_12_11_chunk", true)]
    [InlineData("->  Seq Scan on _hyper_12_11_chunk file_io_stats_1  (cost=0.00..1.01 rows=1 width=8)", true)]
    [InlineData("->  Index Scan using _hyper_12_23_chunk_idx_file_io_stats_time on _hyper_12_23_chunk", true)]
    [InlineData("->  Index Scan Backward using _hyper_3_7_chunk_idx on _hyper_3_7_chunk (never executed)", true)]
    [InlineData("->  Bitmap Heap Scan on _hyper_12_11_chunk", true)]
    [InlineData("->  Custom Scan (ColumnarScan) on _hyper_12_11_chunk", true)]
    [InlineData("->  Custom Scan (DecompressChunk) on _hyper_12_11_chunk", true)]
    [InlineData("->  Bitmap Index Scan on _hyper_12_11_chunk_file_io_stats_collection_time_idx", false)]
    [InlineData("->  Seq Scan on compress_hyper_13_45_chunk", false)]
    [InlineData("->  Seq Scan on file_io_stats", false)]
    public void EachLine_IsAChunkScan_OnlyWhenItReadsTheChunkItself(string line, bool expected)
    {
        Assert.Equal(expected, PlanChunkScans.IsChunkScan(line));
    }
}
