/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9422 and #9424. A compaction pass that merged, re-cut or failed writes one housekeeping history row with what it did, and a
/// pass that did nothing writes none. The class drives <see cref="ArchiveService.CompactParquetFiles"/>, which takes the store's
/// write lock and the process-wide archive lock, so it joins the serialized collection the other compaction classes use.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveCompactionHistoryTests : IDisposable
{
    private const int Servers = 6;

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly string _archiveDir;
    private readonly StoreHousekeepingHistory _history;
    private readonly List<DuckDbInitializer> _initializers = [];

    public ArchiveCompactionHistoryTests()
    {
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _history = new StoreHousekeepingHistory(Path.Combine(_tempDir, "store-health-history.json"));
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private string P(string fileName) => Path.Combine(_archiveDir, fileName).Replace("\\", "/");

    private static void Exec(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /* A file of the table's shape in 2,048-row groups, servers interleaved and rows in a scrambled time order. */
    private string WriteUnsorted(string fileName, int rows, int payloadCopies = 1)
    {
        var path = P(fileName);
        var payload = string.Join(" || ", Enumerable.Repeat("md5(i::VARCHAR) || md5((i + 1)::VARCHAR)", payloadCopies));
        Exec($"COPY (SELECT i::BIGINT AS collection_id, TIMESTAMP '2026-09-28 00:00:00' + to_seconds(((i * 7919) % {rows})::BIGINT) AS collection_time, " +
             $"(1 + i % {Servers})::INTEGER AS server_id, {payload} AS payload FROM range({rows}) t(i)) " +
             $"TO '{path}' ({ParquetCompaction.BuildArchiveCopyOptions(ParquetCompaction.ArchiveRowGroupSize)})");
        return path;
    }

    private ArchiveService NewService()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        return new ArchiveService(initializer, _archiveDir) { History = _history };
    }

    private static long Bytes(string path) => new FileInfo(path).Length;

    private CompactionPassDetail OnlyCompactionRow(out StoreHousekeepingEvent row)
    {
        row = Assert.Single(_history.Read());
        Assert.Equal(StoreHousekeepingHistory.KindCompaction, row.Kind);
        return Assert.IsType<CompactionPassDetail>(row.Compaction);
    }

    /// <summary>
    /// A pass that merges a group writes one row: the groups, the files and bytes in and out, the memory peaks with their source,
    /// and the milliseconds the view rebuild took.
    /// </summary>
    [Fact]
    public void APassThatMerges_WritesOneRowWithItsCounts()
    {
        var inputs = new[] { "20260928_1000_wait_stats.parquet", "20260928_1100_wait_stats.parquet", "20260928_1200_wait_stats.parquet" }
            .Select(n => WriteUnsorted(n, 40_000, payloadCopies: 2)).ToList();
        var bytesIn = inputs.Sum(Bytes);

        var complete = NewService().CompactParquetFiles();

        Assert.True(complete);
        var detail = OnlyCompactionRow(out var row);
        Assert.Equal(StoreHousekeepingHistory.OutcomeOk, row.Outcome);
        Assert.Equal(1, detail.GroupsMerged);
        Assert.Equal(3, detail.FilesIn);
        Assert.Equal(1, detail.FilesOut);
        Assert.Equal(bytesIn, detail.BytesIn);
        Assert.Equal(Bytes(P("202609_wait_stats.parquet")), detail.BytesOut);
        Assert.Equal(0, detail.SortedAtSmallGroupBatches);
        Assert.Equal(0, detail.UnsortedFallbackBatches);
        Assert.Equal(0, detail.FilesRecut);
        Assert.True(detail.Seconds > 0);
        Assert.True(detail.StartUtc <= row.TimeUtc);
        Assert.True(detail.ProcessPeakWorkingSetBytes > 0, "no process peak was recorded");
        Assert.True(detail.DuckDbPeakMemoryBytes > 0, "no DuckDB peak was recorded");
        Assert.False(string.IsNullOrEmpty(detail.MemorySource));
    }

    /// <summary>A pass with nothing to merge, nothing to cut and nothing failing writes no row.</summary>
    [Fact]
    public void AnIdlePass_WritesNoRow()
    {
        /* One monthly file in the final form, already one row group: the merge leaves it and the cut finds it done. */
        Exec($"COPY (SELECT 1::BIGINT AS collection_id, TIMESTAMP '2026-09-28 00:00:00' AS collection_time, 1::INTEGER AS server_id) " +
             $"TO '{P("202609_wait_stats.parquet")}' ({ParquetCompaction.BuildArchiveCopyOptions(ParquetCompaction.SortedArchiveRowGroupSize)})");

        NewService().CompactParquetFiles();
        NewService().CompactParquetFiles();

        Assert.Empty(_history.Read());
    }

    /// <summary>A merge that falls back to the unsorted order is counted, and so is the pass's failure-free outcome.</summary>
    [Fact]
    public void AnUnsortedFallback_IsCounted()
    {
        WriteUnsorted("20260928_1000_query_store_stats.parquet", 60_000, payloadCopies: 4);
        var service = NewService();
        service.MergeMemoryLimit = "2MB";

        service.CompactParquetFiles();

        var detail = OnlyCompactionRow(out _);
        Assert.Equal(1, detail.GroupsMerged);
        Assert.Equal(1, detail.UnsortedFallbackBatches);
        Assert.Equal(0, detail.SortedAtSmallGroupBatches);
    }

    /// <summary>A sorted merge that outgrows the cap at the large row-group size and is sorted again at 2,048 rows is counted apart.</summary>
    [Fact]
    public void ASortedAgainAt2048Rows_IsCounted()
    {
        WriteUnsorted("20260928_1000_query_store_stats.parquet", 125_000, payloadCopies: 8);
        var service = NewService();
        service.MergeMemoryLimit = "128MB";

        service.CompactParquetFiles();

        var detail = OnlyCompactionRow(out _);
        Assert.Equal(1, detail.SortedAtSmallGroupBatches);
        Assert.Equal(0, detail.UnsortedFallbackBatches);
    }

    /// <summary>
    /// A pass that cuts one file and skips another that is over the table's merge batch budget counts both, and the skip under its
    /// reason.
    /// </summary>
    [Fact]
    public void ASkippedRecut_IsCountedByReason()
    {
        var small = WriteUnsorted("20260928_query_store_stats.parquet", 12_000);
        var big = WriteUnsorted("20260927_query_store_stats.parquet", 90_000, payloadCopies: 3);
        var budget = (Bytes(small) + Bytes(big)) / 2;
        var service = NewService();
        service.CompactionBatchInputBytes = budget;
        service.DailyCompactionBatchInputBytes = budget;
        service.SortedCompactionBatchInputBytes = budget;

        service.CompactParquetFiles();

        var detail = OnlyCompactionRow(out _);
        Assert.Equal(0, detail.GroupsMerged);
        Assert.Equal(1, detail.FilesRecut);
        Assert.Equal(1, detail.RecutSkippedOverBatchBudget);
        Assert.Equal(0, detail.RecutSkippedTimeBudget);
        Assert.True(detail.DuckDbPeakMemoryBytes > 0, "the cut's sort reported no DuckDB peak");
    }

    /// <summary>The files a spent time budget leaves for the next pass are counted under the time budget.</summary>
    [Fact]
    public void ARecutLeftForTheNextPass_IsCountedAsTheTimeBudget()
    {
        WriteUnsorted("20260928_query_store_stats.parquet", 12_000);
        WriteUnsorted("20260927_query_store_stats.parquet", 12_000);
        var service = NewService();
        service.DailyCompactionPassBudget = TimeSpan.Zero;

        service.CompactParquetFiles();

        var detail = OnlyCompactionRow(out _);
        Assert.Equal(1, detail.FilesRecut);
        Assert.Equal(1, detail.RecutSkippedTimeBudget);
    }

    /// <summary>A group whose merge throws writes a failed row with the error text, and the pass still returns.</summary>
    [Fact]
    public void AFailedMerge_WritesAFailedRowWithTheError()
    {
        File.WriteAllText(P("20260928_1000_wait_stats.parquet"), "this is not a parquet file");

        NewService().CompactParquetFiles();

        var detail = OnlyCompactionRow(out var row);
        Assert.Equal(StoreHousekeepingHistory.OutcomeFailed, row.Outcome);
        Assert.Contains("wait_stats", row.Message);
        Assert.Equal(0, detail.GroupsMerged);
    }
}
