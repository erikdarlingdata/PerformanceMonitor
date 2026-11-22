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
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9320. A sorted monthly merge at the 512MB limit ran out of memory on two threads for field files (more rows
/// per on-disk MB than the seeded files its byte budget was measured on) and fell back to the unsorted order,
/// whose row groups all mix servers. The sorted attempt now runs on one thread.
/// </summary>
public sealed class SortedMonthlyMergeThreadsTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "SortedMonthlyMergeThreads_" + Guid.NewGuid().ToString("N"));

    public SortedMonthlyMergeThreadsTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); }
        catch (IOException) { /* a temp folder: the next run's folder has another name */ }
    }

    private string P(string name) => Path.Combine(_dir, name).Replace('\\', '/');

    private static long Scalar(DuckDBConnection con, string sql)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public void ASortedMonthlyMergeOfAFieldShapedFile_FinishesSortedWithoutTheFallback()
    {
        /* Field shape: perfmon_stats of one install was 1.16M rows in 8 MB on disk. Numeric columns that repeat in
           runs compress to a few bytes a row, six servers interleaved in time order so the sort has to move them. */
        const int rows = 1_162_302;
        var source = P("20261001_0000_perfmon_stats.parquet");
        var output = P("202610_perfmon_stats.parquet");
        var spill = P("spill");
        Directory.CreateDirectory(spill);

        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        using (var cmd = con.CreateCommand())
        {
            cmd.CommandText =
                "COPY (SELECT TIMESTAMP '2026-10-01 00:00:00' + to_seconds((i * 2592000 // " + rows + ")::BIGINT) AS collection_time, " +
                "(1 + i % 6)::INTEGER AS server_id, 'server' || (1 + i % 6)::VARCHAR AS server_name, " +
                "((i // 50) * 3 + (hash(i, 1) % 97))::BIGINT AS counter_a, ((i // 50) * 7 + (hash(i, 2) % 97))::BIGINT AS counter_b, " +
                "((i // 50) * 11 + (hash(i, 3) % 97))::BIGINT AS counter_c, ((i // 50) * 13 + (hash(i, 4) % 97))::BIGINT AS counter_d, " +
                "((i // 50) * 17 + (hash(i, 5) % 97))::BIGINT AS counter_e, ((i // 50) * 19 + (hash(i, 6) % 97))::BIGINT AS counter_f " +
                $"FROM range({rows}) t(i)) TO '{source}' ({ParquetCompaction.ArchiveCopyOptions})";
            cmd.ExecuteNonQuery();
        }

        var fallbacks = new List<string>();
        ParquetCompaction.MergeBatchToFile("perfmon_stats", [source], output, spill,
            onSortFallback: (table, _, error) => fallbacks.Add($"{table}: {error}"));

        Assert.Empty(fallbacks);
        Assert.Equal(rows, Scalar(con, $"SELECT count(*) FROM read_parquet('{output}')"));
        var mixed = Scalar(con,
            $"SELECT count(*) FROM (SELECT row_group_id FROM parquet_metadata('{output}') WHERE path_in_schema = 'server_id' " +
            "GROUP BY row_group_id HAVING min(stats_min) <> max(stats_max))");
        var groups = Scalar(con, $"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{output}')");
        Assert.True(groups > 100, $"{groups} row groups");
        /* Sorted by server then time, a group holds one server except where one server's run ends. */
        Assert.True(mixed <= 10, $"{mixed} of {groups} row groups mix servers");
    }

    [Theory]
    [InlineData("wait_stats")]
    [InlineData("perfmon_stats")]
    public void TheSortedAttemptRunsOnOneThread_TheSlicedExportsUnsortedMergeKeepsCompactionsThreads(string table)
    {
        Assert.NotNull(ParquetCompaction.ServerOrderByFor(table));
        Assert.Equal(1, ParquetCompaction.ThreadsFor(table, sortedAttempt: true));
        Assert.Equal(ParquetCompaction.ThreadsFor(table), ParquetCompaction.ThreadsFor(table, sortedAttempt: false));
        Assert.True(ParquetCompaction.ThreadsFor(table) > 1, "a monthly table's unsorted merge keeps more than one thread");

        var export = ArchiveService.BuildExportSliceMergePragma(table, "C:/spill");
        Assert.Equal(
            ParquetCompaction.BuildPragma(ParquetCompaction.DefaultMemoryLimit, ParquetCompaction.ThreadsFor(table), "C:/spill"),
            export);
        Assert.NotEqual(
            ParquetCompaction.BuildPragma(ParquetCompaction.DefaultMemoryLimit, ParquetCompaction.ThreadsFor(table, sortedAttempt: true), "C:/spill"),
            export);
    }
}
