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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9320. On one install, sorted monthly merges at the 512MB limit ran out of memory on two threads and fell back
/// to the unsorted order, whose row groups all mix servers. The sorted attempt now runs on one thread. The class
/// is in the CollectionResetGate collection with <see cref="SortedMonthlyMergeThreadsCompactionTests"/>, because
/// both set the one static <see cref="ParquetCompaction.MergeAttemptObservedForTests"/> callback.
/// </summary>
[Collection("CollectionResetGate")]
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
        /* The sorted size (#9407): about ten groups of 122,880 rows. */
        Assert.True(groups >= rows / ParquetCompaction.SortedArchiveRowGroupSize, $"{groups} row groups");
        /* Sorted by server then time, a group holds one server except where one server's run ends. */
        Assert.True(mixed <= 6, $"{mixed} of {groups} row groups mix servers");
    }

    [Theory]
    [InlineData("wait_stats")]
    [InlineData("perfmon_stats")]
    public void TheSlicedExportsUnsortedMergeKeepsCompactionsThreads(string table)
    {
        Assert.NotNull(ParquetCompaction.ServerOrderByFor(table));
        Assert.True(ParquetCompaction.ThreadsFor(table) > 1, "a monthly table's unsorted merge keeps more than one thread");

        var export = ArchiveService.BuildExportSliceMergePragma(table, "C:/spill");
        Assert.Equal(
            ParquetCompaction.BuildPragma(ParquetCompaction.DefaultMemoryLimit, ParquetCompaction.ThreadsFor(table), "C:/spill"),
            export);
    }

    /* A sorted monthly source with several servers, wide enough that a sort of it cannot fit in 2MB. */
    private string MakeWideSource(string path)
    {
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText =
            "COPY (SELECT i::BIGINT AS id, TIMESTAMP '2026-09-28 00:00:00' + to_seconds((i % 86400)::BIGINT) AS collection_time, " +
            "(1 + i % 5)::INTEGER AS server_id, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range(60000) t(i)) " +
            $"TO '{path}' ({ParquetCompaction.ArchiveCopyOptions})";
        cmd.ExecuteNonQuery();
        return path;
    }

    /// <summary>
    /// #9320. MergeBatchToFile with no threads argument: the sorted attempt runs on one thread, and when it runs out of
    /// memory the sorted retry at 2,048-row groups runs on one thread too, and the unsorted fallback after it runs on the
    /// compaction thread count for the table (two for a monthly table). The counts are what each attempt's connection
    /// was set to run on.
    /// </summary>
    [Theory]
    [InlineData("wait_stats")]
    [InlineData("perfmon_stats")]
    public void TheSortedAttemptRunsOnOneThread_AndTheUnsortedFallbackOnCompactionsThreads(string table)
    {
        var source = MakeWideSource(P("wide_source.parquet"));
        var output = P("fallback_out.parquet");
        var spill = P("spill");
        Directory.CreateDirectory(spill);
        var attempts = new List<(bool Sorted, int Threads)>();
        var fallbacks = new List<string>();
        var previous = ParquetCompaction.MergeAttemptObservedForTests;
        ParquetCompaction.MergeAttemptObservedForTests = (path, sorted, threads) =>
        {
            if (path == output)
            {
                lock (attempts) { attempts.Add((sorted, threads)); }
            }
        };

        try
        {
            ParquetCompaction.MergeBatchToFile(table, [source], output, spill,
                memoryLimit: "2MB", onSortFallback: (t, _, _) => fallbacks.Add(t));
        }
        finally
        {
            ParquetCompaction.MergeAttemptObservedForTests = previous;
        }

        Assert.Single(fallbacks);
        Assert.Equal([(true, 1), (true, 1), (false, ParquetCompaction.ThreadsFor(table))], attempts);
        Assert.Equal(2, ParquetCompaction.ThreadsFor(table));
    }
}

/// <summary>
/// #9320. The hourly compaction's own call into the merge passes no threads argument, so a sorted monthly merge's
/// first attempt runs on one thread. Drives <see cref="ArchiveService.CompactParquetFiles"/> over real files. The
/// class takes the write lock the compaction swap needs, so it runs alone like the other classes that drive it (#8142).
/// </summary>
[Collection("CollectionResetGate")]
public sealed class SortedMonthlyMergeThreadsCompactionTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly string _archiveDir;
    private DuckDbInitializer? _initializer;

    public SortedMonthlyMergeThreadsCompactionTests()
    {
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        _initializer?.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* a temp folder: the next run's folder has another name */ }
    }

    private string P(string name) => Path.Combine(_archiveDir, name).Replace(Path.DirectorySeparatorChar, '/');

    private void MakeParquet(string name, long from, long to)
    {
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText =
            "COPY (SELECT i::BIGINT AS id, TIMESTAMP '2026-09-28 00:00:00' + to_seconds(i::BIGINT) AS collection_time, " +
            $"(1 + i % 3)::INTEGER AS server_id FROM range({from}, {to}) t(i)) TO '{P(name)}' ({ParquetCompaction.ArchiveCopyOptions})";
        cmd.ExecuteNonQuery();
    }

    [Theory]
    [InlineData("wait_stats")]
    [InlineData("perfmon_stats")]
    public void ACompactionOfASortedMonthlyTable_RunsItsSortedMergeOnOneThread(string table)
    {
        MakeParquet($"20260928_1400_{table}.parquet", 0, 5_000);
        MakeParquet($"20260928_1500_{table}.parquet", 5_000, 10_000);

        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        var service = new ArchiveService(_initializer, _archiveDir) { WriteLockBudget = DuckDbInvestigationStoreTests.TestLockBudget };
        var attempts = new List<(bool Sorted, int Threads)>();
        var previous = ParquetCompaction.MergeAttemptObservedForTests;
        ParquetCompaction.MergeAttemptObservedForTests = (path, sorted, threads) =>
        {
            if (path.StartsWith(_archiveDir.Replace(Path.DirectorySeparatorChar, '/'), StringComparison.Ordinal))
            {
                lock (attempts) { attempts.Add((sorted, threads)); }
            }
        };

        try
        {
            Assert.True(service.CompactParquetFiles(), "a compaction unit was skipped because the write lock was not free within the test budget");
        }
        finally
        {
            ParquetCompaction.MergeAttemptObservedForTests = previous;
        }

        Assert.Equal([(true, 1)], attempts);
        Assert.True(File.Exists(P($"202609_{table}.parquet")), "the month's merged file is missing");
        Assert.NotEqual(1, ParquetCompaction.ThreadsFor(table));
    }
}
