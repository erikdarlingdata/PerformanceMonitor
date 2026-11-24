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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5410. Compaction merged query_stats and query_store_stats into one file per month, so a one-hour read (the
/// Queries tab's default window) touched about 15-20% of a month's row groups and decoded about 27 rows for each
/// one it wanted. They are now merged one day at a time into <c>YYYYMMDD_table.parquet</c> (and <c>_ptNNN</c>
/// parts), the shape query_snapshots has had since #5393, so the same read touches about 4% of a day's row groups.
/// These tests mirror ArchiveQuerySnapshotsDailyCompactionTests for both tables, and pin the one thing the switch
/// can break: a month file written before it and a day file written after it never hold the same row, because
/// neither archive view dedups.
/// </summary>
public sealed class ArchiveQueryStatsDailyCompactionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public ArchiveQueryStatsDailyCompactionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
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

    private DuckDbInitializer NewInitializer()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        return initializer;
    }

    /* These tests read pruning and order off small row groups of a few thousand rows, so the sorted merges write the old size (#9407). */
    private ArchiveService NewService() => new(NewInitializer(), _archiveDir) { SortedRowGroupSizeOverride = ParquetCompaction.ArchiveRowGroupSize };

    private static void Exec(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long Scalar(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    /* Rows [from, to) over the hour starting at `hour`, one a second, with a payload wide enough that file
       sizes follow row counts. server_id is a column every archive file carries, and query_store_stats' merge
       sorts by it (#9255): one server here, so the sort keeps the time order the tests below read. */
    private void MakeParquet(string fileName, long from, long to, string hour = "2026-09-28 14:00:00") =>
        Exec($"COPY (SELECT i AS id, TIMESTAMP '{hour}' + INTERVAL (i - {from}) SECOND AS collection_time, 1 AS server_id, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range({from}, {to}) t(i)) TO '{P(fileName)}' ({ParquetCompaction.BuildArchiveCopyOptions()})");

    private long SizeOf(string fileName) => new FileInfo(P(fileName).Replace("/", "\\")).Length;

    private string[] ArchiveFileNames() =>
        Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    /* Every row visible through the archive view's two globs for the table. */
    private (long Rows, long DistinctIds) Visible(string table)
    {
        var globs = new List<string>();
        foreach (var pattern in new[] { $"*_{table}.parquet", $"*_{table}_pt???.parquet" })
        {
            if (Directory.GetFiles(_archiveDir, pattern).Length > 0)
                globs.Add(P(pattern));
        }
        Assert.NotEmpty(globs);
        var source = "[" + string.Join(", ", globs.Select(g => $"'{g}'")) + "]";
        return (Scalar($"SELECT count(*) FROM read_parquet({source})"),
                Scalar($"SELECT count(DISTINCT id) FROM read_parquet({source})"));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public void TheTableIsMergedPerDay_WithOneThreadAndTheDailyRowGroupBound(string table)
    {
        Assert.True(ParquetCompaction.IsDailyTable(table));
        Assert.False(ParquetCompaction.ShouldSkipCompaction(table, "20260928"));
        Assert.True(ParquetCompaction.ShouldSkipCompaction(table, "202609"));
        Assert.Equal(1, ParquetCompaction.ThreadsFor(table));
        Assert.Equal(ParquetCompaction.DailyRowGroupBytes, ParquetCompaction.RowGroupBytesFor(table));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public void ADaysPerCycleFiles_BecomeOneDayFile_WithTheSameRowsAndColumns(string table)
    {
        MakeParquet($"20260928_1400_{table}.parquet", 0, 300, "2026-09-28 14:00:00");
        MakeParquet($"20260928_1500_{table}.parquet", 300, 600, "2026-09-28 15:00:00");
        MakeParquet($"20260928_1600_{table}.parquet", 600, 900, "2026-09-28 16:00:00");
        MakeParquet($"20260929_0100_{table}.parquet", 900, 1_000, "2026-09-29 01:00:00");
        MakeParquet($"20260929_0200_{table}.parquet", 1_000, 1_100, "2026-09-29 02:00:00");
        var columnsBefore = Scalar($"SELECT count(*) FROM (DESCRIBE SELECT * FROM read_parquet('{P($"20260928_1400_{table}.parquet")}'))");

        NewService().CompactParquetFiles();

        Assert.Equal([$"20260928_{table}.parquet", $"20260929_{table}.parquet"], ArchiveFileNames());
        Assert.Equal(900, Scalar($"SELECT count(*) FROM read_parquet('{P($"20260928_{table}.parquet")}')"));
        Assert.Equal(200, Scalar($"SELECT count(*) FROM read_parquet('{P($"20260929_{table}.parquet")}')"));
        Assert.Equal((1_100L, 1_100L), Visible(table));
        Assert.Equal(columnsBefore, Scalar($"SELECT count(*) FROM (DESCRIBE SELECT * FROM read_parquet('{P($"20260928_{table}.parquet")}'))"));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public void ABatchOverTheBudget_SplitsIntoPartFiles_AndKeepsEveryRow(string table)
    {
        for (var i = 0; i < 4; i++)
        {
            MakeParquet($"20260928_{14 + i}00_{table}.parquet", i * 500, (i + 1) * 500, $"2026-09-28 {14 + i}:00:00");
        }

        var service = NewService();
        /* Room for two files per batch, not three: smallest-first greedy batching gives two batches of two. */
        service.DailyCompactionBatchInputBytes = 2 * Enumerable.Range(0, 4).Max(i => SizeOf($"20260928_{14 + i}00_{table}.parquet")) + 1;

        service.CompactParquetFiles();

        Assert.Equal([$"20260928_{table}_pt001.parquet", $"20260928_{table}_pt002.parquet"], ArchiveFileNames());
        Assert.Equal((2_000L, 2_000L), Visible(table));
    }

    [Fact]
    public void TheDefaultBudgetForTheTwoTables_IsTheDailyOne_NotTheMonthlyOne()
    {
        var service = NewService();
        Assert.Equal(ParquetCompaction.DailyBatchInputBytes, service.DailyCompactionBatchInputBytes);
        Assert.True(ParquetCompaction.DailyBatchInputBytes < ParquetCompaction.DefaultBatchInputBytes);
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public void RetentionRemovesADayFileAndItsParts_PastTheCutoff(string table)
    {
        var old = DateTime.UtcNow.AddMonths(-(RetentionService.ArchiveRetentionMonths + 2));
        var recent = DateTime.UtcNow;
        foreach (var name in new[]
        {
            $"{old:yyyyMMdd}_{table}.parquet", $"{old:yyyyMMdd}_{table}_pt001.parquet",
            $"{recent:yyyyMMdd}_{table}.parquet", $"{recent:yyyyMMdd}_{table}_pt001.parquet"
        })
        {
            File.WriteAllText(Path.Combine(_archiveDir, name), "parquet");
        }

        new RetentionService(_archiveDir).CleanupOldArchives();

        Assert.Equal([$"{recent:yyyyMMdd}_{table}.parquet", $"{recent:yyyyMMdd}_{table}_pt001.parquet"], ArchiveFileNames());
    }

    /// <summary>
    /// The switch. A store that ran before it holds a month file (<c>YYYYMM_table.parquet</c>, and parts) of rows
    /// merged by the old monthly pass, plus per-cycle files that pass had not folded in yet. Those per-cycle
    /// files were never in the month file, so merging them into day files cannot repeat a row, and the month
    /// file is left exactly as it is. Neither archive view dedups, so a repeat would show as a doubled count.
    /// The reads here go through the real archive view, not a hand glob.
    /// </summary>
    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public async Task TheMonthToDaySeam_ReturnsEveryRowExactlyOnce_ThroughTheArchiveView(string table)
    {
        var initializer = NewInitializer();
        await initializer.InitializeFromTemplateAsync();
        var dbPath = Path.Combine(_tempDir, "test.duckdb");

        /* Before the switch: days 24-26 merged into the month file and a part by the old pass. */
        MakeParquet($"202609_{table}.parquet", 0, 600, "2026-09-24 00:00:00");
        MakeParquet($"202609_{table}_pt001.parquet", 600, 900, "2026-09-26 00:00:00");
        /* Waiting at the switch, never merged: day 27 and the first hours of day 28 (the same month), then day 28's
           later hours arriving after it. */
        MakeParquet($"20260927_0900_{table}.parquet", 900, 1_100, "2026-09-27 09:00:00");
        MakeParquet($"20260927_1000_{table}.parquet", 1_100, 1_300, "2026-09-27 10:00:00");
        MakeParquet($"20260928_1400_{table}.parquet", 1_300, 1_500, "2026-09-28 14:00:00");
        var monthBytes = File.ReadAllBytes(P($"202609_{table}.parquet").Replace("/", "\\"));
        var partBytes = File.ReadAllBytes(P($"202609_{table}_pt001.parquet").Replace("/", "\\"));

        var service = new ArchiveService(initializer, _archiveDir) { SortedRowGroupSizeOverride = ParquetCompaction.ArchiveRowGroupSize };
        service.CompactParquetFiles();
        MakeParquet($"20260928_1500_{table}.parquet", 1_500, 1_700, "2026-09-28 15:00:00");
        service.CompactParquetFiles();

        Assert.Equal(
            [$"20260927_{table}.parquet", $"20260928_{table}.parquet", $"202609_{table}.parquet", $"202609_{table}_pt001.parquet"],
            ArchiveFileNames());
        Assert.Equal(monthBytes, File.ReadAllBytes(P($"202609_{table}.parquet").Replace("/", "\\")));
        Assert.Equal(partBytes, File.ReadAllBytes(P($"202609_{table}_pt001.parquet").Replace("/", "\\")));
        Assert.Equal(400, Scalar($"SELECT count(*) FROM read_parquet('{P($"20260927_{table}.parquet")}')"));

        await initializer.CreateArchiveViewsAsync();
        Assert.Equal(1_700, await ViewScalarAsync(dbPath, $"SELECT COUNT(*) FROM v_{table}"));
        Assert.Equal(1_700, await ViewScalarAsync(dbPath, $"SELECT COUNT(DISTINCT id) FROM v_{table}"));
        /* Windows across the seam return their own rows once: day 26 (the month file's last rows, a part) and day 27
           (the first day file). */
        Assert.Equal(300, await ViewScalarAsync(dbPath,
            $"SELECT COUNT(*) FROM v_{table} WHERE collection_time >= TIMESTAMP '2026-09-26 00:00:00' AND collection_time < TIMESTAMP '2026-09-27 00:00:00'"));
        Assert.Equal(400, await ViewScalarAsync(dbPath,
            $"SELECT COUNT(*) FROM v_{table} WHERE collection_time >= TIMESTAMP '2026-09-27 00:00:00' AND collection_time < TIMESTAMP '2026-09-28 00:00:00'"));
    }

    /// <summary>
    /// After compaction the view reads the day file, and a read over one hour of the day returns that hour's
    /// rows. The day file has several row groups, each carrying collection_time statistics, and the window
    /// overlaps some of them, not all: that is what lets the read skip the rest of the day's row groups.
    /// </summary>
    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public async Task TheViewReadsTheDayFile_AndAOneHourReadTouchesOnlyAFewOfItsRowGroups(string table)
    {
        var initializer = NewInitializer();
        await initializer.InitializeFromTemplateAsync();
        var dbPath = Path.Combine(_tempDir, "test.duckdb");

        /* One archive pass an hour, as in production: the day file takes each new hour in time order, so its row
           groups follow time. (A first pass over a whole day of equal-size files would order them by size.) */
        var service = new ArchiveService(initializer, _archiveDir) { SortedRowGroupSizeOverride = ParquetCompaction.ArchiveRowGroupSize };
        for (var hour = 10; hour < 18; hour++)
        {
            MakeParquet($"20260928_{hour}00_{table}.parquet", hour * 10_000L, hour * 10_000L + RowsPerHour, $"2026-09-28 {hour}:00:00");
            service.CompactParquetFiles();
        }

        Assert.Equal([$"20260928_{table}.parquet"], ArchiveFileNames());

        await initializer.CreateArchiveViewsAsync();
        Assert.Equal(8 * RowsPerHour, await ViewScalarAsync(dbPath, $"SELECT COUNT(*) FROM v_{table}"));
        Assert.Equal(RowsPerHour, await ViewScalarAsync(dbPath,
            $"SELECT COUNT(*) FROM v_{table} WHERE collection_time >= TIMESTAMP '2026-09-28 15:00:00' AND collection_time < TIMESTAMP '2026-09-28 16:00:00'"));

        var meta = $"parquet_metadata('{P($"20260928_{table}.parquet")}') WHERE path_in_schema = 'collection_time'";
        var groups = Scalar($"SELECT count(*) FROM {meta}");
        Assert.True(groups >= 8, $"the day file has {groups} row group(s)");
        Assert.Equal(0, Scalar($"SELECT count(*) FROM {meta} AND stats_min IS NULL"));
        var overlapping = Scalar($"SELECT count(*) FROM {meta} AND CAST(stats_max AS TIMESTAMP) >= TIMESTAMP '2026-09-28 15:00:00' AND CAST(stats_min AS TIMESTAMP) < TIMESTAMP '2026-09-28 16:00:00'");
        Assert.True(overlapping > 0 && overlapping * 4 <= groups, $"{overlapping} of {groups} row groups overlap the one-hour window of an eight-hour day");
    }

    /* An hour of rows, one a second: eight hours make a day file of about ten 2,048-row groups. */
    private const int RowsPerHour = 2_500;

    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public void TheMonthlyAndImportedShapesOfTheTable_AreLeftAlone(string table)
    {
        MakeParquet($"202609_{table}.parquet", 0, 100);
        MakeParquet($"202609_{table}_pt001.parquet", 100, 200);
        MakeParquet($"imported_202609_{table}.parquet", 200, 300);
        MakeParquet($"imported_20260928_1400_{table}.parquet", 300, 400);
        MakeParquet($"imported_20260928_1500_{table}.parquet", 400, 500);
        MakeParquet($"imported_20260927_{table}.parquet", 500, 600);
        MakeParquet($"imported_20260927_{table}_pt001.parquet", 600, 700);

        NewService().CompactParquetFiles();

        Assert.Equal(
            [$"202609_{table}.parquet", $"202609_{table}_pt001.parquet", $"imported_20260927_{table}.parquet", $"imported_20260927_{table}_pt001.parquet",
             $"imported_20260928_1400_{table}.parquet", $"imported_20260928_1500_{table}.parquet", $"imported_202609_{table}.parquet"],
            ArchiveFileNames());
    }

    /* A day's swap journal is named YYYYMMDD_table.swap: a day whose delete is still pending keeps out of the merge. */
    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store_stats")]
    public void AnInputThatCannotBeDeletedYet_KeepsItsDayOutOfTheMerge_UntilItIsGone(string table)
    {
        MakeParquet($"20260928_{table}.parquet", 0, 600, "2026-09-28 14:00:00");
        MakeParquet($"20260928_1400_{table}.parquet", 300, 600, "2026-09-28 14:05:00");
        MakeParquet($"20260928_1500_{table}.parquet", 600, 700, "2026-09-28 15:00:00");
        MakeParquet($"20260928_1600_{table}.parquet", 700, 800, "2026-09-28 16:00:00");
        File.WriteAllLines(P($"20260928_{table}.swap"),
        [
            "state|swapped",
            $"output|replacing|20260928_{table}.parquet",
            $"input|20260928_1400_{table}.parquet"
        ]);

        var service = NewService();
        using (new FileStream(P($"20260928_1400_{table}.parquet").Replace("/", "\\"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            service.CompactParquetFiles();
        }

        Assert.True(File.Exists(P($"20260928_{table}.swap")), "the journal was dropped while its input still existed");
        Assert.True(File.Exists(P($"20260928_1500_{table}.parquet")), "the day was merged while an earlier swap's delete was still pending");

        service.CompactParquetFiles();

        Assert.Equal([$"20260928_{table}.parquet"], ArchiveFileNames());
        Assert.Equal((800L, 800L), Visible(table));
    }

    private static string Text(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(cmd.ExecuteScalar())!;
    }

    /* One entry per server: its row count and a hash of every row's content, so two sources match only when each
       server's rows are the same. */
    private static string RowsPerServer(string source) =>
        Text("SELECT string_agg(server_id::VARCHAR || ':' || n::VARCHAR || ':' || h::VARCHAR, ',' ORDER BY server_id) " +
             "FROM (SELECT server_id, count(*) AS n, sum((hash(id, collection_time, payload) % 1000003)::HUGEINT) AS h " +
             $"FROM read_parquet('{source}') GROUP BY server_id)");

    /// <summary>
    /// #9255. The archiver writes the hot table's rows in time order, which interleaves the servers, so every row
    /// group of an archive file spanned every server and a read filtered to one server decoded them all. The day
    /// merge of query_store_stats sorts by server, then time: each output group holds one server, except the few
    /// that straddle a boundary between two, and the rows themselves are the same.
    /// </summary>
    [Fact]
    public void ADaysQueryStoreFiles_AreMergedIntoRowGroupsOfOneServer_WithEveryServersRowsUnchanged()
    {
        const string table = "query_store_stats";
        const int servers = 5;
        const int hours = 10;
        const int rowsPerHour = 5_000;

        for (var h = 0; h < hours; h++)
        {
            Exec($"COPY (SELECT ({h * rowsPerHour} + i)::BIGINT AS id, TIMESTAMP '2026-09-28 {h:D2}:00:00' + to_seconds((i * 3600 / {rowsPerHour})::BIGINT) AS collection_time, " +
                 $"(1 + i % {servers})::INTEGER AS server_id, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range({rowsPerHour}) t(i)) " +
                 $"TO '{P($"20260928_{h:D2}00_{table}.parquet")}' ({ParquetCompaction.ArchiveCopyOptions})");
        }

        var before = RowsPerServer(P($"*_{table}.parquet"));
        Assert.Equal(servers, before.Split(',').Length);

        NewService().CompactParquetFiles();

        Assert.Equal([$"20260928_{table}.parquet"], ArchiveFileNames());
        var merged = P($"20260928_{table}.parquet");
        Assert.Equal(before, RowsPerServer(merged));
        Assert.Equal(hours * rowsPerHour, Scalar($"SELECT count(*) FROM read_parquet('{merged}')"));

        var groups = Scalar($"SELECT count(*) FROM parquet_metadata('{merged}') WHERE path_in_schema = 'server_id'");
        var mixedGroups = Scalar($"SELECT count(*) FROM parquet_metadata('{merged}') WHERE path_in_schema = 'server_id' AND stats_min_value <> stats_max_value");
        Assert.True(groups >= 4 * servers, $"{groups} row groups: too few to tell sorted from unsorted");
        Assert.True(mixedGroups <= servers - 1, $"{mixedGroups} of {groups} row groups hold more than one server");
    }

    /* A day of the field shape, wide enough that a sort of it cannot fit in a few MB: 5 servers over 60,000 rows
       with a 64-character payload, one file. Returns the file's path. */
    private string MakeWideDay()
    {
        var source = P("wide_source_query_store_stats.parquet");
        Exec("COPY (SELECT i::BIGINT AS id, TIMESTAMP '2026-09-28 00:00:00' + to_seconds((i % 86400)::BIGINT) AS collection_time, " +
             "(1 + i % 5)::INTEGER AS server_id, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range(60000) t(i)) " +
             $"TO '{source}' ({ParquetCompaction.ArchiveCopyOptions})");
        return source;
    }

    /// <summary>
    /// #9255. The sorted merge cannot spill the parquet writer's own allocations, so a batch that outgrows its limit
    /// fails with an out-of-memory error. The merge then reruns the batch unsorted at the default limit: no throw, a
    /// warning, and every row in the output.
    /// </summary>
    [Fact]
    public void ASortedMergeThatRunsOutOfMemory_FallsBackToTheUnsortedMerge_WithEveryRow()
    {
        var source = MakeWideDay();
        var output = P("fallback_out.parquet");
        var fallbacks = new List<(string Table, int Files, string Error)>();

        ParquetCompaction.MergeBatchToFile("query_store_stats", [source], output, P("spill"),
            memoryLimit: "2MB", onSortFallback: (t, f, e) => fallbacks.Add((t, f, e)));

        Assert.Equal(60_000, Scalar($"SELECT count(*) FROM read_parquet('{output}')"));
        Assert.Equal(RowsPerServer(source), RowsPerServer(output));
        var fallback = Assert.Single(fallbacks);
        Assert.Equal("query_store_stats", fallback.Table);
        Assert.Equal(1, fallback.Files);
        Assert.NotEmpty(fallback.Error);
    }

    /// <summary>
    /// #9255. Only memory and temp space send a sorted merge to the unsorted fallback. A COPY that fails for another
    /// reason (here an output folder that does not exist) reaches the caller unchanged.
    /// </summary>
    [Fact]
    public void ASortedMergeThatFailsForAnotherReason_StillThrows_WithoutAFallbackWarning()
    {
        var source = MakeWideDay();
        var fallbacks = new List<string>();

        Assert.Throws<DuckDBException>(() => ParquetCompaction.MergeBatchToFile("query_store_stats", [source],
            P("no_such_folder/out.parquet"), P("spill"), onSortFallback: (t, _, _) => fallbacks.Add(t)));

        Assert.Empty(fallbacks);
    }

    /// <summary>
    /// #9255. A sorted merge runs at the lower limit unless the caller passes one, and the tables that keep no order
    /// stay at the default.
    /// </summary>
    [Fact]
    public void TheSortedMergeRunsAtTheLowerLimit_UnlessTheCallerPassesOne()
    {
        Assert.Equal(ParquetCompaction.SortedMergeMemoryLimit, ParquetCompaction.EffectiveMemoryLimit("query_store_stats", null));
        Assert.Equal("1GB", ParquetCompaction.EffectiveMemoryLimit("query_store_stats", "1GB"));
        Assert.Equal(ParquetCompaction.SortedMergeMemoryLimit, ParquetCompaction.EffectiveMemoryLimit("query_stats", null));
        Assert.Equal(ParquetCompaction.DefaultMemoryLimit, ParquetCompaction.EffectiveMemoryLimit("query_snapshots", null));
    }

    /// <summary>
    /// #9258. The monthly merges sort by server too: a month of a monthly table (here wait_stats) merges into row
    /// groups that each hold one server, except the few that straddle a boundary between two, and every server's rows
    /// are the same as went in.
    /// </summary>
    [Fact]
    public void AMonthsFilesOfAMonthlyTable_AreMergedIntoRowGroupsOfOneServer_WithEveryServersRowsUnchanged()
    {
        const string table = "wait_stats";
        const int servers = 5;
        const int hours = 10;
        const int rowsPerHour = 5_000;

        for (var h = 0; h < hours; h++)
        {
            Exec($"COPY (SELECT ({h * rowsPerHour} + i)::BIGINT AS id, TIMESTAMP '2026-09-28 {h:D2}:00:00' + to_seconds((i * 3600 / {rowsPerHour})::BIGINT) AS collection_time, " +
                 $"(1 + i % {servers})::INTEGER AS server_id, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range({rowsPerHour}) t(i)) " +
                 $"TO '{P($"20260928_{h:D2}00_{table}.parquet")}' ({ParquetCompaction.ArchiveCopyOptions})");
        }

        var before = RowsPerServer(P($"*_{table}.parquet"));
        Assert.Equal(servers, before.Split(',').Length);

        NewService().CompactParquetFiles();

        Assert.Equal([$"202609_{table}.parquet"], ArchiveFileNames());
        var merged = P($"202609_{table}.parquet");
        Assert.Equal(before, RowsPerServer(merged));
        Assert.Equal(hours * rowsPerHour, Scalar($"SELECT count(*) FROM read_parquet('{merged}')"));

        var groups = Scalar($"SELECT count(*) FROM parquet_metadata('{merged}') WHERE path_in_schema = 'server_id'");
        var mixedGroups = Scalar($"SELECT count(*) FROM parquet_metadata('{merged}') WHERE path_in_schema = 'server_id' AND stats_min_value <> stats_max_value");
        Assert.True(groups >= 4 * servers, $"{groups} row groups: too few to tell sorted from unsorted");
        Assert.True(mixedGroups <= servers - 1, $"{mixedGroups} of {groups} row groups hold more than one server");
    }

    /// <summary>
    /// #9258. A sorted monthly table's batches use the smaller sorted budget, so a month over it splits into
    /// <c>_ptNNN</c> parts (each sorted on its own) with every row kept, while a monthly table that keeps no order
    /// still merges the same files into one file under the default 200 MB budget.
    /// </summary>
    [Fact]
    public void ASortedMonthlyTablesBatches_UseTheSortedBudget_AndAnUnsortedTableKeepsTheDefaultOne()
    {
        const int files = 6;
        for (var h = 0; h < files; h++)
        {
            MakeParquet($"20260928_{h:D2}00_wait_stats.parquet", h * 1000L, (h + 1) * 1000L, $"2026-09-28 {h:D2}:00:00");
            MakeParquet($"20260928_{h:D2}00_some_unsorted_table.parquet", h * 1000L, (h + 1) * 1000L, $"2026-09-28 {h:D2}:00:00");
        }

        var perFile = SizeOf("20260928_0000_wait_stats.parquet");
        var service = NewService();
        service.SortedCompactionBatchInputBytes = perFile * 5 / 2;
        service.CompactParquetFiles();

        Assert.Equal(
            ["202609_some_unsorted_table.parquet", "202609_wait_stats_pt001.parquet", "202609_wait_stats_pt002.parquet", "202609_wait_stats_pt003.parquet"],
            ArchiveFileNames());
        Assert.Equal((files * 1000L, files * 1000L), Visible("wait_stats"));
        Assert.Equal((files * 1000L, files * 1000L), Visible("some_unsorted_table"));
    }

    /// <summary>
    /// #9258. A sorted monthly merge that runs out of memory reruns unsorted at the default limit: one fallback
    /// callback for the table, no throw, and every row in the output.
    /// </summary>
    [Fact]
    public void ASortedMonthlyMergeThatRunsOutOfMemory_FallsBackOnce_AndWritesEveryRow()
    {
        var source = MakeWideDay();
        var output = P("monthly_fallback_out.parquet");
        var fallbacks = new List<(string Table, int Files, string Error)>();

        ParquetCompaction.MergeBatchToFile("wait_stats", [source], output, P("spill"),
            memoryLimit: "2MB", onSortFallback: (t, f, e) => fallbacks.Add((t, f, e)));

        Assert.Equal(60_000, Scalar($"SELECT count(*) FROM read_parquet('{output}')"));
        Assert.Equal(RowsPerServer(source), RowsPerServer(output));
        var fallback = Assert.Single(fallbacks);
        Assert.Equal("wait_stats", fallback.Table);
        Assert.Equal(1, fallback.Files);
    }

    /// <summary>
    /// #9258. Every archivable table that is merged per month sorts by server on its OWN time column (capture_time
    /// for the config snapshots, alert_time for the alert log): a wrong or missing column would fail that table's
    /// merge on every pass. Each table here merges two files that carry only its time column and server_id, with no
    /// fallback, and comes out with one server per row group.
    /// </summary>
    [Fact]
    public void EveryMonthlyArchivableTable_MergesSortedOnItsOwnTimeColumn_WithoutFallingBack()
    {
        var checkedTables = 0;
        foreach (var (table, timeColumn) in ArchiveService.ArchivableTables)
        {
            if (ParquetCompaction.IsDailyTable(table) || table == "query_snapshots" || table == "query_stats")
            {
                continue;
            }

            var inputs = new List<string>();
            for (var h = 0; h < 2; h++)
            {
                var path = P($"seed_{table}_{h}.parquet");
                Exec($"COPY (SELECT TIMESTAMP '2026-09-28 {h:D2}:00:00' + to_seconds(i::BIGINT) AS {timeColumn}, (1 + i % 3)::INTEGER AS server_id " +
                     $"FROM range(300) t(i)) TO '{path}' ({ParquetCompaction.ArchiveCopyOptions})");
                inputs.Add(path);
            }

            var output = P($"merged_{table}.parquet");
            var fallbacks = new List<string>();
            ParquetCompaction.MergeBatchToFile(table, inputs, output, P("spill"), rowGroupSize: 100,
                onSortFallback: (t, _, _) => fallbacks.Add(t));

            Assert.Empty(fallbacks);
            Assert.Equal(600, Scalar($"SELECT count(*) FROM read_parquet('{output}')"));
            Assert.True(Scalar($"SELECT count(*) FROM parquet_metadata('{output}') WHERE path_in_schema = 'server_id' AND stats_min_value <> stats_max_value") <= 2,
                $"{table}: a row group holds more than one server");
            checkedTables++;
        }

        Assert.True(checkedTables >= 20, $"only {checkedTables} monthly tables were checked");
    }

    private static async Task<long> ViewScalarAsync(string dbPath, string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }
}
