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

    private ArchiveService NewService() => new(NewInitializer(), _archiveDir);

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
       sizes follow row counts. */
    private void MakeParquet(string fileName, long from, long to, string hour = "2026-09-28 14:00:00") =>
        Exec($"COPY (SELECT i AS id, TIMESTAMP '{hour}' + INTERVAL (i - {from}) SECOND AS collection_time, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range({from}, {to}) t(i)) TO '{P(fileName)}' ({ParquetCompaction.BuildArchiveCopyOptions()})");

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

        var service = new ArchiveService(initializer, _archiveDir);
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
        var service = new ArchiveService(initializer, _archiveDir);
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

    private static async Task<long> ViewScalarAsync(string dbPath, string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }
}
