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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5393. Compaction skipped query_snapshots since #933 (its plan XML expands about 30 times on read, so the
/// 200 MB merge budget runs out of the 4 GB cap), which left one file per archive pass: up to about 24 a day,
/// around 2,200 in the 3-month window, every one opened at bind by each <c>v_query_snapshots</c> read. It is now
/// merged one day at a time into <c>YYYYMMDD_query_snapshots.parquet</c> (and <c>_ptNNN</c> parts) with a much
/// smaller per-batch input budget. These tests pin the day grouping, the budget split, that a failed merge
/// leaves its inputs, that the view and a time-windowed read still see every row, that retention removes day
/// files, and that the table's monthly and imported shapes are still left alone.
/// </summary>
public sealed class ArchiveQuerySnapshotsDailyCompactionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public ArchiveQuerySnapshotsDailyCompactionTests()
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
        Exec($"COPY (SELECT i AS id, TIMESTAMP '{hour}' + INTERVAL (i - {from}) SECOND AS collection_time, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range({from}, {to}) t(i)) TO '{P(fileName)}' (FORMAT PARQUET, ROW_GROUP_SIZE 100)");

    private long SizeOf(string fileName) => new FileInfo(P(fileName).Replace("/", "\\")).Length;

    private string[] ArchiveFileNames() =>
        Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    /* Every row visible through the archive view's two globs for query_snapshots. */
    private (long Rows, long DistinctIds) Visible()
    {
        var globs = new List<string>();
        foreach (var pattern in new[] { "*_query_snapshots.parquet", "*_query_snapshots_pt???.parquet" })
        {
            if (Directory.GetFiles(_archiveDir, pattern).Length > 0)
                globs.Add(P(pattern));
        }
        Assert.NotEmpty(globs);
        var source = "[" + string.Join(", ", globs.Select(g => $"'{g}'")) + "]";
        return (Scalar($"SELECT count(*) FROM read_parquet({source})"),
                Scalar($"SELECT count(DISTINCT id) FROM read_parquet({source})"));
    }

    [Fact]
    public void ADaysPerCycleFiles_BecomeOneDayFile_WithTheSameRowsAndColumns()
    {
        MakeParquet("20260928_1400_query_snapshots.parquet", 0, 300, "2026-09-28 14:00:00");
        MakeParquet("20260928_1500_query_snapshots.parquet", 300, 600, "2026-09-28 15:00:00");
        MakeParquet("20260928_1600_query_snapshots.parquet", 600, 900, "2026-09-28 16:00:00");
        MakeParquet("20260929_0100_query_snapshots.parquet", 900, 1_000, "2026-09-29 01:00:00");
        MakeParquet("20260929_0200_query_snapshots.parquet", 1_000, 1_100, "2026-09-29 02:00:00");
        var columnsBefore = Scalar($"SELECT count(*) FROM (DESCRIBE SELECT * FROM read_parquet('{P("20260928_1400_query_snapshots.parquet")}'))");

        NewService().CompactParquetFiles();

        Assert.Equal(["20260928_query_snapshots.parquet", "20260929_query_snapshots.parquet"], ArchiveFileNames());
        Assert.Equal(900, Scalar($"SELECT count(*) FROM read_parquet('{P("20260928_query_snapshots.parquet")}')"));
        Assert.Equal(200, Scalar($"SELECT count(*) FROM read_parquet('{P("20260929_query_snapshots.parquet")}')"));
        Assert.Equal((1_100L, 1_100L), Visible());
        Assert.Equal(columnsBefore, Scalar($"SELECT count(*) FROM (DESCRIBE SELECT * FROM read_parquet('{P("20260928_query_snapshots.parquet")}'))"));
    }

    [Fact]
    public void ABatchOverTheBudget_SplitsIntoPartFiles_AndKeepsEveryRow()
    {
        for (var i = 0; i < 4; i++)
        {
            MakeParquet($"20260928_{14 + i}00_query_snapshots.parquet", i * 500, (i + 1) * 500, $"2026-09-28 {14 + i}:00:00");
        }

        var service = NewService();
        /* Room for two files per batch, not three: smallest-first greedy batching gives two batches of two. */
        service.DailyCompactionBatchInputBytes = 2 * Enumerable.Range(0, 4).Max(i => SizeOf($"20260928_{14 + i}00_query_snapshots.parquet")) + 1;

        service.CompactParquetFiles();

        Assert.Equal(["20260928_query_snapshots_pt001.parquet", "20260928_query_snapshots_pt002.parquet"], ArchiveFileNames());
        Assert.Equal((2_000L, 2_000L), Visible());
    }

    [Fact]
    public void NewFiles_GoToANewPt002_WhileAFullPt001StaysAsItIs()
    {
        MakeParquet("20260928_query_snapshots_pt001.parquet", 0, 2_000, "2026-09-28 00:00:00");
        MakeParquet("20260928_1500_query_snapshots.parquet", 2_000, 2_100, "2026-09-28 15:00:00");
        MakeParquet("20260928_1600_query_snapshots.parquet", 2_100, 2_200, "2026-09-28 16:00:00");
        var untouched = File.ReadAllBytes(P("20260928_query_snapshots_pt001.parquet").Replace("/", "\\"));

        var service = NewService();
        /* The existing part fills a batch of its own; the two new files fit one batch together. */
        service.DailyCompactionBatchInputBytes = SizeOf("20260928_query_snapshots_pt001.parquet");

        service.CompactParquetFiles();

        Assert.Equal(["20260928_query_snapshots_pt001.parquet", "20260928_query_snapshots_pt002.parquet"], ArchiveFileNames());
        Assert.Equal(untouched, File.ReadAllBytes(P("20260928_query_snapshots_pt001.parquet").Replace("/", "\\")));
        Assert.Equal((2_200L, 2_200L), Visible());
    }

    [Fact]
    public void ASecondPass_FoldsALaterHourIntoTheExistingDayFile()
    {
        MakeParquet("20260928_1400_query_snapshots.parquet", 0, 300, "2026-09-28 14:00:00");
        MakeParquet("20260928_1500_query_snapshots.parquet", 300, 600, "2026-09-28 15:00:00");
        var service = NewService();
        service.CompactParquetFiles();
        Assert.Equal(["20260928_query_snapshots.parquet"], ArchiveFileNames());

        MakeParquet("20260928_1600_query_snapshots.parquet", 600, 900, "2026-09-28 16:00:00");
        service.CompactParquetFiles();

        Assert.Equal(["20260928_query_snapshots.parquet"], ArchiveFileNames());
        Assert.Equal((900L, 900L), Visible());
    }

    [Fact]
    public void AFileAloneInItsBatch_IsLeftAsItIs()
    {
        MakeParquet("20260928_1400_query_snapshots.parquet", 0, 300, "2026-09-28 14:00:00");
        MakeParquet("20260928_1500_query_snapshots.parquet", 300, 600, "2026-09-28 15:00:00");

        var service = NewService();
        /* Smaller than either file: nothing fits beside anything, and a file over the budget is not rewritten
           by itself, which would only rename it while risking the memory the budget bounds. */
        service.DailyCompactionBatchInputBytes = 1;

        service.CompactParquetFiles();

        Assert.Equal(["20260928_1400_query_snapshots.parquet", "20260928_1500_query_snapshots.parquet"], ArchiveFileNames());
    }

    [Fact]
    public void AFailedMerge_LeavesEveryInputInPlace()
    {
        MakeParquet("20260928_1400_query_snapshots.parquet", 0, 300, "2026-09-28 14:00:00");
        MakeParquet("20260928_1500_query_snapshots.parquet", 300, 600, "2026-09-28 15:00:00");
        File.WriteAllText(P("20260928_1600_query_snapshots.parquet"), "this is not a parquet file");

        /* A second day, whose merge comes after the failed one in the same pass: the failure of one day does
           not stop the others. */
        MakeParquet("20260929_1400_query_snapshots.parquet", 1_000, 1_300, "2026-09-29 14:00:00");
        MakeParquet("20260929_1500_query_snapshots.parquet", 1_300, 1_600, "2026-09-29 15:00:00");

        NewService().CompactParquetFiles();

        Assert.Equal(
            ["20260928_1400_query_snapshots.parquet", "20260928_1500_query_snapshots.parquet", "20260928_1600_query_snapshots.parquet",
             "20260929_query_snapshots.parquet"],
            ArchiveFileNames());
        Assert.Equal(300, Scalar($"SELECT count(*) FROM read_parquet('{P("20260928_1400_query_snapshots.parquet")}')"));
        Assert.Equal(600, Scalar($"SELECT count(*) FROM read_parquet('{P("20260929_query_snapshots.parquet")}')"));
    }

    [Fact]
    public void TheMonthlyAndImportedShapesOfTheTable_AreStillLeftAlone()
    {
        MakeParquet("202609_query_snapshots.parquet", 0, 100);
        MakeParquet("202609_query_snapshots_pt001.parquet", 100, 200);
        MakeParquet("imported_202609_query_snapshots.parquet", 200, 300);
        MakeParquet("imported_20260928_1400_query_snapshots.parquet", 300, 400);
        MakeParquet("imported_20260928_1500_query_snapshots.parquet", 400, 500);

        NewService().CompactParquetFiles();

        Assert.Equal(
            ["202609_query_snapshots.parquet", "202609_query_snapshots_pt001.parquet", "imported_20260928_1400_query_snapshots.parquet",
             "imported_20260928_1500_query_snapshots.parquet", "imported_202609_query_snapshots.parquet"],
            ArchiveFileNames());
    }

    [Fact]
    public void OtherTables_StillMergeIntoOneMonthFile()
    {
        MakeParquet("20260928_1400_wait_stats.parquet", 0, 100);
        MakeParquet("20260929_1400_wait_stats.parquet", 100, 200);

        NewService().CompactParquetFiles();

        Assert.Equal(["202609_wait_stats.parquet"], ArchiveFileNames());
    }

    /// <summary>
    /// A day's swap journal is named <c>YYYYMMDD_query_snapshots.swap</c>. The journal reader matched only a
    /// 6-digit month, so a day whose delete was still pending was merged again over its own journal.
    /// </summary>
    [Fact]
    public void AnInputThatCannotBeDeletedYet_KeepsItsDayOutOfTheMerge_UntilItIsGone()
    {
        MakeParquet("20260928_query_snapshots.parquet", 0, 600, "2026-09-28 14:00:00");     /* merged output, in place */
        MakeParquet("20260928_1400_query_snapshots.parquet", 300, 600, "2026-09-28 14:05:00"); /* folded in, not yet deleted */
        MakeParquet("20260928_1500_query_snapshots.parquet", 600, 700, "2026-09-28 15:00:00");
        MakeParquet("20260928_1600_query_snapshots.parquet", 700, 800, "2026-09-28 16:00:00");
        File.WriteAllLines(P("20260928_query_snapshots.swap"),
        [
            "state|swapped",
            "output|replacing|20260928_query_snapshots.parquet",
            "input|20260928_1400_query_snapshots.parquet"
        ]);

        var service = NewService();
        using (new FileStream(P("20260928_1400_query_snapshots.parquet").Replace("/", "\\"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            service.CompactParquetFiles();
        }

        Assert.True(File.Exists(P("20260928_query_snapshots.swap")), "the journal was dropped while its input still existed");
        Assert.True(File.Exists(P("20260928_1500_query_snapshots.parquet")), "the day was merged while an earlier swap's delete was still pending");

        service.CompactParquetFiles();

        Assert.Equal(["20260928_query_snapshots.parquet"], ArchiveFileNames());
        Assert.Equal((800L, 800L), Visible());
    }

    [Fact]
    public void RetentionRemovesADayFileAndItsParts_PastTheCutoff()
    {
        var old = DateTime.UtcNow.AddMonths(-(RetentionService.ArchiveRetentionMonths + 2));
        var recent = DateTime.UtcNow;
        foreach (var name in new[]
        {
            $"{old:yyyyMMdd}_query_snapshots.parquet", $"{old:yyyyMMdd}_query_snapshots_pt001.parquet",
            $"{recent:yyyyMMdd}_query_snapshots.parquet", $"{recent:yyyyMMdd}_query_snapshots_pt001.parquet"
        })
        {
            File.WriteAllText(Path.Combine(_archiveDir, name), "parquet");
        }

        new RetentionService(_archiveDir).CleanupOldArchives();

        Assert.Equal([$"{recent:yyyyMMdd}_query_snapshots.parquet", $"{recent:yyyyMMdd}_query_snapshots_pt001.parquet"], ArchiveFileNames());
    }

    /// <summary>
    /// The real table and view: after compaction <c>v_query_snapshots</c> reads the day files and a read over
    /// one hour of the day returns that hour's rows (pruned by the footer's collection_time statistics, which
    /// the day file keeps for every row group).
    /// </summary>
    [Fact]
    public async Task TheViewReadsTheDayFile_AndAWindowedReadStillFindsItsRows()
    {
        var initializer = NewInitializer();
        await initializer.InitializeAsync();
        var dbPath = Path.Combine(_tempDir, "test.duckdb");

        for (var hour = 14; hour < 17; hour++)
        {
            await ArchiveHourAsync(dbPath, $"20260928_{hour}00_query_snapshots.parquet", hour);
        }

        var service = new ArchiveService(initializer, _archiveDir);
        service.CompactParquetFiles();

        Assert.Equal(["20260928_query_snapshots.parquet"], ArchiveFileNames());

        await initializer.CreateArchiveViewsAsync();
        Assert.Equal(3 * RowsPerHour, await ViewScalarAsync(dbPath, "SELECT COUNT(*) FROM v_query_snapshots"));
        Assert.Equal(RowsPerHour, await ViewScalarAsync(dbPath,
            "SELECT COUNT(*) FROM v_query_snapshots WHERE collection_time >= TIMESTAMP '2026-09-28 15:00:00' AND collection_time < TIMESTAMP '2026-09-28 16:00:00'"));

        /* Every row group of the day file carries collection_time statistics, and they prune: the day file has
           several row groups (2,048 rows each, the archive's row-group size), and the one-hour window's range
           overlaps some of them, not all. That is what lets a windowed read skip the others' plan text. */
        var meta = $"parquet_metadata('{P("20260928_query_snapshots.parquet")}') WHERE path_in_schema = 'collection_time'";
        var groups = Scalar($"SELECT count(*) FROM {meta}");
        Assert.True(groups > 1, $"the day file has {groups} row group(s); a window cannot prune one");
        Assert.Equal(0, Scalar($"SELECT count(*) FROM {meta} AND stats_min IS NULL"));
        var overlapping = Scalar($"SELECT count(*) FROM {meta} AND CAST(stats_max AS TIMESTAMP) >= TIMESTAMP '2026-09-28 15:00:00' AND CAST(stats_min AS TIMESTAMP) < TIMESTAMP '2026-09-28 16:00:00'");
        Assert.True(overlapping > 0 && overlapping < groups, $"{overlapping} of {groups} row groups overlap the one-hour window");
    }

    /* An hour of rows, one a second: three hours make a day file of about four 2,048-row groups. */
    private const int RowsPerHour = 2_500;

    /* imported_YYYYMMDD_query_snapshots.parquet (DataImportService prefixes imported_ to a day file) matched no
       grouping shape, so every pass logged "Unrecognized parquet file format" for it (#5393). It is a day shape
       of the table, grouped under its month, where compaction leaves it alone. */
    [Fact]
    public void AnImportedDayFile_IsRecognised_AndLeftAlone()
    {
        MakeParquet("imported_20260928_query_snapshots.parquet", 0, 100);
        MakeParquet("imported_20260928_query_snapshots_pt001.parquet", 100, 200);
        var logger = new CapturingLogger();

        new ArchiveService(NewInitializer(), _archiveDir, logger).CompactParquetFiles();

        Assert.Equal(["imported_20260928_query_snapshots.parquet", "imported_20260928_query_snapshots_pt001.parquet"], ArchiveFileNames());
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("Unrecognized parquet file format", StringComparison.Ordinal));
    }

    /* A live .archive-pending journal means the run that wrote the file died before it deleted the archived
       rows; recovery finishes that DELETE only while the named file is there. Folding it into the day file and
       removing it would make recovery read "never written", leave the rows in the table, and export them twice. */
    [Fact]
    public void AFileALiveArchiveJournalNames_IsNotFoldedIntoTheDayFile()
    {
        MakeParquet("20260928_1400_query_snapshots.parquet", 0, 300, "2026-09-28 14:00:00");
        MakeParquet("20260928_1500_query_snapshots.parquet", 300, 600, "2026-09-28 15:00:00");
        MakeParquet("20260928_1600_query_snapshots.parquet", 600, 900, "2026-09-28 16:00:00");
        File.WriteAllLines(P("query_snapshots.archive-pending"),
            ["cutoff|2026-09-28T16:00:00.0000000Z", "file|20260928_1600_query_snapshots.parquet"]);

        NewService().CompactParquetFiles();

        Assert.Equal(["20260928_1600_query_snapshots.parquet", "20260928_query_snapshots.parquet", "query_snapshots.archive-pending"], ArchiveFileNames());
        Assert.Equal(600, Scalar($"SELECT count(*) FROM read_parquet('{P("20260928_query_snapshots.parquet")}')"));

        /* Once the journal is gone the file folds in with the next pass. */
        File.Delete(P("query_snapshots.archive-pending"));
        NewService().CompactParquetFiles();
        Assert.Equal(["20260928_query_snapshots.parquet"], ArchiveFileNames());
        Assert.Equal(900, Scalar($"SELECT count(*) FROM read_parquet('{P("20260928_query_snapshots.parquet")}')"));
    }

    /* A first pass over months of per-cycle files merges one day at a time, slowly. Past the pass's time budget it
       starts no further daily merge, and the rest of the backlog goes on in the next pass. A zero budget lets
       exactly the first merge start, whatever the machine's speed. */
    [Fact]
    public void APassThatSpentItsTimeBudget_LeavesTheRestOfTheBacklogForTheNextPass()
    {
        foreach (var day in new[] { 26, 27, 28 })
        {
            var baseId = day * 1_000L;
            MakeParquet($"202609{day}_1400_query_snapshots.parquet", baseId, baseId + 100, $"2026-09-{day} 14:00:00");
            MakeParquet($"202609{day}_1500_query_snapshots.parquet", baseId + 100, baseId + 200, $"2026-09-{day} 15:00:00");
        }

        var service = NewService();
        service.DailyCompactionPassBudget = TimeSpan.Zero;
        var dayFile = new Regex(@"^\d{8}_query_snapshots\.parquet$");
        var perCycle = new Regex(@"^\d{8}_\d{4}_query_snapshots\.parquet$");

        service.CompactParquetFiles();
        Assert.Equal(1, ArchiveFileNames().Count(n => dayFile.IsMatch(n)));
        Assert.Equal(4, ArchiveFileNames().Count(n => perCycle.IsMatch(n)));
        Assert.Equal((600L, 600L), Visible());

        service.CompactParquetFiles();
        service.CompactParquetFiles();
        Assert.Equal(["20260926_query_snapshots.parquet", "20260927_query_snapshots.parquet", "20260928_query_snapshots.parquet"], ArchiveFileNames());
        Assert.Equal((600L, 600L), Visible());
    }

    /* The merge's peak memory follows the row group the writer buffers (#5393), so a daily table's merge output is
       cut by bytes as well as by rows. Six files of 1,000 rows of about 48 KB of plan text each (290 MB
       uncompressed): the row limit alone lets the writer pile several files into one group, the 32 MiB byte bound
       closes a group after each file's chunk. Another table's merge has no byte bound, so it cuts fewer groups. */
    [Fact]
    public void TheDailyMerge_CutsRowGroupsByBytes_AndOtherTablesAreNotCut()
    {
        var inputs = new List<string>();
        for (var f = 0; f < 6; f++)
        {
            var input = P($"wide_input_{f:D2}.parquet");
            Exec($"COPY (SELECT i AS id, TIMESTAMP '2026-09-28 14:00:00' + INTERVAL (i) SECOND AS collection_time, (SELECT string_agg(md5(i::VARCHAR || k::VARCHAR), '') FROM range(0, 1500) u(k)) AS query_plan FROM range({f * 1_000}, {(f + 1) * 1_000}) t(i)) TO '{input}' ({ParquetCompaction.ArchiveCopyOptions})");
            inputs.Add(input);
        }

        var spill = P("spill");
        Directory.CreateDirectory(spill);
        ParquetCompaction.MergeBatchToFile("query_snapshots", inputs, P("out_daily.parquet"), spill, threads: ParquetCompaction.ThreadsFor("query_snapshots"));
        ParquetCompaction.MergeBatchToFile("wait_stats", inputs, P("out_other.parquet"), spill, threads: 1);

        var daily = RowGroupsOf(P("out_daily.parquet"));
        var other = RowGroupsOf(P("out_other.parquet"));
        Assert.True(daily > other, $"{daily} row group(s) in the daily merge output against {other} without the byte bound");
        Assert.Equal(6_000, Scalar($"SELECT count(*) FROM read_parquet('{P("out_daily.parquet")}')"));
        Assert.Equal(0, ParquetCompaction.RowGroupBytesFor("wait_stats"));
        Assert.Equal(ParquetCompaction.DailyRowGroupBytes, ParquetCompaction.RowGroupBytesFor("query_snapshots"));
        Assert.Contains($"ROW_GROUP_SIZE_BYTES {ParquetCompaction.DailyRowGroupBytes}", ParquetCompaction.BuildArchiveCopyOptions(rowGroupBytes: ParquetCompaction.DailyRowGroupBytes), StringComparison.Ordinal);
        Assert.DoesNotContain("ROW_GROUP_SIZE_BYTES", ParquetCompaction.ArchiveCopyOptions, StringComparison.Ordinal);
    }

    private static long RowGroupsOf(string path) =>
        Scalar($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{path}')");

    private sealed class CapturingLogger : ILogger<ArchiveService>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }

    private static async Task ArchiveHourAsync(string dbPath, string fileName, int hour)
    {
        using var connection = new DuckDBConnection($"Data Source={dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = $@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, cpu_time_ms, total_elapsed_time_ms)
SELECT {hour} * 10000 + i, TIMESTAMP '2026-09-28 {hour}:00:00' + INTERVAL (i) SECOND, 1, 'example-sql-01', 55, 'Db', 'SELECT ' || i::VARCHAR, 'running', 10, 20 FROM range(0, {RowsPerHour}) t(i)";
            await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var path = Path.Combine(Path.GetDirectoryName(dbPath)!, "archive", fileName).Replace("\\", "/");
        using (var copy = connection.CreateCommand())
        {
            copy.CommandText = $"COPY query_snapshots TO '{path}' ({ParquetCompaction.BuildArchiveCopyOptions()})";
            await copy.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        using var delete = connection.CreateCommand();
        delete.CommandText = "DELETE FROM query_snapshots";
        await delete.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
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
