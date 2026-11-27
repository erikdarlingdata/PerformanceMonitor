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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9421. The archive layout section of <c>get_store_health</c>: row groups, mixed-server groups, files the one-time re-cut still
/// owes and the leftover swap files, read from parquet footers without the store's locks. Every test runs on a temp folder.
/// </summary>
public sealed class ArchiveLayoutReadTests : IDisposable
{
    private const int Servers = 6;

    /* Dated a few days back, not fixed: the layout names a file that retention deletes within a day, which a fixed date
       would become once it aged out. */
    private static readonly string DayFile = $"{DateTime.UtcNow.Date.AddDays(-3):yyyyMMdd}_query_store_stats.parquet";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;

    public ArchiveLayoutReadTests()
    {
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    private static long Scalar(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    /* A file written the way the exports are: servers interleaved, 2,048-row groups, so every group mixes servers. */
    private string WriteMixed(string fileName, int rows)
    {
        var path = P(fileName);
        Exec($"COPY (SELECT i::BIGINT AS collection_id, TIMESTAMP '2026-09-01 00:00:00' + to_seconds(i) AS collection_time, " +
             $"(1 + i % {Servers})::INTEGER AS server_id FROM range({rows}) t(i)) " +
             $"TO '{path}' ({ParquetCompaction.BuildArchiveCopyOptions(ParquetCompaction.ArchiveRowGroupSize)})");
        return path;
    }

    /* A file the sorted merge wrote: four servers of 24,576 rows, one server per row group of that size (above the size the
       re-cut takes, so the file is not due for it). */
    private string WriteSorted(string fileName)
    {
        var path = P(fileName);
        Exec($"COPY (SELECT i::BIGINT AS collection_id, TIMESTAMP '2026-09-01 00:00:00' + to_seconds(i) AS collection_time, " +
             $"(1 + i % 4)::INTEGER AS server_id FROM range(98304) t(i) ORDER BY server_id, collection_time) " +
             $"TO '{path}' ({ParquetCompaction.BuildArchiveCopyOptions(24_576)})");
        return path;
    }

    private StoreHealthReader NewReader() =>
        new(_duckDb, _archiveDir, new StoreHousekeepingHistory(Path.Combine(_tempDir, "store-health-history.json")));

    private static long MixedGroups(string file) =>
        Scalar($"SELECT count(*) FROM parquet_metadata('{file}') WHERE path_in_schema = 'server_id' AND TRY_CAST(stats_min AS BIGINT) <> TRY_CAST(stats_max AS BIGINT)");

    /// <summary>
    /// One sorted file, one mixed file and leftover swap files: the layout reports the row groups, the mixed groups of the one file
    /// that has them, the file the re-cut still owes and why it is skipping it, and the leftover counts.
    /// </summary>
    [Fact]
    public void TheLayout_ReportsAMixedFileAndLeftoverSwapFiles()
    {
        var sorted = WriteSorted("202609_wait_stats.parquet");
        var mixed = WriteMixed("202608_wait_stats.parquet", 30_000);
        WriteMixed("202609_other_table.parquet", 5_000);
        File.WriteAllText(P("202608_wait_stats.swap"), "swapping");
        File.WriteAllText(P("202607_wait_stats.parquet.tmp"), "x");
        File.WriteAllText(P("202607_wait_stats.parquet.replaced"), "x");
        File.WriteAllText(P("202606_wait_stats.parquet.replaced"), "x");

        var layout = NewReader().ReadWithLayout().Layout;

        Assert.NotNull(layout);
        Assert.False(layout.Partial);
        Assert.Equal(3, layout.FilesTotal);
        Assert.Equal(3, layout.FilesRead);
        Assert.Equal(1, layout.LeftoverTmp);
        Assert.Equal(1, layout.LeftoverSwap);
        Assert.Equal(2, layout.LeftoverReplaced);

        var wait = Assert.Single(layout.Tables, t => t.Table == "wait_stats");
        Assert.Equal(2, wait.Files);
        Assert.Equal(new FileInfo(sorted).Length + new FileInfo(mixed).Length, wait.Bytes);
        Assert.Equal(0, MixedGroups(sorted));
        Assert.Equal(MixedGroups(mixed), wait.MixedServerGroups);
        Assert.True(wait.MixedServerGroups > 1);
        Assert.Equal(Scalar($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{sorted}')")
                     + Scalar($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{mixed}')"), wait.RowGroups);
        Assert.Equal(ParquetCompaction.ArchiveRowGroupSize, Scalar($"SELECT max(row_group_num_rows) FROM parquet_metadata('{mixed}')"));
        Assert.Equal(24_576, wait.LargestGroupRows);
        Assert.Equal(1, wait.FilesNeedingRecut);
        var skip = Assert.Single(wait.RecutSkips);
        Assert.Equal(ArchiveLayoutReader.ReasonLiveJournal, skip.Reason);
        Assert.Equal(1, skip.Files);

        var other = Assert.Single(layout.Tables, t => t.Table == "other_table");
        Assert.Null(other.MixedServerGroups);
        Assert.Equal(0, other.FilesNeedingRecut);
    }

    /// <summary>A file the re-cut owes with nothing in its way is counted as needing it and has no skip reason.</summary>
    [Fact]
    public void AFileDueForTheRecut_WithNothingInItsWay_IsCountedWithNoSkip()
    {
        WriteMixed(DayFile, 30_000);

        var layout = NewReader().ReadWithLayout().Layout;

        var table = Assert.Single(layout!.Tables);
        Assert.Equal(1, table.FilesNeedingRecut);
        Assert.Empty(table.RecutSkips);
    }

    /* The same 2,048-row groups of long rows: each group holds more than half the sorted byte bound, so a cut would not
       change it (#9455). */
    private string WriteWide(string fileName, int rows)
    {
        var path = P(fileName);
        var payload = string.Join(" || ", Enumerable.Repeat("md5(i::VARCHAR) || md5((i + 1)::VARCHAR)", 120));
        Exec($"COPY (SELECT i::BIGINT AS collection_id, TIMESTAMP '2026-09-01 00:00:00' + to_seconds(i) AS collection_time, " +
             $"(1 + i % {Servers})::INTEGER AS server_id, {payload} AS payload FROM range({rows}) t(i)) " +
             $"TO '{path}' ({ParquetCompaction.BuildArchiveCopyOptions(ParquetCompaction.ArchiveRowGroupSize)})");
        return path;
    }

    /// <summary>
    /// A file of small row groups whose groups already hold half the byte bound is at its target, so the re-cut does not pick it
    /// and the layout does not count it. The same row count of short rows is counted. Both agree with the re-cut's own rule.
    /// </summary>
    [Fact]
    public void AFileAtItsByteTarget_IsNotCountedAsNeedingTheRecut()
    {
        var wide = WriteWide(DayFile, 4_096);
        var narrow = WriteMixed($"{DateTime.UtcNow.Date.AddDays(-4):yyyyMMdd}_query_store_stats.parquet", 4_096);
        using (var con = new DuckDBConnection("DataSource=:memory:"))
        {
            con.Open();
            Assert.False(ParquetCompaction.NeedsRecut(con, wide));
            Assert.True(ParquetCompaction.NeedsRecut(con, narrow));
        }

        var layout = NewReader().ReadWithLayout().Layout;

        var table = Assert.Single(layout!.Tables);
        Assert.Equal(1, table.FilesNeedingRecut);
    }

    /// <summary>
    /// #9409. Text that repeats makes a group the byte bound cut read small in the footer, under half the bound, so the row and
    /// byte rule alone would pick the sorted merge's own output on every pass. The merge stamps it, and the layout does not count
    /// a stamped file as owing the re-cut, the same as the re-cut. The same shape without the stamp is counted.
    /// </summary>
    [Fact]
    public void AStampedFileOfRepeatingText_IsNotCountedAsNeedingTheRecut_AndTheSameShapeUnstamped_Is()
    {
        var sourceDir = Path.Combine(_tempDir, "source");
        var spillDir = Path.Combine(_tempDir, "spill").Replace("\\", "/");
        Directory.CreateDirectory(sourceDir);
        Directory.CreateDirectory(spillDir);
        var source = Path.Combine(sourceDir, "source.parquet").Replace("\\", "/");
        Exec($"COPY (SELECT i::BIGINT AS collection_id, TIMESTAMP '2026-09-28 00:00:00' + to_seconds(((i * 7919) % 20000)::BIGINT) AS collection_time, " +
             $"(1 + i % {Servers})::INTEGER AS server_id, repeat(md5((i % 200)::VARCHAR), 128) AS payload FROM range(20000) t(i)) " +
             $"TO '{source}' ({ParquetCompaction.BuildArchiveCopyOptions(ParquetCompaction.ArchiveRowGroupSize)})");

        var stamped = P(DayFile);
        ParquetCompaction.MergeBatchToFile("query_store_stats", [source], stamped, spillDir);

        /* The shape the byte clause alone would pick: several groups, none near the row cap, all under half the byte bound. */
        var groups = Scalar($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{stamped}')");
        var maxRows = Scalar($"SELECT max(row_group_num_rows) FROM parquet_metadata('{stamped}')");
        Assert.True(groups > 1, $"{groups} row group(s)");
        Assert.True(maxRows < ParquetCompaction.RecutBelowRowGroupRows, $"groups of {maxRows} rows");
        using (var con = new DuckDBConnection("DataSource=:memory:"))
        {
            con.Open();
            Assert.True(ParquetCompaction.MaxRowGroupUncompressedBytes(con, stamped) < ParquetCompaction.SortedRowGroupBytes / 2);
            Assert.False(ParquetCompaction.NeedsRecut(con, stamped));
        }

        var layout = NewReader().ReadWithLayout().Layout;

        Assert.Equal(0, Assert.Single(layout!.Tables).FilesNeedingRecut);

        /* The same rows at the same group size, without the stamp. */
        var unstamped = P($"{DateTime.UtcNow.Date.AddDays(-4):yyyyMMdd}_query_store_stats.parquet");
        Exec($"COPY (SELECT * FROM read_parquet('{stamped}')) TO '{unstamped}' ({ParquetCompaction.BuildArchiveCopyOptions((int)maxRows)})");
        Assert.True(Scalar($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{unstamped}')") > 1);
        Assert.True(Scalar($"SELECT max(row_group_num_rows) FROM parquet_metadata('{unstamped}')") < ParquetCompaction.RecutBelowRowGroupRows);
        using (var con = new DuckDBConnection("DataSource=:memory:"))
        {
            con.Open();
            Assert.True(ParquetCompaction.NeedsRecut(con, unstamped));
        }

        layout = NewReader().ReadWithLayout().Layout;

        Assert.Equal(1, Assert.Single(layout!.Tables).FilesNeedingRecut);
    }

    /// <summary>A file larger than the table's merge batch budget is named as skipped, with that reason.</summary>
    [Fact]
    public void AFileOverTheBatchBudget_IsSkippedWithThatReason()
    {
        var file = WriteMixed(DayFile, 30_000);
        var reader = NewReader();
        reader.LayoutBatchBudgetForTests = _ => new FileInfo(file).Length - 1;

        var table = Assert.Single(reader.ReadWithLayout().Layout!.Tables);

        Assert.Equal(1, table.FilesNeedingRecut);
        var skip = Assert.Single(table.RecutSkips);
        Assert.Equal(ArchiveLayoutReader.ReasonOverBatchBudget, skip.Reason);
        Assert.Equal(1, skip.Files);
    }

    /// <summary>
    /// A file the re-cut passes over at this build, on the persisted skip list (a failed re-cut, a sort that ran out of memory, a
    /// merge that wrote it at small groups), shows as skipped. The same entry under another build does not, since a new build
    /// tries the file again.
    /// </summary>
    [Fact]
    public void AFileOnThePersistedSkipList_ShowsAsSkippedAtThisBuild()
    {
        WriteMixed(DayFile, 30_000);
        var listPath = P("recut-skipped.txt");
        File.WriteAllText(listPath, DayFile + "\t" + ArchiveService.CurrentBuildId() + "\n");

        var table = Assert.Single(NewReader().ReadWithLayout().Layout!.Tables);

        Assert.Equal(1, table.FilesNeedingRecut);
        var skip = Assert.Single(table.RecutSkips);
        Assert.Equal(ArchiveLayoutReader.ReasonSortFailed, skip.Reason);
        Assert.Equal(1, skip.Files);

        File.WriteAllText(listPath, DayFile + "\t" + ArchiveService.CurrentBuildId() + "-older\n");

        var later = Assert.Single(NewReader().ReadWithLayout().Layout!.Tables);

        Assert.Equal(1, later.FilesNeedingRecut);
        Assert.Empty(later.RecutSkips);
    }

    /// <summary>A file the persisted list names reads the same list the re-cut wrote, not a copy of its rule: the service's own entry shows.</summary>
    [Fact]
    public void AFileTheRecutSkipped_ShowsAsSkippedInTheLayout()
    {
        WriteMixed(DayFile, 30_000);
        var service = new ArchiveService(_duckDb, _archiveDir);
        service.NoteRecutSkip(ArchiveService.RecutSkipReason.RecutFailed, P(DayFile), new CompactionPassStats());

        var table = Assert.Single(NewReader().ReadWithLayout().Layout!.Tables);

        var skip = Assert.Single(table.RecutSkips);
        Assert.Equal(ArchiveLayoutReader.ReasonSortFailed, skip.Reason);
    }

    /// <summary>A file retention deletes within a day is skipped by the re-cut, and the layout says so.</summary>
    [Fact]
    public void AFileNearRetention_IsSkippedWithThatReason()
    {
        var cutoff = RetentionService.ArchiveCutoff(DateTime.UtcNow);
        WriteMixed($"{cutoff.Date.AddDays(1):yyyyMMdd}_query_store_stats.parquet", 30_000);

        var table = Assert.Single(NewReader().ReadWithLayout().Layout!.Tables);

        Assert.Equal(1, table.FilesNeedingRecut);
        var skip = Assert.Single(table.RecutSkips);
        Assert.Equal(ArchiveLayoutReader.ReasonExpiresSoon, skip.Reason);
    }

    /// <summary>
    /// The free-disk rule is the re-cut's own: a file the merge's rule would fit (about twice the file) but the re-cut's sort
    /// spill (several times the file) would not is named as skipped for free disk.
    /// </summary>
    [Fact]
    public void AFileTheRecutsSpillCannotFit_IsSkippedForFreeDisk()
    {
        var file = WriteMixed(DayFile, 30_000);
        var bytes = new FileInfo(file).Length;
        var reserve = DataVolumeSpace.CompactionReserveBytes(_duckDb.DatabasePath);
        _duckDb.AvailableFreeBytesProvider = _ => 3 * bytes + reserve;

        var table = Assert.Single(NewReader().ReadWithLayout().Layout!.Tables);

        var skip = Assert.Single(table.RecutSkips);
        Assert.Equal(ArchiveLayoutReader.ReasonFreeDisk, skip.Reason);
    }

    /// <summary>
    /// The Store Health window reads with <c>Read()</c> on load and on every Refresh and never shows the layout (#9421), so that read
    /// must not open a single archive file. The MCP read asks for the layout and gets it.
    /// </summary>
    [Fact]
    public async System.Threading.Tasks.Task TheWindowRead_SkipsTheLayoutRead_AndTheMcpReadKeepsIt()
    {
        WriteMixed("202609_wait_stats.parquet", 10_000);
        var reader = NewReader();
        var filesOpened = 0;
        reader.BeforeLayoutFileReadForTests = _ => filesOpened++;

        var windowRead = reader.Read();

        Assert.Null(windowRead.Layout);
        Assert.Equal(0, filesOpened);

        var text = await McpStoreHealthTools.GetStoreHealth(reader);

        Assert.Equal(1, filesOpened);
        Assert.Contains("Layout, read from parquet footers only: 1 of 1 files", text);
    }

    /// <summary>A file removed while the layout is being read is skipped: the call succeeds and the file is not counted.</summary>
    [Fact]
    public void AFileDeletedDuringTheRead_IsSkipped_AndTheCallSucceeds()
    {
        WriteMixed("202609_wait_stats.parquet", 10_000);
        WriteMixed("202608_wait_stats.parquet", 10_000);
        var reader = NewReader();
        reader.BeforeLayoutFileReadForTests = path =>
        {
            if (path.EndsWith("202609_wait_stats.parquet", StringComparison.Ordinal))
            {
                File.Delete(path);
            }
        };

        var snapshot = reader.ReadWithLayout();

        var layout = snapshot.Layout;
        Assert.NotNull(layout);
        Assert.Equal(0, layout.FilesUnreadable);
        Assert.Equal(1, layout.FilesRead);
        var wait = Assert.Single(layout.Tables);
        Assert.Equal(1, wait.Files);
        Assert.Equal(1, wait.FilesRead);
    }

    /// <summary>A read that runs out of its time budget says so and reports how many files it covered.</summary>
    [Fact]
    public void ARead_ThatRunsOutOfTime_IsPartial_AndSaysHowMuchItCovered()
    {
        WriteMixed("202609_wait_stats.parquet", 10_000);
        WriteMixed("202608_wait_stats.parquet", 10_000);
        var reader = NewReader();
        reader.LayoutBudget = TimeSpan.Zero;

        var snapshot = reader.ReadWithLayout();

        var layout = snapshot.Layout!;
        Assert.True(layout.Partial);
        Assert.Equal(2, layout.FilesTotal);
        Assert.Equal(0, layout.FilesRead);
        Assert.Contains("PARTIAL", StoreHealthText.Render(snapshot));
    }

    /// <summary>The layout read never opens row data: a file whose footer is fine but whose pages are damaged still reads.</summary>
    [Fact]
    public void TheLayoutRead_NeedsOnlyTheFooter()
    {
        var file = WriteMixed("202609_wait_stats.parquet", 10_000);
        var bytes = File.ReadAllBytes(file);
        /* Overwrite the first row group's pages (after the 4-byte magic) and leave the footer. */
        for (var i = 8; i < 512; i++)
        {
            bytes[i] = 0xFF;
        }

        File.WriteAllBytes(file, bytes);

        var layout = NewReader().ReadWithLayout().Layout!;

        Assert.Equal(0, layout.FilesUnreadable);
        Assert.Equal(1, layout.FilesRead);
        Assert.True(Assert.Single(layout.Tables).RowGroups > 1);
    }
}
