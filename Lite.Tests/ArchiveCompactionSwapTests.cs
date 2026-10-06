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
/// Hourly compaction merges a month's existing parquet file (or its part files) with the new per-cycle files
/// and replaces it with the result. The existing file is an input of that merge, so until every merged file
/// is in place its rows exist only in the temps. Promotion used to delete the existing file first and move
/// the temp over it; when the move failed (a scanner, backup agent or indexer holding the fresh temp), the
/// failure path deleted the temps and the month was gone. With part files one failed promote lost both old
/// parts and counted the new rows twice. These tests drive <see cref="ArchiveService.CompactParquetFiles"/>
/// over real parquet files with a handle held on a temp at promotion time, and count rows through the same
/// globs the archive views use.
/// </summary>
public sealed class ArchiveCompactionSwapTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public ArchiveCompactionSwapTests()
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

    private ArchiveService NewService()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        return new(initializer, _archiveDir);
    }

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

    /* Rows [from, to) with a payload wide enough that a small budget splits the month into parts. */
    private void MakeParquet(string fileName, long from, long to) =>
        Exec($"COPY (SELECT i AS id, md5(i::VARCHAR) AS payload FROM range({from}, {to}) t(i)) TO '{P(fileName)}' (FORMAT PARQUET, ROW_GROUP_SIZE 10000)");

    /* Every row visible through the archive views' two globs for table t. Like the views, each glob is used
       only while it matches a file: DuckDB fails to bind a glob that matches nothing. */
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

    private string[] ArchiveFileNames() =>
        Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    [Fact]
    public void FailedPromote_KeepsEveryRow_ForASingleOutput()
    {
        MakeParquet("202609_t.parquet", 0, 100_000);
        MakeParquet("20260928_1400_t.parquet", 100_000, 100_100);

        var service = NewService();
        FileStream? hold = null;
        /* A scanner-like handle: readable, but without FileShare.Delete, so the rename fails. */
        service.OnCompactionTempsReadyForTests = temps =>
            hold = new FileStream(temps[^1].Replace("/", "\\"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

        try
        {
            service.CompactParquetFiles();
        }
        finally
        {
            hold?.Dispose();
        }

        var (rows, distinct) = Visible("t");
        Assert.Equal(100_100, rows);
        Assert.Equal(100_100, distinct);
        Assert.Equal(100_000, Scalar($"SELECT count(*) FROM read_parquet('{P("202609_t.parquet")}')"));
        Assert.True(File.Exists(P("20260928_1400_t.parquet")), "the per-cycle input was deleted although its rows were never promoted");
        Assert.DoesNotContain(ArchiveFileNames(), f => f.EndsWith(".replaced", StringComparison.Ordinal) || f.EndsWith(".swap", StringComparison.Ordinal));

        /* The next cycle, with nothing holding the temp, merges the month cleanly. */
        MakeParquet("20260928_1500_t.parquet", 100_100, 100_200);
        service.OnCompactionTempsReadyForTests = null;
        service.CompactParquetFiles();

        (rows, distinct) = Visible("t");
        Assert.Equal(100_200, rows);
        Assert.Equal(100_200, distinct);
        Assert.Equal(["202609_t.parquet"], ArchiveFileNames().Where(f => !f.EndsWith(".tmp", StringComparison.Ordinal)));
    }

    [Fact]
    public void FailedPromote_KeepsEveryRow_ForPartFiles()
    {
        MakeParquet("202609_t_pt001.parquet", 0, 60_000);
        MakeParquet("202609_t_pt002.parquet", 1_000_000, 1_080_000);
        MakeParquet("20260928_1400_t.parquet", 5_000_000, 5_000_100);

        var service = NewService();
        /* A budget that fits the per-cycle file and the smaller part but not the larger one: the merge
           splits into two batches, so both output names are the existing part files. */
        service.CompactionBatchInputBytes =
            new FileInfo(P("202609_t_pt001.parquet").Replace("/", "\\")).Length
            + new FileInfo(P("20260928_1400_t.parquet").Replace("/", "\\")).Length;

        FileStream? hold = null;
        List<string> temps = [];
        service.OnCompactionTempsReadyForTests = t =>
        {
            temps = t.ToList();
            hold = new FileStream(t[^1].Replace("/", "\\"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        };

        try
        {
            service.CompactParquetFiles();
        }
        finally
        {
            hold?.Dispose();
        }

        Assert.Equal(2, temps.Count);

        var (rows, distinct) = Visible("t");
        Assert.Equal(140_100, rows);
        Assert.Equal(140_100, distinct);
        Assert.Equal(60_000, Scalar($"SELECT count(*) FROM read_parquet('{P("202609_t_pt001.parquet")}') WHERE id < 60000"));
        Assert.Equal(80_000, Scalar($"SELECT count(*) FROM read_parquet('{P("202609_t_pt002.parquet")}') WHERE id >= 1000000"));
        Assert.True(File.Exists(P("20260928_1400_t.parquet")));
        Assert.DoesNotContain(ArchiveFileNames(), f => f.EndsWith(".replaced", StringComparison.Ordinal) || f.EndsWith(".swap", StringComparison.Ordinal));

        /* Next cycle: the same inputs merge into two fresh parts, every row exactly once. */
        service.OnCompactionTempsReadyForTests = null;
        service.CompactParquetFiles();

        (rows, distinct) = Visible("t");
        Assert.Equal(140_100, rows);
        Assert.Equal(140_100, distinct);
        Assert.False(File.Exists(P("20260928_1400_t.parquet")), "the per-cycle input survived a successful merge");
    }

    /// <summary>
    /// The state a process kill leaves after the swap moved every merged file in but before it deleted the
    /// inputs: the month's file holds the merged rows and the per-cycle file it folded in is still there.
    /// The next run must delete that input, not count its rows a second time for good.
    /// </summary>
    [Fact]
    public void SwapThatDiedAfterPromotion_IsFinishedBeforeMerging_WithoutDoubleCounting()
    {
        MakeParquet("202609_t.parquet", 0, 1_000);              /* merged output, already in place */
        MakeParquet("202609_t.parquet.replaced", 0, 500);        /* the old month file, set aside */
        MakeParquet("20260928_1400_t.parquet", 500, 1_000);      /* the per-cycle input folded into the output */
        MakeParquet("20260928_1500_t.parquet", 1_000, 1_100);    /* arrived after the kill; a real new input */
        File.WriteAllLines(P("202609_t.swap"),
        [
            "state|swapping",
            "output|replacing|202609_t.parquet",
            "input|20260928_1400_t.parquet"
        ]);

        NewService().CompactParquetFiles();

        var (rows, distinct) = Visible("t");
        Assert.Equal(1_100, rows);
        Assert.Equal(1_100, distinct);
        Assert.Equal(["202609_t.parquet"], ArchiveFileNames());
    }

    /// <summary>
    /// The state a kill leaves in the middle of promotion: the first part is in, the second is still a temp,
    /// both old parts are set aside. The next run must put the old parts back, drop the half-promoted output,
    /// and then merge the month from the files the dead run started with.
    /// </summary>
    [Fact]
    public void SwapThatDiedMidPromotion_IsUndoneBeforeMerging()
    {
        MakeParquet("202609_t_pt001.parquet", 60, 100);                /* promoted output (batch 1 = old pt002 + cycle) */
        MakeParquet("202609_t_pt001.parquet.replaced", 0, 60);         /* old pt001, set aside; its rows live only in pt002's temp */
        MakeParquet("202609_t_pt002.parquet.replaced", 60, 90);        /* old pt002, set aside */
        MakeParquet("202609_t_pt002.parquet.tmp", 0, 60);              /* batch 2 = old pt001, never promoted */
        MakeParquet("20260928_1400_t.parquet", 90, 100);               /* the cycle file */
        File.WriteAllLines(P("202609_t.swap"),
        [
            "state|swapping",
            "output|replacing|202609_t_pt001.parquet",
            "output|replacing|202609_t_pt002.parquet",
            "input|20260928_1400_t.parquet"
        ]);

        NewService().CompactParquetFiles();

        var (rows, distinct) = Visible("t");
        Assert.Equal(100, rows);
        Assert.Equal(100, distinct);
        Assert.DoesNotContain(ArchiveFileNames(), f => f.EndsWith(".replaced", StringComparison.Ordinal) || f.EndsWith(".swap", StringComparison.Ordinal) || f.EndsWith(".tmp", StringComparison.Ordinal));
    }

    /// <summary>
    /// A finished swap whose input cannot be deleted yet (something holds it open) keeps its journal, and that
    /// month stays out of the merge until the input is gone: merging it again would fold the held input, whose
    /// rows are already in the month's file, a second time, and the new swap's journal would write over the
    /// record of the pending delete.
    /// </summary>
    [Fact]
    public void AnInputThatCannotBeDeletedYet_KeepsItsMonthOutOfTheMerge_UntilItIsGone()
    {
        MakeParquet("202609_t.parquet", 0, 1_000);              /* merged output, in place */
        MakeParquet("20260928_1400_t.parquet", 500, 1_000);      /* folded into the output, not yet deleted */
        MakeParquet("20260928_1500_t.parquet", 1_000, 1_100);    /* a new per-cycle file */
        File.WriteAllLines(P("202609_t.swap"),
        [
            "state|swapped",
            "output|replacing|202609_t.parquet",
            "input|20260928_1400_t.parquet"
        ]);

        var service = NewService();
        using (new FileStream(P("20260928_1400_t.parquet").Replace("/", "\\"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            service.CompactParquetFiles();
        }

        Assert.True(File.Exists(P("202609_t.swap")), "the journal was dropped while its input still existed");
        Assert.True(File.Exists(P("20260928_1500_t.parquet")), "the month was merged while an earlier swap's delete was still pending");
        Assert.Equal(1_000, Scalar($"SELECT count(*) FROM read_parquet('{P("202609_t.parquet")}')"));

        /* Released: the next run deletes the input, drops the journal, and merges the month once. */
        service.CompactParquetFiles();

        var (rows, distinct) = Visible("t");
        Assert.Equal(1_100, rows);
        Assert.Equal(1_100, distinct);
        Assert.Equal(["202609_t.parquet"], ArchiveFileNames());
    }

    /// <summary>
    /// A part file imported from a previous install is named <c>imported_YYYYMM_table_ptNNN</c>. The imported
    /// pattern used to capture <c>table_ptNNN</c> as the table, so the group's output was this install's own
    /// part file of the same month, which the promote replaced with the imported rows.
    /// </summary>
    [Fact]
    public void ImportedPartFile_DoesNotReplaceTheLocalPartFileOfTheSameName()
    {
        MakeParquet("202609_t_pt001.parquet", 0, 1_000);
        MakeParquet("imported_202609_t_pt001.parquet", 1_000, 2_000);

        var service = NewService();
        service.CompactParquetFiles();

        /* Both are final monthly shapes, so the group has nothing new to fold and both stay as they are. */
        var (rows, distinct) = Visible("t");
        Assert.Equal(2_000, rows);
        Assert.Equal(2_000, distinct);
        Assert.True(File.Exists(P("202609_t_pt001.parquet")));
        Assert.True(File.Exists(P("imported_202609_t_pt001.parquet")));

        /* With a per-cycle file to fold in, both part files are inputs of one merge and every row survives. */
        MakeParquet("20260928_1400_t.parquet", 2_000, 2_100);
        service.CompactParquetFiles();

        (rows, distinct) = Visible("t");
        Assert.Equal(2_100, rows);
        Assert.Equal(2_100, distinct);
        Assert.False(File.Exists(P("imported_202609_t_pt001.parquet")), "the imported part was not folded into the month");
    }

    [Fact]
    public void ImportedQuerySnapshotsPartFile_IsSkippedLikeTheLocalOne()
    {
        MakeParquet("202609_query_snapshots_pt001.parquet", 0, 100);
        MakeParquet("imported_202609_query_snapshots_pt001.parquet", 100, 200);
        MakeParquet("20260928_1400_query_snapshots.parquet", 200, 300);

        NewService().CompactParquetFiles();

        Assert.Equal(
            ["20260928_1400_query_snapshots.parquet", "202609_query_snapshots_pt001.parquet", "imported_202609_query_snapshots_pt001.parquet"],
            ArchiveFileNames());
        /* Names alone would not catch the imported rows moved over the local part file. */
        Assert.Equal(100, Scalar($"SELECT count(*) FROM read_parquet('{P("202609_query_snapshots_pt001.parquet")}') WHERE id < 100"));
        Assert.Equal(100, Scalar($"SELECT count(*) FROM read_parquet('{P("imported_202609_query_snapshots_pt001.parquet")}') WHERE id >= 100"));
    }

    /// <summary>
    /// A legacy <c>all_table</c> file used to be merged in its own group whose output was named for the current
    /// month, replacing the current month's own file. It is now an input of that month's group.
    /// </summary>
    [Fact]
    public void LegacyAllFile_IsFoldedIntoTheCurrentMonth_NotOverIt()
    {
        var month = DateTime.UtcNow.ToString("yyyyMM");
        MakeParquet($"{month}_t.parquet", 0, 1_000);
        MakeParquet("all_t.parquet", 1_000, 1_500);
        MakeParquet($"{month}01_1400_t.parquet", 1_500, 1_600);

        NewService().CompactParquetFiles();

        var (rows, distinct) = Visible("t");
        Assert.Equal(1_600, rows);
        Assert.Equal(1_600, distinct);
        Assert.Equal([$"{month}_t.parquet"], ArchiveFileNames());
    }
}
