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
using System.Threading;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9407. A merge that sorts by server, then time writes DuckDB's default row-group size, an unsorted COPY keeps 2,048
/// rows, and compaction cuts an existing sorted file that still has small groups once. Row groups are read back from
/// <c>parquet_metadata</c>. The class drives <see cref="ArchiveService.CompactParquetFiles"/>, which takes the store's write
/// lock and the process-wide archive lock, so it joins the serialized collection the other compaction classes use.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveSortedRowGroupSizeTests : IDisposable
{
    private const int Servers = 6;
    private const string DayFile = "20260928_query_store_stats.parquet";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public ArchiveSortedRowGroupSizeTests()
    {
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

    private static long Scalar(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static void Exec(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /* A file of the table's shape written the way the unsorted exports are: rows in a scrambled time order with the servers
       interleaved, 2,048-row groups, so every group of it mixes servers. */
    private string WriteUnsorted(string fileName, int rows, int payloadCopies = 1)
    {
        var path = P(fileName);
        var payload = string.Join(" || ", Enumerable.Repeat("md5(i::VARCHAR) || md5((i + 1)::VARCHAR)", payloadCopies));
        Exec($"COPY (SELECT i::BIGINT AS collection_id, TIMESTAMP '2026-09-28 00:00:00' + to_seconds(((i * 7919) % {rows})::BIGINT) AS collection_time, " +
             $"(1 + i % {Servers})::INTEGER AS server_id, {payload} AS payload FROM range({rows}) t(i)) " +
             $"TO '{path}' ({ParquetCompaction.BuildArchiveCopyOptions(ParquetCompaction.ArchiveRowGroupSize)})");
        return path;
    }

    private static long MaxGroupRows(string file) =>
        Scalar($"SELECT max(row_group_num_rows) FROM parquet_metadata('{file}')");

    private static long Groups(string file) =>
        Scalar($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{file}')");

    private static long MixedGroups(string file) =>
        Scalar($"SELECT count(*) FROM parquet_metadata('{file}') WHERE path_in_schema = 'server_id' AND TRY_CAST(stats_min AS BIGINT) <> TRY_CAST(stats_max AS BIGINT)");

    private static long RowHash(string file) =>
        Scalar($"SELECT sum((hash(collection_id, server_id, collection_time) % 1000000007)::BIGINT)::BIGINT FROM read_parquet('{file}')");

    private ArchiveService NewService(int? sortedRowGroupSize = null)
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        return new ArchiveService(initializer, _archiveDir) { SortedRowGroupSizeOverride = sortedRowGroupSize };
    }

    /* No file is left in the archive folder but the ones the test expects: no temp beside the original, no swap journal, no
       set-aside file. */
    private void AssertOnlyTheDayFile()
    {
        Assert.Equal([DayFile], Directory.GetFiles(_archiveDir).Select(Path.GetFileName).OrderBy(n => n).ToArray());
    }

    /// <summary>
    /// A sorted merge of more rows than the old size writes groups larger than 2,048. A table that keeps no order still
    /// writes 2,048-row groups from the same input.
    /// </summary>
    [Theory]
    [InlineData("wait_stats", true)]
    [InlineData("query_store_stats", true)]
    [InlineData("query_stats", true)]
    [InlineData("other_table", false)]
    public void ASortedMerge_WritesLargeRowGroups_AndAnUnsortedOneKeeps2048(string table, bool sorted)
    {
        const int rows = 30_000;
        var source = WriteUnsorted("source.parquet", rows);
        var output = P("merged.parquet");
        Directory.CreateDirectory(P("spill"));

        ParquetCompaction.MergeBatchToFile(table, [source], output, P("spill"));

        Assert.Equal(rows, Scalar($"SELECT count(*) FROM read_parquet('{output}')"));
        if (sorted)
        {
            Assert.True(MaxGroupRows(output) > ParquetCompaction.ArchiveRowGroupSize, $"the largest row group holds {MaxGroupRows(output)} rows");
        }
        else
        {
            Assert.Equal(ParquetCompaction.ArchiveRowGroupSize, MaxGroupRows(output));
            Assert.True(Groups(output) > 1);
        }
    }

    /// <summary>
    /// An existing file of a sorted table with small groups that mix every server is rewritten sorted: the same rows (count and
    /// a per-row hash), groups of one server in time order, and nothing left beside it.
    /// </summary>
    [Fact]
    public void ASmallGroupFileOfASortedTable_IsCutAgainSorted_WithTheSameRows()
    {
        const int rows = 30_000;
        var file = WriteUnsorted(DayFile, rows);
        var hashBefore = RowHash(file);
        Assert.True(MixedGroups(file) == Groups(file), "the seeded file should mix servers in every group");

        NewService(sortedRowGroupSize: 4096).CompactParquetFiles();

        AssertOnlyTheDayFile();
        Assert.Equal(rows, Scalar($"SELECT count(*) FROM read_parquet('{file}')"));
        Assert.Equal(hashBefore, RowHash(file));
        Assert.Equal(4096, MaxGroupRows(file));
        Assert.True(MixedGroups(file) <= Servers - 1, $"{MixedGroups(file)} of {Groups(file)} row groups hold more than one server");
    }

    /// <summary>
    /// At the production size a cut file is one row group, and a second pass finds nothing to cut: it starts no merge and
    /// leaves the file's bytes alone.
    /// </summary>
    [Fact]
    public void ASecondPass_FindsNothingToCut()
    {
        const int rows = 30_000;
        var file = WriteUnsorted(DayFile, rows);
        var merges = new List<string>();
        var previous = ParquetCompaction.MergeAttemptObservedForTests;
        ParquetCompaction.MergeAttemptObservedForTests = (path, _, _) => { lock (merges) { merges.Add(path); } };
        try
        {
            NewService().CompactParquetFiles();
            Assert.Equal(1, Groups(file));
            Assert.Single(merges);
            var bytes = File.ReadAllBytes(file);

            NewService().CompactParquetFiles();

            Assert.Single(merges);
            Assert.True(bytes.AsSpan().SequenceEqual(File.ReadAllBytes(file)), "the second pass rewrote the file");
            AssertOnlyTheDayFile();
        }
        finally
        {
            ParquetCompaction.MergeAttemptObservedForTests = previous;
        }
    }

    /// <summary>
    /// A re-cut that fails after its temp is written, or is interrupted there by shutdown, leaves the original byte for
    /// byte, with no temp, journal or set-aside file in the folder.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AFailedOrInterruptedCut_LeavesTheOriginalByteForByte(bool interrupt)
    {
        var file = WriteUnsorted(DayFile, 30_000);
        var bytes = File.ReadAllBytes(file);
        using var cts = new CancellationTokenSource();
        var tempSeen = false;
        var service = NewService(sortedRowGroupSize: 4096);
        service.AfterRecutMergedForTests = temp =>
        {
            tempSeen = File.Exists(temp) && temp.EndsWith(".tmp", StringComparison.Ordinal);
            if (interrupt)
            {
                cts.Cancel();
            }
            else
            {
                throw new IOException("the swap could not start");
            }
        };

        service.CompactParquetFiles(cancellationToken: cts.Token);

        Assert.True(tempSeen, "the cut did not reach its temp file");
        Assert.True(bytes.AsSpan().SequenceEqual(File.ReadAllBytes(file)), "the original changed");
        AssertOnlyTheDayFile();
    }

    /// <summary>
    /// A sort that runs out of memory (here a 2 MB limit over a wide file) does not swap in the unsorted order: the original
    /// stays byte for byte, and the file is not sorted again by later passes of the process.
    /// </summary>
    [Fact]
    public void ACutWhoseSortFallsBack_KeepsTheOriginal_AndIsNotTriedAgain()
    {
        var file = WriteUnsorted(DayFile, 60_000, payloadCopies: 4);
        var bytes = File.ReadAllBytes(file);
        var attempts = new List<bool>();
        var previous = ParquetCompaction.MergeAttemptObservedForTests;
        ParquetCompaction.MergeAttemptObservedForTests = (_, sorted, _) => { lock (attempts) { attempts.Add(sorted); } };
        try
        {
            var service = NewService(sortedRowGroupSize: 4096);
            service.RecutMemoryLimit = "2MB";
            service.CompactParquetFiles();
            Assert.Equal([true], attempts);
            service.CompactParquetFiles();
            Assert.Equal([true], attempts);
            Assert.True(bytes.AsSpan().SequenceEqual(File.ReadAllBytes(file)), "the original changed");
            AssertOnlyTheDayFile();
        }
        finally
        {
            ParquetCompaction.MergeAttemptObservedForTests = previous;
        }
    }

    private string DayPath => P(DayFile);

    /// <summary>
    /// A merge whose swap cannot delete an input leaves its journal. The re-cut of the same pass must not take a file of that
    /// group: its swap would write the same journal name over the live one and delete it when it resolves, which leaves rows
    /// read twice or a set-aside file never restored. The journal and the merged file stay as the merge left them.
    /// </summary>
    [Fact]
    public void AMergeThatLeavesItsJournal_KeepsItsGroupOutOfTheCut()
    {
        var input = WriteUnsorted("20260928_1000_query_store_stats.parquet", 30_000);
        var journal = P("20260928_query_store_stats.swap");
        byte[]? journalAtSwap = null;
        byte[]? outputAtSwap = null;
        var service = NewService(sortedRowGroupSize: 4096);
        service.AfterCompactionSwapForTests = _ =>
        {
            journalAtSwap = File.Exists(journal) ? File.ReadAllBytes(journal) : null;
            outputAtSwap = File.ReadAllBytes(DayPath);

            /* The merge wrote 4,096-row groups. Raise the size the cut aims for so the merged file would be a candidate. */
            service.SortedRowGroupSizeOverride = 8192;
        };

        /* A scanner-like handle: readable, but without FileShare.Delete, so the input cannot be deleted. */
        using (new FileStream(input.Replace("/", "\\"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            service.CompactParquetFiles();
        }

        Assert.NotNull(journalAtSwap);
        Assert.True(journalAtSwap!.AsSpan().SequenceEqual(File.ReadAllBytes(journal)), "the journal was rewritten or removed");
        Assert.True(outputAtSwap!.AsSpan().SequenceEqual(File.ReadAllBytes(DayPath)), "the merged file was cut again");
        Assert.False(File.Exists(DayPath + ".tmp"));
    }

    /// <summary>
    /// A journal that appears after the pass started, for a file the cut is about to take, is checked right before that
    /// cut: the file is left alone and the journal keeps its bytes.
    /// </summary>
    [Fact]
    public void AJournalThatAppearsDuringThePass_KeepsItsFileOutOfTheCut()
    {
        var candidate = WriteUnsorted("20260927_query_store_stats.parquet", 30_000);
        WriteUnsorted("20260928_1000_query_store_stats.parquet", 30_000);
        var bytes = File.ReadAllBytes(candidate);
        var journal = P("20260927_query_store_stats.swap");
        string[] journalLines = ["state|swapped", "output|replacing|20260927_query_store_stats.parquet"];
        var service = NewService(sortedRowGroupSize: 4096);
        service.AfterCompactionSwapForTests = _ => File.WriteAllLines(journal, journalLines);

        service.CompactParquetFiles();

        Assert.True(bytes.AsSpan().SequenceEqual(File.ReadAllBytes(candidate)), "the file was cut under a live journal");
        Assert.Equal(journalLines, File.ReadAllLines(journal));
        Assert.False(File.Exists(candidate + ".tmp"));
    }

    /// <summary>
    /// Retention deletes an expired file under the write lock. A source removed after the cut's row-count check has no file at
    /// its name for the swap to replace: the cut drops its temp and the file stays gone.
    /// </summary>
    [Fact]
    public void ASourceRemovedBeforeTheSwap_IsNotBroughtBack()
    {
        WriteUnsorted(DayFile, 30_000);
        var service = NewService(sortedRowGroupSize: 4096);
        service.AfterRecutMergedForTests = _ => File.Delete(DayPath);

        service.CompactParquetFiles();

        Assert.Empty(Directory.GetFiles(_archiveDir));
    }

    /// <summary>
    /// The merge leaves a file over its batch budget alone. A file over that budget is not cut either: the sort is one thread
    /// holding the archive lock, which the hourly archive does not wait for.
    /// </summary>
    [Fact]
    public void AFileOverTheMergeBudget_IsNotCut()
    {
        var file = WriteUnsorted(DayFile, 30_000);
        var bytes = File.ReadAllBytes(file);
        var merges = new List<string>();
        var previous = ParquetCompaction.MergeAttemptObservedForTests;
        ParquetCompaction.MergeAttemptObservedForTests = (path, _, _) => { lock (merges) { merges.Add(path); } };
        try
        {
            var service = NewService(sortedRowGroupSize: 4096);
            service.DailyCompactionBatchInputBytes = bytes.Length - 1;
            service.CompactParquetFiles();

            Assert.Empty(merges);
            Assert.True(bytes.AsSpan().SequenceEqual(File.ReadAllBytes(file)), "the file was cut");
            AssertOnlyTheDayFile();

            /* The same file under a budget that holds it is cut. */
            service = NewService(sortedRowGroupSize: 4096);
            service.DailyCompactionBatchInputBytes = bytes.Length;
            service.CompactParquetFiles();

            Assert.Single(merges);
            Assert.Equal(4096, MaxGroupRows(file));
        }
        finally
        {
            ParquetCompaction.MergeAttemptObservedForTests = previous;
        }
    }

    /// <summary>
    /// The sort spills about the size of its source on top of the file it writes. A volume with room for the source and the
    /// reserve, but not for the spill, does not start the cut.
    /// </summary>
    [Fact]
    public void ACutNeedsRoomForTheSortsSpill_AsWellAsTheOutput()
    {
        var file = WriteUnsorted(DayFile, 30_000);
        var bytes = File.ReadAllBytes(file);
        var reserve = DataVolumeSpace.CompactionReserveBytes(Path.Combine(_tempDir, "test.duckdb"));

        var service = NewService(sortedRowGroupSize: 4096);
        _initializers[^1].AvailableFreeBytesProvider = _ => reserve + bytes.Length + bytes.Length / 2;
        service.CompactParquetFiles();

        Assert.True(bytes.AsSpan().SequenceEqual(File.ReadAllBytes(file)), "the file was cut without room for the spill");

        service = NewService(sortedRowGroupSize: 4096);
        _initializers[^1].AvailableFreeBytesProvider = _ => reserve + 2L * bytes.Length;
        service.CompactParquetFiles();

        Assert.Equal(4096, MaxGroupRows(file));
    }

    /// <summary>
    /// The sort of the cut is the merge's sorted attempt: when it runs out of memory the call returns false and leaves nothing
    /// at the output, so the caller keeps the original.
    /// </summary>
    [Fact]
    public void TryRecutToFile_ReturnsFalse_AndLeavesNoOutput_WhenTheSortRunsOutOfMemory()
    {
        var source = WriteUnsorted("wide_source.parquet", 60_000, payloadCopies: 4);
        var output = P("wide_out.parquet.tmp");
        Directory.CreateDirectory(P("spill"));

        var done = ParquetCompaction.TryRecutToFile("query_store_stats", source, output, P("spill"), "2MB", 4096);

        Assert.False(done);
        Assert.False(File.Exists(output));
    }

    /// <summary>
    /// A compaction merge that falls back to the unsorted order writes 2,048-row groups, which the cut would take. The merge
    /// marks its output so the cut of the same pass leaves it alone: no sorted attempt follows the three of the merge.
    /// </summary>
    [Fact]
    public void AMergeThatFallsBackUnsorted_LeavesItsOutputOutOfTheCut()
    {
        WriteUnsorted("20260928_1000_query_store_stats.parquet", 60_000, payloadCopies: 4);
        var attempts = new List<bool>();
        var previous = ParquetCompaction.MergeAttemptObservedForTests;
        ParquetCompaction.MergeAttemptObservedForTests = (_, sorted, _) => { lock (attempts) { attempts.Add(sorted); } };
        try
        {
            var service = NewService();
            service.MergeMemoryLimit = "2MB";
            service.CompactParquetFiles();

            /* Sorted at the large size, sorted at 2,048 rows, then unsorted. */
            Assert.Equal([true, true, false], attempts);
            Assert.True(Groups(DayPath) > 1);
            Assert.Equal(ParquetCompaction.ArchiveRowGroupSize, MaxGroupRows(DayPath));
        }
        finally
        {
            ParquetCompaction.MergeAttemptObservedForTests = previous;
        }
    }

    private const int WideRows = 125_000;
    private const int WideCopies = 8;
    /* Measured with this source: a sorted merge at 80 to 192 MB runs out of memory at 122,880-row groups and fits at 2,048. */
    private const string WideLimit = "128MB";

    /// <summary>
    /// A batch of long rows can pass the memory cap at the large row-group size, where the writer holds a whole group, yet fit
    /// at 2,048 rows. The merge sorts it again at 2,048 rows before it gives up the order: no fallback to the unsorted merge,
    /// and a server per row group but for the seams.
    /// </summary>
    [Fact]
    public void ASortedMergeThatOutgrowsTheCapAtTheLargeSize_IsSortedAgainAt2048Rows()
    {
        var source = WriteUnsorted("wide_source.parquet", WideRows, WideCopies);
        var output = P("wide_merged.parquet");
        Directory.CreateDirectory(P("spill"));
        var retries = new List<string>();
        var fallbacks = new List<string>();

        ParquetCompaction.MergeBatchToFile("query_store_stats", [source], output, P("spill"), memoryLimit: WideLimit,
            onSortFallback: (t, _, _) => fallbacks.Add(t), onSortedAtSmallGroups: (t, _, _) => retries.Add(t));

        Assert.Empty(fallbacks);
        Assert.Single(retries);
        Assert.Equal(WideRows, Scalar($"SELECT count(*) FROM read_parquet('{output}')"));
        Assert.Equal(ParquetCompaction.ArchiveRowGroupSize, MaxGroupRows(output));
        Assert.True(MixedGroups(output) <= Servers - 1, $"{MixedGroups(output)} of {Groups(output)} row groups hold more than one server");
    }
}
