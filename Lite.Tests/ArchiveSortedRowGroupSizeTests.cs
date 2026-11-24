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
}
