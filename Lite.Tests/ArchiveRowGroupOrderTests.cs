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
/// #9195. A read of the archive's query_store_stats prunes row groups by their footer min and max of server_id and
/// collection_time, so the files have to come out of the export and compaction paths in server, then time order. These
/// tests drive the real size reset (<see cref="ArchiveService.ArchiveAllAndResetAsync"/>, which writes the hot table's rows
/// in the order they were inserted) and the real compaction, seed rows out of time order across six servers, and read the
/// order back from <c>parquet_metadata</c>.
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this
   class joins the serialized collection CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class ArchiveRowGroupOrderTests : IDisposable
{
    private const int Servers = 6;

    /* These tests read the order of a few thousand rows from small row groups, so the sorted merges write the old size (#9407). */
    private const int SmallGroups = 2048;

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveRowGroupOrderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
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

    /* Rows of one hot query_store_stats table, inserted in a scrambled time order: the time is a bijection of the row
       number (7919 is prime and does not divide the row count), so every row has its own second, no two tie, and the
       servers interleave as collection rounds write them. */
    private async Task<DuckDbInitializer> SeedHotAsync(int rows)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();
        await InsertHotAsync(rows, idBase: 0);
        return initializer;
    }

    private async Task InsertHotAsync(int rows, int idBase)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     last_execution_time, query_hash, execution_count, avg_cpu_time_us, query_plan_hash)
SELECT -(i + {idBase}), TIMESTAMP '2026-09-01 00:00:00' + to_seconds((i * 7919) % {rows}), 1 + i % {Servers}, 'S' || (1 + i % {Servers}),
       'SalesDb', i + {idBase}, i + {idBase}, 'Regular', TIMESTAMP '2026-09-01 00:00:00', '0x' || md5(i::VARCHAR), 1, 100, '0x' || md5((i + 1)::VARCHAR)
FROM range({rows}) t(i)";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /* Row groups of the file whose server_id min and max differ, and the pairs of single-server groups of the same
       server whose collection_time ranges overlap (a time window or a server read then cannot skip either). Both stats
       must exist on every group, or the counts would pass without checking anything (#5381). */
    private static (long Groups, long Mixed, long OverlappingPairs) Order(string file)
    {
        var groups = Scalar($"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{file}')");
        var withStats = Scalar($@"
SELECT count(*) FROM parquet_metadata('{file}') WHERE path_in_schema = 'server_id' AND stats_min IS NOT NULL AND stats_max IS NOT NULL");
        var withTimeStats = Scalar($@"
SELECT count(*) FROM parquet_metadata('{file}') WHERE path_in_schema = 'collection_time' AND stats_min IS NOT NULL AND stats_max IS NOT NULL");
        Assert.True(withStats == groups && withTimeStats == groups, $"{groups} row groups, {withStats} with server_id stats, {withTimeStats} with collection_time stats");

        var mixed = Scalar($@"
SELECT count(*) FROM parquet_metadata('{file}') WHERE path_in_schema = 'server_id' AND TRY_CAST(stats_min AS BIGINT) <> TRY_CAST(stats_max AS BIGINT)");
        var overlapping = Scalar($@"
WITH g AS (
    SELECT s.row_group_id, TRY_CAST(s.stats_min AS BIGINT) AS server, TRY_CAST(t.stats_min AS TIMESTAMP) AS mn, TRY_CAST(t.stats_max AS TIMESTAMP) AS mx
    FROM parquet_metadata('{file}') s
    JOIN parquet_metadata('{file}') t ON t.row_group_id = s.row_group_id AND t.path_in_schema = 'collection_time'
    WHERE s.path_in_schema = 'server_id' AND TRY_CAST(s.stats_min AS BIGINT) = TRY_CAST(s.stats_max AS BIGINT))
SELECT count(*) FROM g a JOIN g b ON a.server = b.server AND a.row_group_id < b.row_group_id AND a.mn <= b.mx AND b.mn <= a.mx");
        return (groups, mixed, overlapping);
    }

    /* A sum over every row's identity, so a rewrite that drops, repeats or alters a row changes it. */
    private static long RowHash(string from) =>
        Scalar($"SELECT sum((hash(collection_id, server_id, collection_time) % 1000000007)::BIGINT)::BIGINT FROM {from}");

    private long HotRowHash()
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT sum((hash(collection_id, server_id, collection_time) % 1000000007)::BIGINT)::BIGINT FROM query_store_stats";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private string SingleQueryStoreFile() =>
        Directory.GetFiles(_archiveDir, "*_query_store_stats.parquet").Single().Replace('\\', '/');

    /// <summary>
    /// A size reset exports the hot table in the order the rows were inserted, so the one file a day holds (a reset every half
    /// day writes about two) had row groups spanning every server and every part of the day. Compaction left a lone
    /// per-cycle file of a day table alone, so that shape was the archive's. It now rewrites it sorted by server, then time,
    /// with every row kept.
    /// </summary>
    [Fact]
    public async Task ASizeResetsQueryStoreExport_EndsUpInRowGroupsOfOneServerInTimeOrder()
    {
        const int rows = 24_000;
        var initializer = await SeedHotAsync(rows);

        Assert.True(await new ArchiveService(initializer, _archiveDir) { SortedRowGroupSizeOverride = SmallGroups }.ArchiveAllAndResetAsync(TestContext.Current.CancellationToken));

        var file = SingleQueryStoreFile();
        Assert.Equal(rows, Scalar($"SELECT count(*) FROM read_parquet('{file}')"));
        Assert.Equal(rows, Scalar($"SELECT count(DISTINCT (server_id, collection_time)) FROM read_parquet('{file}')"));

        var (groups, mixed, overlapping) = Order(file);
        Assert.True(groups >= 8, $"{groups} row groups: too few to tell sorted from unsorted");
        Assert.True(mixed <= Servers - 1, $"{mixed} of {groups} row groups hold more than one server");
        Assert.True(overlapping == 0, $"{overlapping} pairs of one server's row groups overlap in collection_time");
    }

    /// <summary>
    /// The same read of the files a day holds when several per-cycle files merge (the hourly archive passes): the rows of each
    /// file are out of time order, and the merged day comes out in server, then time order too.
    /// </summary>
    [Fact]
    public void ADaysPerCycleQueryStoreFiles_MergeIntoRowGroupsOfOneServerInTimeOrder()
    {
        const int filesPerDay = 4;
        const int rowsPerFile = 6_000;
        for (var f = 0; f < filesPerDay; f++)
        {
            Exec($"COPY (SELECT (i + {f * rowsPerFile})::BIGINT AS id, TIMESTAMP '2026-09-28 00:00:00' + to_seconds((((i * 7919) % {rowsPerFile}) + {f * rowsPerFile})::BIGINT) AS collection_time, " +
                 $"(1 + i % {Servers})::INTEGER AS server_id, md5(i::VARCHAR) || md5((i + 1)::VARCHAR) AS payload FROM range({rowsPerFile}) t(i)) " +
                 $"TO '{P($"20260928_{f:D2}00_query_store_stats.parquet")}' ({ParquetCompaction.ArchiveCopyOptions})");
        }

        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        new ArchiveService(initializer, _archiveDir) { SortedRowGroupSizeOverride = SmallGroups }.CompactParquetFiles();

        var file = SingleQueryStoreFile();
        Assert.Equal(filesPerDay * rowsPerFile, Scalar($"SELECT count(*) FROM read_parquet('{file}')"));
        var (groups, mixed, overlapping) = Order(file);
        Assert.True(groups >= 8, $"{groups} row groups: too few to tell sorted from unsorted");
        Assert.True(mixed <= Servers - 1, $"{mixed} of {groups} row groups hold more than one server");
        Assert.True(overlapping == 0, $"{overlapping} pairs of one server's row groups overlap in collection_time");
    }

    /// <summary>
    /// The field's shape after a size reset lands on a day that is already merged: the day's merged file sits beside a
    /// per-cycle export of the same day, the two together are over the batch budget, so each is a batch of one. The export is
    /// the unsorted one, so it is rewritten sorted into the day's next part file, and the merged file, already in its final
    /// form, is left exactly as it is.
    /// </summary>
    [Fact]
    public async Task AResetExportBesideTheDaysMergedFile_IsRewrittenSortedIntoAPartFile_AndTheMergedFileStays()
    {
        const int rows = 24_000;
        var initializer = await SeedHotAsync(rows);
        Assert.True(await new ArchiveService(initializer, _archiveDir) { SortedRowGroupSizeOverride = SmallGroups }.ArchiveAllAndResetAsync(TestContext.Current.CancellationToken));
        new ArchiveService(initializer, _archiveDir) { SortedRowGroupSizeOverride = SmallGroups }.CompactParquetFiles();
        var merged = SingleQueryStoreFile();
        Assert.EndsWith("_query_store_stats.parquet", merged);
        Assert.Equal(rows, Scalar($"SELECT count(*) FROM read_parquet('{merged}')"));

        /* The second reset of the same day. The reset compacts what it exported before it returns, so the service that runs it
           carries a budget no two files fit in: the merged file and the export are each a batch of one. The rows the pair holds
           are read before the reset, from the merged file and the hot table. */
        await InsertHotAsync(rows, idBase: rows);
        var expectedHash = RowHash($"read_parquet('{merged}')") + HotRowHash();
        var mergedBytesBefore = File.ReadAllBytes(merged);
        var mergedWrittenBefore = File.GetLastWriteTimeUtc(merged);

        Assert.True(await new ArchiveService(initializer, _archiveDir) { DailyCompactionBatchInputBytes = 1, SortedRowGroupSizeOverride = SmallGroups }
            .ArchiveAllAndResetAsync(TestContext.Current.CancellationToken));

        Assert.True(File.Exists(merged), "the day's merged file is gone");
        Assert.Equal(mergedWrittenBefore, File.GetLastWriteTimeUtc(merged));
        Assert.True(mergedBytesBefore.AsSpan().SequenceEqual(File.ReadAllBytes(merged)), "the day's merged file was rewritten");
        Assert.Empty(Directory.GetFiles(_archiveDir, "????????_????_query_store_stats.parquet"));
        var part = Directory.GetFiles(_archiveDir, "*_query_store_stats_pt*.parquet").Single().Replace('\\', '/');
        Assert.Equal(rows, Scalar($"SELECT count(*) FROM read_parquet('{part}')"));
        Assert.Equal(2 * rows, Scalar($"SELECT count(*) FROM read_parquet('{P("*_query_store_stats*.parquet")}')"));
        Assert.Equal(expectedHash, RowHash($"read_parquet('{P("*_query_store_stats*.parquet")}')"));

        var (groups, mixed, overlapping) = Order(part);
        Assert.True(groups >= 8, $"{groups} row groups: too few to tell sorted from unsorted");
        Assert.True(mixed <= Servers - 1, $"{mixed} of {groups} row groups hold more than one server");
        Assert.True(overlapping == 0, $"{overlapping} pairs of one server's row groups overlap in collection_time");
    }
}
