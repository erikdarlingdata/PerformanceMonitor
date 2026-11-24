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

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     last_execution_time, query_hash, execution_count, avg_cpu_time_us, query_plan_hash)
SELECT -i, TIMESTAMP '2026-09-01 00:00:00' + to_seconds((i * 7919) % {rows}), 1 + i % {Servers}, 'S' || (1 + i % {Servers}),
       'SalesDb', i, i, 'Regular', TIMESTAMP '2026-09-01 00:00:00', '0x' || md5(i::VARCHAR), 1, 100, '0x' || md5((i + 1)::VARCHAR)
FROM range({rows}) t(i)";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        return initializer;
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
SELECT count(*) FROM parquet_metadata('{file}') WHERE path_in_schema = 'server_id' AND stats_min::BIGINT <> stats_max::BIGINT");
        var overlapping = Scalar($@"
WITH g AS (
    SELECT s.row_group_id, s.stats_min::BIGINT AS server, t.stats_min::TIMESTAMP AS mn, t.stats_max::TIMESTAMP AS mx
    FROM parquet_metadata('{file}') s
    JOIN parquet_metadata('{file}') t ON t.row_group_id = s.row_group_id AND t.path_in_schema = 'collection_time'
    WHERE s.path_in_schema = 'server_id' AND s.stats_min::BIGINT = s.stats_max::BIGINT)
SELECT count(*) FROM g a JOIN g b ON a.server = b.server AND a.row_group_id < b.row_group_id AND a.mn <= b.mx AND b.mn <= a.mx");
        return (groups, mixed, overlapping);
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

        Assert.True(await new ArchiveService(initializer, _archiveDir).ArchiveAllAndResetAsync(TestContext.Current.CancellationToken));

        var file = SingleQueryStoreFile();
        Assert.Matches(@"/\d{8}_query_store_stats\.parquet$", file);
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
        new ArchiveService(initializer, _archiveDir).CompactParquetFiles();

        var file = SingleQueryStoreFile();
        Assert.Equal(filesPerDay * rowsPerFile, Scalar($"SELECT count(*) FROM read_parquet('{file}')"));
        var (groups, mixed, overlapping) = Order(file);
        Assert.True(groups >= 8, $"{groups} row groups: too few to tell sorted from unsorted");
        Assert.True(mixed <= Servers - 1, $"{mixed} of {groups} row groups hold more than one server");
        Assert.True(overlapping == 0, $"{overlapping} pairs of one server's row groups overlap in collection_time");
    }
}
