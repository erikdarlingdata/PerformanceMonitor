/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9352: a row equal to another in every column but <c>collection_id</c> is a copy of it, not a slice of the interval. The
/// repair used to sum a group's <c>execution_count</c> whatever its rows were, so a group of exact copies came out with
/// twice its executions, in the hot store and in every archive file it rewrote.
/// </summary>
public sealed class SliceRepairCopiesTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9352;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _archivePath;
    private DuckDBConnection? _seed;

    public SliceRepairCopiesTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _archivePath = Path.Combine(Path.GetTempPath(), $"pm-9352-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_archivePath);
    }

    public void Dispose()
    {
        _seed?.Dispose();
        try
        {
            if (Directory.Exists(_archivePath))
            {
                Directory.Delete(_archivePath, recursive: true);
            }
        }
        catch (IOException)
        {
            /* A scratch directory that outlives one test run is noise, not a failure. */
        }
    }

    private static readonly DateTime T = new(2026, 11, 21, 10, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// A pair of exact copies stays ONE row carrying the ORIGINAL count, and a group of two distinct slices plus a copy of
    /// the first sums only the two slices. On the old code the pair came out at twice its count and the mixed group
    /// counted the copy as a third slice.
    /// </summary>
    [Fact]
    public async Task HotStore_ExactCopiesAreDropped_AndOnlyDistinctSlicesAreSummed()
    {
        /* Interval 1: two copies. Interval 2: slice A, a copy of A, and a different slice B. Interval 3: one clean row. */
        await SeedAsync(collectionId: 1, intervalId: 1, executionCount: 100, avgDurationUs: 1778, lastExecution: T);
        await SeedAsync(collectionId: 2, intervalId: 1, executionCount: 100, avgDurationUs: 1778, lastExecution: T);
        await SeedAsync(collectionId: 3, intervalId: 2, executionCount: 100, avgDurationUs: 1778, lastExecution: T);
        await SeedAsync(collectionId: 4, intervalId: 2, executionCount: 100, avgDurationUs: 1778, lastExecution: T);
        await SeedAsync(collectionId: 5, intervalId: 2, executionCount: 25, avgDurationUs: 2245, lastExecution: T.AddMinutes(1));
        await SeedAsync(collectionId: 6, intervalId: 3, executionCount: 7, avgDurationUs: 300, lastExecution: T);

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        var result = await service.RepairAsync();

        Assert.Empty(result.Failures);
        Assert.Equal(1 + 2, result.RowsRemoved);

        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE runtime_stats_interval_id = 1"));
        Assert.Equal(100L, await ScalarAsync("SELECT execution_count FROM query_store_stats WHERE runtime_stats_interval_id = 1"));
        Assert.Equal(1778L, await ScalarAsync("SELECT avg_duration_us FROM query_store_stats WHERE runtime_stats_interval_id = 1"));

        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE runtime_stats_interval_id = 2"));
        Assert.Equal(125L, await ScalarAsync("SELECT execution_count FROM query_store_stats WHERE runtime_stats_interval_id = 2"));
        Assert.Equal(1871L, await ScalarAsync("SELECT avg_duration_us FROM query_store_stats WHERE runtime_stats_interval_id = 2"));

        Assert.Equal(7L, await ScalarAsync("SELECT execution_count FROM query_store_stats WHERE runtime_stats_interval_id = 3"));
    }

    /// <summary>
    /// A pure-copy group whose slice has a zero count is still left unchanged: the weighted mean of a zero-count slice is
    /// NULL, and the survivor must keep the average it already had.
    /// </summary>
    [Fact]
    public async Task HotStore_ACopyOfAZeroCountRow_KeepsTheSurvivorsOwnAverage()
    {
        await SeedAsync(collectionId: 1, intervalId: 1, executionCount: 0, avgDurationUs: 55, lastExecution: T);
        await SeedAsync(collectionId: 2, intervalId: 1, executionCount: 0, avgDurationUs: 55, lastExecution: T);

        var result = await new QueryStoreSliceRepairService(_duckDb, _archivePath).RepairAsync();

        Assert.Equal(1, result.RowsRemoved);
        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats"));
        Assert.Equal(55L, await ScalarAsync("SELECT avg_duration_us FROM query_store_stats"));
    }

    /// <summary>
    /// The same rule in an archive file. The survey's conservation figure counts the file's distinct slices, so a rewrite that
    /// drops a copy is not refused for losing that copy's executions.
    /// </summary>
    [Fact]
    public async Task Archive_ExactCopiesAreDropped_AndTheRewriteIsNotRefusedForIt()
    {
        var file = Path.Combine(_archivePath, "202611_query_store_stats.parquet");
        await WriteArchiveWithCopiesAsync(file);

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        var survey = await service.SurveyAsync();
        var archived = Assert.Single(survey.Archive);
        Assert.Equal(6, archived.Rows);
        Assert.Equal(2, archived.Groups);
        Assert.Equal(5, archived.SplitRows);
        Assert.Equal(100L + 125L + 55L, archived.Executions);

        var result = await service.RepairAsync(survey: survey);

        Assert.Empty(result.Failures);
        Assert.Equal(3, result.RowsRemoved);

        var rows = await QueryArchiveAsync(file, "SELECT query_id, execution_count, avg_duration_us FROM read_parquet('{0}') ORDER BY query_id");
        Assert.Equal(3, rows.Count);
        Assert.Equal([1L, 100L, 1778L], rows[0]);
        Assert.Equal([2L, 125L, 1871L], rows[1]);
        Assert.Equal([3L, 55L, 500L], rows[2]);
    }

    /// <summary>
    /// #9352: a backfill slice that re-walks a range the store already holds writes the same rows again, under the same stamp
    /// (two passes whose chunks line up) or under another one (a live cycle's rows). The guard removes the rows of the slice that
    /// an earlier row equals in the key and every measurement, keeps the earlier copy, and keeps a row whose measurements differ
    /// (the newer state of the interval) and a row of another query.
    /// </summary>
    [Fact]
    public async Task Backfill_AChunkThatIsAlreadyStored_LeavesNoSecondCopy()
    {
        var stamp = T.AddHours(5);
        var later = T.AddHours(6);

        /* Query 1: a live row, then the backfill stamps a second copy of it (id 5). Query 2: a row of an earlier pass, then an exact
           same-stamp copy (id 6). Query 3: the same interval with more executions, which is newer state and stays. Query 4: only once. */
        await SeedStampedAsync(collectionId: 1, stamp: T, queryId: 1, executionCount: 10, avgDurationUs: 100);
        await SeedStampedAsync(collectionId: 2, stamp: stamp, queryId: 2, executionCount: 20, avgDurationUs: 200);
        await SeedStampedAsync(collectionId: 3, stamp: T, queryId: 3, executionCount: 30, avgDurationUs: 300);
        await SeedStampedAsync(collectionId: 5, stamp: stamp, queryId: 1, executionCount: 10, avgDurationUs: 100);
        await SeedStampedAsync(collectionId: 6, stamp: stamp, queryId: 2, executionCount: 20, avgDurationUs: 200);
        await SeedStampedAsync(collectionId: 7, stamp: stamp, queryId: 3, executionCount: 35, avgDurationUs: 310);
        await SeedStampedAsync(collectionId: 8, stamp: stamp, queryId: 4, executionCount: 5, avgDurationUs: 50);
        await SeedStampedAsync(collectionId: 9, stamp: later, queryId: 4, executionCount: 5, avgDurationUs: 50);

        Assert.Equal(8L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats"));

        int removed;
        using (_duckDb.AcquireReadLock())
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            removed = RemoteCollectorService.RemoveStoredBackfillCopies(connection, ServerId, "DB", stamp);
        }

        Assert.Equal(2, removed);
        Assert.Equal(6L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats"));
        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 1"));
        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 2"));
        Assert.Equal(2L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 3"));
        Assert.Equal(2L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 4"));
        Assert.Equal(1L, await ScalarAsync("SELECT collection_id FROM query_store_stats WHERE query_id = 1"));
    }

    private async Task SeedStampedAsync(long collectionId, DateTime stamp, long queryId, long executionCount, long avgDurationUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_duration_us, min_duration_us, max_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id)
VALUES ($1, $2, $3, 'SRV', 'DB', $4, 11, 'Regular', $5, $5, 'SELECT 1', '0xH', $6, $7, 998, 4708, '0xP', false, 0, 7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = stamp });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = T });
        cmd.Parameters.Add(new DuckDBParameter { Value = executionCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seed is null)
        {
            _seed = _duckDb.CreateConnection();
            await _seed.OpenAsync();
        }

        return _seed;
    }

    private async Task SeedAsync(long collectionId, long intervalId, long executionCount, long avgDurationUs, DateTime lastExecution)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_duration_us, min_duration_us, max_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id)
VALUES ($1, $2, $3, 'SRV', 'DB', $4, 11, 'Regular', $2, $5, 'SELECT 1', '0xH', $6, $7, 998, 4708, '0xP', false, 0, $4)";
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = T });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastExecution });
        cmd.Parameters.Add(new DuckDBParameter { Value = executionCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Query 1: two copies. Query 2: slice A, a copy of A, slice B. Query 3: one clean row. Pre-tier-2 shape, as the older archive files have.</summary>
    private async Task WriteArchiveWithCopiesAsync(string file)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
COPY (
    SELECT * FROM (VALUES
        (1::BIGINT, TIMESTAMP '2026-11-21 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-11-21 09:55:00', TIMESTAMP '2026-11-21 10:00:00', 'SELECT 1', '0xH', 100::BIGINT, 1778::BIGINT, 998::BIGINT, 4708::BIGINT),
        (2::BIGINT, TIMESTAMP '2026-11-21 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-11-21 09:55:00', TIMESTAMP '2026-11-21 10:00:00', 'SELECT 1', '0xH', 100::BIGINT, 1778::BIGINT, 998::BIGINT, 4708::BIGINT),
        (3::BIGINT, TIMESTAMP '2026-11-21 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 2::BIGINT, 22::BIGINT, 'Regular',
         TIMESTAMP '2026-11-21 09:55:00', TIMESTAMP '2026-11-21 10:00:00', 'SELECT 2', '0xH2', 100::BIGINT, 1778::BIGINT, 998::BIGINT, 4708::BIGINT),
        (4::BIGINT, TIMESTAMP '2026-11-21 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 2::BIGINT, 22::BIGINT, 'Regular',
         TIMESTAMP '2026-11-21 09:55:00', TIMESTAMP '2026-11-21 10:00:00', 'SELECT 2', '0xH2', 100::BIGINT, 1778::BIGINT, 998::BIGINT, 4708::BIGINT),
        (5::BIGINT, TIMESTAMP '2026-11-21 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 2::BIGINT, 22::BIGINT, 'Regular',
         TIMESTAMP '2026-11-21 09:55:00', TIMESTAMP '2026-11-21 10:01:00', 'SELECT 2', '0xH2', 25::BIGINT, 2245::BIGINT, 998::BIGINT, 5100::BIGINT),
        (6::BIGINT, TIMESTAMP '2026-11-21 10:05:00', {ServerId}::INTEGER, 'SRV', 'DB', 3::BIGINT, 33::BIGINT, 'Regular',
         TIMESTAMP '2026-11-21 10:01:00', TIMESTAMP '2026-11-21 10:05:00', 'SELECT 3', '0xH3', 55::BIGINT, 500::BIGINT, 400::BIGINT, 600::BIGINT)
    ) AS t(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
           execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash,
           execution_count, avg_duration_us, min_duration_us, max_duration_us)
) TO '{file.Replace("'", "''", StringComparison.Ordinal)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<long> ScalarAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? 0L : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private async Task<List<object[]>> QueryArchiveAsync(string file, string sqlTemplate)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = string.Format(CultureInfo.InvariantCulture, sqlTemplate, file.Replace("'", "''", StringComparison.Ordinal));

        var rows = new List<object[]>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var values = new object[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = Convert.ToInt64(reader.GetValue(i), CultureInfo.InvariantCulture);
            }

            rows.Add(values);
        }

        return rows;
    }
}

/// <summary>
/// #9352: the repair runs once per install. Its completion marker and attempt record used to live in the store, and the size
/// reset replaces the store, so the next start surveyed everything again. These run the real
/// <see cref="ArchiveService.ArchiveAllAndResetAsync"/> and then start the repair on the fresh store.
/// </summary>
/* ArchiveAllAndResetAsync touches CollectionResetGate and ArchiveService's static archive lock, both process-wide, so this
   class joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class SliceRepairOnceTests : IDisposable
{
    private static readonly DateTime T = new(2026, 11, 21, 10, 0, 0, DateTimeKind.Unspecified);

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;

    public SliceRepairOnceTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        CollectionResetGate.ResetForTests();
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>
    /// A repair that completed, then a size reset, then a start on the fresh store: the survey does not run, no attempt is
    /// added, and the split rows the fresh store holds are not touched.
    /// </summary>
    [Fact]
    public async Task AfterASizeReset_TheNextStartupRepairDoesNotRunAgain()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await SeedSplitPairAsync(queryId: 1);

        var first = new QueryStoreSliceRepairService(_duckDb, _archiveDir);
        await first.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, first.SurveysRun);
        Assert.True(await first.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1L, await first.AttemptCountAsync(TestContext.Current.CancellationToken));

        await new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync(TestContext.Current.CancellationToken);

        /* The premise: the reset really did take the in-store marker with the old store. */
        Assert.Equal(0L, await ScalarAsync(
            $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '{QueryStoreSliceRepairService.MarkerTable}'"));

        await SeedSplitPairAsync(queryId: 2);

        var afterReset = new QueryStoreSliceRepairService(_duckDb, _archiveDir);
        await afterReset.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, afterReset.SurveysRun);
        Assert.True(await afterReset.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1L, await afterReset.AttemptCountAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 2"));
    }

    /// <summary>
    /// A store that already carries the in-store marker (written by a build before the file existed) counts as repaired, and the
    /// file is written from it on that first read, so a reset that takes the table away cannot take the answer with it.
    /// </summary>
    [Fact]
    public async Task AStoreThatAlreadyHasTheMarker_CountsAsRepaired_AndKeepsCountingAfterTheMarkerIsGone()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await ExecAsync(
            $"CREATE TABLE {QueryStoreSliceRepairService.MarkerTable} (completed_at TIMESTAMP NOT NULL, rows_removed BIGINT NOT NULL); " +
            $"INSERT INTO {QueryStoreSliceRepairService.MarkerTable} VALUES (TIMESTAMP '2026-11-21 08:18:00', 12)");

        Assert.True(await new QueryStoreSliceRepairService(_duckDb, _archiveDir).AlreadyRepairedAsync(TestContext.Current.CancellationToken));

        await ExecAsync($"DROP TABLE {QueryStoreSliceRepairService.MarkerTable}");

        Assert.True(await new QueryStoreSliceRepairService(_duckDb, _archiveDir).AlreadyRepairedAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Attempts a store recorded before the file existed carry over to it, so the attempt cap keeps its meaning after a reset
    /// takes the table: a store that already used its two attempts does not get them back.
    /// </summary>
    [Fact]
    public async Task AttemptsRecordedInTheStore_CarryOver_SoTheCapSurvivesAReset()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await ExecAsync(
            $"CREATE TABLE {QueryStoreSliceRepairService.AttemptTable} (attempted_at TIMESTAMP NOT NULL, hot_groups BIGINT NOT NULL, archive_files BIGINT NOT NULL, unreadable_files BIGINT NOT NULL); " +
            $"INSERT INTO {QueryStoreSliceRepairService.AttemptTable} VALUES (TIMESTAMP '2026-11-21 08:18:00', 4, 1, 0), (TIMESTAMP '2026-11-21 18:25:00', 3, 1, 0)");

        Assert.Equal((long)QueryStoreSliceRepairService.MaxStartupAttempts, await new QueryStoreSliceRepairService(_duckDb, _archiveDir).AttemptCountAsync(TestContext.Current.CancellationToken));

        await ExecAsync($"DROP TABLE {QueryStoreSliceRepairService.AttemptTable}");
        await SeedSplitPairAsync(queryId: 3);

        var afterReset = new QueryStoreSliceRepairService(_duckDb, _archiveDir);
        Assert.Equal((long)QueryStoreSliceRepairService.MaxStartupAttempts, await afterReset.AttemptCountAsync(TestContext.Current.CancellationToken));

        await afterReset.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, afterReset.SurveysRun);
        Assert.Equal(2L, await ScalarAsync("SELECT COUNT(*) FROM query_store_stats WHERE query_id = 3"));
    }

    private async Task ExecAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock(TestContext.Current.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task SeedSplitPairAsync(long queryId)
    {
        using var readLock = _duckDb.AcquireReadLock(TestContext.Current.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var (id, count, avg) in new[] { (queryId * 10, 100L, 1778L), (queryId * 10 + 1, 25L, 2245L) })
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_duration_us, min_duration_us, max_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id)
VALUES ($1, $2, 9352, 'SRV', 'DB', $3, 11, 'Regular', $2, $2, 'SELECT 1', '0xH', $4, $5, 998, 4708, '0xP', false, 0, 6001)";
            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            cmd.Parameters.Add(new DuckDBParameter { Value = T });
            cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
            cmd.Parameters.Add(new DuckDBParameter { Value = count });
            cmd.Parameters.Add(new DuckDBParameter { Value = avg });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task<long> ScalarAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock(TestContext.Current.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var value = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return value is null or DBNull ? 0L : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }
}
