/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #3953 writer against a real PostgreSQL store (<c>DARLING_TEST_PG</c>), driven through the REAL write path:
/// <see cref="DarlingCollectorRunner.WriteBackfillBatchAsync{TRow}"/> routes to the same private
/// <c>WriteBatchAsync</c> -> <c>CopyBatchOnceAsync</c> every live path uses, so these batches are COPYed and applied
/// exactly as the collector's are.
///
/// <para>The oracle is the raw read's own dedup: for every Regular identity in raw, the table row must equal
/// <c>GROUP BY</c> + <c>(array_agg(col ORDER BY collection_time DESC, execution_count DESC))[1]</c>, column for column,
/// and the table must hold nothing else.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it (one test drops the
   table's unique index, which must never happen to a shared store). It never touches the shared database's tables,
   so it cannot race the live collection, and serializing it would be pure slowdown. */
public sealed class QueryStoreIntervalLatestWriterTests
{
    private const int ServerId = -3953001;

    private static readonly DateTime T0 = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* ---- the oracle -------------------------------------------------------------------------------------- */

    /// <summary>Every column the table stores, in one fixed order, for a set comparison against raw's dedup.</summary>
    private const string TableRowsSql = @"
SELECT
    database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count, query_text
FROM collect.query_store_interval_latest
WHERE server_id = @server_id";

    /// <summary>Raw's dedup: the shipped PlanRegressionSql's GROUP BY and array_agg order, over every column.</summary>
    private const string RawDedupSql = @"
SELECT
    database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    (array_agg(collection_time ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(query_plan_hash ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(query_hash ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(execution_count ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(avg_cpu_time_us ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(avg_duration_us ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(last_execution_time ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(is_forced_plan ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(force_failure_count ORDER BY collection_time DESC, execution_count DESC))[1],
    (array_agg(query_text ORDER BY collection_time DESC, execution_count DESC))[1]
FROM collect.query_store_stats
WHERE server_id = @server_id
AND   execution_type_desc = 'Regular'
GROUP BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time";

    private static async Task AssertTableEqualsRawDedupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var tableOnly = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM (({TableRowsSql}) EXCEPT ({RawDedupSql})) AS d", ct);
        var rawOnly = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM (({RawDedupSql}) EXCEPT ({TableRowsSql})) AS d", ct);
        var tableCount = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM ({TableRowsSql}) AS d", ct);
        var rawCount = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM ({RawDedupSql}) AS d", ct);

        Assert.True(rawCount > 0, "the seed wrote no Regular raw rows, so the comparison below is vacuous");
        Assert.Equal(0, tableOnly);
        Assert.Equal(0, rawOnly);
        Assert.Equal(rawCount, tableCount);
    }

    /* ---- the seed ---------------------------------------------------------------------------------------- */

    private static QueryStoreCollector.Row Row(
        string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
        long cpuUs, string? role = null, string type = "Regular", string? text = null) => new()
        {
            DatabaseName = database,
            QueryId = queryId,
            PlanId = planId,
            ExecutionTypeDesc = type,
            FirstExecutionTime = first,
            LastExecutionTime = last,
            QueryHash = "0x" + queryId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + planId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = cpuUs,
            AvgDurationUs = cpuUs * 2,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            ReplicaRole = role,
            RuntimeStatsIntervalId = intervalId,
            QueryText = text,
        };

    private static ServerRuntime Server() => new()
    {
        Config = new MonitoredServer { Name = "qsil-3953", Host = "qsil-3953-host" },
        ConnectionString = "Server=qsil-3953-host",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "qsil-3953-host",
        ServerId = ServerId,
        EngineEdition = 3,
    };

    private static async Task WriteAsync(
        DarlingCollectorRunner runner, DateTime collectionTime, CollectorContext context, CancellationToken ct,
        params QueryStoreCollector.Row[] rows)
    {
        /* One batch per database, as the enumerated and Azure paths write them. */
        foreach (var batch in rows.GroupBy(r => r.DatabaseName))
        {
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, batch.ToList(), Server(), collectionTime, context, ct);
        }
    }

    /// <summary>
    /// A multi-cycle stream at the collector's shape: open refreshes, a close, a new interval, NULL and named replica
    /// roles, two rows in one batch sharing an identity (two replica groups under one role name), non-Regular rows, a
    /// backdated backfill slice that loses to a newer live snapshot, and one that fills a hole alone.
    /// </summary>
    private static async Task WriteStreamAsync(DarlingCollectorRunner runner, CollectorContext context, CancellationToken ct)
    {
        var i100 = T0;
        var i101 = T0.AddHours(1);

        await WriteAsync(runner, T0.AddMinutes(10), context, ct,
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(9), 10, 500),
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(8), 5, 700, role: "secondary1"),
            Row("qsA", 2, 21, 100, i100, T0.AddMinutes(7), 3, 900),
            Row("qsA", 2, 21, 100, i100, T0.AddMinutes(7), 1, 900, type: "Aborted"),
            Row("qsB", 3, 31, 100, i100, T0.AddMinutes(6), 7, 300));

        await WriteAsync(runner, T0.AddMinutes(40), context, ct,
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(39), 20, 510),
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(38), 9, 650, role: "secondary1"),
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(37), 12, 640, role: "secondary1"));

        await WriteAsync(runner, T0.AddMinutes(65), context, ct,
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(59), 25, 505),
            Row("qsA", 1, 14, 101, i101, T0.AddMinutes(64), 4, 2500),
            Row("qsB", 3, 31, 100, i100, T0.AddMinutes(58), 9, 310));

        /* #2022 backfill: a backdated slice. Its interval-100 snapshot loses to the newer live ones; its interval-99
           row fills a hole no live batch wrote, and carries inline text as backfill slices do. */
        await WriteAsync(runner, T0.AddMinutes(30), context, ct,
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(29), 15, 520),
            Row("qsA", 5, 51, 99, T0.AddHours(-1), T0.AddMinutes(-2), 2, 4000, text: "SELECT 5"));
    }

    /* ---- the tests --------------------------------------------------------------------------------------- */

    [Fact]
    public async Task TheTableEqualsTheRawDedup_ThroughTheRealWritePath_AndReapplyingChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 writer test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = NewContext();

        var before = DateTime.UtcNow;
        await WriteStreamAsync(runner, context, ct);

        await AssertTableEqualsRawDedupAsync(connection, ct);
        Assert.Equal(0, context.QueryStoreIntervalMisses);
        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_latest_pending", ct));

        /* The two replica groups sharing one role name collapse to the higher count, the read's own tie-break. */
        Assert.Equal(12, await ScalarLongAsync(connection,
            "SELECT execution_count FROM collect.query_store_interval_latest WHERE server_id = @server_id AND replica_role = 'secondary1'", ct));

        /* Coverage: created from the clock, never from a batch's (here, historical) collection_time, and
           applied_through starts there too, so these older batches leave it alone. */
        var (filledSince, appliedThrough) = await CoverageAsync(connection, ct);
        Assert.True(filledSince >= before.AddSeconds(-1), $"filled_since {filledSince:o} predates the first apply's clock {before:o}");
        Assert.Equal(filledSince, appliedThrough);

        /* A batch newer than the stamp advances applied_through to exactly its collection_time. */
        var ahead = TruncateToSeconds(DateTime.UtcNow.AddHours(1));
        await WriteAsync(runner, ahead, context, ct, Row("qsB", 6, 61, 102, T0.AddHours(2), T0.AddHours(2).AddMinutes(5), 3, 800));
        Assert.Equal(ahead, (await CoverageAsync(connection, ct)).AppliedThrough);
        Assert.Equal(filledSince, (await CoverageAsync(connection, ct)).FilledSince);
        await AssertTableEqualsRawDedupAsync(connection, ct);

        /* Idempotence: re-writing a stale batch (a replay, or the #3099 re-attempt) changes nothing in the table. */
        var tableBefore = await ScalarStringAsync(connection, $"SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM ({TableRowsSql}) AS t", ct);
        await WriteAsync(runner, T0.AddMinutes(40), context, ct,
            Row("qsA", 1, 11, 100, T0, T0.AddMinutes(39), 20, 510));
        Assert.Equal(tableBefore, await ScalarStringAsync(connection, $"SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM ({TableRowsSql}) AS t", ct));
        await AssertTableEqualsRawDedupAsync(connection, ct);
    }

    [Fact]
    public async Task AnApplyFault_StoresRawAndQueuesAReplay_WhichTheNextApplyDrains()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 writer test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = NewContext();

        await WriteAsync(runner, T0.AddMinutes(10), context, ct, Row("qsA", 1, 11, 100, T0, T0.AddMinutes(9), 10, 500));

        /* Without its unique index, ON CONFLICT cannot infer an arbiter (42P10): every apply faults. The faulted
           batch is stamped ahead of the coverage row's clock, so the record's applied_through advance is visible. */
        var faulted = TruncateToSeconds(DateTime.UtcNow.AddHours(1));
        await ExecAsync(connection, "DROP INDEX collect.ux_query_store_interval_latest", ct);
        await WriteAsync(runner, faulted, context, ct, Row("qsA", 1, 11, 100, T0, T0.AddMinutes(39), 20, 510));

        /* Raw kept the batch; the table did not get it; the batch is queued, and applied_through moved past it. */
        Assert.Equal(1, context.QueryStoreIntervalMisses);
        Assert.Equal(2, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_stats WHERE server_id = @server_id", ct));
        Assert.Equal(10, await ScalarLongAsync(connection, "SELECT execution_count FROM collect.query_store_interval_latest WHERE server_id = @server_id", ct));
        Assert.Equal(1, await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_latest_pending WHERE server_id = @server_id AND failure LIKE '42P10%'", ct));
        Assert.Equal(faulted, (await CoverageAsync(connection, ct)).AppliedThrough);

        /* Restored: the next batch replays the queued one first, and the pending table drains. */
        await ExecAsync(connection, PgMigrations.Scripts.Single(m => m.Version == 140).Sql, ct);
        await WriteAsync(runner, T0.AddMinutes(65), context, ct, Row("qsB", 3, 31, 100, T0, T0.AddMinutes(58), 9, 310));

        Assert.Equal(1, context.QueryStoreIntervalMisses);
        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_latest_pending", ct));
        await AssertTableEqualsRawDedupAsync(connection, ct);

        /* The collection_log note says it, in #3099's shape. */
        Assert.Contains("1 Query Store batch(es)", DarlingCollectorRunner.QueryStoreIntervalMissNote(1), StringComparison.Ordinal);
        Assert.Null(DarlingCollectorRunner.QueryStoreIntervalMissNote(0));
    }

    [Fact]
    public async Task AHeldLock_TripsTheLockTimeoutIntoTheSamePath_AndRawStillCommits()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 writer test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = NewContext();

        await WriteAsync(runner, T0.AddMinutes(10), context, ct, Row("qsA", 1, 11, 100, T0, T0.AddMinutes(9), 10, 500));

        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await using (var hold = await holder.BeginTransactionAsync(ct))
        {
            await using (var lockCommand = new NpgsqlCommand("LOCK TABLE collect.query_store_interval_latest IN ACCESS EXCLUSIVE MODE", holder, hold))
            {
                await lockCommand.ExecuteNonQueryAsync(ct);
            }

            await WriteAsync(runner, T0.AddMinutes(40), context, ct, Row("qsA", 1, 11, 100, T0, T0.AddMinutes(39), 20, 510));
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(1, context.QueryStoreIntervalMisses);
        Assert.Equal(2, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_stats WHERE server_id = @server_id", ct));
        Assert.Equal(1, await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_latest_pending WHERE server_id = @server_id AND failure LIKE '55P03%'", ct));
    }

    [Fact]
    public async Task RawWrittenBehindTheWritersBack_RestartsCoverageAtTheNextGapCheck()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 writer test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var context = NewContext();

        await WriteAsync(new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), T0.AddMinutes(10), context, ct,
            Row("qsA", 1, 11, 100, T0, T0.AddMinutes(9), 10, 500));
        var (firstFilledSince, _) = await CoverageAsync(connection, ct);

        /* A build without the writer (a rollback) stores a batch the table never sees, after the coverage row's stamp. */
        var behindTheBack = TruncateToSeconds(DateTime.UtcNow.AddMinutes(5));
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, execution_count) " +
            "VALUES (9000000001, '" + behindTheBack.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + "', @server_id, 'qsil-3953-host', 'qsA', 1, 11, 'Regular', '2026-09-01 00:00', '2026-09-01 01:29', 40)", ct);

        /* The re-upgraded process's first batch runs the gap check before its COPY. */
        var restarted = DateTime.UtcNow;
        await WriteAsync(new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), T0.AddMinutes(95), context, ct,
            Row("qsA", 1, 11, 100, T0, T0.AddMinutes(94), 45, 500));

        var (filledSince, appliedThrough) = await CoverageAsync(connection, ct);
        Assert.True(filledSince > firstFilledSince, "the gap check did not restart coverage");
        Assert.True(filledSince > behindTheBack, $"filled_since {filledSince:o} does not clear the unapplied raw row at {behindTheBack:o}");
        Assert.True(filledSince >= restarted.AddSeconds(-1), $"filled_since {filledSince:o} is below the restart clock {restarted:o}");
        Assert.Equal(behindTheBack, appliedThrough);
        Assert.Equal(0, context.QueryStoreIntervalMisses);
    }

    /* ---- helpers ----------------------------------------------------------------------------------------- */

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static CollectorContext NewContext() => new()
    {
        ServerId = ServerId,
        ServerName = "qsil-3953-host",
        CollectionTime = DateTime.UtcNow,
        Deltas = new CollectorDeltaCalculator(),
    };

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<(DateTime FilledSince, DateTime AppliedThrough)> CoverageAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT filled_since, applied_through FROM collect.query_store_interval_latest_coverage WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "no coverage row for the server");
        return (reader.GetDateTime(0), reader.GetDateTime(1));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ScalarStringAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        return (string?)await command.ExecuteScalarAsync(ct);
    }
}
