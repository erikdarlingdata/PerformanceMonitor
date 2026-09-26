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
/// The #3953 (V145) wide-table writer against a real PostgreSQL store (<c>DARLING_TEST_PG</c>), driven through the
/// REAL write path (<see cref="DarlingCollectorRunner.WriteBackfillBatchAsync{TRow}"/>), which applies V143's
/// Regular-only table AND this wide, every-outcome table in the SAME transaction. Mirrors
/// <see cref="QueryStoreIntervalLatestWriterTests"/>'s shape.
///
/// <para>The oracle is raw's own <c>DISTINCT ON</c>, over every column the wide table stores, every outcome — no
/// <c>execution_type_desc = 'Regular'</c> filter, unlike V143's oracle.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], for the same reason as
   QueryStoreIntervalLatestWriterTests: everything here lives inside its own ScratchPostgres database (one test
   drops the wide table's unique index), so it cannot race live collection. Serialized instead against the
   OTHER ScratchPostgres classes on this same assembly's parallel pool (review-4341-r1, the flaky-test note):
   see QueryStoreIntervalWideFaultInjectionCollection for why. */
[Collection("query-store-interval-wide-fault-injection")]
public sealed class QueryStoreIntervalWideWriterTests
{
    private const int ServerId = -3953900;

    private static readonly DateTime T0 = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* ---- the oracle: every column the wide table stores (57), reused for both sides of the comparison ------- */

    private static readonly string[] WideColumns =
    {
        "collection_time", "server_id", "database_name", "query_id", "plan_id", "execution_type_desc",
        "first_execution_time", "last_execution_time", "module_name", "query_text", "query_hash",
        "execution_count", "avg_duration_us", "min_duration_us", "max_duration_us", "avg_cpu_time_us",
        "min_cpu_time_us", "max_cpu_time_us", "avg_logical_io_reads", "min_logical_io_reads",
        "max_logical_io_reads", "avg_logical_io_writes", "min_logical_io_writes", "max_logical_io_writes",
        "avg_physical_io_reads", "min_physical_io_reads", "max_physical_io_reads", "avg_clr_time_us",
        "min_clr_time_us", "max_clr_time_us", "min_dop", "max_dop", "avg_query_max_used_memory",
        "min_query_max_used_memory", "max_query_max_used_memory", "avg_rowcount", "min_rowcount",
        "max_rowcount", "avg_num_physical_io_reads", "min_num_physical_io_reads", "max_num_physical_io_reads",
        "avg_log_bytes_used", "min_log_bytes_used", "max_log_bytes_used", "avg_tempdb_space_used",
        "min_tempdb_space_used", "max_tempdb_space_used", "plan_type", "plan_forcing_type", "is_forced_plan",
        "force_failure_count", "last_force_failure_reason", "compatibility_level", "query_plan_hash",
        "replica_role", "runtime_stats_interval_id", "interval_start_time_utc",
    };

    private static string TableRowsSql =>
        $"SELECT {string.Join(", ", WideColumns)} FROM collect.query_store_interval_wide WHERE server_id = @server_id";

    /// <summary>
    /// Raw's own dedup, every outcome, via <c>DISTINCT ON</c> (equivalent to the shipped array_agg tie-break, and
    /// exactly the predicate <see cref="QueryStoreIntervalWide.UpsertSql"/> itself uses). No batch here is ever
    /// written without being applied in the same transaction, so comparing against ALL of raw is equivalent to
    /// "a window ending at applied_through" (design review item 2): applied_through always reaches the newest row.
    /// </summary>
    private static string RawDedupSql => $@"
SELECT DISTINCT ON ({QueryStoreIntervalWide.BatchIdentityColumns})
    {string.Join(", ", WideColumns)}
FROM collect.query_store_stats
WHERE server_id = @server_id
AND   first_execution_time IS NOT NULL
ORDER BY
    {QueryStoreIntervalWide.BatchIdentityColumns},
    collection_time DESC,
    execution_count DESC";

    private static async Task AssertTableEqualsRawDedupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var tableOnly = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM (({TableRowsSql}) EXCEPT ({RawDedupSql})) AS d", ct);
        var rawOnly = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM (({RawDedupSql}) EXCEPT ({TableRowsSql})) AS d", ct);
        var tableCount = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM ({TableRowsSql}) AS d", ct);
        var rawCount = await ScalarLongAsync(connection, $"SELECT COUNT(*) FROM ({RawDedupSql}) AS d", ct);

        Assert.True(rawCount > 0, "the seed wrote no raw rows with a first_execution_time, so the comparison below is vacuous");
        Assert.Equal(0, tableOnly);
        Assert.Equal(0, rawOnly);
        Assert.Equal(rawCount, tableCount);
    }

    /* ---- the seed ---------------------------------------------------------------------------------------- */

    private static QueryStoreCollector.Row Row(
        string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
        long cpuUs, string? role = null, string type = "Regular") => new()
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
        };

    private static ServerRuntime Server() => new()
    {
        Config = new MonitoredServer { Name = "qsiw-3953", Host = "qsiw-3953-host" },
        ConnectionString = "Server=qsiw-3953-host",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "qsiw-3953-host",
        ServerId = ServerId,
        EngineEdition = 3,
    };

    private static async Task WriteAsync(
        DarlingCollectorRunner runner, DateTime collectionTime, CollectorContext context, CancellationToken ct,
        params QueryStoreCollector.Row[] rows)
    {
        foreach (var batch in rows.GroupBy(r => r.DatabaseName))
        {
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, batch.ToList(), Server(), collectionTime, context, ct);
        }
    }

    /* ---- the tests --------------------------------------------------------------------------------------- */

    /// <summary>
    /// One batch lands in raw, V143 and the wide table in one transaction (the real write path never diverts a
    /// batch to only one of them). Every outcome reaches the wide table, including Aborted/Exception, which V143
    /// never stores. Re-applying the migration and re-writing a stale batch both change nothing.
    /// </summary>
    [Fact]
    public async Task TheWideTableEqualsTheRawDedup_EveryOutcome_AndReapplyingChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 wide writer test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = NewContext();

        var i100 = T0;
        await WriteAsync(runner, T0.AddMinutes(10), context, ct,
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(9), 10, 500),
            Row("qsA", 2, 21, 100, i100, T0.AddMinutes(7), 3, 900, type: "Aborted"),
            Row("qsA", 2, 21, 100, i100, T0.AddMinutes(7), 1, 900, type: "Exception"),
            Row("qsB", 3, 31, 100, i100, T0.AddMinutes(6), 7, 300));
        await WriteAsync(runner, T0.AddMinutes(40), context, ct,
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(39), 20, 510));

        /* Migration re-apply is idempotent: every CREATE is IF NOT EXISTS, so a second run is a no-op. */
        await PgMigrations.MigrateAsync(connection, ct);

        await AssertTableEqualsRawDedupAsync(connection, ct);

        /* Both non-Regular outcomes reached the wide table — the headline difference from V143. */
        Assert.Equal(2, await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_wide WHERE server_id = @server_id AND execution_type_desc <> 'Regular'", ct));
        /* V143 stores only Regular: qsA/1, qsA/1 (updated), qsB/3 — never the Aborted/Exception pair. */
        Assert.Equal(0, await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_latest WHERE server_id = @server_id AND query_id = 2", ct));

        /* The newer batch's running-max win landed. */
        Assert.Equal(20, await ScalarLongAsync(connection,
            "SELECT execution_count FROM collect.query_store_interval_wide WHERE server_id = @server_id AND query_id = 1 AND execution_type_desc = 'Regular'", ct));

        /* Idempotence: re-writing a stale batch changes nothing in the wide table. */
        var tableBefore = await ScalarStringAsync(connection, $"SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM ({TableRowsSql}) AS t", ct);
        await WriteAsync(runner, T0.AddMinutes(40), context, ct,
            Row("qsA", 1, 11, 100, i100, T0.AddMinutes(39), 20, 510));
        Assert.Equal(tableBefore, await ScalarStringAsync(connection, $"SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM ({TableRowsSql}) AS t", ct));
        await AssertTableEqualsRawDedupAsync(connection, ct);
    }

    /// <summary>
    /// A fault that only the WIDE apply hits (its unique index is gone) records a wide pending row and leaves
    /// V143's claim and rows alone — the two applies run under independent savepoints in the same transaction.
    /// </summary>
    [Fact]
    public async Task AWideOnlyApplyFault_RecordsAWidePendingRow_AndLeavesV143Intact()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 wide writer test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = NewContext();

        await WriteAsync(runner, T0.AddMinutes(10), context, ct, Row("qsA", 1, 11, 100, T0, T0.AddMinutes(9), 10, 500));
        var (v143FilledSinceBefore, v143AppliedThroughBefore) = await CoverageAsync(connection, "query_store_interval_latest_coverage", ct);

        await ExecAsync(connection, "DROP INDEX collect.ux_query_store_interval_wide", ct);
        await WriteAsync(runner, T0.AddMinutes(40), context, ct, Row("qsA", 1, 11, 100, T0, T0.AddMinutes(39), 20, 510));

        /* V143 applied normally: its row, and its claim, moved on exactly as if nothing had failed. */
        Assert.Equal(20, await ScalarLongAsync(connection, "SELECT execution_count FROM collect.query_store_interval_latest WHERE server_id = @server_id", ct));
        var (v143FilledSinceAfter, v143AppliedThroughAfter) = await CoverageAsync(connection, "query_store_interval_latest_coverage", ct);
        Assert.Equal(v143FilledSinceBefore, v143FilledSinceAfter);
        Assert.Equal(v143AppliedThroughBefore, v143AppliedThroughAfter);

        /* The wide table missed it: still the first batch's row, and a pending replay row queued. */
        Assert.Equal(10, await ScalarLongAsync(connection, "SELECT execution_count FROM collect.query_store_interval_wide WHERE server_id = @server_id", ct));
        Assert.Equal(1, await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id AND failure LIKE '42P10%'", ct));

        /* Restored: the next batch replays the queued one, and the wide pending table drains. */
        await ExecAsync(connection, PgMigrations.Scripts.Single(m => m.Version == 145).Sql, ct);
        await WriteAsync(runner, T0.AddMinutes(65), context, ct, Row("qsB", 3, 31, 100, T0, T0.AddMinutes(58), 9, 310));

        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_wide_pending", ct));
        await AssertTableEqualsRawDedupAsync(connection, ct);
    }

    /// <summary>
    /// A batch applied only to V143 (an old writer that never knew about the wide table) is caught by the wide
    /// table's own hourly gap check: raw has a row no apply of the wide table ever accounted for, so its coverage
    /// restarts above it, and V143's own coverage — which DID account for that row — is untouched.
    /// </summary>
    [Fact]
    public async Task ABatchAppliedOnlyToV143_IsCaughtByTheWideGapCheck()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 wide writer test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var context = NewContext();

        /* Establish the wide table's coverage row first (a fresh install's EnsureCoverageSql claims everything up
           to "now" as covered, so the gap this test forces has to be AFTER that point, not before it — otherwise
           it is indistinguishable from ordinary pre-install history). */
        await WriteAsync(new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), T0.AddMinutes(5), context, ct,
            Row("qsA", 9, 91, 900, T0, T0.AddMinutes(4), 1, 100));
        var (_, wideAppliedThroughAtInstall) = await CoverageAsync(connection, "query_store_interval_wide_coverage", ct);

        /* Simulate an old writer: raw + V143's own real apply, directly, never touching QueryStoreIntervalWide, for
           a batch strictly AFTER the wide table's claim so far. */
        var oldBatchTime = TruncateToSeconds(wideAppliedThroughAtInstall.AddSeconds(2));
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, execution_count) " +
            "VALUES (9200000001, '" + oldBatchTime.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + "', @server_id, 'qsiw-3953-host', 'qsA', 1, 11, 'Regular', '2026-09-01 00:00', '2026-09-01 01:29', 40)", ct);

        var v143 = new QueryStoreIntervalLatest();
        await using (var applyConn = new NpgsqlConnection(scratch.ConnectionString))
        {
            await applyConn.OpenAsync(ct);
            Assert.True(await v143.PrepareServerAsync(applyConn, ServerId, 30, ct));
            await using var tx = await applyConn.BeginTransactionAsync(ct);
            var applied = await v143.ApplyBatchAsync(applyConn, tx, ServerId, oldBatchTime, new[] { "qsA" }, skipApply: false, 30, ct);
            Assert.Equal(QueryStoreIntervalLatest.ApplyResult.Applied, applied);
            await tx.CommitAsync(ct);
        }

        /* V143 has the row (plus the earlier install row) and its claim covers it; the wide table only has the
           earlier install row — it has never heard of the old-writer batch. */
        Assert.Equal(2, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_latest WHERE server_id = @server_id", ct));
        Assert.Equal(1, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_wide WHERE server_id = @server_id", ct));
        Assert.Equal(0, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_wide WHERE server_id = @server_id AND query_id = 1", ct));

        /* A FRESH runner instance: its QueryStoreIntervalWide has never gap-checked this server, so the check
           below runs on this prepare rather than waiting for GapCheckInterval to elapse in real wall-clock time. */
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var restarted = DateTime.UtcNow;
        await WriteAsync(runner, T0.AddMinutes(95), context, ct, Row("qsA", 1, 11, 100, T0, T0.AddMinutes(94), 45, 500));

        var (wideFilledSince, wideAppliedThrough) = await CoverageAsync(connection, "query_store_interval_wide_coverage", ct);
        Assert.True(wideFilledSince > oldBatchTime, $"the wide gap check did not restart coverage past the unaccounted row at {oldBatchTime:o}");
        Assert.True(wideFilledSince >= restarted.AddSeconds(-1), $"filled_since {wideFilledSince:o} is below the restart clock {restarted:o}");
        Assert.Equal(oldBatchTime, wideAppliedThrough);

        /* V143's own gap check found nothing wrong: it already accounted for that row itself. */
        var (v143FilledSince, _) = await CoverageAsync(connection, "query_store_interval_latest_coverage", ct);
        Assert.True(v143FilledSince <= oldBatchTime.AddSeconds(1), "V143's coverage was reset even though V143 itself already applied the row");
    }

    [Fact]
    public async Task Retention_DropsWideIntervalsPastNineDays_AndStalePendingRows_AndKeepsTheRest()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 wide retention test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = NewContext();

        var now = TruncateToSeconds(DateTime.UtcNow);
        await WriteAsync(runner, now.AddDays(-10), context, ct,
            Row("qsA", 1, 11, 100, now.AddDays(-10).AddHours(-2), now.AddDays(-10).AddHours(-1), 10, 500));
        await WriteAsync(runner, now.AddDays(-2), context, ct,
            Row("qsA", 1, 11, 101, now.AddDays(-2).AddHours(-2), now.AddDays(-2).AddHours(-1), 10, 500));
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) " +
            "VALUES (@server_id, '2026-01-01', 'old', '2026-01-01'), (@server_id, '2026-01-02', 'new', now() AT TIME ZONE 'UTC')", ct);

        var floorBefore = await ScalarLongAsync(connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_wide WHERE server_id = @server_id AND runtime_stats_interval_id = 100", ct);
        Assert.Equal(1, floorBefore);

        await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, logger: null, ct);

        /* Past 9 days dropped, the 2-day-old row kept — the table floor (MIN(first_execution_time)) moves with it. */
        Assert.Equal(1, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_wide WHERE server_id = @server_id", ct));
        Assert.Equal(101, await ScalarLongAsync(connection, "SELECT runtime_stats_interval_id FROM collect.query_store_interval_wide WHERE server_id = @server_id", ct));
        Assert.Equal(1, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id AND database_name = 'new'", ct));
        Assert.Equal(1, await ScalarLongAsync(connection, "SELECT COUNT(*) FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id", ct));

        var tableFloor = await ScalarStringAsync(connection, "SELECT MIN(first_execution_time)::text FROM collect.query_store_interval_wide WHERE server_id = @server_id", ct);
        Assert.Equal(now.AddDays(-2).AddHours(-2).ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture), tableFloor);
    }

    /* ---- helpers ----------------------------------------------------------------------------------------- */

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static CollectorContext NewContext() => new()
    {
        ServerId = ServerId,
        ServerName = "qsiw-3953-host",
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

    private static async Task<(DateTime FilledSince, DateTime AppliedThrough)> CoverageAsync(NpgsqlConnection connection, string coverageTable, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT filled_since, applied_through FROM collect.{coverageTable} WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"no coverage row in {coverageTable} for the server");
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
