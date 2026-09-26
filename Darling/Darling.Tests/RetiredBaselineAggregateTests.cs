/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// Pins the #2007 retirement of the CPU/IO baseline aggregates. The invariants that must not
/// drift: the retired names stay OUT of <see cref="TimescaleSupport.BaselineAggregates"/> (a
/// re-add would resurrect an object the startup sweep deletes — a create/drop fight on every
/// restart); the retirement list itself names exactly the two orphans; the drop SQL discriminates
/// continuous-aggregate from plain-fallback-view (a CAGG is also a <c>relkind='v'</c> view, and
/// the two need different DROP verbs); and the worker runs the drop in the UNGATED fallback block
/// so both store shapes are cleaned. The live half exercises the sweep against dev Postgres in
/// all three states an upgraded store can present: the CAGG shape (with policies riding along
/// into the drop), the plain-view shape, and already-gone (idempotent no-op).
/// </summary>
public sealed class RetiredBaselineAggregateTests
{
    [Fact]
    public void RetiredList_NamesExactlyTheTwoOrphans()
    {
        Assert.Equal(
            new[] { "cpu_utilization_baseline", "file_io_baseline" },
            TimescaleSupport.RetiredBaselineRelations);
    }

    [Fact]
    public void BaselineAggregates_DoNotContainRetiredNames_SoTheSweepCannotRecreateWhatItDrops()
    {
        var living = TimescaleSupport.BaselineAggregates.Select(a => a.View).ToArray();

        Assert.Equal(7, living.Length);
        foreach (var retired in TimescaleSupport.RetiredBaselineRelations)
        {
            Assert.DoesNotContain(retired, living);
        }
    }

    [Fact]
    public void DropSql_DiscriminatesCaggFromPlainView_AndProbesWithoutPrivileges()
    {
        var sql = TimescaleSupport.DropRetiredBaselineRelationSql("cpu_utilization_baseline");

        /* Both verbs present, chosen by the continuous_aggregates membership check — and the
           timescaledb_information reference is itself to_regclass-guarded so the block runs on
           stores that never had the extension. */
        Assert.Contains("DROP MATERIALIZED VIEW IF EXISTS collect.cpu_utilization_baseline CASCADE", sql, StringComparison.Ordinal);
        Assert.Contains("DROP VIEW IF EXISTS collect.cpu_utilization_baseline CASCADE", sql, StringComparison.Ordinal);
        Assert.Contains("timescaledb_information.continuous_aggregates", sql, StringComparison.Ordinal);
        Assert.Contains("to_regclass('timescaledb_information.continuous_aggregates')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Worker_RunsTheRetirementDrop_InTheUngatedFallbackBlock()
    {
        var worker = ReadWorkerSource();

        /* #3817: both calls moved into the shared convergence list, so their ORDER is now the list's order
           (pinned in StoreObjectConvergenceTests, where the one-order claim lives) and their REACHABILITY is
           the ungated segment call's. Both halves of the original property are still asserted here — the drop
           runs, and it runs before the ensure — because this file is where a reader looks for the retirement
           drop's placement; what changed is that "before the ensure" is read off the list the product
           iterates rather than off two adjacent awaits. */
        var dropAt = worker.IndexOf("TimescaleSupport.DropRetiredBaselineAggregatesAsync(", StringComparison.Ordinal);
        var ensureAt = worker.IndexOf("TimescaleSupport.EnsureBaselineFallbackViewsAsync(", StringComparison.Ordinal);
        var plainModeAt = worker.IndexOf("StoreObjectConvergenceStage.Ungated, startupConvergence, stoppingToken);", StringComparison.Ordinal);

        Assert.True(dropAt > 0, "the worker must run the retirement drop");
        /* Same reachability argument the fallback ensure carries (BaselineSupplyTests): after the
           TimescaleDB block's catch, so plain-PG stores' fallback VIEWS are cleaned too — the
           retired names exist in both implementations in the field. */
        Assert.True(plainModeAt > 0,
            "the retirement drop must be run by the UNGATED convergence segment — after the TimescaleDB block, on every path");
        Assert.True(ensureAt > dropAt,
            "drop retired names before the ensure sweep runs (order is hygiene, not correctness — the ensure list no longer contains them)");
    }

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, "could not locate the repo root from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}

/// <summary>
/// The live retirement sweep against dev Postgres, in the three states an upgraded store presents.
/// The fixture recreates the RETIRED objects from their pre-#2007 definitions inline (the
/// constants are gone from the codebase — that is the point), sourced on a hypertable the same
/// idempotent way the worker converts them.
/// </summary>
[Collection("live-postgres")]
public sealed class RetiredBaselineAggregateLiveTests
{
    [Fact]
    public async Task Sweep_DropsCaggWithPolicies_ThenPlainView_ThenNoOps_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live retirement sweep.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* ---- fixture: the pre-#2007 CAGG shape for cpu (policies riding along), the plain
                fallback-view shape for file_io — one sweep must clean BOTH implementations.
                create_hypertable is the worker's own idempotent conversion; CAGG CREATE cannot
                run inside a transaction, so every statement executes on its own. */
        await ExecuteAsync(connection,
            "SELECT create_hypertable('collect.cpu_utilization_stats', by_range('collection_time', INTERVAL '1 day'), if_not_exists => true, migrate_data => true)", ct);
        await ExecuteAsync(connection, @"
CREATE MATERIALIZED VIEW IF NOT EXISTS collect.cpu_utilization_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT server_id, time_bucket('1 hour', collection_time) AS bucket, collection_time,
       sum(sqlserver_cpu_utilization) AS cpu_sum,
       sum(power(sqlserver_cpu_utilization, 2)) AS cpu_sumsq,
       count(sqlserver_cpu_utilization) AS cpu_count
FROM collect.cpu_utilization_stats
GROUP BY server_id, bucket, collection_time
WITH NO DATA", ct);
        await ExecuteAsync(connection,
            "SELECT add_continuous_aggregate_policy('collect.cpu_utilization_baseline', start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', if_not_exists => true)", ct);
        await ExecuteAsync(connection,
            "SELECT add_retention_policy('collect.cpu_utilization_baseline', drop_after => INTERVAL '35 days', if_not_exists => true)", ct);
        await ExecuteAsync(connection,
            "CREATE OR REPLACE VIEW collect.file_io_baseline AS SELECT server_id, date_trunc('hour', collection_time) AS bucket, collection_time, count(*) AS row_count FROM collect.file_io_stats GROUP BY 1, 2, 3", ct);

        /* ---- the sweep: both shapes drop in one pass; policies go with the CAGG. */
        var dropped = await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, ct);
        Assert.Equal(2, dropped);

        using (var check = new NpgsqlCommand(
            "SELECT to_regclass('collect.cpu_utilization_baseline') IS NULL AND to_regclass('collect.file_io_baseline') IS NULL", connection))
        {
            Assert.True(await check.ExecuteScalarAsync(ct) is true, "both retired relations must be gone");
        }

        using (var policyCheck = new NpgsqlCommand(
            "SELECT COUNT(*) FROM timescaledb_information.continuous_aggregates WHERE view_name = ANY($1)", connection))
        {
            policyCheck.Parameters.AddWithValue(TimescaleSupport.RetiredBaselineRelations);
            Assert.Equal(0L, await policyCheck.ExecuteScalarAsync(ct));
        }

        /* ---- idempotent: a second pass finds nothing. */
        Assert.Equal(0, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, ct));
    }

    /// <summary>
    /// #3653 (A6 + the mechanical half of A10), proven live end to end on TimescaleDB: the legacy
    /// <c>wait_stats_baseline</c> and its interval-honest successor built side by side over the SAME planted
    /// history, the restart collection's fabricated zero IN the legacy sum and ABSENT from the successor, the
    /// provider reading the legacy relation while the successor is shallower for this server and the successor
    /// once it reaches as far, and the retirement sweep holding the legacy on day one and dropping it —
    /// policies and all — once the clock says the successor covers the tier.
    ///
    /// <para><b>The planted history is chosen so the legacy read and the successor read DIFFER.</b> The
    /// magnitude heuristic (<c>LAG &gt; 10000</c>) catches a restart zero only when the collection before it was
    /// busy. Here the collection before the restart carries 5,000 ms — a real but quiet minute — so the
    /// heuristic cannot see the restart, the legacy sum counts its zero as a sample (12 samples, mean 45,417),
    /// and the successor, which dropped it on the collector's own interval-0 verdict, does not (11 samples, mean
    /// 49,545). That difference is the A6 contamination made measurable, and it is what makes each supply choice
    /// below observable rather than inferred. Twelve collections per hour, because the Full tier needs
    /// <c>BaselineMath.CollapseThreshold</c> (10) samples in the bucket to be selected at all.</para>
    ///
    /// <para>Two hours are planted: one of measured-interval collections (a restart, a measured idle zero) and
    /// one of pre-column NULL-interval collections (a zero after a busy minute that only the heuristic can
    /// drop, and an idle zero after it that must survive), so all three interval states are exercised in one
    /// pass. Skips without TimescaleDB — the plain-view half of the supersession is covered by the anomaly
    /// tests' fallback-view runs and by <c>SupersededBaselineRelationDropsAt</c>'s pure pins.</para>
    /// </summary>
    [Fact]
    public async Task Supersession_RestartZeroInLegacyNotSuccessor_ProviderFollowsCoverage_SweepWaitsForTheTier_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live supersession test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = 9107; // own id — this test cleans its own rows
        const string serverName = "supersession-e2e";
        const string waitType = "SUPERSESSION_E2E_WAIT";
        var legacy = TimescaleSupport.LegacyWaitStatsBaselineView;
        var successor = TimescaleSupport.WaitStatsIntervalBaselineView;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.SkipWhen(!await TimescaleSupport.DetectAsync(connection, ct),
            "The live supersession test needs TimescaleDB: the coverage difference it proves exists only between materializations.");

        /* ---- fixture. wait_stats as a hypertable (the worker's own idempotent conversion), the two names
                cleared in whichever implementation an earlier run or a sibling test left (CREATE ... IF NOT
                EXISTS would silently no-op against a plain fallback view), our rows gone. */
        await ExecuteAsync(connection,
            "SELECT create_hypertable('collect.wait_stats', by_range('collection_time', INTERVAL '1 day'), if_not_exists => true, migrate_data => true)", ct);
        await ExecuteAsync(connection, TimescaleSupport.DropRetiredBaselineRelationSql(legacy), ct);
        await ExecuteAsync(connection, TimescaleSupport.DropRetiredBaselineRelationSql(successor), ct);
        await ExecuteAsync(connection, $"DELETE FROM collect.wait_stats WHERE server_id = {serverId}", ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            /* Monday 10:00 UTC, 8-14 days back — inside every 30-day window used below, and a different
               (hour, dow) bucket from the anomaly tests' fixture only by server id. */
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var hour1 = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);
            var hour2 = hour1.AddHours(1);

            /* Hour 1 — MEASURED intervals (300 s at 5-minute spacing, twelve collections). c2 is a
               quiet-but-real 5,000 ms collection, c3 is the restart as the collector writes it (delta 0 WITH
               interval 0), c4 a measured idle zero, the rest 60,000 ms. */
            long[] hour1Deltas = { 60000, 5000, 0, 0, 60000, 60000, 60000, 60000, 60000, 60000, 60000, 60000 };
            var collectionId = 900001L;
            for (var i = 0; i < hour1Deltas.Length; i++)
            {
                await InsertWaitAsync(connection, collectionId++, hour1.AddMinutes(5 * i), serverId, serverName, waitType, hour1Deltas[i], i == 2 ? 0 : 300, ct);
            }

            /* Hour 2 — PRE-COLUMN rows (NULL interval) at 120,000 ms per 300 s (400 ms/s, above the rate arm's
               100 ms/s bar): c3 is a zero after a busy collection (only the heuristic can drop it, and it
               must), c4 a zero after a zero (kept, genuine idle). */
            long[] hour2Deltas = { 120000, 120000, 0, 0, 120000, 120000, 120000, 120000, 120000, 120000, 120000, 120000 };
            for (var i = 0; i < hour2Deltas.Length; i++)
            {
                await InsertWaitAsync(connection, collectionId++, hour2.AddMinutes(5 * i), serverId, serverName, waitType, hour2Deltas[i], null, ct);
            }

            /* Both aggregates, side by side, from the SAME registered/legacy texts the product carries; the
               legacy gets the refresh policy every pre-#3653 store gave it, so the drop can be shown to cascade
               it. Legacy refreshed over everything; successor over hour 2 ONLY, so for this server it is one
               hour shallower than the legacy — the real-time union cannot fill hour 1 for it, because an
               un-materialized region BELOW the watermark is served by neither branch. */
            await ExecuteAsync(connection, TimescaleSupport.LegacyCreateWaitStatsBaselineSql, ct);
            await ExecuteAsync(connection, TimescaleSupport.CreateWaitStatsIntervalBaselineSql, ct);
            await ExecuteAsync(connection,
                $"SELECT add_continuous_aggregate_policy('collect.{legacy}', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', if_not_exists => true)", ct);
            await RefreshFromAsync(connection, legacy, hour1, ct);
            await RefreshFromAsync(connection, successor, hour2, ct);

            /* ---- A6, measured: the restart collection is a total_wait_ms = 0 row in the legacy sum and no row
                    at all in the successor; the successor carries the measured interval for hour 1 and NULL for
                    the pre-column hour. */
            var restartAt = hour1.AddMinutes(10);
            Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT total_wait_ms FROM collect.{legacy} WHERE server_id = {serverId} AND collection_time = $1", restartAt, ct));
            Assert.Equal(24L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{legacy} WHERE server_id = {serverId}", null, ct));

            /* min(bucket) for this server: the legacy reaches hour 1, the successor only hour 2 — the state the
               supply rule has to notice. */
            Assert.Equal(hour1, await ScalarAsync<DateTime>(connection, $"SELECT min(bucket) FROM collect.{legacy} WHERE server_id = {serverId}", null, ct));
            Assert.Equal(hour2, await ScalarAsync<DateTime>(connection, $"SELECT min(bucket) FROM collect.{successor} WHERE server_id = {serverId}", null, ct));

            var provider = new PgBaselineProvider(postgres);
            var analysisTime = hour1.AddDays(7);

            /* ---- the provider reads the LEGACY relation while the successor is shallower: hour 1 is the
                    contaminated statistic (12 samples, the unseen restart zero among them). Hour 2 is the same
                    on either supply (NULL-interval rows keep the heuristic, which drops c3's zero after a
                    120,000 collection and keeps c4's zero after a zero): 11 samples, mean 109,091. */
            var legacyRead = await provider.GetBaselineAsync(serverId, MetricNames.WaitStats, analysisTime);
            Assert.Equal(12L, legacyRead.SampleCount);
            Assert.Equal(545000.0 / 12.0, legacyRead.Mean, 0.01);
            Assert.Equal(BaselineTier.Full, legacyRead.Tier);

            var hour2Read = await provider.GetBaselineAsync(serverId, MetricNames.WaitStats, analysisTime.AddHours(1));
            Assert.Equal(11L, hour2Read.SampleCount);
            Assert.Equal(1200000.0 / 11.0, hour2Read.Mean, 0.01);
            Assert.Equal(BaselineTier.Full, hour2Read.Tier);

            /* ---- day one: the sweep judges the legacy CAGG against the tier horizon and HOLDS it — policy
                    and all (the policy's presence is asserted here so the cascade below has a before-state). */
            var verdict = await TimescaleSupport.JudgeSupersededBaselineRelationAsync(connection, legacy, successor, DateTime.UtcNow, ct);
            Assert.Equal(TimescaleSupport.SupersededBaselineDecision.SuccessorShort, verdict.Decision);
            Assert.True(verdict.LegacyIsContinuousAggregate);
            /* #4289: this legacy aggregate has the restart-zero row refreshed into it (line ~267), so it HOLDS
               rows — the state that still waits for the successor's coverage, unlike
               EmptySupersededBaselineLiveTests.EmptyLegacyAggregate_DropsOnSight_EvenWithAnEmptySuccessor_AgainstDevPostgres. */
            Assert.True(verdict.LegacyHoldsRows);
            Assert.Equal(hour2, verdict.SuccessorOldest);
            await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, DateTime.UtcNow, ct);
            Assert.True(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationExistsSql(legacy), null, ct), "the legacy aggregate must survive a day-one sweep");
            /* timescaledb_information.jobs names a continuous-aggregate policy's target by the VIEW's
               schema and name (2.28.1, verified on the rig), not by its materialization hypertable. */
            var legacyPolicySql = $"SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = '{legacy}' AND proc_name = 'policy_refresh_continuous_aggregate'";
            Assert.Equal(1L, await ScalarAsync<long>(connection, legacyPolicySql, null, ct));

            /* ---- the successor catches up (the backfill's forced refresh over the whole range): now it
                    reaches as far as the legacy for this server and the provider switches to it — the restart
                    zero is gone from the statistic. */
            await RefreshFromAsync(connection, successor, hour1, ct, force: true);
            Assert.Equal(23L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{successor} WHERE server_id = {serverId}", null, ct));
            Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{successor} WHERE server_id = {serverId} AND collection_time = $1", restartAt, ct));
            Assert.Equal(300, await ScalarAsync<int>(connection, $"SELECT sample_interval_seconds FROM collect.{successor} WHERE server_id = {serverId} AND collection_time = $1", hour1, ct));
            Assert.True(await ScalarAsync<bool>(connection, $"SELECT sample_interval_seconds IS NULL FROM collect.{successor} WHERE server_id = {serverId} AND collection_time = $1", hour2, ct));

            provider.ClearCache();
            var successorRead = await provider.GetBaselineAsync(serverId, MetricNames.WaitStats, analysisTime);
            Assert.Equal(11L, successorRead.SampleCount);
            Assert.Equal(545000.0 / 11.0, successorRead.Mean, 0.01);
            Assert.Equal(BaselineTier.Full, successorRead.Tier);

            /* The rate arm over the same supply: hour 1 rates off the STORED 300 s (c1 200 ms/s, c2 16.67, c4 0,
               c5..c12 200 — the window's first collection is rated, not dropped, and c4's measured zero is kept
               whatever c2 read); hour 2 off the LAG gap (its c1's prior is hour 1's c12, 300 s back, so it
               rates at 400; c3's zero after 400 ms/s drops on the heuristic; c4's idle zero after it survives). */
            var rateHour1 = await provider.GetBaselineAsync(serverId, MetricNames.WaitMsPerSec, analysisTime);
            Assert.Equal(11L, rateHour1.SampleCount);
            Assert.Equal((200.0 * 9 + 5000.0 / 300.0 + 0) / 11.0, rateHour1.Mean, 0.01);
            var rateHour2 = await provider.GetBaselineAsync(serverId, MetricNames.WaitMsPerSec, analysisTime.AddHours(1));
            Assert.Equal(11L, rateHour2.SampleCount);
            Assert.Equal((400.0 * 10 + 0) / 11.0, rateHour2.Mean, 0.01);

            /* ---- day forty: the clock says the successor covers the tier; the legacy drops with its policy. */
            var dayForty = DateTime.UtcNow.AddDays(40);
            var dayFortyVerdict = await TimescaleSupport.JudgeSupersededBaselineRelationAsync(connection, legacy, successor, dayForty, ct);
            Assert.Equal(TimescaleSupport.SupersededBaselineDecision.Drop, dayFortyVerdict.Decision);
            Assert.True(dayFortyVerdict.LegacyHoldsRows, "this legacy still HOLDS rows at day forty — it drops on coverage, not on #4289's empty-aggregate rule");
            Assert.True(await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, dayForty, ct) >= 1);
            Assert.False(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationExistsSql(legacy), null, ct), "the legacy aggregate must be gone");
            Assert.True(await ScalarAsync<bool>(connection, TimescaleSupport.BaselineRelationExistsSql(successor), null, ct), "the successor must stay");
            Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect' AND view_name = '{legacy}'", null, ct));
            Assert.Equal(0L, await ScalarAsync<long>(connection, legacyPolicySql, null, ct));
            Assert.Equal(TimescaleSupport.SupersededBaselineDecision.LegacyAbsent,
                (await TimescaleSupport.JudgeSupersededBaselineRelationAsync(connection, legacy, successor, dayForty, ct)).Decision);

            /* And with the legacy gone the provider reads the successor without a coverage question. */
            provider.ClearCache();
            var afterRetirement = await provider.GetBaselineAsync(serverId, MetricNames.WaitStats, analysisTime);
            Assert.Equal(11L, afterRetirement.SampleCount);
            Assert.Equal(545000.0 / 11.0, afterRetirement.Mean, 0.01);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await ExecuteAsync(cleanup, TimescaleSupport.DropRetiredBaselineRelationSql(legacy), cleanupCt);
                await ExecuteAsync(cleanup, TimescaleSupport.DropRetiredBaselineRelationSql(successor), cleanupCt);
                await ExecuteAsync(cleanup, $"DELETE FROM collect.wait_stats WHERE server_id = {serverId}", cleanupCt);
            });
        }
    }

    private static async Task InsertWaitAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, int serverId, string serverName, string waitType, long deltaWaitMs, int? intervalSeconds, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(deltaWaitMs);
        command.Parameters.AddWithValue(intervalSeconds.HasValue ? intervalSeconds.Value : DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshFromAsync(NpgsqlConnection connection, string view, DateTime from, System.Threading.CancellationToken ct, bool force = false)
    {
        using var command = new NpgsqlCommand(TimescaleSupport.RefreshContinuousAggregateSql(view, force), connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(from);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, DateTime? at, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        if (at is DateTime bound)
        {
            command.Parameters.AddWithValue(bound);
        }

        var value = await command.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return (T)Convert.ChangeType(value, typeof(T), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #4289: the field-store defect, reproduced. A legacy CONTINUOUS AGGREGATE with nothing ever feeding its
/// source (no <c>wait_stats</c> rows at all) leaves it, and its interval-honest successor backfilled from
/// that same empty source, BOTH empty — the state <see cref="TimescaleSupport.SupersededBaselineRelationDropsAt"/>'s
/// coverage rule could never resolve, because a <c>NULL</c> successor oldest bucket never compares <c>&lt;=</c>
/// any horizon. Before #4289 that verdict was <c>SuccessorShort</c> forever; the legacy relation, and its
/// refresh/retention/compression jobs, would outlive the store.
///
/// <para><b>#1776 own-store</b>, not <c>[Collection("live-postgres")]</c>: <see cref="TimescaleSupport.BaselineRelationHasRowsSql"/>
/// reads through the legacy view's real-time aggregation (every legacy CREATE carries
/// <c>timescaledb.materialized_only = false</c>), which unions in EVERY row of the raw source table —
/// unscoped by <c>server_id</c>. On the shared <c>darlingtest</c> store, other live tests' own
/// <c>wait_stats</c> rows would make the legacy view non-empty regardless of what this test seeds or deletes,
/// so "empty" can only be asserted truthfully on a database nothing else ever writes to.</para>
/// </summary>
public sealed class EmptySupersededBaselineLiveTests
{
    [Fact]
    public async Task EmptyLegacyAggregate_DropsOnSight_EvenWithAnEmptySuccessor_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4289 empty-legacy test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #4289 empty-legacy test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        /* Same start order as the real service (DailyStitchLiveTests, SuccessorDailyLiveTests): collection_log
           becomes a hypertable BEFORE the ensure sweep, because collection_health_hourly (one of the ordinary
           HourlyAggregates the sweep also creates) selects FROM it. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* No refresh policy on this scratch database is ever exercised — nothing here calls run_job or waits on
           the scheduler — but stopped anyway, matching every other own-store fixture in this file, so a policy
           firing mid-test can never be the explanation for an unexpected row. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        /* The successor (wait_stats_interval_baseline) comes from the ordinary ensure sweep, WITH NO DATA — an
           empty successor, half of this test's state. Nothing ever inserts into collect.wait_stats on this
           scratch database, so the LEGACY's real-time aggregation stays empty too, once created below. */
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        const string legacy = TimescaleSupport.LegacyWaitStatsBaselineView;
        const string successor = TimescaleSupport.WaitStatsIntervalBaselineView;

        await using (var create = new NpgsqlCommand(TimescaleSupport.LegacyCreateWaitStatsBaselineSql, connection) { CommandTimeout = 120 })
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        Assert.False(await HasRowsAsync(connection, legacy, ct), "the legacy aggregate must be empty for this test to mean anything");
        Assert.False(await HasRowsAsync(connection, successor, ct), "the successor must be empty for this test to mean anything");

        var verdict = await TimescaleSupport.JudgeSupersededBaselineRelationAsync(connection, legacy, successor, DateTime.UtcNow, ct);
        Assert.Equal(TimescaleSupport.SupersededBaselineDecision.Drop, verdict.Decision);
        Assert.True(verdict.LegacyIsContinuousAggregate);
        Assert.False(verdict.LegacyHoldsRows);
        Assert.Null(verdict.SuccessorOldest);

        Assert.Equal(1, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, DateTime.UtcNow, ct));
        Assert.False(await RelationExistsAsync(connection, legacy, ct), "the empty legacy aggregate must be gone");
        Assert.True(await RelationExistsAsync(connection, successor, ct), "the successor must stay, empty or not");
    }

    /// <summary>#4292 Low 1: when the has-rows probe itself fails, the legacy must survive, not read as empty.
    /// <see cref="TimescaleSupport.JudgeSupersededBaselineRelationAsync"/> has no catch of its own, so the
    /// exception reaches <see cref="TimescaleSupport.DropRetiredBaselineAggregatesAsync"/>'s
    /// per-relation catch, which logs and skips the drop. Forced with a lock timeout (a plain <c>REVOKE SELECT</c>
    /// does not work here — <see cref="ScratchPostgres"/> connects as a superuser, which bypasses grants):
    /// a second connection holds <c>ACCESS EXCLUSIVE</c> on the legacy relation in an open transaction, and the
    /// judged connection carries a 1-second <c>lock_timeout</c>, so its <c>EXISTS</c> read against the legacy
    /// view times out server-side.</summary>
    [Fact]
    public async Task LegacyProbeFails_LegacySurvives_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4292 probe-error test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #4292 probe-error test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* Stopped for the same reason as the sibling test above: a policy firing mid-test must never be able
           to explain an unexpected lock wait or row. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        const string legacy = TimescaleSupport.LegacyWaitStatsBaselineView;
        const string successor = TimescaleSupport.WaitStatsIntervalBaselineView;

        await using (var create = new NpgsqlCommand(TimescaleSupport.LegacyCreateWaitStatsBaselineSql, connection) { CommandTimeout = 120 })
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        await using var locker = new NpgsqlConnection(scratch.ConnectionString);
        await locker.OpenAsync(ct);
        await using var lockTransaction = await locker.BeginTransactionAsync(ct);
        await using (var lockCommand = new NpgsqlCommand($"LOCK TABLE collect.{legacy} IN ACCESS EXCLUSIVE MODE", locker, lockTransaction))
        {
            await lockCommand.ExecuteNonQueryAsync(ct);
        }

        await using (var setLockTimeout = new NpgsqlCommand("SET lock_timeout = '1s'", connection))
        {
            await setLockTimeout.ExecuteNonQueryAsync(ct);
        }

        /* The lock blocks the has-rows probe, which times out at 1s and (correctly, today) propagates all the
           way out of DropRetiredBaselineAggregatesAsync's per-relation try, so the drop statement it guards is
           never attempted and this returns almost immediately -- dropped stays 0. Released 1.5s in regardless,
           on its own timer: a REGRESSION this test must still be able to catch -- a catch inside
           JudgeSupersededBaselineRelationAsync that swallows the probe error and answers "empty" -- reaches
           that same drop statement, which would otherwise sit behind the SAME lock and time out too, hiding the
           regression behind a false "dropped stayed 0". Releasing on a fixed timer, not after the drop
           completes, lets a wrongly-attempted drop actually succeed once unblocked, so it shows up as dropped=1. */
        var dropTask = TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, DateTime.UtcNow, ct);
        var releaseTask = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(1.5), ct);
            await lockTransaction.RollbackAsync(ct);
        }, ct);

        Assert.Equal(0, await dropTask);
        await releaseTask;

        Assert.True(await RelationExistsAsync(connection, legacy, ct), "the legacy aggregate must survive a failed probe");
        Assert.True(await RelationExistsAsync(connection, successor, ct), "the successor must stay regardless");
    }

    /// <summary>#4292: the keep side of the same coverage question. A legacy CONTINUOUS AGGREGATE that HOLDS
    /// rows, paired with a successor whose own oldest bucket is NEWER than the retention horizon (the
    /// successor is "short"), must not be dropped: <see cref="TimescaleSupport.SupersededBaselineRelationDropsAt"/>
    /// only drops a row-holding CAGG once the successor's coverage reaches the horizon. Two hours are planted
    /// into <c>collect.wait_stats</c> — one 40 days back (older than the 35-day <see
    /// cref="TimescaleSupport.BaselineRetentionSpan"/> horizon) and one 5 days back — and the LEGACY aggregate
    /// is never refreshed, so its real-time aggregation (<c>timescaledb.materialized_only = false</c>) unions in
    /// BOTH raw hours regardless of age, and it holds rows. The SUCCESSOR is refreshed ONLY from the 5-day-back
    /// hour forward, the same trick <see cref="RetiredBaselineAggregateLiveTests.Supersession_RestartZeroInLegacyNotSuccessor_ProviderFollowsCoverage_SweepWaitsForTheTier_AgainstDevPostgres"/>
    /// uses to make a successor shallower than its legacy: the un-materialized 40-day-old hour sits BELOW the
    /// resulting watermark, so it is served by neither the materialization nor the real-time union, and the
    /// successor's own oldest bucket lands at the 5-day-back hour — newer than the horizon, i.e. short.</summary>
    [Fact]
    public async Task NonEmptyLegacyAggregate_SuccessorShort_IsKept()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4292 non-empty-legacy test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #4292 non-empty-legacy test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        /* Same start order as the real service (DailyStitchLiveTests, SuccessorDailyLiveTests): collection_log
           becomes a hypertable BEFORE the ensure sweep, because collection_health_hourly (one of the ordinary
           HourlyAggregates the sweep also creates) selects FROM it. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* Stopped for the same reason as the sibling facts above: a policy firing mid-test must never be able
           to explain an unexpected row or watermark move. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        /* The successor (wait_stats_interval_baseline) comes from the ordinary ensure sweep, WITH NO DATA. */
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        const string legacy = TimescaleSupport.LegacyWaitStatsBaselineView;
        const string successor = TimescaleSupport.WaitStatsIntervalBaselineView;

        await using (var create = new NpgsqlCommand(TimescaleSupport.LegacyCreateWaitStatsBaselineSql, connection) { CommandTimeout = 120 })
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        const int serverId = 9292; // own id — this own-store scratch database has no sibling rows to collide with
        const string serverName = "nonempty-legacy-short-successor";
        const string waitType = "NONEMPTY_LEGACY_SHORT_SUCCESSOR_WAIT";
        var hourOld = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-40).Date.AddHours(9), DateTimeKind.Unspecified);
        var hourRecent = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-5).Date.AddHours(9), DateTimeKind.Unspecified);

        await InsertWaitAsync(connection, 929201L, hourOld, serverId, serverName, waitType, 60000, 300, ct);
        await InsertWaitAsync(connection, 929202L, hourRecent, serverId, serverName, waitType, 60000, 300, ct);

        /* The legacy aggregate is never refreshed: its real-time aggregation (materialized_only = false) unions
           in every raw row regardless of age when nothing has ever been materialized, so it holds both hours. */
        Assert.True(await HasRowsAsync(connection, legacy, ct), "the legacy aggregate must hold rows for this test to mean anything");

        /* The successor is refreshed ONLY from the recent hour forward, so the 40-day-old hour falls below the
           resulting watermark and is invisible to it — the successor's own oldest bucket lands at the recent
           hour, newer than the 35-day horizon. */
        await RefreshFromAsync(connection, successor, hourRecent, ct);
        Assert.Equal(hourRecent, await ScalarAsync<DateTime>(connection, $"SELECT min(bucket) FROM collect.{successor} WHERE server_id = {serverId}", null, ct));

        var verdict = await TimescaleSupport.JudgeSupersededBaselineRelationAsync(connection, legacy, successor, DateTime.UtcNow, ct);
        Assert.Equal(TimescaleSupport.SupersededBaselineDecision.SuccessorShort, verdict.Decision);
        Assert.True(verdict.LegacyIsContinuousAggregate);
        Assert.True(verdict.LegacyHoldsRows);
        Assert.Equal(hourRecent, verdict.SuccessorOldest);

        Assert.Equal(0, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, null, DateTime.UtcNow, ct));
        Assert.True(await RelationExistsAsync(connection, legacy, ct), "a row-holding legacy aggregate must survive while its successor is short");
        Assert.True(await HasRowsAsync(connection, legacy, ct), "the surviving legacy aggregate must still return its rows");
        Assert.True(await RelationExistsAsync(connection, successor, ct), "the successor must stay regardless");
    }

    private static async Task InsertWaitAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, int serverId, string serverName, string waitType, long deltaWaitMs, int? intervalSeconds, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collect.wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(deltaWaitMs);
        command.Parameters.AddWithValue(intervalSeconds.HasValue ? intervalSeconds.Value : DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshFromAsync(NpgsqlConnection connection, string view, DateTime from, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(TimescaleSupport.RefreshContinuousAggregateSql(view), connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(from);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, DateTime? at, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        if (at is DateTime bound)
        {
            command.Parameters.AddWithValue(bound);
        }

        var value = await command.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return (T)Convert.ChangeType(value, typeof(T), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    private static async Task<bool> HasRowsAsync(NpgsqlConnection connection, string view, System.Threading.CancellationToken ct)
    {
        using var probe = new NpgsqlCommand(TimescaleSupport.BaselineRelationHasRowsSql(view), connection) { CommandTimeout = 120 };
        return await probe.ExecuteScalarAsync(ct) is true;
    }

    private static async Task<bool> RelationExistsAsync(NpgsqlConnection connection, string view, System.Threading.CancellationToken ct)
    {
        using var probe = new NpgsqlCommand(TimescaleSupport.BaselineRelationExistsSql(view), connection) { CommandTimeout = 120 };
        return await probe.ExecuteScalarAsync(ct) is true;
    }
}
