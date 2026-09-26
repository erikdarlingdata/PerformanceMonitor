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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4231 stage 3a live pins: <c>DarlingDataReader.GetTopQueriesByCpuRoutedAsync</c> actually routes to the
/// hourly rollup once raw's floor ages past the window, THROUGH the product's own routed read (never by
/// running the rollup SQL directly — the standing rule, #4382/#4341-family). Raw and hourly must agree on
/// the (database_name, query_hash) totals for the same window, once rolled up.
///
/// <para><b>#1776 own-store</b> — mints a scratch database (it materializes a continuous aggregate the
/// shared fixture must never inherit), so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class TopQueriesHourlyRoutingLiveTests
{
    private const int ServerId = -943821;
    private const string ServerName = "a4231-3a-topn-hourly-routing";
    private const string Db = "TopNHourlyDb";

    /// <summary>Fixed anchor, never wall-clock relative (per the family's DailyStitchLiveTests precedent):
    /// a Monday, three whole days before "now" for this test's purposes — the window ages past raw's floor
    /// once we delete raw's rows for it, which is what actually drives the router, not calendar time.</summary>
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task GetTopQueriesByCpuRoutedAsync_RoutesToHourlyOnceRawIsPurged_AndTotalsAgree()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4231 stage-3a routing test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #4231 stage-3a routing test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var windowEnd = WindowStart.AddDays(1);
        var bodySucceeded = false;
        try
        {
            /* ── seed (a): the clean population — three query_hash groups, two "hosts" per group's raw rows
               (host_object_name varies) so the raw-vs-hourly rollup collapse is exercised, and every row has a
               real (nonzero) sample_interval_seconds. ── */
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "0xTOPQ1", "usp_HostA", 500_000L, 400_000L, 10L, 3600);
            await PlantAsync(connection, ct, WindowStart.AddHours(1).AddMinutes(20), "0xTOPQ1", "usp_HostB", 300_000L, 250_000L, 6L, 3600);
            await PlantAsync(connection, ct, WindowStart.AddHours(2), "0xTOPQ2", "usp_HostA", 200_000L, 180_000L, 5L, 3600);
            await PlantAsync(connection, ct, WindowStart.AddHours(3), "0xTOPQ3", "usp_HostB", 50_000L, 40_000L, 2L, 3600);

            /* ── seed (b): one raw row for a query_hash that appears ONLY as a zero-interval first-collection
               row. The hourly successor's CREATE bakes in IntervalHonestSourceFilter
               (sample_interval_seconds IS DISTINCT FROM 0), so this row is admitted to raw's read but
               EXCLUDED from the hourly rollup by construction — reported below as a real finding, not bent
               into a passing assertion. ── */
            await PlantAsync(connection, ct, WindowStart.AddHours(4), "0xZEROINTERVAL", "usp_HostA", 900_000L, 900_000L, 1L, 0);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* ── call 1: raw still covers the window (nothing purged yet) — must report tier_used = raw. ── */
            var rawResult = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                dataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, cancellationToken: ct);
            Assert.Equal(RetentionTier.Raw, rawResult.Tier);

            var rawTotalsByKey = RollUpByHash(rawResult.Rows);
            Assert.True(rawTotalsByKey.ContainsKey("0xTOPQ1"), "seed (a) group 1 must be present in the raw read");
            Assert.True(rawTotalsByKey.ContainsKey("0xZEROINTERVAL"), "raw's read admits the zero-interval row (#4231 3a finding)");

            /* Refresh the hourly successor over the window BEFORE deleting raw — the product's own
               backfill/refresh path, not a hand-built rollup row. */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);

            /* ── RED-on-dev checkpoint: before raw is purged, the routed call at dev's pre-fix shape would
               have gone raw-only regardless of age (no router existed) — this call proves nothing about dev,
               it establishes the successor now HOLDS the window's data before we take raw away. ── */
            var floors = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, await TimescaleSupport.DetectRollupsAsync(dataSource, ct), ct);
            Assert.Equal(WindowStart.AddHours(1), floors.FloorOf(TimescaleSupport.QueryStatsIntervalHourlyView));

            /* ── move raw's floor past the window: delete W's raw rows, as retention would. ── */
            await using (var purge = new NpgsqlCommand(
                "DELETE FROM collect.query_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection))
            {
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* ── call 2: raw no longer covers the window — must route to hourly. ── */
            var hourlyResult = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                dataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, cancellationToken: ct);
            Assert.Equal(RetentionTier.Hourly, hourlyResult.Tier);
            Assert.NotEmpty(hourlyResult.Rows);

            var hourlyTotalsByKey = RollUpByHash(hourlyResult.Rows);

            /* ── the equality assertion, over the (database_name, query_hash) totals for the population the
               hourly rollup CAN answer — seed (a)'s three groups. Not 0xZEROINTERVAL: that row is excluded
               from the hourly rollup by IntervalHonestSourceFilter, so raw and hourly deliberately DISAGREE
               there — asserted separately below as the real finding, never smoothed into this comparison. ── */
            foreach (var hash in new[] { "0xTOPQ1", "0xTOPQ2", "0xTOPQ3" })
            {
                Assert.True(rawTotalsByKey.TryGetValue(hash, out var rawTotal), $"{hash} missing from raw's totals");
                Assert.True(hourlyTotalsByKey.TryGetValue(hash, out var hourlyTotal), $"{hash} missing from the hourly-routed totals");
                Assert.Equal(rawTotal.CpuUs, hourlyTotal.CpuUs);
                Assert.Equal(rawTotal.Executions, hourlyTotal.Executions);
            }

            /* ── the finding (brief step 2): the zero-interval row counted in raw is ABSENT from the hourly
               rollup — a real precision loss at the hourly tier, not a test that was bent to hide it. Raw's
               0xZEROINTERVAL total is 900,000 CPU-us / 1 execution; the hourly-routed read has no row for it
               at all. Reported in the PR body's Deviations section; raw's own read is unchanged in this
               lane. ── */
            Assert.False(hourlyTotalsByKey.ContainsKey("0xZEROINTERVAL"),
                "the zero-interval seed row is excluded from the hourly rollup by IntervalHonestSourceFilter — raw and hourly disagree here BY DESIGN (#4231 3a finding, see PR body)");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    private static Dictionary<string, (long CpuUs, long Executions)> RollUpByHash(IEnumerable<DarlingDataReader.TopQueryRow> rows)
    {
        var totals = new Dictionary<string, (long CpuUs, long Executions)>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            totals.TryGetValue(row.QueryHash, out var running);
            totals[row.QueryHash] = (running.CpuUs + row.TotalCpuUs, running.Executions + row.TotalExecutions);
        }
        return totals;
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string queryHash, string hostObjectName,
        long cpuUs, long elapsedUs, long executions, int intervalSeconds)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     host_object_name, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(queryHash);
        insert.Parameters.AddWithValue("0x" + queryHash.TrimStart('0', 'x'));
        insert.Parameters.AddWithValue(hostObjectName);
        insert.Parameters.AddWithValue(cpuUs);
        insert.Parameters.AddWithValue(elapsedUs);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(intervalSeconds);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
