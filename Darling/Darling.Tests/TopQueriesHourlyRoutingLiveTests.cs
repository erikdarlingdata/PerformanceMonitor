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

            /* ── seed (b'): #4394's own case — a planted zero-interval row with a nonzero delta, for a
               group that ALSO has ordinary, nonzero-interval rows (0xTOPQ1, already seeded above). The
               collector doesn't write this shape (a zero interval comes with zero deltas — see #2235
               CollectorDeltaCalculator), but planting it is what makes the filter observable: before #4394,
               TopQueriesSql summed this row's delta_worker_time into raw's total for 0xTOPQ1, while the
               hourly successor's CREATE already excludes it via IntervalHonestSourceFilter — the two tiers
               ranked the same window differently. #4394 adds the same filter to raw's read, so raw and
               hourly now agree on 0xTOPQ1's total. RED on dev: raw's 0xTOPQ1 total exceeds hourly's by
               exactly this row's 700,000 CPU-us / 1 execution. ── */
            await PlantAsync(connection, ct, WindowStart.AddHours(1).AddMinutes(40), "0xTOPQ1", "usp_HostA", 700_000L, 650_000L, 1L, 0);

            /* ── seed (b): one raw row for a query_hash that appears ONLY as a zero-interval first-collection
               row. The hourly successor's CREATE bakes in IntervalHonestSourceFilter
               (sample_interval_seconds IS DISTINCT FROM 0), and #4394 now excludes it from raw's read too, so
               a hash whose ONLY rows are zero-interval is absent from BOTH tiers. ── */
            await PlantAsync(connection, ct, WindowStart.AddHours(4), "0xZEROINTERVAL", "usp_HostA", 900_000L, 900_000L, 1L, 0);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* ── call 1: raw still covers the window (nothing purged yet) — must report tier_used = raw. ── */
            var rawResult = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                dataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, cancellationToken: ct);
            Assert.Equal(RetentionTier.Raw, rawResult.Tier);

            var rawTotalsByKey = RollUpByHash(rawResult.Rows);
            Assert.True(rawTotalsByKey.ContainsKey("0xTOPQ1"), "seed (a) group 1 must be present in the raw read");
            /* #4394: 0xZEROINTERVAL's only row is zero-interval, so raw's own read now excludes it too —
               the fix, not a regression; see the finding assertion below for the equivalent hourly check. */
            Assert.False(rawTotalsByKey.ContainsKey("0xZEROINTERVAL"),
                "a hash whose only rows are zero-interval is excluded from raw's own read by #4394's fix");

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

            /* ── call 2: raw no longer covers the window — must route to hourly. A FRESH data source, not
               the one call 1 used: #4231 3a finding — ComposeStoreAvailability caches coverage per
               NpgsqlDataSource instance for ReprobeInterval (5 minutes), unconditionally, including the null
               hourly floor call 1's probe measured before RefreshAsync ran above. Reusing dataSource here
               replays that stale null floor and the router's ReachesFurtherBack fallback lands back on Raw —
               not a router bug, a cache-freshness gap in the composer that a real 5-minute-old probe would
               also hit on a production store the instant a backfill finishes. See PR body Deviations. ── */
            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var hourlyResult = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                hourlyDataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, cancellationToken: ct);
            Assert.Equal(RetentionTier.Hourly, hourlyResult.Tier);
            Assert.NotEmpty(hourlyResult.Rows);

            var hourlyTotalsByKey = RollUpByHash(hourlyResult.Rows);

            /* ── the equality assertion, over the (database_name, query_hash) totals for the population BOTH
               tiers can answer — seed (a)'s three groups, now INCLUDING 0xTOPQ1 which also carries seed (b')'s
               zero-interval row (#4394's pin): before the fix raw's 0xTOPQ1 total exceeded hourly's by
               that row's 700,000 CPU-us / 1 execution; after the fix both tiers exclude it and agree. ── */
            foreach (var hash in new[] { "0xTOPQ1", "0xTOPQ2", "0xTOPQ3" })
            {
                Assert.True(rawTotalsByKey.TryGetValue(hash, out var rawTotal), $"{hash} missing from raw's totals");
                Assert.True(hourlyTotalsByKey.TryGetValue(hash, out var hourlyTotal), $"{hash} missing from the hourly-routed totals");
                Assert.Equal(rawTotal.CpuUs, hourlyTotal.CpuUs);
                Assert.Equal(rawTotal.Executions, hourlyTotal.Executions);
            }

            /* ── 0xZEROINTERVAL's only row is zero-interval, so #4394 excludes it from BOTH tiers — it is
               absent from the hourly rollup exactly as it was before this fix (that half was never broken),
               and now also absent from raw's own read (checked above), which is this fix. ── */
            Assert.False(hourlyTotalsByKey.ContainsKey("0xZEROINTERVAL"),
                "the zero-interval seed row is excluded from the hourly rollup by IntervalHonestSourceFilter");

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

    /// <summary>With <c>min_dop</c> or <c>group_by=host_object</c> on a window the age alone would send to the rollup, the read stays on raw and reports the window floor raw actually holds; the filter is never dropped.</summary>
    [Fact]
    public async Task GetTopQueriesByCpu_AgedWindow_WithMinDopOrRollUp_ReadsRaw_AndDisclosesTheFloor()
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
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "0xTOPQ1", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2);
            await PlantAsync(connection, ct, WindowStart.AddHours(2), "0xTOPQ2", "usp_HostA", 200_000L, 180_000L, 5L, 3600, maxDop: 2);
            var survivor = WindowStart.AddHours(5);
            await PlantAsync(connection, ct, survivor, "0xTOPQ3", "usp_HostA", 300_000L, 250_000L, 7L, 3600, maxDop: 2);

            var hoursBack = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
            var asOf = windowEnd.ToString("o");

            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await using (var purge = new NpgsqlCommand(
                "DELETE FROM collect.query_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection))
            {
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(WindowStart.AddHours(3));
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* A fresh data source: coverage is cached per data source, so it must be read after the refresh. */
            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            foreach (var call in new Func<Task<string>>[]
            {
                () => DarlingMcpDataTools.GetTopQueriesByCpu(hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, min_dop: 2, as_of: asOf),
                () => DarlingMcpDataTools.GetTopQueriesByCpu(hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, group_by: "host_object", as_of: asOf),
            })
            {
                using var doc = System.Text.Json.JsonDocument.Parse(await call());
                var root = doc.RootElement;
                Assert.Equal("raw", root.GetProperty("tier_used").GetString());
                Assert.True(root.GetProperty("window_truncated").GetBoolean());
                Assert.Equal(DarlingMcpTestData.TruncateToSeconds(survivor).ToString("o"), root.GetProperty("effective_start").GetString());
                Assert.Contains("stayed on raw", root.GetProperty("precision_note").GetString());
            }

            using (var dopDoc = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, min_dop: 2, as_of: asOf)))
            {
                Assert.NotEqual(System.Text.Json.JsonValueKind.Null, dopDoc.RootElement.GetProperty("filter_applied").ValueKind);
            }

            /* An empty forced-raw page says what part of the window it covered. */
            using var emptyDoc = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, min_dop: 5, as_of: asOf));
            Assert.Contains("still holds (from", emptyDoc.RootElement.GetProperty("message").GetString() ?? emptyDoc.RootElement.ToString());
            Assert.True(emptyDoc.RootElement.ToString().Contains("window_truncated", StringComparison.Ordinal));

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


    /// <summary>On the hourly tier the columns the rollup does not carry are JSON null (not 0, false or empty), <c>sql_handle</c> is returned, and a hash whose raw text is gone has a null <c>query_text</c> with a <c>text_note</c>.</summary>
    [Fact]
    public async Task HourlyRouted_MissingColumnsAreNull_AndSqlHandleIsCarried()
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
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "0xTOPQ1", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2);
            var hoursBack = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
            var asOf = windowEnd.ToString("o");

            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await using (var purge = new NpgsqlCommand(
                "DELETE FROM collect.query_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection))
            {
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var doc = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.Equal("hourly", doc.RootElement.GetProperty("tier_used").GetString());
            var row = doc.RootElement.GetProperty("queries")[0];
            foreach (var column in new[]
            {
                "total_logical_reads", "total_logical_writes", "total_physical_reads", "total_rows", "total_spills", "avg_reads",
                "min_dop", "max_dop", "is_parallel", "query_plan_hash", "plan_handle", "min_cpu_ms", "max_cpu_ms",
                "min_elapsed_ms", "max_elapsed_ms", "distinct_texts", "query_text",
            })
            {
                Assert.Equal(System.Text.Json.JsonValueKind.Null, row.GetProperty(column).ValueKind);
            }

            Assert.Equal("0xTOPQ1", row.GetProperty("sql_handle").GetString());
            Assert.Equal(System.Text.Json.JsonValueKind.String, row.GetProperty("text_note").ValueKind);
            Assert.Contains("may be a WAITFOR shell", row.GetProperty("text_note").GetString(), StringComparison.Ordinal);

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


    /// <summary>Coverage on the hourly tier is checked per server: a server whose first bucket is later than the window start reports <c>window_truncated</c> with the first bucket as <c>effective_start</c>; a server covering the window does not.</summary>
    [Fact]
    public async Task HourlyRouted_YoungServer_ReportsWindowTruncated()
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
            const int youngServerId = ServerId + 1;
            await DarlingMcpTestData.RegisterServerAsync(connection, youngServerId, ServerName + "-young", ct);
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "0xTOPQ1", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2);
            await PlantAsync(connection, ct, WindowStart.AddHours(10), "0xTOPQ2", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2, serverId: youngServerId, serverName: ServerName + "-young");
            var hoursBack = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
            var asOf = windowEnd.ToString("o");

            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await using (var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time >= $1 AND collection_time < $2", connection))
            {
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var young = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName + "-young", hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.Equal("hourly", young.RootElement.GetProperty("tier_used").GetString());
            Assert.True(young.RootElement.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(WindowStart.AddHours(10).ToString("o"), young.RootElement.GetProperty("effective_start").GetString());
            Assert.Contains("hourly rollup", young.RootElement.GetProperty("truncation_note").GetString());

            using var covered = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.False(covered.RootElement.GetProperty("window_truncated").GetBoolean());

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


    [Fact]
    public async Task HourlyRouted_StitchedWindow_CoverageProbeSplitsAtTheFloor()
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
            /* A stitched window: the legacy hourly holds the whole window, the successor only from +8h. Server A
               has a legacy bucket at +1h AND a successor-era bucket at +10h, so its first bucket (the least of the
               two probe halves) is the legacy one. Server B has rows only in the successor's era (+12h), so
               its first bucket is the successor half's. */
            const int successorOnlyServerId = ServerId + 1;
            await DarlingMcpTestData.RegisterServerAsync(connection, successorOnlyServerId, ServerName + "-successor", ct);
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "0xTOPQ1", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2);
            await PlantAsync(connection, ct, WindowStart.AddHours(10), "0xTOPQ2", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2);
            await PlantAsync(connection, ct, WindowStart.AddHours(12), "0xTOPQ3", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2, serverId: successorOnlyServerId, serverName: ServerName + "-successor");
            /* Server D: a zero-interval row at +11h (the legacy rollup buckets it, the successor's filter drops it)
               and an ordinary row at +13h. Its first bucket at or after the stitch floor must come from the
               successor (+13h): a legacy probe without the `< F` bound would answer +11h. */
            const int legacyOnlyServerId = ServerId + 2;
            await DarlingMcpTestData.RegisterServerAsync(connection, legacyOnlyServerId, ServerName + "-legacyonly", ct);
            await PlantAsync(connection, ct, WindowStart.AddHours(11), "0xTOPQ4", "usp_HostA", 500_000L, 400_000L, 10L, 0, maxDop: 2, serverId: legacyOnlyServerId, serverName: ServerName + "-legacyonly");
            await PlantAsync(connection, ct, WindowStart.AddHours(13), "0xTOPQ4", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2, serverId: legacyOnlyServerId, serverName: ServerName + "-legacyonly");
            var hoursBack = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
            var asOf = windowEnd.ToString("o");

            await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart.AddHours(8), windowEnd.AddHours(1), ct);
            await using (var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time >= $1 AND collection_time < $2", connection))
            {
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollups = await TimescaleSupport.DetectRollupsAsync(hourlyDataSource, ct);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(hourlyDataSource, rollups, ct);
            Assert.NotNull(coverage.StitchFloor(TimescaleSupport.QueryStatsHourlyView, RollupCoverage.StitchTier.Hourly, WindowStart));

            using var both = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.Equal("hourly", both.RootElement.GetProperty("tier_used").GetString());
            Assert.Equal(WindowStart.AddHours(1).ToString("o"), both.RootElement.GetProperty("effective_start").GetString());

            using var successorOnly = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName + "-successor", hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.Equal("hourly", successorOnly.RootElement.GetProperty("tier_used").GetString());
            Assert.Equal(WindowStart.AddHours(12).ToString("o"), successorOnly.RootElement.GetProperty("effective_start").GetString());

            using var legacyOnly = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName + "-legacyonly", hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.Equal(WindowStart.AddHours(13).ToString("o"), legacyOnly.RootElement.GetProperty("effective_start").GetString());

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


    [Fact]
    public async Task HourlyRouted_EndBeyondTheMaterializationCeiling_SaysNothingAfterItWasRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly routing test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: a scratch database, materializing continuous aggregates. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly routing test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var windowEnd = WindowStart.AddDays(1);
        var hoursBack = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
        var asOf = windowEnd.ToString("o");
        var bodySucceeded = false;
        try
        {
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "0xTOPQ1", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 2);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(-3), ct);
            await using (var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time >= $1 AND collection_time < $2", connection))
            {
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }
            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var doc = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.Equal("hourly", doc.RootElement.GetProperty("tier_used").GetString());
            var note = doc.RootElement.GetProperty("precision_note").GetString()!;
            Assert.Contains("the hourly rollup is materialized only to " + WindowStart.AddHours(2).ToString("o") + "; nothing after it was read", note, StringComparison.Ordinal);
            Assert.DoesNotContain("included whole", note, StringComparison.Ordinal);
            /* An end cut is not a start cut: the window flag stays about the start. */
            Assert.False(doc.RootElement.GetProperty("window_truncated").GetBoolean());

            /* The cpu attribution reads samples over the served span (first bucket to the ceiling), not the requested
               day: samples sit only inside +1h..+2h, 50% busy on 4 cores = 0.5 * 4 * 3600 s. */
            await using (var props = new NpgsqlCommand(@"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, is_hadr_enabled, is_clustered)
VALUES ($1,$2,$3,$4,'Enterprise Edition (64-bit)','15.0.4322.2','RTM',3,4,16,65536,2,8,false,false)", connection))
            {
                props.Parameters.AddWithValue(CollectionIdGenerator.Next());
                props.Parameters.AddWithValue(WindowStart.AddHours(2));
                props.Parameters.AddWithValue(ServerId);
                props.Parameters.AddWithValue(ServerName);
                await props.ExecuteNonQueryAsync(ct);
            }

            for (var minute = 0; minute <= 60; minute += 10)
            {
                await using var cpu = new NpgsqlCommand(
                    "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $2, 50, 0)", connection);
                cpu.Parameters.AddWithValue(CollectionIdGenerator.Next());
                cpu.Parameters.AddWithValue(WindowStart.AddHours(1).AddMinutes(minute));
                cpu.Parameters.AddWithValue(ServerId);
                cpu.Parameters.AddWithValue(ServerName);
                await cpu.ExecuteNonQueryAsync(ct);
            }

            /* A bucket materializes AFTER the coverage snapshot measured its ceiling (the snapshot is cached): the
               view now holds a bucket at +5h, and neither the ranked read nor the first-bucket probes may see it. */
            await PlantAsync(connection, ct, WindowStart.AddHours(5), "0xTOPQ2", "usp_HostB", 900_000L, 800_000L, 20L, 3600, maxDop: 2);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart.AddHours(5), WindowStart.AddHours(6), ct);
            await using (var purge2 = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time >= $1 AND collection_time < $2", connection))
            {
                purge2.Parameters.AddWithValue(WindowStart);
                purge2.Parameters.AddWithValue(windowEnd);
                await purge2.ExecuteNonQueryAsync(ct);
            }

            using var ceilingDoc = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, as_of: asOf));
            Assert.True(ceilingDoc.RootElement.TryGetProperty("queries", out _), ceilingDoc.RootElement.ToString());
            var ceilingRows = ceilingDoc.RootElement.GetProperty("queries").EnumerateArray().ToList();
            Assert.Single(ceilingRows);
            Assert.Equal("0xTOPQ1", ceilingRows[0].GetProperty("query_hash").GetString());
            Assert.Contains("nothing after it was read", ceilingDoc.RootElement.GetProperty("precision_note").GetString()!, StringComparison.Ordinal);
            Assert.Equal(7200.0, ceilingDoc.RootElement.GetProperty("cpu_attribution").GetProperty("sql_cpu_seconds_in_window").GetDouble());

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

    [Fact]
    public async Task ForcedRaw_OverAWindowRawNoLongerHolds_SaysNothingWasRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly routing test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: a scratch database, materializing continuous aggregates. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly routing test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var windowEnd = WindowStart.AddDays(1);
        var hoursBack = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
        var asOf = windowEnd.ToString("o");
        var bodySucceeded = false;
        try
        {
            await PlantAsync(connection, ct, WindowStart.AddHours(1), "0xTOPQ1", "usp_HostA", 500_000L, 400_000L, 10L, 3600, maxDop: 4);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await using (var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time >= $1 AND collection_time < $2", connection))
            {
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }
            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var doc = System.Text.Json.JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                hourlyDataSource, ServerName, hours_back: hoursBack, top: 10, parallel_only: true, as_of: asOf));
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            Assert.Contains("raw query_stats holds nothing in this window", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.True(doc.RootElement.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
            Assert.Equal(System.Text.Json.JsonValueKind.Null, doc.RootElement.GetProperty("hints").GetProperty("effective_start").ValueKind);

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
        long cpuUs, long elapsedUs, long executions, int intervalSeconds, int maxDop = 0, int serverId = ServerId, string? serverName = null)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     host_object_name, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds,
     min_dop, max_dop)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName ?? ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(queryHash);
        insert.Parameters.AddWithValue("0x" + queryHash.TrimStart('0', 'x'));
        insert.Parameters.AddWithValue(hostObjectName);
        insert.Parameters.AddWithValue(cpuUs);
        insert.Parameters.AddWithValue(elapsedUs);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(intervalSeconds);
        insert.Parameters.AddWithValue((long)maxDop);
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
