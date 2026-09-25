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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 (A6, lane LA-8b2): the live proof the daily-tier stitch (#4184) shipped without — one seeded
/// server, six real calendar days, on a real TimescaleDB store, read through every daily-tier caller
/// <see cref="RollupCoverage.StitchedRelationSql"/> now serves: FinOps' <c>DatabaseResourceUsageSqlFor</c>,
/// a Compose panel through <see cref="ComposeSourceRouter"/> and <see cref="ComposeCompiler"/>, and
/// <see cref="DailySummarySql.RangeSqlFor(RetentionTier, RollupCoverage, DateTime)"/>'s not-carried probe —
/// plus a smoke check that all six stitched pairs (three hourly, three daily) produce SQL that actually
/// executes. Trend/query-history reads are deliberately absent: <c>DurationTrendRouting.ResolveTier</c> can
/// only return <c>Raw</c> or <c>Hourly</c>, so no trend reader ever reaches a daily FROM clause.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables, so it cannot race the live
   collection, and serializing it would be pure slowdown. Leave it out; this comment is here so the next sweep
   does not "fix" it. */
public sealed class DailyStitchLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never in this range.</summary>
    private const int ServerId = -936553;
    private const string ServerName = "a6-la8b2-daily-stitch";
    private const string Db = "StitchDb";

    /// <summary>Fixed anchor date (never wall clock, per #3653 A6 ruling): a Monday, chosen only so the six
    /// calendar days it seeds are unambiguous. Every seeded timestamp below is already whole-second (#4155).</summary>
    private static readonly DateTime D0 = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task DailyTierReads_StitchAtF_d_AcrossFinOps_Compose_AndDailySummary_WithANotCarriedHole()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live A6 daily-stitch test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live A6 daily-stitch test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        /* #3653 A6 lane LB-6 / SuccessorDailyLiveTests: the worker's real start order converts collection_log
           to a hypertable BEFORE the ensure sweep; collection_health_hourly's CREATE selects FROM it, so
           skipping this on the rig-rule store rejects the ensure sweep with 0A000. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* Stop the scratch database's own scheduler before the ensure sweep creates refresh policies (#3986):
           this test deliberately leaves the successor daily unrefreshed for one day in the middle of its own
           refreshed span, and an auto-fired policy run (no initial_start) would refresh it out from under the
           seed before the assertions run. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Six whole UTC days, D0..D5. Each day gets three query_stats rows: two ordinary ones
               (QH1, QH2 — sample_interval_seconds = 3600, admitted by every successor's WHERE) and one
               "restart" row (RESTARTQ — sample_interval_seconds = 0, admitted only by the LEGACY, since every
               interval-honest successor's CREATE carries "sample_interval_seconds IS DISTINCT FROM 0"). RESTARTQ
               is the query_hash that appears ONLY in such rows, so every day's legacy total, successor total
               and unique-query count differ by exactly this one row. QH1's row lands on the hour (:00) so the
               successor hourly's materialized floor, once refreshed from D3, is exactly D3 00:00 — day-aligned,
               which keeps F_d pinned at D3 rather than sliding to D4 through StitchedRelationSql's ceilDay rule. */
            async Task PlantDayAsync(DateTime day)
            {
                async Task PlantAsync(DateTime at, string hash, long workerUs, long executions, int intervalSeconds)
                {
                    await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10)", connection);
                    insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
                    insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
                    insert.Parameters.AddWithValue(ServerId);
                    insert.Parameters.AddWithValue(ServerName);
                    insert.Parameters.AddWithValue(Db);
                    insert.Parameters.AddWithValue(hash);
                    insert.Parameters.AddWithValue("0x" + hash);
                    insert.Parameters.AddWithValue(workerUs);
                    insert.Parameters.AddWithValue(executions);
                    insert.Parameters.AddWithValue(intervalSeconds);
                    await insert.ExecuteNonQueryAsync(ct);
                }

                await PlantAsync(day, "A6DAILYQ1", 1000, 10, 3600);
                await PlantAsync(day.AddHours(1).AddMinutes(5), "A6DAILYQ2", 2000, 20, 3600);
                await PlantAsync(day.AddHours(2).AddMinutes(5), "A6DAILYRESTART", 7000, 1, 0);
            }

            for (var i = 0; i < 6; i++)
            {
                await PlantDayAsync(D0.AddDays(i));
            }

            var d6 = D0.AddDays(6);
            var d3 = D0.AddDays(3);
            var d4 = D0.AddDays(4);
            var d5 = D0.AddDays(5);

            /* Legacy (query-grain and db-grain): refreshed for all six days, both tiers — the frozen trio's
               steady state. */
            foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDbHourlyView })
            {
                await RefreshAsync(connection, view, D0, d6, ct);
            }
            foreach (var view in new[] { TimescaleSupport.QueryStatsDailyView, TimescaleSupport.QueryStatsDbDailyView })
            {
                await RefreshAsync(connection, view, D0, d6, ct);
            }

            /* Successor hourlies: refreshed from D3 onward for both families, so the query-grain successor's
               floor lands exactly on D3 00:00 (F_d's ceilDay operand). */
            foreach (var view in new[] { TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsDbIntervalHourlyView })
            {
                await RefreshAsync(connection, view, d3, d6, ct);
            }

            /* Successor dailies. The DB-grain pair (FinOps' own source) is refreshed cleanly across D3-D5, no
               hole, so the FinOps window total is an exact, gap-free sum. The QUERY-grain pair (Compose's and
               the daily summary's source) is refreshed for D3 and D5 but DELIBERATELY NOT D4: D4 is below the
               successor's last materialized day (D5), so its hole registers as "not carried at this tier" (a
               NULL unique_queries) rather than "not reached yet" (which reads from raw instead). */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDbIntervalDailyView, d3, d6, ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, d3, d4, ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalDailyView, d5, d6, ct);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            Assert.True(rollups.QueryGrainIntervalDaily);
            Assert.True(rollups.DbGrainIntervalDaily);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

            /* F_d, read back off the product's own boundary function: exactly D3, inside the [D0, D6) window. */
            var fd = coverage.StitchFloor(TimescaleSupport.QueryStatsDailyView, RollupCoverage.StitchTier.Daily, D0);
            Assert.Equal(d3, fd);

            /* ── FinOps: DatabaseResourceUsageSqlFor at the daily tier, over the whole window ──
               Window total = legacy (D0, D1, D2: 10,000 each) + successor (D3, D4, D5: 3,000 each, no hole on
               this pair) = 39,000 worker-time units / 1,000 = 39.0 ms; 93 + 90 = 183 executions. Neither
               all-legacy (60.0 ms / 186 executions) nor all-successor (18.0 ms / 180 executions). */
            var finOpsSql = ViewerDataService.DatabaseResourceUsageSqlFor(RetentionTier.Daily, coverage, D0);
            await using (var finOps = new NpgsqlCommand(finOpsSql, connection))
            {
                finOps.Parameters.AddWithValue(ServerId);
                finOps.Parameters.AddWithValue(D0);
                await using var reader = await finOps.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), "FinOps database-resource read returned no row for the seeded database");
                Assert.Equal(Db, reader.GetString(0));
                Assert.Equal(39.0, Convert.ToDouble(reader.GetValue(1)), 3);
                Assert.Equal(183L, Convert.ToInt64(reader.GetValue(5)));
                Assert.False(await reader.ReadAsync(ct), "only one database was seeded");
            }

            /* ── Compose: a daily-tier panel through ComposeSourceRouter + ComposeCompiler ──
               One point per day the stitched relation actually holds a row for: D0-D2 (legacy), D3 and D5
               (successor) — five points, D4 absent (the successor daily has no row there, and Compose has no
               not-carried concept; that hole is what the daily summary's own probe exists to describe). */
            var json = JsonNode.Parse(
                "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}")!;
            var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)json, Array.Empty<string>());
            Assert.True(parseError is null, parseError);

            var context = new ComposeRunContext(
                new[] { ServerName }, D0, d6, ComposeRunContext.NoVariables, rollups, D0.AddDays(400), coverage);

            var (compiled, compileError) = ComposeCompiler.Compile(plan!, context);
            Assert.True(compileError is null, compileError);
            Assert.NotNull(compiled);
            Assert.Contains("UNION ALL", compiled!.Sql, StringComparison.Ordinal);
            Assert.Contains($"FROM collect.{TimescaleSupport.QueryStatsDailyView}", compiled.Sql, StringComparison.Ordinal);
            Assert.Contains($"FROM collect.{TimescaleSupport.QueryStatsIntervalDailyView}", compiled.Sql, StringComparison.Ordinal);

            var totalsByDay = new Dictionary<DateTime, double>();
            await using (var composeCommand = new NpgsqlCommand(compiled.Sql, connection))
            {
                foreach (var p in compiled.Parameters)
                {
                    composeCommand.Parameters.Add(p);
                }

                await using var composeReader = await composeCommand.ExecuteReaderAsync(ct);
                while (await composeReader.ReadAsync(ct))
                {
                    totalsByDay[composeReader.GetDateTime(0)] = Convert.ToDouble(composeReader.GetValue(1));
                }
            }

            Assert.Equal(5, totalsByDay.Count);
            Assert.False(totalsByDay.ContainsKey(d4), "D4's successor daily was deliberately left unrefreshed");
            var scale = totalsByDay[D0];
            Assert.True(scale > 0, "D0 must have carried a nonzero total");
            Assert.Equal(scale, totalsByDay[D0.AddDays(1)], 3);
            Assert.Equal(scale, totalsByDay[D0.AddDays(2)], 3);
            Assert.Equal(0.3 * scale, totalsByDay[d3], 3);
            Assert.Equal(0.3 * scale, totalsByDay[d5], 3);

            /* ── Daily summary: RangeSqlFor(Daily, coverage, windowStart), the not-carried probe ──
               unique_queries: legacy's 3 (QH1, QH2, RESTARTQ) below F_d, successor's 2 (QH1, QH2 — RESTARTQ
               filtered out) at/after F_d, one row per day, no gap, no overlap; D4 reads NULL (not carried —
               the successor-side probe's source is the successor HOURLY, which IS refreshed for D4). */
            var dailySummarySql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, coverage, D0);
            var dailySummaryRows = await ReadDailySummaryAsync(connection, dailySummarySql, ServerId, D0, d6, ct);

            Assert.Equal(6, dailySummaryRows.Count);
            Assert.Equal(new[] { D0, D0.AddDays(1), D0.AddDays(2), d3, d4, d5 }, dailySummaryRows.Select(r => r.Day).OrderBy(d => d).ToArray());
            var byDay = dailySummaryRows.ToDictionary(r => r.Day, r => r.Unique);
            Assert.Equal(3L, byDay[D0]);
            Assert.Equal(3L, byDay[D0.AddDays(1)]);
            Assert.Equal(3L, byDay[D0.AddDays(2)]);
            Assert.Equal(2L, byDay[d3]);
            Assert.Null(byDay[d4]);
            Assert.Equal(2L, byDay[d5]);

            /* ── Red first (#3653 A6): re-probe the SAME store with RollupAvailability.WithoutIntervalDailies —
               the product's own probe, forced back to the pre-A6-daily shape. With no successor daily
               available, StitchFloor answers null and the whole window reads the legacy alone: D3 goes back to
               3 (not 2) and D4 — which the real coverage reads NULL — goes back to 3 as well (not carried
               never triggers over an unstitched legacy that actually holds every day). This is the assertion
               that would have failed before this PR wired the daily stitch up; it is kept green here, pinned
               against the OLD coverage rather than reverted product code, because the shape it protects (a
               store whose service predates the successor dailies) still has to degrade this exact way. */
            var legacyOnlyCoverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, RollupAvailability.WithoutIntervalDailies, ct);
            Assert.Null(legacyOnlyCoverage.StitchFloor(TimescaleSupport.QueryStatsDailyView, RollupCoverage.StitchTier.Daily, D0));
            var legacyOnlySql = DailySummarySql.RangeSqlFor(RetentionTier.Daily, legacyOnlyCoverage, D0);
            var legacyOnlyRows = await ReadDailySummaryAsync(connection, legacyOnlySql, ServerId, D0, d6, ct);
            var legacyOnlyByDay = legacyOnlyRows.ToDictionary(r => r.Day, r => r.Unique);
            Assert.Equal(6, legacyOnlyRows.Count);
            Assert.Equal(3L, legacyOnlyByDay[d3]);
            Assert.Equal(3L, legacyOnlyByDay[d4]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                /* Confirms nothing woke the scratch database's scheduler mid-test and refreshed the successor
                   daily's deliberate D4 hole out from under the seed (#3986); only asserted when the body
                   succeeded, per LiveStoreCleanup's own discipline (#1794) — asserting here on a failed body
                   would replace the real failure with this one. Nothing else needs restoring: the scratch
                   database is dropped whole, below. */
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>
    /// All six stitch pairs (#3653 A6 design v2 §2) — the three <see cref="TimescaleSupport.SupersededHourlyRollups"/>
    /// plus the three <see cref="TimescaleSupport.SupersededDailyRollups"/> — produce a
    /// <see cref="RollupCoverage.StitchedRelationSql"/> that actually executes against the relations the ensure
    /// sweep creates. A hand-built <see cref="RollupCoverage"/> (fixed floors, <see cref="RollupAvailability.All"/>)
    /// forces every pair into its genuine UNION ALL stitch form rather than a single-relation degrade, so this
    /// proves each pair's <c>s_stitchColumnsByLegacy</c> list is a real subset of both sides' columns — no seeded
    /// rows needed, since <c>LIMIT 0</c> never reads one.
    /// </summary>
    [Fact]
    public async Task AllSixStitchPairs_SelectStarLimit0_SucceedsOnAForcedStitch()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live six-pairs stitch smoke test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live six-pairs stitch smoke test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var legacyFloor = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var successorDailyFloor = legacyFloor.AddDays(3);
        var farWindowStart = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        var floors = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        foreach (var (legacy, successor, _) in TimescaleSupport.SupersededHourlyRollups)
        {
            floors[legacy] = legacyFloor;
            floors[successor] = legacyFloor;
        }
        foreach (var (legacyDaily, successorDaily, successorHourly) in TimescaleSupport.SupersededDailyRollups)
        {
            floors[legacyDaily] = legacyFloor;
            floors[successorDaily] = successorDailyFloor;
            floors[successorHourly] = legacyFloor;
        }

        var coverage = new RollupCoverage(floors, new Dictionary<string, DateTime>(StringComparer.Ordinal), RollupAvailability.All);

        foreach (var (legacy, _, _) in TimescaleSupport.SupersededHourlyRollups)
        {
            var sql = coverage.StitchedRelationSql(legacy, "a", farWindowStart, RollupCoverage.StitchTier.Hourly);
            Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
            await using var command = new NpgsqlCommand($"SELECT * FROM {sql} LIMIT 0", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.False(await reader.ReadAsync(ct));
        }

        foreach (var (legacyDaily, _, _) in TimescaleSupport.SupersededDailyRollups)
        {
            var sql = coverage.StitchedRelationSql(legacyDaily, "a", farWindowStart, RollupCoverage.StitchTier.Daily);
            Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
            await using var command = new NpgsqlCommand($"SELECT * FROM {sql} LIMIT 0", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.False(await reader.ReadAsync(ct));
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<(DateTime Day, long? Unique)>> ReadDailySummaryAsync(
        NpgsqlConnection connection, string sql, int serverId, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(sql, connection);
        read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
        var rows = new List<(DateTime Day, long? Unique)>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3)));
        }
        return rows;
    }
}
