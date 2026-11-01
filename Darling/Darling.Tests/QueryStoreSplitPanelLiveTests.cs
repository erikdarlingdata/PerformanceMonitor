/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6115: a fleet Query Store panel reads the interval table for the servers that pass the gate and raw
/// <c>collect.query_store_stats</c> only for the ones that fail, instead of sending the whole panel to raw when one server fails.
/// Four servers hold the same Query Store history in raw (two snapshots per interval, so the raw dedupe has work to do). Servers 1 to 3 also
/// have it on the interval table with a coverage row old enough to pass the gate. Server 4 has raw rows and a coverage row that began an
/// hour ago, so the gate refuses it. Server 5 has no Query Store rows and no coverage row.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The tests reach DARLING_TEST_PG only to CREATE and DROP their own
   database through ScratchPostgres, then work entirely inside it. */
public sealed class QueryStoreSplitPanelLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #6115 split-panel live tests.";

    private static readonly string[] s_panels =
    {
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"server\"],\"viz\":\"table\"}",
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"viz\":\"stat\"}",
        "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"timeBucket\":\"hour\",\"viz\":\"line\"}",
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_max_duration_us\",\"aggregate\":\"max\",\"topN\":5,\"groupBy\":[\"query_hash\"],\"timeBucket\":\"hour\",\"viz\":\"line\"}",
        "{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"topN\":10,\"groupBy\":[\"database_name\"],\"viz\":\"table\"}",
    };

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection,
            "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (1, 'srv1', TRUE), (2, 'srv2', TRUE), (3, 'srv3', TRUE), (4, 'srv4', TRUE), (5, 'srv5', TRUE)", ct);

        /* Raw: 20 hours, 3 queries a server, two snapshots of each interval (the :10 one is the older, smaller). */
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc,
 runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
 execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us, max_cpu_time_us)
SELECT row_number() OVER (ORDER BY s, h, q, snap), t.ct, s, 'srv' || s, 'db' || (q % 2), 'mod' || q, 'hash' || (q + s), q, q, 'Regular',
       s * 100000 + h * 10 + q, t.hr, t.hr, t.ct,
       CASE snap WHEN 1 THEN 5 ELSE 9 + q END, 100 * q + s, 50 * q, 10, 1000 * q + s * 7 + h, 500 * q
FROM generate_series(1, 4) AS s
CROSS JOIN generate_series(1, 20) AS h
CROSS JOIN generate_series(1, 3) AS q
CROSS JOIN generate_series(1, 2) AS snap
CROSS JOIN LATERAL (SELECT date_trunc('hour', now() AT TIME ZONE 'UTC') - h * interval '1 hour' AS hr) AS base
CROSS JOIN LATERAL (SELECT base.hr + CASE snap WHEN 1 THEN interval '10 minutes' ELSE interval '40 minutes' END AS ct, base.hr) AS t", ct);

        /* The interval table for servers 1 to 3 holds each interval's latest snapshot, which is what the raw dedupe keeps. One old row each
           puts the table's floor ten days back, which the gate's purge-edge clause needs. */
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_hash,
 execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_hash,
       execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc
FROM collect.query_store_stats WHERE server_id <= 3 AND collection_time::time >= TIME '00:30' AND date_part('minute', collection_time) = 40;

INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_hash,
 execution_count, avg_duration_us, avg_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT (now() AT TIME ZONE 'UTC') - interval '10 days', s, 'db0', 900, 900, 'Regular', (now() AT TIME ZONE 'UTC') - interval '10 days', (now() AT TIME ZONE 'UTC') - interval '10 days',
       'modOld', 'hashOld', 1, 1, 1, 9000000 + s, (now() AT TIME ZONE 'UTC') - interval '10 days'
FROM generate_series(1, 3) AS s;

INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through)
VALUES (1, (now() AT TIME ZONE 'UTC') - interval '30 hours', now() AT TIME ZONE 'UTC'),
       (2, (now() AT TIME ZONE 'UTC') - interval '30 hours', now() AT TIME ZONE 'UTC'),
       (3, (now() AT TIME ZONE 'UTC') - interval '30 hours', now() AT TIME ZONE 'UTC'),
       (4, (now() AT TIME ZONE 'UTC') - interval '1 hour', now() AT TIME ZONE 'UTC')", ct);
    }

    private static async Task<List<string>> RunRowsAsync(NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, compileError) = ComposeCompiler.Compile(plan, context);
        Assert.True(compileError is null, compileError);
        await using var command = new NpgsqlCommand(compiled!.Sql, connection) { CommandTimeout = 120 };
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /// <summary><paramref name="at"/>: the window's end. Without it the end is the clock now, which moves between two reads; a test that compares
    /// one read with a later one passes the same value to both (#7081: a 24-hour window that starts a 5-minute stamp later drops that stamp's rows).</summary>
    private static async Task<(DateTime Start, DateTime End, PanelPlan Plan, RollupAvailability Rollups, RollupCoverage Coverage)> PrepareAsync(
        NpgsqlDataSource source, string panel, CancellationToken ct, DateTime? at = null)
    {
        var end = at ?? DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var rollups = await TimescaleSupport.DetectRollupsAsync(source, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(source, rollups, ct);
        return (end.AddHours(-24), end, plan!, rollups, coverage);
    }

    [Fact]
    public async Task OneRefusedServer_ReadsRawForItself_AndTheIntervalTableForTheRest_AndEqualsTheAllRawRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

        foreach (var panel in s_panels)
        {
            var (start, end, plan, rollups, coverage) = await PrepareAsync(source, panel, ct);
            var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct, plan);
            Assert.True(resolution.Eligible, "servers 1 to 3 pass, so the panel must read the interval table for them: " + panel);
            Assert.Equal(new[] { 4 }, resolution.RawServerIds);

            var split = new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, rollups, end, coverage, true, resolution.WideStart,
                QueryStoreGroupMembers: resolution.GroupMembers, QueryStoreStampThrough: resolution.StampThrough, QueryStoreRawServerIds: resolution.RawServerIds);
            var allRaw = split with { QueryStoreWideEligible = false, QueryStoreRawServerIds = null, QueryStoreWideStart = null, QueryStoreStampThrough = null };

            var expected = await RunRowsAsync(connection, plan, allRaw, ct);
            var got = await RunRowsAsync(connection, plan, split, ct);
            Assert.NotEmpty(expected);
            Assert.Equal(expected, got);

            /* The statement text says which source each server reads from. */
            var (compiled, _) = ComposeCompiler.Compile(plan, split);
            Assert.Contains("collect.query_store_interval_wide", compiled!.Sql, StringComparison.Ordinal);
            Assert.Contains("collect.query_store_stats", compiled.Sql, StringComparison.Ordinal);
            Assert.Contains("<> ALL(", compiled.Sql, StringComparison.Ordinal);
            Assert.Contains("server_id = ANY(", compiled.Sql, StringComparison.Ordinal);
        }

        /* Through the real runner: it answers, and its statement reads both sources. */
        var body = new JsonObject { ["panel"] = JsonNode.Parse(s_panels[0]), ["hours"] = 24 };
        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(source, body, ct);
        Assert.True(outcome.Payload is not null, "the split panel must run: " + outcome.Error);
        var sql = outcome.Payload!["sql"]!.GetValue<string>();
        Assert.Contains("query_store_interval_wide", sql, StringComparison.Ordinal);
        Assert.Contains("qs_rn", sql, StringComparison.Ordinal);
        var servers = outcome.Payload["rows"]!.AsArray().Select(r => r!["server"]!.GetValue<string>()).OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "srv1", "srv2", "srv3", "srv4" }, servers);
    }

    [Fact]
    public async Task EveryServerPassing_IsTodaysWideText_EveryServerFailing_IsTodaysRawText_AndAServerWithNoRowsSendsNothingToRaw()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var panel = s_panels[0];
        var (start, end, plan, rollups, coverage) = await PrepareAsync(source, panel, ct);

        async Task<(string Sql, IReadOnlyList<int>? Raw, bool Eligible)> RunAsync(params string[] scope)
        {
            var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, scope, start, end, null, ct, plan);
            var context = new ComposeRunContext(scope, start, end, ComposeRunContext.NoVariables, rollups, end, coverage, resolution.Eligible, resolution.WideStart,
                QueryStoreGroupMembers: resolution.GroupMembers, QueryStoreStampThrough: resolution.StampThrough, QueryStoreRawServerIds: resolution.RawServerIds);
            var (compiled, error) = ComposeCompiler.Compile(plan, context);
            Assert.True(error is null, error);
            return (compiled!.Sql, resolution.RawServerIds, resolution.Eligible);
        }

        /* Every server passes: the all-wide text, with no raw half and no split parameter. */
        var allPass = await RunAsync("srv1", "srv2", "srv3");
        Assert.True(allPass.Eligible);
        Assert.Null(allPass.Raw);
        Assert.DoesNotContain("qs_rn", allPass.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("<> ALL(", allPass.Sql, StringComparison.Ordinal);

        /* No server passes: today's all-raw text. */
        var nonePass = await RunAsync("srv4");
        Assert.False(nonePass.Eligible);
        Assert.Null(nonePass.Raw);
        Assert.DoesNotContain("query_store_interval_wide", nonePass.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ALL(", nonePass.Sql, StringComparison.Ordinal);

        /* Server 5 has no Query Store rows and no coverage row: with the passing servers the panel stays all-wide, and beside the refused
           one only the refused one reads raw. Before #6115 it refused the gate and sent every panel it was in to raw. */
        var withEmpty = await RunAsync("srv1", "srv2", "srv3", "srv5");
        Assert.True(withEmpty.Eligible);
        Assert.Null(withEmpty.Raw);
        Assert.DoesNotContain("qs_rn", withEmpty.Sql, StringComparison.Ordinal);

        var emptyAndRefused = await RunAsync("srv1", "srv5", "srv4");
        Assert.True(emptyAndRefused.Eligible);
        Assert.Equal(new[] { 4 }, emptyAndRefused.Raw);

        /* The same server WITH rows in the window and no coverage row cannot use the interval table, so it reads raw like any refused server. */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count) "
            + "VALUES (999999, (now() AT TIME ZONE 'UTC') - interval '2 hours', 5, 'srv5', 'db0', 1, 1, 'Regular', 1, (now() AT TIME ZONE 'UTC') - interval '3 hours', (now() AT TIME ZONE 'UTC') - interval '3 hours', 3)", ct);
        var withRows = await RunAsync("srv1", "srv5");
        Assert.True(withRows.Eligible);
        Assert.Equal(new[] { 5 }, withRows.Raw);
    }

    private const string RawColumns =
        "collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, "
        + "runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, "
        + "max_duration_us, max_cpu_time_us";

    /// <summary>One raw Query Store row an hour-ish before <paramref name="clock"/>, outside every seeded interval id, carrying <paramref name="executions"/>:
    /// a row a split panel reads only if it reads that server raw. Its times come from the test's one clock value, not from the store's now() (#7081).</summary>
    private static Task PoisonAsync(NpgsqlConnection connection, int serverId, long executions, DateTime clock, CancellationToken ct) =>
        ExecAsync(connection,
            $"INSERT INTO collect.query_store_stats ({RawColumns}) VALUES ({900_000_000 + serverId}, TIMESTAMP '{ComposeStampLiveSupport.At(clock.AddMinutes(-30))}', {serverId}, 'srv{serverId}', 'db0', NULL, NULL, 1, 1, 'Regular', "
            + $"{900_000_000 + serverId}, TIMESTAMP '{ComposeStampLiveSupport.At(clock.AddHours(-1))}', TIMESTAMP '{ComposeStampLiveSupport.At(clock.AddHours(-1))}', TIMESTAMP '{ComposeStampLiveSupport.At(clock.AddMinutes(-30))}', {executions}, 1, 1, 1, 1)", ct);

    /// <summary>#6115: the stamp-rollup route (V173) with a refused server. Servers 1 to 3 have 28 built rollup hours and pass the gate; server 4
    /// has raw rows only and a coverage row an hour old, so the gate refuses it. The statement has four arms (rollup, stale, tail, and the raw half).</summary>
    [Fact]
    public async Task TheStampRollupRoute_WithARefusedServer_EqualsTheAllRawRead_AndItsRawArmReadsOnlyTheRefusedServer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        /* #7081: the one clock value of the body. Every window end, the refused server's coverage claim and the poison rows' times come from it.
           A window end read from the clock at each read moved by a 5-minute stamp between two reads, and the split total of server 1 fell by that
           stamp's rows (6 million executions) after a row the split does not read had been added. */
        var clock = ComposeStampLiveSupport.ClockWithin(hourNow);
        var bodySucceeded = false;
        try
        {
            /* Servers 1 to 3 hold the same history raw as on the wide table (so the all-raw read is the oracle), and server 4 holds a copy of
               server 1's, raw only. The old wide row per server puts the table's floor ten days back, which the gate's purge-edge clause needs. */
            await ExecAsync(connection, $@"
INSERT INTO collect.query_store_stats ({RawColumns})
SELECT row_number() OVER (ORDER BY w.server_id, w.collection_time, w.runtime_stats_interval_id), w.collection_time, w.server_id, 'srv' || w.server_id, w.database_name, w.module_name,
       w.query_hash, w.query_id, w.plan_id, w.execution_type_desc, w.runtime_stats_interval_id, w.interval_start_time_utc, w.first_execution_time, w.last_execution_time,
       w.execution_count, w.avg_duration_us, w.avg_cpu_time_us, w.max_duration_us, w.max_cpu_time_us
FROM collect.query_store_interval_wide AS w;

INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (4, 'srv4', TRUE);
INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through)
VALUES (4, TIMESTAMP '{ComposeStampLiveSupport.At(clock.AddHours(-1))}', TIMESTAMP '{ComposeStampLiveSupport.At(clock)}');
INSERT INTO collect.query_store_stats ({RawColumns})
SELECT 1000000 + row_number() OVER (ORDER BY r.collection_time, r.runtime_stats_interval_id), r.collection_time, 4, 'srv4', r.database_name, r.module_name,
       r.query_hash, r.query_id, r.plan_id, r.execution_type_desc, 5000000 + r.runtime_stats_interval_id, r.interval_start_time_utc, r.first_execution_time, r.last_execution_time,
       r.execution_count, r.avg_duration_us, r.avg_cpu_time_us, r.max_duration_us, r.max_cpu_time_us
FROM collect.query_store_stats AS r WHERE r.server_id = 1", ct);
            foreach (var server in new[] { 1, 2, 3 })
            {
                await ComposeStampLiveSupport.InsertRowAsync(connection, server, hourNow.AddDays(-10), 8_000_000 + server, ct);
            }

            async Task<(List<string> Split, List<string> AllRaw, string Sql)> RunBothAsync(string panel)
            {
                var (start, end, plan, rollups, coverage) = await PrepareAsync(source, panel, ct, clock);
                var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct, plan);
                Assert.True(resolution.Eligible, "servers 1 to 3 pass, so the panel must read the wide route: " + panel);
                Assert.Equal(new[] { 4 }, resolution.RawServerIds);
                Assert.Equal(hourNow.AddHours(-2), resolution.StampThrough);

                var split = new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, rollups, end, coverage, true, resolution.WideStart,
                    QueryStoreGroupMembers: resolution.GroupMembers, QueryStoreStampThrough: resolution.StampThrough, QueryStoreRawServerIds: resolution.RawServerIds);
                var allRaw = split with { QueryStoreWideEligible = false, QueryStoreRawServerIds = null, QueryStoreWideStart = null, QueryStoreStampThrough = null };
                var (compiled, error) = ComposeCompiler.Compile(plan, split);
                Assert.True(error is null, error);
                return (await RunRowsAsync(connection, plan, split, ct), await RunRowsAsync(connection, plan, allRaw, ct), compiled!.Sql);
            }

            var panels = new[] { s_panels[0], "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}" };
            foreach (var panel in panels)
            {
                var (got, expected, sql) = await RunBothAsync(panel);

                /* The rollup is read, the raw half is a fourth arm, and its predicate names the refused server alone. */
                Assert.Contains("query_store_compose_stamp", sql, StringComparison.Ordinal);
                Assert.Contains("qs_rn", sql, StringComparison.Ordinal);
                Assert.Contains("server_id = ANY(", sql, StringComparison.Ordinal);
                Assert.Contains("<> ALL(", sql, StringComparison.Ordinal);
                Assert.NotEmpty(expected);
                Assert.Equal(expected, got);
            }

            /* The raw arm reads the refused server's raw rows: a new raw row for server 4 shows in the split result and in the all-raw one, alike. */
            await PoisonAsync(connection, 4, 1_000, clock, ct);
            var afterRefused = await RunBothAsync(panels[0]);
            Assert.Equal(afterRefused.AllRaw, afterRefused.Split);

            /* ... and only that server's: a new raw row for a passing server is read by the all-raw route and by no part of the split. */
            await PoisonAsync(connection, 1, 5_000_000, clock, ct);
            var afterPassing = await RunBothAsync(panels[0]);
            Assert.Equal(afterRefused.Split, afterPassing.Split);
            Assert.NotEqual(afterPassing.AllRaw, afterPassing.Split);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    private static Task LogRowsAsync(NpgsqlConnection connection, long logId, int serverId, string ago, long rows, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
            + $"VALUES ({6_115_000_000L + logId}, {serverId}, 'srv{serverId}', 'query_store', (now() AT TIME ZONE 'UTC') - interval '{ago}', 20, 'SUCCESS', {rows})", ct);

    private static async Task<ComposeRunOutcome> RunPanelAsync(NpgsqlDataSource source, CancellationToken ct) =>
        await DarlingWebEndpoints.RunComposedPanelAsync(source, new JsonObject { ["panel"] = JsonNode.Parse(s_panels[0]), ["hours"] = 24 }, ct);

    /// <summary>#6115: the raw half of the read guard (<see cref="QueryStoreWideReadGuard.Estimate.RawRows"/>). Server 4 is read raw; its collection
    /// log says how many rows that is. The same panel is let through under the limit and refused over it, with the raw half's servers counted.</summary>
    [Fact]
    public async Task TheRawHalfOfTheReadGuard_RefusesASplitPanelOverTheLimit_AndLetsOneUnderIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* No log row: the raw half adds nothing, so the panel runs (the guard fails open for a server with no data). */
        Assert.True((await RunPanelAsync(source, ct)).Payload is not null, "no collection log rows: the panel must run");

        /* Under the limit: a thousand rows collected an hour ago. */
        await LogRowsAsync(connection, 1, 4, "1 hour", 1_000, ct);
        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var under = await QueryStoreWideReadGuard.EstimateSplitAsync(source, null, end, null, new[] { 4 }, end.AddHours(-24), StorageCommandDeadlines.McpReadSeconds, ct);
        Assert.InRange(under!.Value.RawRows, 900L, 1_000L);
        Assert.Equal(1, under.Value.RawServers);
        /* The wide half leaves the raw server out: its log rows are not counted a second time as a wide day. */
        Assert.Equal(0L, under.Value.DailyRows);
        var ranUnder = await RunPanelAsync(source, ct);
        Assert.True(ranUnder.Payload is not null, "a raw half under the limit must run: " + ranUnder.Error);
        /* #6115 review (F2): the payload itself says how many servers were read raw, whether or not the wide start moved. */
        Assert.Equal(
            "1 server whose interval table could not serve the window was read from the raw tables, from the window's own start (#6115).",
            ranUnder.Payload!["query_store_raw_servers_note"]!.GetValue<string>());

        /* Over the limit: ten million more rows. No server has a built day, so the wide half counts nothing and the raw half alone refuses. */
        await LogRowsAsync(connection, 2, 4, "2 hours", ComposeLimits.MaxQueryStoreWideRows + 3_000_000, ct);
        var over = await RunPanelAsync(source, ct);
        Assert.True(over.Payload is null, "a raw half over the limit must be refused; the panel ran");
        Assert.False(over.IsServerError);
        Assert.StartsWith("This panel needs about ", over.Error, StringComparison.Ordinal);
        Assert.Contains("Query Store rows (1 server, ", over.Error, StringComparison.Ordinal);
        Assert.EndsWith("Choose fewer servers or a shorter window.", over.Error, StringComparison.Ordinal);

        /* With a built day on a wide server the sum is the two halves, and the count is both. */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) "
            + "VALUES (1, DATE '2026-01-01', 2, now() AT TIME ZONE 'UTC', 1000)", ct);
        var both = await RunPanelAsync(source, ct);
        Assert.True(both.Payload is null);
        Assert.Contains("Query Store rows (2 servers, ", both.Error, StringComparison.Ordinal);
    }

    /// <summary>#6115 review (F1): a server with no built day is counted from the collection log rows INSIDE the panel's window, not its newest day
    /// times the window's days. Before the fix every recently disabled server (the daily builder's GC deletes its built rows) added a full day for every
    /// day of the window, so a mature store's fleet panel near the limit was refused where it ran before.</summary>
    [Fact]
    public async Task AServerWithNoBuiltDay_IsCountedForTheLogRowsInsideTheWindowOnly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var start = end.AddDays(-7);

        /* Server 1 has no built day and collected only before the window: it adds 0 (it is the newest day of a server that stopped). */
        await LogRowsAsync(connection, 1, 1, "10 days", 1_000_000, ct);
        var before = await QueryStoreWideReadGuard.EstimateAsync(source, null, null, StorageCommandDeadlines.McpReadSeconds, ct, null, start, end);
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(0, 0), before);
        Assert.Equal(0L, before!.Value.RowsOver(start, end));

        /* Server 2 has no built day and collected 5,000 rows inside the window (and 1,000 before it): the estimate adds the 5,000. */
        await LogRowsAsync(connection, 2, 2, "2 days", 3_000, ct);
        await LogRowsAsync(connection, 3, 2, "5 days", 2_000, ct);
        await LogRowsAsync(connection, 4, 2, "9 days", 1_000, ct);
        var inside = await QueryStoreWideReadGuard.EstimateAsync(source, null, null, StorageCommandDeadlines.McpReadSeconds, ct, null, start, end);
        Assert.Equal(1, inside!.Value.Servers);
        Assert.InRange(inside.Value.RowsOver(start, end), 5_000L, 5_010L);

        /* A mature store: every enabled server has a built day, one disabled server has only old log rows (the GC removed its built rows).
           The estimate is what origin/dev gave: the built days alone. */
        await ExecAsync(connection, "DELETE FROM collect.collection_log", ct);
        await ExecAsync(connection, "UPDATE collect.servers SET is_enabled = FALSE WHERE server_id = 5", ct);
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) "
            + "VALUES (1, DATE '2026-01-01', 2, now() AT TIME ZONE 'UTC', 100), (2, DATE '2026-01-01', 2, now() AT TIME ZONE 'UTC', 200), "
            + "(3, DATE '2026-01-01', 2, now() AT TIME ZONE 'UTC', 300), (4, DATE '2026-01-01', 2, now() AT TIME ZONE 'UTC', 400)", ct);
        await LogRowsAsync(connection, 5, 5, "20 days", 9_000_000, ct);
        await LogRowsAsync(connection, 6, 5, "21 days", 9_000_000, ct);
        var mature = await QueryStoreWideReadGuard.EstimateAsync(source, null, null, StorageCommandDeadlines.McpReadSeconds, ct, null, start, end);
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(1_000, 4), mature);
    }

    /// <summary>#6115: a server with no built day in <c>collect.query_store_top_daily_built</c> (the most recently enrolled copies) is counted from
    /// the collection log, not as 0. On the load-test store 85 of 211 servers had none, the estimate at 210 servers was about the one at 126, and
    /// a panel the guard let through timed out at 60 s.</summary>
    [Fact]
    public async Task AWideServerWithNoBuiltDay_IsCountedFromTheCollectionLog_AndAPanelItPushesOverTheLimitIsRefused()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Server 1 passes the gate, has no built day, and collected nine million rows in the day before its newest log row. */
        await LogRowsAsync(connection, 1, 1, "1 hour", 3_000_000, ct);
        await LogRowsAsync(connection, 2, 1, "2 hours", 3_000_000, ct);
        await LogRowsAsync(connection, 3, 1, "3 hours", 3_000_000, ct);
        /* Server 4 is read raw: its log rows belong to the raw half and never to the wide estimate. */
        await LogRowsAsync(connection, 4, 4, "1 hour", 4_000_000, ct);

        var fleet = await QueryStoreWideReadGuard.EstimateAsync(source, null, null, StorageCommandDeadlines.McpReadSeconds, ct);
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(13_000_000, 2), fleet);
        var withoutRaw = await QueryStoreWideReadGuard.EstimateAsync(source, null, null, StorageCommandDeadlines.McpReadSeconds, ct, new[] { 4 });
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(9_000_000, 1), withoutRaw);
        /* A server with neither a built day nor a log row still adds nothing: the guard fails open. */
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(0, 0), await QueryStoreWideReadGuard.EstimateAsync(source, new[] { "srv2" }, null, StorageCommandDeadlines.McpReadSeconds, ct));

        var refused = await RunPanelAsync(source, ct);
        Assert.True(refused.Payload is null, "nine million collected rows for one wide server must refuse the panel; it ran");
        Assert.False(refused.IsServerError);
        Assert.StartsWith("This panel needs about ", refused.Error, StringComparison.Ordinal);
        Assert.Contains("Query Store rows (", refused.Error, StringComparison.Ordinal);

        /* A built day, once the builder has one, is the better figure and replaces the log's: the same server is then small, and the panel runs. */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) "
            + "VALUES (1, DATE '2026-01-01', 2, now() AT TIME ZONE 'UTC', 1000)", ct);
        Assert.Equal(new QueryStoreWideReadGuard.Estimate(1_000, 1), await QueryStoreWideReadGuard.EstimateAsync(source, null, null, StorageCommandDeadlines.McpReadSeconds, ct, new[] { 4 }));
        var ran = await RunPanelAsync(source, ct);
        Assert.True(ran.Payload is not null, "with a small built day the panel must run: " + ran.Error);
    }
}
