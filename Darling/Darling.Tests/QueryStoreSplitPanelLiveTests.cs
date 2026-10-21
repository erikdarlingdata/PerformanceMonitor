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

    private static async Task<(DateTime Start, DateTime End, PanelPlan Plan, RollupAvailability Rollups, RollupCoverage Coverage)> PrepareAsync(
        NpgsqlDataSource source, string panel, CancellationToken ct)
    {
        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
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
            var resolution = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct, plan);
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
            var resolution = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(source, scope, start, end, null, ct, plan);
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
}
