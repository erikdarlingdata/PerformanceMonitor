/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7746: a newly enrolled server's 24-hour fleet Query Store panel reads the interval table, and keeps the fleet rollup (V239) on
/// for the whole panel, once the table's floor over every server shows the purge has dropped nothing of this server's. Servers 1 to 3
/// hold 20 hours of raw history and the same intervals on the interval table, plus one ten-day-old interval each, so the table's floor
/// over every server sits ten days back. Server 4 holds the same 20 hours on both and nothing older: its own floor is 20 hours back,
/// inside the window start minus the 26-hour purge-edge margin, which is the shape every server has for about two days after enrollment.
/// The third test takes the old intervals away, so the whole table is young, and gives server 4 a raw snapshot inside the window whose
/// interval is below every table floor: the shape of an interval the table dropped while raw still holds a snapshot of it.
/// </summary>
/* #1776 own-store: each test creates its own database through ScratchPostgres, then works entirely inside it. */
public sealed class QueryStoreYoungServerWideFloorLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #7746 young-server live tests.";

    private const string FleetPanel =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Four servers with the same 20 hours of raw history (two snapshots an interval) and the same intervals on the interval table.
    /// Servers 1 to 3 also hold one interval ten days old. Every coverage row began 30 hours ago, before the 24-hour window starts.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection,
            "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (1, 'srv1', TRUE), (2, 'srv2', TRUE), (3, 'srv3', TRUE), (4, 'srv4', TRUE)", ct);

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

        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_hash,
 execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_hash,
       execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc
FROM collect.query_store_stats WHERE date_part('minute', collection_time) = 40;

INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_hash,
 execution_count, avg_duration_us, avg_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT (now() AT TIME ZONE 'UTC') - interval '10 days', s, 'db0', 900, 900, 'Regular', (now() AT TIME ZONE 'UTC') - interval '10 days', (now() AT TIME ZONE 'UTC') - interval '10 days',
       'modOld', 'hashOld', 1, 1, 1, 9000000 + s, (now() AT TIME ZONE 'UTC') - interval '10 days'
FROM generate_series(1, 3) AS s;

INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through)
SELECT s, (now() AT TIME ZONE 'UTC') - interval '30 hours', now() AT TIME ZONE 'UTC'
FROM generate_series(1, 4) AS s", ct);
    }

    private static async Task<(NpgsqlConnection Connection, NpgsqlDataSource Source, DateTime Start, DateTime End, PanelPlan Plan, RollupAvailability Rollups, RollupCoverage Coverage)> OpenSeededAsync(
        ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);
        var source = NpgsqlDataSource.Create(scratch.ConnectionString);

        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(FleetPanel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var rollups = await TimescaleSupport.DetectRollupsAsync(source, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(source, rollups, ct);
        return (connection, source, end.AddHours(-24), end, plan!, rollups, coverage);
    }

    [Fact]
    public async Task ANewlyEnrolledServer_ReadsTheIntervalTable_AndItsOwnPanelHasNoRawArm()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        var (connection, source, start, end, plan, rollups, coverage) = await OpenSeededAsync(scratch, ct);
        await using var connectionScope = connection;
        await using var sourceScope = source;

        /* The gate itself, for the young server alone: its own floor is 20 hours back, inside the margin, and the table's floor over
           every server is ten days back, so the purge has dropped nothing of this server's and the table is read. */
        var decision = await QueryStoreIntervalWide.ResolveReadAsync(
            connection, 4, start, end, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, QueryStoreIntervalWide.LegacyRows.Allow, ct);
        Assert.True(decision.UseTable, "a server enrolled 30 hours ago must read the interval table for a 24-hour window");
        Assert.False(decision.DecisionFailed);

        /* The same server's own 24-hour panel: eligible, no server on the raw route, and the statement reads no raw row. */
        var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, new[] { "srv4" }, start, end, null, ct, plan);
        Assert.True(resolution.Eligible, "the young server's own panel must read the interval table");
        Assert.Null(resolution.RawServerIds);

        var context = new ComposeRunContext(new[] { "srv4" }, start, end, ComposeRunContext.NoVariables, rollups, end, coverage, true, resolution.WideStart,
            QueryStoreGroupMembers: resolution.GroupMembers, QueryStoreStampThrough: resolution.StampThrough, QueryStoreRawServerIds: resolution.RawServerIds);
        var (compiled, compileError) = ComposeCompiler.Compile(plan, context);
        Assert.True(compileError is null, compileError);
        Assert.Contains("collect.query_store_interval_wide", compiled!.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("qs_rn", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("<> ALL(", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OneYoungServerAmongOld_SendsNoServerToRaw_AndTheFleetTableStaysOnForThePanel()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        var (connection, source, start, end, plan, rollups, coverage) = await OpenSeededAsync(scratch, ct);
        await using var connectionScope = connection;
        await using var sourceScope = source;

        var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, null, start, end, null, ct, plan);
        Assert.True(resolution.Eligible);
        Assert.Null(resolution.RawServerIds);
        Assert.True(resolution.FleetReady, "the migrated store is at or past the fleet rollup's rung");

        /* The plan shape. The fleet rollup's arm is taken when no server is on the raw route: with the resolution's answer the compile reads
           the fleet table and no raw row; with one server on the raw route, the answer the gate gave this fleet before, it reads neither. The
           stamp bound is pinned at the window end so the compile is the stamp-mode text, whose arm 1 is the one that can read the fleet table. */
        var allWide = new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, rollups, end, coverage, true, resolution.WideStart,
            QueryStoreGroupMembers: resolution.GroupMembers, QueryStoreStampThrough: end, QueryStoreRawServerIds: resolution.RawServerIds,
            QueryStoreFleetReady: resolution.FleetReady);
        Assert.True(ComposeCompiler.ReadsFleetRollup(plan, allWide));
        var (fleet, fleetError) = ComposeCompiler.Compile(plan, allWide);
        Assert.True(fleetError is null, fleetError);
        Assert.Contains("query_store_compose_fleet", fleet!.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("qs_rn", fleet.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("<> ALL(", fleet.Sql, StringComparison.Ordinal);

        var oneRaw = allWide with { QueryStoreRawServerIds = new[] { 4 } };
        Assert.False(ComposeCompiler.ReadsFleetRollup(plan, oneRaw));
        var (split, splitError) = ComposeCompiler.Compile(plan, oneRaw);
        Assert.True(splitError is null, splitError);
        Assert.DoesNotContain("query_store_compose_fleet", split!.Sql, StringComparison.Ordinal);
        Assert.Contains("collect.query_store_stats", split.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerWhoseTableDroppedAnIntervalRawStillHolds_KeepsTheRawArm()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        var (connection, source, start, end, plan, rollups, coverage) = await OpenSeededAsync(scratch, ct);
        await using var connectionScope = connection;
        await using var sourceScope = source;

        /* No interval older than 20 hours is left anywhere on the table, so its floor over every server is inside the margin too. Raw holds,
           for server 4, a snapshot collected two hours ago of an interval that began 40 hours ago: below every table floor, so the table
           cannot hold it, while the raw read of the window counts it. */
        await ExecAsync(connection, "DELETE FROM collect.query_store_interval_wide WHERE query_id = 900", ct);
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, "
            + "runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us) "
            + $"VALUES (900000004, TIMESTAMP '{ComposeStampLiveSupport.At(end.AddHours(-2))}', 4, 'srv4', 'db0', 'modDropped', 'hashDropped', 901, 901, 'Regular', 900000004, "
            + $"TIMESTAMP '{ComposeStampLiveSupport.At(end.AddHours(-40))}', TIMESTAMP '{ComposeStampLiveSupport.At(end.AddHours(-40))}', TIMESTAMP '{ComposeStampLiveSupport.At(end.AddHours(-2))}', 7, 1, 1, 1, 1)", ct);

        var decision = await QueryStoreIntervalWide.ResolveReadAsync(
            connection, 4, start, end, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, QueryStoreIntervalWide.LegacyRows.Allow, ct);
        Assert.False(decision.UseTable, "no floor shows the purge dropped nothing of this server's, so the server reads raw");
        Assert.False(decision.DecisionFailed);

        var resolution = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(source, new[] { "srv4" }, start, end, null, ct, plan);
        Assert.False(resolution.Eligible, "the only server in scope reads raw, so the panel is the raw statement");

        var raw = new ComposeRunContext(new[] { "srv4" }, start, end, ComposeRunContext.NoVariables, rollups, end, coverage, resolution.Eligible, resolution.WideStart,
            QueryStoreGroupMembers: resolution.GroupMembers, QueryStoreStampThrough: resolution.StampThrough, QueryStoreRawServerIds: resolution.RawServerIds);
        var (compiled, compileError) = ComposeCompiler.Compile(plan, raw);
        Assert.True(compileError is null, compileError);
        Assert.Contains("collect.query_store_stats", compiled!.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_interval_wide", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_compose_fleet", compiled.Sql, StringComparison.Ordinal);

        /* The raw statement counts the dropped interval's executions; a table read would have lost them. */
        await using var command = new NpgsqlCommand(compiled.Sql, connection) { CommandTimeout = 120 };
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        var rows = new System.Collections.Generic.List<string>();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
            }
        }

        Assert.Contains(rows, r => r.Contains("hashDropped", StringComparison.Ordinal));
    }
}
