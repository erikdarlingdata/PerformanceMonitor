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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605 part 2, LA-8: the live exactness proof for the Query Store wide-table Compose route. Seeds two
/// servers and three databases through the real collector write path (<see cref="DarlingCollectorRunner.WriteBackfillBatchAsync"/>,
/// the same call <see cref="QueryStoreIntervalWideGridLiveTests.SeedGridAsync"/> uses), then compiles and runs
/// the SAME panel twice through the real <see cref="ComposeCompiler"/> — once with the context forced to raw
/// (<c>QueryStoreWideEligible: false</c>) and once with it forced to the wide table (<c>true</c>) — and asserts
/// the rows are IDENTICAL, values compared exactly (no tolerance). "Forced" rather than routed through
/// <see cref="DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync"/> because that method decides eligibility
/// from wall-clock coverage state a historical seed cannot honestly satisfy; this test proves the COMPILED SQL
/// on each side reads the same answer, which is the thing eligibility gates access to. The wiring that decides
/// which side a real run takes is covered separately by the compile-level pins in
/// <c>DarlingComposeTests.Compile_QueryStoreWideEligible_*</c> and by reading
/// <see cref="DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync"/>'s call to
/// <see cref="QueryStoreIntervalWide.ReadsTableAsync"/> against the same server-scope contract this test's
/// eligibility case below (a pending batch) exercises directly.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it, so it cannot race
   live collection. */
public sealed class ComposeQueryStoreWideExactnessLiveTests
{
    private const int ServerId1 = -46051;
    private const int ServerId2 = -46052;
    private const string ServerName1 = "qswide-exact-1";
    private const string ServerName2 = "qswide-exact-2";

    private static readonly DateTime WindowStart = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddDays(3);

    [Fact]
    public async Task RawAndWide_ReturnIdenticalRows_AcrossEveryCase_AndTheRedMutationFails()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 Query Store wide-table exactness live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId1, ServerName1, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId2, ServerName2, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await SeedAsync(runner, ServerId1, ServerName1, WindowStart, ct);
        await SeedAsync(runner, ServerId2, ServerName2, WindowStart, ct);

        var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, ct);

        /* ── Case 1: Ranked top-N by query_hash, SUM(qs_executions) ── */
        await AssertIdenticalAsync(connection, rollups, coverage, ct,
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}",
            "case 1: Ranked top-N by query_hash, SUM(qs_executions)");

        /* ── Case 2: minute-grain TimeSeries of qs_executions ── */
        await AssertIdenticalAsync(connection, rollups, coverage, ct,
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"minute\",\"viz\":\"line\"}",
            "case 2: minute-grain TimeSeries of qs_executions");

        /* ── Case 3: hour-grain with module_name LIKE ── */
        await AssertIdenticalAsync(connection, rollups, coverage, ct,
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\","
            + "\"filters\":[{\"dimension\":\"module_name\",\"op\":\"like\",\"value\":\"usp_%\"}]}",
            "case 3: hour-grain with module_name LIKE");

        /* ── Case 4: the qs_max_duration_us measure (MAX) ── */
        await AssertIdenticalAsync(connection, rollups, coverage, ct,
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_max_duration_us\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"line\"}",
            "case 4: qs_max_duration_us (MAX)");

        /* ── Case 5: a Scalar ── */
        await AssertIdenticalAsync(connection, rollups, coverage, ct,
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"viz\":\"stat\"}",
            "case 5: Scalar SUM(qs_executions)");

        /* ── the RED: mutate the wide relation by dropping the Aborted/Exception rows for query_hash 2's
           identity (the design's own suggested mutation: "drop the execution_type_desc distinction"), so the
           wide table's SUM for that identity under-counts what raw still has. Snapshot the removed rows first
           so the mutation can be reverted exactly, rather than re-seeding. */
        await ExecAsync(connection,
            "CREATE TEMP TABLE tmp_removed_wide_rows AS SELECT * FROM collect.query_store_interval_wide WHERE execution_type_desc <> 'Regular'", ct);
        await ExecAsync(connection,
            "DELETE FROM collect.query_store_interval_wide WHERE execution_type_desc <> 'Regular'", ct);

        var mismatch = await CompareAsync(connection, rollups, coverage,
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}", ct);
        Assert.False(mismatch.Identical, "expected dropping the Aborted/Exception rows from the wide table to break exactness (a real difference, not a false green)");

        /* revert */
        await ExecAsync(connection, "INSERT INTO collect.query_store_interval_wide SELECT * FROM tmp_removed_wide_rows", ct);

        var restored = await CompareAsync(connection, rollups, coverage,
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}", ct);
        Assert.True(restored.Identical, "expected exactness restored once the mutation was reverted");
    }

    /* ---- the eligibility cases: pending batch and literal-end-before-applied_through both fall back to raw ---- */

    [Fact]
    public async Task Eligibility_PendingBatch_And_LiteralEndBeforeAppliedThrough_BothRouteRaw()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 Query Store wide-table eligibility live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId1, ServerName1, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        await SeedAsync(runner, ServerId1, ServerName1, WindowStart, ct);

        await ForceFilledSinceAsync(connection, ServerId1, WindowStart.AddDays(-1), ct);
        var appliedThrough = await ScalarDateTimeAsync(connection, ServerId1, ct);

        /* passing case first */
        var (passing, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId1, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.True(passing, "expected the gate to pick the table for the seeded, forced-covered window");

        /* a pending batch must route raw */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES ($1, $2, 'qswide-exact-1-a', $2)",
            ServerId1, WindowStart, ct);
        var (pendingRoutes, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId1, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.False(pendingRoutes, "a pending batch must read raw regardless of coverage");
        await ExecAsync(connection, "DELETE FROM collect.query_store_interval_wide_pending WHERE server_id = $1", ServerId1, ct);

        /* a literal end before applied_through must route raw */
        var (literalEndRoutes, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId1, WindowStart, WindowEnd, appliedThrough.AddMinutes(-1), QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.False(literalEndRoutes, "a literal end before applied_through must read raw");
    }

    /* ---- the seed: two databases per server, hour-spanning intervals, mixed outcomes, a LIKE-able module ---- */

    private static async Task SeedAsync(DarlingCollectorRunner runner, int serverId, string serverName, DateTime windowStart, CancellationToken ct)
    {
        var context = new CollectorContext { ServerId = serverId, ServerName = serverName, CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = serverName, Host = serverName },
            ConnectionString = "Server=" + serverName,
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = serverName,
            ServerId = serverId,
            EngineEdition = 3,
        };

        async Task WriteAsync(DateTime collectionTime, params QueryStoreCollector.Row[] rows)
        {
            foreach (var batch in rows.GroupBy(r => r.DatabaseName))
            {
                await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, batch.ToList(), server, collectionTime, context, ct);
            }
        }

        QueryStoreCollector.Row Row(
            string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
            long cpuUs, long durationUs, string? module = null, string? role = null, string type = "Regular") => new()
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
                AvgDurationUs = durationUs,
                MaxDurationUs = durationUs * 2,
                MaxCpuTimeUs = cpuUs * 2,
                ModuleName = module,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                ReplicaRole = role,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = first,
            };

        /* An anchor interval 45 days before windowStart (mirrors QueryStoreIntervalWideGridLiveTests'
           SeedGridAsync): pushes this server's table floor comfortably past clause 3's IntervalSpanMargin
           check, so windowStart itself can sit exactly on the seeded span without the gate refusing on table
           floor alone. Outside the compared window, so it never appears in either side's rows. */
        var anchor = windowStart.AddDays(-45);
        await WriteAsync(anchor.AddMinutes(10), Row("qsA", 900, 9001, 9000, anchor, anchor.AddMinutes(5), 1, 50, 100));

        /* an interval spanning an hour boundary: fetched twice, once before and once after the hour tick,
           with growing execution_count both times */
        var spanStart = windowStart.AddHours(1).AddMinutes(50);
        await WriteAsync(spanStart.AddMinutes(5),
            Row("qsA", 1, 11, 100, spanStart, spanStart.AddMinutes(4), 3, 500, 1000, module: "usp_GetOrders"));
        await WriteAsync(spanStart.AddMinutes(20),
            Row("qsA", 1, 11, 100, spanStart, spanStart.AddMinutes(19), 9, 520, 1040, module: "usp_GetOrders"));

        /* Aborted/Exception rows beside a Regular row for the same identity shape (different query ids, same
           hour) — distinct RuntimeStatsIntervalId per outcome, as Query Store itself gives each outcome its
           own interval row. */
        var day0 = windowStart.AddHours(3);
        await WriteAsync(day0.AddMinutes(10),
            Row("qsA", 2, 21, 200, day0, day0.AddMinutes(9), 4, 700, 1400, module: "usp_GetOrders"),
            Row("qsA", 2, 21, 201, day0, day0.AddMinutes(9), 2, 700, 1400, type: "Aborted", module: "usp_GetOrders"),
            Row("qsA", 2, 21, 202, day0, day0.AddMinutes(9), 1, 700, 1400, type: "Exception", module: "usp_GetOrders"));

        /* a second database, a module that does NOT match the LIKE filter, a replica role */
        await WriteAsync(day0.AddMinutes(15),
            Row("qsB", 3, 31, 300, day0.AddMinutes(1), day0.AddMinutes(10), 6, 300, 600, module: "adhoc_report", role: "secondary1"));

        /* re-fetched open interval across three collections, growing execution_count (the running-max shape) */
        var day1 = windowStart.AddDays(1).AddHours(2);
        await WriteAsync(day1.AddMinutes(10), Row("qsB", 4, 41, 400, day1, day1.AddMinutes(9), 5, 200, 400, module: "usp_Reprice"));
        await WriteAsync(day1.AddMinutes(25), Row("qsB", 4, 41, 400, day1, day1.AddMinutes(24), 12, 220, 440, module: "usp_Reprice"));
        await WriteAsync(day1.AddMinutes(40), Row("qsB", 4, 41, 400, day1, day1.AddMinutes(39), 20, 250, 500, module: "usp_Reprice"));

        /* a plain no-module row on a third database */
        var day2 = windowStart.AddDays(2).AddHours(4);
        await WriteAsync(day2.AddMinutes(5), Row("qsC", 5, 51, 500, day2, day2.AddMinutes(4), 7, 150, 300));
    }

    /* ---- compile the same panel twice (raw forced, wide forced) and compare rows exactly ---- */

    private static async Task AssertIdenticalAsync(
        NpgsqlConnection connection, RollupAvailability rollups, RollupCoverage coverage, CancellationToken ct, string planJson, string caseName)
    {
        var (identical, rawCount) = await CompareAsync(connection, rollups, coverage, planJson, ct);
        Assert.True(rawCount > 0, $"{caseName}: the seed produced no raw rows; the comparison would be vacuous");
        Assert.True(identical, $"{caseName}: raw and wide-table rows differ");
    }

    private static async Task<(bool Identical, long RawCount)> CompareAsync(
        NpgsqlConnection connection, RollupAvailability rollups, RollupCoverage coverage, string planJson, CancellationToken ct)
    {
        var json = JsonNode.Parse(planJson)!;
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)json, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        var rawContext = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, rollups, WindowEnd, coverage, QueryStoreWideEligible: false);
        var wideContext = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, rollups, WindowEnd, coverage, QueryStoreWideEligible: true);

        var rawRows = await RunAsync(connection, plan!, rawContext, ct);
        var wideRows = await RunAsync(connection, plan!, wideContext, ct);

        var rawSet = rawRows.Select(RowKey).OrderBy(k => k, StringComparer.Ordinal).ToList();
        var wideSet = wideRows.Select(RowKey).OrderBy(k => k, StringComparer.Ordinal).ToList();

        return (rawSet.SequenceEqual(wideSet), rawRows.Count);
    }

    private static string RowKey(object?[] row) =>
        string.Join("|", row.Select(v => v is null ? "<null>" : Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)));

    private static async Task<List<object?[]>> RunAsync(NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, compileError) = ComposeCompiler.Compile(plan, context);
        Assert.True(compileError is null, compileError);
        Assert.NotNull(compiled);

        var rows = new List<object?[]>();
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new object?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(values);
        }

        return rows;
    }

    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, int serverId, DateTime value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = $1 WHERE server_id = $2", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime> ScalarDateTimeAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(serverId);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, int serverId, DateTime cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(serverId);
        await command.ExecuteNonQueryAsync(ct);
    }
}
