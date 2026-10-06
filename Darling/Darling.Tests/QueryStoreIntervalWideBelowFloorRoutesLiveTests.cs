/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4689: exactness of the other two below-floor table routes. The Viewer grid's table SQL and one compiled Compose
/// <c>query_store_stats</c> panel each read the interval table at the resolved read start after both purges ran
/// (raw's chunks dropped, the table's expired intervals deleted) and must equal raw's answer over the same start
/// taken before either purge. The seed includes an interval whose last snapshot lands after a day.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreIntervalWideBelowFloorRoutesLiveTests
{
    private const int TestTop = 50;

    private const string PanelJson =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    private static DateTime ExpectedReadStart => QueryStoreIntervalWideBelowFloorLiveTests.S.AddHours(2).AddHours(26);

    [Fact]
    public async Task ViewerGrid_TableAtReadStart_EqualsRawTakenBeforeBothPurges()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct, lateSnapshot: true);
        var readStart = ExpectedReadStart;

        await GridSnapshotAsync(rig, "grid_raw", raw: true, readStart, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, QueryStoreIntervalWideBelowFloorLiveTests.S.AddDays(2), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.PurgeTableAsync(rig.Connection, QueryStoreIntervalWideBelowFloorLiveTests.S.AddMinutes(90), ct);

        var plan = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.True(plan.UseTable);
        Assert.Equal(readStart, plan.ReadStart);

        await GridSnapshotAsync(rig, "grid_table", raw: false, plan.ReadStart, ct);
        var exact = await QueryStoreIntervalWideBelowFloorLiveTests.DiffAsync(rig.Connection, "grid_raw", "grid_table", ct);
        Assert.True(exact.CountA > 0, "the seed produced no raw rows; the comparison would be vacuous");
        Assert.Equal(exact.CountA, exact.CountB);
        Assert.Equal(0, exact.AOnly);
        Assert.Equal(0, exact.BOnly);

        /* Bound at the window start instead: the purge deleted intervals raw returned. */
        await GridSnapshotAsync(rig, "grid_table_s", raw: false, QueryStoreIntervalWideBelowFloorLiveTests.S, ct);

        var unbound = await QueryStoreIntervalWideBelowFloorLiveTests.DiffAsync(rig.Connection, "grid_table", "grid_table_s", ct);
        Assert.True(unbound.AOnly + unbound.BOnly > 0, "a bind at the window start must not equal the read-start bind");
    }

    [Fact]
    public async Task ComposePanel_TableAtReadStart_EqualsRawTakenBeforeBothPurges()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct, lateSnapshot: true);
        var s = QueryStoreIntervalWideBelowFloorLiveTests.S;
        var readStart = ExpectedReadStart;

        var rollups = await TimescaleSupport.DetectRollupsAsync(rig.Postgres, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(rig.Postgres, rollups, ct);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(PanelJson)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        /* Raw's answer over [readStart, end] while raw still holds every chunk. */
        await ComposeSnapshotAsync(rig, "compose_raw", plan!,
            new ComposeRunContext(null, readStart, rig.End, ComposeRunContext.NoVariables, rollups, rig.End, coverage, false), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, s.AddDays(2), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.PurgeTableAsync(rig.Connection, s.AddMinutes(90), ct);

        var resolution = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(rig.Postgres, null, s, rig.End, rig.End, ct);
        Assert.True(resolution.Eligible);
        Assert.Equal(readStart, resolution.WideStart);

        await ComposeSnapshotAsync(rig, "compose_table", plan!,
            new ComposeRunContext(null, s, rig.End, ComposeRunContext.NoVariables, rollups, rig.End, coverage, resolution.Eligible, resolution.WideStart), ct);
        var exact = await QueryStoreIntervalWideBelowFloorLiveTests.DiffAsync(rig.Connection, "compose_raw", "compose_table", ct);
        Assert.True(exact.CountA > 0, "the seed produced no raw rows; the comparison would be vacuous");
        Assert.Equal(exact.CountA, exact.CountB);
        Assert.Equal(0, exact.AOnly);
        Assert.Equal(0, exact.BOnly);

        /* No exact start bound: the panel reads from the window start and differs. */
        await ComposeSnapshotAsync(rig, "compose_table_s", plan!,
            new ComposeRunContext(null, s, rig.End, ComposeRunContext.NoVariables, rollups, rig.End, coverage, true), ct);
        var unbound = await QueryStoreIntervalWideBelowFloorLiveTests.DiffAsync(rig.Connection, "compose_raw", "compose_table_s", ct);
        Assert.True(unbound.AOnly + unbound.BOnly > 0, "a bind at the window start must not equal the read-start bind");
    }

    private static async Task GridSnapshotAsync(
        QueryStoreIntervalWideBelowFloorLiveTests.Rig rig, string name, bool raw, DateTime start, CancellationToken ct)
    {
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection, "DROP TABLE IF EXISTS " + name, null, ct);
        var sql = raw ? ViewerDataService.QueryStoreTopSql : ViewerDataService.QueryStoreTopTableSql;
        await using var command = new NpgsqlCommand("CREATE TEMP TABLE " + name + " AS " + sql, rig.Connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = QueryStoreIntervalWideBelowFloorLiveTests.ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        if (raw)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(rig.End, DateTimeKind.Unspecified) });
        }
        else
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(rig.End, DateTimeKind.Unspecified) });
        }

        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TestTop });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = PerformanceMonitor.Darling.Storage.TopFill.FirstCandidates(TestTop) });  /* #5313: the round's candidate limit, bound last */
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ComposeSnapshotAsync(
        QueryStoreIntervalWideBelowFloorLiveTests.Rig rig, string name, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection, "DROP TABLE IF EXISTS " + name, null, ct);
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        await using var command = new NpgsqlCommand("CREATE TEMP TABLE " + name + " AS " + compiled!.Sql, rig.Connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
