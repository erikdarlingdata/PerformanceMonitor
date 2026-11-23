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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #9357: a fleet top-procedures cell over 24 hours, end to end on a throwaway store. The rows are written by the collector's own write
/// path (<c>WriteBatchAsync</c>), so the hour ledger is the one the writer keeps, and the hourly rollup is refreshed over them. The cell
/// then runs through <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/> (the runner, with its count guard):
/// <list type="bullet">
/// <item>With ledger and rollup in agreement the payload SQL reads <c>procedure_stats_interval_hourly AS mid</c> (the hourly plus raw
/// edges route was taken) and the ranked rows equal, as JSON, the rows of the same panel compiled for the raw route.</item>
/// <item>With one hour's ledger count made to disagree (higher, lower, or missing) the same cell reads raw, and its rows are the raw rows.</item>
/// </list>
/// The class runs on one fixed anchor and never reads the wall clock, like <c>ComposeHourlyRawEdgesGuardLiveTests</c>.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class ComposeProcedureStatsEdgesEndToEndLiveTests
{
    private const int ServerIdA = -945401;
    private const int ServerIdB = -945402;
    private const string NameA = "ProcLedgerA";
    private const string NameB = "ProcLedgerB";
    private const string Hybrid = "procedure_stats_interval_hourly AS mid";

    private const string TopProcedures =
        "{\"source\":\"procedure_stats\",\"measure\":\"proc_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}";

    /// <summary>37 minutes and 21 seconds into an hour, so the window start is not on an hour and h1 is the next whole hour.</summary>
    private static readonly DateTime Anchor = new(2026, 1, 5, 12, 37, 21, DateTimeKind.Utc);

    private static readonly DateTime H2 = new(2026, 1, 5, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime H1 = new(2026, 1, 4, 13, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task AFleetTopProceduresCell_OverTheWritersLedger_TakesTheRoute_AndMatchesRaw()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "Set DARLING_TEST_PG to run the #9357 end-to-end live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await ProcedureStatsHourLedgerLiveTests.Rig.OpenAsync(ct, timescale: true);

        var bodySucceeded = false;
        try
        {
            await SeedThroughTheWriterAsync(rig, ct);
            var (hybrid, raw) = await RunBothAsync(rig, ct);

            Assert.Contains(Hybrid, (string)hybrid["sql"]!, StringComparison.Ordinal);
            Assert.True(raw.Count >= 6, "the seeded fixture ranks several procedures");
            Assert.Equal(raw.ToJsonString(), hybrid["rows"]!.ToJsonString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => Task.CompletedTask);
        }
    }

    [Theory]
    [InlineData("ledger_ahead")]
    [InlineData("ledger_behind")]
    [InlineData("ledger_row_missing")]
    public async Task OneHoursLedgerCountThatDisagrees_ReadsRaw_WithTheRawRows(string disagreement)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "Set DARLING_TEST_PG to run the #9357 end-to-end live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await ProcedureStatsHourLedgerLiveTests.Rig.OpenAsync(ct, timescale: true);

        var bodySucceeded = false;
        try
        {
            await SeedThroughTheWriterAsync(rig, ct);
            var (agreed, agreedRaw) = await RunBothAsync(rig, ct);
            Assert.Contains(Hybrid, (string)agreed["sql"]!, StringComparison.Ordinal);

            var bucket = H1.AddHours(5);
            var change = disagreement switch
            {
                "ledger_ahead" => "UPDATE collect.procedure_stats_hour_ledger SET n = n + 1 WHERE server_id = $1 AND bucket = $2",
                "ledger_behind" => "UPDATE collect.procedure_stats_hour_ledger SET n = n - 1 WHERE server_id = $1 AND bucket = $2",
                _ => "DELETE FROM collect.procedure_stats_hour_ledger WHERE server_id = $1 AND bucket = $2",
            };
            await using (var command = new NpgsqlCommand(change, rig.Connection))
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerIdA });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(bucket, DateTimeKind.Unspecified) });
                Assert.Equal(1, await command.ExecuteNonQueryAsync(ct));
            }

            var (fallback, fallbackRaw) = await RunBothAsync(rig, ct);
            Assert.DoesNotContain("_interval_hourly", (string)fallback["sql"]!, StringComparison.Ordinal);
            Assert.Equal(fallbackRaw.ToJsonString(), fallback["rows"]!.ToJsonString());
            Assert.Equal(agreedRaw.ToJsonString(), fallback["rows"]!.ToJsonString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => Task.CompletedTask);
        }
    }

    /// <summary>24 hours of procedure rows on two servers through <c>WriteBatchAsync</c>: whole-hour middle rows at distinct values (so
    /// SUM ranks them apart), a restart row that carries a delta, edge rows on both sides of the whole-hour middle and a row
    /// before the window. Then the rollup is refreshed over the window and the ledger's <c>counted_since</c> is moved below it, as
    /// it would be after the ledger had run for the window's length.</summary>
    private static async Task SeedThroughTheWriterAsync(ProcedureStatsHourLedgerLiveTests.Rig rig, CancellationToken ct)
    {
        var servers = new[] { (Id: ServerIdA, Name: NameA), (Id: ServerIdB, Name: NameB) };
        for (var hour = -1; hour <= 24; hour++)
        {
            var at = H1.AddHours(hour);
            foreach (var (id, name) in servers)
            {
                var server = ProcedureStatsHourLedgerLiveTests.Rig.Server(id, name);
                var rows = new List<ProcedureStatsCollector.Row>();
                for (var p = 0; p < 6; p++)
                {
                    rows.Add(Row("usp_P" + p.ToString(CultureInfo.InvariantCulture), at, 100 + 7 * (hour + 1) + 13 * p + (id == ServerIdA ? 0 : 5), 60));
                }

                if (hour == 7)
                {
                    rows.Add(Row("usp_P0", at, 999_999, 0));
                }

                await rig.WriteAsync(server, at.AddMinutes(hour % 2 == 0 ? 10 : 40), rows, ct);
            }
        }

        /* edge rows inside the window but outside the whole-hour middle, and one row before the window */
        var edge = ProcedureStatsHourLedgerLiveTests.Rig.Server(ServerIdA, NameA);
        await rig.WriteAsync(edge, H1.AddMinutes(-20), new List<ProcedureStatsCollector.Row> { Row("usp_Edge", H1, 31, 60) }, ct);
        await rig.WriteAsync(edge, H2.AddMinutes(20), new List<ProcedureStatsCollector.Row> { Row("usp_Edge", H2, 41, 60) }, ct);
        await rig.WriteAsync(edge, H1.AddHours(-3), new List<ProcedureStatsCollector.Row> { Row("usp_Before", H1, 51, 60) }, ct);

        await using (var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", rig.Connection))
        {
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(H1.AddHours(-4), DateTimeKind.Unspecified));
            refresh.Parameters.AddWithValue(DateTime.SpecifyKind(H2, DateTimeKind.Unspecified));
            await refresh.ExecuteNonQueryAsync(ct);
        }

        await using var since = new NpgsqlCommand($"UPDATE {ProcedureStatsHourLedger.StateTable} SET counted_since = $1", rig.Connection);
        since.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified) });
        Assert.Equal(1, await since.ExecuteNonQueryAsync(ct));
    }

    private static ProcedureStatsCollector.Row Row(string name, DateTime at, long worker, int interval) => new(
        "AppDb", "dbo", name, "P ", at, at, 1, worker, worker, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0,
        "0x" + name, "0xP" + name, null, null)
    {
        Precomputed = new ProcedureStatsCollector.ReadTimeDeltas(1, worker, worker, 1, 1, 1, 0, interval),
    };

    /// <summary>Runs the cell through the runner (the 24 hour window ending at the anchor) and the same panel compiled for the raw
    /// route on its own connection, and returns both. The panel must be a candidate for exactly the hours the
    /// fixture is laid out on.</summary>
    private static async Task<(JsonObject Payload, JsonArray Raw)> RunBothAsync(ProcedureStatsHourLedgerLiveTests.Rig rig, CancellationToken ct)
    {
        var body = new JsonObject { ["panel"] = JsonNode.Parse(TopProcedures), ["hours"] = 24 };
        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(rig.DataSource, body, ct, nowUtc: Anchor);
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");

        var windowStart = Anchor.AddHours(-24);
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(rig.DataSource, ct);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(TopProcedures)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        var candidate = ComposeSourceRouter.HourlyRawEdgesCandidate(plan!, Anchor, windowStart, Anchor, rollups, coverage);
        Assert.NotNull(candidate);
        Assert.Equal(H1, candidate!.HourStartUtc);
        Assert.Equal(H2, candidate.HourEndUtc);

        var rawContext = new ComposeRunContext(null, windowStart, Anchor, ComposeRunContext.NoVariables, rollups, Anchor, coverage, ModuleMapMissing: true);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, rawContext);
        Assert.True(compileError is null, compileError);
        Assert.DoesNotContain("_interval_hourly", compiled!.Sql, StringComparison.Ordinal);

        await using var connection = await rig.DataSource.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(compiled.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var rows = new JsonArray();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new JsonObject();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : DarlingWebEndpoints.DbValueToJson(reader.GetValue(i));
            }

            rows.Add(row);
        }

        Assert.NotEmpty(rows);
        return (outcome.Payload!, rows);
    }
}
