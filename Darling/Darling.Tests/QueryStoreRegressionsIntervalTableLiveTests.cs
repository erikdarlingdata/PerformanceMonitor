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
using System.Text.Json;
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
/// #5809: get_query_store_regressions, and the viewer's Regressions grid, read <c>query_store_interval_wide</c> where the
/// wide gate allows, and raw <c>query_store_stats</c> otherwise. Seeded through the real write path (raw and the table
/// together): a query whose CPU and duration regress between the 7-day baseline and the window, each interval collected
/// twice with a growing count, an Aborted run beside it, and a query that did not regress. Both paths return the same
/// regressions, the interval path's plan reads no <c>query_store_stats</c> row, and a store whose table does not cover
/// the span reads raw and still returns them.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race live collection. It
   shares the below-floor tests' gap-cache collection, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsIntervalTableLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsReg";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task SeedAsync(NpgsqlDataSource postgres, DateTime baselineStart, DateTime start, CancellationToken ct)
    {
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsreg-host", CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsreg", Host = "qsreg-host" },
            ConnectionString = "Server=qsreg-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsreg-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 9000;
        QueryStoreCollector.Row Row(long queryId, long planId, string outcome, DateTime first, long executions, long cpuUs, long durationUs) => new()
        {
            DatabaseName = Db,
            QueryId = queryId,
            PlanId = planId,
            ExecutionTypeDesc = outcome,
            FirstExecutionTime = first,
            LastExecutionTime = first.AddMinutes(30),
            QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + planId.ToString("X8", CultureInfo.InvariantCulture),
            QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = cpuUs,
            AvgDurationUs = durationUs,
            AvgLogicalIoReads = cpuUs / 10,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = intervalId,
            IntervalStartTimeUtc = first,
        };

        /* One interval, collected twice: a first snapshot 20 minutes in and the latest 50 minutes in, with a growing count.
           Only the latest may be counted, on both paths. */
        async Task IntervalAsync(DateTime first, Func<DateTime, long, List<QueryStoreCollector.Row>> rows)
        {
            intervalId++;
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, rows(first, 1), server, first.AddMinutes(20), context, ct);
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, rows(first, 3), server, first.AddMinutes(50), context, ct);
        }

        /* The table's floor: an interval two days before the baseline, outside both arms, so the gate's #5541 edge holds. */
        await IntervalAsync(baselineStart.AddDays(-2), (f, k) => [Row(30, 301, "Regular", f, 5 * k, 1_000, 2_000)]);

        /* Baseline: query 10 runs cheap, query 20 the same as in the window. */
        foreach (var day in new[] { 1, 3, 5 })
        {
            await IntervalAsync(baselineStart.AddDays(day), (f, k) =>
            [
                Row(10, 101, "Regular", f, 10 * k, 1_000, 2_000),
                Row(20, 201, "Regular", f, 4 * k, 3_000, 3_000),
            ]);
        }

        /* Window: query 10 regresses on a new plan and also times out once per interval; query 20 does not. */
        foreach (var hour in new[] { 2, 10 })
        {
            await IntervalAsync(start.AddHours(hour), (f, k) =>
            [
                Row(10, 102, "Regular", f, 6 * k, 5_000, 9_000),
                Row(10, 102, "Aborted", f, k, 40_000, 30_000_000),
                Row(20, 201, "Regular", f, 4 * k, 3_000, 3_000),
            ]);
        }
    }

    private static string Describe(DarlingQueryStoreRegressionReader.RegressionRow r) => string.Join("|",
        r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
        Math.Round(r.RecentCpuMs, 6), Math.Round(r.BaselineReads, 6), Math.Round(r.RecentReads, 6), Math.Round(r.AdditionalDurationMs, 6),
        r.BaselineExecCount, r.RecentExecCount, r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime);

    private static string Describe(ViewerQueryStoreRegressionRow r) => string.Join("|",
        r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
        Math.Round(r.RecentCpuMs, 6), Math.Round(r.BaselineReads, 6), Math.Round(r.RecentReads, 6), Math.Round(r.AdditionalDurationMs, 6),
        r.BaselineExecCount, r.RecentExecCount, r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime);

    /// <summary>The old read, run as it shipped: <see cref="DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql"/> over raw.</summary>
    private static async Task<List<string>> RawReadAsync(NpgsqlConnection connection, DateTime start, DateTime end, DateTime baselineStart, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, connection);
        AddParameters(command, start, end, baselineStart);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(Describe(new DarlingQueryStoreRegressionReader.RegressionRow(
                reader.GetString(0), reader.GetInt64(1), reader.GetDouble(2), reader.GetDouble(3), null, reader.GetDouble(5), reader.GetDouble(6),
                null, reader.GetDouble(8), reader.GetDouble(9), null, reader.GetDouble(11), reader.GetInt64(12), reader.GetInt64(13),
                reader.GetInt32(14), reader.GetInt32(15), reader.GetString(16), reader.GetString(17), reader.IsDBNull(18) ? null : reader.GetDateTime(18))));
        }

        return rows;
    }

    private static void AddParameters(NpgsqlCommand command, DateTime start, DateTime end, DateTime baselineStart)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
        command.Parameters.Add(DatabaseFilter.One(Db).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 50 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
    }

    /// <summary>Every relation an EXPLAIN (FORMAT JSON) plan reads, by its <c>Relation Name</c>.</summary>
    private static async Task<List<string>> PlanRelationsAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DateTime baselineStart, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection);
        AddParameters(command, start, end, baselineStart);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var relations = new List<string>();
        void Walk(JsonElement node)
        {
            if (node.ValueKind == JsonValueKind.Object)
            {
                foreach (var p in node.EnumerateObject())
                {
                    if (p.Name == "Relation Name") relations.Add(p.Value.GetString()!);
                    Walk(p.Value);
                }
            }
            else if (node.ValueKind == JsonValueKind.Array)
            {
                foreach (var e in node.EnumerateArray()) Walk(e);
            }
        }

        Walk(doc.RootElement);
        return relations;
    }

    [Fact]
    public async Task ARegressionBetweenTheBaselineAndTheWindow_IsTheSameFromTheIntervalTableAndFromRaw()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5809 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsreg', 'qsreg', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var start = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-24), DateTimeKind.Unspecified);
        start = start.AddTicks(-(start.Ticks % TimeSpan.TicksPerSecond));
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        await SeedAsync(postgres, baselineStart, start, ct);
        /* The window ends now, as the MCP tool's does: at or after the coverage row's applied_through (the gate's clause 4). */
        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);

        /* The old read: query 10 regressed, query 20 did not. */
        var raw = await RawReadAsync(connection, start, end, baselineStart, ct);
        Assert.Single(raw);
        Assert.StartsWith(Db + "|10|", raw[0], StringComparison.Ordinal);

        /* The interval path serves, and returns exactly the old read's rows. */
        var table = await DarlingQueryStoreRegressionReader.TryGetQueryStoreRegressionsFromTableAsync(
            postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct);
        Assert.NotNull(table);
        Assert.Equal(raw, table.Select(Describe).ToList());
        var row = Assert.Single(table);
        /* Latest snapshots only, Aborted included: baseline 3 x 30, window 2 x (18 + 3). */
        Assert.Equal(90, row.BaselineExecCount);
        Assert.Equal(42, row.RecentExecCount);

        var served = await DarlingQueryStoreRegressionReader.GetQueryStoreRegressionsAsync(
            postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct);
        Assert.Equal(raw, served.Select(Describe).ToList());

        /* The viewer's grid takes the same route to the same rows. */
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            var viewerRows = await viewer.GetQueryStoreRegressionsAsync(ServerId, start, end, [Db], cancellationToken: ct);
            Assert.Equal(raw, viewerRows.Select(Describe).ToList());
        }

        /* The interval path's plan reads no query_store_stats row; the raw read's does (so the check can fail). */
        var tableRelations = await PlanRelationsAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, ct);
        Assert.Contains("query_store_interval_wide", tableRelations);
        Assert.DoesNotContain(tableRelations, r => r.StartsWith("query_store_stats", StringComparison.Ordinal));
        var rawRelations = await PlanRelationsAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, ct);
        Assert.Contains(rawRelations, r => r.StartsWith("query_store_stats", StringComparison.Ordinal));

        /* The #5541 floor rule: coverage that starts after the baseline start means the table does not hold the span, so
           the gate says raw, and the read still returns the same rows from raw. */
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(1), ct);
        Assert.Null(await DarlingQueryStoreRegressionReader.TryGetQueryStoreRegressionsFromTableAsync(
            postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct));
        var fallback = await DarlingQueryStoreRegressionReader.GetQueryStoreRegressionsAsync(
            postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct);
        Assert.Equal(raw, fallback.Select(Describe).ToList());
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            var viewerRows = await viewer.GetQueryStoreRegressionsAsync(ServerId, start, end, [Db], cancellationToken: ct);
            Assert.Equal(raw, viewerRows.Select(Describe).ToList());
        }
    }
}
