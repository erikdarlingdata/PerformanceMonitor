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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3953: a literal end before <c>applied_through</c> reads raw (<see cref="QueryStoreIntervalWide.UseTable"/>
/// clause 4), so a later re-fetch of the SAME interval past that end cannot clip the answer a bounded read
/// gives back. This pins one traced identity straddling a literal end on both callers this table serves — the
/// viewer grid (<see cref="ViewerDataService.GetQueryStoreTopQueriesAsync"/>) and the MCP/web top read
/// (<see cref="DarlingDataReader.GetQueryStoreTopAsync(NpgsqlDataSource,int,DateTime,DateTime,int,string,CancellationToken)"/>)
/// — seeded through the real write path so <c>applied_through</c> is the product's own apply-time stamp, not a
/// forced value.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and
   then works entirely inside it, the same shape as QueryStoreIntervalWideGridLiveTests and
   QueryStoreTopMcpLiveTests. */
public sealed class QueryStoreTopLiteralEndStraddleLiveTests
{
    private const int ServerId = -3953932;
    private const int TestTop = 50;

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddDays(3);
    /* Between the two collections below: raw's own $3 keeps the 192 snapshot and drops the 385 one, and
       clause 4 (a literal end this far in the past is always before applied_through, the real wall clock the
       apply below stamps it at) sends both callers to raw before either ever opens
       collect.query_store_interval_wide. */
    private static readonly DateTime LiteralEnd = WindowStart.AddDays(2);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task LiteralEndBeforeAppliedThrough_ReadsRaw_OnBothCallers_WithZeroWideTableScans()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 literal-end straddle live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await SeedStraddleAsync(runner, ct);

        var appliedThrough = await ScalarDateTimeAsync(connection,
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct);
        Assert.True(LiteralEnd < appliedThrough,
            $"the pin's premise: LiteralEnd ({LiteralEnd:o}) must sit before applied_through ({appliedThrough:o}), the real wall clock the apply above stamped");

        async Task<(long SeqScan, long IdxScan)> WideTableScanCountsAsync()
        {
            await using var command = new NpgsqlCommand(
                "SELECT COALESCE(seq_scan, 0), COALESCE(idx_scan, 0) FROM pg_stat_user_tables WHERE relname = 'query_store_interval_wide'", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return (0, 0);
            }

            return (reader.GetInt64(0), reader.GetInt64(1));
        }

        var before = await WideTableScanCountsAsync();

        /* The viewer grid: a custom range whose literal end straddles the two snapshots. */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var gridRows = await viewer.GetQueryStoreTopQueriesAsync(ServerId, WindowStart, WindowEnd, TestTop, null, literalEndUtc: LiteralEnd);
        var gridRow = gridRows.Single(r => r.DatabaseName == "qsStraddle" && r.QueryId == 7001);
        Assert.Equal(192, gridRow.TotalExecutions);

        /* The MCP/web top read: endUtc doubles as both the window end and the gate's literal end (#3953's own
           doc comment on DarlingDataReader.GetQueryStoreTopAsync), so an explicit as_of/end IS this parameter. */
        var mcpRows = await DarlingDataReader.GetQueryStoreTopAsync(postgres, ServerId, WindowStart, LiteralEnd, TestTop, null, ct);
        var mcpRow = mcpRows.Single(r => r.DatabaseName == "qsStraddle" && r.QueryId == 7001);
        Assert.Equal(192, mcpRow.TotalExecutions);

        var after = await WideTableScanCountsAsync();
        Assert.Equal(before, after);
    }

    /// <summary>
    /// One identity, two collections of the SAME Query Store runtime-stats interval (same
    /// <c>first_execution_time</c>): the first inside the window, execution_count 192; the second AFTER
    /// <see cref="LiteralEnd"/>, execution_count 385 — Query Store's own re-fetch-the-open-interval cadence.
    /// Both go through <see cref="DarlingCollectorRunner.WriteBackfillBatchAsync"/> (the product's own apply
    /// path), so <c>applied_through</c> is the real wall clock the second apply ran at, not a forced value.
    /// </summary>
    private static async Task SeedStraddleAsync(DarlingCollectorRunner runner, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "qsiw-straddle-host",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsiw-straddle", Host = "qsiw-straddle-host" },
            ConnectionString = "Server=qsiw-straddle-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsiw-straddle-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        var day0 = WindowStart.AddHours(1);

        var insideWindow = new QueryStoreCollector.Row
        {
            DatabaseName = "qsStraddle",
            QueryId = 7001,
            PlanId = 70011,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = day0,
            LastExecutionTime = day0.AddMinutes(9),
            QueryHash = "0x00007001",
            QueryPlanHash = "0x00070011",
            ExecutionCount = 192,
            AvgCpuTimeUs = 100,
            AvgDurationUs = 200,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = 7000,
        };
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { insideWindow }, server, day0.AddMinutes(10), context, ct);

        var afterLiteralEnd = LiteralEnd.AddHours(1);
        var reFetched = new QueryStoreCollector.Row
        {
            DatabaseName = "qsStraddle",
            QueryId = 7001,
            PlanId = 70011,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = day0,
            LastExecutionTime = afterLiteralEnd.AddMinutes(-5),
            QueryHash = "0x00007001",
            QueryPlanHash = "0x00070011",
            ExecutionCount = 385,
            AvgCpuTimeUs = 110,
            AvgDurationUs = 210,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = 7000,
        };
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { reFetched }, server, afterLiteralEnd, context, ct);
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    private static async Task<DateTime> ScalarDateTimeAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }
}
