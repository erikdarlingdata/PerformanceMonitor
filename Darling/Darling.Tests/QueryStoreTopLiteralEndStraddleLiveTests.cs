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
///
/// <para><b>Isolating clause 4.</b> <see cref="SeedStraddleAsync"/>'s own coverage bookkeeping
/// (<c>QueryStoreIntervalWide.EnsureCoverageSql</c>) raises <c>filled_since</c> to the wall clock the FIRST apply
/// ran at — not to <see cref="WindowStart"/>'s own, much earlier, historical span — so left alone,
/// <c>filled_since &gt; WindowStart</c> and clause 2 alone sends both reads to raw: the pin's own 192/192 would
/// pass whether or not clause 4 does anything. This test forces <c>filled_since</c> back below
/// <see cref="WindowStart"/> after seeding (the same move <c>QueryStoreIntervalWideGridLiveTests</c> makes for
/// the identical reason), so every clause EXCEPT 4 passes, and adds a positive control — the same seeded store,
/// read with an open end — that proves the table path is live for this identity before trusting that the
/// literal-end reads' 192/zero-scans means clause 4 specifically.</para>
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

        /* WriteBackfillBatchAsync's own coverage bookkeeping raises filled_since to the WALL-CLOCK "now" the
           first apply ran at (EnsureCoverageSql), not to this seed's own historical span far in 2026-09. Left
           alone, filled_since > WindowStart and clause 2 ALONE sends both reads to raw — the assertions below
           would pass whether or not clause 4 does anything. Force it back, the same move
           QueryStoreIntervalWideGridLiveTests.ForceFilledSinceAsync makes for the identical reason, so every
           clause except 4 passes and the literal-end reads below isolate clause 4 alone. */
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        var appliedThrough = await ScalarDateTimeAsync(connection,
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct);
        var filledSince = await ScalarDateTimeAsync(connection,
            "SELECT filled_since FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct);
        Assert.True(filledSince <= WindowStart,
            $"the pin's premise: filled_since ({filledSince:o}) must be at or before WindowStart ({WindowStart:o}), so only clause 4 can refuse the table below");
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

        /* ---- the positive control: the SAME seeded store, an open end, proves the table path is live here
           before the literal-end assertions below are read as clause 4's own doing. If this ever refuses
           again, the message below reads UseTable's own inputs — read on the SAME connection, with the SAME
           SQL ReadsTableAsync uses — instead of leaving the next reader to re-derive them from the source. ---- */
        var (useTableOpenEnd, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, literalWindowEnd: null,
            DarlingDataReader.QueryStoreTopMinWindow, 30, null, ct);
        if (!useTableOpenEnd)
        {
            var pendingCount = await ScalarLongAsync(connection,
                "SELECT COUNT(*) FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id", ct);
            var rawFloor = await ScalarNullableDateTimeAsync(connection,
                "SELECT MIN(range_start) AT TIME ZONE 'UTC' FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats'", ct);
            var tableFloor = await ScalarNullableDateTimeAsync(connection,
                "SELECT MIN(t.first_execution_time) FROM collect.query_store_interval_wide AS t WHERE t.server_id = @server_id", ct);
            Assert.Fail(
                $"the positive control: an open end over this seeded, forced-covered window must route to the table "
                + $"(filled_since={filledSince:o}, applied_through={appliedThrough:o}, pending={pendingCount}, "
                + $"rawFloor={rawFloor:o}, tableFloor={tableFloor:o}, window={WindowStart:o}-{WindowEnd:o}, "
                + $"minWindow={DarlingDataReader.QueryStoreTopMinWindow})");
        }

        var (useTableStraddle, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, literalWindowEnd: LiteralEnd,
            DarlingDataReader.QueryStoreTopMinWindow, 30, null, ct);
        Assert.False(useTableStraddle, "clause 4 alone must refuse the table for the straddling literal end");

        var beforeOpenEnd = await WideTableScanCountsAsync();
        var appliedThroughAsOf = appliedThrough.AddMinutes(1);
        var mcpOpenEndRows = await DarlingDataReader.GetQueryStoreTopAsync(postgres, ServerId, WindowStart, appliedThroughAsOf, TestTop, null, ct);
        var mcpOpenEndRow = mcpOpenEndRows.Single(r => r.DatabaseName == "qsStraddle" && r.QueryId == 7001);
        Assert.Equal(385, mcpOpenEndRow.TotalExecutions);
        var afterOpenEnd = await WideTableScanCountsAsync();
        Assert.True(afterOpenEnd.SeqScan + afterOpenEnd.IdxScan > beforeOpenEnd.SeqScan + beforeOpenEnd.IdxScan,
            "the positive control's MCP read (a literal end at/after applied_through) must actually scan the wide table");

        var before = await WideTableScanCountsAsync();

        /* The viewer grid: a custom range whose literal end straddles the two snapshots. The product never
           passes an endUtc past literalEndUtc for a custom range (see the doc comment on
           ViewerDataService.GetQueryStoreTopQueriesAsync) - for a custom range they are the same value - so this
           passes LiteralEnd for both, matching raw's own bound to the straddle point instead of past it. */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var gridRows = await viewer.GetQueryStoreTopQueriesAsync(ServerId, WindowStart, LiteralEnd, TestTop, null, literalEndUtc: LiteralEnd);
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

        /* Clause 3's own floor control: UseTable's tableFloor is MIN(first_execution_time) across the WHOLE
           server, not just this identity, and clause 3 refuses unless raw's chunk floor or WindowStart minus
           IntervalSpanMargin ("skewFloor") reaches at or past it. Left with only the 7001 identity below, the
           table's floor sits at day0 (WindowStart + 1h) - AFTER skewFloor (WindowStart - 1 day) - so clause 3
           alone refuses even the open-end positive control, before clause 4 is ever reached. A second,
           unrelated identity anchored at or before skewFloor pulls the server's table floor down there, so
           clause 3 passes and only clause 4 (LiteralEnd vs applied_through) can still refuse the straddle read. */
        var floorAnchor = new QueryStoreCollector.Row
        {
            DatabaseName = "qsStraddle",
            QueryId = 6999,
            PlanId = 69991,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = WindowStart.AddDays(-2),
            LastExecutionTime = WindowStart.AddDays(-2).AddMinutes(9),
            QueryHash = "0x00006999",
            QueryPlanHash = "0x00069991",
            ExecutionCount = 10,
            AvgCpuTimeUs = 50,
            AvgDurationUs = 100,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = 6998,
        };
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { floorAnchor }, server, WindowStart.AddDays(-2).AddMinutes(10), context, ct);

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

    /// <summary>Same move as <c>QueryStoreIntervalWideGridLiveTests.ForceFilledSinceAsync</c>: backdates the
    /// coverage row's <c>filled_since</c> past the seed's own wall-clock stamp, so clause 2 cannot be the reason
    /// a read below picks raw.</summary>
    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, DateTime value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        command.Parameters.AddWithValue("value", DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
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

    /// <summary>Same gate-input probe as <see cref="ScalarDateTimeAsync"/>, NULL-tolerant for a floor that may not
    /// exist (no chunk, no table row) — read into the positive control's own failure message, not asserted on.</summary>
    private static async Task<DateTime?> ScalarNullableDateTimeAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        var result = await command.ExecuteScalarAsync(ct);
        return result is DateTime dt ? dt : null;
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}
