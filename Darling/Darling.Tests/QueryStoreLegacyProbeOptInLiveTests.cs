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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Clause 6 of the wide-table gate (<see cref="QueryStoreIntervalWide.HasLegacyRowSql"/>) is opt-in
/// (<c>LegacyRows.Refuse</c>). Only a reader that places every snapshot of a legacy row at its own
/// <c>collection_time</c> (the Viewer's duration trend) needs it. Every other reader keeps the newest snapshot per interval,
/// which the table holds for a legacy row too, so it must not pay the probe: on compressed chunks the probe decompresses every
/// batch of the server to prove there are none, and the analysis pass ran it on every pass for every server.
///
/// <para>The seed holds legacy rows (no <c>interval_start_time_utc</c>), one of them re-fetched across three collections with
/// rising cumulative counts, beside ordinary rows. Seeded through the real write path, read through the real readers.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and then works entirely inside it. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreLegacyProbeOptInLiveTests
{
    private const int ServerId = -6267001;
    private const string Db = "legacyprobe";

    /* The three statements the failure read's pass is told apart by. The wide and raw reads share their select list. */
    private const string FailureSelect = "SELECT database_name, query_id, execution_type_desc, execution_count, last_execution_time";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public DateTime End { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, params NpgsqlParameter[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static NpgsqlParameter Stamp(string name, DateTime value) =>
        new(name, NpgsqlDbType.Timestamp) { Value = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) };

    /* The shipped statements bind by position ($1, $2), and Npgsql refuses a mix with named parameters. */
    private static NpgsqlParameter Positional(DateTime value) =>
        new() { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) };

    /// <summary>
    /// Seeds one server's last 17 days. Query 10: a LEGACY timeout row (no interval start) re-fetched three times with cumulative
    /// counts 1, 2 and 4, so the table and raw each keep the 4 and a reader that summed snapshots would see 7. Query 13: errors
    /// in two ordinary intervals. Query 14: legacy timeouts in the baseline (40) and the recent window (3), a falling rate.
    /// Every query also has a Regular row in the baseline and in the recent window, some of them legacy.
    /// </summary>
    private static async Task<Rig> SeedAsync(CancellationToken ct)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #6267 legacy probe opt-in live test.");

        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'legacyprobe', 'legacyprobe', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", new NpgsqlParameter("server_id", ServerId));

        var now = LiveClock.Now();
        var end = now.AddTicks(-(now.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "legacyprobe-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "legacyprobe", Host = "legacyprobe-host" },
            ConnectionString = "Server=legacyprobe-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "legacyprobe-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long nextId = 8000;
        async Task W(string type, DateTime first, long queryId, long executions, bool legacy, long? intervalId = null, int fetch = 0)
        {
            var id = intervalId ?? ++nextId;
            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = queryId * 10,
                ExecutionTypeDesc = type,
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(30),
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = executions,
                AvgCpuTimeUs = 1_000,
                AvgDurationUs = 2_000,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = id,
                /* Legacy: no interval start, the row the probe looks for. */
                IntervalStartTimeUtc = legacy ? null : first,
            };
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(40 + (30 * fetch)), context, ct);
        }

        var b1 = end.AddDays(-5);
        var r2 = end.AddHours(-5);
        var r3 = end.AddHours(-3);

        /* The table's floor: an ordinary interval before the read's window. */
        await W("Regular", end.AddDays(-17), 90, 5, legacy: false);

        foreach (var (q, legacy) in new[] { (10L, true), (13L, false), (14L, true) })
        {
            await W("Regular", b1, q, 100, legacy);
            await W("Regular", r2, q, 100, legacy);
        }

        /* 10: one legacy interval fetched three times, 1 then 2 then 4 timeouts: 4 recent runs, no baseline. Flagged. */
        await W("Aborted", r2, 10, 1, legacy: true, intervalId: 8100, fetch: 0);
        await W("Aborted", r2, 10, 2, legacy: true, intervalId: 8100, fetch: 1);
        await W("Aborted", r2, 10, 4, legacy: true, intervalId: 8100, fetch: 2);
        /* 13: errors 0 -> 5 across two ordinary intervals. Flagged. */
        await W("Exception", r2, 13, 2, legacy: false);
        await W("Exception", r3, 13, 3, legacy: false);
        /* 14: legacy timeouts, 40 in the baseline and 3 recent: the rate fell, not flagged. */
        await W("Aborted", b1, 14, 40, legacy: true);
        await W("Aborted", r2, 14, 3, legacy: true);

        await ExecAsync(connection, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT row_number() OVER (ORDER BY t), @server_id, 'legacyprobe-log', 'query_store', t, 'SUCCESS'
FROM generate_series(@from, @to, INTERVAL '5 minutes') AS t",
            new NpgsqlParameter("server_id", ServerId), Stamp("from", end.AddDays(-20)), Stamp("to", end));
        await ExecAsync(connection,
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id",
            new NpgsqlParameter("server_id", ServerId), Stamp("value", end.AddDays(-19)));

        /* The seed really is legacy: the shipped probe finds rows in the window the failure read uses. */
        Assert.True(await HasLegacyRowAsync(connection, end.AddDays(-15), end), "the seed must hold a legacy row in the window");

        return new Rig { Scratch = scratch, Connection = connection, Postgres = postgres, End = end };
    }

    /// <summary>
    /// The end of a read window that is a literal end: <c>max(applied_through, rig.End)</c>. The gate refuses a literal end before
    /// the server's <c>applied_through</c>, and the seed's writes can stamp it past <see cref="Rig.End"/> on a slow runner (#6479).
    /// </summary>
    private static async Task<DateTime> QueryEndAsync(Rig rig, CancellationToken ct)
    {
        var appliedThrough = (DateTime)(await new NpgsqlCommand(
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture), rig.Connection)
            .ExecuteScalarAsync(ct))!;
        return appliedThrough > rig.End ? appliedThrough : rig.End;
    }

    private static async Task<bool> HasLegacyRowAsync(NpgsqlConnection connection, DateTime start, DateTime end)
    {
        await using var command = new NpgsqlCommand(QueryStoreIntervalWide.HasLegacyRowSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(Positional(start));
        command.Parameters.Add(Positional(end));
        return (bool)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    /// <summary>
    /// The failure read of the analysis pass (#6267) over a window holding legacy rows: it reads the wide table WITHOUT running the
    /// legacy-row probe, and what it reads equals the raw form's answer exactly, row for row. The pass is the real collector, so
    /// the two counts below are what the pass sent, not what a test wrote. With the probe forced on for this caller the gate
    /// refuses the table (the probe finds the legacy rows), the pass reads raw, and the wide count is 0 where this test wants 1.
    /// </summary>
    [Fact]
    public async Task TheFailureRead_SkipsTheLegacyProbe_ReadsTheWideTable_AndEqualsTheRawFormExactly_WithLegacyRowsInTheWindow()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await SeedAsync(ct);
        var end = rig.End;

        /* The gate, as the failure read calls it (no opt-in): the table. And with the probe asked for, the same window is refused. */
        var windowStart = end.AddDays(-14).AddHours(-1);
        var (useTable, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            rig.Connection, ServerId, windowStart, end, null, QueryStoreIntervalWide.TopMinWindow, 30, null, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, ct);
        Assert.True(useTable, "a window holding legacy rows must still route the failure read to the table");
        var (forcedProbe, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            rig.Connection, ServerId, windowStart, end, null, QueryStoreIntervalWide.TopMinWindow, 30, null, legacyRows: QueryStoreIntervalWide.LegacyRows.Refuse, ct);
        Assert.False(forcedProbe, "with the probe on, the legacy rows must refuse the table (the control for the assertion above)");

        /* The two forms of the failure read, same bounds, compared on every column. */
        var recentStart = end.AddHours(-PlanRegressionFailureRule.RecentHours);
        var baselineHours = Math.Max(1.0, (recentStart - windowStart).TotalHours);
        async Task<List<(string, long, long, long, long, long, bool, bool, bool, bool)>> ReadAsync(string sql, bool raw)
        {
            await using var command = new NpgsqlCommand(sql.Replace(PgSecondaryReplicaScope.Marker, string.Empty, StringComparison.Ordinal), rig.Connection);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, windowStart);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, recentStart);
            command.Parameters.AddWithValue(NpgsqlDbType.Double, (double)PlanRegressionFailureRule.RecentHours);
            command.Parameters.AddWithValue(NpgsqlDbType.Double, baselineHours);
            if (raw)
            {
                command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, windowStart.AddDays(-1));
            }

            var rows = new List<(string, long, long, long, long, long, bool, bool, bool, bool)>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rows.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5),
                    reader.GetBoolean(6), reader.GetBoolean(7), reader.GetBoolean(8), reader.GetBoolean(9)));
            }

            return rows;
        }

        var wideRows = await ReadAsync(PgFactCollector.PlanRegressionFailureWideSql, raw: false);
        var rawRows = await ReadAsync(PgFactCollector.PlanRegressionFailureRawSql, raw: true);
        Assert.Equal(2, rawRows.Count);
        Assert.Equal(rawRows, wideRows);
        /* The legacy re-fetched interval counts once, at its newest snapshot: 4 runs, not 1 + 2 + 4. */
        Assert.Equal(4, rawRows.Single(r => r.Item2 == 10).Item3);

        /* The real pass, counting what it sent. */
        var pass = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "legacyprobe-host",
            TimeRangeStart = end.AddHours(-1),
            TimeRangeEnd = end,
            ServerUtcOffset = TimeSpan.Zero,
        };
        using var probes = new NpgsqlCommandCounter(rig.Scratch.DatabaseName, "SELECT EXISTS", "interval_start_time_utc IS NULL");
        using var wideReads = new NpgsqlCommandCounter(rig.Scratch.DatabaseName, FailureSelect, "FROM query_store_interval_wide");
        using var rawReads = new NpgsqlCommandCounter(rig.Scratch.DatabaseName, FailureSelect, "FROM query_store_stats");
        using var controls = new NpgsqlCommandCounter(rig.Scratch.DatabaseName, "legacy_probe_count_control");
        await using (var control = rig.Postgres.CreateCommand("SELECT 1 AS legacy_probe_count_control"))
        {
            await control.ExecuteScalarAsync(ct);
        }

        Assert.True(controls.Count == 1, $"The Npgsql activity listener counted {controls.Count} control commands instead of 1, so the counts below would be meaningless.");

        var facts = await new PgFactCollector(rig.Postgres).CollectFactsAsync(pass);
        var fact = Assert.Single(facts, f => f.Key == "PLAN_REGRESSION");

        Assert.Equal(0, probes.Count);
        Assert.Equal(1, wideReads.Count);
        Assert.Equal(0, rawReads.Count);

        Assert.Equal(2, fact.Metadata["failure_offender_count"]);
        Assert.Equal(1, fact.Metadata["timeout_offender_count"]);
        Assert.Equal(10, fact.Metadata["worst_timeout_query_id"]);
        Assert.Equal(4, fact.Metadata["worst_timeout_recent_runs"]);
        Assert.Equal(1, fact.Metadata["error_offender_count"]);
        Assert.Equal(13, fact.Metadata["worst_error_query_id"]);
        Assert.Equal(5, fact.Metadata["worst_error_recent_runs"]);
    }

    /// <summary>
    /// The one reader that needs clause 6, the Viewer's duration trend, asks for it and a legacy row in the window still refuses
    /// the table. A reader that does not ask gets the table over the same window. (The Viewer's own end-to-end proof, which
    /// reads the series it returns, is <c>QueryStoreDurationTrendTableLiveTests</c>.)
    /// </summary>
    [Fact]
    public async Task TheDurationTrendsGate_StillRefusesALegacyWindow_WhenItAsksForClause6_AndAReaderThatDoesNotAskGetsTheTable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await SeedAsync(ct);
        var start = rig.End.AddDays(-6);

        var (asked, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            rig.Connection, ServerId, start, rig.End, null, TimeSpan.FromHours(48), 30, null, legacyRows: QueryStoreIntervalWide.LegacyRows.Refuse, ct);
        Assert.False(asked, "a legacy row in the window must refuse the table for a reader that places every snapshot");

        var (notAsked, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            rig.Connection, ServerId, start, rig.End, null, TimeSpan.FromHours(48), 30, null, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, ct);
        Assert.True(notAsked, "the same window must route a reader that keeps the newest snapshot per interval to the table");
    }

    /// <summary>
    /// The composed panels' fleet gate (<see cref="ComposePanelRunner.ResolveQueryStoreWideEligibleAsync"/>) over a server whose
    /// window holds legacy rows: it does not ask for clause 6, so the server stays on the interval table and is not sent raw.
    /// </summary>
    [Fact]
    public async Task TheComposeGate_KeepsAServerWithLegacyRowsOnTheTable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await SeedAsync(ct);

        /* The literal end must not fall before the server's applied_through (#6479): the seed stamps it from the clock at write
           time, which on a slow runner is past rig.End, and the gate answers that with "not eligible", not an error. */
        var queryEnd = await QueryEndAsync(rig, ct);
        var got = await ComposePanelRunner.ResolveQueryStoreWideEligibleAsync(rig.Postgres, null, rig.End.AddDays(-6), queryEnd, queryEnd, ct);
        Assert.True(got.Eligible);
        Assert.Null(got.RawServerIds);
    }

    /// <summary>
    /// A second non-trend reader, the MCP top (<see cref="DarlingDataReader.GetQueryStoreTopWithReachAsync"/>): over the legacy
    /// seed it reads the table, and the table's answer equals the raw statement's, key for key and count for count.
    /// </summary>
    [Fact]
    public async Task TheMcpTopRead_ReadsTheTable_WithLegacyRowsInTheWindow_AndEqualsRaw()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await SeedAsync(ct);
        var queryEnd = await QueryEndAsync(rig, ct);
        var start = rig.End.AddDays(-6);
        const int top = 50;

        var viaGate = await DarlingDataReader.GetQueryStoreTopWithReachAsync(rig.Postgres, ServerId, start, queryEnd, top, DatabaseFilter.All, null, null, ct);
        Assert.NotNull(viaGate.Table);
        Assert.True(viaGate.Table!.Value.UseTable);

        var rawKeys = new List<(string, long, long, string, string?, long)>();
        await using (var command = new NpgsqlCommand(DarlingDataReader.QueryStoreTopSql, rig.Connection))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            command.Parameters.Add(Positional(start));
            command.Parameters.Add(Positional(queryEnd));
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
            command.Parameters.Add(DatabaseFilter.All.Parameter());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FirstCandidates(top) });
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rawKeys.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetString(5),
                    reader.IsDBNull(16) ? null : reader.GetString(16), reader.GetInt64(7)));
            }
        }

        Assert.True(rawKeys.Count > 0, "the seed produced no raw rows; the comparison would be vacuous");
        Assert.Equal(
            rawKeys.OrderBy(k => k).ToList(),
            viaGate.Rows.Select(r => (r.DatabaseName, r.QueryId, r.PlanId, r.ExecutionTypeDesc, r.ReplicaRole, r.TotalExecutions)).OrderBy(k => k).ToList());
        /* The legacy re-fetched interval counts once, at its newest snapshot. */
        Assert.Equal(4, viaGate.Rows.Single(r => r.QueryId == 10 && r.ExecutionTypeDesc == "Aborted").TotalExecutions);
    }
}
