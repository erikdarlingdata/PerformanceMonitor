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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
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

    private static async Task SeedAsync(NpgsqlDataSource postgres, DateTime baselineStart, DateTime start, CancellationToken ct, string db = Db, long intervalBase = 9000)
    {
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsreg-host", CollectionTime = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsreg", Host = "qsreg-host" },
            ConnectionString = "Server=qsreg-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsreg-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = intervalBase;
        QueryStoreCollector.Row Row(long queryId, long planId, string outcome, DateTime first, long executions, long cpuUs, long durationUs) => new()
        {
            DatabaseName = db,
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

        /* Window: query 10 regresses on a new plan and also times out once per interval; query 20 does not.
           #6781: 60 ms of CPU a run against the baseline's 1 ms adds 2.1 s over its 36 runs, over the 1 s floor, so the row is a cost row
           and not only a timeouts row (at 5 ms it added 144 ms and survived on its Aborted runs alone). */
        foreach (var hour in new[] { 2, 10 })
        {
            await IntervalAsync(start.AddHours(hour), (f, k) =>
            [
                Row(10, 102, "Regular", f, 6 * k, 60_000, 9_000),
                Row(10, 102, "Aborted", f, k, 40_000, 30_000_000),
                Row(20, 201, "Regular", f, 4 * k, 3_000, 3_000),
            ]);
        }
    }

    private static string Describe(DarlingQueryStoreRegressionReader.RegressionRow r) => string.Join("|",
        r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
        Math.Round(r.RecentCpuMs, 6), Math.Round(r.AdditionalDurationMs, 6),
        r.BaselineExecCount, r.RecentExecCount, r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime);

    private static string Describe(ViewerQueryStoreRegressionRow r) => string.Join("|",
        r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
        Math.Round(r.RecentCpuMs, 6), Math.Round(r.AdditionalDurationMs, 6),
        r.BaselineExecCount, r.RecentExecCount, r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime);

    /// <summary>The old read, run as it shipped: <see cref="DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql"/> over raw,
    /// or <paramref name="sql"/> when a test runs another form of the statement (the table form, at its fixed 26-hour margin).</summary>
    private static async Task<List<string>> RawReadAsync(NpgsqlConnection connection, DateTime start, DateTime end, DateTime baselineStart, CancellationToken ct, string? sql = null)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(sql ?? DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, connection);
        AddParameters(command, start, end, baselineStart);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(Describe(new DarlingQueryStoreRegressionReader.RegressionRow(
                reader.GetString(0), reader.GetInt64(1), reader.GetDouble(2), reader.GetDouble(3), null, reader.GetDouble(5), reader.GetDouble(6),
                null, reader.GetDouble(8), reader.GetInt64(9), reader.GetInt64(10),
                reader.GetInt32(11), reader.GetInt32(12), reader.GetString(13), reader.GetString(14), reader.IsDBNull(15) ? null : reader.GetDateTime(15))));
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
        /* #5506: no day from the daily totals (this test builds none), so the whole baseline is the live half. */
        var plan = QueryStoreRegressionBaseline.AllLive(start);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
        command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
        /* #6750: the start of the hours held, the row filters' start here. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        /* #6750: the failure arm's row-filter start and its hours held, the same on this one-tier read. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
    }

    /// <summary>
    /// Every scan node of the statement's EXECUTED plan (EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)), as the table it reads
    /// and the rows it returned. A scan names a partition or a TimescaleDB chunk, never its parent, so each node's
    /// relation is resolved to its root through <c>pg_inherits</c>, which both a chunk and a native partition carry.
    /// </summary>
    private static async Task<List<(string Table, long Rows)>> ExecutedScansAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DateTime baselineStart, CancellationToken ct, bool viewerParameters = false)
    {
        var scans = new List<(string Schema, string Name, long Rows)>();
        await using (var command = new NpgsqlCommand("EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) " + sql, connection))
        {
            if (viewerParameters)
            {
                /* The viewer's copy numbers its own: $1 server, $2 start, $3 end, $4 database filter, $5 baseline start. */
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
                command.Parameters.Add(DatabaseFilter.One(Db).Parameter());
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
                var plan = QueryStoreRegressionBaseline.AllLive(start);
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RunDays });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
                command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
                /* #6750: the start of the hours held, the row filters' start here. */
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
                /* #6750: the failure arm's row-filter start and its hours held, the same on this one-tier read. */
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
                command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
            }
            else
            {
                AddParameters(command, start, end, baselineStart);
            }

            var json = (string)(await command.ExecuteScalarAsync(ct))!;
            using var doc = JsonDocument.Parse(json);
            void Walk(JsonElement node)
            {
                if (node.ValueKind == JsonValueKind.Object)
                {
                    if (node.TryGetProperty("Relation Name", out var name))
                    {
                        var loops = node.TryGetProperty("Actual Loops", out var l) ? l.GetDouble() : 1;
                        var rows = node.TryGetProperty("Actual Rows", out var r) ? r.GetDouble() : 0;
                        scans.Add((node.GetProperty("Schema").GetString()!, name.GetString()!, (long)Math.Round(rows * loops)));
                    }

                    foreach (var p in node.EnumerateObject()) Walk(p.Value);
                }
                else if (node.ValueKind == JsonValueKind.Array)
                {
                    foreach (var e in node.EnumerateArray()) Walk(e);
                }
            }

            Walk(doc.RootElement);
        }

        var resolved = new List<(string Table, long Rows)>();
        foreach (var (schema, name, rows) in scans)
        {
            await using var root = new NpgsqlCommand("""
                WITH RECURSIVE up (oid, depth) AS (
                    SELECT format('%I.%I', $1, $2)::regclass::oid, 0
                    UNION ALL
                    SELECT i.inhparent, up.depth + 1
                    FROM pg_inherits AS i
                    JOIN up ON i.inhrelid = up.oid
                )
                SELECT pc.relname::text
                FROM up
                JOIN pg_class AS pc ON pc.oid = up.oid
                ORDER BY up.depth DESC
                LIMIT 1
                """, connection);
            root.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schema });
            root.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });
            resolved.Add(((string)(await root.ExecuteScalarAsync(ct))!, rows));
        }

        return resolved;
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

        var start = LiveClock.Now().AddHours(-24);
        start = start.AddTicks(-(start.Ticks % TimeSpan.TicksPerSecond));
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        await SeedAsync(postgres, baselineStart, start, ct);
        /* The window ends now, as the MCP tool's does: at or after the coverage row's applied_through (the gate's clause 4). */
        var end = LiveClock.Now();
        /* #6374: the seed's writer stamps the coverage row's applied_through with the REAL clock (the product's own
           DateTime.UtcNow), not LiveClock, so a DARLING_TEST_CLOCK_UTC behind the real clock put the window's end before
           applied_through, clause 4 sent the read to raw, and the interval path did not serve. The end is therefore never
           before the real clock; with an override at or after the real clock, the hour the window sits at comes from
           LiveClock. */
        var realNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        end = end > realNow ? end : realNow;
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
        /* Latest snapshots only, Regular executions only (#5506: the daily totals hold no other kind): baseline 3 x 30, window 2 x 18. */
        Assert.Equal(90, row.BaselineExecCount);
        Assert.Equal(36, row.RecentExecCount);
        /* #6781: the row clears the cost test's added-CPU floor, so the equality below compares a cost row on every side. */
        Assert.True((row.RecentCpuMs - row.BaselineCpuMs) * row.RecentExecCount >= QueryStoreRegressionRules.CostMinAddedCpuMs, Describe(row));

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct);
        Assert.Equal(raw, served.Rows.Select(Describe).ToList());
        /* The tool's history_source names the tier that answered. */
        Assert.Equal("interval_table", served.Source);

        /* The viewer's grid takes the same route to the same rows. */
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            var viewerTable = await viewer.TryGetQueryStoreRegressionsFromTableAsync(ServerId, start, end, null, [Db], baselineStart, null, ct);
            Assert.NotNull(viewerTable);
            Assert.Equal(raw, viewerTable.Select(Describe).ToList());
            var viewerRows = await viewer.GetQueryStoreRegressionsAsync(ServerId, start, end, [Db], cancellationToken: ct);
            Assert.Equal(raw, viewerRows.Select(Describe).ToList());
        }

        /* The interval path's executed plan reads no query_store_stats row, and reads interval_wide rows; the raw read's
           plan reads query_store_stats rows, so the check can fail. */
        var tableScans = await ExecutedScansAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, ct);
        Assert.DoesNotContain(tableScans, n => n.Table == "query_store_stats");
        Assert.True(tableScans.Where(n => n.Table == "query_store_interval_wide").Sum(n => n.Rows) > 0, string.Join(", ", tableScans));
        var viewerScans = await ExecutedScansAsync(connection, ViewerDataService.QueryStoreRegressionsTableSql, start, end, baselineStart, ct, viewerParameters: true);
        Assert.DoesNotContain(viewerScans, n => n.Table == "query_store_stats");
        Assert.True(viewerScans.Where(n => n.Table == "query_store_interval_wide").Sum(n => n.Rows) > 0, string.Join(", ", viewerScans));
        var rawScans = await ExecutedScansAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, ct);
        Assert.True(rawScans.Where(n => n.Table == "query_store_stats").Sum(n => n.Rows) > 0, string.Join(", ", rawScans));

        /* The #5541 floor rule: coverage that starts after the baseline start means the table does not hold the span, so
           the gate says raw, and the read still returns the same rows from raw. */
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(1), ct);
        Assert.Null(await DarlingQueryStoreRegressionReader.TryGetQueryStoreRegressionsFromTableAsync(
            postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct));
        var fallback = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct);
        Assert.Equal(raw, fallback.Rows.Select(Describe).ToList());
        Assert.Equal("raw", fallback.Source);
        await using (var viewer = new ViewerDataService(scratch.ConnectionString))
        {
            Assert.Null(await viewer.TryGetQueryStoreRegressionsFromTableAsync(ServerId, start, end, null, [Db], baselineStart, null, ct));
            var viewerRows = await viewer.GetQueryStoreRegressionsAsync(ServerId, start, end, [Db], cancellationToken: ct);
            Assert.Equal(raw, viewerRows.Select(Describe).ToList());
        }
    }

    /// <summary>
    /// #5506 step 2: one server with the seeded intervals of <see cref="SeedAsync"/>, the table's gate satisfied, and a health
    /// capture every six hours at <paramref name="intervalMinutes"/> (none when null), so the measured margin is three hours for
    /// 60-minute intervals and the fixed 26 hours with no capture.
    /// </summary>
    private sealed class MarginRig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required DateTime Start { get; init; }
        public required DateTime End { get; init; }
        public required DateTime BaselineStart { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task<MarginRig> CreateMarginRigAsync(string baseCs, int? intervalMinutes, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseCs, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsreg', 'qsreg', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var start = LiveClock.Now().AddHours(-24);
        start = start.AddTicks(-(start.Ticks % TimeSpan.TicksPerSecond));
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        await SeedAsync(postgres, baselineStart, start, ct);
        var end = LiveClock.Now();
        var realNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        end = end > realNow ? end : realNow;
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);

        if (intervalMinutes is int minutes)
        {
            /* The oldest capture is five days before the baseline starts, so it is older than the day the margin lookup needs. */
            await using var health = new NpgsqlCommand("""
                INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
                SELECT row_number() OVER (), c.c, $1, 'qsreg', $2, $3
                FROM generate_series($4, $5, interval '6 hours') AS c(c)
                """, connection);
            health.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            health.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Db });
            health.Parameters.Add(new NpgsqlParameter<int> { TypedValue = minutes });
            health.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(baselineStart.AddDays(-5), DateTimeKind.Unspecified) });
            health.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
            await health.ExecuteNonQueryAsync(ct);
        }

        return new MarginRig { Scratch = scratch, Connection = connection, Postgres = postgres, Start = start, End = end, BaselineStart = baselineStart };
    }

    /// <summary>
    /// Two intervals that break the invariant a 60-minute server keeps (every row's first execution lies within the interval length
    /// plus the catch-up of its collection), so only the shorter margin can miss them. Both are query 10's. The baseline one began
    /// ten hours before the baseline starts and ran into it. The recent one began ten hours before the window and ran into it.
    /// The 26-hour margin reads both, the three-hour margin reads neither.
    /// </summary>
    private static async Task SeedOutOfMarginIntervalsAsync(NpgsqlConnection connection, MarginRig rig, CancellationToken ct)
    {
        const string columns = """
            (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
             last_execution_time, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us,
             avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, runtime_stats_interval_id, interval_start_time_utc)
            """;
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO collect.query_store_interval_wide {columns}
            VALUES ($1, $2, $3, 10, 101, 'Regular', $4, $5, '0x0000000A', '0x00000065', 50, 2000, 1000, 100, 0, 0, 1, 7001, $4),
                   ($6, $2, $3, 10, 102, 'Regular', $7, $8, '0x0000000A', '0x00000066', 100, 9000, 5000, 500, 0, 0, 1, 7002, $7)
            """, connection);
        DateTime U(DateTime d) => DateTime.SpecifyKind(d, DateTimeKind.Unspecified);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = U(rig.BaselineStart.AddMinutes(30)) });
        insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Db });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = U(rig.BaselineStart.AddHours(-10)) });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = U(rig.BaselineStart.AddMinutes(10)) });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = U(rig.Start.AddHours(1)) });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = U(rig.Start.AddHours(-10)) });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = U(rig.Start.AddMinutes(5)) });
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// #5506 step 2: on a server whose measured interval is 60 minutes the table read uses the three-hour margin
    /// (<see cref="QueryStoreIntervalWide.ResolveEdgeMarginAsync"/>), as the aborts and top reads do, and returns the rows the
    /// fixed 26-hour margin returns on a store that keeps the invariant. Two seeded intervals that break it prove the narrower
    /// statement ran: the fixed statement counts them, the MCP read and the viewer's grid do not. Without that proof the
    /// equality would hold for either margin.
    /// </summary>
    [Fact]
    public async Task AServerWithAMeasuredMargin_ReadsTheNarrowFloor_AndReturnsTheFixedMarginsRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var rig = await CreateMarginRigAsync(baseCs!, 60, ct);
        Assert.Equal(TimeSpan.FromHours(3),
            await QueryStoreIntervalWide.ResolveEdgeMarginAsync(rig.Connection, ServerId, rig.BaselineStart, rig.End, 60, ct));

        /* The invariant holds: the narrow read and the 26-hour statement agree, and there is a regression to agree on. */
        var fixedBefore = await RawReadAsync(rig.Connection, rig.Start, rig.End, rig.BaselineStart, ct, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql);
        Assert.Single(fixedBefore);
        var narrowBefore = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, ct);
        Assert.Equal("interval_table", narrowBefore.Source);
        Assert.Equal(fixedBefore, narrowBefore.Rows.Select(Describe).ToList());
        /* #6781: the seeded row clears the cost test's added-CPU floor on its own (the later reads add to its runs, never take away). */
        var seeded = Assert.Single(narrowBefore.Rows);
        Assert.True((seeded.RecentCpuMs - seeded.BaselineCpuMs) * seeded.RecentExecCount >= QueryStoreRegressionRules.CostMinAddedCpuMs, Describe(seeded));

        /* Two intervals the 26-hour margin admits and the three-hour margin drops. */
        await SeedOutOfMarginIntervalsAsync(rig.Connection, rig, ct);
        var fixedAfter = await RawReadAsync(rig.Connection, rig.Start, rig.End, rig.BaselineStart, ct, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql);
        Assert.NotEqual(fixedBefore, fixedAfter);
        Assert.Contains("|140|136|", Assert.Single(fixedAfter), StringComparison.Ordinal);

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, ct);
        Assert.Equal("interval_table", served.Source);
        Assert.Equal(fixedBefore, served.Rows.Select(Describe).ToList());

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var viewerTable = await viewer.TryGetQueryStoreRegressionsFromTableAsync(ServerId, rig.Start, rig.End, null, [Db], rig.BaselineStart, null, ct);
        Assert.NotNull(viewerTable);
        Assert.Equal(fixedBefore, viewerTable.Select(Describe).ToList());
        var viewerRows = await viewer.GetQueryStoreRegressionsAsync(ServerId, rig.Start, rig.End, [Db], cancellationToken: ct);
        Assert.Equal(fixedBefore, viewerRows.Select(Describe).ToList());
    }

    /// <summary>
    /// #5506 step 2: a server with no usable measurement (no health capture) keeps the fixed 26-hour margin, so the same two
    /// intervals ARE counted, by the MCP read and by the viewer's grid.
    /// </summary>
    [Fact]
    public async Task AServerWithNoMeasurement_KeepsTheFixedMargin()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var rig = await CreateMarginRigAsync(baseCs!, null, ct);
        Assert.Equal(QueryStoreIntervalWide.PurgeEdgeMargin,
            await QueryStoreIntervalWide.ResolveEdgeMarginAsync(rig.Connection, ServerId, rig.BaselineStart, rig.End, 60, ct));

        await SeedOutOfMarginIntervalsAsync(rig.Connection, rig, ct);
        var fixedRows = await RawReadAsync(rig.Connection, rig.Start, rig.End, rig.BaselineStart, ct, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql);
        Assert.Contains("|140|136|", Assert.Single(fixedRows), StringComparison.Ordinal);

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, ct);
        Assert.Equal("interval_table", served.Source);
        Assert.Equal(fixedRows, served.Rows.Select(Describe).ToList());

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var viewerRows = await viewer.GetQueryStoreRegressionsAsync(ServerId, rig.Start, rig.End, [Db], cancellationToken: ct);
        Assert.Equal(fixedRows, viewerRows.Select(Describe).ToList());
    }

    /// <summary>
    /// #5506: the label comes from a keyed lookup of collect.query_store_text per output row. Three databases on one server
    /// share query_id 10. Two hold different text, the third holds none and falls back to the aggregate's sample. Each row
    /// must carry its own database's label, on the MCP read and the viewer's grid, so a lookup that dropped the database
    /// from its key would hand one database's text to another.
    /// </summary>
    [Fact]
    public async Task SharedQueryIdAcrossDatabases_EachRowCarriesItsOwnText_AndAMissingTextFallsBackToTheSample()
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

        var start = LiveClock.Now().AddHours(-24);
        start = start.AddTicks(-(start.Ticks % TimeSpan.TicksPerSecond));
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        string[] dbs = ["qsRegA", "qsRegB", "qsRegC"];
        for (var i = 0; i < dbs.Length; i++)
        {
            await SeedAsync(postgres, baselineStart, start, ct, dbs[i], 9000 + (i * 100));
        }

        var end = LiveClock.Now();
        var realNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        end = end > realNow ? end : realNow;
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);

        /* Text for A and B only, whatever the write path stored: database C has none, so it keeps the sample. */
        await using (var del = new NpgsqlCommand("DELETE FROM collect.query_store_text WHERE server_id = $1", connection))
        {
            del.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            await del.ExecuteNonQueryAsync(ct);
        }

        foreach (var (db, text) in new[] { (dbs[0], "text from database A"), (dbs[1], "text from database B") })
        {
            await using var ins = new NpgsqlCommand(
                "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, $2, 10, $3, now())", connection);
            ins.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            ins.Parameters.Add(new NpgsqlParameter<string> { TypedValue = db });
            ins.Parameters.Add(new NpgsqlParameter<string> { TypedValue = text });
            await ins.ExecuteNonQueryAsync(ct);
        }

        var filter = DatabaseFilter.Of(dbs);
        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            postgres, ServerId, start, end, filter, 50, baselineStart, ct);
        Assert.Equal("interval_table", served.Source);
        var labels = served.Rows.ToDictionary(r => r.DatabaseName, r => r.QueryTextSample);
        Assert.Equal(3, labels.Count);
        Assert.Equal("text from database A", labels[dbs[0]]);
        Assert.Equal("text from database B", labels[dbs[1]]);
        Assert.Equal("SELECT 10", labels[dbs[2]]);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var viewerRows = await viewer.GetQueryStoreRegressionsAsync(ServerId, start, end, dbs, cancellationToken: ct);
        var viewerLabels = viewerRows.ToDictionary(r => r.DatabaseName, r => r.QueryTextSample);
        Assert.Equal(labels, viewerLabels);
    }
}
