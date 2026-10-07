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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5448's merge gate: PLAN_REGRESSION over per-day totals (<see cref="PgFactCollector.PlanRegressionDailySql"/>) returns
/// EXACTLY what <see cref="PgFactCollector.PlanRegressionTableSql"/> returns over the same snapshots, whichever days are
/// built. The store is seeded through the real write path (open and closed snapshots of each interval, non-Regular rows,
/// two replica roles, a forced plan, plans that share a hash, an interval that straddles midnight, a NULL execution
/// count), the days are built with <see cref="PlanRegressionDaily.BuildDaySql"/>, and the rows are compared in order,
/// column for column. The window start is day-aligned, which is the case the two reads are equal for by construction.
///
/// <para>Cases: no day built (the live half alone), every day built, a mixed set, built days the coverage claim does not
/// reach, a late interval that invalidates the days it can touch (the stale day is left out of the built set, as the
/// collector's built-days read leaves it), and the same store after the rebuild. A last case proves the late-interval
/// case is not vacuous: reading the stale day anyway gives a different answer.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionDailyParityLiveTests
{
    private const int ServerId = -5448004;

    /// <summary>Day-aligned, so the window's exact start and M are the same instant.</summary>
    private static readonly DateTime TimeRangeStart = new(2026, 9, 11, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime WindowStart = TimeRangeStart.AddDays(-14);

    private static readonly DateTime T0 = TimeRangeStart.AddDays(-10);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task EveryBuiltSet_EqualsTheTableRead_AndALateIntervalInvalidatesThenRebuilds()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 parity test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await SeedAsync(runner, ct);
        /* A NULL execution count on one interval of 800's cheap plan: SUM skips it on both routes. */
        await ExecAsync(connection,
            "UPDATE collect.query_store_interval_latest SET execution_count = NULL WHERE server_id = @s AND query_id = 800 AND plan_id = 8001 AND first_execution_time = @f",
            ct, ("f", T0.AddDays(2).AddHours(12)));
        /* Coverage below every seeded row, as a store that has filled for longer than the window would have. */
        await ExecAsync(connection, "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @a WHERE server_id = @s",
            ct, ("a", WindowStart.AddDays(-2)));

        var expected = await TableRowsAsync(connection, ct);
        Assert.True(expected.Count >= 5, $"the seed produced {expected.Count} PLAN_REGRESSION row(s); the comparison needs real regressions");

        var allDays = Enumerable.Range(0, 12).Select(i => DateOnly.FromDateTime(T0.AddDays(i))).ToList();

        /* ---- G empty: the daily read's live half alone is the table read ---- */
        Assert.Empty(await BuiltDaysAsync(connection, ct));
        Assert.Equal(expected, await DailyRowsAsync(connection, ct));

        /* ---- all built ---- */
        foreach (var day in allDays) await BuildDayAsync(connection, day, ct);
        var all = await BuiltDaysAsync(connection, ct);
        Assert.Equal(allDays, all);
        Assert.Equal(expected, await DailyRowsAsync(connection, ct));

        /* ---- mixed: four days unbuilt, the rest built ---- */
        await ExecAsync(connection, "DELETE FROM collect.plan_regression_daily_built WHERE server_id = @s AND day IN (@d1, @d2, @d3, @d4)", ct,
            ("d1", allDays[1].ToDateTime(TimeOnly.MinValue)), ("d2", allDays[2].ToDateTime(TimeOnly.MinValue)),
            ("d3", allDays[6].ToDateTime(TimeOnly.MinValue)), ("d4", allDays[9].ToDateTime(TimeOnly.MinValue)));
        var mixed = await BuiltDaysAsync(connection, ct);
        Assert.Equal(allDays.Count - 4, mixed.Count);
        Assert.Equal(expected, await DailyRowsAsync(connection, ct));

        /* ---- built days the coverage claim does not reach are left out, and the read is still equal ---- */
        foreach (var day in allDays) await BuildDayAsync(connection, day, ct);
        await ExecAsync(connection, "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @a WHERE server_id = @s",
            ct, ("a", T0.AddDays(4).AddHours(12)));
        var covered = await BuiltDaysAsync(connection, ct);
        Assert.Equal(allDays.Where(d => d >= DateOnly.FromDateTime(T0.AddDays(5))).ToList(), covered);
        Assert.Equal(expected, await DailyRowsAsync(connection, ct));
        await ExecAsync(connection, "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @a WHERE server_id = @s",
            ct, ("a", WindowStart.AddDays(-2)));

        /* ---- a late interval into a closed day: the trigger's bump (done here by hand, the seed is older than the
           trigger's 16-day reach) leaves the day and the one after it invalid ---- */
        var lateStart = T0.AddDays(2).AddHours(14);
        await WriteAsync(runner, [(1.0, Row("qsA", 100, 1001, 5000, lateStart, lateStart.AddMinutes(30), 4000, 10))], lateStart.AddMinutes(40), ct);
        await ExecAsync(connection, "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @a WHERE server_id = @s",
            ct, ("a", WindowStart.AddDays(-2)));
        await ExecAsync(connection, "UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1 WHERE server_id = @s AND day IN (@d1, @d2)",
            ct, ("d1", allDays[2].ToDateTime(TimeOnly.MinValue)), ("d2", allDays[3].ToDateTime(TimeOnly.MinValue)));

        var afterLate = await TableRowsAsync(connection, ct);
        Assert.NotEqual(expected, afterLate);
        var stale = await BuiltDaysAsync(connection, ct);
        Assert.DoesNotContain(allDays[2], stale);
        Assert.DoesNotContain(allDays[3], stale);
        Assert.Equal(afterLate, await DailyRowsAsync(connection, ct));

        /* Not vacuous: had the stale day been read from its old totals, the answer would be wrong. */
        var withStale = await DailyRowsAsync(connection, ct, stale.Concat([allDays[2]]).OrderBy(d => d).ToList());
        Assert.NotEqual(afterLate, withStale);

        /* ---- rebuilt: valid again, and equal ---- */
        await BuildDayAsync(connection, allDays[2], ct);
        await BuildDayAsync(connection, allDays[3], ct);
        Assert.Equal(allDays, await BuiltDaysAsync(connection, ct));
        Assert.Equal(afterLate, await DailyRowsAsync(connection, ct));

    }

    /* ---------------------------------------------------------------------------------------------------------- */

    private static QueryStoreCollector.Row Row(
        string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
        long cpuUs, string? role = null, string type = "Regular", bool forced = false, string? planHash = null) => new()
        {
            DatabaseName = database,
            QueryId = queryId,
            PlanId = planId,
            ExecutionTypeDesc = type,
            FirstExecutionTime = first,
            LastExecutionTime = last,
            QueryHash = "0xQ" + queryId.ToString(CultureInfo.InvariantCulture),
            QueryPlanHash = planHash ?? "0xP" + planId.ToString(CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = cpuUs,
            AvgDurationUs = cpuUs * 3,
            IsForcedPlan = forced,
            ForceFailureCount = forced ? 2 : 0,
            ReplicaRole = role,
            RuntimeStatsIntervalId = intervalId,
        };

    private static (CollectorContext Context, ServerRuntime Server) Harness()
    {
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "qsil-5448-host",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsil-5448", Host = "qsil-5448-host" },
            ConnectionString = "Server=qsil-5448-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsil-5448-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };
        return (context, server);
    }

    /// <summary>Ships batches of (fraction, row) by fraction, one database at a time, at one collection time per fraction.</summary>
    private static async Task WriteAsync(
        DarlingCollectorRunner runner, IReadOnlyList<(double Fraction, QueryStoreCollector.Row Row)> rows, DateTime closedCollection, CancellationToken ct)
    {
        var (context, server) = Harness();
        foreach (var fraction in rows.Select(r => r.Fraction).Distinct().OrderBy(f => f))
        {
            var batch = rows.Where(r => r.Fraction == fraction).Select(r => r.Row).ToList();
            var collectionTime = fraction >= 1.0 ? closedCollection : closedCollection.AddMinutes(-20);
            foreach (var perDatabase in batch.GroupBy(r => r.DatabaseName))
            {
                await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, perDatabase.ToList(), server, collectionTime, context, ct);
            }
        }
    }

    /// <summary>
    /// Ten days of intervals, each an open snapshot and then its closed one. Query 100 regresses on the NULL role,
    /// 200 on a named one, 300 has one plan, 400 is only ever non-Regular, 500's intervals straddle midnight, 600's
    /// cheap plan is forced, 700's plans 7001 and 7002 share a hash, and 800 has a NULL execution count on one interval.
    /// </summary>
    private static async Task SeedAsync(DarlingCollectorRunner runner, CancellationToken ct)
    {
        var interval = 5000L;
        for (var day = 0; day < 10; day++)
        {
            var start = T0.AddDays(day).AddHours(12);
            interval++;

            var rows = new List<(double Fraction, QueryStoreCollector.Row Row)>();
            void Both(string db, long query, long plan, long execs, long cpu, string? role = null, string type = "Regular", bool forced = false, string? hash = null)
            {
                rows.Add((0.5, Row(db, query, plan, interval, start, start.AddMinutes(25), execs / 2, cpu, role, type, forced, hash)));
                rows.Add((1.0, Row(db, query, plan, interval, start, start.AddMinutes(55), execs, cpu, role, type, forced, hash)));
            }

            if (day is >= 1 and <= 3) Both("qsA", 100, 1001, 300, 100);
            if (day is 8 or 9) Both("qsA", 100, 1002, 20000, 1000);
            if (day == 2) Both("qsB", 200, 2001, 500, 200, role: "secondary1");
            if (day == 9) Both("qsB", 200, 2002, 30000, 900, role: "secondary1");
            Both("qsB", 300, 3001, 1000, 50);
            if (day == 5) Both("qsA", 400, 4001, 90, 70, type: "Aborted");
            if (day is >= 1 and <= 3) Both("qsA", 600, 6001, 400, 60, forced: true);
            if (day is 8 or 9) Both("qsA", 600, 6002, 25000, 800);
            if (day is >= 1 and <= 3) Both("qsA", 700, 7001, 200, 40, hash: "0xPSAME");
            if (day == 5) Both("qsA", 700, 7002, 200, 40, hash: "0xPSAME");
            if (day == 9) Both("qsA", 700, 7003, 30000, 700);
            if (day is >= 1 and <= 3) Both("qsA", 800, 8001, 200, 30);
            if (day is 8 or 9) Both("qsA", 800, 8002, 20000, 600);

            await WriteAsync(runner, rows, start.AddMinutes(65), ct);

            /* 500: an interval that opens at 23:40 and closes at 00:35 the next day, so its first execution is in one
               day and its last in the next. Shipped on its own, collected after it closes. */
            var late = T0.AddDays(day).AddHours(23).AddMinutes(40);
            var straddle = new List<(double Fraction, QueryStoreCollector.Row Row)>();
            if (day is >= 1 and <= 3)
            {
                straddle.Add((0.5, Row("qsA", 500, 5001, interval + 100, late, late.AddMinutes(10), 150, 80)));
                straddle.Add((1.0, Row("qsA", 500, 5001, interval + 100, late, late.AddMinutes(55), 300, 80)));
            }

            if (day is 8 or 9)
            {
                straddle.Add((0.5, Row("qsA", 500, 5002, interval + 100, late, late.AddMinutes(10), 10000, 900)));
                straddle.Add((1.0, Row("qsA", 500, 5002, interval + 100, late, late.AddMinutes(55), 20000, 900)));
            }

            await WriteAsync(runner, straddle, late.AddMinutes(65), ct);
        }
    }

    /* ---- the reads, bound as the collector binds them --------------------------------------------------- */

    private static async Task<List<string>> TableRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PgFactCollector.PlanRegressionTableSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(WindowStart);
        command.Parameters.AddWithValue(TimeRangeStart.AddDays(-15));
        command.Parameters.AddWithValue(TimeRangeStart.AddDays(-15));
        return await RowsAsync(command, ct);
    }

    private static async Task<List<DateOnly>> BuiltDaysAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var days = new List<DateOnly>();
        await using var command = new NpgsqlCommand(PgFactCollector.PlanRegressionBuiltDaysSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionWindowFloor(WindowStart));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) days.Add(reader.GetFieldValue<DateOnly>(0));
        return days;
    }

    private static async Task<List<string>> DailyRowsAsync(NpgsqlConnection connection, CancellationToken ct, List<DateOnly>? days = null)
    {
        days ??= await BuiltDaysAsync(connection, ct);
        var floor = PgFactCollector.PlanRegressionWindowFloor(WindowStart);
        await using var command = new NpgsqlCommand(PgFactCollector.PlanRegressionDailySql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = days.ToArray() });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionLiveFloor(floor, days));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor.AddDays(-1));
        return await RowsAsync(command, ct);
    }

    /// <summary>What the builder does for one day (lane 3's tick, minus its lock and timeout): replace the day's rows, then
    /// stamp the built row at the sequence read before the build.</summary>
    private static async Task BuildDayAsync(NpgsqlConnection connection, DateOnly day, CancellationToken ct)
    {
        var d = day.ToDateTime(TimeOnly.MinValue);
        await ExecAsync(connection, "INSERT INTO collect.plan_regression_daily_built (server_id, day) VALUES (@s, @d) ON CONFLICT DO NOTHING",
            ct, ("d", d));
        long seq;
        await using (var read = new NpgsqlCommand("SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = @s AND day = @d", connection))
        {
            read.Parameters.AddWithValue("s", ServerId);
            read.Parameters.AddWithValue("d", NpgsqlDbType.Date, day);
            seq = (long)(await read.ExecuteScalarAsync(ct))!;
        }

        await ExecAsync(connection, "DELETE FROM collect.plan_regression_daily WHERE server_id = @s AND day = @d", ct, ("d", d));
        await using (var build = new NpgsqlCommand(PlanRegressionDaily.BuildDaySql, connection))
        {
            build.Parameters.AddWithValue(ServerId);
            build.Parameters.AddWithValue(NpgsqlDbType.Date, day);
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, d.AddDays(1));
            build.Parameters.AddWithValue(NpgsqlDbType.Timestamp, d.AddDays(-1));
            await build.ExecuteScalarAsync(ct);
        }

        await ExecAsync(connection, "UPDATE collect.plan_regression_daily_built SET built_seq = @q, built_at = @d, source_rows = 0 WHERE server_id = @s AND day = @d",
            ct, ("d", d), ("q", seq));
    }

    private static async Task<List<string>> RowsAsync(NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i)
                    ? "<null>"
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
            }

            rows.Add(string.Join("|", values));
        }

        return rows;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("s", ServerId);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
