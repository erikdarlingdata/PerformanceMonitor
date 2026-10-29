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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5506: the regressions read takes its baseline's closed days from <c>collect.plan_regression_daily</c> (#5448) and the
/// rest live, and the answer is the same as an all-live weighted read of the same window. Seeded through the real write
/// path over more than 7 days (raw, the wide table and the latest table together), the real builder fills the daily
/// totals, and the facts are: the served rows equal the all-live rows and a hand-worked weighted figure; a day whose
/// built row is stale is read live (its totals are poisoned, so reading them would show); a day with no built row is read
/// live; a valid day IS read from the totals (poisoned, the answer moves); an execution just either side of midnight,
/// of the window start, is counted exactly once; and the output carries no reads fields.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. It shares the gap-cache collection with the other
   regressions table tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsDailyBaselineLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsDaily";

    /// <summary>#6781: every interval's CPU and duration are the seeded microseconds times this, so each regressing query's rise
    /// adds at least 1 s of CPU over the window (<see cref="PerformanceMonitor.Common.QueryStoreRegressionRules.CostMinAddedCpuMs"/>).
    /// The hand-worked figures below scale by it too.</summary>
    private const long CpuScale = 1_000;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The re-collected interval of the extra fixture (query 41): its identity, so the second snapshot upserts the first.</summary>
    private const long RecollectedIntervalId = 7900;

    /// <summary>The recollected interval of the recent-days fixture (query 53): its identity, so the second snapshot upserts the first.</summary>
    private const long RecentRecollectedIntervalId = 8300;

    /// <summary>The recent window the recent-days tests read (#5506): 06:00 four days ago, so the whole days three and two
    /// days ago (and yesterday, once the builder's watermark passed today's midnight) lie inside it.</summary>
    private static DateTime RecentStart(DateTime today) => today.AddDays(-4).AddHours(6);

    private static async Task SeedAsync(NpgsqlDataSource postgres, DateTime day0, DateTime start, CancellationToken ct, bool extra = false, bool recent = false)
    {
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsdaily-host", CollectionTime = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsdaily", Host = "qsdaily-host" },
            ConnectionString = "Server=qsdaily-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsdaily-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 7000;
        /* One interval, written once, last execution 30 minutes after its first. Its day, for the daily totals and for the
           read's split, is the day of that last execution. */
        async Task IntervalAsync(
            DateTime first, long queryId, long planId, long executions, long cpuUs, long? reuseIntervalId = null, int lastMinutes = 30,
            string? text = null, string? replicaRole = null, DateTime? collectedAt = null)
        {
            if (reuseIntervalId is null)
            {
                intervalId++;
            }

            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = planId,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(lastMinutes),
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + planId.ToString("X8", CultureInfo.InvariantCulture),
                QueryText = text ?? "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ReplicaRole = replicaRole,
                ExecutionCount = executions,
                AvgCpuTimeUs = cpuUs * CpuScale,
                AvgDurationUs = cpuUs * 2 * CpuScale,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = reuseIntervalId ?? intervalId,
                IntervalStartTimeUtc = first,
            };
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, collectedAt ?? first.AddMinutes(lastMinutes + 10), context, ct);
        }

        /* The table's floor: an interval before the baseline, outside both arms, so the gate's #5541 edge holds. */
        await IntervalAsync(day0.AddDays(-3).AddHours(10), 30, 301, 5, 1_000);

        /* Query 10, the regressing one. Day names are day0 + n, so D-7 is day0 + 1. The baseline is [start - 7 days, start). */
        await IntervalAsync(day0.AddDays(1).AddHours(-0.33), 10, 101, 5, 4_000);      /* first D-8 23:40, last D-7 00:10: the leading seam, built day D-7 */
        await IntervalAsync(day0.AddDays(2).AddHours(10), 10, 101, 100, 1_000);       /* D-6 10:00 */
        await IntervalAsync(day0.AddDays(4).AddHours(-0.33), 10, 101, 1, 9_000);      /* first D-5 23:40, last D-4 00:10: a seam inside the run, day D-4 */
        await IntervalAsync(day0.AddDays(5).AddHours(10), 10, 101, 10, 2_000);        /* D-3 10:00 */
        await IntervalAsync(day0.AddDays(6).AddHours(-0.33), 10, 101, 7, 3_000);      /* first D-3 23:40, last D-2 00:10: the trailing seam, on the last run day D-2, so the daily totals serve it */
        await IntervalAsync(start.AddMinutes(-50), 10, 101, 20, 1_500);               /* last = start - 20 min: baseline, live */
        await IntervalAsync(start.AddMinutes(-10), 10, 102, 50, 5_000);               /* last = start + 20 min: recent, never baseline */
        await IntervalAsync(start.AddHours(2), 10, 102, 10, 4_000);                   /* recent */
        /* Query 20 does not regress. */
        foreach (var hours in new[] { 24.0 * 2 + 10, 24.0 * 5 + 10, 24.0 * 7 + 3, 24.0 * 7 + 9 })
        {
            await IntervalAsync(day0.AddHours(hours), 20, 201, 8, 3_000);
        }

        if (extra)
        {
            /* Query 40, two plans in the baseline: plan 401 on D-6 (a built day) and plan 402 just before the window start (read
               live), one plan recently. Its CPU triples recently, so it is listed. */
            await IntervalAsync(day0.AddDays(2).AddHours(11), 40, 401, 20, 1_000);
            await IntervalAsync(start.AddMinutes(-50), 40, 402, 20, 1_000);
            await IntervalAsync(start.AddHours(3), 40, 401, 20, 3_000);

            /* Query 41, an interval re-collected with a growing count after the day was built, and its last execution crossing
               midnight: first D-5 23:00, first snapshot last D-5 23:30 with 10 executions; the second (written after the build,
               see RecollectAsync) last D-4 00:30 with 25. */
            await IntervalAsync(day0.AddDays(3).AddHours(23), 41, 411, 10, 1_000, RecollectedIntervalId);
            await IntervalAsync(start.AddHours(4), 41, 411, 10, 5_000);

            /* Query 10 again, a seam interval on the open day after the run: first D-2 23:40, last D-1 00:10. D-1 is no run day
               and its last execution is before the window start (D-1 06:00), so the live baseline serves it, from the high
               range. Its first execution is below D-1's midnight, so only a high range start below the next midnight reaches it
               (#5506): the latest form's raised bound, the next midnight minus the edge margin. The shared seed has no such row. */
            await IntervalAsync(day0.AddDays(7).AddHours(-0.33), 10, 101, 4, 6_000);
        }

        if (recent)
        {
            /* #5506, the recent window's seams. T is today, T-n its days; the recent window starts at T-4 06:00
               (RecentStart), so T-3 and T-2 are whole recent days. Every query has a cheap baseline interval on T-6 and dearer
               recent ones, so each is a cost row and its recent figures are in the output. */
            var t = day0.AddDays(8);
            var s2 = RecentStart(t);
            DateTime T(int days, double hours) => t.AddDays(days).AddHours(hours);

            /* Query 50: every seam of the recent split. */
            await IntervalAsync(T(-6, 10), 50, 501, 8, 1_000);
            await IntervalAsync(s2.AddMinutes(-30), 50, 501, 5, 3_000);                 /* last = the window start exactly: recent, live low range */
            await IntervalAsync(T(-3, 23 + 29 / 60.0), 50, 501, 7, 3_000);              /* last T-3 23:59, a recent day */
            await IntervalAsync(T(-4, 12), 50, 501, 3, 3_000);                          /* the partial day the window starts in, live */
            await IntervalAsync(T(-4, 23 + 40 / 60.0), 50, 501, 4, 3_500);              /* first T-4 23:40, last T-3 00:10: into the first recent day */
            await IntervalAsync(T(-3, 23 + 40 / 60.0), 50, 501, 6, 3_200);              /* first T-3 23:40, last T-2 00:10: inside the run */
            await IntervalAsync(T(-2, 23 + 40 / 60.0), 50, 501, 2, 3_100);              /* first T-2 23:40, last T-1 00:10: out of the run's last day */

            /* Query 51: two plans, one on a recent day and one live. The recent day's row carries the larger text, so the
               sample is right only if the recent days' text is read. */
            await IntervalAsync(T(-6, 11), 51, 511, 8, 1_000);
            await IntervalAsync(T(-2, 10), 51, 511, 9, 4_000, text: "SELECT 51 zz");
            await IntervalAsync(T(-4, 12), 51, 512, 3, 4_000);

            /* Query 52: one plan on a recent day under two replica roles, so two daily rows for one plan. All its recent rows
               are on a recent day: its text sample comes from the recent days' probe alone. */
            await IntervalAsync(T(-6, 12), 52, 521, 8, 1_000);
            await IntervalAsync(T(-3, 10), 52, 521, 5, 5_000, replicaRole: "PRIMARY");
            await IntervalAsync(T(-3, 10), 52, 521, 6, 6_000, replicaRole: "SECONDARY");

            /* Query 53: an interval on T-3 that RecollectRecentAsync moves across midnight after the build. */
            await IntervalAsync(T(-6, 13), 53, 531, 8, 1_000);
            await IntervalAsync(T(-3, 23), 53, 531, 10, 3_000, RecentRecollectedIntervalId);

            /* Query 54: a row of T-3 collected before its last execution, at T-4 02:00. A baseline start between T-4 00:00 and
               that collection keeps it out of the live read and in T-3's totals: the strict rule (D - 1 day >= the baseline
               start) is what keeps T-3 live then. */
            await IntervalAsync(T(-4, 4), 54, 541, 10, 1_000);
            await IntervalAsync(T(-4, 7), 54, 541, 2, 3_000);
            await IntervalAsync(T(-4, 1), 54, 541, 6, 3_000, lastMinutes: 1410, collectedAt: T(-4, 2));

            /* Query 55: a row of T-2 collected at T-1 02:00. A window ending at T-1 01:00 does not read it, and T-2's totals hold it. */
            await IntervalAsync(T(-6, 14), 55, 551, 8, 1_000);
            await IntervalAsync(T(-3, 10), 55, 551, 4, 3_000);
            await IntervalAsync(T(-2, 20), 55, 551, 5, 3_000, collectedAt: T(-1, 2));
        }
    }

    /// <summary>The second snapshot of query 53's interval: the same identity, a larger count, a last execution after midnight.</summary>
    private static async Task RecollectRecentAsync(NpgsqlDataSource postgres, DateTime today, CancellationToken ct)
    {
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsdaily-host", CollectionTime = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsdaily", Host = "qsdaily-host" },
            ConnectionString = "Server=qsdaily-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsdaily-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };
        var first = today.AddDays(-3).AddHours(23);
        var row = new QueryStoreCollector.Row
        {
            DatabaseName = Db,
            QueryId = 53,
            PlanId = 531,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = first,
            LastExecutionTime = first.AddMinutes(90),
            QueryHash = "0x" + 53.ToString("X8", CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + 531.ToString("X8", CultureInfo.InvariantCulture),
            QueryText = "SELECT 53",
            ExecutionCount = 25,
            AvgCpuTimeUs = 3_000 * CpuScale,
            AvgDurationUs = 6_000 * CpuScale,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = RecentRecollectedIntervalId,
            IntervalStartTimeUtc = first,
        };
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(100), context, ct);
    }

    /// <summary>The second snapshot of query 41's interval: the same identity, a larger count, a last execution after midnight.</summary>
    private static async Task RecollectAsync(NpgsqlDataSource postgres, DateTime day0, CancellationToken ct)
    {
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsdaily-host", CollectionTime = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsdaily", Host = "qsdaily-host" },
            ConnectionString = "Server=qsdaily-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsdaily-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };
        var first = day0.AddDays(3).AddHours(23);
        var row = new QueryStoreCollector.Row
        {
            DatabaseName = Db,
            QueryId = 41,
            PlanId = 411,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = first,
            LastExecutionTime = first.AddMinutes(90),
            QueryHash = "0x" + 41.ToString("X8", CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + 411.ToString("X8", CultureInfo.InvariantCulture),
            QueryText = "SELECT 41",
            ExecutionCount = 25,
            AvgCpuTimeUs = 1_000 * CpuScale,
            AvgDurationUs = 2_000 * CpuScale,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = RecollectedIntervalId,
            IntervalStartTimeUtc = first,
        };
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(100), context, ct);
    }

    private static string Describe(DarlingQueryStoreRegressionReader.RegressionRow r) => string.Join("|",
        r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
        Math.Round(r.RecentCpuMs, 6), Math.Round(r.AdditionalDurationMs, 6), r.BaselineExecCount, r.RecentExecCount,
        r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime);

    /// <summary>One form of the statement run by hand with a given plan: the all-live reference is <see cref="QueryStoreRegressionBaseline.AllLive"/>.</summary>
    private static async Task<List<string>> RunAsync(
        NpgsqlConnection connection, string sql, DateTime start, DateTime end, DateTime baselineStart, QueryStoreRegressionBaseline.Plan plan, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
        command.Parameters.Add(DatabaseFilter.One(Db).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 50 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
        command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
        /* #6750: the start of the hours held, the row filters' start here. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        /* #6750: the failure arm's row-filter start and its hours held, the same on this one-tier read. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(Describe(new DarlingQueryStoreRegressionReader.RegressionRow(
                reader.GetString(0), reader.GetInt64(1), reader.GetDouble(2), reader.GetDouble(3), null, reader.GetDouble(5), reader.GetDouble(6),
                null, reader.GetDouble(8), reader.GetInt64(9), reader.GetInt64(10), reader.GetInt32(11), reader.GetInt32(12),
                reader.GetString(13), reader.GetString(14), reader.IsDBNull(15) ? null : reader.GetDateTime(15))));
        }

        return rows;
    }

    private static async Task<QueryStoreRegressionBaseline.Plan> ResolveAsync(
        NpgsqlDataSource postgres, DateTime baselineStart, DateTime start, CancellationToken ct, DateTime? end = null)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        return await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, baselineStart, start, 60, null, ct, windowEnd: end);
    }

    /// <summary>
    /// The MCP statement for <paramref name="plan"/> (<see cref="DarlingQueryStoreRegressionReader.TableSqlFor"/>) run by hand,
    /// every column of every row in order, each value in a round-trip text form, so two forms compare byte for byte. The recent
    /// days' three parameters ($11 to $13) are bound only on the form that names them.
    /// </summary>
    internal static async Task<List<string>> RunFullAsync(
        NpgsqlConnection connection, int serverId, string database, DateTime start, DateTime end, DateTime baselineStart,
        QueryStoreRegressionBaseline.Plan plan, CancellationToken ct)
    {
        var sql = DarlingQueryStoreRegressionReader.TableSqlFor(plan);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
        command.Parameters.Add(DatabaseFilter.One(database).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 5000 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
        command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
        /* #6750: the start of the hours held, the row filters' start here. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        /* #6750: the failure arm's row-filter start and its hours held, the same on this one-tier read. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        if (QueryStoreRegressionBaseline.ReadsRecentDays(plan))
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RecentDays });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.RecentLowEnd });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.RecentHighStart });
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? "<null>" : reader.GetValue(i) switch
                {
                    double d => d.ToString("R", CultureInfo.InvariantCulture),
                    DateTime t => t.ToString("O", CultureInfo.InvariantCulture),
                    IFormattable x => x.ToString(null, CultureInfo.InvariantCulture),
                    var v => v.ToString() ?? "",
                };
            }

            rows.Add(string.Join("|", values));
        }

        return rows;
    }

    /// <summary>The rows of the recent-days plan and of the same plan with no recent day: they must be the same, column for column.</summary>
    private static async Task<(List<string> WithRecent, List<string> AllLiveRecent)> BothRecentFormsAsync(
        NpgsqlConnection connection, DateTime start, DateTime end, DateTime baselineStart, QueryStoreRegressionBaseline.Plan plan, CancellationToken ct) =>
        (await RunFullAsync(connection, ServerId, Db, start, end, baselineStart, plan, ct),
         await RunFullAsync(connection, ServerId, Db, start, end, baselineStart, plan with { RecentDays = [] }, ct));

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static string DayText(DateTime d) => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheBaselineFromTheDailyTotals_IsTheAllLiveWeightedBaseline_AtEverySeam()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsdaily', 'qsdaily', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        /* The window starts at 06:00 yesterday, so at any hour of the day the baseline holds D-7 through D-2 whole, and its
           edges (D-8, D-1 up to 06:00) are partial days. day0 is D-8's midnight. */
        var today = LiveClock.Now().Date;
        var start = today.AddDays(-1).AddHours(6);
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        var day0 = today.AddDays(-8);
        await SeedAsync(postgres, day0, start, ct);
        var end = LiveClock.Now();
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);
        await ExecAsync(connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{baselineStart.AddDays(-3):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);
        Assert.Equal(1L, await ScalarAsync(connection, $"SELECT count(*) FROM collect.query_store_interval_latest_coverage WHERE server_id = {ServerId}", ct));

        /* The real builder fills the daily totals. */
        var built = await PlanRegressionDaily.RunTickAsync(postgres, LiveClock.Now(), NullLogger.Instance, ct);
        Assert.True(built.Built > 0, "the builder built no day");

        string Day(int offset) => DayText(today.AddDays(offset));
        var wholeDays = Enumerable.Range(-7, 6).Select(o => DateOnly.FromDateTime(today.AddDays(o))).ToArray();

        /* The reference: the same window read all live, from the wide table and from raw. Hand-worked for query 10 (Regular,
           weighted per execution): baseline 143 executions and 200,000 us of CPU = 1.398601 ms, so the day-seam intervals
           (first 23:40, last 00:10) count once each, in the day of their last execution. Recent 60 executions, (50 x 5000 + 10 x
           4000) / 60 = 4.833333 ms: the interval that began 10 minutes before the window start ended inside it, so it is recent
           and not baseline. */
        var allLive = QueryStoreRegressionBaseline.AllLive(start);
        var reference = await RunAsync(connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false), start, end, baselineStart, allLive, ct);
        var rawReference = await RunAsync(connection, DarlingQueryStoreRegressionReader.BuildSql(wide: false, useDailyTotals: false), start, end, baselineStart, allLive, ct);
        Assert.Equal(reference, rawReference);
        var only = Assert.Single(reference);
        var fields = only.Split('|');
        Assert.Equal("10", fields[1]);
        Assert.Equal(200_000.0 * CpuScale / 143 / 1000, double.Parse(fields[4], CultureInfo.InvariantCulture), 5);
        Assert.Equal(143L, long.Parse(fields[7], CultureInfo.InvariantCulture));
        Assert.Equal(60L, long.Parse(fields[8], CultureInfo.InvariantCulture));
        Assert.Equal(290_000.0 * CpuScale / 60 / 1000, double.Parse(fields[5], CultureInfo.InvariantCulture), 5);

        async Task<List<string>> ServedAsync()
        {
            var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(postgres, ServerId, start, end, DatabaseFilter.One(Db), 50, baselineStart, ct);
            return served.Rows.Select(Describe).ToList();
        }

        /* 1. Fresh: D-7 through D-2 come from the daily totals, and the served rows are the all-live rows. */
        var plan = await ResolveAsync(postgres, baselineStart, start, ct);
        Assert.Equal(wholeDays, plan.RunDays);
        Assert.Equal(reference, await ServedAsync());
        var fromTotals = await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, plan, ct);
        Assert.Equal(reference, fromTotals);
        var fromTotalsRaw = await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, plan, ct);
        Assert.Equal(reference, fromTotalsRaw);

        /* 2. A stale built day is read live. Its totals are poisoned and its late_seq has moved, so reading them would show. */
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 5 WHERE server_id = {ServerId} AND day = DATE '{Day(-6)}'", ct);
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1 WHERE server_id = {ServerId} AND day = DATE '{Day(-6)}'", ct);
        plan = await ResolveAsync(postgres, baselineStart, start, ct);
        Assert.DoesNotContain(DateOnly.FromDateTime(today.AddDays(-6)), plan.RunDays);
        Assert.Equal(reference, await ServedAsync());
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, plan, ct));
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, plan, ct));

        /* 3. A day with no built row is read live: the row of D-4, the day that holds the seam interval, is gone. */
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily_built WHERE server_id = {ServerId} AND day = DATE '{Day(-4)}'", ct);
        await ExecAsync(connection, $"DELETE FROM collect.plan_regression_daily WHERE server_id = {ServerId} AND day = DATE '{Day(-4)}'", ct);
        plan = await ResolveAsync(postgres, baselineStart, start, ct);
        Assert.DoesNotContain(DateOnly.FromDateTime(today.AddDays(-4)), plan.RunDays);
        Assert.Equal(reference, await ServedAsync());
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql, start, end, baselineStart, plan, ct));
        Assert.Equal(reference, await RunAsync(connection, DarlingQueryStoreRegressionReader.QueryStoreRegressionsSql, start, end, baselineStart, plan, ct));

        /* 4. A VALID day is read from the totals: poison one and the answer moves, so the built table is what is read. */
        var valid = Assert.Single(plan.RunDays, d => d == DateOnly.FromDateTime(today.AddDays(-3)));
        await ExecAsync(connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 5 WHERE server_id = {ServerId} AND day = DATE '{valid:yyyy-MM-dd}'", ct);
        var poisoned = await ServedAsync();
        Assert.NotEqual(reference, poisoned);
        /* The executions did not move, only the cost the poisoned day carried. */
        Assert.Equal(143L, long.Parse(poisoned[0].Split('|')[7], CultureInfo.InvariantCulture));
    }

    /// <summary>A store with the window's rows seeded and the daily totals built (the fixture of the first test, optionally with
    /// the extra queries), ready for a read. The scratch database is the caller's to dispose.</summary>
    private sealed class Fixture : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required DateTime Today { get; init; }
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

    private static async Task<Fixture> BuildFixtureAsync(bool extra, bool timescale, CancellationToken ct, bool recent = false)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (timescale)
        {
            Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for the raw floor test");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsdaily', 'qsdaily', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);
        var today = LiveClock.Now().Date;
        var start = today.AddDays(-1).AddHours(6);
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        await SeedAsync(postgres, today.AddDays(-8), start, ct, extra, recent);
        var end = LiveClock.Now();
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);
        await ExecAsync(connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{baselineStart.AddDays(-3):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);
        var built = await PlanRegressionDaily.RunTickAsync(postgres, LiveClock.Now(), NullLogger.Instance, ct);
        Assert.True(built.Built > 0, "the builder built no day");
        return new Fixture { Scratch = scratch, Connection = connection, Postgres = postgres, Today = today, Start = start, End = end, BaselineStart = baselineStart };
    }

    private static async Task<List<string>> AllLiveAsync(Fixture f, CancellationToken ct) =>
        await RunAsync(f.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false), f.Start, f.End, f.BaselineStart, QueryStoreRegressionBaseline.AllLive(f.Start), ct);

    private static async Task<List<string>> ServedRowsAsync(Fixture f, CancellationToken ct) =>
        (await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(f.Postgres, ServerId, f.Start, f.End, DatabaseFilter.One(Db), 50, f.BaselineStart, ct)).Rows.Select(Describe).ToList();

    /// <summary>
    /// Review of #6260 (F1, #5506): a day reads valid when its build saw every late row, but a late batch whose apply into
    /// <c>query_store_interval_latest</c> failed is pending, and its trigger bump rolled back with the failed apply. The wide
    /// table holds those executions, the totals do not. With a pending batch for the server no built day is used, the way
    /// PLAN_REGRESSION gates its own built days.
    /// </summary>
    [Fact]
    public async Task WithAPendingLatestBatchForTheServer_NoBuiltDayIsUsed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: false, timescale: false, ct);
        var reference = await AllLiveAsync(f, ct);
        Assert.Single(reference);

        /* The built days would be used, and a poisoned total shows it. */
        var plan = await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct);
        Assert.NotEmpty(plan.RunDays);
        var day = plan.RunDays[^1];
        await ExecAsync(f.Connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 5 WHERE server_id = {ServerId} AND day = DATE '{day:yyyy-MM-dd}'", ct);
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));

        /* A pending batch for the server: the baseline is read live, the poisoned total is not read, the answer is the all-live one. */
        await ExecAsync(f.Connection, $"INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES ({ServerId}, TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', '{Db}', TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', 'x')", ct);
        plan = await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct);
        Assert.Empty(plan.RunDays);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
    }

    /// <summary>
    /// Review of #6260 (F2, #5506): the raw arm's live rows start at raw's oldest chunk, and the tool's <c>effective_start</c>
    /// says nothing older was read. A whole valid day below that floor must not come from the totals. With the clamp the run
    /// days all start at or after the floor; without it (the wide arm's call) the older days are taken.
    /// </summary>
    [Fact]
    public async Task OnTheRawArm_DaysBelowTheRawChunkFloor_AreNotTakenFromTheTotals()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: false, timescale: true, ct);

        /* Raw loses its oldest chunks: the floor now sits inside the baseline, at about D-3. */
        await ExecAsync(f.Connection, $"SELECT drop_chunks('collect.query_store_stats', older_than => TIMESTAMP '{f.Today.AddDays(-3):yyyy-MM-dd HH:mm:ss}')", ct);
        var rawFloor = (DateTime)(await ScalarAsync(f.Connection,
            "SELECT MIN(ch.range_start) AT TIME ZONE 'UTC' FROM timescaledb_information.chunks AS ch WHERE ch.hypertable_schema = 'collect' AND ch.hypertable_name = 'query_store_stats'", ct))!;
        Assert.True(rawFloor > f.BaselineStart.AddDays(2), "the floor must sit inside the baseline for this test to mean anything");

        async Task<QueryStoreRegressionBaseline.Plan> PlanAsync(bool clamp)
        {
            await using var connection = await f.Postgres.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
            return await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, f.BaselineStart, f.Start, 60, null, ct, clampToRawFloor: clamp);
        }

        var unclamped = await PlanAsync(clamp: false);
        Assert.Contains(DateOnly.FromDateTime(f.Today.AddDays(-7)), unclamped.RunDays);

        var clamped = await PlanAsync(clamp: true);
        Assert.NotEmpty(clamped.RunDays);
        Assert.All(clamped.RunDays, day => Assert.True(day.ToDateTime(TimeOnly.MinValue) >= rawFloor, $"{day} is below the raw floor {rawFloor:o}"));
        Assert.DoesNotContain(DateOnly.FromDateTime(f.Today.AddDays(-7)), clamped.RunDays);
    }

    /// <summary>
    /// A query with more than one plan in the baseline (one on a built day, one read live) counts both, and an interval
    /// re-collected with a growing count whose last execution crossed midnight after the build is counted once, in the day of
    /// its latest snapshot, whatever the totals held for it. Both equal the all-live read of the same window.
    /// </summary>
    [Fact]
    public async Task MorePlansAcrossTheBuiltAndLiveParts_AndAnIntervalRecollectedAcrossMidnight_MatchTheAllLiveRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: true, timescale: false, ct);
        await RecollectAsync(f.Postgres, f.Today.AddDays(-8), ct);

        var reference = await AllLiveAsync(f, ct);
        var served = await ServedRowsAsync(f, ct);
        Assert.Equal(reference, served);

        string[] Row(List<string> rows, long queryId) => rows.Single(r => r.StartsWith(Db + "|" + queryId.ToString(CultureInfo.InvariantCulture) + "|", StringComparison.Ordinal)).Split('|');

        /* Query 40: plan 401 on a built day and plan 402 live give 2 plans in the baseline and 1 recent. Describe fields:
           7 baseline executions, 9 plans. */
        var q40 = Row(served, 40);
        Assert.Equal("40", q40[1]);
        Assert.Equal(40L, long.Parse(q40[7], CultureInfo.InvariantCulture));
        Assert.Equal(2, int.Parse(q40[9], CultureInfo.InvariantCulture));
        Assert.Equal(1, int.Parse(q40[10], CultureInfo.InvariantCulture));

        /* Query 41: the interval counts 25 executions, once. */
        var q41 = Row(served, 41);
        Assert.Equal(25L, long.Parse(q41[7], CultureInfo.InvariantCulture));
        Assert.Equal(10L, long.Parse(q41[8], CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// #5506: where latest's coverage holds the whole read and no batch is pending, the table read takes its three Regular arms
    /// from <c>query_store_interval_latest</c>, and the rows equal the wide read's. A poisoned latest row proves which table
    /// was read: the served rows move, in the MCP read and in the viewer's grid. A pending latest batch, or latest's coverage
    /// starting after the baseline start, sends the arms back to the wide table, and the poisoned row is not read.
    /// </summary>
    [Fact]
    public async Task TheRegularArms_ReadLatestWhereItsCoverageHolds_AndFallBackToWideOnAPendingBatchOrLateCoverage()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: true, timescale: false, ct);
        var reference = await AllLiveAsync(f, ct);
        Assert.NotEmpty(reference);

        /* A measured 60-minute interval, so the reads take the three-hour margin and the latest form raises its high range
           start to the last run day's next midnight (D-1) minus three hours (D-2 21:00). The extra seed's query 10 interval
           (first D-2 23:40, last D-1 00:10) is read only from the high live range, and its first execution is above that raised
           bound and below D-1's midnight: it must still be counted, and a bound at or above the midnight loses it. */
        await ExecAsync(f.Connection, $"""
            INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
            SELECT row_number() OVER (), c.c, {ServerId}, 'qsdaily', '{Db}', 60
            FROM generate_series(TIMESTAMP '{f.BaselineStart.AddDays(-5):yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{f.End:yyyy-MM-dd HH:mm:ss}', interval '6 hours') AS c(c)
            """, ct);
        Assert.Equal(TimeSpan.FromHours(3), await QueryStoreIntervalWide.ResolveEdgeMarginAsync(f.Connection, ServerId, f.BaselineStart, f.End, 60, ct));

        async Task<List<string>> ViewerRowsAsync()
        {
            await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(f.Scratch.ConnectionString);
            var rows = await viewer.TryGetQueryStoreRegressionsFromTableAsync(ServerId, f.Start, f.End, null, [Db], f.BaselineStart, null, ct);
            Assert.NotNull(rows);
            return rows.Select(r => string.Join("|",
                r.DatabaseName, r.QueryId, Math.Round(r.BaselineDurationMs, 6), Math.Round(r.RecentDurationMs, 6), Math.Round(r.BaselineCpuMs, 6),
                Math.Round(r.RecentCpuMs, 6), Math.Round(r.AdditionalDurationMs, 6), r.BaselineExecCount, r.RecentExecCount,
                r.BaselinePlanCount, r.RecentPlanCount, r.Severity, r.QueryTextSample, r.LastExecutionTime)).ToList();
        }

        /* The gate allows latest, and both forms of the statement give the same rows, with the run days and all live. */
        var plan = await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct);
        Assert.True(plan.RegularFromLatest);
        Assert.NotEmpty(plan.RunDays);

        /* #5506: a wider window (from T-4 06:00) holds whole recent days, and the gate takes them here. Each fallback below
           leaves none. */
        var recentStart = RecentStart(f.Today);
        Task<QueryStoreRegressionBaseline.Plan> RecentPlanAsync() => ResolveAsync(f.Postgres, f.BaselineStart, recentStart, ct, f.End);
        Assert.NotEmpty((await RecentPlanAsync()).RecentDays);

        /* The raised bound is D-1's midnight minus the margin, and the seam interval's first execution (D-2 23:40) lies between
           it and the midnight. The latest form read with that bound gives the all-live rows. The same read with the bound at the
           next midnight (no margin) loses the interval, so the rows move: the margin term is what keeps it. */
        var latestSql = DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: true, regularFromLatest: true);
        var nextMidnight = f.Today.AddDays(-1);
        var raised = QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, TimeSpan.FromHours(3));
        Assert.Equal(nextMidnight.AddHours(-3), raised.HighStart);
        Assert.Equal(reference, await RunAsync(f.Connection, latestSql, f.Start, f.End, f.BaselineStart, raised, ct));
        Assert.NotEqual(reference, await RunAsync(f.Connection, latestSql, f.Start, f.End, f.BaselineStart, plan with { HighStart = nextMidnight }, ct));
        Assert.Equal(reference, await RunAsync(f.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: true, regularFromLatest: true), f.Start, f.End, f.BaselineStart, plan, ct));
        Assert.Equal(reference, await RunAsync(f.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false, regularFromLatest: true), f.Start, f.End, f.BaselineStart, QueryStoreRegressionBaseline.AllLive(f.Start), ct));
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
        var viewerReference = await ViewerRowsAsync();
        Assert.Equal(reference, viewerReference);

        /* Poison the seam interval in latest only. Only the baseline's high live arm reads it (its last execution is before the
           window start), so the served rows move if that arm reads latest. Then restore it. */
        var seamPoison = $"server_id = {ServerId} AND query_id = 10 AND first_execution_time < TIMESTAMP '{nextMidnight:yyyy-MM-dd HH:mm:ss}' AND first_execution_time >= TIMESTAMP '{nextMidnight.AddHours(-1):yyyy-MM-dd HH:mm:ss}'";
        Assert.Equal(1L, await ScalarAsync(f.Connection, $"SELECT count(*) FROM collect.query_store_interval_latest WHERE {seamPoison}", ct));
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest SET avg_cpu_time_us = avg_cpu_time_us * 5 WHERE {seamPoison}", ct);
        Assert.Equal(reference, await AllLiveAsync(f, ct));
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));
        Assert.NotEqual(viewerReference, await ViewerRowsAsync());
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest SET avg_cpu_time_us = avg_cpu_time_us / 5 WHERE {seamPoison}", ct);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));

        /* Poison the recent intervals in latest only: the served rows move, so the recent arm reads latest too. */
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest SET avg_cpu_time_us = avg_cpu_time_us * 5 WHERE server_id = {ServerId} AND last_execution_time >= TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}'", ct);
        Assert.Equal(reference, await AllLiveAsync(f, ct));
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));
        Assert.NotEqual(viewerReference, await ViewerRowsAsync());

        /* Latest's coverage starts after the baseline start: the arms read wide, and the rows are the wide rows again. */
        var filledSince = (DateTime)(await ScalarAsync(f.Connection, $"SELECT filled_since FROM collect.query_store_interval_latest_coverage WHERE server_id = {ServerId}", ct))!;
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{f.BaselineStart.AddHours(1):yyyy-MM-dd HH:mm:ss}' WHERE server_id = {ServerId}", ct);
        Assert.False((await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct)).RegularFromLatest);
        Assert.Empty((await RecentPlanAsync()).RecentDays);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
        Assert.Equal(viewerReference, await ViewerRowsAsync());

        /* Coverage restored, then a pending latest batch: the arms read wide again. */
        await ExecAsync(f.Connection, $"UPDATE collect.query_store_interval_latest_coverage SET filled_since = TIMESTAMP '{filledSince:yyyy-MM-dd HH:mm:ss.ffffff}' WHERE server_id = {ServerId}", ct);
        Assert.True((await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct)).RegularFromLatest);
        Assert.NotEmpty((await RecentPlanAsync()).RecentDays);
        Assert.NotEqual(reference, await ServedRowsAsync(f, ct));
        await ExecAsync(f.Connection, $"INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES ({ServerId}, TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', '{Db}', TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', 'x')", ct);
        Assert.False((await ResolveAsync(f.Postgres, f.BaselineStart, f.Start, ct)).RegularFromLatest);
        Assert.Empty((await RecentPlanAsync()).RecentDays);
        Assert.Equal(reference, await ServedRowsAsync(f, ct));
        Assert.Equal(viewerReference, await ViewerRowsAsync());

        /* The raw arm never takes recent days, whatever the gate says. */
        await ExecAsync(f.Connection, $"DELETE FROM collect.query_store_interval_latest_pending WHERE server_id = {ServerId}", ct);
        await using (var connection = await f.Postgres.OpenConnectionAsync(ct))
        await using (var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct))
        {
            var raw = await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, f.BaselineStart, recentStart, 60, null, ct, clampToRawFloor: true, windowEnd: f.End);
            Assert.Empty(raw.RecentDays);
        }

        Assert.NotEmpty((await RecentPlanAsync()).RecentDays);
    }

    /// <summary>
    /// #5506: the recent window's whole built days read from <c>plan_regression_daily</c>, and its edges live, give exactly
    /// the rows of the same plan with the whole recent window read live: every column, in order. Seeded at every seam of the
    /// split (an interval ending exactly at the window start, one at 23:59 on a recent day, intervals crossing midnight into,
    /// inside and out of the run, the partial day the window starts in, two plans across a recent day and a live one, and two
    /// daily rows for one plan under two replica roles), with the latest rows' text where the text table has none. The MCP read
    /// takes the days too: a poisoned recent day moves its rows.
    /// </summary>
    [Fact]
    public async Task TheRecentWindowFromTheDailyTotals_IsTheAllLiveRecent_AtEverySeam()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: false, timescale: false, ct, recent: true);
        var start = RecentStart(f.Today);
        /* A baseline start the interval table's gate accepts for the MCP read (its floor is T-11 10:00, plus the 26-hour edge). */
        var baselineStart = f.Today.AddDays(-10).AddHours(12);
        var t3 = DateOnly.FromDateTime(f.Today.AddDays(-3));
        var t2 = DateOnly.FromDateTime(f.Today.AddDays(-2));

        var plan = await ResolveAsync(f.Postgres, baselineStart, start, ct, f.End);
        Assert.True(plan.RegularFromLatest);
        Assert.Contains(t3, plan.RecentDays);
        Assert.Contains(t2, plan.RecentDays);
        Assert.Equal(f.Today.AddDays(-3), plan.RecentLowEnd);

        var (withRecent, allLiveRecent) = await BothRecentFormsAsync(f.Connection, start, f.End, baselineStart, plan, ct);
        Assert.Equal(allLiveRecent, withRecent);
        /* The all-live form is also today's latest statement and the wide one. */
        Assert.Equal(allLiveRecent, await RunFullAsync(f.Connection, ServerId, Db, start, f.End, baselineStart, plan with { RecentDays = [], RegularFromLatest = false }, ct));

        string[] Row(List<string> rows, long queryId) => rows.Single(r => r.StartsWith(Db + "|" + queryId.ToString(CultureInfo.InvariantCulture) + "|", StringComparison.Ordinal)).Split('|');

        /* Query 50: 5 + 7 + 3 + 4 + 6 + 2 recent executions, each counted once. Query 51: two recent plans. Query 52: 11
           executions over two daily rows of one plan, and its text from the recent days. Query 51's text is the recent day's. */
        Assert.Equal("27", Row(withRecent, 50)[10]);
        Assert.Equal("2", Row(withRecent, 51)[12]);
        Assert.Equal("11", Row(withRecent, 52)[10]);
        Assert.Equal("1", Row(withRecent, 52)[12]);
        Assert.Equal("SELECT 52", Row(withRecent, 52)[14]);
        Assert.Equal("SELECT 51 zz", Row(withRecent, 51)[14]);

        /* The latest form's raised high bound (the measured margin) keeps the same rows. */
        var raised = QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, TimeSpan.FromHours(3));
        Assert.True(raised.RecentHighStart > plan.RecentHighStart);
        Assert.Equal(allLiveRecent, await RunFullAsync(f.Connection, ServerId, Db, start, f.End, baselineStart, raised, ct));

        /* The MCP read takes the recent days: its rows are the all-live rows, and poisoning a recent day's totals moves them. */
        async Task<List<string>> ServedAsync() =>
            (await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(f.Postgres, ServerId, start, f.End, DatabaseFilter.One(Db), 50, baselineStart, ct)).Rows.Select(Describe).ToList();
        var reference = await RunAsync(f.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false), start, f.End, baselineStart, QueryStoreRegressionBaseline.AllLive(start), ct);
        Assert.Equal(reference, await ServedAsync());
        await ExecAsync(f.Connection, $"UPDATE collect.plan_regression_daily SET cpu_us_sum = cpu_us_sum * 5 WHERE server_id = {ServerId} AND day = DATE '{t2:yyyy-MM-dd}'", ct);
        Assert.NotEqual(reference, await ServedAsync());
    }

    /// <summary>
    /// #5506, the defect the gate exists for: day D (T-3) is built whole, then one of its intervals is recollected across
    /// midnight (its last execution moves to D + 1, its count rises) and D is not rebuilt. The day's late mark makes it
    /// invalid, so D is read live and the rows equal the all-live recent read; a gate that took D would count the old snapshot.
    /// Then the other gates: a pending latest batch, a baseline start that leaves D - 1 day below it (a row of D collected in
    /// between would be in D's totals and not in the live read), and a window that ends before latest's applied_through
    /// (a row of D collected after the end would be in D's totals).
    /// </summary>
    [Fact]
    public async Task ARecentDayRecollectedAcrossMidnight_OrOutsideTheGates_IsReadLive()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: false, timescale: false, ct, recent: true);
        var start = RecentStart(f.Today);
        var baselineStart = f.Today.AddDays(-10).AddHours(12);
        var t3 = DateOnly.FromDateTime(f.Today.AddDays(-3));
        var t2 = DateOnly.FromDateTime(f.Today.AddDays(-2));
        string[] Row(List<string> rows, long queryId) => rows.Single(r => r.StartsWith(Db + "|" + queryId.ToString(CultureInfo.InvariantCulture) + "|", StringComparison.Ordinal)).Split('|');

        /* 1. Recollected across midnight, not rebuilt. */
        Assert.Contains(t3, (await ResolveAsync(f.Postgres, baselineStart, start, ct, f.End)).RecentDays);
        await RecollectRecentAsync(f.Postgres, f.Today, ct);
        var plan = await ResolveAsync(f.Postgres, baselineStart, start, ct, f.End);
        Assert.True(plan.RegularFromLatest);
        Assert.DoesNotContain(t3, plan.RecentDays);
        var (withRecent, allLiveRecent) = await BothRecentFormsAsync(f.Connection, start, f.End, baselineStart, plan, ct);
        Assert.Equal(allLiveRecent, withRecent);
        Assert.Equal("25", Row(withRecent, 53)[10]);

        /* 2. A pending latest batch: no recent day, and the arms read wide. */
        await ExecAsync(f.Connection, $"INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES ({ServerId}, TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', '{Db}', TIMESTAMP '{f.Start:yyyy-MM-dd HH:mm:ss}', 'x')", ct);
        var pending = await ResolveAsync(f.Postgres, baselineStart, start, ct, f.End);
        Assert.False(pending.RegularFromLatest);
        Assert.Empty(pending.RecentDays);
        Assert.Equal(allLiveRecent, await RunFullAsync(f.Connection, ServerId, Db, start, f.End, baselineStart, pending, ct));
        await ExecAsync(f.Connection, $"DELETE FROM collect.query_store_interval_latest_pending WHERE server_id = {ServerId}", ct);

        /* Rebuild every day, so T-3 and T-2 are whole and valid again for the gates below. */
        var rebuilt = await PlanRegressionDaily.RunTickAsync(f.Postgres, LiveClock.Now(), NullLogger.Instance, ct);
        Assert.True(rebuilt.Built > 0, "the builder rebuilt no day");
        Assert.Contains(t3, (await ResolveAsync(f.Postgres, baselineStart, start, ct, f.End)).RecentDays);

        /* 3. The strict rule: a baseline start at T-4 03:00 leaves T-3 - 1 day below it. Query 54's row of T-3 was collected
           at T-4 02:00, so the live read drops it and T-3's totals hold it: T-3 must be read live. */
        var lateBaseline = f.Today.AddDays(-4).AddHours(3);
        var strict = await ResolveAsync(f.Postgres, lateBaseline, start, ct, f.End);
        Assert.True(strict.RegularFromLatest);
        Assert.DoesNotContain(t3, strict.RecentDays);
        Assert.Contains(t2, strict.RecentDays);
        var (strictWith, strictLive) = await BothRecentFormsAsync(f.Connection, start, f.End, lateBaseline, strict, ct);
        Assert.Equal(strictLive, strictWith);
        Assert.Equal("2", Row(strictWith, 54)[10]);

        /* 4. A window ending at T-1 01:00, before latest's applied_through. T-2 lies inside it, but query 55's row of T-2 was
           collected at T-1 02:00, after the end: the live read drops it, T-2's totals hold it. No recent day is taken. */
        var earlyEnd = f.Today.AddDays(-1).AddHours(1);
        var early = await ResolveAsync(f.Postgres, baselineStart, start, ct, earlyEnd);
        Assert.True(early.RegularFromLatest);
        Assert.Empty(early.RecentDays);
        var (earlyWith, earlyLive) = await BothRecentFormsAsync(f.Connection, start, earlyEnd, baselineStart, early, ct);
        Assert.Equal("4", Row(earlyWith, 55)[10]);

        /* Force T-2 as a recent day anyway, the day the gate refused. The plan is built the way the resolver builds it. T-2's
           totals hold query 55's row collected after the end, so the read differs from the live one, and that row is the whole
           difference: every other row is identical. */
        var forced = QueryStoreRegressionBaseline.WithRecentDays(early, [t2]);
        Assert.True(QueryStoreRegressionBaseline.ReadsRecentDays(forced));
        var earlyForced = await RunFullAsync(f.Connection, ServerId, Db, start, earlyEnd, baselineStart, forced, ct);
        Assert.NotEqual(earlyLive, earlyForced);
        var q55 = Db + "|55|";
        Assert.Equal(
            earlyLive.Where(r => !r.StartsWith(q55, StringComparison.Ordinal)).ToList(),
            earlyForced.Where(r => !r.StartsWith(q55, StringComparison.Ordinal)).ToList());
        Assert.Equal("9", Row(earlyForced, 55)[10]);
    }
    /// <summary>
    /// #6750 part 3, review round 2: the recent-days tier with the self-statement keys. Query 50 regresses on the recent days read from
    /// the daily totals. Once its stored text carries the monitor's marker, the MCP read hides it with the gate closed (the text probe,
    /// with the recent days' sample from latest) and with it open (the keys), and lists it again when the key is taken away, so it is
    /// the key that hides it on this tier.
    /// </summary>
    [Fact]
    public async Task TheRecentDaysTier_HidesTheMonitorsOwnStatement_WithTheSelfKeyGateClosedAndOpen()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var f = await BuildFixtureAsync(extra: false, timescale: false, ct, recent: true);
        var start = RecentStart(f.Today);
        var baselineStart = f.Today.AddDays(-10).AddHours(12);
        Assert.NotEmpty((await ResolveAsync(f.Postgres, baselineStart, start, ct, f.End)).RecentDays);

        async Task<List<long>> ServedIdsAsync() =>
            (await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(f.Postgres, ServerId, start, f.End, DatabaseFilter.One(Db), 50, baselineStart, ct)).Rows.Select(r => r.QueryId).ToList();
        Assert.Contains(50L, await ServedIdsAsync());

        await QueryStoreTextWriter.WriteAsync(f.Connection, ServerId, Db,
            [new FetchedQueryText(50, "SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ 50", null)],
            DateTime.SpecifyKind(LiveClock.Now().AddMinutes(1), DateTimeKind.Utc), 30, cancellationToken: ct);
        Assert.False(await QueryStoreSelfKeys.ReadyAsync(f.Connection, null, 30, ct));
        var closed = await ServedIdsAsync();
        Assert.DoesNotContain(50L, closed);

        Assert.NotNull(await QueryStoreSelfKeys.BackfillAsync(f.Connection, ct));
        Assert.True(await QueryStoreSelfKeys.ReadyAsync(f.Connection, null, 30, ct));
        Assert.Equal(closed, await ServedIdsAsync());

        await ExecAsync(f.Connection, "DELETE FROM collect.query_store_self_keys", ct);
        Assert.Contains(50L, await ServedIdsAsync());
    }
}
