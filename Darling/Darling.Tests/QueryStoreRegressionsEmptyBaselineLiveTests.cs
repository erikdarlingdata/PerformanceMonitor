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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6750: a store whose raw floor sits after the window start. The table read binds the gate's clamp, so its baseline start is
/// raw's floor and sits after <c>baseline_end</c> at 168 hours. The baseline holds nothing and the recent window is cut. The note
/// says so, the baseline's per-hour rates are NULL (not a negative divisor's), and the recent per-hour rate divides by the hours
/// the recent side holds, not by 168. A 24-hour read of the same store is the old read, row for row.
/// </summary>
/* Own-store like QueryStoreRegressionsIntervalTableLiveTests: ScratchPostgres, and the gap-cache collection the gate's cadence check needs. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsEmptyBaselineLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsEmpty";
    private const string ServerName = "qsempty";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public DateTime RawFloor { get; set; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>A hypertable store with one query: Regular runs from nine days back (the table's floor), then Aborted runs 30, 20 and
    /// 10 hours back. Raw's chunks older than three days are dropped, so raw's floor is after the 168-hour window's start.</summary>
    private static async Task<Rig> StartAsync(CancellationToken ct)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");

        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rig = new Rig { Scratch = scratch, Connection = connection, Postgres = postgres };

        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, $@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, '{ServerName}', '{ServerName}', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var now = LiveClock.Now();
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsempty-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = "qsempty-host" },
            ConnectionString = "Server=qsempty-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsempty-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 7000;
        async Task IntervalAsync(DateTime first, string outcome, long cpuUs)
        {
            intervalId++;
            QueryStoreCollector.Row Row(long k) => new()
            {
                DatabaseName = Db,
                QueryId = 10,
                PlanId = 101,
                ExecutionTypeDesc = outcome,
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(30),
                QueryHash = "0x0000000A",
                QueryPlanHash = "0x00000065",
                QueryText = "SELECT 10",
                ExecutionCount = 10 * k,
                AvgCpuTimeUs = cpuUs,
                AvgDurationUs = cpuUs * 2,
                AvgLogicalIoReads = cpuUs / 10,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = first,
            };

            /* One interval collected twice, the later snapshot with the higher count: 30 runs once it is deduped. */
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { Row(1) }, server, first.AddMinutes(20), context, ct);
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { Row(3) }, server, first.AddMinutes(50), context, ct);
        }

        foreach (var daysBack in new[] { 9, 6, 5, 4 })
        {
            await IntervalAsync(now.AddDays(-daysBack), "Regular", 1_000);
        }

        foreach (var hoursBack in new[] { 30, 20, 10 })
        {
            await IntervalAsync(now.AddHours(-hoursBack), "Aborted", 40_000);
        }

        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, now.AddDays(-20), now.AddHours(1), null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, now.AddDays(-16), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(
            new QueryStoreIntervalWideBelowFloorLiveTests.Rig { Scratch = scratch, Connection = connection, Postgres = postgres }, now.AddDays(-3), ct);
        rig.RawFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalWideBelowFloorLiveTests.ChunkFloorSql, ct))!;
        Assert.True(rig.RawFloor > now.AddDays(-7), $"raw's floor {rig.RawFloor:o} should be after the 168-hour window's start");
        return rig;
    }

    [Fact]
    public async Task ARawFloorAfterTheWindowStart_SaysTheBaselineIsEmpty_AndDividesTheRecentRateByTheHoursItHolds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);

        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168)).RootElement;
        Assert.Equal("interval_table", hit.GetProperty("history_source").GetString());
        Assert.True(hit.GetProperty("window_truncated").GetBoolean());

        /* The sentence matches the comparison: the store starts after baseline_end and after recent_window_start. */
        var effective = DateTime.Parse(hit.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.True(effective > DateTime.Parse(hit.GetProperty("baseline_end").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal));
        var note = hit.GetProperty("truncation_note").GetString()!;
        Assert.EndsWith(QueryStoreRegressionRules.EmptyBaselineTail + " " + QueryStoreRegressionRules.RecentCutTail + " " + QueryStoreRegressionRules.RecentHoursHeldTail, note, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreRegressionRules.BaselineWindowTail, note, StringComparison.Ordinal);

        /* Timeouts were tested, cost was not: the one row is a timeouts row. */
        var row = Assert.Single(hit.GetProperty("regressions").EnumerateArray());
        Assert.Equal("timeouts", row.GetProperty("regression_reason").GetString());

        /* The baseline holds no hour, so its per-hour rates are NULL. A negative divisor gave zeros here (0 / -x). */
        Assert.Equal(JsonValueKind.Null, row.GetProperty("baseline_aborted_per_hour").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("baseline_regular_per_hour").ValueKind);

        /* 90 runs over the hours from raw's floor to the window's end, not over 168. */
        var held = (DateTime.UtcNow - rig.RawFloor).TotalHours;
        var perHour = row.GetProperty("recent_aborted_per_hour").GetDouble();
        Assert.InRange(perHour, (90 / held) - 0.05, (90 / held) + 0.05);
        Assert.True(Math.Abs(perHour - (90.0 / 168)) > 0.2, $"the rate {perHour} still divides by 168 hours");
    }

    [Fact]
    public async Task ATwentyFourHourRead_OfTheSameStore_IsTheOldReadRowForRow()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);

        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddSeconds(1);
        var start = end.AddHours(-24);
        /* The table read's baseline start is the gate's clamp: raw's floor, which is inside the 7 days before this window. */
        var baselineStart = rig.RawFloor;
        Assert.True(baselineStart < start);

        var current = DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: false);
        var old = current
            .Replace(
                "GREATEST(CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamp) - CAST($6 AS timestamp))) / 3600.0 AS double precision), 0) AS base_hours",
                "CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamp) - CAST($6 AS timestamp))) / 3600.0 AS double precision) AS base_hours", StringComparison.Ordinal)
            .Replace(
                "(CAST($3 AS timestamp) - GREATEST(CAST($2 AS timestamp), CAST($6 AS timestamp)))",
                "(CAST($3 AS timestamp) - CAST($2 AS timestamp))", StringComparison.Ordinal);
        Assert.NotEqual(current, old);

        var now = await RunAsync(rig.Connection, current, start, end, baselineStart, ct);
        var before = await RunAsync(rig.Connection, old, start, end, baselineStart, ct);
        Assert.NotEmpty(now);
        Assert.Equal(before, now);
        /* The fixture is not vacuous: the baseline's per-hour rate is a positive number here (30 runs over the hours the baseline holds). */
        Assert.True(now.Any(line => line.Contains("baseline_aborted_per_hour=1.", StringComparison.Ordinal)), string.Join("\n", now));

        /* And through the tool: the baseline starts before baseline_end, so the old sentence stays. */
        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 24)).RootElement;
        Assert.EndsWith(QueryStoreRegressionRules.BaselineWindowTail, hit.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);
    }

    private static async Task<List<string>> RunAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DateTime baselineStart, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
        command.Parameters.Add(DatabaseFilter.One(Db).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 50 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        var plan = QueryStoreRegressionBaseline.AllLive(start);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
        command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
        var lines = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i =>
                reader.GetName(i) + "=" + (reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)))));
        }

        return lines;
    }
}
