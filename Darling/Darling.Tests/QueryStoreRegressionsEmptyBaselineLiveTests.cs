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
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and works entirely inside it. It shares the gap-cache collection with the other regressions table
   tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsEmptyBaselineLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsEmpty";
    private const string ServerName = "qsempty";
    private const string OtherDb = "qsEmptyOther";

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

    /// <summary>A hypertable store with one query: Regular runs from nine days back (the table's floor) and one just inside the
    /// three days raw keeps, then Aborted runs 30, 20 and 10 hours back. Raw's chunks older than three days are dropped, so raw's
    /// floor is after the 168-hour window's start.</summary>
    private static async Task<Rig> StartAsync(CancellationToken ct, bool withOtherDatabase = false)
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
        async Task IntervalAsync(DateTime first, string outcome, long cpuUs, string database = Db, long queryId = 10)
        {
            intervalId++;
            QueryStoreCollector.Row Row(long k) => new()
            {
                DatabaseName = database,
                QueryId = queryId,
                PlanId = queryId * 10 + 1,
                ExecutionTypeDesc = outcome,
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(30),
                QueryHash = "0x0000000A",
                QueryPlanHash = "0x00000065",
                QueryText = "SELECT " + queryId,
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

        /* Raw's floor is the start of the oldest chunk the drop keeps. This interval's snapshots are just inside the three days, so
           the floor is at least 46 hours before a 24-hour window's start, at any time of day. Without it the oldest kept snapshot was
           the 30-hour interval's own, and from 05:40 to 06:00 UTC its chunk began after that interval's start, so the 24-hour
           baseline lost its only Aborted interval and the read came back empty (PR CI run 40272513437). */
        await IntervalAsync(now.AddDays(-3).AddHours(1), "Regular", 1_000);

        foreach (var hoursBack in new[] { 30, 20, 10 })
        {
            await IntervalAsync(now.AddHours(-hoursBack), "Aborted", 40_000);
        }

        if (withOtherDatabase)
        {
            /* #6829: a second database that began 20 hours ago, so it has a recent side and no baseline while the server has one. */
            await IntervalAsync(now.AddHours(-20), "Regular", 1_000, OtherDb, 20);
        }

        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, now.AddDays(-20), now.AddHours(1), null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, now.AddDays(-16), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(
            new QueryStoreIntervalWideBelowFloorLiveTests.Rig { Scratch = scratch, Connection = connection, Postgres = postgres }, now.AddDays(-3), ct);
        rig.RawFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalWideBelowFloorLiveTests.ChunkFloorSql, ct))!;
        Assert.True(rig.RawFloor > now.AddDays(-7), $"raw's floor {rig.RawFloor:o} should be after the 168-hour window's start");
        return rig;
    }

    /// <summary>
    /// #6829 on the table path: a 24 hour read whose baseline window starts 8 days back is served from the per-interval tables, whose
    /// history starts a little under 8 days back, after the baseline window's start. A database that began inside the window has no
    /// baseline, and the answer names where the table history starts, in the same date form as effective_start, not a retention figure.
    /// </summary>
    [Fact]
    public async Task ADatabaseWithNoBaseline_OnTheTablePath_NamesWhereTheTableHistoryStarts()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct, withOtherDatabase: true);
        /* The collector's successful runs that stored rows, from before the baseline window: the log the cause reads. */
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
VALUES (900000001, @server_id, 'qsempty', 'query_store', now() - INTERVAL '19 days', 'SUCCESS', 5)", null, ct);

        var root = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 24, database_name: OtherDb)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var hints = root.GetProperty("hints");
        Assert.StartsWith("The window reaches further back", hints.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);
        var effective = DateTime.Parse(hints.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains($"Query Store history starts {effective.ToString("MMM d", CultureInfo.InvariantCulture)} and the baseline window", message, StringComparison.Ordinal);
        Assert.DoesNotContain("raw Query Store data", message, StringComparison.Ordinal);
        Assert.DoesNotContain("keeps", message, StringComparison.Ordinal);
        Assert.Contains("dropped by retention", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The raw path. Since #6750 part 2 the table path serves this store's full baseline at 168 hours (its own tables reach below raw's
    /// floor), so the empty-baseline state exists only where raw answers. A pending wide batch sends the read to raw, where raw's floor
    /// is after the window start.
    /// </summary>
    [Fact]
    public async Task ARawFloorAfterTheWindowStart_SaysTheBaselineIsEmpty_AndDividesTheRecentRateByTheHoursItHolds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection,
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES (@server_id, @cutoff, 'qsEmpty', @cutoff)",
            rig.RawFloor, ct);

        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168)).RootElement;
        Assert.Equal("raw", hit.GetProperty("history_source").GetString());
        Assert.True(hit.GetProperty("window_truncated").GetBoolean());

        /* The sentence matches the comparison: the store starts after baseline_end and after recent_window_start. */
        var effective = DateTime.Parse(hit.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.True(effective > DateTime.Parse(hit.GetProperty("baseline_end").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal));
        var note = hit.GetProperty("truncation_note").GetString()!;
        Assert.EndsWith(QueryStoreRegressionRules.EmptyBaselineTail + " " + QueryStoreRegressionRules.RecentCutTail + " " + QueryStoreRegressionRules.RecentHoursHeldTail, note, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreRegressionRules.BaselineWindowTail, note, StringComparison.Ordinal);

        /* No interval ran before baseline_end here, so cost has nothing to flag against: the one row is a timeouts row. */
        var row = Assert.Single(hit.GetProperty("regressions").EnumerateArray());
        Assert.Equal("timeouts", row.GetProperty("regression_reason").GetString());

        /* The baseline holds no hour, so its per-hour rates are NULL. A negative divisor gave zeros here (0 / -x). */
        Assert.Equal(JsonValueKind.Null, row.GetProperty("baseline_aborted_per_hour").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("baseline_regular_per_hour").ValueKind);

        /* 90 runs over the hours from the answer's own effective_start to the window's end, not over 168. */
        var held = (DateTime.UtcNow - effective).TotalHours;
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
                "GREATEST(CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamp) - CAST($11 AS timestamp))) / 3600.0 AS double precision), 0) AS base_hours",
                "CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamp) - CAST($11 AS timestamp))) / 3600.0 AS double precision) AS base_hours", StringComparison.Ordinal)
            .Replace(
                "(CAST($3 AS timestamp) - GREATEST(CAST($2 AS timestamp), CAST($11 AS timestamp)))",
                "(CAST($3 AS timestamp) - CAST($2 AS timestamp))", StringComparison.Ordinal);
        Assert.NotEqual(current, old);

        var now = await RunAsync(rig.Connection, current, start, end, baselineStart, ct);
        var before = await RunAsync(rig.Connection, old, start, end, baselineStart, ct);
        Assert.NotEmpty(now);
        Assert.Equal(before, now);
        /* The fixture is not vacuous: the baseline's per-hour rate is the 30-hour interval's 30 runs over the hours from raw's floor to
           the window's start. */
        var expected = 30 / (start - baselineStart).TotalHours;
        Assert.True(now.Any(line => Field(line, "baseline_aborted_per_hour") is double rate && Math.Abs(rate - expected) < 0.01),
            $"expected baseline_aborted_per_hour {expected:0.000}:\n" + string.Join("\n", now));

        /* And through the tool (#6750 part 2): at 24 hours the baseline starts at 8 days, below raw's floor (about 3 days back), so the
           table serves each arm from its own start. The note is the table note, not raw's BaselineWindowTail. The floors are read back
           from the store: both tables start at the 9 day old interval, so each arm starts a purge-edge margin after it. */
        var wideFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(rig.Connection, QueryStoreIntervalWide.PlainTableFloorSql.Replace("$1", "@server_id", StringComparison.Ordinal), ct))!;
        var latestFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(rig.Connection, QueryStoreIntervalLatest.PlainTableFloorSql.Replace("$1", "@server_id", StringComparison.Ordinal), ct))!;
        /* The answer may be the empty envelope (nothing rose), which carries the notice's keys under hints. */
        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 24)).RootElement;
        Assert.Equal("interval_table", hit.TryGetProperty("history_source", out var source) ? source.GetString() : "interval_table");
        var carrier = hit.TryGetProperty("truncation_note", out _) ? hit : hit.GetProperty("hints");
        var dayNote = carrier.GetProperty("truncation_note").GetString()!;
        Assert.StartsWith("The window reaches further back", dayNote, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreRegressionRules.BaselineWindowTail, dayNote, StringComparison.Ordinal);
        if (hit.TryGetProperty("cost_baseline_start", out var costStart))
        {
            Assert.Equal(McpHelpers.FormatEffectiveStart(latestFloor + QueryStoreIntervalWide.PurgeEdgeMargin), costStart.GetString());
            Assert.Equal(McpHelpers.FormatEffectiveStart(wideFloor + QueryStoreIntervalWide.PurgeEdgeMargin), hit.GetProperty("failure_baseline_start").GetString());
        }
        else
        {
            Assert.Equal(McpHelpers.FormatEffectiveStart(wideFloor + QueryStoreIntervalWide.PurgeEdgeMargin), carrier.GetProperty("effective_start").GetString());
        }

        Assert.Contains(McpHelpers.FormatEffectiveStart(wideFloor + QueryStoreIntervalWide.PurgeEdgeMargin), dayNote, StringComparison.Ordinal);
    }

    private static double? Field(string line, string name)
    {
        foreach (var part in line.Split('|'))
        {
            if (part.StartsWith(name + "=", StringComparison.Ordinal)
                && double.TryParse(part.AsSpan(name.Length + 1), NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            {
                return value;
            }
        }

        return null;
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
        /* #6750: the start of the hours held, the row filters' start here. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        /* #6750: the failure arm's row-filter start and its hours held, the same on this one-tier read. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = baselineStart });
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
