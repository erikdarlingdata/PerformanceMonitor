/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6829: get_query_store_regressions on a store whose raw tier keeps 3 days. A 168 hour read compares a baseline of days 14 to 7
/// back, which retention dropped. The no-baseline answer named two causes that were both false (the history is inside the window, or
/// the server has nothing older than the baseline lookback) for a server whose collector ran before the baseline window. It now
/// names retention, with the baseline dates and the date where the kept data starts (the oldest surviving row, never a retention
/// figure), and carries the window-floor hints. A server whose collector only started inside the window keeps the stock causes.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and works entirely inside it. It shares the gap-cache collection with the other regressions table
   tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsRetentionCauseLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string ServerName = "qsretention";
    private const string Db = "qsRetained";
    private const string OtherDb = "qsOther";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// A hypertable store. Regular runs of <see cref="Db"/> at 9 days back and just inside the three days raw keeps, and a run of
    /// <see cref="OtherDb"/> a day back. Raw's chunks older than three days are dropped, so the 9 day run is gone and raw's data starts
    /// after the 168 hour window's start. The interval tables are never filled, so the read is the raw one. The query_store collector's
    /// successful runs, one a day from <paramref name="collectorFromDaysBack"/> days back, are the log that says when it ran.
    /// </summary>
    private static async Task<Rig> StartAsync(int collectorFromDaysBack, CancellationToken ct, bool collectionGap = false)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #6829 live tests.");

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
VALUES (@server_id, '{ServerName}', '{ServerName}', TRUE, 16, now() - INTERVAL '21 days', now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var now = LiveClock.Now();
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsretention-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = "qsretention-host" },
            ConnectionString = "Server=qsretention-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsretention-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 8000;
        async Task IntervalAsync(string database, long queryId, DateTime first)
        {
            intervalId++;
            QueryStoreCollector.Row Row(long k) => new()
            {
                DatabaseName = database,
                QueryId = queryId,
                PlanId = queryId * 10 + 1,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = first,
                LastExecutionTime = first.AddMinutes(30),
                QueryHash = "0x0000000A",
                QueryPlanHash = "0x00000065",
                QueryText = "SELECT " + queryId,
                ExecutionCount = 10 * k,
                AvgCpuTimeUs = 1_000,
                AvgDurationUs = 2_000,
                AvgLogicalIoReads = 100,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = first,
            };

            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { Row(1) }, server, first.AddMinutes(20), context, ct);
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { Row(3) }, server, first.AddMinutes(50), context, ct);
        }

        if (collectionGap)
        {
            /* Collection broke from day 10 to day 1: the only rows are a day old, so raw's data starts a day back although raw keeps 4. */
            await IntervalAsync(Db, 10, now.AddDays(-1).AddHours(-2));
            await IntervalAsync(OtherDb, 20, now.AddDays(-1));
        }
        else
        {
            await IntervalAsync(Db, 10, now.AddDays(-9));
            await IntervalAsync(Db, 10, now.AddDays(-3).AddHours(1));
            await IntervalAsync(OtherDb, 20, now.AddDays(-1));
        }

        /* One successful run a day from collectorFromDaysBack back. With a gap, none from day 10 to day 2 (the runs resume at day 1). */
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, $@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
SELECT row_number() OVER (ORDER BY t), @server_id, '{ServerName}', 'query_store', t, 'SUCCESS', 5
FROM generate_series(now() - INTERVAL '{collectorFromDaysBack} days', now(), INTERVAL '1 day') AS t
WHERE NOT {(collectionGap ? "TRUE" : "FALSE")} OR t < now() - INTERVAL '10 days' OR t > now() - INTERVAL '2 days'", null, ct);

        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(
            new QueryStoreIntervalWideBelowFloorLiveTests.Rig { Scratch = scratch, Connection = connection, Postgres = postgres }, now.AddDays(-3), ct);
        var rawFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalWideBelowFloorLiveTests.ChunkFloorSql, ct))!;
        Assert.True(rawFloor > now.AddDays(-7), $"raw's floor {rawFloor:o} should be after the 168-hour window's start");
        return rig;
    }

    [Fact]
    public async Task AServerCollectedBeforeTheBaselineWindow_WhoseRawDataStartsAfterIt_NamesRetentionAsTheCause()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(collectorFromDaysBack: 20, ct);

        var answer = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168);
        var root = JsonDocument.Parse(answer).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        var now = DateTime.UtcNow;
        var baselineStart = now.AddHours(-168).AddDays(-7);
        var oldest = await OldestRawRowDateAsync(rig, ct);
        Assert.Contains("The baseline window (", message, StringComparison.Ordinal);
        Assert.Contains(baselineStart.ToString("MMM d", System.Globalization.CultureInfo.InvariantCulture), message, StringComparison.Ordinal);
        Assert.Contains($"raw Query Store data, which starts {oldest}, so there is no baseline to compare against", message, StringComparison.Ordinal);
        /* The remedy is the arithmetic's: a shorter hours_back leaves a PARTIAL baseline from the kept data, never the whole window. */
        Assert.Contains($"the partial baseline from {oldest} to that end", message, StringComparison.Ordinal);
        Assert.DoesNotContain("keeps", message, StringComparison.Ordinal);
        Assert.DoesNotContain("inside the data that is kept", message, StringComparison.Ordinal);
        Assert.DoesNotContain("whole collected history", message, StringComparison.Ordinal);
        Assert.DoesNotContain("none older than the baseline lookback", message, StringComparison.Ordinal);

        var hints = root.GetProperty("hints");
        Assert.True(hints.GetProperty("window_truncated").GetBoolean());
        Assert.Contains("further back than this server's raw query_store_stats retains", hints.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);
        Assert.NotEqual(JsonValueKind.Null, hints.GetProperty("effective_start").ValueKind);
    }

    [Fact]
    public async Task AServerWhoseCollectorStartedInsideTheWindow_KeepsTheStockCauses_AndCarriesTheHints()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(collectorFromDaysBack: 2, ct);

        var answer = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168);
        var root = JsonDocument.Parse(answer).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("whole collected history falls inside the last 168 hour(s)", message, StringComparison.Ordinal);
        Assert.Contains("none older than the baseline lookback yet", message, StringComparison.Ordinal);
        Assert.DoesNotContain("raw Query Store data, which starts", message, StringComparison.Ordinal);
        Assert.True(root.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
    }

    [Fact]
    public async Task ADatabaseWithNoOlderBaseline_OnAStoreWhoseRawDataStartsInsideTheBaselineWindow_SaysRetentionMayHaveDroppedIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(collectorFromDaysBack: 20, ct);

        /* 48 hours: the baseline window is days 9 to 2 back, raw holds the first database's run from day 3, so the server has a baseline
           and the second database, which began a day ago, has none. */
        var answer = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 48, database_name: OtherDb);
        var root = JsonDocument.Parse(answer).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains($"database_name '{OtherDb}'", message, StringComparison.Ordinal);
        Assert.Contains("dropped by retention", message, StringComparison.Ordinal);
        Assert.Contains($"This server's raw Query Store data starts {await OldestRawRowDateAsync(rig, ct)} and the baseline window", message, StringComparison.Ordinal);
        Assert.DoesNotContain("keeps", message, StringComparison.Ordinal);
        Assert.DoesNotContain("whole collected history", message, StringComparison.Ordinal);
        Assert.True(root.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
    }

    /// <summary>
    /// The collection-gap shape. The collector ran productively 20 days back, then nothing from day 10 to day 2, so raw holds only a
    /// day of rows although it keeps 4 days. The oldest row is a day old: the answer says where the data starts, and must not present
    /// that span as what the store keeps.
    /// </summary>
    [Fact]
    public async Task AServerWhoseCollectionBrokeForDays_NamesTheOldestRowsDate_NotARetentionClaim()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(collectorFromDaysBack: 20, ct, collectionGap: true);

        var root = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        var oldest = await OldestRawRowDateAsync(rig, ct);
        Assert.Contains($"raw Query Store data, which starts {oldest}, so there is no baseline to compare against", message, StringComparison.Ordinal);
        Assert.DoesNotContain("keeps", message, StringComparison.Ordinal);
        Assert.DoesNotContain("day of raw", message, StringComparison.Ordinal);
        Assert.DoesNotContain("days of raw", message, StringComparison.Ordinal);
    }

    /// <summary>The date, in the answer's own form, of the oldest row the store holds for the server.</summary>
    private static async Task<string> OldestRawRowDateAsync(Rig rig, CancellationToken ct)
    {
        var oldest = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(rig.Connection,
            "SELECT min(collection_time) FROM collect.query_store_stats WHERE server_id = " + ServerId, ct))!;
        return oldest.ToString("MMM d", System.Globalization.CultureInfo.InvariantCulture);
    }
}
