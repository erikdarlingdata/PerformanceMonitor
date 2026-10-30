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
/// #6750: <see cref="DarlingQueryStoreRegressionReader.GetTableCoverageAsync"/> answers each side with a witness arm (an interval that
/// started inside the side) and an edge arm (one that started up to the read's margin before it and last ran inside). Each case seeds
/// one interval through the real write path whose only possible witness is the edge arm, with a 3 hour margin, so dropping the edge
/// arm turns the answer false. Every instant derives from one seeded <c>now</c>, none from a chunk boundary.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsTableCoverageLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsCover";
    private const string ServerName = "qscover";
    private static readonly TimeSpan Margin = TimeSpan.FromHours(3);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Seeds one Regular interval that first ran at <c>$4 + firstOffset</c> and last ran at <c>$4 + lastOffset</c>
    /// (<c>$2</c> is <c>$4</c> plus 3 days), or none when both are null. Returns the coverage read from the wide table, then from
    /// latest.</summary>
    private static async Task<List<(bool HasBaseline, bool HasRecent)>> CoverageAsync(TimeSpan? firstOffset, TimeSpan? lastOffset, CancellationToken ct)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var now = LiveClock.Now();
        var windowStart = now.AddHours(-24);
        var costStart = windowStart.AddDays(-3);

        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, $@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, '{ServerName}', '{ServerName}', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName + "-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = ServerName + "-host" },
            ConnectionString = "Server=" + ServerName + "-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = ServerName + "-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        if (firstOffset is TimeSpan first && lastOffset is TimeSpan last)
        {
            var firstRun = costStart + first;
            var lastRun = costStart + last;
            var rows = new List<QueryStoreCollector.Row>
            {
                new()
                {
                    DatabaseName = Db,
                    QueryId = 71,
                    PlanId = 171,
                    ExecutionTypeDesc = "Regular",
                    FirstExecutionTime = firstRun,
                    LastExecutionTime = lastRun,
                    QueryHash = "0x00000047",
                    QueryPlanHash = "0x000000AB",
                    QueryText = "SELECT 71",
                    ExecutionCount = 10,
                    AvgCpuTimeUs = 1_000,
                    AvgDurationUs = 2_000,
                    AvgLogicalIoReads = 100,
                    IsForcedPlan = false,
                    ForceFailureCount = 0,
                    RuntimeStatsIntervalId = 71_001,
                    IntervalStartTimeUtc = firstRun,
                },
            };
            var collected = lastRun.AddMinutes(10);
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, rows, server, DateTime.SpecifyKind(collected, DateTimeKind.Unspecified), context, ct);
        }

        var results = new List<(bool HasBaseline, bool HasRecent)>();
        foreach (var fromLatest in new[] { false, true })
        {
            var spans = new DarlingQueryStoreRegressionReader.RegressionSpans(costStart, costStart, false, fromLatest, Margin);
            results.Add(await DarlingQueryStoreRegressionReader.GetTableCoverageAsync(postgres, ServerId, windowStart, now, spans, DatabaseFilter.All, ct));
        }

        return results;
    }

    /// <summary>The only baseline-side interval started before the cost start and last ran inside [$4, $2): only the edge arm sees it.</summary>
    [Fact]
    public async Task HasBaseline_IsTrue_WhenOnlyAnIntervalThatStartedBeforeTheCostStartLastRanInsideTheBaseline()
    {
        var ct = TestContext.Current.CancellationToken;
        var results = await CoverageAsync(TimeSpan.FromHours(-2), TimeSpan.FromHours(1), ct);
        Assert.All(results, r => Assert.True(r.HasBaseline));
        Assert.All(results, r => Assert.False(r.HasRecent));
    }

    /// <summary>The only recent-side interval started before $2 and last ran after it: only the edge arm sees it.</summary>
    [Fact]
    public async Task HasRecent_IsTrue_WhenOnlyAnIntervalThatStartedBeforeTheWindowLastRanInsideIt()
    {
        var ct = TestContext.Current.CancellationToken;
        /* $2 is $4 + 3 days. */
        var results = await CoverageAsync(TimeSpan.FromDays(3) - TimeSpan.FromHours(2), TimeSpan.FromDays(3) + TimeSpan.FromHours(1), ct);
        Assert.All(results, r => Assert.True(r.HasRecent));
        Assert.All(results, r => Assert.False(r.HasBaseline));
    }

    /// <summary>Neither: no interval at all, and one that started further before the cost start than the margin allows.</summary>
    [Fact]
    public async Task BothAreFalse_WhenNothingStartedInsideTheMargin()
    {
        var ct = TestContext.Current.CancellationToken;
        foreach (var none in await CoverageAsync(null, null, ct))
        {
            Assert.False(none.HasBaseline);
            Assert.False(none.HasRecent);
        }

        foreach (var beyond in await CoverageAsync(-Margin - TimeSpan.FromHours(1), TimeSpan.FromHours(1), ct))
        {
            Assert.False(beyond.HasBaseline);
            Assert.False(beyond.HasRecent);
        }
    }
}
