/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7875: Lite's Query Store regressions read puts an interval on one side of the window start only, by the last_execution_time
/// of its latest snapshot (the line Darling's read splits on, #6750). It used to dedupe the baseline and the recent window in
/// two separate collection_time ranges, so an interval with a snapshot before the window start and another after it was the
/// latest snapshot of both and its executions were counted in both. The coverage probe splits the same way.
/// </summary>
public sealed class QueryStoreRegressionsWindowSplitTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "WindowSplitSrv";
    private const string Db = "AppDb";

    private readonly DuckDbInitializer _duckDb;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreRegressionsWindowSplitTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>Query 1 has a baseline interval 3 days back (100 runs at 1 ms) and one interval with a snapshot a minute before the
    /// window start (100 runs) and its latest a minute after it (150 runs, last run after the start). Those 150 runs are the recent
    /// window's alone: 100 baseline runs and 150 recent. The old read gave 200 and 150: its baseline kept the latest snapshot of each interval
    /// collected before the start (interval 1's 100 and interval 2's 100 from a minute before it), and its recent window kept interval 2's 150.</summary>
    [Fact]
    public async Task AnIntervalSnapshottedOnBothSidesOfTheWindowStart_CountsOnTheRecentSideOnly()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddDays(-3), start.AddDays(-3), 100, 1000, intervalId: 1, queryId: 1);
        await SeedAsync(start.AddMinutes(-1), start.AddMinutes(-1), 100, 10_000, intervalId: 2, queryId: 1);
        await SeedAsync(start.AddMinutes(1), start.AddMinutes(1), 150, 10_000, intervalId: 2, queryId: 1);

        var row = Assert.Single(await ReadAsync(asOf));

        Assert.Equal(1, row.QueryId);
        Assert.Equal(100, row.BaselineExecCount);
        Assert.Equal(150, row.RecentExecCount);
        Assert.Equal(10.0, row.RecentCpuMs, 3);
        Assert.Equal(1.0, row.BaselineCpuMs, 3);
    }

    /// <summary>Query 2's busy interval was last run before the window start, and its latest snapshot was collected before it: it is
    /// the baseline's alone (250 baseline runs with the older interval), and the recent window holds only its own 150.</summary>
    [Fact]
    public async Task AnIntervalWhoseLatestSnapshotIsBeforeTheWindowStart_CountsOnTheBaselineSideOnly()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddDays(-3), start.AddDays(-3), 100, 1000, intervalId: 1, queryId: 2);
        await SeedAsync(start.AddMinutes(-30), start.AddMinutes(-30), 150, 10_000, intervalId: 2, queryId: 2);
        await SeedAsync(start.AddMinutes(30), start.AddMinutes(30), 150, 20_000, intervalId: 3, queryId: 2);

        var row = Assert.Single(await ReadAsync(asOf));

        Assert.Equal(250, row.BaselineExecCount);
        Assert.Equal(150, row.RecentExecCount);
    }

    /// <summary>An interval last run before the window start but snapshotted again after it is the baseline's: the read puts it there,
    /// so the coverage probe says the baseline has rows and the recent window has none.</summary>
    [Fact]
    public async Task Coverage_SplitsOnTheLastRunOfTheIntervalsLatestSnapshot()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddMinutes(-1), start.AddMinutes(-2), 100, 10_000, intervalId: 1, queryId: 3);
        await SeedAsync(start.AddMinutes(1), start.AddMinutes(-2), 100, 10_000, intervalId: 1, queryId: 3);

        var (hasBaseline, hasRecent) = await new LocalDataService(_duckDb).GetQueryStoreRegressionCoverageAsync(_serverId, 2, asOf);

        Assert.True(hasBaseline);
        Assert.False(hasRecent);
    }

    /// <summary>#7875, Darling's rule (#6750): the store's data starts after the window start, and one interval last ran before it but was
    /// collected after it (a new install's first collection). The baseline has a row but holds no hours, so it counts as empty, as Darling's
    /// does: its 5 timeouts are not compared with the 5 recent ones (that comparison has no rate to make), the query is listed for timeouts
    /// as against an empty baseline, and the baseline's per-hour rate stays null.</summary>
    [Fact]
    public async Task ABaselineThatHoldsNoHours_CountsAsEmpty_AndTheRecentTimeoutsAreListed()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddMinutes(10), start.AddMinutes(-5), 5, 1000, intervalId: 1, queryId: 4, executionType: "Aborted");
        await SeedAsync(start.AddMinutes(20), start.AddMinutes(20), 5, 1000, intervalId: 2, queryId: 4, executionType: "Aborted");

        var row = Assert.Single(await ReadAsync(asOf));

        Assert.Equal(4, row.QueryId);
        Assert.Equal("timeouts", row.RegressionReason);
        Assert.Equal(5, row.Aborted.RecentCount);
        Assert.Null(row.Aborted.BaselinePerHour);
    }

    /// <summary>The same baseline that holds no hours, with Errors (Exception runs) instead of timeouts: the errors arm of the empty-baseline
    /// rule lists the query on its 5 recent errors and the baseline's per-hour figure stays null, not compared with the 5 baseline errors.</summary>
    [Fact]
    public async Task ABaselineThatHoldsNoHours_ListsTheRecentErrors_AndLeavesTheBaselineRateNull()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddMinutes(10), start.AddMinutes(-5), 5, 1000, intervalId: 1, queryId: 5, executionType: "Exception");
        await SeedAsync(start.AddMinutes(20), start.AddMinutes(20), 5, 1000, intervalId: 2, queryId: 5, executionType: "Exception");

        var row = Assert.Single(await ReadAsync(asOf));

        Assert.Equal(5, row.QueryId);
        Assert.Equal("errors", row.RegressionReason);
        Assert.Equal(5, row.Exception.RecentCount);
        Assert.Null(row.Exception.BaselinePerHour);
    }

    /// <summary>An empty baseline puts a timeouts row in the HIGH band, as a baseline that had none does: the 5 baseline timeouts have no rate
    /// to divide, so the row is not a MEDIUM one.</summary>
    [Fact]
    public async Task ABaselineThatHoldsNoHours_PutsTheTimeoutsRowInTheHighBand()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddMinutes(10), start.AddMinutes(-5), 5, 1000, intervalId: 1, queryId: 6, executionType: "Aborted");
        await SeedAsync(start.AddMinutes(20), start.AddMinutes(20), 5, 1000, intervalId: 2, queryId: 6, executionType: "Aborted");

        var row = Assert.Single(await ReadAsync(asOf));

        Assert.Equal("timeouts", row.RegressionReason);
        Assert.Equal("HIGH", row.Severity);
    }

    /// <summary>The errors twin of the timeouts band test above.</summary>
    [Fact]
    public async Task ABaselineThatHoldsNoHours_PutsTheErrorsRowInTheHighBand()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddMinutes(10), start.AddMinutes(-5), 5, 1000, intervalId: 1, queryId: 7, executionType: "Exception");
        await SeedAsync(start.AddMinutes(20), start.AddMinutes(20), 5, 1000, intervalId: 2, queryId: 7, executionType: "Exception");

        var row = Assert.Single(await ReadAsync(asOf));

        Assert.Equal("errors", row.RegressionReason);
        Assert.Equal("HIGH", row.Severity);
    }

    /// <summary>The tool description says a baseline with no hours still lists cost against an interval that ran before the window start and
    /// was collected after it. Query 8's interval 1 is that interval (100 Regular runs at 1 ms, last run before the start, collected after
    /// it, so the store holds no baseline hour), and the 150 runs at 10 ms in the window are compared with it (they add 1.35 s of CPU, over the cost floor).</summary>
    [Fact]
    public async Task ABaselineThatHoldsNoHours_ListsCostAgainstAnIntervalThatRanBeforeTheStart()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddMinutes(10), start.AddMinutes(-5), 100, 1000, intervalId: 1, queryId: 8);
        await SeedAsync(start.AddMinutes(20), start.AddMinutes(20), 150, 10_000, intervalId: 2, queryId: 8);

        var row = Assert.Single(await ReadAsync(asOf));

        Assert.Equal(8, row.QueryId);
        Assert.Equal("cost", row.RegressionReason);
        Assert.Equal(100, row.BaselineExecCount);
        Assert.Equal(1.0, row.BaselineCpuMs, 3);
        Assert.Equal(10.0, row.RecentCpuMs, 3);
    }

    /// <summary>An interval whose only snapshot was collected after the window start but last ran before it: the read puts it on the baseline
    /// side, so the probe says the baseline has rows and the recent window has none. The old probe tested collection_time and said the opposite.</summary>
    [Fact]
    public async Task Coverage_PutsAnIntervalCollectedAfterTheStartButLastRunBeforeItOnTheBaselineSide()
    {
        var (asOf, start) = Window();
        await SeedAsync(start.AddMinutes(10), start.AddMinutes(-5), 100, 10_000, intervalId: 1, queryId: 9);

        var (hasBaseline, hasRecent) = await new LocalDataService(_duckDb).GetQueryStoreRegressionCoverageAsync(_serverId, 2, asOf);

        Assert.True(hasBaseline);
        Assert.False(hasRecent);
    }

    private static (DateTime AsOf, DateTime Start) Window()
    {
        var asOf = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
        return (asOf, asOf.AddHours(-2));
    }

    private Task<System.Collections.Generic.List<QueryStoreRegressionRow>> ReadAsync(DateTime asOf) =>
        new LocalDataService(_duckDb).GetQueryStoreRegressionsAsync(_serverId, hoursBack: 2, maxRows: 50, asOfUtc: asOf);

    private async Task SeedAsync(DateTime collectionTime, DateTime lastExecutionTime, long executions, long avgCpuUs, long intervalId, long queryId, string executionType = "Regular")
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $12, $8, $9, $9, 100, $10, 'SELECT * FROM dbo.Widgets', $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 9L });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgCpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastExecutionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = executionType });
        await cmd.ExecuteNonQueryAsync();
    }
}
