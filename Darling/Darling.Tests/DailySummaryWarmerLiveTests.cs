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
/// #8370: after a service start the closed-day block of <c>get_daily_summary_range</c> is built by the warmer, not by a
/// server's first visitor. The cache starts cold, the warmer runs one pass over two seeded servers, and the first
/// <c>get_daily_summary_range</c> call for each is a cache hit for the closed days: a wait row planted on a closed day
/// AFTER the pass is not in the answer, exactly as a second read within a block's lifetime does not see one (#4232).
/// Gated on DARLING_TEST_PG.
/// </summary>
[Collection("live-postgres")]
public sealed class DailySummaryWarmerLiveTests
{
    private const string ServerA = "daily-summary-warmer-a";
    private const string ServerB = "daily-summary-warmer-b";
    private static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerA);
    private static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerB);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WarmerPass_MakesEachServersFirstRangeRead_ACacheHitForTheClosedDays()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerB, ct);

            var today = DateTime.UtcNow.Date;
            var closedDay = today.AddDays(-2); // well past the two-hour grace: closed on every read below
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(10), 1000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(10), 3000, ct);

            /* Cold, as after a service start. */
            DarlingHealthReader.ResetRangeCacheForTests();

            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, ServerA), (ServerIdB, ServerB)],
                warm: (serverId, name, token) => Warmers.WarmAsync(dataSource, serverId, name, token),
                enabled: () => true,
                admission: null);
            await Warmers.RunPassAsync(warmer, ct);

            /* Land a second row on the closed day of each server AFTER the pass. A visitor whose first read is served
               from the warmed block does not see it; a cold first read does. */
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(11), 4000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(11), 4000, ct);

            Assert.Equal(1000m, await ClosedDayWaitMsAsync(dataSource, ServerA, closedDay, today));
            Assert.Equal(3000m, await ClosedDayWaitMsAsync(dataSource, ServerB, closedDay, today));

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#8451: the service's own shape. The warmer reads through the worker's pool, and a visitor's
    /// <c>get_daily_summary_range</c> goes through the web host's own pool over the same store, built by the product's
    /// host-pool constructor with the web host's application name. The block the pass built must serve that first read: a
    /// block the web host's read does not find leaves every server's first visitor on the cold read, warmer or not.</summary>
    [Fact]
    public async Task WarmerPass_ThroughTheWorkersPool_ServesTheWebHostsFirstRangeRead()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var workerPool = NpgsqlDataSource.Create(cs!);
        await using var webPool = DarlingWorker.CreateHostStoreDataSource(cs!, DarlingManagedPostgres.WebApplicationName);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerB, ct);

            var today = DateTime.UtcNow.Date;
            var closedDay = today.AddDays(-2);
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(10), 1000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(10), 3000, ct);

            DarlingHealthReader.ResetRangeCacheForTests();

            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, ServerA), (ServerIdB, ServerB)],
                warm: (serverId, name, token) => Warmers.WarmAsync(workerPool, serverId, name, token),
                enabled: () => true,
                admission: null);
            await Warmers.RunPassAsync(warmer, ct);

            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(11), 4000, ct);
            await PlantWaitAsync(connection, ServerIdB, ServerB, closedDay.AddHours(11), 4000, ct);

            Assert.Equal(1000m, await ClosedDayWaitMsAsync(webPool, ServerA, closedDay, today));
            Assert.Equal(3000m, await ClosedDayWaitMsAsync(webPool, ServerB, closedDay, today));

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#8824: a visitor who opens a server an hour after the service started read the days before the retention
    /// horizon from the store, because the block's sub-horizon segment is only good for an hour (#6809) and nothing had
    /// re-read it since the first pass. The warmer's later passes now refresh that segment before it lapses, so the first
    /// visit after the hour runs the same statements as the visit after it: the open day's read and nothing more. The
    /// cache clock is moved by hand: pass one at noon, pass two one refresh interval later, the
    /// visitor 75 minutes after noon, past the hour the first pass's read was good for.</summary>
    [Fact]
    public async Task LaterWarmerPass_RefreshesTheSubHorizonDays_SoTheFirstVisitAfterTheHourRunsNoMoreStatementsThanTheNext()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);

            var today = DateTime.UtcNow.Date;
            var noon = today.AddHours(12);
            var fakeNow = noon;
            DarlingHealthReader.UseRangeCacheClockForTests(() => fakeNow);

            /* The Overview's window starts 30 days back. The test means nothing unless the store's retention horizon puts
               the window's first day under the sub-horizon floor, so it asks the store rather than assume the default. */
            var probe = await DarlingHealthReader.GetDailySummaryRangeAsync(dataSource, ServerIdA, today.AddDays(-1), today.AddDays(1), cancellationToken: ct);
            var windowStart = today.AddDays(-(DailySummaryWarmer.DaysBack - 1));
            Assert.True(DailySummaryRangeCache.TrustFloorFor(probe.RetentionHorizon) > windowStart,
                $"the store's retention horizon {probe.RetentionHorizon:yyyy-MM-dd} leaves no sub-horizon day in the window starting {windowStart:yyyy-MM-dd}");
            DarlingHealthReader.UseRangeCacheClockForTests(() => fakeNow);

            await PlantWaitAsync(connection, ServerIdA, ServerA, today.AddDays(-2).AddHours(10), 1000, ct);

            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, ServerA)],
                warm: (serverId, name, token) => Warmers.WarmAsync(dataSource, serverId, name, token),
                enabled: () => true,
                admission: null);
            await Warmers.RunPassAsync(warmer, ct);

            fakeNow = noon + DailySummaryWarmer.RefreshInterval;
            await Warmers.RunPassAsync(warmer, ct);

            fakeNow = noon.AddMinutes(75);
            var first = await StatementsRunAsync(() => DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerA, 31));
            var second = await StatementsRunAsync(() => DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerA, 31));

            SkipWhenTheDayChanged(today);
            Assert.True(first == second,
                $"the first visit after the hour ran {first} statements and the visit after it ran {second}: the first one re-read the days before the retention horizon");

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#9039: a range read that throws its rows away (the warmer's) runs fewer statements on a held block than the same read
    /// that keeps them, because the open days' statement is left out. The kept read still returns today's row.</summary>
    [Fact]
    public async Task RangeRead_ThatSkipsTheOpenDayTail_RunsFewerStatementsOnAHeldBlock_AndTheKeptReadStillHasToday()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            var today = DateTime.UtcNow.Date;
            /* #9047: the wait goes inside today, so the kept read has a row for today whatever the hour. One hour back is yesterday before 01:00 UTC. */
            var plantedAt = DateTime.UtcNow.AddMinutes(-1);
            if (plantedAt < today)
            {
                plantedAt = today;
            }

            await PlantWaitAsync(connection, ServerIdA, ServerA, plantedAt, 1000, ct);
            var from = today.AddDays(-(DailySummaryWarmer.DaysBack - 1));

            async Task<DarlingHealthReader.DailySummaryRangeReadResult> Read(bool skipOpenDayTail) => await DarlingHealthReader.GetDailySummaryRangeAsync(
                dataSource, ServerIdA, from, today.AddDays(1), refreshSubHorizon: true, skipOpenDayTail: skipOpenDayTail, cancellationToken: ct);

            DarlingHealthReader.ResetRangeCacheForTests();
            await Read(skipOpenDayTail: false); /* builds the block and warms what the first read of a store warms */

            var skipped = 0;
            var kept = 0;
            DarlingHealthReader.DailySummaryRangeReadResult keptResult = null!;
            skipped = await StatementsRunAsync(async () =>
            {
                await Read(skipOpenDayTail: true);
                return string.Empty;
            });
            kept = await StatementsRunAsync(async () =>
            {
                keptResult = await Read(skipOpenDayTail: false);
                return string.Empty;
            });

            SkipWhenTheDayChanged(today);
            Assert.True(skipped < kept, $"the read that skips the open days ran {skipped} statements and the read that keeps them ran {kept}");
            Assert.Contains(keptResult.Rows, row => row.SummaryDate == today);

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#8824: the Overview's wait strip reads the 24-hour <c>total_waits</c> trend, which has no cache of its own, so
    /// the warm pass makes the same read to keep the store's pages recent. The read is seen by what it costs the store: a warm
    /// of a server the trend read resolves runs more statements than the same warm of a name it cannot resolve.</summary>
    [Fact]
    public async Task WarmAsync_AlsoReadsTheWaitTrend_ForTheServer()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            var today = DateTime.UtcNow.Date;
            await PlantWaitAsync(connection, ServerIdA, ServerA, DateTime.UtcNow.AddHours(-3), 1000, ct);

            /* One warm first, so what the first read of a store warms (registry, settings) is not counted against either side. */
            await Warmers.WarmAsync(dataSource, ServerIdA, "no-such-server-for-the-trend-read", ct);

            DarlingHealthReader.ResetRangeCacheForTests();
            var withoutTrend = await StatementsRunAsync(async () =>
            {
                await Warmers.WarmAsync(dataSource, ServerIdA, "no-such-server-for-the-trend-read", ct);
                return string.Empty;
            });

            DarlingHealthReader.ResetRangeCacheForTests();
            var withTrend = await StatementsRunAsync(async () =>
            {
                await Warmers.WarmAsync(dataSource, ServerIdA, ServerA, ct);
                return string.Empty;
            });

            SkipWhenTheDayChanged(today);
            Assert.True(withTrend > withoutTrend,
                $"the warm of a resolvable server ran {withTrend} statements and the warm of an unresolvable name ran {withoutTrend}: the pass did not read the wait trend");

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#8824: the trend read answers a failure as a refusal or an error envelope, not a throw. A pass whose trend read
    /// failed still builds the daily summary block, counts the server as warmed (so the pass is complete and waits the refresh
    /// interval, not the retry ladder) and says in the log that the trend read failed. A server name the trend read cannot
    /// resolve stands for any failed trend read.</summary>
    [Fact]
    public async Task WarmerPass_WhenTheTrendReadFails_StillWarmsTheDailySummary_CountsTheServerDone_AndLogsTheFailure()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);

            var today = DateTime.UtcNow.Date;
            var closedDay = today.AddDays(-2);
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(10), 1000, ct);

            DarlingHealthReader.ResetRangeCacheForTests();

            var log = new CapturingTestLogger();
            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, "no-such-server-for-the-trend-read")],
                warm: (serverId, name, token) => Warmers.WarmAsync(dataSource, serverId, name, token, log),
                enabled: () => true,
                admission: null);
            var pass = await Warmers.RunPassAsync(warmer, ct);

            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(11), 4000, ct);

            Assert.Equal(1, pass.Warmed);
            Assert.Equal(0, pass.Failed);
            Assert.True(pass.Complete);
            Assert.Contains(log.Lines, line => line.Contains("wait trend read failed", StringComparison.Ordinal));
            Assert.Equal(1, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Equal(1000m, await ClosedDayWaitMsAsync(dataSource, ServerA, closedDay, today));

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#9334: on a fresh install the first pass runs before the first connection writes the server's registry row. The
    /// pass decides that from the store's own registry, logs no Warning (the wait trend read used to log "no servers are
    /// registered yet") and builds no block for the server; the pass after the row exists warms it. The closed day's wait is read
    /// after each step: a block built by the first pass would still hold the one seeded row when the second row lands.</summary>
    [Fact]
    public async Task WarmerPass_ForAServerWithNoRegistryRowYet_LogsNoWarningAndBuildsNothing_ThenWarmsItOnceRegistered()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            var today = DateTime.UtcNow.Date;
            var closedDay = today.AddDays(-2);
            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(10), 1000, ct);

            DarlingHealthReader.ResetRangeCacheForTests();

            var log = new CapturingTestLogger();
            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, ServerA)],
                warm: (serverId, name, token) => Warmers.WarmAsync(dataSource, serverId, name, token, log),
                enabled: () => true,
                admission: null,
                logger: log,
                registered: token => Warmers.RegisteredIdsAsync(dataSource, token));

            var first = await Warmers.RunPassAsync(warmer, ct);

            Assert.Equal(0, first.Warmed);
            Assert.Equal(1, first.Skipped);
            Assert.True(first.Complete);
            Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(11), 4000, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);

            var second = await Warmers.RunPassAsync(warmer, ct);

            Assert.Equal(1, second.Warmed);
            Assert.Equal(0, second.Skipped);
            Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

            await PlantWaitAsync(connection, ServerIdA, ServerA, closedDay.AddHours(12), 9000, ct);

            /* The second pass built the block from both rows planted before it; the third row, planted after, is not in it. A
               first pass that had built a block would show 1000 here, and no warm at all would show 14000. */
            var json = await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerA, 31);
            SkipWhenTheDayChanged(today);
            using var doc = JsonDocument.Parse(json);
            var row = Assert.Single(
                doc.RootElement.GetProperty("days").EnumerateArray().ToArray(),
                d => d.GetProperty("summary_date").GetString() == closedDay.ToString("yyyy-MM-dd"));
            Assert.Equal(5000m, row.GetProperty("total_wait_time_sec").GetDecimal() * 1000m);

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#8824: the service reads one store through three pools, and the warm pass runs through the worker's. A visitor's
    /// first read of a server goes through the web host's pool, which used to probe the store's rollup coverage itself (the
    /// longest statement of that read on a large store) because the warm pass's probe was kept for the worker's pool alone. The
    /// first visit now runs the statements the visit after it runs, and no probe of its own.</summary>
    [Fact]
    public async Task WarmerPass_ThroughTheWorkersPool_LeavesTheWebHostsFirstVisitNoCoverageProbeToRun()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary warmer test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var workerPool = NpgsqlDataSource.Create(cs!);
        await using var webPool = DarlingWorker.CreateHostStoreDataSource(cs!, DarlingManagedPostgres.WebApplicationName);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);

            var today = DateTime.UtcNow.Date;
            await PlantWaitAsync(connection, ServerIdA, ServerA, today.AddDays(-2).AddHours(10), 1000, ct);

            DarlingHealthReader.ResetRangeCacheForTests();

            var warmer = Warmers.Create(
                servers: () => [(ServerIdA, ServerA)],
                warm: (serverId, name, token) => Warmers.WarmAsync(workerPool, serverId, name, token),
                enabled: () => true,
                admission: null);
            await Warmers.RunPassAsync(warmer, ct);

            /* The web host's pool has opened no connection yet, and a new connection's own setup is counted as a statement of
               the read that opens it. It is the pool's, not the read's: open one first so neither visit counts it. */
            await using (var warmedConnection = await webPool.OpenConnectionAsync(ct))
            {
                await using var ping = warmedConnection.CreateCommand();
                ping.CommandText = "SELECT 1";
                await ping.ExecuteScalarAsync(ct);
            }

            var first = await StatementsRunAsync(() => DarlingMcpHealthTools.GetDailySummaryRange(webPool, ServerA, 31));
            var second = await StatementsRunAsync(() => DarlingMcpHealthTools.GetDailySummaryRange(webPool, ServerA, 31));

            SkipWhenTheDayChanged(today);
            Assert.True(first == second,
                $"the web host's first visit after the warm pass ran {first} statements and the visit after it ran {second}: the first one probed the store's rollup coverage again");

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The warmer and the visitor read the UTC day from the real clock themselves, so a run that crosses 00:00 UTC
    /// plants its rows for one day and reads another, and fails on correct code. Such a run is skipped, with the reason.</summary>
    private static void SkipWhenTheDayChanged(DateTime today) =>
        Assert.SkipWhen(DateTime.UtcNow.Date != today,
            $"UTC midnight passed during the test (it started on {today:yyyy-MM-dd}): the warmer and the visitor read the real clock, so the day they asked for is not the day the rows were planted for.");

    /// <summary>How many store statements the read ran, counted the way the slow-read log counts them.</summary>
    private static async Task<int> StatementsRunAsync(Func<Task<string>> read)
    {
        ReadStatementCapture.Register();
        using var opened = ReadScope.Open(null);
        opened.Scope.CaptureStatements = true;
        await read();
        return opened.Scope.StatementTotal;
    }

    /// <summary>The closed day's total wait in ms from the FIRST <c>get_daily_summary_range</c> call this test makes for
    /// the server: the Overview's own read, 31 days ending today with no anchor.</summary>
    private static async Task<decimal> ClosedDayWaitMsAsync(NpgsqlDataSource dataSource, string serverName, DateTime closedDay, DateTime today)
    {
        var json = await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, serverName, 31);
        SkipWhenTheDayChanged(today);
        using var doc = JsonDocument.Parse(json);
        var days = doc.RootElement.GetProperty("days").EnumerateArray().ToArray();
        var row = Assert.Single(days, d => d.GetProperty("summary_date").GetString() == closedDay.ToString("yyyy-MM-dd"));
        var waitMs = row.GetProperty("total_wait_time_sec").GetDecimal() * 1000m;
        Assert.True(waitMs is 1000m or 3000m,
            $"{serverName}: the first get_daily_summary_range read after the warmer pass was a cache MISS for {closedDay:yyyy-MM-dd}: it read {waitMs} ms of wait from the store, the seeded row plus the one planted after the pass.");
        return waitMs;
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime atUtc, long waitMs, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(atUtc), serverId, serverName, "CXPACKET", 5L, waitMs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var serverId in new[] { ServerIdA, ServerIdB })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM wait_stats WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", serverId);
        }
    }
}
