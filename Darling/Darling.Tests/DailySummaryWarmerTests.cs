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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8370: the daily-summary warmer's schedule, driven by a fake clock and a recording delay rather than real time. It warms
/// every server once after the service starts, again one minute after each UTC day boundary (00:01, when the Overview's
/// 31-days-ending-today key moves to the new day), runs the new day's pass within that minute when a pass straddles
/// midnight, backs off after a failed or refused pass, skips its pass while nothing reads the cache, and stops at once
/// when the service shuts down.
/// </summary>
public sealed class DailySummaryWarmerTests
{
    private static readonly DateTime Start = new(2026, 11, 8, 10, 0, 0, DateTimeKind.Utc);

    private static readonly IReadOnlyList<(int ServerId, string Name)> TwoServers = [(1, "alpha"), (2, "beta")];

    /// <summary>The next UTC day boundary plus the warmer's own margin, measured from <paramref name="nowUtc"/>: the
    /// instant the Overview's 31-days-ending-today key moves to the new day, so a block built then serves every visitor
    /// of that day.</summary>
    private static DateTime NextBoundary(DateTime nowUtc) =>
        nowUtc.Date.AddDays(1).Add(Warmers.PassMargin);

    /// <summary>#9334: a fresh install's first pass runs before the first connection registers the server. The server is
    /// skipped (no warm, no heavy slot, no Warning, the pass still complete) and warmed on the pass after it registers.</summary>
    [Fact]
    public async Task Pass_SkipsAServerTheRegistryDoesNotHoldYet_WithoutAWarning_AndWarmsItOnceItIsRegistered()
    {
        var ct = TestContext.Current.CancellationToken;
        var registeredIds = new HashSet<int> { 1 };
        var warmed = new List<int>();
        var log = new CapturingTestLogger();
        var warmer = Warmers.Create(
            servers: () => TwoServers,
            warm: (serverId, _, _) =>
            {
                warmed.Add(serverId);
                return Task.CompletedTask;
            },
            enabled: () => true,
            admission: null,
            logger: log,
            registered: _ => Task.FromResult<IReadOnlySet<int>>(registeredIds));

        var first = await Warmers.RunPassAsync(warmer, ct);

        Assert.Equal([1], warmed);
        Assert.Equal(1, first.Warmed);
        Assert.Equal(1, first.Skipped);
        Assert.Equal(0, first.Failed);
        Assert.True(first.Complete);
        Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

        registeredIds.Add(2);
        var second = await Warmers.RunPassAsync(warmer, ct);

        Assert.Equal([1, 1, 2], warmed);
        Assert.Equal(2, second.Warmed);
        Assert.Equal(0, second.Skipped);
        Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
    }

    /// <summary>#9334: a registry read that fails must not stop the pass or hide servers: each is warmed as before, so a store
    /// that cannot answer fails the reads with their own Warnings.</summary>
    [Fact]
    public async Task Pass_WhenTheRegistryReadFails_WarmsEveryServerAsBefore()
    {
        var ct = TestContext.Current.CancellationToken;
        var warmed = new List<int>();
        var warmer = Warmers.Create(
            servers: () => TwoServers,
            warm: (serverId, _, _) =>
            {
                warmed.Add(serverId);
                return Task.CompletedTask;
            },
            enabled: () => true,
            admission: null,
            registered: _ => throw new InvalidOperationException("registry unavailable"));

        var pass = await Warmers.RunPassAsync(warmer, ct);

        Assert.Equal([1, 2], warmed);
        Assert.Equal(2, pass.Warmed);
        Assert.Equal(0, pass.Skipped);
    }

    [Fact]
    public async Task RunAsync_WarmsEveryServerOnceAfterTheStartDelay_ThenWaitsTheRefreshInterval()
    {
        var harness = new Harness(Start) { StopAfterDelays = 2 };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        var passAt = Start + Warmers.StartDelay;
        Assert.Equal(Warmers.StartDelay, harness.Delays[0]);
        Assert.Equal([(1, "alpha", passAt), (2, "beta", passAt)], harness.Warmed);
        Assert.Equal(Warmers.RefreshInterval, harness.Delays[1]); // #8824: not the day boundary 14 hours away
    }

    [Fact]
    public async Task RunAsync_RepeatsEveryRefreshInterval_AndRunsAPassAtTheBoundaryItself()
    {
        /* Start 23:20: the first pass is at 23:22, the next at 23:52, and the one after lands on the boundary (00:01), not
           a whole interval later, so the new day's key is built the minute it is in use. */
        var harness = new Harness(new DateTime(2026, 11, 8, 23, 20, 0, DateTimeKind.Utc)) { StopAfterDelays = 4 };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        var boundary = new DateTime(2026, 11, 9, 0, 1, 0, DateTimeKind.Utc);
        Assert.Equal(
            [new DateTime(2026, 11, 8, 23, 22, 0, DateTimeKind.Utc), new DateTime(2026, 11, 8, 23, 52, 0, DateTimeKind.Utc), boundary],
            harness.Warmed.Where(w => w.ServerId == 1).Select(w => w.At).ToArray());
        Assert.Equal(boundary - new DateTime(2026, 11, 8, 23, 52, 0, DateTimeKind.Utc), harness.Delays[2]);
        Assert.Equal(Warmers.RefreshInterval, harness.Delays[3]);
    }

    [Fact]
    public async Task RunAsync_StopsWithoutAPassWhenShutdownCancelsTheStartWait()
    {
        var harness = new Harness(Start) { StopAfterDelays = 1 };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.Empty(harness.Warmed);
        Assert.Single(harness.Delays);
    }

    [Fact]
    public async Task RunAsync_StopsMidPassWhenShutdownCancelsAWarm()
    {
        var harness = new Harness(Start);
        harness.WarmOverride = (_, _, ct) =>
        {
            harness.Cts.Cancel();
            throw new OperationCanceledException(ct);
        };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.Empty(harness.Warmed);
        Assert.Single(harness.Delays); // no second wait was scheduled after the cancelled pass
    }

    [Fact]
    public async Task RunAsync_BacksOffAfterAFailedServer_FollowsTheRetryLadderOnEachFailure_AndReturnsToTheRefreshIntervalOnSuccess()
    {
        /* Delays[0] is the start wait; Delays[1..4] follow the four failed passes; Delays[5] follows the pass that succeeded. */
        var harness = new Harness(Start) { StopAfterDelays = 6 };
        var betaFailures = 0;
        harness.WarmOverride = (serverId, _, _) =>
        {
            if (serverId == 2 && betaFailures < 4)
            {
                betaFailures++;
                throw new InvalidOperationException("seeded store fault");
            }

            return Task.CompletedTask;
        };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        /* The 30-minute refresh interval is a healthy pass's cadence. A failing server is retried at 15, 30, 60 and 120 minutes,
           not every 30 minutes, each try holding a heavy slot until it times out. */
        Assert.Equal(TimeSpan.FromMinutes(15), harness.Delays[1]);
        Assert.Equal(TimeSpan.FromMinutes(30), harness.Delays[2]);
        Assert.Equal(TimeSpan.FromMinutes(60), harness.Delays[3]);
        Assert.Equal(TimeSpan.FromMinutes(120), harness.Delays[4]);
        var fifthPassAt = Start + Warmers.StartDelay + harness.Delays[1] + harness.Delays[2] + harness.Delays[3] + harness.Delays[4];
        Assert.True(NextBoundary(fifthPassAt) - fifthPassAt > Warmers.RefreshInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), harness.Delays[5]);
        Assert.Equal(5, harness.Warmed.Count(w => w.ServerId == 1)); // alpha is warmed on every pass, a cache hit after the first
        Assert.Single(harness.Warmed, w => w.ServerId == 2);
    }

    [Fact]
    public async Task RunAsync_SkipsThePassWhileNothingReadsTheCache_AndLooksAgainWithinAMinute()
    {
        var harness = new Harness(Start) { StopAfterDelays = 2, Enabled = false };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.Empty(harness.Warmed);
        Assert.True(harness.Delays[1] <= TimeSpan.FromMinutes(1), $"a warmer that is off looks again after {harness.Delays[1]}");
    }

    /// <summary>#8554: a start whose materialization repair is still running when the two-minute start delay ends. The first pass
    /// used to wait for the next 15-minute look, so a server opened in that gap read its closed days cold.</summary>
    [Fact]
    public async Task RunAsync_WarmsWithinAFewMinutesOfARepairEnding_NotAQuarterOfAnHourLater()
    {
        var harness = new Harness(Start) { StopAfterDelays = 6, Enabled = false };
        var repairEndsAt = Start + Warmers.StartDelay + TimeSpan.FromMinutes(3);
        harness.BeforeDelay = () =>
        {
            if (harness.Now >= repairEndsAt)
            {
                harness.Enabled = true;
            }
        };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.NotEmpty(harness.Warmed);
        Assert.True(harness.Warmed[0].At - repairEndsAt <= TimeSpan.FromMinutes(2),
            $"the first pass ran {harness.Warmed[0].At - repairEndsAt} after the repair ended");
        Assert.Equal(2, harness.Warmed.Count(w => w.At == harness.Warmed[0].At));
    }

    [Fact]
    public async Task RunAsync_BacksOffWhenTheHeavyReadGateRefusesIt()
    {
        var admission = new ReadAdmission(TimeProvider.System, TimeSpan.Zero, null, maxHeavyReads: 1);
        /* The one slot is taken in ANOTHER flow: an admission in this test's own flow would mark it as held and let
           a nested admission pass. */
        using var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, "get_wait_stats", ReadWindowBand.Longer, CancellationToken.None));
        Assert.True(held.MayRun);

        var harness = new Harness(Start) { StopAfterDelays = 2, Admission = admission };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.Empty(harness.Warmed);
        Assert.Equal(Warmers.RetryDelay, harness.Delays[1]);
    }

    [Fact]
    public async Task RunAsync_TakesOneHeavySlotPerServer_AndGivesItBackBetweenServers()
    {
        var admission = new ReadAdmission(TimeProvider.System, TimeSpan.Zero, null, maxHeavyReads: 1);
        var harness = new Harness(Start) { StopAfterDelays = 2, Admission = admission };
        var runningDuringWarm = new List<int>();
        harness.WarmOverride = (_, _, _) =>
        {
            runningDuringWarm.Add(admission.Stats().Running);
            return Task.CompletedTask;
        };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.Equal([1, 1], runningDuringWarm); // each server's read holds exactly one slot
        Assert.Equal(0, admission.Stats().Running); // and the pass leaves none held
        Assert.Equal(Warmers.RefreshInterval, harness.Delays[1]);
    }

    [Fact]
    public async Task ABlockBuiltByTheBoundaryPass_ServesAVisitorHalfAnHourIntoTheDay_WithATailReadOnly()
    {
        var passAt = Warmers.NextPassUtc(Start);
        var visitorAt = passAt.Date.AddMinutes(30);
        Assert.True(passAt < visitorAt, $"the boundary pass at {passAt:HH:mm} UTC runs after a {visitorAt:HH:mm} UTC visitor, who pays the cold read");

        var now = passAt;
        var cache = new DailySummaryRangeCache<DateTime>(() => now);
        var reads = new List<(DateTime From, DateTime To)>();
        Task<List<DateTime>> Run(DateTime from, DateTime to, CancellationToken _)
        {
            reads.Add((from, to));
            return Task.FromResult(Days(from, to));
        }

        /* The Overview's key for the pass's day: 31 days ending today, as of now. */
        var fromDate = passAt.Date.AddDays(-(DailySummaryWarmer.DaysBack - 1));
        var toDate = passAt.Date.AddDays(1);
        await cache.GetRangeAsync("store", 1, fromDate, toDate, "sql", asOfNow: true, d => d, Run);
        Assert.Equal([(fromDate, toDate)], reads);

        reads.Clear();
        now = visitorAt;
        var rows = await cache.GetRangeAsync("store", 1, fromDate, toDate, "sql", asOfNow: true, d => d, Run);
        Assert.Equal(Days(fromDate, toDate), rows);
        var tail = Assert.Single(reads);
        Assert.Equal(toDate, tail.To);
        Assert.True(tail.From >= passAt.Date.AddDays(-1), $"the visitor re-read from {tail.From:yyyy-MM-dd}: the whole range, not the open tail");
    }

    [Fact]
    public void DailySummaryWarmerEnabled_IsAFileKnob_DefaultOn_ReadFromTheConfigFile_AndListedInTheDiagnosticsShape()
    {
        Assert.True(new DarlingConfig().DailySummaryWarmerEnabled);

        var off = System.Text.Json.JsonSerializer.Deserialize<DarlingConfig>("{\"dailySummaryWarmerEnabled\": false}");
        Assert.NotNull(off);
        Assert.False(off.DailySummaryWarmerEnabled);

        /* Classified in the bundle's config shape beside queryStoreBackfillEnabled and rawChunkIntervalReconcileEnabled, so
           the shape census knows the key; a setting in neither list cannot reach a bundle. */
        Assert.Contains("DarlingConfig.DailySummaryWarmerEnabled", DiagnosticsBundle.ConfigShapeProjected.Concat(DiagnosticsBundle.ConfigShapeExcluded));
    }

    private static List<DateTime> Days(DateTime from, DateTime to)
    {
        var days = new List<DateTime>();
        for (var day = from; day < to; day = day.AddDays(1))
        {
            days.Add(day);
        }

        return days;
    }

    [Fact]
    public async Task RunAsync_WhenAPassStraddlesMidnight_RunsTheNewDaysPassWithinTheMargin_NotADayLater()
    {
        /* The start delay puts the pass at 23:59; the second server's read runs it past midnight, so that server is warmed
           under the new day's key and the first is not. */
        var harness = new Harness(new DateTime(2026, 11, 8, 23, 57, 0, DateTimeKind.Utc)) { StopAfterDelays = 3 };
        var straddled = false;
        harness.WarmOverride = (serverId, _, _) =>
        {
            if (serverId == 2 && !straddled)
            {
                straddled = true;
                harness.Advance(TimeSpan.FromSeconds(90));
            }

            return Task.CompletedTask;
        };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        var midnight = new DateTime(2026, 11, 9, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(midnight.AddSeconds(30), harness.Warmed[1].At);
        Assert.True(harness.Delays[1] <= Warmers.PassMargin,
            $"the pass after the straddle waits {harness.Delays[1]}: the servers warmed before midnight sit under the old day's key until then");
        Assert.Equal([midnight.Add(Warmers.PassMargin), midnight.Add(Warmers.PassMargin)], harness.Warmed.Skip(2).Select(w => w.At).ToArray());
        Assert.Equal(Warmers.RefreshInterval, harness.Delays[2]);
    }

    /// <summary>A fake clock, a recording delay that advances it, and a warm callback that records each call.</summary>
    private sealed class Harness
    {
        public Harness(DateTime now) => Now = now;

        public DateTime Now { get; private set; }

        public void Advance(TimeSpan by) => Now += by;
        public List<TimeSpan> Delays { get; } = new();
        public List<(int ServerId, string Name, DateTime At)> Warmed { get; } = new();
        public CancellationTokenSource Cts { get; } = new();
        public int StopAfterDelays { get; init; } = int.MaxValue;
        public bool Enabled { get; set; } = true;
        public Action? BeforeDelay { get; set; }
        public ReadAdmission? Admission { get; init; }
        public Func<int, string, CancellationToken, Task>? WarmOverride { get; set; }

        public DailySummaryWarmer Build(IReadOnlyList<(int ServerId, string Name)> servers) => Warmers.Create(
            servers: () => servers,
            warm: async (serverId, name, ct) =>
            {
                if (WarmOverride is { } warm)
                {
                    await warm(serverId, name, ct);
                }

                Warmed.Add((serverId, name, Now));
            },
            enabled: () => Enabled,
            admission: Admission,
            clock: () => Now,
            delay: DelayAsync);

        private async Task DelayAsync(TimeSpan delay, CancellationToken ct)
        {
            Delays.Add(delay);
            BeforeDelay?.Invoke();
            if (Delays.Count >= StopAfterDelays)
            {
                Cts.Cancel();
                throw new OperationCanceledException(ct);
            }

            Now += delay;
            await Task.Yield();
        }
    }
}

/// <summary>The warmer's surface as the tests drive it: the product's constants, one constructor and its two entry points.</summary>
internal static class Warmers
{
    public static TimeSpan StartDelay => DailySummaryWarmer.StartDelay;
    public static TimeSpan PassMargin => DailySummaryWarmer.PassMargin;
    public static TimeSpan RetryDelay => DailySummaryWarmer.RetryDelay;
    public static TimeSpan RefreshInterval => DailySummaryWarmer.RefreshInterval;

    public static DateTime NextPassUtc(DateTime nowUtc) => DailySummaryWarmer.NextPassUtc(nowUtc);

    public static DailySummaryWarmer Create(
        Func<IReadOnlyList<(int ServerId, string Name)>> servers,
        Func<int, string, CancellationToken, Task> warm,
        Func<bool> enabled,
        ReadAdmission? admission,
        Func<DateTime>? clock = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Microsoft.Extensions.Logging.ILogger? logger = null,
        Func<CancellationToken, Task<IReadOnlySet<int>>>? registered = null)
        => new(servers, warm, enabled, admission, logger ?? NullLogger.Instance, clock, delay, registered);

    /// <summary>The product's registry read: the ids of the servers the store holds, the set the warm reads resolve through.</summary>
    public static Task<IReadOnlySet<int>> RegisteredIdsAsync(Npgsql.NpgsqlDataSource postgres, CancellationToken ct)
        => DailySummaryWarmer.RegisteredServerIdsAsync(postgres, ct);

    /// <summary>The product's own warm read for one server: the Overview's range through the daily summary reader.</summary>
    public static Task WarmAsync(Npgsql.NpgsqlDataSource postgres, int serverId, string name, CancellationToken ct, Microsoft.Extensions.Logging.ILogger? logger = null)
        => DailySummaryWarmer.WarmAsync(postgres, serverId, name, registryState: null, logger: logger, ct);

    public static Task RunAsync(DailySummaryWarmer warmer, CancellationToken ct) => warmer.RunAsync(ct);

    public static Task<DailySummaryWarmPass> RunPassAsync(DailySummaryWarmer warmer, CancellationToken ct) => warmer.RunPassAsync(ct);
}
