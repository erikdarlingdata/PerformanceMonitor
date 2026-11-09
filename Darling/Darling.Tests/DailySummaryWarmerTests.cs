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
/// every server once after the service starts, again at each UTC day boundary plus the cache's closed grace (when the
/// blocks it built stop being served), backs off after a failed or refused pass, skips its pass while nothing reads the
/// cache, and stops at once when the service shuts down.
/// </summary>
public sealed class DailySummaryWarmerTests
{
    private static readonly DateTime Start = new(2026, 11, 8, 10, 0, 0, DateTimeKind.Utc);

    private static readonly IReadOnlyList<(int ServerId, string Name)> TwoServers = [(1, "alpha"), (2, "beta")];

    /// <summary>The next UTC day boundary plus the cache's closed grace plus the warmer's own margin, measured from
    /// <paramref name="nowUtc"/>: the instant the blocks built before it stop being served.</summary>
    private static DateTime NextBoundary(DateTime nowUtc) =>
        nowUtc.Date.AddDays(1).Add(DailySummaryRangeCache<object>.ClosedGrace).Add(Warmers.PassMargin);

    [Fact]
    public async Task RunAsync_WarmsEveryServerOnceAfterTheStartDelay_ThenWaitsForTheDayBoundaryPlusGrace()
    {
        var harness = new Harness(Start) { StopAfterDelays = 2 };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        var passAt = Start + Warmers.StartDelay;
        Assert.Equal(Warmers.StartDelay, harness.Delays[0]);
        Assert.Equal([(1, "alpha", passAt), (2, "beta", passAt)], harness.Warmed);
        Assert.Equal(NextBoundary(passAt) - passAt, harness.Delays[1]);
    }

    [Fact]
    public async Task RunAsync_RunsASecondPassAtTheBoundary_AndRebuildsEveryServerThere()
    {
        var harness = new Harness(Start) { StopAfterDelays = 3 };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        var firstPassAt = Start + Warmers.StartDelay;
        var secondPassAt = NextBoundary(firstPassAt);
        Assert.Equal(new DateTime(2026, 11, 9, 2, 1, 0, DateTimeKind.Utc), secondPassAt);
        Assert.Equal(4, harness.Warmed.Count);
        Assert.Equal([secondPassAt, secondPassAt], harness.Warmed.Skip(2).Select(w => w.At).ToArray());
        Assert.Equal(TimeSpan.FromDays(1), harness.Delays[2]); // the pass after that is one whole day later
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
    public async Task RunAsync_BacksOffAfterAFailedServer_DoublesOnTheNextFailure_AndReturnsToTheBoundaryOnSuccess()
    {
        var harness = new Harness(Start) { StopAfterDelays = 4 };
        var betaFailures = 0;
        harness.WarmOverride = (serverId, _, _) =>
        {
            if (serverId == 2 && betaFailures < 2)
            {
                betaFailures++;
                throw new InvalidOperationException("seeded store fault");
            }

            return Task.CompletedTask;
        };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.Equal(Warmers.RetryDelay, harness.Delays[1]);
        Assert.Equal(Warmers.RetryDelay + Warmers.RetryDelay, harness.Delays[2]);
        var thirdPassAt = Start + Warmers.StartDelay + harness.Delays[1] + harness.Delays[2];
        Assert.Equal(NextBoundary(thirdPassAt) - thirdPassAt, harness.Delays[3]);
        Assert.Equal(3, harness.Warmed.Count(w => w.ServerId == 1)); // alpha is warmed on every pass, a cache hit after the first
        Assert.Single(harness.Warmed, w => w.ServerId == 2);
    }

    [Fact]
    public async Task RunAsync_SkipsThePassWhileNothingReadsTheCache_AndLooksAgainAfterTheRetryDelay()
    {
        var harness = new Harness(Start) { StopAfterDelays = 2, Enabled = false };

        await Warmers.RunAsync(harness.Build(TwoServers), harness.Cts.Token);

        Assert.Empty(harness.Warmed);
        Assert.Equal(Warmers.RetryDelay, harness.Delays[1]);
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
        Assert.Equal(NextBoundary(Start + Warmers.StartDelay) - (Start + Warmers.StartDelay), harness.Delays[1]);
    }

    /// <summary>A fake clock, a recording delay that advances it, and a warm callback that records each call.</summary>
    private sealed class Harness
    {
        public Harness(DateTime now) => Now = now;

        public DateTime Now { get; private set; }
        public List<TimeSpan> Delays { get; } = new();
        public List<(int ServerId, string Name, DateTime At)> Warmed { get; } = new();
        public CancellationTokenSource Cts { get; } = new();
        public int StopAfterDelays { get; init; } = int.MaxValue;
        public bool Enabled { get; init; } = true;
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

    public static DailySummaryWarmer Create(
        Func<IReadOnlyList<(int ServerId, string Name)>> servers,
        Func<int, string, CancellationToken, Task> warm,
        Func<bool> enabled,
        ReadAdmission? admission,
        Func<DateTime>? clock = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
        => new(servers, warm, enabled, admission, NullLogger.Instance, clock, delay);

    /// <summary>The product's own warm read for one server: the Overview's range through the daily summary reader.</summary>
    public static Task WarmAsync(Npgsql.NpgsqlDataSource postgres, int serverId, string name, CancellationToken ct)
        => DailySummaryWarmer.WarmAsync(postgres, serverId, name, registryState: null, logger: null, ct);

    public static Task RunAsync(DailySummaryWarmer warmer, CancellationToken ct) => warmer.RunAsync(ct);

    public static Task RunPassAsync(DailySummaryWarmer warmer, CancellationToken ct) => warmer.RunPassAsync(ct);
}
