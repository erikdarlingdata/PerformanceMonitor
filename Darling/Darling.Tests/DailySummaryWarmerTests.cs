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
using System.Reflection;
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
        var rig = new Rig(Start) { StopAfterDelays = 2 };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        var passAt = Start + Warmers.StartDelay;
        Assert.Equal(Warmers.StartDelay, rig.Delays[0]);
        Assert.Equal([(1, "alpha", passAt), (2, "beta", passAt)], rig.Warmed);
        Assert.Equal(NextBoundary(passAt) - passAt, rig.Delays[1]);
    }

    [Fact]
    public async Task RunAsync_RunsASecondPassAtTheBoundary_AndRebuildsEveryServerThere()
    {
        var rig = new Rig(Start) { StopAfterDelays = 3 };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        var firstPassAt = Start + Warmers.StartDelay;
        var secondPassAt = NextBoundary(firstPassAt);
        Assert.Equal(new DateTime(2026, 11, 9, 2, 1, 0, DateTimeKind.Utc), secondPassAt);
        Assert.Equal(4, rig.Warmed.Count);
        Assert.Equal([secondPassAt, secondPassAt], rig.Warmed.Skip(2).Select(w => w.At).ToArray());
        Assert.Equal(TimeSpan.FromDays(1), rig.Delays[2]); // the pass after that is one whole day later
    }

    [Fact]
    public async Task RunAsync_StopsWithoutAPassWhenShutdownCancelsTheStartWait()
    {
        var rig = new Rig(Start) { StopAfterDelays = 1 };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        Assert.Empty(rig.Warmed);
        Assert.Single(rig.Delays);
    }

    [Fact]
    public async Task RunAsync_StopsMidPassWhenShutdownCancelsAWarm()
    {
        var rig = new Rig(Start);
        rig.WarmOverride = (_, _, ct) =>
        {
            rig.Cts.Cancel();
            throw new OperationCanceledException(ct);
        };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        Assert.Empty(rig.Warmed);
        Assert.Single(rig.Delays); // no second wait was scheduled after the cancelled pass
    }

    [Fact]
    public async Task RunAsync_BacksOffAfterAFailedServer_DoublesOnTheNextFailure_AndReturnsToTheBoundaryOnSuccess()
    {
        var rig = new Rig(Start) { StopAfterDelays = 4 };
        var betaFailures = 0;
        rig.WarmOverride = (serverId, _, _) =>
        {
            if (serverId == 2 && betaFailures < 2)
            {
                betaFailures++;
                throw new InvalidOperationException("seeded store fault");
            }

            return Task.CompletedTask;
        };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        Assert.Equal(Warmers.RetryDelay, rig.Delays[1]);
        Assert.Equal(Warmers.RetryDelay + Warmers.RetryDelay, rig.Delays[2]);
        var thirdPassAt = Start + Warmers.StartDelay + rig.Delays[1] + rig.Delays[2];
        Assert.Equal(NextBoundary(thirdPassAt) - thirdPassAt, rig.Delays[3]);
        Assert.Equal(3, rig.Warmed.Count(w => w.ServerId == 1)); // alpha is warmed on every pass, a cache hit after the first
        Assert.Single(rig.Warmed, w => w.ServerId == 2);
    }

    [Fact]
    public async Task RunAsync_SkipsThePassWhileNothingReadsTheCache_AndLooksAgainAfterTheRetryDelay()
    {
        var rig = new Rig(Start) { StopAfterDelays = 2, Enabled = false };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        Assert.Empty(rig.Warmed);
        Assert.Equal(Warmers.RetryDelay, rig.Delays[1]);
    }

    [Fact]
    public async Task RunAsync_BacksOffWhenTheHeavyReadGateRefusesIt()
    {
        var admission = new ReadAdmission(TimeProvider.System, TimeSpan.Zero, null, maxHeavyReads: 1);
        /* The one slot is taken in ANOTHER flow: an admission in this test's own flow would mark it as held and let
           a nested admission pass. */
        using var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, "get_wait_stats", ReadWindowBand.Longer, CancellationToken.None));
        Assert.True(held.MayRun);

        var rig = new Rig(Start) { StopAfterDelays = 2, Admission = admission };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        Assert.Empty(rig.Warmed);
        Assert.Equal(Warmers.RetryDelay, rig.Delays[1]);
    }

    [Fact]
    public async Task RunAsync_TakesOneHeavySlotPerServer_AndGivesItBackBetweenServers()
    {
        var admission = new ReadAdmission(TimeProvider.System, TimeSpan.Zero, null, maxHeavyReads: 1);
        var rig = new Rig(Start) { StopAfterDelays = 2, Admission = admission };
        var runningDuringWarm = new List<int>();
        rig.WarmOverride = (_, _, _) =>
        {
            runningDuringWarm.Add(admission.Stats().Running);
            return Task.CompletedTask;
        };

        await Warmers.RunAsync(rig.Build(TwoServers), rig.Cts.Token);

        Assert.Equal([1, 1], runningDuringWarm); // each server's read holds exactly one slot
        Assert.Equal(0, admission.Stats().Running); // and the pass leaves none held
        Assert.Equal(NextBoundary(Start + Warmers.StartDelay) - (Start + Warmers.StartDelay), rig.Delays[1]);
    }

    /// <summary>A fake clock, a recording delay that advances it, and a warm callback that records each call.</summary>
    private sealed class Rig
    {
        public Rig(DateTime now) => Now = now;

        public DateTime Now { get; private set; }
        public List<TimeSpan> Delays { get; } = new();
        public List<(int ServerId, string Name, DateTime At)> Warmed { get; } = new();
        public CancellationTokenSource Cts { get; } = new();
        public int StopAfterDelays { get; init; } = int.MaxValue;
        public bool Enabled { get; init; } = true;
        public ReadAdmission? Admission { get; init; }
        public Func<int, string, CancellationToken, Task>? WarmOverride { get; set; }

        public object Build(IReadOnlyList<(int ServerId, string Name)> servers) => Warmers.Create(
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

/// <summary>The warmer's surface, reached by name so this file compiles whether or not the service has it: a service
/// without one fails these tests at their first assert, not at build time.</summary>
internal static class Warmers
{
    private static readonly Type? WarmerType = typeof(DarlingWorker).Assembly.GetType("PerformanceMonitor.Darling.Service.DailySummaryWarmer");

    private static Type Required()
    {
        Assert.True(WarmerType is not null, "the service has no DailySummaryWarmer: nothing builds the closed-day block before a server's first visitor");
        return WarmerType!;
    }

    private static T Static<T>(string name) =>
        (T)Required().GetField(name, BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)!.GetValue(null)!;

    public static TimeSpan StartDelay => Static<TimeSpan>("StartDelay");
    public static TimeSpan PassMargin => Static<TimeSpan>("PassMargin");
    public static TimeSpan RetryDelay => Static<TimeSpan>("RetryDelay");

    /// <summary>False when the service has no warmer; the caller's own assert then shows what that costs.</summary>
    public static bool Exists => WarmerType is not null;

    public static object Create(
        Func<IReadOnlyList<(int ServerId, string Name)>> servers,
        Func<int, string, CancellationToken, Task> warm,
        Func<bool> enabled,
        ReadAdmission? admission,
        Func<DateTime>? clock = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        var type = Required();
        var ctor = Assert.Single(type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic));
        return ctor.Invoke([servers, warm, enabled, admission, NullLogger.Instance, clock, delay]);
    }

    /// <summary>The product's own warm read for one server: the Overview's range through the daily summary reader.</summary>
    public static async Task WarmAsync(Npgsql.NpgsqlDataSource postgres, int serverId, string name, CancellationToken ct)
    {
        var target = Required().GetMethod("WarmAsync", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
        Assert.True(target is not null, "DailySummaryWarmer has no WarmAsync");
        await (Task)target!.Invoke(null, [postgres, serverId, name, null, null, ct])!;
    }

    public static Task RunAsync(object warmer, CancellationToken ct) => Invoke(warmer, "RunAsync", ct);

    public static Task RunPassAsync(object warmer, CancellationToken ct) => Invoke(warmer, "RunPassAsync", ct);

    private static async Task Invoke(object warmer, string method, CancellationToken ct)
    {
        var target = Required().GetMethod(method, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        Assert.True(target is not null, $"DailySummaryWarmer has no {method}");
        await (Task)target!.Invoke(warmer, [ct])!;
    }
}
