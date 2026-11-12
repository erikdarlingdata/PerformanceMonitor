/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4710: unreachable servers must not use up the fleet's collection slots, and a command timeout must not
/// force a reconnect while the connection is still good.
/// </summary>
public sealed class ServerConnectBackoffTests
{
    [Fact]
    public void Delay_GrowsFromSixtySecondsToTheCap_AndHolds()
    {
        var seconds = Enumerable.Range(1, 8)
            .Select(failures => ServerConnectBackoff.NextDelay(failures, 0.5).TotalSeconds)
            .ToArray();

        Assert.Equal(new double[] { 60, 120, 240, 240, 240, 240, 240, 240 }, seconds);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(50)]
    public void Delay_JitterStaysInsideTheBand_AndTheLongestWaitIsUnderFiveMinutes(int failures)
    {
        var low = ServerConnectBackoff.NextDelay(failures, 0.0);
        var high = ServerConnectBackoff.NextDelay(failures, 1.0);
        var middle = ServerConnectBackoff.NextDelay(failures, 0.5);

        Assert.True(low < middle && middle < high, "jitter must spread the delay");
        Assert.Equal(middle.TotalSeconds * 0.8, low.TotalSeconds, 6);
        Assert.Equal(middle.TotalSeconds * 1.2, high.TotalSeconds, 6);
        Assert.True(high < TimeSpan.FromMinutes(5), "a 10-minute-class wait would delay recovery detection too long");
        Assert.True(middle >= TimeSpan.FromSeconds(60), "never faster than the old fixed retry");
    }

    [Fact]
    public void ADefinitionEdit_ResetsTheBackoff()
    {
        var loopState = typeof(DarlingWorker).GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;
        var state = NewLoopState(loopState, new MonitoredServer { Name = "s", Host = "old.invalid", StoredServerId = 7 });
        loopState.GetProperty("ConsecutiveConnectFailures")!.SetValue(state, 5);
        loopState.GetProperty("NextConnectAttempt")!.SetValue(state, DateTime.UtcNow.AddMinutes(5));

        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);
        typeof(DarlingWorker).GetField("_collectorFaultStacks", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, new CollectorFaultStackLog());

        var list = Activator.CreateInstance(typeof(List<>).MakeGenericType(loopState))!;
        list.GetType().GetMethod("Add")!.Invoke(list, new[] { state });
        var edited = new MonitoredServer { Name = "s", Host = "new.invalid", StoredServerId = 7 };
        typeof(DarlingWorker).GetMethod("ReconcileServers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(worker, new object[] { list, new List<MonitoredServer> { edited } });

        Assert.Equal(0, (int)loopState.GetProperty("ConsecutiveConnectFailures")!.GetValue(state)!);
        Assert.Equal(DateTime.MinValue, (DateTime)loopState.GetProperty("NextConnectAttempt")!.GetValue(state)!);
    }

    internal static object NewLoopState(Type loopState, MonitoredServer config)
    {
        var state = Activator.CreateInstance(loopState)!;
        loopState.GetProperty("Config")!.SetValue(state, config);
        return state;
    }

    internal static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return RepoFile.ReadRepoFile(Path.Combine(dir!, relative));
    }
}

public sealed class ServerConnectGateOccupancyTests
{
    [Fact]
    public async Task DeadServers_DoNotHoldFleetPermits_WhileTheirConnectsAreInFlight()
    {
        const int dead = 30;
        using var fleetGate = new SemaphoreSlim(4, 4);
        using var probeGate = new SemaphoreSlim(ServerConnectProbe.GateWidth, ServerConnectProbe.GateWidth);
        using var cts = new CancellationTokenSource();
        var inFlight = 0;

        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);
        typeof(DarlingWorker).GetField("_collectorFaultStacks", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, new CollectorFaultStackLog());
        typeof(DarlingWorker).GetField("_connectProbeGate", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, probeGate);
        /* A dead server: the connect parks (SqlClient's 15 s) and never answers. */
        worker.ConnectOverride = async (_, token) =>
        {
            Interlocked.Increment(ref inFlight);
            await Task.Delay(Timeout.Infinite, token);
            throw new InvalidOperationException("unreachable");
        };

        var loopState = typeof(DarlingWorker).GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;
        var process = typeof(DarlingWorker).GetMethod("ProcessServerSweepAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var bodies = Enumerable.Range(0, dead).Select(i =>
        {
            var state = ServerConnectBackoffTests.NewLoopState(loopState, new MonitoredServer { Name = $"dead-{i}", Host = $"dead-{i}.invalid" });
            return (Task)process.Invoke(worker, new object?[] { state, null, null, null, null, new DarlingConfig(), fleetGate, cts.Token })!;
        }).ToArray();

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (Volatile.Read(ref inFlight) < ServerConnectProbe.GateWidth && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }

        /* The connect gate is full of dead servers' attempts and the rest are queued behind it, yet not one
           of them holds, or is queued for, a fleet permit: healthy servers still get all four. */
        Assert.Equal(ServerConnectProbe.GateWidth, Volatile.Read(ref inFlight));
        Assert.Equal(4, fleetGate.CurrentCount);
        Assert.True(await fleetGate.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
        fleetGate.Release();

        await cts.CancelAsync();
        foreach (var body in bodies)
        {
            try
            {
                await body;
            }
            catch (OperationCanceledException)
            {
                /* Shutdown unwinding, as intended. */
            }
        }

        Assert.Equal(4, fleetGate.CurrentCount);
    }

    [Fact]
    public void TheWorker_ConnectsBeforeItTakesTheFleetPermit()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        var body = source.IndexOf("private async Task ProcessServerSweepAsync(", StringComparison.Ordinal);
        var attempt = source.IndexOf("ServerConnectProbe.AttemptAsync(", body, StringComparison.Ordinal);
        var fleetWait = source.IndexOf("await gate.WaitAsync(stoppingToken);", body, StringComparison.Ordinal);

        Assert.True(body > 0 && attempt > body, "the body must run the connect attempt through the connect gate");
        Assert.True(attempt < fleetWait, "the connect attempt must come before the fleet gate wait");
        Assert.DoesNotContain("DarlingServerConnector.ConnectAsync(server.Config, _logger, cancellationToken)", source, StringComparison.Ordinal);
    }
}

/// <summary>
/// #4710: the in-flight check's wording. What an operator reads there decides whether they raise the fleet's
/// slot count or look at a server, so the words are pinned.
/// </summary>
public sealed class SweepInFlightWordingTests
{
    [Fact]
    public void ABodyWaitingForAFleetSlot_KeepsTheQueuedWording()
    {
        Assert.Equal("queued for a slot", SweepInFlightWording.DebugState(running: false, runningSeconds: 0, connecting: false, connectSeconds: 0));
        Assert.Equal(
            "[{Server}] collection body has waited {Elapsed:F0}s for a free slot (fleet concurrency limit {Limit}) \u2014 queued, not stalled; it has not started yet",
            SweepInFlightWording.NotStartedInfoTemplate(connecting: false));
    }

    [Fact]
    public void ARunningBody_ShowsItsExecutionClock_EvenIfAConnectStampIsStillSet()
    {
        Assert.Equal("running 75s", SweepInFlightWording.DebugState(running: true, runningSeconds: 75.4, connecting: false, connectSeconds: 0));
        Assert.Equal("running 75s", SweepInFlightWording.DebugState(running: true, runningSeconds: 75.4, connecting: true, connectSeconds: 12));
    }

    [Fact]
    public void ABodyInsideItsConnectAttempt_IsNamedAsConnecting_NotAsQueuedForASlot()
    {
        Assert.Equal("connecting 75s", SweepInFlightWording.DebugState(running: false, runningSeconds: 0, connecting: true, connectSeconds: 75.4));

        var info = SweepInFlightWording.NotStartedInfoTemplate(connecting: true);
        Assert.Equal(
            "[{Server}] collection body has been connecting for {Elapsed:F0}s \u2014 connect attempts run outside the collection slots (at most {Gate} at once), so this is not the fleet concurrency limit; the server is slow to answer or down, or the attempt is waiting behind other connect attempts; the body has not started yet",
            info);
        Assert.DoesNotContain("free slot", info, StringComparison.Ordinal);
        Assert.DoesNotContain("{Limit}", info, StringComparison.Ordinal);
    }

    [Fact]
    public void TheConnectingLine_RendersWithTheConnectSecondsAndTheConnectGateWidth()
    {
        var logger = new CapturingTestLogger();
        logger.LogInformation(SweepInFlightWording.NotStartedInfoTemplate(connecting: true), "dead-1", 75.4, ServerConnectProbe.GateWidth);

        var line = Assert.Single(logger.Lines);
        Assert.Contains("[dead-1] collection body has been connecting for 75s", line, StringComparison.Ordinal);
        Assert.Contains($"(at most {ServerConnectProbe.GateWidth} at once)", line, StringComparison.Ordinal);
    }
}

/// <summary>
/// #4710: the connect attempt runs before the fleet permit, so the run stamp is still 0 for its whole length.
/// The body stamps the connect stage on a field of its own, so the in-flight check can tell a body inside a
/// connect attempt from one that is waiting for a fleet slot.
/// </summary>
public sealed class ServerConnectStageStampTests
{
    private static readonly Type LoopState = typeof(DarlingWorker).GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;

    private static long Stamp(object state, string field) => (long)LoopState.GetField(field)!.GetValue(state)!;

    private static DarlingWorker NewWorker(SemaphoreSlim probeGate, Func<MonitoredServer, CancellationToken, Task<ServerRuntime>> connect)
    {
        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);
        typeof(DarlingWorker).GetField("_collectorFaultStacks", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, new CollectorFaultStackLog());
        typeof(DarlingWorker).GetField("_connectProbeGate", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, probeGate);
        worker.ConnectOverride = connect;
        return worker;
    }

    private static Task StartBody(DarlingWorker worker, object state, SemaphoreSlim fleetGate, CancellationToken token)
    {
        var process = typeof(DarlingWorker).GetMethod("ProcessServerSweepAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)process.Invoke(worker, new object?[] { state, null, null, null, null, new DarlingConfig(), fleetGate, token })!;
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }

        return condition();
    }

    [Fact]
    public async Task ABodyInsideItsConnectAttempt_HasAConnectStampAndNoRunStamp_AndAShutdownCancelClearsIt()
    {
        using var fleetGate = new SemaphoreSlim(4, 4);
        using var probeGate = new SemaphoreSlim(ServerConnectProbe.GateWidth, ServerConnectProbe.GateWidth);
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource();
        var worker = NewWorker(probeGate, async (_, token) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
            throw new InvalidOperationException("unreachable");
        });
        var state = ServerConnectBackoffTests.NewLoopState(LoopState, new MonitoredServer { Name = "dead", Host = "dead.invalid" });

        var body = StartBody(worker, state, fleetGate, cts.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.NotEqual(0, Stamp(state, "ConnectStartedTicks"));
        Assert.Equal(0, Stamp(state, "RunStartedTicks"));

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => body);

        Assert.Equal(0, Stamp(state, "ConnectStartedTicks"));
        Assert.Equal(4, fleetGate.CurrentCount);
    }

    [Fact]
    public async Task WhenTheAttemptEnds_TheConnectStampClears_AndTheBodyIsQueuedForTheFleetPermit()
    {
        using var fleetGate = new SemaphoreSlim(0, 1);
        using var probeGate = new SemaphoreSlim(ServerConnectProbe.GateWidth, ServerConnectProbe.GateWidth);
        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource();
        var worker = NewWorker(probeGate, (_, _) =>
        {
            entered.TrySetResult();
            throw new InvalidOperationException("down");
        });
        var state = ServerConnectBackoffTests.NewLoopState(LoopState, new MonitoredServer { Name = "down", Host = "down.invalid" });

        var body = StartBody(worker, state, fleetGate, cts.Token);

        /* The override runs after the stamp, so "entered and clear" means the stamp was set and then cleared. */
        Assert.True(await WaitUntilAsync(() => entered.Task.IsCompleted && Stamp(state, "ConnectStartedTicks") == 0));

        /* The attempt has ended and the body waits for the fleet permit, which nothing releases: neither stamp
           is set, which is the "queued for a slot" state. */
        Assert.Equal(0, Stamp(state, "RunStartedTicks"));
        Assert.False(body.IsCompleted);

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => body);
        Assert.Equal(0, Stamp(state, "ConnectStartedTicks"));
    }

    [Fact]
    public void TheWorker_StampsTheConnectStageAroundTheAttempt_ClearsItInAFinally_AndResetsItAtLaunch()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        var body = source.IndexOf("private async Task ProcessServerSweepAsync(", StringComparison.Ordinal);
        var stamp = source.IndexOf("Interlocked.Exchange(ref server.ConnectStartedTicks, DateTime.UtcNow.Ticks);", body, StringComparison.Ordinal);
        var attempt = source.IndexOf("ServerConnectProbe.AttemptAsync(", body, StringComparison.Ordinal);
        var clear = source.IndexOf("Interlocked.Exchange(ref server.ConnectStartedTicks, 0);", attempt, StringComparison.Ordinal);
        var fleetWait = source.IndexOf("await gate.WaitAsync(stoppingToken);", body, StringComparison.Ordinal);

        Assert.True(body > 0 && stamp > body && stamp < attempt, "the stamp must come just before the connect attempt");
        Assert.True(attempt < clear && clear < fleetWait, "the stamp must be cleared before the fleet permit wait");
        Assert.Contains("finally", source.Substring(attempt, clear - attempt), StringComparison.Ordinal);

        var launch = source.IndexOf("server.InFlightSweep = ProcessServerSweepAsync(", StringComparison.Ordinal);
        var reset = source.LastIndexOf("Interlocked.Exchange(ref server.ConnectStartedTicks, 0);", launch, StringComparison.Ordinal);
        Assert.True(reset > 0 && launch - reset < 300, "the launch must reset the connect stamp beside the run stamp, before it starts the body");
    }

    [Fact]
    public void TheInFlightCheck_ReadsTheConnectStampOnlyForABodyThatIsNotRunning_AndWordsTheInfoFromIt()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();

        Assert.Contains("var connectStartedTicks = running ? 0 : Interlocked.Read(ref server.ConnectStartedTicks);", source, StringComparison.Ordinal);
        Assert.Contains("SweepInFlightWording.DebugState(running, runningSeconds, connecting, connectSeconds)", source, StringComparison.Ordinal);
        Assert.Contains("SweepInFlightWording.NotStartedInfoTemplate(connecting)", source, StringComparison.Ordinal);
        Assert.Contains("connecting ? ServerConnectProbe.GateWidth : EffectiveSweepWidth", source, StringComparison.Ordinal);
    }
}

/// <summary>
/// #4710: a connect attempt is made before the fleet permit is taken, so a reload can replace the server's
/// definition while the attempt runs or while the body waits for the permit. The install step must not put a
/// runtime built from the OLD definition on a server that has since been edited, nor back off the NEW
/// definition because the old one failed.
/// </summary>
public sealed class ServerConnectDefinitionEditTests
{
    private static readonly Type LoopState = typeof(DarlingWorker).GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;

    private static DarlingWorker NewWorker()
    {
        var worker = (DarlingWorker)RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);
        typeof(DarlingWorker).GetField("_collectorFaultStacks", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, new CollectorFaultStackLog());
        typeof(DarlingWorker).GetField("_connectProbeGate", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, new SemaphoreSlim(ServerConnectProbe.GateWidth, ServerConnectProbe.GateWidth));
        return worker;
    }

    private static ServerRuntime RuntimeFor(MonitoredServer config) => new()
    {
        Config = config,
        ConnectionString = "Server=s.invalid;Integrated Security=true",
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
        StorageName = "s",
        ServerId = 7,
    };

    private static void Set(object state, string name, object? value) => LoopState.GetProperty(name)!.SetValue(state, value);

    private static object? Get(object state, string name) => LoopState.GetProperty(name)!.GetValue(state);

    /// <summary>
    /// Calls TryConnectAsync with the definition the server holds NOW (<paramref name="current"/>) and an
    /// attempt made with <paramref name="attempted"/>. Retired is set so a runtime that IS installed is dropped
    /// again at the retired re-check, right after the install bookkeeping and before any store work: the
    /// seeded failure count (5) and failure text are what tell an install from a discard.
    /// </summary>
    private static async Task<object> TryConnectAsync(MonitoredServer current, MonitoredServer attempted, bool failed, ServerRuntime? attemptRuntime = null)
    {
        var state = ServerConnectBackoffTests.NewLoopState(LoopState, current);
        Set(state, "ConsecutiveConnectFailures", 5);
        Set(state, "NextConnectAttempt", DateTime.UtcNow.AddMinutes(5));
        Set(state, "LastConnectFailureLogged", "earlier failure");
        LoopState.GetField("Retired")!.SetValue(state, true);

        var attempt = new ConnectAttempt(
            attempted,
            failed ? null : attemptRuntime ?? RuntimeFor(attempted),
            failed ? new InvalidOperationException("unreachable") : null);
        var tryConnect = typeof(DarlingWorker).GetMethod("TryConnectAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        await (Task)tryConnect.Invoke(NewWorker(), new object?[] { state, null, new DarlingConfig(), attempt, CancellationToken.None })!;
        return state;
    }

    [Fact]
    public async Task ConnectionEditAfterTheAttempt_InstallsNothing_AndConnectsAgainAtOnce()
    {
        var state = await TryConnectAsync(
            current: new MonitoredServer { Name = "s", Host = "new.invalid", StoredServerId = 7 },
            attempted: new MonitoredServer { Name = "s", Host = "old.invalid", StoredServerId = 7 },
            failed: false);

        Assert.Null(Get(state, "Runtime"));
        Assert.Equal(DateTime.MinValue, (DateTime)Get(state, "NextConnectAttempt")!);
        Assert.Equal(5, (int)Get(state, "ConsecutiveConnectFailures")!);
        Assert.Equal("earlier failure", (string?)Get(state, "LastConnectFailureLogged"));
    }

    [Fact]
    public async Task NonConnectionEditAfterTheAttempt_StillInstallsTheRuntime()
    {
        /* A cost (or alert-delivery) edit replaces the held definition too. ServerDefinitionEquals ignores
           it, so this attempt is still good: comparing references would throw away every connect that raced a
           FinOps edit. */
        var state = await TryConnectAsync(
            current: new MonitoredServer { Name = "s", Host = "same.invalid", StoredServerId = 7, MonthlyCostUsd = 900m },
            attempted: new MonitoredServer { Name = "s", Host = "same.invalid", StoredServerId = 7, MonthlyCostUsd = 100m },
            failed: false);

        Assert.Equal(0, (int)Get(state, "ConsecutiveConnectFailures")!);
        Assert.Null(Get(state, "LastConnectFailureLogged"));
    }

    [Fact]
    public async Task APlanCaptureEditDuringTheAttempt_IsOnTheInstalledRuntime_NotTheValueTheAttemptWasBuiltWith()
    {
        /* #6771: capture_plans is outside ServerDefinitionEquals (an edit must not reconnect), so this attempt is still good and
           installs. The runtime it carries was stamped from the attempt's definition (the old value); ReconcileServers had no runtime
           to re-stamp while the body waited, so the install itself must take the held definition's value. */
        var attemptRuntime = RuntimeFor(new MonitoredServer { Name = "s", Host = "same.invalid", StoredServerId = 7, CapturePlans = null });
        attemptRuntime.CapturePlans = null;

        await TryConnectAsync(
            current: new MonitoredServer { Name = "s", Host = "same.invalid", StoredServerId = 7, CapturePlans = false },
            attempted: new MonitoredServer { Name = "s", Host = "same.invalid", StoredServerId = 7, CapturePlans = null },
            failed: false,
            attemptRuntime);

        Assert.False(attemptRuntime.CapturePlans);
    }

    [Fact]
    public async Task FailedAttemptWithTheOldDefinition_DoesNotBackOffTheNewOne()
    {
        var state = await TryConnectAsync(
            current: new MonitoredServer { Name = "s", Host = "new.invalid", StoredServerId = 7 },
            attempted: new MonitoredServer { Name = "s", Host = "old.invalid", StoredServerId = 7 },
            failed: true);

        Assert.Equal(DateTime.MinValue, (DateTime)Get(state, "NextConnectAttempt")!);
        Assert.Equal(5, (int)Get(state, "ConsecutiveConnectFailures")!);
        Assert.Equal("earlier failure", (string?)Get(state, "LastConnectFailureLogged"));
    }

    [Fact]
    public async Task ADefinitionEditDuringTheAttempt_IsConnectedAgainWithTheNewDefinitionOnTheNextBody()
    {
        var original = new MonitoredServer { Name = "s", Host = "old.invalid", StoredServerId = 7 };
        var edited = new MonitoredServer { Name = "s", Host = "new.invalid", StoredServerId = 7 };
        var state = ServerConnectBackoffTests.NewLoopState(LoopState, original);
        var list = Activator.CreateInstance(typeof(List<>).MakeGenericType(LoopState))!;
        list.GetType().GetMethod("Add")!.Invoke(list, new[] { state });

        var worker = NewWorker();
        var hosts = new List<string>();
        worker.ConnectOverride = (target, _) =>
        {
            hosts.Add(target.Host);
            if (hosts.Count == 1)
            {
                /* The reload lands while the first attempt is in flight. */
                typeof(DarlingWorker).GetMethod("ReconcileServers", BindingFlags.NonPublic | BindingFlags.Instance)!
                    .Invoke(worker, new object[] { list, new List<MonitoredServer> { edited } });
            }

            return Task.FromResult(RuntimeFor(target));
        };

        var process = typeof(DarlingWorker).GetMethod("ProcessServerSweepAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        using var fleetGate = new SemaphoreSlim(4, 4);
        for (var body = 0; body < 2; body++)
        {
            await (Task)process.Invoke(worker, new object?[] { state, null, null, null, null, new DarlingConfig(), fleetGate, CancellationToken.None })!;
        }

        Assert.Equal(new[] { "old.invalid", "new.invalid" }, hosts);
    }
}

public sealed class ConnectionFaultDispositionTests
{
    private static ServerRuntime SqlRuntime() => new()
    {
        Config = new MonitoredServer { Name = "s", Host = "s.invalid" },
        ConnectionString = "Server=s.invalid;Integrated Security=true",
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
        StorageName = "s",
        ServerId = 1,
    };

    [Fact]
    public async Task CommandTimeout_WithAHealthyProbe_KeepsTheRuntime()
    {
        var probes = 0;
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(-2, 11), SqlRuntime(),
            (_, _) => { probes++; return Task.FromResult(true); }, TestContext.Current.CancellationToken);

        Assert.False(drop);
        Assert.Equal(1, probes);
    }

    [Fact]
    public async Task CommandTimeout_WithAFailedProbe_DropsTheRuntime()
    {
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(-2, 11), SqlRuntime(),
            (_, _) => Task.FromResult(false), TestContext.Current.CancellationToken);

        Assert.True(drop);
    }

    [Fact]
    public async Task FatalClass_DropsAtOnce_WithoutProbing()
    {
        var probes = 0;
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(4060, 20), SqlRuntime(),
            (_, _) => { probes++; return Task.FromResult(true); }, TestContext.Current.CancellationToken);

        Assert.True(drop);
        Assert.Equal(0, probes);
    }

    [Fact]
    public async Task OrdinaryQueryError_KeepsTheRuntime_WithoutProbing()
    {
        var probes = 0;
        var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            SqlExceptionFactory.Create(8134, 16), SqlRuntime(),
            (_, _) => { probes++; return Task.FromResult(true); }, TestContext.Current.CancellationToken);

        Assert.False(drop);
        Assert.Equal(0, probes);
    }

    private static readonly DateTime T0 = new(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ASecondTimeout_InsideTheReuseWindow_ReusesTheVerdict_WithoutAnotherLogin(bool alive)
    {
        var memo = new LivenessProbeMemo();
        var probes = 0;
        var outcome = alive ? LivenessProbeOutcome.LoggedIn : LivenessProbeOutcome.Dead;
        Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> probe = (_, _) => { probes++; return Task.FromResult(outcome); };
        var ct = TestContext.Current.CancellationToken;

        var first = await memo.ProbeAsync(SqlRuntime(), probe, T0, ct);
        var second = await memo.ProbeAsync(SqlRuntime(), probe, T0.AddSeconds(LivenessProbeMemo.ReuseSeconds - 1), ct);

        Assert.Equal(alive, first);
        Assert.Equal(alive, second);
        Assert.Equal(1, probes);
    }

    [Fact]
    public async Task ATimeout_PastTheReuseWindow_ProbesAgain_AndTakesTheNewVerdict()
    {
        var memo = new LivenessProbeMemo();
        var answers = new Queue<LivenessProbeOutcome>([LivenessProbeOutcome.LoggedIn, LivenessProbeOutcome.Dead]);
        var probes = 0;
        Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> probe = (_, _) => { probes++; return Task.FromResult(answers.Dequeue()); };
        var ct = TestContext.Current.CancellationToken;

        Assert.True(await memo.ProbeAsync(SqlRuntime(), probe, T0, ct));
        Assert.False(await memo.ProbeAsync(SqlRuntime(), probe, T0.AddSeconds(LivenessProbeMemo.ReuseSeconds), ct));
        Assert.Equal(2, probes);
    }

    [Fact]
    public async Task AClockThatSteppedBack_ProbesAgain()
    {
        var memo = new LivenessProbeMemo();
        var probes = 0;
        Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> probe = (_, _) => { probes++; return Task.FromResult(LivenessProbeOutcome.LoggedIn); };
        var ct = TestContext.Current.CancellationToken;

        await memo.ProbeAsync(SqlRuntime(), probe, T0, ct);
        await memo.ProbeAsync(SqlRuntime(), probe, T0.AddSeconds(-1), ct);

        Assert.Equal(2, probes);
    }

    [Fact]
    public async Task ThreeTimeoutsInARow_CostOneProbeLogin_ThroughTheDisposition()
    {
        var memo = new LivenessProbeMemo();
        var probes = 0;
        Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> probe = (_, _) => { probes++; return Task.FromResult(LivenessProbeOutcome.LoggedIn); };
        var ct = TestContext.Current.CancellationToken;

        for (var i = 0; i < 3; i++)
        {
            var at = T0.AddSeconds(i * 5);
            var drop = await ConnectionFaultDisposition.ShouldDropRuntimeAsync(
                SqlExceptionFactory.Create(-2, 11), SqlRuntime(),
                (runtime, token) => memo.ProbeAsync(runtime, probe, at, token), ct);
            Assert.False(drop);
        }

        Assert.Equal(1, probes);
        Assert.NotNull(memo.Last);
        Assert.True(memo.Last!.Alive);
    }

    [Fact]
    public async Task ClearingTheMemo_ForgetsTheVerdict_SoTheNextTimeoutProbes()
    {
        var memo = new LivenessProbeMemo();
        var probes = 0;
        Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> probe = (_, _) => { probes++; return Task.FromResult(LivenessProbeOutcome.LoggedIn); };
        var ct = TestContext.Current.CancellationToken;

        await memo.ProbeAsync(SqlRuntime(), probe, T0, ct);
        memo.Clear();

        Assert.Null(memo.Last);
        await memo.ProbeAsync(SqlRuntime(), probe, T0.AddSeconds(1), ct);
        Assert.Equal(2, probes);
    }

    [Fact]
    public async Task AProbeThatReturnsAfterTheMemoWasCleared_LeavesNeitherStreakNorVerdictBehind()
    {
        // #7851: run A's Dead verdict clears the memo while run B's probe is still in flight. B's late TimedOut is
        // about the connection that was dropped, so it must not start the reconnected server at 1 of 3 or hold a verdict.
        var memo = new LivenessProbeMemo();
        var held = new TaskCompletionSource<LivenessProbeOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ct = TestContext.Current.CancellationToken;

        var b = memo.ProbeAsync(SqlRuntime(), (_, _) => held.Task, T0, ct);
        memo.Clear();
        held.SetResult(LivenessProbeOutcome.TimedOut);

        Assert.True(await b); // B still gets its own answer
        Assert.Equal(0, memo.TimedOutProbesInARow);
        Assert.Null(memo.Last);

        var probes = 0;
        await memo.ProbeAsync(SqlRuntime(), (_, _) => { probes++; return Task.FromResult(LivenessProbeOutcome.LoggedIn); }, T0.AddSeconds(1), ct);
        Assert.Equal(1, probes);
    }

    [Fact]
    public async Task AClearThatLandsBetweenTheGenerationCheckAndTheWrite_IsNotFollowedByAStaleWrite()
    {
        // #7851: the generation compare and the writes run under one lock, so a Clear that arrives between them waits
        // and runs after the write. Without the lock it runs first and the probe's write lands in the new generation.
        var memo = new LivenessProbeMemo();
        Task? clear = null;
        memo.BetweenCheckAndWriteForTests = () =>
        {
            clear = Task.Run(memo.Clear);
            clear.Wait(TimeSpan.FromMilliseconds(100)); // blocks on the lock while the probe holds it
        };

        await memo.ProbeAsync(SqlRuntime(), (_, _) => Task.FromResult(LivenessProbeOutcome.TimedOut), T0, TestContext.Current.CancellationToken);
        await clear!;

        Assert.Null(memo.Last);
        Assert.Equal(0, memo.TimedOutProbesInARow);
    }

    [Fact]
    public async Task AProbeThatLogsInAfterTheMemoWasCleared_LeavesTheNewGenerationsStreakAndVerdictAlone()
    {
        // The stale LoggedIn must not end the streak the reconnected server has built since.
        var memo = new LivenessProbeMemo();
        var held = new TaskCompletionSource<LivenessProbeOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ct = TestContext.Current.CancellationToken;

        var stale = memo.ProbeAsync(SqlRuntime(), (_, _) => held.Task, T0, ct);
        memo.Clear();
        await memo.ProbeAsync(SqlRuntime(), (_, _) => Task.FromResult(LivenessProbeOutcome.TimedOut), T0, ct);
        var verdict = memo.Last;
        held.SetResult(LivenessProbeOutcome.LoggedIn);

        Assert.True(await stale);
        Assert.Equal(1, memo.TimedOutProbesInARow);
        Assert.Same(verdict, memo.Last);
        Assert.Equal(LivenessProbeOutcome.TimedOut, memo.Last?.Outcome);
    }

    /// <summary>The real probe with its login replaced by one that throws <paramref name="fault"/>.</summary>
    private static Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> ProbeWhoseLoginThrows(Exception fault) =>
        (runtime, token) => ConnectionFaultDisposition.ProbeFreshConnectionAsync(runtime, (_, _) => Task.FromException(fault), token);

    /// <summary>The real probe with its login replaced by one that completes.</summary>
    private static readonly Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> ProbeWhoseLoginSucceeds =
        (runtime, token) => ConnectionFaultDisposition.ProbeFreshConnectionAsync(runtime, (_, _) => Task.CompletedTask, token);

    private static SqlException Timeout() => SqlExceptionFactory.Create(-2, 11);

    /// <summary>
    /// The production wiring short of the worker: a command timeout settled by <see cref="ConnectionFaultDisposition.ShouldDropRuntimeAsync"/>
    /// through <paramref name="memo"/> as of <paramref name="at"/>, with <paramref name="probe"/> standing where the real login would.
    /// True when the runtime is to be dropped.
    /// </summary>
    private static Task<bool> SettleTimeoutAsync(LivenessProbeMemo memo, Func<ServerRuntime, CancellationToken, Task<LivenessProbeOutcome>> probe, DateTime at) =>
        ConnectionFaultDisposition.ShouldDropRuntimeAsync(
            Timeout(), SqlRuntime(),
            (runtime, token) => memo.ProbeAsync(runtime, probe, at, token), TestContext.Current.CancellationToken);

    /// <summary>The instant of the n-th timeout when they arrive one memo window apart, so each one probes afresh.</summary>
    private static DateTime Window(int n) => T0.AddSeconds(n * LivenessProbeMemo.ReuseSeconds);

    [Fact]
    public async Task TheProbe_TellsATimedOutLoginFromADeadServer_AndFromOneThatLoggedIn()
    {
        var ct = TestContext.Current.CancellationToken;

        Assert.Equal(LivenessProbeOutcome.LoggedIn, await ProbeWhoseLoginSucceeds(SqlRuntime(), ct));
        Assert.Equal(LivenessProbeOutcome.TimedOut, await ProbeWhoseLoginThrows(Timeout())(SqlRuntime(), ct));
        Assert.Equal(LivenessProbeOutcome.Dead, await ProbeWhoseLoginThrows(SqlExceptionFactory.Create(10061, 20))(SqlRuntime(), ct));
        Assert.Equal(LivenessProbeOutcome.Dead, await ProbeWhoseLoginThrows(SqlExceptionFactory.Create(53, 20))(SqlRuntime(), ct));
        Assert.Equal(LivenessProbeOutcome.Dead, await ProbeWhoseLoginThrows(SqlExceptionFactory.Create(18456, 14))(SqlRuntime(), ct));
        Assert.Equal(LivenessProbeOutcome.Dead, await ProbeWhoseLoginThrows(new InvalidOperationException("not a driver fault"))(SqlRuntime(), ct));
    }

    [Fact]
    public async Task CommandTimeout_WhenTheProbesOwnLoginTimesOut_KeepsTheRuntime()
    {
        var drop = await SettleTimeoutAsync(new LivenessProbeMemo(), ProbeWhoseLoginThrows(Timeout()), T0);

        Assert.False(drop);
    }

    [Theory]
    [InlineData(10061, 20)]
    [InlineData(53, 20)]
    [InlineData(18456, 14)]
    public async Task CommandTimeout_WhenTheProbeIsRefusedOrFailsToLogIn_DropsTheRuntime(int number, byte errorClass)
    {
        var drop = await SettleTimeoutAsync(new LivenessProbeMemo(), ProbeWhoseLoginThrows(SqlExceptionFactory.Create(number, errorClass)), T0);

        Assert.True(drop);
    }

    [Fact]
    public async Task CommandTimeout_WhenTheProbeFaultsOutsideTheDriver_DropsTheRuntime()
    {
        var drop = await SettleTimeoutAsync(new LivenessProbeMemo(), ProbeWhoseLoginThrows(new InvalidOperationException("not a driver fault")), T0);

        Assert.True(drop);
    }

    [Fact]
    public async Task CommandTimeout_WhenTheProbeLogsIn_KeepsTheRuntime()
    {
        var drop = await SettleTimeoutAsync(new LivenessProbeMemo(), ProbeWhoseLoginSucceeds, T0);

        Assert.False(drop);
    }

    [Fact]
    public async Task TimeoutsWhoseProbeLoginsAllTimeOut_PastTheMemoWindow_KeepTheRuntimeThreeTimes_AndDropItOnTheFourth()
    {
        var memo = new LivenessProbeMemo();
        var timedOut = ProbeWhoseLoginThrows(Timeout());

        for (var n = 0; n < LivenessProbeMemo.MaxTimedOutProbesInARow; n++)
        {
            Assert.False(await SettleTimeoutAsync(memo, timedOut, Window(n)));
            Assert.Equal(n + 1, memo.TimedOutProbesInARow);
            Assert.Equal(LivenessProbeOutcome.TimedOut, memo.Last!.Outcome);
        }

        Assert.True(await SettleTimeoutAsync(memo, timedOut, Window(LivenessProbeMemo.MaxTimedOutProbesInARow)));
        Assert.False(memo.Last!.Alive);
        Assert.Equal(3, LivenessProbeMemo.MaxTimedOutProbesInARow);
    }

    [Fact]
    public async Task ACompletedRun_EndsTheTimedOutStreak_SoTheNextTimedOutProbeKeepsTheRuntime()
    {
        var memo = new LivenessProbeMemo();
        var timedOut = ProbeWhoseLoginThrows(Timeout());
        for (var n = 0; n < LivenessProbeMemo.MaxTimedOutProbesInARow; n++)
        {
            Assert.False(await SettleTimeoutAsync(memo, timedOut, Window(n)));
        }

        memo.RunCompleted();

        Assert.Equal(0, memo.TimedOutProbesInARow);
        Assert.False(await SettleTimeoutAsync(memo, timedOut, Window(LivenessProbeMemo.MaxTimedOutProbesInARow)));
        Assert.Equal(1, memo.TimedOutProbesInARow);
    }

    [Fact]
    public async Task AProbeThatLogsIn_EndsTheTimedOutStreak_SoTheNextTimedOutProbeKeepsTheRuntime()
    {
        var memo = new LivenessProbeMemo();
        var timedOut = ProbeWhoseLoginThrows(Timeout());
        for (var n = 0; n < LivenessProbeMemo.MaxTimedOutProbesInARow; n++)
        {
            Assert.False(await SettleTimeoutAsync(memo, timedOut, Window(n)));
        }

        Assert.False(await SettleTimeoutAsync(memo, ProbeWhoseLoginSucceeds, Window(LivenessProbeMemo.MaxTimedOutProbesInARow)));
        Assert.Equal(0, memo.TimedOutProbesInARow);

        Assert.False(await SettleTimeoutAsync(memo, timedOut, Window(LivenessProbeMemo.MaxTimedOutProbesInARow + 1)));
        Assert.Equal(1, memo.TimedOutProbesInARow);
    }

    /// <summary>A worker whose store nothing listens on: the collection_log write fails and is swallowed, as it is for any observability write.</summary>
    private static DarlingWorker NewWorker(LivenessProbeOutcome probeFinds, ILogger<DarlingWorker>? logger = null)
    {
        var worker = new DarlingWorker(
            logger ?? NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1"),
            LivenessProbeOverride = (_, _) => Task.FromResult(probeFinds),
        };
        worker.SelfAlertsForTests = new DarlingSelfAlertEvaluator(
            new DarlingSelfAlertTests.FakeSettings(), new DarlingSelfAlertTests.RecordingDeliverer(),
            new DarlingSelfAlertTests.FakeHistoryStore(), _ => false);
        return worker;
    }

    private static DarlingCollectorRunner NewRunner() => new(
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x"),
        new CollectorDeltaCalculator(),
        new CapturingTestLogger(),
        databaseScope: (_, _) => new List<string>(),
        separatelyMonitoredDatabases: _ => new List<string>(),
        installId: () => "x");

    /// <summary>One collector run through the worker's real RunOneAsync, with the collector's own dispatch replaced by <paramref name="dispatch"/>.</summary>
    private static async Task RunOneAsync(DarlingWorker worker, DarlingWorker.ServerLoopState server, Func<Task<CollectorRunResult>> dispatch)
    {
        worker.DispatchOverrideForTests = (_, _, _) => dispatch();
        var runOne = typeof(DarlingWorker).GetMethod("RunOneAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        await (Task<int>)runOne.Invoke(
            worker, [server, NewRunner(), "wait_stats", null, TestContext.Current.CancellationToken, false, null, null, null])!;
    }

    [Fact]
    public async Task TheWorker_KeepsTheRuntime_WhenTheProbesOwnLoginTimesOut_AndACompletedRunEndsTheStreak()
    {
        var worker = NewWorker(LivenessProbeOutcome.TimedOut);
        var runtime = SqlRuntime();
        var server = new DarlingWorker.ServerLoopState { Config = runtime.Config, Runtime = runtime };

        await RunOneAsync(worker, server, () => throw Timeout());

        Assert.Same(runtime, server.Runtime);
        Assert.Equal(DateTime.MinValue, server.NextConnectAttempt);
        Assert.Equal(LivenessProbeOutcome.TimedOut, server.Liveness.Last?.Outcome);
        Assert.Equal(1, server.Liveness.TimedOutProbesInARow);

        await RunOneAsync(worker, server, () => Task.FromResult(new CollectorRunResult(3, 5, 7, [])));

        Assert.Same(runtime, server.Runtime);
        Assert.Equal(0, server.Liveness.TimedOutProbesInARow);
    }

    [Fact]
    public async Task TheWorker_DropsTheRuntime_ClearsTheMemo_AndArmsTheReconnect_WhenTheProbeFindsTheServerDead()
    {
        var worker = NewWorker(LivenessProbeOutcome.Dead);
        var runtime = SqlRuntime();
        var server = new DarlingWorker.ServerLoopState { Config = runtime.Config, Runtime = runtime };
        var before = DateTime.UtcNow;

        await RunOneAsync(worker, server, () => throw Timeout());

        Assert.Null(server.Runtime);
        Assert.Null(server.Liveness.Last);
        Assert.True(server.NextConnectAttempt >= before.AddSeconds(60));
    }

    [Fact]
    public async Task TheWorker_NamesTheOtherRunsDrop_NotAKeptConnection_WhenTheProbeTimesOutAfterTheMemoWasCleared()
    {
        // #7851: another run dropped the runtime and cleared the memo while this run's probe was in flight. The probe's own
        // login timed out, and the connection is already gone, so "keeping the connection" would be false twice over.
        var log = new CapturingTestLogger();
        var worker = NewWorker(LivenessProbeOutcome.TimedOut, new WorkerLog(log));
        var runtime = SqlRuntime();
        var server = new DarlingWorker.ServerLoopState { Config = runtime.Config, Runtime = runtime };
        worker.LivenessProbeOverride = (_, _) =>
        {
            server.Liveness.Clear();
            return Task.FromResult(LivenessProbeOutcome.TimedOut);
        };

        await RunOneAsync(worker, server, () => throw Timeout());

        Assert.Same(runtime, server.Runtime); // no second drop
        Assert.Contains(log.Lines, line => line.Contains("another run already dropped and reset the connection") && line.Contains("timed out"));
        Assert.DoesNotContain(log.Lines, line => line.Contains("keeping the connection"));
    }

    [Fact]
    public async Task TheWorker_LogsTheProbesStreak_WhenItsOwnLoginTimesOutOnTheCurrentConnection()
    {
        var log = new CapturingTestLogger();
        var worker = NewWorker(LivenessProbeOutcome.TimedOut, new WorkerLog(log));
        var runtime = SqlRuntime();
        var server = new DarlingWorker.ServerLoopState { Config = runtime.Config, Runtime = runtime };

        await RunOneAsync(worker, server, () => throw Timeout());

        Assert.Contains(log.Lines, line => line.Contains("so did the liveness probe's own login") && line.Contains("probe 1 of 3"));
    }

    /// <summary>The worker's typed logger over a <see cref="CapturingTestLogger"/>.</summary>
    private sealed class WorkerLog(CapturingTestLogger inner) : ILogger<DarlingWorker>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>SqlException has no public constructor; this builds one through the driver's internals.</summary>
    private static class SqlExceptionFactory
    {
        public static SqlException Create(int number, byte errorClass)
        {
            const BindingFlags all = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static;
            var errorCtor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .OrderByDescending(c => c.GetParameters().Length).First();
            var ints = new Queue<object>(new object[] { number, 0, 0, 0 });
            var bytes = new Queue<object>(new object[] { (byte)0, errorClass, (byte)0, (byte)0 });
            var strings = new Queue<object>(new object[] { "server", "message", "procedure", "extra", "extra" });
            var args = errorCtor.GetParameters().Select(p =>
                p.ParameterType == typeof(int) ? ints.Dequeue()
                : p.ParameterType == typeof(byte) ? bytes.Dequeue()
                : p.ParameterType == typeof(string) ? strings.Dequeue()
                : p.ParameterType == typeof(uint) ? (object)0u
                : p.ParameterType == typeof(Exception) ? null!
                : p.HasDefaultValue ? p.DefaultValue! : throw new InvalidOperationException($"unmapped SqlError ctor parameter {p.ParameterType}")).ToArray();
            var error = errorCtor.Invoke(args);

            var collection = Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
            typeof(SqlErrorCollection).GetMethod("Add", all, null, new[] { typeof(SqlError) }, null)!.Invoke(collection, new[] { error });

            var create = typeof(SqlException).GetMethods(all)
                .Where(m => m.Name == "CreateException" && m.GetParameters().Length >= 2
                    && m.GetParameters()[0].ParameterType == typeof(SqlErrorCollection)
                    && m.GetParameters()[1].ParameterType == typeof(string))
                .OrderBy(m => m.GetParameters().Length).First();
            var createArgs = create.GetParameters().Select((p, i) =>
                i == 0 ? collection
                : i == 1 ? "11.0"
                : p.ParameterType == typeof(Guid) ? Guid.Empty
                : p.HasDefaultValue ? p.DefaultValue! : null!).ToArray();
            return (SqlException)create.Invoke(null, createArgs)!;
        }
    }
}
