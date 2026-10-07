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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5479: the per-server self-alerts (Collection Stopped and its siblings) run in a fleet-level pass the sweep loop
/// launches every tick, not at the top of each server's collection body. A body the memory launch guard held off, or one
/// that never finishes, used to silence them: a fleet that stopped collecting paged nobody for 25 hours.
/// </summary>
public sealed class SelfAlertFleetPassTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(5);

    private sealed class WorkerLogger(CapturingTestLogger inner) : ILogger<DarlingWorker>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => inner.Log(logLevel, eventId, state, exception, formatter);
    }

    internal static DarlingWorker MakeWorker(CapturingTestLogger? logger = null) =>
        new(
            logger is null ? NullLogger<DarlingWorker>.Instance : new WorkerLogger(logger),
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());

    internal static DarlingWorker.ServerLoopState MakeServer(string name, bool connected = false)
    {
        var config = new MonitoredServer { Name = name, Host = name + ".invalid" };
        return new DarlingWorker.ServerLoopState
        {
            Config = config,
            Runtime = connected
                ? new ServerRuntime
                {
                    Config = config,
                    ConnectionString = "Server=tcp:" + name + ",1433;Initial Catalog=master;Encrypt=True",
                    Target = new CollectorTargetInfo(),
                    StorageName = name,
                    ServerId = 7,
                }
                : null,
        };
    }

    internal static async Task<bool> BecomesTrueAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        return condition();
    }

    /// <summary>
    /// Collection Stopped during a hold: no body is launched and none is ever called, yet the server is evaluated. On the
    /// old shape the evaluation was the first thing the body did, so a held body meant no evaluation at all.
    /// </summary>
    [Fact]
    public async Task AServerWithNoBodyLaunched_IsStillEvaluatedByTheFleetPass()
    {
        var worker = MakeWorker();
        var evaluated = new List<string>();
        worker.SelfAlertServerOverride = (server, _) =>
        {
            lock (evaluated)
            {
                evaluated.Add(server.Config.DisplayName);
            }

            return Task.CompletedTask;
        };
        var servers = new[] { MakeServer("example-sql-01"), MakeServer("example-sql-02") };

        Assert.True(worker.TryStartSelfAlertPass(servers, TestContext.Current.CancellationToken));

        Assert.True(await BecomesTrueAsync(() => { lock (evaluated) { return evaluated.Count == 2; } }));
        Assert.All(servers, s => Assert.Null(s.InFlightSweep));
    }

    /// <summary>
    /// The two halves of #5479 together: the memory launch guard is forced to hold (through its test seam, a sampler that
    /// reads over the line and a collection that frees nothing), a server has had no success for longer than the
    /// Collection Stopped window, and Collection Stopped fires from the fleet pass while the guard still holds and no body
    /// has been launched or called. On the old shape the alert was the first statement of the body the guard held off.
    /// The store read is the one piece stood in for: the evaluator's own judgement and edge apply run on a last success
    /// three hours back, as the evaluator's restart pins do.
    /// </summary>
    [Fact]
    public async Task CollectionStopped_FiresFromTheFleetPass_WhileTheLaunchGuardStillHolds_AndNoBodyRuns()
    {
        var harness = new DarlingSelfAlertTests.Harness();
        var start = harness.Now;
        var evaluator = harness.Build();
        var lastSuccess = start.AddHours(-3);

        var worker = MakeWorker();
        var clock = TimeSpan.Zero;
        worker.LaunchGuard = new LaunchMemoryGuard(
            () => new LaunchMemoryReading(1500L * 1024 * 1024, 1536L * 1024 * 1024, "test metric", "test limit"),
            () => { },
            () => clock,
            NullLogger.Instance);
        const int serverId = 424242;
        worker.SelfAlertServerOverride = async (server, ct) =>
        {
            var stopped = evaluator.JudgeCollectionStopped(serverId, lastSuccess, 10, 9, out var reason);
            await evaluator.ApplyCollectionStoppedAsync(serverId, server.Config.DisplayName, stopped, reason, ct);
        };
        var servers = new[] { MakeServer("example-sql-01", connected: true) };

        /* The window has not passed after the start: silent. Then it has, and the guard has held the whole time. */
        Assert.False(worker.LaunchGuard.MayLaunch(0));
        Assert.True(worker.TryStartSelfAlertPass(servers, TestContext.Current.CancellationToken));
        Assert.True(await BecomesTrueAsync(() => worker.TryStartSelfAlertPass(servers, TestContext.Current.CancellationToken)));
        Assert.Empty(harness.Deliverer.Outcomes);

        harness.Now = start.AddMinutes(30);
        servers[0].NextSelfAlertSweep = DateTime.MinValue;
        clock = TimeSpan.FromMinutes(30);
        Assert.False(worker.LaunchGuard.MayLaunch(0));
        Assert.True(await BecomesTrueAsync(() => worker.TryStartSelfAlertPass(servers, TestContext.Current.CancellationToken)));

        Assert.True(await BecomesTrueAsync(() => harness.Deliverer.Outcomes.Any(o => o.MetricName == "Collection Stopped")));
        var fired = Assert.Single(harness.Deliverer.Outcomes, o => o.MetricName == "Collection Stopped");
        Assert.StartsWith("No successful collection in 30 minutes", fired.CurrentValue, StringComparison.Ordinal);
        Assert.True(worker.LaunchGuard.IsHolding, "the guard must still be holding when the alert fires");
        Assert.Null(servers[0].InFlightSweep);
    }

    /// <summary>
    /// A server whose body never finishes is evaluated on its cadence all the same: the pass does not look at the body.
    /// </summary>
    [Fact]
    public async Task AServerWhoseBodyNeverFinishes_StillGetsItsEvaluationOnCadence()
    {
        var worker = MakeWorker();
        var evaluations = 0;
        worker.SelfAlertServerOverride = (_, _) =>
        {
            Interlocked.Increment(ref evaluations);
            return Task.CompletedTask;
        };
        var server = MakeServer("example-sql-01", connected: true);
        server.InFlightSweep = new TaskCompletionSource().Task;

        for (var round = 1; round <= 3; round++)
        {
            server.NextSelfAlertSweep = DateTime.MinValue;
            var previous = worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken);
            Assert.True(previous);
            Assert.True(await BecomesTrueAsync(() => Volatile.Read(ref evaluations) == round), $"round {round} was not evaluated");
        }

        /* On its cadence: a tick inside the 30 second window evaluates nothing. */
        Assert.True(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(3, Volatile.Read(ref evaluations));
    }

    /// <summary>
    /// Two passes at once never evaluate the same server: while one pass is still running the next tick is skipped, and
    /// the server is evaluated once even though its stamp is due again.
    /// </summary>
    [Fact]
    public async Task APassStillRunning_MakesTheNextTickSkip_SoNoServerIsEvaluatedByTwoThreads()
    {
        var worker = MakeWorker();
        var release = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        var concurrent = 0;
        var maxConcurrent = 0;
        var evaluations = 0;
        worker.SelfAlertServerOverride = async (_, _) =>
        {
            var now = Interlocked.Increment(ref concurrent);
            InterlockedMax(ref maxConcurrent, now);
            Interlocked.Increment(ref evaluations);
            started.TrySetResult();
            await release.Task;
            Interlocked.Decrement(ref concurrent);
        };
        var server = MakeServer("example-sql-01");

        Assert.True(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        await started.Task.WaitAsync(Patience, TestContext.Current.CancellationToken);

        /* The stamp is due again, and the pass is still inside the first evaluation. */
        server.NextSelfAlertSweep = DateTime.MinValue;
        Assert.False(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        Assert.False(worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken));
        Assert.Equal(1, Volatile.Read(ref evaluations));
        Assert.Equal(1, Volatile.Read(ref maxConcurrent));

        release.SetResult();
        Assert.True(await BecomesTrueAsync(() => worker.TryStartSelfAlertPass([server], TestContext.Current.CancellationToken)));
        Assert.True(await BecomesTrueAsync(() => Volatile.Read(ref evaluations) == 2));
        Assert.Equal(1, Volatile.Read(ref maxConcurrent));
    }

    /// <summary>
    /// The fleet pass runs the servers at the fleet gate's width, not one at a time (#5481 round 1): twenty servers whose
    /// evaluation takes 100 ms each finish in about a fifth of the serial 2 seconds at width 4, each evaluated once, never
    /// more than the width at a time. At 500 servers the one-at-a-time pass overran every tick.
    /// </summary>
    [Fact]
    public async Task TheFleetPass_EvaluatesAtTheSweepWidth_NotOneServerAtATime()
    {
        var worker = MakeWorker();
        var concurrent = 0;
        var maxConcurrent = 0;
        var perServer = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
        var finished = 0;
        worker.SelfAlertServerOverride = async (server, ct) =>
        {
            InterlockedMax(ref maxConcurrent, Interlocked.Increment(ref concurrent));
            perServer.AddOrUpdate(server.Config.DisplayName, 1, (_, n) => n + 1);
            await Task.Delay(100, ct);
            Interlocked.Decrement(ref concurrent);
            Interlocked.Increment(ref finished);
        };
        var servers = Enumerable.Range(1, 20).Select(i => MakeServer($"example-sql-{i:00}")).ToArray();

        var clock = System.Diagnostics.Stopwatch.StartNew();
        Assert.True(worker.TryStartSelfAlertPass(servers, TestContext.Current.CancellationToken, sweepWidth: 4));
        Assert.True(await BecomesTrueAsync(() => Volatile.Read(ref finished) == 20));
        clock.Stop();

        Assert.Equal(20, perServer.Count);
        Assert.All(perServer.Values, n => Assert.Equal(1, n));
        Assert.InRange(Volatile.Read(ref maxConcurrent), 3, 4);
        Assert.True(clock.ElapsedMilliseconds < 1300, $"20 servers at 100 ms and width 4 took {clock.ElapsedMilliseconds} ms; serial is 2000 ms");
    }

    /// <summary>
    /// A cancellation inside one server's evaluation that is not the service shutting down (the custom-alert evaluator
    /// rethrows every <see cref="OperationCanceledException"/>) is logged and the pass goes on to the servers after it
    /// (#5481 round 1). It used to end the pass, and the servers behind it starved if it repeated.
    /// </summary>
    [Fact]
    public async Task ANonShutdownCancellationInOneServer_IsLogged_AndTheRestOfThePassGoesOn()
    {
        var logger = new CapturingTestLogger();
        var worker = MakeWorker(logger);
        var evaluated = new List<string>();
        worker.SelfAlertServerOverride = (server, _) =>
        {
            lock (evaluated)
            {
                evaluated.Add(server.Config.DisplayName);
            }

            if (server.Config.DisplayName == "example-sql-01")
            {
                throw new OperationCanceledException("a timeout inside one evaluator");
            }

            return Task.CompletedTask;
        };

        Assert.True(worker.TryStartSelfAlertPass(
            [MakeServer("example-sql-01"), MakeServer("example-sql-02"), MakeServer("example-sql-03")],
            TestContext.Current.CancellationToken, sweepWidth: 1));

        Assert.True(await BecomesTrueAsync(() => { lock (evaluated) { return evaluated.Count == 3; } }));
        Assert.Equal(["example-sql-01", "example-sql-02", "example-sql-03"], evaluated);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Contains("self-alert pass failed", logger.Joined);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        do
        {
            seen = Volatile.Read(ref target);
        }
        while (value > seen && Interlocked.CompareExchange(ref target, value, seen) != seen);
    }

    /// <summary>
    /// A retired server is skipped, and one server's throw is logged without skipping the servers after it.
    /// </summary>
    [Fact]
    public async Task ARetiredServerIsSkipped_AndOneServersThrowDoesNotSkipTheRest()
    {
        var worker = MakeWorker();
        var evaluated = new List<string>();
        worker.SelfAlertServerOverride = (server, _) =>
        {
            lock (evaluated)
            {
                evaluated.Add(server.Config.DisplayName);
            }

            if (server.Config.DisplayName == "example-sql-01")
            {
                throw new InvalidOperationException("store read failed");
            }

            return Task.CompletedTask;
        };
        var retired = MakeServer("example-sql-00");
        retired.Retired = true;

        Assert.True(worker.TryStartSelfAlertPass([retired, MakeServer("example-sql-01"), MakeServer("example-sql-02")], TestContext.Current.CancellationToken));

        Assert.True(await BecomesTrueAsync(() => { lock (evaluated) { return evaluated.Count == 2; } }));
        lock (evaluated)
        {
            Assert.Equal(["example-sql-01", "example-sql-02"], evaluated.Order().ToArray());
        }
    }

    /// <summary>
    /// An operator pause still evaluates nothing, as before: the launch sits after the pause's <c>continue</c>, before the
    /// launch loop and outside the memory guard's branch, and the body no longer evaluates anything itself.
    /// </summary>
    [Fact]
    public void TheLoopLaunchesThePass_AfterThePauseContinue_BeforeTheLaunchLoop_AndTheBodyEvaluatesNothing()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        var loop = source.IndexOf("private async Task RunCollectionLoopAsync(", StringComparison.Ordinal);
        Assert.True(loop > 0);

        var pause = source.IndexOf("if (!ShouldRunCollection(_paused))", loop, StringComparison.Ordinal);
        var pauseContinue = source.IndexOf("continue;", pause, StringComparison.Ordinal);
        var launch = source.IndexOf("TryStartSelfAlertPass(sweepTargets, stoppingToken, StoreConfigProvider.ClampConcurrentSweeps(config.MaxConcurrentSweeps));", loop, StringComparison.Ordinal);
        var guard = source.IndexOf("var mayLaunchSweeps = _launchMemoryGuard.MayLaunch(", loop, StringComparison.Ordinal);
        var launchLoop = source.IndexOf("foreach (var server in sweepTargets)", loop, StringComparison.Ordinal);
        Assert.True(pause > 0 && pauseContinue > pause, "the pause gate was not found");
        Assert.True(launch > pauseContinue, "the self-alert pass must sit after the operator pause's continue");
        Assert.True(launch < guard && launch < launchLoop, "the self-alert pass must not depend on the guard or the launch loop");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(source, @"TryStartSelfAlertPass\(sweepTargets"));

        var bodyStart = source.IndexOf("private async Task ProcessServerSweepAsync(", StringComparison.Ordinal);
        var nextMember = System.Text.RegularExpressions.Regex.Match(
            source.Substring(bodyStart + 100), @"\r?\n    (?:private|internal|public|protected)[^\r\n]*\(");
        Assert.True(nextMember.Success, "the member after the body was not found");
        var body = source.Substring(bodyStart, 100 + nextMember.Index);
        Assert.Contains("RunDueCollectorsAsync(server, runner", body, StringComparison.Ordinal);
        Assert.DoesNotContain("EvaluateStoreAlertsAsync", body, StringComparison.Ordinal);
        Assert.DoesNotContain("EvaluateServerAsync", body, StringComparison.Ordinal);
        Assert.DoesNotContain("NextSelfAlertSweep", body, StringComparison.Ordinal);
        Assert.DoesNotContain("NextCustomAlertSweep", body, StringComparison.Ordinal);

        /* Shutdown waits for the tracked pass like it does for the ticks. */
        Assert.Contains("inFlightSweeps.Add(_selfAlertPass);", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The held-slot count sits at the guard's break, and the first pass that launches again raises the skip-credit floor
    /// the way the first pass after an operator pause does, before any body launches.
    /// </summary>
    [Fact]
    public void TheLoop_CountsHeldSlotsAtTheGuardBreak_AndResumesTheFloorBeforeTheFirstLaunch()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        var loop = source.IndexOf("private async Task RunCollectionLoopAsync(", StringComparison.Ordinal);
        var held = source.IndexOf("if (!mayLaunchSweeps)", loop, StringComparison.Ordinal);
        var count = source.IndexOf("CountHeldSlots(sweepTargets, DateTime.UtcNow);", held, StringComparison.Ordinal);
        var brk = source.IndexOf("break;", count, StringComparison.Ordinal);
        var resumeIf = source.IndexOf("if (heldSinceLastLaunch)", brk, StringComparison.Ordinal);
        var resume = source.IndexOf("_skipCreditFloor.Resume(DateTime.UtcNow);", resumeIf, StringComparison.Ordinal);
        var launch = source.IndexOf("server.InFlightSweep = ProcessServerSweepAsync(", loop, StringComparison.Ordinal);

        Assert.True(held > 0 && count > held && brk > count, "the held-slot count must come before the guard's break");
        Assert.True(resumeIf > brk && resume > resumeIf && resume < launch, "the floor must be raised before any body launches");
    }

    /// <summary>
    /// Held slots: 10 minutes of a 1-minute collector's slots come due while the guard holds the pass off, and each is
    /// counted once, within a pass of its due time, spread over the passes. The first launch after the release counts none
    /// of them again, because the loop raises the floor. On the old shape the hold counted 0 and the release counted 10.
    /// </summary>
    [Fact]
    public async Task HeldSlots_AreCountedAsTheyComeDue_AndTheFirstLaunchAfterTheReleaseCountsNoneOfThemAgain()
    {
        var worker = MakeWorker();
        var server = MakeServer("example-sql-01", connected: true);
        var held = DateTime.UtcNow.AddMinutes(-10);
        server.NextDue["wait_stats"] = held;
        var stats = worker.FleetGateStatsForTest!;

        long previous = 0;
        var perPass = new List<long>();
        for (var pass = 1; pass * 15 <= 600; pass++)
        {
            worker.CountHeldSlots([server], held.AddSeconds(pass * 15));
            var skipped = stats.Snapshot().Skipped;
            perPass.Add(skipped - previous);
            previous = skipped;
        }

        /* 10 minutes, 1-minute slots: the slot at the stamp is served by the first run, and the 10 after it are lost. */
        Assert.Equal(10, previous);
        Assert.All(perPass, n => Assert.InRange(n, 0, 1));
        Assert.Equal(40, perPass.Count);
        Assert.Equal(10, perPass.Count(n => n == 1));

        /* A pass that counts again at the same time counts nothing. */
        Assert.Equal(0, worker.CountHeldSlots([server], held.AddMinutes(10)));

        /* The release: the loop raises the floor, then the first body runs the slot. It counts no skipped slot. */
        worker.SkipCreditFloorForTest.Resume(DateTime.UtcNow);
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(1);
        var before = stats.Snapshot();
        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);
        var after = stats.Snapshot();

        Assert.Equal(before.Skipped, after.Skipped);
        Assert.True(after.Run > before.Run, "the slot at the stamp must run after the release");
    }

    /// <summary>
    /// Without the floor raise the same release would count the held slots a second time: this is the burst the field saw.
    /// It pins that the raise is what stops it, so a loop that stopped calling it would show here.
    /// </summary>
    [Fact]
    public async Task WithoutTheFloorRaise_TheFirstLaunchAfterTheHoldWouldCountTheHeldSlotsAgain()
    {
        var worker = MakeWorker();
        var server = MakeServer("example-sql-01", connected: true);
        server.NextDue["wait_stats"] = DateTime.UtcNow.AddMinutes(-10);
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(1);
        var stats = worker.FleetGateStatsForTest!;

        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);

        Assert.InRange(stats.Snapshot().Skipped, 9, 10);
    }

    [Fact]
    public void AServerWithABodyInFlight_ADisconnectedServer_AndARetiredServer_AreNotCountedByTheHold()
    {
        var worker = MakeWorker();
        var due = DateTime.UtcNow.AddMinutes(-10);
        var inFlight = MakeServer("example-sql-01", connected: true);
        inFlight.InFlightSweep = new TaskCompletionSource().Task;
        var disconnected = MakeServer("example-sql-02");
        var retired = MakeServer("example-sql-03", connected: true);
        retired.Retired = true;
        foreach (var s in new[] { inFlight, disconnected, retired })
        {
            s.NextDue["wait_stats"] = due;
        }

        Assert.Equal(0, worker.CountHeldSlots([inFlight, disconnected, retired], DateTime.UtcNow));
    }
}

/// <summary>
/// The live half of the fleet pass tests (#5479, #5481 round 1): the production evaluator reads a real store, so the
/// class joins the shared-store collection instead of serializing the pure tests above with it.
/// </summary>
[Collection("live-postgres")]
public sealed class SelfAlertFleetPassLiveTests
{
    private const int LivePassServerId = -770179;

    /// <summary>
    /// The production evaluator through the fleet pass, with the memory guard forced to hold (#5481 round 1). Every other
    /// test here stands in for the evaluation; this one reads the store. A server's last SUCCESS collection_log row is
    /// three hours old, the guard holds and no body is launched, and Collection Stopped is delivered.
    /// </summary>
    [Fact]
    public async Task LiveStore_CollectionStopped_FiresFromTheProductionEvaluator_ThroughTheFleetPass_WhileTheGuardHolds()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet pass.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLivePassRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            using (var insert = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, 'example-sql-live', 'wait_stats', $3, 0, 'SUCCESS', NULL, 0, 0, 0)", connection))
            {
                insert.Parameters.AddWithValue(9_100_000L);
                insert.Parameters.AddWithValue(LivePassServerId);
                insert.Parameters.AddWithValue(utcNow.AddHours(-3));
                await insert.ExecuteNonQueryAsync(ct);
            }

            var harness = new DarlingSelfAlertTests.Harness { Now = DateTime.UtcNow };
            var worker = SelfAlertFleetPassTests.MakeWorker();
            worker.StoreForTests = postgres;
            worker.SelfAlertsForTests = harness.Build();
            var clock = TimeSpan.Zero;
            worker.LaunchGuard = new LaunchMemoryGuard(
                () => new LaunchMemoryReading(1500L * 1024 * 1024, 1536L * 1024 * 1024, "test metric", "test limit"),
                () => { },
                () => clock,
                NullLogger.Instance);
            var server = SelfAlertFleetPassTests.MakeServer("example-sql-live");
            server.Config.StoredServerId = LivePassServerId;

            /* The service has just started: the stale row is judged from the start, so the first pass is silent. */
            Assert.False(worker.LaunchGuard.MayLaunch(0));
            Assert.True(worker.TryStartSelfAlertPass([server], ct));
            Assert.True(await SelfAlertFleetPassTests.BecomesTrueAsync(() => worker.TryStartSelfAlertPass([server], ct)));
            Assert.DoesNotContain(harness.Deliverer.Outcomes, o => o.MetricName == "Collection Stopped");

            /* Past the window since the start, with the guard still holding and no body launched or called. */
            harness.Now = harness.Now.AddMinutes(31);
            clock = TimeSpan.FromMinutes(31);
            server.NextSelfAlertSweep = DateTime.MinValue;
            Assert.False(worker.LaunchGuard.MayLaunch(0));
            Assert.True(await SelfAlertFleetPassTests.BecomesTrueAsync(() => worker.TryStartSelfAlertPass([server], ct)));

            Assert.True(await SelfAlertFleetPassTests.BecomesTrueAsync(() => harness.Deliverer.Outcomes.Any(o => o.MetricName == "Collection Stopped")));
            Assert.Single(harness.Deliverer.Outcomes, o => o.MetricName == "Collection Stopped");
            Assert.True(worker.LaunchGuard.IsHolding, "the guard must still be holding when the alert fires");
            Assert.Null(server.InFlightSweep);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteLivePassRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    private static async Task DeleteLivePassRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {LivePassServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #5479: the pure rule behind the held-slot count.
/// </summary>
public sealed class HeldSlotsTests
{
    private static readonly DateTime T0 = new(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Minute = TimeSpan.FromMinutes(1);

    [Fact]
    public void AGridCollector_LosesASlotOnceTheNextOneIsDue_TheNumberALateRunWouldStepOver()
    {
        Assert.Equal(0, HeldSlots.Lost(T0, T0, DateTime.MinValue, Minute, Minute));
        Assert.Equal(0, HeldSlots.Lost(T0, T0.AddSeconds(59), DateTime.MinValue, Minute, Minute));
        Assert.Equal(1, HeldSlots.Lost(T0, T0.AddSeconds(60), DateTime.MinValue, Minute, Minute));

        for (var seconds = 0; seconds <= 6 * 3600; seconds += 15)
        {
            var now = T0.AddSeconds(seconds);
            Assert.Equal(CollectorCadence.SkippedSlots(T0, now, Minute), HeldSlots.Lost(T0, now, DateTime.MinValue, Minute, Minute));
        }
    }

    [Fact]
    public void ASlotIsCountedOnce_WhateverTheNumberOfPasses_AndSpreadOverThePassesAsTheyComeDue()
    {
        var mark = default(HeldSlotMark);
        long counted = 0;
        var perPass = new List<long>();
        for (var pass = 1; pass <= 4 * 60; pass++)
        {
            var n = HeldSlots.Newly(T0, T0.AddSeconds(pass * 15), DateTime.MinValue, Minute, Minute, ref mark);
            counted += n;
            perPass.Add(n);
        }

        Assert.Equal(60, counted);
        Assert.All(perPass, n => Assert.InRange(n, 0, 1));

        /* The same instant again, and an earlier one, add nothing. */
        Assert.Equal(0, HeldSlots.Newly(T0, T0.AddHours(1), DateTime.MinValue, Minute, Minute, ref mark));
        Assert.Equal(0, HeldSlots.Newly(T0, T0.AddMinutes(30), DateTime.MinValue, Minute, Minute, ref mark));
    }

    [Fact]
    public void AMovedStamp_StartsTheCountAgain_BecauseABodyRanAndCountedItsOwnSlots()
    {
        var mark = default(HeldSlotMark);
        Assert.Equal(5, HeldSlots.Newly(T0, T0.AddMinutes(5), DateTime.MinValue, Minute, Minute, ref mark));

        var moved = T0.AddMinutes(6);
        Assert.Equal(0, HeldSlots.Newly(moved, T0.AddMinutes(6), DateTime.MinValue, Minute, Minute, ref mark));
        Assert.Equal(2, HeldSlots.Newly(moved, T0.AddMinutes(8), DateTime.MinValue, Minute, Minute, ref mark));
    }

    /// <summary>The interval changes mid-hold (a reload from 60 to 1 minute during a 2 hour hold): the due stamp stays, so
    /// counting at the new interval from the stamp would invent 119 slots at once. The mark restarts at the change and only
    /// slots after it count (#5481 round 1).</summary>
    [Fact]
    public void AnIntervalChangeMidHold_RestartsTheMark_AndInventsNoSlots()
    {
        var hour = TimeSpan.FromMinutes(60);
        var mark = default(HeldSlotMark);
        Assert.Equal(2, HeldSlots.Newly(T0, T0.AddHours(2), DateTime.MinValue, hour, hour, ref mark));

        var change = T0.AddHours(2);
        Assert.Equal(0, HeldSlots.Newly(T0, change, DateTime.MinValue, Minute, Minute, ref mark));
        Assert.Equal(0, HeldSlots.Newly(T0, change.AddSeconds(59), DateTime.MinValue, Minute, Minute, ref mark));

        /* From the change on: the slot at the change is lost a minute later, then one a minute. */
        Assert.Equal(1, HeldSlots.Newly(T0, change.AddMinutes(1), DateTime.MinValue, Minute, Minute, ref mark));
        Assert.Equal(4, HeldSlots.Newly(T0, change.AddMinutes(5), DateTime.MinValue, Minute, Minute, ref mark));
    }

    [Fact]
    public void ARaisedFloor_CountsFromTheFloor_AndNeverCountsASlotBeforeIt()
    {
        var mark = default(HeldSlotMark);
        Assert.Equal(10, HeldSlots.Newly(T0, T0.AddMinutes(10), DateTime.MinValue, Minute, Minute, ref mark));

        /* The loop was not running for a while (a sleep): the floor moves to now, and only slots after it count. */
        var floor = T0.AddMinutes(10);
        Assert.Equal(0, HeldSlots.Newly(T0, T0.AddMinutes(10), floor, Minute, Minute, ref mark));
        Assert.Equal(3, HeldSlots.Newly(T0, T0.AddMinutes(13), floor, Minute, Minute, ref mark));

        /* The release raises the floor to now: nothing before it counts, so the first body counts none of the hold. */
        var release = T0.AddMinutes(13).AddSeconds(30);
        Assert.Equal(0, HeldSlots.Lost(T0, release, release, Minute, Minute));
    }

    [Fact]
    public void ACollectorWithARunTime_LosesItsDayOnceTheGraceHasPassed_NotWhenTheNextDayIsDue()
    {
        var day = TimeSpan.FromDays(1);
        var grace = CollectorRunTime.Grace;

        Assert.Equal(0, HeldSlots.Lost(T0, T0.Add(grace).AddSeconds(-1), DateTime.MinValue, day, grace));
        Assert.Equal(1, HeldSlots.Lost(T0, T0.Add(grace), DateTime.MinValue, day, grace));
        Assert.Equal(1, HeldSlots.Lost(T0, T0.AddDays(1), DateTime.MinValue, day, grace));
        Assert.Equal(2, HeldSlots.Lost(T0, T0.AddDays(1).Add(grace), DateTime.MinValue, day, grace));
    }
}
