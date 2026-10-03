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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The pieces the daily-run cap and the hang watchdog share (#4999): a worker with no store, a server that is due
/// for one collector, a stand-in for the collector run that a test can hold open, and a logger that keeps what it
/// is told. Nothing here needs a store or a monitored server.
/// </summary>
internal static class DailyRunKit
{
    internal const string Daily = "index_object_stats";

    internal static readonly TimeSpan Patience = TimeSpan.FromSeconds(5);

    internal static DarlingWorker MakeWorker(DailyRunLogger logger) =>
        new(
            logger,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());

    internal static DarlingWorker.ServerLoopState MakeServer(int serverId, string collector, CollectorTargetInfo? target = null)
    {
        var host = $"cap-test-{serverId}";
        var config = new MonitoredServer { Name = host, Host = host };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
            Target = target ?? new CollectorTargetInfo(),
            StorageName = host,
            ServerId = serverId,
        };
        var server = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
        server.NextDue[collector] = DateTime.UtcNow.AddMinutes(-5);
        return server;
    }

    internal static async Task<bool> EndsWithinAsync(Task task, TimeSpan patience) =>
        await Task.WhenAny(task, Task.Delay(patience)) == task;

    internal static async Task<bool> BecomesTrueAsync(Func<bool> condition, TimeSpan patience)
    {
        var deadline = DateTime.UtcNow + patience;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(10);
        }

        return condition();
    }

    internal static int Count(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}

/// <summary>The stand-in for a collector run: each named collector holds open until a test releases it, one release per run.</summary>
internal sealed class HeldRuns
{
    private readonly object _lock = new();
    private readonly SemaphoreSlim _release = new(0);
    private readonly HashSet<string> _held;
    private int _started;

    /// <param name="worker">The worker whose collector runs this stands in for.</param>
    /// <param name="held">The collectors whose runs hold open. Every other collector returns at once.</param>
    public HeldRuns(DarlingWorker worker, params string[] held)
    {
        _held = [.. held];
        worker.RunOneBodyOverride = async (_, _, collector, _) =>
        {
            if (_held.Contains(collector))
            {
                lock (_lock)
                {
                    _started++;
                }

                /* Ignores the token, as a run in the middle of a store write does. */
                await _release.WaitAsync(CancellationToken.None);
            }

            return 1;
        };
    }

    public int Started
    {
        get
        {
            lock (_lock)
            {
                return _started;
            }
        }
    }

    public void ReleaseOne() => _release.Release();

    public void ReleaseAll() => _release.Release(1000);
}

/// <summary>A logger that keeps every line it is given, so a test can read what the worker said.</summary>
internal sealed class DailyRunLogger : ILogger<DarlingWorker>
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    public List<(LogLevel Level, string Message)> Snapshot()
    {
        lock (_entries)
        {
            return [.. _entries];
        }
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        lock (_entries)
        {
            _entries.Add((logLevel, formatter(state, exception)));
        }
    }
}

/// <summary>
/// #4999: the limiter behind the daily-run cap is a semaphore whose size can change while runs hold permits. These
/// cases pin what a plain semaphore cannot do: widen and start the waiting runs, narrow without taking anything
/// back, and let a cancelled waiter go without leaking a permit.
/// </summary>
public sealed class DailyRunLimiterTests
{
    private static readonly TimeSpan Patience = DailyRunKit.Patience;

    [Fact]
    public void TryAcquire_TakesUpToTheCap_ThenSaysNo()
    {
        var limiter = new DailyRunLimiter(2);

        Assert.True(limiter.TryAcquire());
        Assert.True(limiter.TryAcquire());
        Assert.False(limiter.TryAcquire());
        Assert.Equal(2, limiter.InUse);

        limiter.Release();
        Assert.True(limiter.TryAcquire());
    }

    [Fact]
    public async Task AWaitingRun_StartsWhenAPermitIsReleased_OldestFirst()
    {
        var limiter = new DailyRunLimiter(1);
        Assert.True(limiter.TryAcquire());

        var first = limiter.AcquireAsync(CancellationToken.None);
        var second = limiter.AcquireAsync(CancellationToken.None);
        Assert.Equal(2, limiter.Waiting);
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        limiter.Release();
        await first.WaitAsync(Patience, TestContext.Current.CancellationToken);
        Assert.False(second.IsCompleted, "one permit freed, one run started: the younger waiter keeps waiting");

        limiter.Release();
        await second.WaitAsync(Patience, TestContext.Current.CancellationToken);
        Assert.Equal(1, limiter.InUse);
        Assert.Equal(0, limiter.Waiting);
    }

    [Fact]
    public async Task Widening_StartsTheWaitingRuns()
    {
        var limiter = new DailyRunLimiter(1);
        Assert.True(limiter.TryAcquire());
        var first = limiter.AcquireAsync(CancellationToken.None);
        var second = limiter.AcquireAsync(CancellationToken.None);

        Assert.True(limiter.SetCap(3));

        await Task.WhenAll(first, second).WaitAsync(Patience, TestContext.Current.CancellationToken);
        Assert.Equal(3, limiter.InUse);
        Assert.Equal(3, limiter.Cap);
        Assert.False(limiter.SetCap(3), "the same cap is no change");
    }

    [Fact]
    public async Task Narrowing_TakesNothingBack_AndAWaitingRunStartsOnlyOnceFewerThanTheCapAreGoing()
    {
        var limiter = new DailyRunLimiter(3);
        Assert.True(limiter.TryAcquire());
        Assert.True(limiter.TryAcquire());
        Assert.True(limiter.TryAcquire());

        Assert.True(limiter.SetCap(1));
        Assert.Equal(3, limiter.InUse);
        Assert.False(limiter.TryAcquire());

        var waiting = limiter.AcquireAsync(CancellationToken.None);
        limiter.Release();
        limiter.Release();
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(waiting.IsCompleted, "one run is still going and the cap is one: the waiting run must not start");

        limiter.Release();
        await waiting.WaitAsync(Patience, TestContext.Current.CancellationToken);
        Assert.Equal(1, limiter.InUse);
    }

    [Fact]
    public async Task ACancelledWaiter_LeavesTheQueue_AndTakesNoPermit()
    {
        var limiter = new DailyRunLimiter(1);
        Assert.True(limiter.TryAcquire());
        using var cancel = new CancellationTokenSource();
        var waiting = limiter.AcquireAsync(cancel.Token);
        Assert.Equal(1, limiter.Waiting);

        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(0, limiter.Waiting);

        limiter.Release();
        Assert.Equal(0, limiter.InUse);
        Assert.True(limiter.TryAcquire(), "the cancelled run left no permit behind");
    }

    [Fact]
    public void TheCap_IsNeverBelowOne()
    {
        var limiter = new DailyRunLimiter(0);
        Assert.Equal(1, limiter.Cap);

        Assert.True(limiter.SetCap(5));
        Assert.True(limiter.SetCap(-3));
        Assert.Equal(1, limiter.Cap);
    }
}

/// <summary>
/// #4999: the daily-run cap comes from the store's connection pool, not from a flat 16. Every daily run holds a
/// store connection for its whole run (index_object_stats took 43 minutes in a field case) and so does every sweep
/// body, so 16 daily runs plus a sweep nine wide asked a pool of 24 for more connections than it has. The cap is
/// the smaller of 16 and (pool - sweep width - 8 kept free for reads), never below 1.
/// </summary>
public sealed class DailyRunCapTests
{
    private static readonly TimeSpan Patience = DailyRunKit.Patience;

    [Theory]
    [InlineData(24, 9, 7)]
    [InlineData(24, 4, 12)]
    [InlineData(24, 16, 1)]
    [InlineData(100, 9, 16)]
    [InlineData(100, 4, 16)]
    [InlineData(10, 9, 1)]
    [InlineData(1, 4, 1)]
    [InlineData(0, 4, 1)]
    [InlineData(int.MaxValue, 1, 16)]
    [InlineData(int.MinValue, 16, 1)]
    public void TheCap_IsThePoolLessTheSweepWidthLessTheReserve_BetweenOneAndSixteen(int pool, int sweepWidth, int expected) =>
        Assert.Equal(expected, DarlingWorker.DailyRunCapFor(pool, sweepWidth));

    [Fact]
    public void APoolThatBoundsNothing_LeavesTheCeiling() =>
        Assert.Equal(DarlingWorker.MaxConcurrentDailyRuns, DarlingWorker.DailyRunCapFor(null, 9));

    [Fact]
    public void TheReserve_IsEight_AndTheCeilingSixteen()
    {
        Assert.Equal(8, DarlingWorker.DailyRunPoolReserve);
        Assert.Equal(16, DarlingWorker.MaxConcurrentDailyRuns);
    }

    [Theory]
    [InlineData("Host=127.0.0.1;Port=5432;Database=d;Username=u;Maximum Pool Size=24", 24)]
    [InlineData("Host=127.0.0.1;Port=5432;Database=d;Username=u;MaxPoolSize=7", 7)]
    [InlineData("Host=127.0.0.1;Port=5432;Database=d;Username=u;Password=secret;Maximum Pool Size=50", 50)]
    [InlineData("Host=127.0.0.1;Port=5432;Database=d;Username=u", 100)]
    public async Task ThePool_IsReadFromTheStoresOwnDataSource_WhateverTheOperatorSetOrNpgsqlDefaults(string connectionString, int expected)
    {
        await using var dataSource = NpgsqlDataSource.Create(connectionString);

        Assert.Equal(expected, DarlingWorker.StorePoolMaxSize(dataSource));
    }

    [Fact]
    public async Task ThePool_IsUnbounded_WhenPoolingIsOffOrThereIsNoStore()
    {
        await using var unpooled = NpgsqlDataSource.Create("Host=127.0.0.1;Port=5432;Database=d;Username=u;Pooling=false");

        Assert.Null(DarlingWorker.StorePoolMaxSize(unpooled));
        Assert.Null(DarlingWorker.StorePoolMaxSize(null));
    }

    [Fact]
    public async Task TheCapInForce_FollowsThePoolAndTheSweepWidth_AndARunOverItWaits()
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, DailyRunKit.Daily);
        var ct = TestContext.Current.CancellationToken;

        /* A pool of 24 and a sweep nine wide: 24 - 9 - 8 = 7. The flat 16 let all eight of these start. */
        worker.ApplyDailyRunCap(24, 9);
        Assert.Equal(7, worker.DailyRunCap);

        var servers = Enumerable.Range(0, 8).Select(i => DailyRunKit.MakeServer(7100 + i, DailyRunKit.Daily)).ToList();
        try
        {
            foreach (var server in servers)
            {
                await worker.RunDueCollectorsAsync(server, null!, ct);
            }

            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 7, Patience), "seven daily runs start at once");
            await Task.Delay(300, ct);
            Assert.Equal(7, runs.Started);
            Assert.Equal(7, worker.DailyRunsHoldingPermits);

            /* The sweep is narrowed to four: the cap is recomputed to 12 and the waiting run starts. */
            worker.ApplyDailyRunCap(24, 4);
            Assert.Equal(12, worker.DailyRunCap);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 8, Patience), "a wider cap starts the run that was waiting");

            /* Narrowed to the floor of one: the eight going keep going, and a ninth waits behind all of them. */
            worker.ApplyDailyRunCap(24, 16);
            Assert.Equal(1, worker.DailyRunCap);
            var ninth = DailyRunKit.MakeServer(7200, DailyRunKit.Daily);
            await worker.RunDueCollectorsAsync(ninth, null!, ct);
            await Task.Delay(300, ct);
            Assert.Equal(8, runs.Started);

            for (var i = 0; i < 7; i++)
            {
                runs.ReleaseOne();
            }

            await Task.Delay(300, ct);
            Assert.Equal(8, runs.Started);

            runs.ReleaseOne();
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 9, Patience), "the ninth starts once nothing else is going");
        }
        finally
        {
            runs.ReleaseAll();
            await DailyRunKit.BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 0, Patience);
        }
    }

    [Fact]
    public void TheCap_IsLoggedOnceAtStart_AndAgainOnlyWhenItChanges()
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);

        List<string> CapLines() => logger.Snapshot()
            .Where(e => e.Level == LogLevel.Information && e.Message.Contains("Daily collectors run at most", StringComparison.Ordinal))
            .Select(e => e.Message)
            .ToList();

        worker.ApplyDailyRunCap(24, 9);
        var start = Assert.Single(CapLines());
        Assert.Contains("at most 7 ", start, StringComparison.Ordinal);
        Assert.Contains("(24)", start, StringComparison.Ordinal);
        Assert.Contains("(9)", start, StringComparison.Ordinal);

        worker.ApplyDailyRunCap(24, 9);
        Assert.Single(CapLines());

        worker.ApplyDailyRunCap(24, 4);
        Assert.Equal(2, CapLines().Count);
        Assert.Contains("at most 12 ", CapLines()[1], StringComparison.Ordinal);

        worker.ApplyDailyRunCap(100, 4);
        Assert.Equal(3, CapLines().Count);

        /* A width that moves the pool-bound cap nowhere (16 either way) says nothing. */
        worker.ApplyDailyRunCap(100, 9);
        Assert.Equal(3, CapLines().Count);
    }

    [Fact]
    public void TheLoop_SetsTheCapAtStart_AndWheneverTheSweepWidthMoves()
    {
        var source = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        /* The collection loop needs a store, so what a case here can pin is that the two places the width is set are
           the two places that recompute the cap, and that the permits are no longer a fixed-size semaphore. */
        Assert.Equal(1, DailyRunKit.Count(source, "ApplyDailyRunCap(StorePoolMaxSize(postgres), initialSweepWidth);"));
        Assert.Equal(1, DailyRunKit.Count(source, "ApplyDailyRunCap(StorePoolMaxSize(postgres), reloadedSweepWidth);"));
        Assert.Equal(0, DailyRunKit.Count(source, "new(MaxConcurrentDailyRuns, MaxConcurrentDailyRuns)"));
    }
}

/// <summary>
/// #4999: the hang watchdog covered only the per-server bodies, and a daily run is detached from them, so a daily run
/// that stopped making progress showed up only as stale data at 36 hours. It now watches the daily runs that are
/// executing, on a threshold of their own (15 minutes, not the sweep's 60 seconds) and with the same one Warning,
/// naming the server and the collector. The three runs detached by name are watched the same way
/// (<see cref="DetachedByNameWatchdogTests"/>).
/// </summary>
public sealed class DailyRunWatchdogTests
{
    private static readonly TimeSpan Patience = DailyRunKit.Patience;

    private static List<string> Warnings(DailyRunLogger logger) =>
        logger.Snapshot().Where(e => e.Level == LogLevel.Warning).Select(e => e.Message).ToList();

    [Fact]
    public async Task ADailyRun_ExecutingPastTheThreshold_IsReportedOnce_NamingTheServerAndTheCollector()
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, DailyRunKit.Daily);
        var server = DailyRunKit.MakeServer(7301, DailyRunKit.Daily);
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the daily run starts");
            var startedBy = DateTime.UtcNow;

            Assert.Equal(1, worker.WatchDailyRuns(startedBy.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds + 1)));

            var warning = Assert.Single(Warnings(logger));
            Assert.Contains(server.Config.DisplayName, warning, StringComparison.Ordinal);
            Assert.Contains(DailyRunKit.Daily, warning, StringComparison.Ordinal);

            /* One Warning per run: a later tick finds the run still going and says nothing more. */
            Assert.Equal(0, worker.WatchDailyRuns(startedBy.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds + 600)));
            Assert.Single(Warnings(logger));

            /* The end of a run the watchdog warned about is logged, and the run is no longer watched. */
            runs.ReleaseAll();
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 0, Patience), "the run ends");
            Assert.Contains(
                logger.Snapshot(),
                e => e.Level == LogLevel.Information
                    && e.Message.Contains("daily run completed", StringComparison.Ordinal)
                    && e.Message.Contains(DailyRunKit.Daily, StringComparison.Ordinal));
            Assert.Equal(0, worker.WatchDailyRuns(startedBy.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds + 3600)));
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    [Fact]
    public async Task ADailyRun_UnderTheThreshold_IsNotReported()
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, DailyRunKit.Daily);
        var server = DailyRunKit.MakeServer(7302, DailyRunKit.Daily);
        var ct = TestContext.Current.CancellationToken;
        var before = DateTime.UtcNow;

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the daily run starts");

            Assert.Equal(0, worker.WatchDailyRuns(before.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds - 20)));
            Assert.Empty(Warnings(logger));
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    /// <summary>
    /// A healthy daily run can take minutes (database_config reads every database on its server), so the sweep's
    /// 60 seconds is not the measure for it: a run that has been executing for five minutes is working, and
    /// reporting it would warn once a day on every server with many databases for no fault.
    /// </summary>
    [Fact]
    public async Task ADailyRun_ExecutingForFiveMinutes_IsNotReported_ButOnePastFifteenIsReportedOnce()
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, DailyRunKit.Daily);
        var server = DailyRunKit.MakeServer(7305, DailyRunKit.Daily);
        var ct = TestContext.Current.CancellationToken;
        var before = DateTime.UtcNow;

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the daily run starts");
            var startedBy = DateTime.UtcNow;

            /* Five minutes on, and ten: past the sweep's threshold, inside the daily run's own. */
            Assert.Equal(0, worker.WatchDailyRuns(before.AddMinutes(5)));
            Assert.Equal(0, worker.WatchDailyRuns(before.AddMinutes(10)));
            Assert.Empty(Warnings(logger));

            /* Past fifteen minutes it is reported, once, naming the server and the collector. */
            Assert.Equal(1, worker.WatchDailyRuns(startedBy.AddMinutes(15).AddSeconds(1)));
            var warning = Assert.Single(Warnings(logger));
            Assert.Contains(server.Config.DisplayName, warning, StringComparison.Ordinal);
            Assert.Contains(DailyRunKit.Daily, warning, StringComparison.Ordinal);
            Assert.Equal(0, worker.WatchDailyRuns(startedBy.AddMinutes(45)));
            Assert.Single(Warnings(logger));
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    /// <summary>
    /// The daily run's threshold is its own number, and the sweep's stays what it was: the two measure different
    /// work, so changing one must not move the other.
    /// </summary>
    [Fact]
    public void TheDailyRunThreshold_IsFifteenMinutes_AndTheSweepsStaysSixtySeconds()
    {
        Assert.Equal(15 * 60, DarlingWorker.DailyRunWatchdogSeconds);
        Assert.Equal(60, DarlingWorker.SweepWatchdogSeconds);
    }

    [Fact]
    public async Task ADailyRun_WaitingForAPermit_IsNotReported_HoweverLongItWaits()
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, DailyRunKit.Daily);
        var going = DailyRunKit.MakeServer(7303, DailyRunKit.Daily);
        var queued = DailyRunKit.MakeServer(7304, DailyRunKit.Daily);
        var ct = TestContext.Current.CancellationToken;

        /* A pool this small leaves room for one daily run, so the second is queued behind the first. */
        worker.ApplyDailyRunCap(1, 4);

        try
        {
            await worker.RunDueCollectorsAsync(going, null!, ct);
            await worker.RunDueCollectorsAsync(queued, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "one daily run starts");
            await Task.Delay(200, ct);
            Assert.Equal(1, runs.Started);
            Assert.Equal(2, worker.InFlightDailyRuns.Count);

            /* An hour on, the run that is going is reported and the one that is queued is not: waiting for a permit is
               capacity, and only a run that is executing can be stuck. */
            Assert.Equal(1, worker.WatchDailyRuns(DateTime.UtcNow.AddHours(1)));
            var warning = Assert.Single(Warnings(logger));
            Assert.Contains(going.Config.DisplayName, warning, StringComparison.Ordinal);
            Assert.DoesNotContain(queued.Config.DisplayName, warning, StringComparison.Ordinal);
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    [Fact]
    public void TheSweepLoop_RunsTheDailyWatchdog_EveryTick_AfterItsLaunchLoop()
    {
        var source = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        /* The sweep loop needs a store to run, so what a case here can pin is that it makes the call once, after the
           launch loop that watches the per-server bodies and not inside it. */
        Assert.Equal(1, DailyRunKit.Count(source, "WatchDailyRuns(DateTime.UtcNow);"));
        var launch = source.IndexOf("server.InFlightSweep = ProcessServerSweepAsync(", StringComparison.Ordinal);
        var watch = source.IndexOf("WatchDailyRuns(DateTime.UtcNow);", StringComparison.Ordinal);
        Assert.True(launch >= 0 && watch > launch, "the daily watchdog is called after the launch loop");
    }
}

/// <summary>
/// #4999: the hang watchdog watches the three collectors detached by name (query_store, plan_correction,
/// pg_wait_sampling) as it watches a daily run: one that has been executing for 15 minutes gets ONE Warning naming
/// its server and collector. They were left out at first, on the reasoning that query_store runs 100 to 230 seconds
/// on a healthy server and would warn on every run, which held only for the sweep's 60-second line; a healthy run is
/// far inside 15 minutes. A run detached by name takes no daily-run permit, so its watch starts when it starts
/// executing, and it is watched whatever the daily runs are doing.
/// </summary>
public sealed class DetachedByNameWatchdogTests
{
    private static readonly TimeSpan Patience = DailyRunKit.Patience;

    private static List<string> Warnings(DailyRunLogger logger) =>
        logger.Snapshot().Where(e => e.Level == LogLevel.Warning).Select(e => e.Message).ToList();

    /// <summary>pg_wait_sampling is a PostgreSQL collector, so a server that is due for it has to be a PostgreSQL target.</summary>
    private static CollectorTargetInfo TargetFor(string collector) =>
        collector == "pg_wait_sampling"
            ? new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }
            : new CollectorTargetInfo();

    [Theory]
    [InlineData("query_store")]
    [InlineData("plan_correction")]
    [InlineData("pg_wait_sampling")]
    public async Task ARunDetachedByName_ExecutingPastTheThreshold_IsReportedOnce_NamingTheServerAndTheCollector(string collector)
    {
        Assert.True(DarlingWorker.IsDetachedByName(collector), "the collector under test is one of the three detached by name");
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, collector);
        var server = DailyRunKit.MakeServer(7410, collector, TargetFor(collector));
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the run starts");
            var startedBy = DateTime.UtcNow;

            Assert.Equal(1, worker.WatchDailyRuns(startedBy.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds + 1)));

            var warning = Assert.Single(Warnings(logger));
            Assert.Contains(server.Config.DisplayName, warning, StringComparison.Ordinal);
            Assert.Contains(collector, warning, StringComparison.Ordinal);

            /* The run holds no daily-run permit, so the Warning does not say that it does. */
            Assert.DoesNotContain("permit", warning, StringComparison.Ordinal);
            Assert.DoesNotContain("daily", warning, StringComparison.Ordinal);

            /* One Warning per run: a later tick finds the run still going and says nothing more. */
            Assert.Equal(0, worker.WatchDailyRuns(startedBy.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds + 600)));
            Assert.Single(Warnings(logger));

            /* The end of a run the watchdog warned about is logged, and the run is no longer watched. */
            runs.ReleaseAll();
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 0, Patience), "the run ends");
            Assert.Contains(
                logger.Snapshot(),
                e => e.Level == LogLevel.Information
                    && e.Message.Contains("run completed", StringComparison.Ordinal)
                    && e.Message.Contains(collector, StringComparison.Ordinal));
            Assert.Equal(0, worker.WatchDailyRuns(startedBy.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds + 3600)));
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    /// <summary>
    /// A healthy query_store run takes 100 to 230 seconds and a healthy plan_correction or pg_wait_sampling run far
    /// less, so none of them is reported for running a few minutes: only a run well past a healthy one is.
    /// </summary>
    [Theory]
    [InlineData("query_store")]
    [InlineData("plan_correction")]
    [InlineData("pg_wait_sampling")]
    public async Task ARunDetachedByName_UnderTheThreshold_IsNotReported(string collector)
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, collector);
        var server = DailyRunKit.MakeServer(7411, collector, TargetFor(collector));
        var ct = TestContext.Current.CancellationToken;
        var before = DateTime.UtcNow;

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the run starts");

            /* Four minutes on is past a healthy query_store run's 230 seconds, and 20 seconds short of the line. */
            Assert.Equal(0, worker.WatchDailyRuns(before.AddMinutes(4)));
            Assert.Equal(0, worker.WatchDailyRuns(before.AddSeconds(DarlingWorker.DailyRunWatchdogSeconds - 20)));
            Assert.Empty(Warnings(logger));
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    /// <summary>
    /// A run detached by name never waits for a daily-run permit, so with every permit in use it still starts at once,
    /// and its watch starts with it. The daily run that holds the only permit and the run detached by name are each
    /// reported, once, by their own server and collector.
    /// </summary>
    [Fact]
    public async Task ARunDetachedByName_TakesNoPermit_AndIsWatchedFromTheMomentItStarts()
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, DailyRunKit.Daily, "query_store");
        var daily = DailyRunKit.MakeServer(7412, DailyRunKit.Daily);
        var byName = DailyRunKit.MakeServer(7413, "query_store");
        var ct = TestContext.Current.CancellationToken;

        /* A pool this small leaves room for one daily run, and the daily run below takes it. */
        worker.ApplyDailyRunCap(1, 4);

        try
        {
            await worker.RunDueCollectorsAsync(daily, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the daily run takes the only permit");
            await worker.RunDueCollectorsAsync(byName, null!, ct);
            Assert.True(
                await DailyRunKit.BecomesTrueAsync(() => runs.Started == 2, Patience),
                "the run detached by name starts beside the daily run and does not queue for a permit");

            Assert.Equal(2, worker.WatchDailyRuns(DateTime.UtcNow.AddHours(1)));
            var warnings = Warnings(logger);
            Assert.Equal(2, warnings.Count);
            Assert.Single(
                warnings,
                w => w.Contains(daily.Config.DisplayName, StringComparison.Ordinal)
                    && w.Contains(DailyRunKit.Daily, StringComparison.Ordinal));
            Assert.Single(
                warnings,
                w => w.Contains(byName.Config.DisplayName, StringComparison.Ordinal)
                    && w.Contains("query_store", StringComparison.Ordinal));
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    /// <summary>
    /// The watches are keyed by (server, collector), which holds only if two runs of one collector never execute on one
    /// server at once. A run detached by name is held to one at a time by a single-flight gate it takes before its
    /// watch begins (query_store's per-server gate, and the per-(server, collector) gate of the other two), so a
    /// second run that comes due while the first is still going is skipped before it is watched, and cannot replace
    /// or remove the first run's watch.
    /// </summary>
    [Theory]
    [InlineData("query_store")]
    [InlineData("plan_correction")]
    [InlineData("pg_wait_sampling")]
    public async Task ASecondRunOfTheSameCollectorOnTheServer_IsSkippedBeforeItIsWatched_SoTheFirstRunsWatchStands(string collector)
    {
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, collector);
        var server = DailyRunKit.MakeServer(7414, collector, TargetFor(collector));
        var ct = TestContext.Current.CancellationToken;

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the first run starts");

            /* It comes due again while the first run is still going. */
            server.NextDue[collector] = DateTime.UtcNow.AddMinutes(-5);
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(
                await DailyRunKit.BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 1, Patience),
                "the second run ends at once, skipped, and only the first is still in flight");
            Assert.Equal(1, runs.Started);

            /* The first run is still watched, once. */
            Assert.Equal(1, worker.WatchDailyRuns(DateTime.UtcNow.AddHours(1)));
            Assert.Single(Warnings(logger));
        }
        finally
        {
            runs.ReleaseAll();
        }
    }
}

/// <summary>
/// #4999: the three collectors detached by name (query_store, plan_correction, pg_wait_sampling) used to run
/// untracked, so the shutdown drain that waits for the other detached runs did not wait for them. They are tracked the
/// same way now, and a shutdown waits for one that is still going.
/// </summary>
public sealed class DetachedByNameShutdownDrainTests
{
    private static readonly TimeSpan Patience = DailyRunKit.Patience;

    [Theory]
    [InlineData("query_store")]
    [InlineData("plan_correction")]
    public async Task TheShutdownDrain_WaitsForARunDetachedByName_ThatIsStillGoing(string collector)
    {
        Assert.True(DarlingWorker.IsDetachedByName(collector), "the collector under test is one of the three detached by name");
        var logger = new DailyRunLogger();
        var worker = DailyRunKit.MakeWorker(logger);
        var runs = new HeldRuns(worker, collector);
        var server = DailyRunKit.MakeServer(7401, collector);
        using var stopping = new CancellationTokenSource();

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, stopping.Token);
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => runs.Started == 1, Patience), "the run starts and the pass ends beside it");

            /* The service stops with the run still going. */
            stopping.Cancel();
            var drain = worker.DrainInFlightAsync([]);
            Assert.False(
                await DailyRunKit.EndsWithinAsync(drain, TimeSpan.FromMilliseconds(500)),
                "the stop must wait for a run detached by name that is still going, not finish beside it");
            Assert.Single(worker.InFlightDailyRuns);

            runs.ReleaseAll();
            Assert.True(await DailyRunKit.EndsWithinAsync(drain, Patience), "the stop finishes once the run has");
            Assert.True(await DailyRunKit.BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 0, Patience), "a run that ended is no longer tracked");
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    [Fact]
    public void NoRunDetachedByName_IsDroppedUntracked()
    {
        var source = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        /* The behavioral cases above cover two of the three by name; this covers the call itself, which is one line
           for all three. A discarded RunDetachedAsync is what left them out of the shutdown wait. */
        Assert.Equal(0, DailyRunKit.Count(source, "_ = RunDetachedAsync("));
        Assert.Equal(2, DailyRunKit.Count(source, "TrackDailyRun(RunDetachedAsync("));
    }
}
