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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4938: a collector that runs once a day used to run inline in its server's sequential pass, so one long
/// run (index_object_stats took 43 minutes across 72 databases in a field case) held back every 1-minute
/// collector on that server for its whole length. Every daily collector now runs detached from the pass,
/// behind a per-(server, collector) single-flight limit, and a fleet-wide cap bounds how many of them run at
/// once. These cases drive <see cref="DarlingWorker.RunDueCollectorsAsync"/> with a stand-in for the collector
/// run (<see cref="DarlingWorker.RunOneBodyOverride"/>) that a test can hold open, so "the pass ends", "the
/// run is still going" and "the 17th run waits" are observed directly, with no store and no monitored server.
/// </summary>
public sealed class DailyCollectorDetachTests
{
    private const string Daily = "index_object_stats";
    private const string FastTier = "wait_stats";

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(5);

    private static DarlingWorker MakeWorker(RecordingLogger logger) =>
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

    private static DarlingWorker.ServerLoopState MakeServer(int serverId, params string[] dueCollectors)
    {
        var host = $"detach-test-{serverId}";
        var config = new MonitoredServer { Name = host, Host = host };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo(),
            StorageName = host,
            ServerId = serverId,
        };
        var server = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
        foreach (var name in dueCollectors)
        {
            MarkDue(server, name);
        }

        return server;
    }

    private static void MarkDue(DarlingWorker.ServerLoopState server, string collector) =>
        server.NextDue[collector] = DateTime.UtcNow.AddMinutes(-5);

    /// <summary>
    /// The stand-in for a collector run. A daily run holds open until a test releases it, one release per run,
    /// and a 1-minute run returns at once. Every start and finish is recorded under a lock because the runs
    /// are detached and report from the thread pool.
    /// </summary>
    private sealed class FakeRuns
    {
        private readonly object _lock = new();
        private readonly SemaphoreSlim _release = new(0);
        private readonly bool _holdsThroughStop;
        private readonly HashSet<string> _holds;
        private readonly List<(int ServerId, string Collector)> _started = [];
        private readonly List<(int ServerId, string Collector)> _finished = [];
        private readonly Dictionary<int, ServerRuntime> _dailyRuntimes = [];

        /// <param name="worker">The worker whose collector runs this stands in for.</param>
        /// <param name="holdsThroughStop">
        /// A daily run's hold ignores the token the run was dispatched with, as a run in the middle of a store write
        /// does, so a test can stop the service while runs are still going and see what the stop does to each one.
        /// </param>
        /// <param name="holds">
        /// The collectors whose run holds open until a test releases it: index_object_stats unless a test names
        /// others, such as an on-load collector whose scheduled run is detached too.
        /// </param>
        public FakeRuns(DarlingWorker worker, bool holdsThroughStop = false, string[]? holds = null)
        {
            _holdsThroughStop = holdsThroughStop;
            _holds = new HashSet<string>(holds ?? [Daily], StringComparer.Ordinal);
            worker.RunOneBodyOverride = async (_, runtime, collector, ct) =>
            {
                var key = (runtime.ServerId, collector);
                lock (_lock)
                {
                    _started.Add(key);
                    if (collector == Daily)
                    {
                        _dailyRuntimes[runtime.ServerId] = runtime;
                    }
                }

                if (_holds.Contains(collector))
                {
                    await _release.WaitAsync(_holdsThroughStop ? CancellationToken.None : ct);
                }

                lock (_lock)
                {
                    _finished.Add(key);
                }

                return 1;
            };
        }

        public int Started(string collector)
        {
            lock (_lock)
            {
                return _started.Count(s => s.Collector == collector);
            }
        }

        public int Finished(string collector)
        {
            lock (_lock)
            {
                return _finished.Count(s => s.Collector == collector);
            }
        }

        public HashSet<int> FinishedServers(string collector)
        {
            lock (_lock)
            {
                return _finished.Where(s => s.Collector == collector).Select(s => s.ServerId).ToHashSet();
            }
        }

        public bool HasStarted(int serverId, string collector)
        {
            lock (_lock)
            {
                return _started.Contains((serverId, collector));
            }
        }

        /// <summary>The runtime the daily run for this server was given when it started, or null if it has not.</summary>
        public ServerRuntime? DailyRuntimeOf(int serverId)
        {
            lock (_lock)
            {
                return _dailyRuntimes.GetValueOrDefault(serverId);
            }
        }

        public void ReleaseOne() => _release.Release();

        public void ReleaseAll() => _release.Release(1000);
    }

    private static async Task<bool> EndsWithinAsync(Task task, TimeSpan patience) =>
        await Task.WhenAny(task, Task.Delay(patience)) == task;

    private static async Task<bool> BecomesTrueAsync(Func<bool> condition, TimeSpan patience)
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

    [Fact]
    public async Task TheServerPassEnds_AndTheFastTierRunsOnTime_WhileADailyRunIsBlocked()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var server = MakeServer(4101, Daily, FastTier);
        var ct = TestContext.Current.CancellationToken;

        var firstPass = worker.RunDueCollectorsAsync(server, null!, ct);
        try
        {
            Assert.True(
                await EndsWithinAsync(firstPass, Patience),
                "the server pass must end while the daily run is still blocked, not wait inside it");
            Assert.Equal(1, runs.Started(Daily));
            Assert.Equal(1, runs.Started(FastTier));
            Assert.Equal(0, runs.Finished(Daily));

            /* A minute later the 1-minute collector comes due again, and the daily run is still going. */
            MarkDue(server, FastTier);
            var secondPass = worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await EndsWithinAsync(secondPass, Patience), "the next pass must not queue behind the daily run");
            Assert.Equal(2, runs.Started(FastTier));
            Assert.Equal(1, runs.Started(Daily));
            Assert.Equal(0, runs.Finished(Daily));
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(firstPass, Patience);
        }
    }

    [Fact]
    public async Task TheSeventeenthDailyRunWaitsForAPermit_ThenRuns_AndNoRunIsDropped()
    {
        const int Servers = 17;
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var servers = Enumerable.Range(0, Servers).Select(i => MakeServer(4200 + i, Daily)).ToList();
        var ct = TestContext.Current.CancellationToken;

        var passes = servers.Select(s => worker.RunDueCollectorsAsync(s, null!, ct)).ToList();
        try
        {
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) >= 16, Patience), "16 daily runs start at once");

            /* Give the 17th every chance to start; it must not. */
            await Task.Delay(300, ct);
            Assert.Equal(16, runs.Started(Daily));
            Assert.Equal(0, runs.Finished(Daily));

            /* One Debug line says the run is waiting, once, and it names the collector. */
            var waitLines = WaitLines(logger);
            Assert.Single(waitLines);
            Assert.Contains(Daily, waitLines[0].Message, StringComparison.Ordinal);

            /* Its next tick finds the waiting run still unfinished: it skips, and does not log a second wait. */
            var waiting = servers.Single(s => !runs.HasStarted(s.Runtime!.ServerId, Daily));
            MarkDue(waiting, Daily);
            await worker.RunDueCollectorsAsync(waiting, null!, ct);
            Assert.Equal(16, runs.Started(Daily));
            Assert.Single(WaitLines(logger));

            /* One run finishes and the waiting one starts. */
            runs.ReleaseOne();
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == Servers, Patience), "the 17th run starts when a permit frees");

            runs.ReleaseAll();
            Assert.True(await BecomesTrueAsync(() => runs.Finished(Daily) == Servers, Patience), "every run finishes");
            Assert.Equal(Servers, runs.FinishedServers(Daily).Count);
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(passes), Patience);
        }
    }

    private static List<(LogLevel Level, string Message, Exception? Exception)> WaitLines(RecordingLogger logger) =>
        logger.Snapshot()
            .Where(e => e.Level == LogLevel.Debug && e.Message.Contains("daily-run permit", StringComparison.Ordinal))
            .ToList();

    /* Shutdown. The service stops with daily runs going, and nothing but the shutdown drain (InFlightDailyRuns) holds
       a detached run any more. Its budget is 15 s, so a drain that ends inside Patience ended because the runs did. */

    [Fact]
    public async Task TheShutdownDrain_WaitsForADailyRunThatIsStillGoing()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker, holdsThroughStop: true);
        var server = MakeServer(4501, Daily);
        using var stopping = new CancellationTokenSource();

        try
        {
            await worker.RunDueCollectorsAsync(server, null!, stopping.Token);
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 1, Patience), "the daily run starts");
            Assert.Single(worker.InFlightDailyRuns);

            /* The service stops with the run still going. */
            stopping.Cancel();
            var drain = worker.DrainInFlightAsync([]);
            Assert.False(
                await EndsWithinAsync(drain, TimeSpan.FromMilliseconds(500)),
                "the stop must wait for a daily run that is still going, not finish beside it");
            Assert.Equal(0, runs.Finished(Daily));

            runs.ReleaseAll();
            Assert.True(await EndsWithinAsync(drain, Patience), "the stop finishes once the run has");
            Assert.Equal(1, runs.Finished(Daily));
            Assert.True(await BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 0, Patience), "a run that ended is no longer tracked");
        }
        finally
        {
            runs.ReleaseAll();
        }
    }

    [Fact]
    public async Task ADailyRunQueuedForAPermit_EndsAtOnceWhenTheServiceStops_AndNothingFaults()
    {
        const int Servers = 17;
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker, holdsThroughStop: true);
        var servers = Enumerable.Range(0, Servers).Select(i => MakeServer(4600 + i, Daily)).ToList();
        using var stopping = new CancellationTokenSource();

        var passes = servers.Select(s => worker.RunDueCollectorsAsync(s, null!, stopping.Token)).ToList();
        try
        {
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 16, Patience), "16 daily runs start at once");
            Assert.True(await BecomesTrueAsync(() => WaitLines(logger).Count == 1, Patience), "the 17th run is queued for a permit");
            var tracked = worker.InFlightDailyRuns.ToList();
            Assert.Equal(Servers, tracked.Count);

            /* Only the queued run can see the stop: the 16 that started hold on a wait that ignores it. */
            stopping.Cancel();
            Assert.True(
                await BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 16, Patience),
                "a run queued for a permit must end at once on the stopping token");
            var ended = tracked.Single(t => t.IsCompleted);
            Assert.True(
                ended.Status == TaskStatus.RanToCompletion,
                $"the cancel must be contained inside the run, but the run ended {ended.Status}");
            Assert.Equal(16, runs.Started(Daily));
            Assert.Equal(0, runs.Finished(Daily));

            /* The 16 that were going are still awaited, and once they end every run is complete without a fault. */
            var drain = worker.DrainInFlightAsync([]);
            Assert.False(await EndsWithinAsync(drain, TimeSpan.FromMilliseconds(300)), "the stop must wait for the 16 runs still going");
            runs.ReleaseAll();
            Assert.True(await EndsWithinAsync(drain, Patience), "the stop finishes once the runs have");
            Assert.All(tracked, t => Assert.Equal(TaskStatus.RanToCompletion, t.Status));
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(passes), Patience);
        }
    }

    [Fact]
    public void TheShutdownMakesItsWaitThroughTheDrain_ThatAddsTheDailyRuns()
    {
        var worker = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        /* The wait the cases above call is the wait the shutdown makes: ExecuteAsync reaches it once, and it is
           the one place the detached daily runs join the shutdown wait. */
        Assert.Equal(1, CountOf(worker, "await DrainInFlightAsync(inFlightSweeps);"));
        Assert.Equal(1, CountOf(worker, "inFlight.AddRange(InFlightDailyRuns);"));
    }

    /* The re-read after a long wait. A run queued for a permit can wait for hours behind other daily runs, so what it
       works on is read again once it has its permit. The override is given the runtime the run is about to use. */

    private sealed record QueuedRun(List<DarlingWorker.ServerLoopState> Servers, DarlingWorker.ServerLoopState Queued, List<Task> Passes);

    /// <summary>
    /// 17 servers come due for a daily run together: 16 start and hold a permit, and the 17th is queued for one.
    /// Returns once the 17th is confirmed queued, so a test can change its server while the run waits.
    /// </summary>
    private static async Task<QueuedRun> QueueTheSeventeenthAsync(
        DarlingWorker worker, RecordingLogger logger, FakeRuns runs, int firstServerId, CancellationToken ct)
    {
        var servers = Enumerable.Range(0, 17).Select(i => MakeServer(firstServerId + i, Daily)).ToList();
        var passes = servers.Select(s => worker.RunDueCollectorsAsync(s, null!, ct)).ToList();

        Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 16, Patience), "16 daily runs start at once");
        Assert.True(await BecomesTrueAsync(() => WaitLines(logger).Count == 1, Patience), "the 17th run is queued for a permit");
        return new QueuedRun(servers, servers.Single(s => !runs.HasStarted(s.Runtime!.ServerId, Daily)), passes);
    }

    /// <summary>The same server after a reconnect: the same identity, and a new connection.</summary>
    private static ServerRuntime Reconnected(ServerRuntime old) => Reconnected(old, old.Target);

    /// <summary>The same server after a reconnect that reached a target of a different kind.</summary>
    private static ServerRuntime Reconnected(ServerRuntime old, CollectorTargetInfo target) =>
        new()
        {
            Config = old.Config,
            ConnectionString = old.ConnectionString + ";Application Name=reconnected",
            Target = target,
            StorageName = old.StorageName,
            ServerId = old.ServerId,
        };

    [Fact]
    public async Task ADailyRun_ThatWaitedForAPermitAndFindsItsServerRemoved_DoesNotRun()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var ct = TestContext.Current.CancellationToken;
        var queued = await QueueTheSeventeenthAsync(worker, logger, runs, 4700, ct);
        try
        {
            var queuedId = queued.Queued.Runtime!.ServerId;

            /* The server is removed while the run waits. Its connection is left in place, so it is the removal that
               stops the run and not the missing connection (the next case). */
            queued.Queued.Retired = true;

            runs.ReleaseOne();
            Assert.True(
                await BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 15 || runs.HasStarted(queuedId, Daily), Patience),
                "the queued run ends once a permit frees");
            Assert.False(runs.HasStarted(queuedId, Daily), "a run that finds its server removed must not run");
            Assert.Equal(16, runs.Started(Daily));
            Assert.Equal(15, worker.InFlightDailyRuns.Count);
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(queued.Passes), Patience);
        }
    }

    [Fact]
    public async Task ADailyRun_ThatWaitedForAPermitAndFindsItsServerDisconnected_DoesNotRun()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var ct = TestContext.Current.CancellationToken;
        var queued = await QueueTheSeventeenthAsync(worker, logger, runs, 4800, ct);
        try
        {
            var queuedId = queued.Queued.Runtime!.ServerId;

            queued.Queued.Runtime = null;

            runs.ReleaseOne();
            Assert.True(
                await BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 15 || runs.HasStarted(queuedId, Daily), Patience),
                "the queued run ends once a permit frees");
            Assert.False(runs.HasStarted(queuedId, Daily), "a run that finds its server disconnected must not run");
            Assert.Equal(16, runs.Started(Daily));
            Assert.Equal(15, worker.InFlightDailyRuns.Count);
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(queued.Passes), Patience);
        }
    }

    [Fact]
    public async Task ADailyRun_ThatWaitedForAPermitAndFindsItsServerReconnected_UsesTheNewConnection()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var ct = TestContext.Current.CancellationToken;
        var queued = await QueueTheSeventeenthAsync(worker, logger, runs, 4900, ct);
        try
        {
            var capturedAtDispatch = queued.Queued.Runtime!;
            var queuedId = capturedAtDispatch.ServerId;

            /* The server reconnects while the run waits: same identity, a new connection. */
            var reconnected = Reconnected(capturedAtDispatch);
            queued.Queued.Runtime = reconnected;

            runs.ReleaseOne();
            Assert.True(await BecomesTrueAsync(() => runs.HasStarted(queuedId, Daily), Patience), "the queued run starts once a permit frees");

            var used = runs.DailyRuntimeOf(queuedId);
            Assert.False(
                ReferenceEquals(used, capturedAtDispatch),
                "a run that waited must not use the connection its server had when it was dispatched");
            Assert.True(
                ReferenceEquals(used, reconnected),
                "a run that waited must use the connection its server has now");
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(queued.Passes), Patience);
        }
    }

    /* What the run asks again after the wait. The dispatch asked more than whether the server is still there: whether
       collection is paused (the loop dispatches nothing while it is), whether the collector is still enabled for the
       server, and whether it still applies to the target. Each one is changed while the 17th run waits. */

    private static void SetField(DarlingWorker worker, string name, object value) =>
        typeof(DarlingWorker)
            .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, value);

    /// <summary>Frees one permit, so the queued run wakes, and returns once that run has ended or has started.</summary>
    private static async Task WakeTheQueuedRunAsync(DarlingWorker worker, FakeRuns runs, int queuedId)
    {
        runs.ReleaseOne();
        Assert.True(
            await BecomesTrueAsync(() => worker.InFlightDailyRuns.Count == 15 || runs.HasStarted(queuedId, Daily), Patience),
            "the queued run ends once a permit frees");
    }

    [Fact]
    public async Task ADailyRun_ThatWaitedForAPermitAndFindsCollectionPaused_DoesNotRun_AndIsDueAgainOnResume()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var ct = TestContext.Current.CancellationToken;
        var queued = await QueueTheSeventeenthAsync(worker, logger, runs, 5300, ct);
        try
        {
            var queuedId = queued.Queued.Runtime!.ServerId;
            Assert.True(queued.Queued.NextDue[Daily] > DateTime.UtcNow.AddHours(1), "the dispatch moved the due time a day on");

            /* Collection is paused while the run waits. */
            SetField(worker, "_paused", true);

            await WakeTheQueuedRunAsync(worker, runs, queuedId);
            Assert.False(runs.HasStarted(queuedId, Daily), "a run that finds collection paused must not run");
            Assert.Equal(16, runs.Started(Daily));
            Assert.Equal(15, worker.InFlightDailyRuns.Count);

            /* Nothing else moves a due time on a resume, so a skipped run left a day on would wait until tomorrow:
               it is due again, and the first pass after the resume runs it. */
            Assert.True(queued.Queued.NextDue[Daily] <= DateTime.UtcNow, "a run skipped for the pause must be due again, not a day away");
            SetField(worker, "_paused", false);
            await worker.RunDueCollectorsAsync(queued.Queued, null!, ct);
            Assert.True(await BecomesTrueAsync(() => runs.HasStarted(queuedId, Daily), Patience), "the pass after the resume runs it");
        }
        finally
        {
            SetField(worker, "_paused", false);
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(queued.Passes), Patience);
        }
    }

    [Fact]
    public async Task ADailyRun_ThatWaitedForAPermitAndFindsItsCollectorDisabled_DoesNotRun()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var ct = TestContext.Current.CancellationToken;
        var queued = await QueueTheSeventeenthAsync(worker, logger, runs, 5400, ct);
        try
        {
            var queuedId = queued.Queued.Runtime!.ServerId;

            /* An operator turns the collector off for this server while the run waits. */
            SetField(worker, "_scheduleOverrides", new List<ScheduleOverride> { new(queuedId, Daily, null, null, Enabled: false) });

            await WakeTheQueuedRunAsync(worker, runs, queuedId);
            Assert.False(runs.HasStarted(queuedId, Daily), "a run whose collector was disabled while it waited must not run");
            Assert.Equal(16, runs.Started(Daily));
            Assert.Equal(15, worker.InFlightDailyRuns.Count);
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(queued.Passes), Patience);
        }
    }

    [Fact]
    public async Task ADailyRun_ThatWaitedForAPermitAndFindsTheCollectorNoLongerApplyingToItsTarget_DoesNotRun()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var ct = TestContext.Current.CancellationToken;
        var queued = await QueueTheSeventeenthAsync(worker, logger, runs, 5500, ct);
        try
        {
            var capturedAtDispatch = queued.Queued.Runtime!;
            var queuedId = capturedAtDispatch.ServerId;
            Assert.True(CollectorCatalog.AppliesTo(Daily, capturedAtDispatch.Target));

            /* The server reconnects while the run waits, and the target it reaches now is a PostgreSQL one: a SQL Server
               collector does not apply to it. */
            var postgres = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql };
            Assert.False(CollectorCatalog.AppliesTo(Daily, postgres));
            queued.Queued.Runtime = Reconnected(capturedAtDispatch, postgres);

            await WakeTheQueuedRunAsync(worker, runs, queuedId);
            Assert.False(runs.HasStarted(queuedId, Daily), "a run whose collector no longer applies to the target must not run");
            Assert.Equal(16, runs.Started(Daily));
            Assert.Equal(15, worker.InFlightDailyRuns.Count);
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(queued.Passes), Patience);
        }
    }

    /* A reconnect while a detached run is still going. The at-connect loop runs the on-load collectors inline, and
       each one's scheduled run is detached (its recapture is daily). A fault and a reconnect in the middle of that
       scheduled run brought the loop to the same collector, and a second run started beside the first. */

    private const string OnLoad = "server_config";

    private static EffectiveSchedule OnLoadSchedule(int serverId) => StoreConfigProvider.ResolveSchedule(OnLoad, serverId, []);

    [Fact]
    public async Task AReconnect_WhileTheScheduledRunOfAnOnLoadCollectorIsStillGoing_StartsNoSecondRun()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker, holds: [OnLoad]);
        var server = MakeServer(5601, OnLoad);
        var ct = TestContext.Current.CancellationToken;

        var scheduledPass = worker.RunDueCollectorsAsync(server, null!, ct);
        Task? atConnect = null;
        try
        {
            Assert.True(await EndsWithinAsync(scheduledPass, Patience));
            Assert.True(await BecomesTrueAsync(() => runs.Started(OnLoad) == 1, Patience), "the scheduled run is going");

            /* The reconnect brings the at-connect loop to the same collector. */
            atConnect = worker.RunOnLoadAsync(server, null!, OnLoad, 5601, OnLoadSchedule(5601), ct);
            Assert.True(
                await EndsWithinAsync(atConnect, Patience),
                "a reconnect must leave out an on-load collector whose scheduled run is still going, not run it a second time and wait on it");
            Assert.Equal(1, runs.Started(OnLoad));
            Assert.Contains(
                logger.Snapshot(),
                e => e.Level == LogLevel.Information
                     && e.Message.Contains(OnLoad, StringComparison.Ordinal)
                     && e.Message.Contains("still going", StringComparison.Ordinal));
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(scheduledPass, Patience);
            if (atConnect is not null)
            {
                await EndsWithinAsync(atConnect, Patience);
            }
        }
    }

    [Fact]
    public async Task TheAtConnectRunOfAnOnLoadCollector_RunsWhenNoScheduledRunHoldsTheSlot_AndHoldsItOnlyForItsOwnRun()
    {
        var worker = MakeWorker(new RecordingLogger());
        var runs = new FakeRuns(worker, holds: [OnLoad]);
        var server = MakeServer(5701);
        var ct = TestContext.Current.CancellationToken;

        var atConnect = worker.RunOnLoadAsync(server, null!, OnLoad, 5701, OnLoadSchedule(5701), ct);
        try
        {
            Assert.True(await BecomesTrueAsync(() => runs.Started(OnLoad) == 1, Patience), "with no scheduled run going, the at-connect run happens");
            Assert.False(atConnect.IsCompleted, "the at-connect run is inline: the connect waits for it");

            /* While it runs it holds the slot, so a scheduled run of the same collector leaves it alone. */
            MarkDue(server, OnLoad);
            await worker.RunDueCollectorsAsync(server, null!, ct);
            await Task.Delay(100, ct);
            Assert.Equal(1, runs.Started(OnLoad));

            runs.ReleaseAll();
            Assert.True(await EndsWithinAsync(atConnect, Patience));

            /* And gives it back: the next scheduled run starts. */
            MarkDue(server, OnLoad);
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await BecomesTrueAsync(() => runs.Started(OnLoad) == 2, Patience), "the slot was released after the at-connect run");
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(atConnect, Patience);
        }
    }

    [Fact]
    public void TheAtConnectLoop_RunsAnOnLoadCollectorOnlyThroughTheMethodThatTakesTheSlot()
    {
        var worker = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        Assert.Equal(1, CountOf(worker, "await RunOnLoadAsync(server, runner, name, serverId, effective, cancellationToken);"));

        /* The one other call that spells the collector `name` and passes no peer mark is the snapshot's, which takes
           the slot itself; a second one would be the at-connect loop running a collector without it. */
        Assert.Equal(1, CountOf(worker, "await RunOneAsync(server, runner, name, peerMaxAtDispatchMs: null, cancellationToken);"));
    }

    /* A server removed and added back. The id is the registration's, so the new state has the removed one's id, and a
       run of the removed state that is still going, or still queued for a permit, held the slot the new state's first
       daily run needs: that run skipped, and a skipped daily run waits a day. */

    private static ServerRuntime RuntimeFor(MonitoredServer config, int serverId) =>
        new()
        {
            Config = config,
            ConnectionString = $"Server=tcp:{config.Host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo(),
            StorageName = config.Host,
            ServerId = serverId,
        };

    private static void Reconcile(DarlingWorker worker, List<DarlingWorker.ServerLoopState> servers, List<MonitoredServer> desired) =>
        typeof(DarlingWorker)
            .GetMethod("ReconcileServers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(worker, [servers, desired]);

    [Fact]
    public async Task AServerRemovedAndAddedBack_WhileItsDailyRunIsStillGoing_IsNotHeldOffForADay()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var ct = TestContext.Current.CancellationToken;
        const int ServerId = 5801;
        const int NeighbourId = 5802;

        var config = new MonitoredServer { Name = "readded", Host = "readded", StoredServerId = ServerId };
        var first = new DarlingWorker.ServerLoopState { Config = config, Runtime = RuntimeFor(config, ServerId) };
        MarkDue(first, Daily);
        var neighbourConfig = new MonitoredServer { Name = "neighbour", Host = "neighbour", StoredServerId = NeighbourId };
        var neighbour = new DarlingWorker.ServerLoopState { Config = neighbourConfig, Runtime = RuntimeFor(neighbourConfig, NeighbourId) };
        MarkDue(neighbour, Daily);
        var servers = new List<DarlingWorker.ServerLoopState> { first, neighbour };

        var passes = new List<Task>
        {
            worker.RunDueCollectorsAsync(first, null!, ct),
            worker.RunDueCollectorsAsync(neighbour, null!, ct),
        };
        try
        {
            Assert.True(await EndsWithinAsync(Task.WhenAll(passes), Patience));
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 2, Patience), "both daily runs are going");

            /* The operator disables the server and enables it again while its run is still going. */
            Reconcile(worker, servers, [neighbourConfig]);
            Assert.True(first.Retired);
            Assert.Single(servers);
            Reconcile(worker, servers, [neighbourConfig, config]);
            var second = servers.Single(s => s.Config.ServerId == ServerId);
            Assert.NotSame(first, second);

            /* The connect path gives the new state its connection, and its daily collector is due. */
            second.Runtime = RuntimeFor(config, ServerId);
            MarkDue(second, Daily);
            await worker.RunDueCollectorsAsync(second, null!, ct);
            Assert.True(
                await BecomesTrueAsync(() => runs.Started(Daily) == 3, Patience),
                "a server added back must run its daily collector, not skip it for a day behind the run of the state that was removed");

            /* Only the removed server's slot was cleared: a neighbour's run that is still going keeps its slot. */
            MarkDue(neighbour, Daily);
            await worker.RunDueCollectorsAsync(neighbour, null!, ct);
            await Task.Delay(100, ct);
            Assert.Equal(3, runs.Started(Daily));
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(Task.WhenAll(passes), Patience);
        }
    }

    [Fact]
    public async Task ADailyCollector_WhosePreviousDetachedRunIsStillGoing_SkipsThatTick()
    {
        var logger = new RecordingLogger();
        var worker = MakeWorker(logger);
        var runs = new FakeRuns(worker);
        var server = MakeServer(4301, Daily);
        var ct = TestContext.Current.CancellationToken;

        var firstPass = worker.RunDueCollectorsAsync(server, null!, ct);
        try
        {
            Assert.True(await EndsWithinAsync(firstPass, Patience), "the pass must end while the daily run is blocked");
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 1, Patience));

            MarkDue(server, Daily);
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.Equal(1, runs.Started(Daily));
            Assert.Contains(
                logger.Snapshot(),
                e => e.Level == LogLevel.Information && e.Message.Contains("previous", StringComparison.Ordinal) && e.Message.Contains(Daily, StringComparison.Ordinal));

            /* The skip lasts only while the run does: once it finishes, the next due tick runs. */
            runs.ReleaseAll();
            Assert.True(await BecomesTrueAsync(() => runs.Finished(Daily) == 1, Patience));
            var restarted = false;
            for (var attempt = 0; attempt < 100 && !restarted; attempt++)
            {
                MarkDue(server, Daily);
                await worker.RunDueCollectorsAsync(server, null!, ct);
                restarted = runs.Started(Daily) == 2;
                if (!restarted)
                {
                    await Task.Delay(20, ct);
                }
            }

            Assert.True(restarted, "a finished daily run must not keep skipping the next tick");
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(firstPass, Patience);
        }
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(60, false)]
    [InlineData(1439, false)]
    [InlineData(1440, true)]
    [InlineData(2880, true)]
    public void ADailyInterval_IsOneDayOrMore(int effectiveIntervalMinutes, bool expected) =>
        Assert.Equal(expected, DarlingWorker.IsDailyInterval(effectiveIntervalMinutes));

    [Fact]
    public void TheDailyCollectors_AreTheOnLoadSetAndTheCatalogsOneDayCollectors()
    {
        var daily = CollectorScheduleDefaults.All
            .Where(kv => DarlingWorker.IsDailyInterval(CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(kv.Value.FrequencyMinutes)))
            .Select(kv => kv.Key)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToList();

        Assert.Equal(
            new[]
            {
                "database_config", "database_scoped_config", "index_object_stats", "pg_column_stats", "pg_extension_availability",
                "pg_index_bloat", "pg_index_usage_stats", "server_config", "server_properties", "trace_flags",
            },
            daily);
    }

    [Fact]
    public void ADetachedDailyRun_DoesNotFoldItsDurationIntoTheSweepBodysPeerMark()
    {
        /* index_object_stats carries no wall-clock budget, so the inline run folds; detached, it must not. */
        Assert.False(CollectorCatalog.HasWallClockBudget(Daily));
        Assert.True(DarlingWorker.FoldsIntoSweepPeerMark(Daily, detachedDaily: false));
        Assert.False(DarlingWorker.FoldsIntoSweepPeerMark(Daily, detachedDaily: true));
        Assert.True(DarlingWorker.FoldsIntoSweepPeerMark(FastTier, detachedDaily: false));
    }

    /// <summary>
    /// #4999: the exclusion is keyed on "ran detached", not on "budgeted". pg_wait_sampling is detached by name
    /// (#3604) and carries no wall-clock budget, so the budget rule alone let its ~30 s run into the peer mark of
    /// whichever body happened to be running when it finished: the same false-slow-peers reading a daily run
    /// would have caused. query_store and plan_correction are budgeted and stay out either way.
    /// </summary>
    [Theory]
    [InlineData("pg_wait_sampling")]
    [InlineData("query_store")]
    [InlineData("plan_correction")]
    public void AnyDetachedRun_ByName_DoesNotFoldIntoTheSweepBodysPeerMark(string name)
    {
        Assert.True(DarlingWorker.IsDetachedByName(name));
        Assert.False(DarlingWorker.FoldsIntoSweepPeerMark(name, detachedDaily: false));
    }

    [Fact]
    public void TheDetachedRuleAloneKeepsAnUnbudgetedDetachedRunOutOfThePeerMark()
    {
        /* The case the budget rule cannot reach: detached, and unbudgeted. */
        Assert.False(CollectorCatalog.HasWallClockBudget("pg_wait_sampling"));

        var server = MakeServer(4401);
        Assert.Equal(-1, server.SweepPeerMaxMs);

        /* A finished detached run leaves the mark as the body made it, whichever kind of detached run it is. */
        DarlingWorker.FoldIntoSweepPeerMark(server, "pg_wait_sampling", detachedDaily: false, sqlMs: 30_000);
        Assert.Equal(-1, server.SweepPeerMaxMs);
        DarlingWorker.FoldIntoSweepPeerMark(server, Daily, detachedDaily: true, sqlMs: 2_600_000);
        Assert.Equal(-1, server.SweepPeerMaxMs);

        /* An ordinary unbudgeted run in the body still folds, and a budgeted one still does not (#2864). */
        DarlingWorker.FoldIntoSweepPeerMark(server, FastTier, detachedDaily: false, sqlMs: 120);
        Assert.Equal(120, server.SweepPeerMaxMs);
        Assert.True(CollectorCatalog.HasWallClockBudget("procedure_stats"));
        DarlingWorker.FoldIntoSweepPeerMark(server, "procedure_stats", detachedDaily: false, sqlMs: 9_000);
        Assert.Equal(120, server.SweepPeerMaxMs);

        /* And a detached run finishing after the body has built its mark does not raise it. */
        DarlingWorker.FoldIntoSweepPeerMark(server, "pg_wait_sampling", detachedDaily: false, sqlMs: 30_000);
        Assert.Equal(120, server.SweepPeerMaxMs);
    }

    private static async Task<(int CollectorsRun, List<(string Collector, string Reason)> Skipped)> ReadSnapshotAsync(Task<CommandOutcome> snapshot)
    {
        var outcome = await snapshot;
        Assert.True(outcome.Success);
        using var json = System.Text.Json.JsonDocument.Parse(outcome.ResultJson!);
        var skipped = new List<(string, string)>();
        if (json.RootElement.TryGetProperty("skipped", out var list))
        {
            foreach (var item in list.EnumerateArray())
            {
                skipped.Add((item.GetProperty("collector").GetString()!, item.GetProperty("reason").GetString()!));
            }
        }

        return (json.RootElement.GetProperty("collectorsRun").GetInt32(), skipped);
    }

    /// <summary>
    /// #4999: snapshot_now takes the same single-flight slot a scheduled daily run holds. While that run is going
    /// the snapshot leaves the collector out and says its scheduled run is still going, instead of running it a
    /// second time beside the first. The snapshot's other collectors still run, and it still ends.
    /// </summary>
    [Fact]
    public async Task ASnapshot_SkipsADailyCollector_WhoseScheduledRunIsStillGoing_AndSaysSo()
    {
        var worker = MakeWorker(new RecordingLogger());
        var runs = new FakeRuns(worker);
        var server = MakeServer(4501, Daily);
        var ct = TestContext.Current.CancellationToken;

        var scheduledPass = worker.RunDueCollectorsAsync(server, null!, ct);
        Task<CommandOutcome>? snapshot = null;
        try
        {
            Assert.True(await EndsWithinAsync(scheduledPass, Patience));
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 1, Patience), "the scheduled daily run is going");

            snapshot = worker.RunSnapshotAsync([server], null!, server.Config.ServerId, ct);
            Assert.True(
                await EndsWithinAsync(snapshot, Patience),
                "the snapshot must skip the collector whose scheduled run holds the slot, not run it a second time and wait on it");

            var (collectorsRun, skipped) = await ReadSnapshotAsync(snapshot);
            Assert.Equal(1, runs.Started(Daily));
            Assert.True(collectorsRun > 0, "the snapshot's other collectors still run");
            var only = Assert.Single(skipped);
            Assert.Equal(Daily, only.Collector);
            Assert.Contains("scheduled", only.Reason, StringComparison.Ordinal);
            Assert.Contains("still going", only.Reason, StringComparison.Ordinal);
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(scheduledPass, Patience);
            if (snapshot is not null)
            {
                await EndsWithinAsync(snapshot, Patience);
            }
        }
    }

    /// <summary>
    /// #4999: with the slot free the snapshot runs the daily collector inline (the snapshot waits for it), holds
    /// the slot for that run, and gives it back after: the next scheduled run is not skipped by a snapshot that
    /// is over.
    /// </summary>
    [Fact]
    public async Task ASnapshot_RunsADailyCollectorInline_HoldingTheSlotOnlyForThatRun()
    {
        var worker = MakeWorker(new RecordingLogger());
        var runs = new FakeRuns(worker);
        var server = MakeServer(4601);
        var ct = TestContext.Current.CancellationToken;

        var snapshot = worker.RunSnapshotAsync([server], null!, server.Config.ServerId, ct);
        try
        {
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 1, Patience), "the snapshot runs the daily collector");
            Assert.False(snapshot.IsCompleted, "the daily run is inline: the snapshot waits for it");

            runs.ReleaseAll();
            Assert.True(await EndsWithinAsync(snapshot, Patience));
            var (_, skipped) = await ReadSnapshotAsync(snapshot);
            Assert.Empty(skipped);

            /* The slot is free again: a scheduled run of the same collector starts. */
            MarkDue(server, Daily);
            await worker.RunDueCollectorsAsync(server, null!, ct);
            Assert.True(await BecomesTrueAsync(() => runs.Started(Daily) == 2, Patience), "the slot was released after the snapshot's run");
        }
        finally
        {
            runs.ReleaseAll();
            await EndsWithinAsync(snapshot, Patience);
        }
    }

    /// <summary>
    /// The run reaches the mark only through the rule above: the one call in the run, and the one place that
    /// writes the high-water mark. A write inline in the run would bypass the rule and read as correct in every
    /// case above.
    /// </summary>
    [Fact]
    public void TheMarkIsWrittenOnlyThroughTheRuleThatKeepsDetachedRunsOut()
    {
        var worker = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        Assert.Equal(1, CountOf(worker, "FoldIntoSweepPeerMark(server, collectorName, detachedDaily, result.SqlMs);"));
        Assert.Equal(1, CountOf(worker, "server.SweepPeerMaxMs = (int)Math.Min("));
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private sealed class RecordingLogger : ILogger<DarlingWorker>
    {
        private List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = [];

        public List<(LogLevel Level, string Message, Exception? Exception)> Snapshot()
        {
            lock (Entries)
            {
                return [.. Entries];
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (Entries)
            {
                Entries.Add((logLevel, formatter(state, exception), exception));
            }
        }
    }
}
