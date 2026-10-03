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
        private readonly List<(int ServerId, string Collector)> _started = [];
        private readonly List<(int ServerId, string Collector)> _finished = [];

        public FakeRuns(DarlingWorker worker)
        {
            worker.RunOneBodyOverride = async (server, collector, ct) =>
            {
                var key = (server.Runtime!.ServerId, collector);
                lock (_lock)
                {
                    _started.Add(key);
                }

                if (collector == Daily)
                {
                    await _release.WaitAsync(ct);
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
