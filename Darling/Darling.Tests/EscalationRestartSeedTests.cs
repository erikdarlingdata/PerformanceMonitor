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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5715 part B: the Warning-to-Critical escalation across a service restart. The "history" is the first engine's own
/// delivered outcomes, read back through the seed reader the way the alert-history store is; the restart is a NEW
/// engine over the same adapter, state store and deliverer, so only the in-memory tier is lost. The observable is the
/// Critical's <see cref="AlertContext.EscalationBypassesCooldown"/> flag, which is what lets the email and webhook
/// cooldowns (seeded from the same history) through. The stores' own rules are pinned in
/// <c>EscalationSeedHistoryStoreTests</c> (Lite) and <c>EscalationSeedLiveStoreTests</c> (Darling).
/// </summary>
public sealed class EscalationRestartSeedTests
{
    private static readonly DateTime T0 = new(2026, 10, 13, 10, 0, 0, DateTimeKind.Utc);

    private enum Level { Clear, Warning, Critical }

    public static readonly object[][] Alerts =
    {
        new object[] { "High CPU" },
        new object[] { "Volume Free Space" },
        new object[] { "Runnable Queue" },
    };

    private static AlertEngineTests.Harness Armed(string metric)
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        switch (metric)
        {
            case "High CPU": h.Settings.CpuEnabled = true; break;
            case "Volume Free Space": h.Settings.LowDiskEnabled = true; break;
            case "Runnable Queue": h.Settings.RunnableQueueEnabled = true; break;
            default: throw new ArgumentOutOfRangeException(nameof(metric), metric, null);
        }

        return h;
    }

    /// <summary>One evaluation of <paramref name="metric"/> showing <paramref name="level"/>; returns how many of that
    /// alert's outcomes it delivered.</summary>
    private static async Task<int> StepAsync(string metric, AlertEngineTests.Harness h, AlertEngine engine, DateTime at, Level level, int step)
    {
        h.Now = at;
        var before = h.Deliverer.Outcomes.Count(o => o.MetricName == metric);
        switch (metric)
        {
            case "High CPU":
            {
                double? value = level switch { Level.Warning => 90, Level.Critical => 97, _ => 10 };
                for (var i = 0; i < 3; i++)
                {
                    await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: value, totalCpu: value));
                }

                break;
            }
            case "Volume Free Space":
            {
                h.Adapter.Volumes.Clear();
                if (level != Level.Clear)
                {
                    var freeMb = level == Level.Warning ? 8192 : Math.Max(100, 2900 - (step * 1200));
                    h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 102400, FreeMb = freeMb });
                }

                await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
                break;
            }
            case "Runnable Queue":
            {
                double[] averages = level switch { Level.Warning => new double[] { 9, 9, 9 }, Level.Critical => new double[] { 9, 9, 9, 60 }, _ => new double[] { 0, 0, 0 } };
                var samples = new List<RunnableQueueSample>();
                for (var i = 0; i < averages.Length; i++)
                {
                    samples.Add(new RunnableQueueSample(at.AddMinutes(-(averages.Length - 1 - i)).AddSeconds(-5), averages[i], (long)(averages[i] * 4), 4));
                }

                h.Adapter.RunnableQueue = samples;
                await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
                break;
            }
        }

        return h.Deliverer.Outcomes.Count(o => o.MetricName == metric) - before;
    }

    /// <summary>The alert history, as the store answers it: the tier of the newest delivered fire of the metric at or
    /// after the window start. Every outcome the deliverer received here was a delivered fire.</summary>
    private static Func<EscalationSeedQuery, CancellationToken, Task<AlertSeverityLevel?>> HistoryOf(AlertEngineTests.Harness h) =>
        (query, _) => Task.FromResult<AlertSeverityLevel?>(
            h.Deliverer.Outcomes.LastOrDefault(o => o.MetricName == query.MetricName && o.ServerKey == query.ServerKey) is { } fire
                /* The row's tier, else what the grids make of a tierless row (Volume Free Space persists a tier only for a Critical fire). */
                ? fire.Context?.SeverityOverride ?? fire.Severity ?? (PerformanceMonitor.Common.AlertMetricClassifier.IsCritical(fire.MetricName)
                    ? AlertSeverityLevel.Critical
                    : AlertSeverityLevel.Warning)
                : null);

    private static AlertOutcome Last(string metric, AlertEngineTests.Harness h) => h.Deliverer.Outcomes.Last(o => o.MetricName == metric);

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task AfterARestart_AWarningFromHistory_MakesTheFirstCriticalAnEscalation(string metric)
    {
        var h = Armed(metric);
        var before = h.Build();
        Assert.Equal(1, await StepAsync(metric, h, before, T0, Level.Warning, 0));

        /* The service restarts: a new engine, with the history reader wired. */
        h.EscalationSeedReader = HistoryOf(h);
        var after = h.Build();

        Assert.Equal(1, await StepAsync(metric, h, after, T0.AddMinutes(1), Level.Critical, 1));
        Assert.Equal(AlertSeverityLevel.Critical, Last(metric, h).Severity);
        Assert.True(Last(metric, h).Context!.EscalationBypassesCooldown);

        /* The Critical restarted the clock: it is an ordinary Critical from here on. */
        Assert.Equal(0, await StepAsync(metric, h, after, T0.AddMinutes(2), Level.Critical, 2));
    }

    [Theory]
    [MemberData(nameof(Alerts))]
    public async Task AfterARestart_WithNoSeedReader_TheFirstCriticalIsOnlyAFirstFire(string metric)
    {
        var h = Armed(metric);
        var before = h.Build();
        Assert.Equal(1, await StepAsync(metric, h, before, T0, Level.Warning, 0));

        var after = h.Build();

        /* High CPU's persisted state re-arms its in-engine cooldown, so there the Critical is held outright; the others
           deliver it as a plain first fire. Either way nothing carries the bypass the email and webhook cooldowns need. */
        await StepAsync(metric, h, after, T0.AddMinutes(1), Level.Critical, 1);
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == metric && o.Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task AfterARestart_ACriticalFromHistory_IsNotAnEscalation()
    {
        var h = Armed("High CPU");
        var before = h.Build();
        Assert.Equal(1, await StepAsync("High CPU", h, before, T0, Level.Critical, 0));

        h.EscalationSeedReader = HistoryOf(h);
        var after = h.Build();

        /* Back to Warning, then Critical again, inside the cooldown: the incident already delivered its Critical. */
        Assert.Equal(0, await StepAsync("High CPU", h, after, T0.AddMinutes(1), Level.Warning, 1));
        Assert.Equal(0, await StepAsync("High CPU", h, after, T0.AddMinutes(2), Level.Critical, 2));
    }

    [Fact]
    public async Task TheSeedAsksForAllSixOptedInMetrics_WithTheirResolutionTitles_AndTheCooldownWindow()
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        var queries = new List<EscalationSeedQuery>();
        h.EscalationSeedReader = (q, _) => { queries.Add(q); return Task.FromResult<AlertSeverityLevel?>(null); };

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(
            TierEscalation.OptedInMetrics.OrderBy(m => m, StringComparer.Ordinal),
            queries.Select(q => q.MetricName).OrderBy(m => m, StringComparer.Ordinal));
        Assert.Equal(6, queries.Count);

        var expected = new Dictionary<string, string>
        {
            ["High CPU"] = "CPU Resolved",
            ["Deadlocks Detected"] = "Deadlocks Cleared",
            ["Poison Wait"] = "Poison Waits Cleared",
            ["Runnable Queue"] = "Runnable Queue Cleared",
            ["Client Timeouts"] = "Client Timeouts Cleared",
            ["Volume Free Space"] = "Volume Free Space Resolved",
        };
        foreach (var q in queries)
        {
            Assert.Equal(expected[q.MetricName], q.ResolutionTitle);
            Assert.Equal(T0 - TimeSpan.FromMinutes(h.Settings.CooldownMinutes), q.SinceUtc);
            /* Round 1: the engine was built at T0, and only rows before that instant are history. */
            Assert.Equal(T0, q.UntilUtc);
            /* Round 1: only Volume Free Space's Warning rows are tierless by design. */
            Assert.Equal(q.MetricName == "Volume Free Space", q.TierlessRowIsWarning);
            Assert.Equal(AlertEngineTests.Harness.Snapshot().ServerKey, q.ServerKey);
            /* Only a muted Client Timeouts fire leaves the tier unrecorded in process. */
            Assert.Equal(q.MetricName != "Client Timeouts", q.CountMuted);
        }
    }

    [Fact]
    public void EveryResolutionTitleThePinNamesIsTheOneTheEngineEmits_NotAFreshLiteral()
    {
        var source = System.IO.File.ReadAllText(RepoFile("PerformanceMonitor.Alerting/AlertEngine.cs"));

        /* A resolution row is stored under its title, so a title edited at one site alone would make the seed
           stop matching it. The engine names each through EscalationSeed; no bare literal may reappear. */
        foreach (var title in new[]
        {
            "CPU Resolved", "Deadlocks Cleared", "Poison Waits Cleared", "Runnable Queue Cleared",
            "Client Timeouts Cleared", "Volume Free Space Resolved",
        })
        {
            Assert.DoesNotMatch(@"(?m)^\s*""" + System.Text.RegularExpressions.Regex.Escape(title) + @""",", source);
        }
    }

    [Fact]
    public async Task AReaderThatThrows_LeavesTheFireUnchanged_AndIsCounted()
    {
        var h = Armed("High CPU");
        h.ReadFailures = new AlertReadFailureCounter();
        h.EscalationSeedReader = (_, _) => throw new InvalidOperationException("history is down");
        var engine = h.Build();

        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0, Level.Warning, 0));
        Assert.False(Last("High CPU", h).Context!.EscalationBypassesCooldown);
        Assert.Equal(AlertSeverityLevel.Warning, Last("High CPU", h).Severity);

        /* The in-process escalation still works: a failed seed read costs the seed and nothing else. */
        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0.AddMinutes(1), Level.Critical, 1));
        Assert.True(Last("High CPU", h).Context!.EscalationBypassesCooldown);

        var reading = h.ReadFailures.ReadFor(AlertEngineTests.Harness.Snapshot().ServerKey);
        Assert.True(reading.ServerReadFailures > 0);
        Assert.Equal("escalation tier seed", reading.LastFailureRead);
    }

    [Fact]
    public async Task TheReaderIsCalledOncePerServerKeyAndFamily_AcrossSeveralSweeps()
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        var calls = new List<(string Key, string Metric)>();
        h.EscalationSeedReader = (q, _) => { calls.Add((q.ServerKey, q.MetricName)); return Task.FromResult<AlertSeverityLevel?>(null); };
        var engine = h.Build();

        var first = AlertEngineTests.Harness.Snapshot();
        var second = first with { ServerKey = "202" };
        for (var i = 0; i < 4; i++)
        {
            h.Now = T0.AddMinutes(i);
            await engine.EvaluateServerAsync(first);
            await engine.EvaluateServerAsync(second);
        }

        Assert.Equal(12, calls.Count);
        Assert.Equal(6, calls.Count(c => c.Key == first.ServerKey));
        Assert.Equal(6, calls.Count(c => c.Key == "202"));
        Assert.Equal(12, calls.Distinct().Count());
    }

    [Fact]
    public async Task AFailedRead_IsRetriedOnTheNextSweep_AndOnlyForTheFamilyThatFailed()
    {
        var h = Armed("High CPU");
        var attempts = new Dictionary<string, int>();
        h.EscalationSeedReader = (q, _) =>
        {
            attempts[q.MetricName] = attempts.GetValueOrDefault(q.MetricName) + 1;
            if (q.MetricName == "High CPU" && attempts[q.MetricName] == 1)
            {
                throw new InvalidOperationException("transient");
            }

            return Task.FromResult<AlertSeverityLevel?>(q.MetricName == "High CPU" ? AlertSeverityLevel.Warning : null);
        };
        var engine = h.Build();

        /* Sweep 1: the CPU read fails and nothing is seeded, so a Critical now is a plain first fire. */
        h.Now = T0;
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot(sqlCpu: 10, totalCpu: 10));
        Assert.Equal(1, attempts["High CPU"]);

        /* Sweep 2: the retry succeeds and seeds the Warning, so the Critical is the escalation. */
        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0.AddMinutes(1), Level.Critical, 1));
        Assert.True(Last("High CPU", h).Context!.EscalationBypassesCooldown);

        /* Sweep 3 onward: read once more nowhere. */
        await StepAsync("High CPU", h, engine, T0.AddMinutes(2), Level.Critical, 2);
        Assert.Equal(2, attempts["High CPU"]);
        Assert.All(attempts.Where(a => a.Key != "High CPU"), a => Assert.Equal(1, a.Value));
    }

    [Fact]
    public async Task TheSeedStopsOnceThisProcessHasRecordedTheTier_SoALateSuccessNeverReachesIntoItsIncident()
    {
        var h = Armed("High CPU");
        var reads = 0;
        h.EscalationSeedReader = (q, _) =>
        {
            if (q.MetricName == "High CPU" && ++reads <= 3)
            {
                throw new InvalidOperationException("transient");
            }

            return Task.FromResult<AlertSeverityLevel?>(q.MetricName == "High CPU" ? AlertSeverityLevel.Warning : null);
        };
        var engine = h.Build();

        /* The first three reads fail (the CPU gate needs three samples to open), so the Warning fires in process. */
        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0, Level.Warning, 0));

        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0.AddMinutes(1), Level.Critical, 1));
        Assert.True(Last("High CPU", h).Context!.EscalationBypassesCooldown);

        /* The process Recorded the Warning, so the fourth read never happens. */
        Assert.Equal(3, reads);
    }

    /// <summary>A store whose only live member is the tier read, answering from rows it holds the way the real stores
    /// do: the shared resolver, the exclusive upper bound, and the host's per-family tierless rule.</summary>
    private sealed class RowsStore : IAlertHistoryStore
    {
        public List<(DateTime At, string Metric, string? Context)> Rows { get; } = new();
        public List<(string Metric, AlertSeverityLevel? Fallback, DateTime? Until)> Asked { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);

        public Task<AlertSeverityLevel?> GetLastFiredTierAsync(
            string serverId, string metricName, string resolutionMetricName, DateTime sinceUtc, bool countMuted = true,
            AlertSeverityLevel? tierWhenRowCarriesNone = null, DateTime? untilUtc = null)
        {
            Asked.Add((metricName, tierWhenRowCarriesNone, untilUtc));
            var newest = Rows
                .Where(r => r.Metric == metricName && r.At >= sinceUtc && (untilUtc is null || r.At < untilUtc))
                .OrderByDescending(r => r.At)
                .Cast<(DateTime At, string Metric, string? Context)?>()
                .FirstOrDefault();
            return Task.FromResult(newest is { } row
                ? AlertHistoryEscalationSeed.Resolve(row.At, row.Context, null, tierWhenRowCarriesNone)
                : null);
        }
    }

    [Fact]
    public async Task OnLite_WithNoResolutionRows_ACriticalFromAClosedIncident_DoesNotCancelTheNextIncidentsEscalation()
    {
        /* Lite writes no resolution rows, so the Critical of an incident that closed stays readable for a whole cooldown.
           Volume Free Space closes only on a transition this process saw, so the seed is what the next incident meets. */
        var h = Armed("Volume Free Space");
        var before = h.Build();
        Assert.Equal(1, await StepAsync("Volume Free Space", h, before, T0, Level.Critical, 0));

        /* Restart a minute later; the disk recovered while the service was down, and the history has no resolution row. */
        h.Now = T0.AddMinutes(1);
        h.EscalationSeedReader = HistoryOf(h);
        var after = h.Build();
        await StepAsync("Volume Free Space", h, after, T0.AddMinutes(1), Level.Clear, 1);

        /* A fresh incident past the cooldown: Warning, then Critical a minute on. */
        Assert.Equal(1, await StepAsync("Volume Free Space", h, after, T0.AddMinutes(7), Level.Warning, 2));
        /* Volume Free Space persists a tier only for a Critical fire: its Warning has none. */
        Assert.NotEqual(AlertSeverityLevel.Critical, Last("Volume Free Space", h).Severity);
        Assert.Equal(1, await StepAsync("Volume Free Space", h, after, T0.AddMinutes(8), Level.Critical, 3));
        Assert.Equal(AlertSeverityLevel.Critical, Last("Volume Free Space", h).Severity);
        Assert.True(Last("Volume Free Space", h).Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task ATierlessRow_SeedsNothing_ExceptVolumeFreeSpacesWhichIsAWarning()
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        var store = new RowsStore();
        foreach (var metric in TierEscalation.OptedInMetrics)
        {
            store.Rows.Add((T0.AddMinutes(-1), metric, null));
        }

        h.EscalationSeedReader = EscalationSeed.ReaderFor(store);
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Equal(6, store.Asked.Count);
        /* The fallback comes from the engine's family table, not from the metric name: "Deadlocks Detected" and
           "Poison Wait" are Critical by name, and a tierless row there used to seed Critical. */
        Assert.All(store.Asked, a => Assert.Equal(a.Metric == "Volume Free Space" ? AlertSeverityLevel.Warning : null, a.Fallback));
        Assert.All(store.Asked, a => Assert.Equal(T0, a.Until));
        Assert.Null(AlertHistoryEscalationSeed.Resolve(T0.AddMinutes(-1), null, null, null));
        Assert.Equal(AlertSeverityLevel.Warning, AlertHistoryEscalationSeed.Resolve(T0.AddMinutes(-1), null, null, AlertSeverityLevel.Warning));
    }

    [Fact]
    public async Task ATierlessVolumeFreeSpaceRow_StillSeedsAWarning_ThroughTheReader()
    {
        var h = Armed("Volume Free Space");
        var store = new RowsStore();
        store.Rows.Add((T0.AddMinutes(-1), "Volume Free Space", null));
        h.EscalationSeedReader = EscalationSeed.ReaderFor(store);
        var engine = h.Build();

        Assert.Equal(1, await StepAsync("Volume Free Space", h, engine, T0.AddMinutes(1), Level.Critical, 0));
        Assert.True(Last("Volume Free Space", h).Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task ARowWrittenAfterTheEngineStarted_IsIgnored()
    {
        var h = Armed("High CPU");
        var store = new RowsStore();
        h.EscalationSeedReader = EscalationSeed.ReaderFor(store);
        var engine = h.Build();

        /* A Warning row at the start instant or later is this process's own, already in memory (or, here, not a row the
           seed may read): the Critical is a plain first fire. */
        store.Rows.Add((T0, "High CPU", "{\"Severity\":\"Warning\"}"));
        store.Rows.Add((T0.AddSeconds(30), "High CPU", "{\"Severity\":\"Warning\"}"));

        Assert.Equal(1, await StepAsync("High CPU", h, engine, T0.AddMinutes(1), Level.Critical, 0));
        Assert.False(Last("High CPU", h).Context!.EscalationBypassesCooldown);

        /* And a Warning row from BEFORE the start is history. */
        var h2 = Armed("High CPU");
        var store2 = new RowsStore();
        store2.Rows.Add((T0.AddSeconds(-1), "High CPU", "{\"Severity\":\"Warning\"}"));
        h2.EscalationSeedReader = EscalationSeed.ReaderFor(store2);
        Assert.Equal(1, await StepAsync("High CPU", h2, h2.Build(), T0.AddMinutes(1), Level.Critical, 0));
        Assert.True(Last("High CPU", h2).Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task ARead_ThatFailsAtFirst_ButSucceedsAfterThisProcessClosedAnIncident_SeedsNothing()
    {
        /* Deadlocks Detected closes its incident on every sweep that has none, so the first healthy sweep after a restart is
           this process finishing one. */
        var h = new AlertEngineTests.Harness { Now = T0 };
        h.Settings.DeadlockEnabled = true;
        h.Settings.DeadlockRateThresholds = new DeadlockRateThresholds(7.0, 31.0);
        var reads = 0;
        h.EscalationSeedReader = (q, _) =>
        {
            if (q.MetricName != "Deadlocks Detected")
            {
                return Task.FromResult<AlertSeverityLevel?>(null);
            }

            return ++reads == 1
                ? throw new InvalidOperationException("history is down")
                : Task.FromResult<AlertSeverityLevel?>(AlertSeverityLevel.Warning);
        };
        var engine = h.Build();

        /* Sweep 1: the read fails and the (empty) incident is closed. Sweep 2: the store is back, and its Warning is from
           before the incident this process just finished. */
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        h.Now = T0.AddMinutes(1);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Equal(1, reads);

        /* A fresh incident that is Critical from its first evaluation is a plain first fire. */
        h.Now = T0.AddMinutes(2);
        for (var i = 0; i < 40; i++)
        {
            h.Adapter.Deadlocks.Add(AlertEngineTests.DeadlockRow());
        }

        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var fire = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Deadlocks Detected");
        Assert.Equal(AlertSeverityLevel.Critical, fire.Severity);
        Assert.False(fire.Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public void TheTierEscalation_RemembersThatTheProcessRecordedOrClosedAKey_WhichHighestCannotSay()
    {
        var escalation = new TierEscalation();
        Assert.False(escalation.IsTouched("1"));

        escalation.Restore("1", AlertSeverityLevel.Warning);
        Assert.False(escalation.IsTouched("1"));

        escalation.Record("2", AlertSeverityLevel.Warning);
        Assert.True(escalation.IsTouched("2"));

        escalation.Close("3");
        Assert.True(escalation.IsTouched("3"));
        Assert.Null(escalation.Highest("3"));
        Assert.False(escalation.IsTouched("4"));
    }

    [Fact]
    public async Task WithTheStoreUnreachable_TheReadsStopAfterOneCooldown()
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        var calls = new List<(string Metric, DateTime At)>();
        h.EscalationSeedReader = (q, _) =>
        {
            calls.Add((q.MetricName, h.Now));
            throw new InvalidOperationException("history is down");
        };
        var engine = h.Build();

        /* A sweep every 30 s for twice the 5-minute cooldown. */
        for (var i = 0; i < 20; i++)
        {
            h.Now = T0.AddSeconds(30 * i);
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        Assert.NotEmpty(calls);
        Assert.All(calls, c => Assert.True(c.At < T0.AddMinutes(h.Settings.CooldownMinutes), "no read once a cooldown has passed since the start"));
        /* Ten sweeps fall inside the cooldown; a family nothing in this process touches is retried on every one of them. */
        Assert.Contains(calls.GroupBy(c => c.Metric), g => g.Count() == 10);
        Assert.All(calls.GroupBy(c => c.Metric), g => Assert.InRange(g.Count(), 1, 10));
    }

    [Fact]
    public void ThePgStoreOverridesTheReader_NotTheInterfaceDefault()
    {
        /* Darling's half; Lite's store is pinned in Lite.Tests (EscalationSeedHistoryStoreTests.TheStoreOverridesTheInterfaceDefault). */
        var type = typeof(PerformanceMonitor.Darling.Storage.PgAlertHistoryStore);
        var map = type.GetInterfaceMap(typeof(IAlertHistoryStore));
        var method = typeof(IAlertHistoryStore).GetMethod(nameof(IAlertHistoryStore.GetLastFiredTierAsync))!;
        var index = Array.IndexOf(map.InterfaceMethods, method);
        Assert.True(index >= 0);
        Assert.Equal(type, map.TargetMethods[index].DeclaringType);
    }

    [Fact]
    public void BothHostsWireTheSeedReaderToTheirHistoryStore_WhereTheEngineIsBuilt()
    {
        /* Lite builds the engine in MainWindow, Darling in DarlingWorker; deprecated/ is not wired and not touched. */
        Assert.Contains(
            "escalationSeedReader: PerformanceMonitor.Alerting.EscalationSeed.ReaderFor(_alertHistoryStore)",
            System.IO.File.ReadAllText(RepoFile("Lite/MainWindow.xaml.cs")), StringComparison.Ordinal);
        Assert.Contains(
            "escalationSeedReader: EscalationSeed.ReaderFor(historyStore)",
            Darling.Tests.RepoFile.ReadRepoFile(RepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs")), StringComparison.Ordinal);
    }

    private static string RepoFile(string relative, [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!);
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return System.IO.Path.Combine(dir!.FullName, relative);
    }
}
