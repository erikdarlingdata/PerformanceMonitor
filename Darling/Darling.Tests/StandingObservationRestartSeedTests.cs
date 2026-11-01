// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7068: Force Plan Failures and Database File Growth remember, per plan or file, the observation they last fired on, and
/// fire again only for a newer one. That memory was in the process, so every restart read a standing plan or file as news.
/// A restart here is a second engine over the same fakes, with the alert history answering from what the first one
/// delivered, the way the store answers from its rows.
/// </summary>
public sealed class StandingObservationRestartSeedTests
{
    private const string Key = "101";
    private static readonly DateTime T0 = new(2026, 10, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime SeenAt = T0.AddMinutes(-3);

    private sealed class History
    {
        private readonly AlertEngineTests.Harness _h;
        private readonly List<(DateTime At, AlertOutcome Fire)> _fires = new();
        private readonly List<(DateTime At, string Title)> _resolutions = new();
        private int _seenFires;
        private int _seenResolutions;

        public History(AlertEngineTests.Harness h) => _h = h;

        public int ReadsAsked { get; private set; }

        public bool Throws { get; set; }

        public void Sync()
        {
            foreach (var fire in _h.Deliverer.Outcomes.Skip(_seenFires))
            {
                _fires.Add((_h.Now, fire));
            }

            _seenFires = _h.Deliverer.Outcomes.Count;
            foreach (var resolution in _h.Resolutions.Skip(_seenResolutions))
            {
                _resolutions.Add((_h.Now, resolution.Title));
            }

            _seenResolutions = _h.Resolutions.Count;
        }

        public Func<OpenFireQuery, CancellationToken, Task<AlertHistoryFire?>> Reader => (query, _) =>
        {
            ReadsAsked++;
            if (Throws)
            {
                throw new InvalidOperationException("history is down");
            }

            var fire = _fires.LastOrDefault(f => f.Fire.MetricName == query.MetricName && f.Fire.ServerKey == query.ServerKey && f.At < query.UntilUtc);
            if (fire.Fire is null || _resolutions.Any(r => query.ResolutionTitles.Contains(r.Title) && r.At >= fire.At))
            {
                return Task.FromResult<AlertHistoryFire?>(null);
            }

            return Task.FromResult<AlertHistoryFire?>(new AlertHistoryFire(
                fire.At, fire.Fire.NumericCurrentValue ?? 0,
                fire.Fire.Context is null ? null : AlertContextSerializer.Serialize(fire.Fire.Context)));
        };
    }

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        h.Settings.ForcePlanFailureEnabled = true;
        h.Settings.FileGrowthEnabled = true;
        return h;
    }

    private static AlertEngine Restart(AlertEngineTests.Harness h, History history, bool seeded)
    {
        h.Now += TimeSpan.FromDays(3);
        h.OpenFireReader = seeded ? history.Reader : null;
        return h.Build();
    }

    private static async Task SweepAsync(AlertEngineTests.Harness h, AlertEngine engine, History history)
    {
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        history.Sync();
        h.Now += TimeSpan.FromMinutes(1);
    }

    private static int Fires(AlertEngineTests.Harness h, string metric) => h.Deliverer.Outcomes.Count(o => o.MetricName == metric);

    private static void SetPlans(AlertEngineTests.Harness h, DateTime observedAt, params int[] planIds)
    {
        h.Adapter.ForcePlanFailures.Clear();
        foreach (var planId in planIds)
        {
            h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo
            {
                DatabaseName = "Sales", QueryId = 11, PlanId = planId, ForcingType = "MANUAL", FailureReason = "NO_INDEX",
                FailureDelta = 3, TotalFailures = 9, ObservedAtUtc = observedAt
            });
        }
    }

    private static void SetFiles(AlertEngineTests.Harness h, DateTime observedAt, params string[] fileNames)
    {
        h.Adapter.Files.Clear();
        foreach (var fileName in fileNames)
        {
            h.Adapter.Files.Add(new DatabaseFileGrowthInfo
            {
                DatabaseName = "tempdb", FileName = fileName, PhysicalName = @"D:\data\" + fileName + ".mdf", FileTypeDesc = "ROWS",
                TotalSizeMb = 90_000, GrowthMb = 20_480, GrowthWindowMinutes = 60,
                VolumeMountPoint = @"D:\", VolumeTotalMb = 4_000_000, VolumeFreeMb = 3_000_000, ObservedAtUtc = observedAt
            });
        }
    }

    private const string Growth = "Database File Growth";

    public enum Alert
    {
        ForcePlan,
        FileGrowth,
    }

    private static string MetricOf(Alert a) => a == Alert.ForcePlan ? ForcePlanTokens.MetricName : Growth;

    private static void Set(Alert a, AlertEngineTests.Harness h, DateTime observedAt, string name = "a")
    {
        if (a == Alert.ForcePlan)
        {
            SetPlans(h, observedAt, name == "a" ? 22 : 23);
        }
        else
        {
            SetFiles(h, observedAt, name == "a" ? "tempdev" : "tempdev2");
        }
    }

    [Theory]
    [InlineData(Alert.ForcePlan)]
    [InlineData(Alert.FileGrowth)]
    public async Task AStandingObservation_AfterARestart_FiresNothing(Alert alert)
    {
        var h = Armed();
        var history = new History(h);
        Set(alert, h, SeenAt);
        await SweepAsync(h, h.Build(), history);
        Assert.Equal(1, Fires(h, MetricOf(alert)));

        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history);
        await SweepAsync(h, engine, history);

        Assert.Equal(1, Fires(h, MetricOf(alert)));
    }

    [Theory]
    [InlineData(Alert.ForcePlan)]
    [InlineData(Alert.FileGrowth)]
    public async Task WithNoHistoryReader_TheSameRestart_AnnouncesTheStandingObservationAgain(Alert alert)
    {
        var h = Armed();
        var history = new History(h);
        Set(alert, h, SeenAt);
        await SweepAsync(h, h.Build(), history);

        var engine = Restart(h, history, seeded: false);
        await SweepAsync(h, engine, history);

        Assert.Equal(2, Fires(h, MetricOf(alert)));
    }

    [Theory]
    [InlineData(Alert.ForcePlan)]
    [InlineData(Alert.FileGrowth)]
    public async Task ANewerObservation_AfterARestart_FiresAsNews(Alert alert)
    {
        var h = Armed();
        var history = new History(h);
        Set(alert, h, SeenAt);
        await SweepAsync(h, h.Build(), history);

        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history);
        Set(alert, h, h.Now);
        await SweepAsync(h, engine, history);

        Assert.Equal(2, Fires(h, MetricOf(alert)));
    }

    [Theory]
    [InlineData(Alert.ForcePlan)]
    [InlineData(Alert.FileGrowth)]
    public async Task AConditionThatClearedBeforeTheRestart_AndCameBack_FiresAgain(Alert alert)
    {
        var h = Armed();
        var history = new History(h);
        var first = h.Build();
        Set(alert, h, SeenAt);
        await SweepAsync(h, first, history);
        h.Adapter.ForcePlanFailures.Clear();
        h.Adapter.Files.Clear();
        await SweepAsync(h, first, history);
        Assert.NotEmpty(h.Resolutions);

        var engine = Restart(h, history, seeded: true);
        Set(alert, h, h.Now);
        await SweepAsync(h, engine, history);

        Assert.Equal(2, Fires(h, MetricOf(alert)));
    }

    [Theory]
    [InlineData(Alert.ForcePlan)]
    [InlineData(Alert.FileGrowth)]
    public async Task AConditionThatClearedWhileTheServiceWasDown_AndCameBack_FiresAgain(Alert alert)
    {
        var h = Armed();
        var history = new History(h);
        Set(alert, h, SeenAt);
        await SweepAsync(h, h.Build(), history);

        /* No resolution row: the service was not running when it cleared. The new process sees it healthy first. */
        var engine = Restart(h, history, seeded: true);
        h.Adapter.ForcePlanFailures.Clear();
        h.Adapter.Files.Clear();
        await SweepAsync(h, engine, history);
        Set(alert, h, h.Now);
        await SweepAsync(h, engine, history);

        Assert.Equal(2, Fires(h, MetricOf(alert)));
    }

    [Theory]
    [InlineData(Alert.ForcePlan)]
    [InlineData(Alert.FileGrowth)]
    public async Task ADifferentPlanOrFile_AfterARestart_FiresAsNews(Alert alert)
    {
        var h = Armed();
        var history = new History(h);
        Set(alert, h, SeenAt);
        await SweepAsync(h, h.Build(), history);

        var engine = Restart(h, history, seeded: true);
        Set(alert, h, SeenAt, name: "b");
        await SweepAsync(h, engine, history);

        Assert.Equal(2, Fires(h, MetricOf(alert)));
    }

    [Theory]
    [InlineData(Alert.ForcePlan)]
    [InlineData(Alert.FileGrowth)]
    public async Task AFailedHistoryRead_FallsBackToFiring_AndIsReadOnce(Alert alert)
    {
        var h = Armed();
        var history = new History(h);
        Set(alert, h, SeenAt);
        await SweepAsync(h, h.Build(), history);

        history.Throws = true;
        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history);
        await SweepAsync(h, engine, history);

        Assert.Equal(2, Fires(h, MetricOf(alert)));
        Assert.Equal(1, history.ReadsAsked);
    }
}
