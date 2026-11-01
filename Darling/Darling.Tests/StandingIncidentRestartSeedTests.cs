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
/// #7068: a standing Volume Free Space (or Version Store (PVS)) condition is announced once, not again every time the
/// service starts. The engine's worsening gate, cooldown stamp and tier were all in memory, so every restart read the
/// same volume at the same level as a new incident. A restart here is a second engine built over the same fakes, with
/// the alert history answering from what the first one delivered, the way the store answers from its rows.
/// </summary>
public sealed class StandingIncidentRestartSeedTests
{
    private const string Key = "101";
    private static readonly DateTime T0 = new(2026, 10, 1, 10, 0, 0, DateTimeKind.Utc);

    /// <summary>The alert history as the store answers it: every fire the deliverer got and every resolution the engine
    /// reported, each stamped with the harness clock at the sweep that produced it.</summary>
    private sealed class History
    {
        private readonly AlertEngineTests.Harness _h;
        private readonly List<(DateTime At, AlertOutcome Fire)> _fires = new();
        private readonly List<(DateTime At, string Title, string Message)> _resolutions = new();
        private int _seenFires;
        private int _seenResolutions;

        public History(AlertEngineTests.Harness h) => _h = h;

        public int ReadsAsked { get; private set; }

        public bool Throws { get; set; }

        /// <summary>Stamps what the last sweep produced.</summary>
        public void Sync()
        {
            foreach (var fire in _h.Deliverer.Outcomes.Skip(_seenFires))
            {
                _fires.Add((_h.Now, fire));
            }

            _seenFires = _h.Deliverer.Outcomes.Count;
            foreach (var resolution in _h.Resolutions.Skip(_seenResolutions))
            {
                _resolutions.Add((_h.Now, resolution.Title, resolution.Message));
            }

            _seenResolutions = _h.Resolutions.Count;
        }

        public Func<OpenFireQuery, CancellationToken, Task<AlertHistoryStanding>> Reader => (query, _) =>
        {
            ReadsAsked++;
            if (Throws)
            {
                throw new InvalidOperationException("history is down");
            }

            /* Every fire and every clear the first engine produced, as the stores read them. */
            var fires = Enumerable.Reverse(_fires)
                .Where(f => f.Fire.MetricName == query.MetricName && f.Fire.ServerKey == query.ServerKey && f.At < query.UntilUtc)
                .OrderByDescending(f => f.At)
                .Select(f => new AlertHistoryFire(
                    f.At, f.Fire.NumericCurrentValue ?? 0,
                    f.Fire.Context is null ? null : AlertContextSerializer.Serialize(f.Fire.Context)))
                .ToList();
            var clears = _resolutions
                .Where(r => query.ResolutionTitles.Contains(r.Title))
                .Select(r => new AlertHistoryClear(r.At, null, r.Message))
                .ToList();
            return Task.FromResult(new AlertHistoryStanding(fires, clears));
        };
    }

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness { Now = T0 };
        h.Settings.LowDiskEnabled = true;
        h.Settings.PvsEnabled = true;
        return h;
    }

    /// <summary>A new engine over the same fakes, one day later by default: the service restarted. With a reader it is the
    /// engine after #7068, without one it is the engine before.</summary>
    private static AlertEngine Restart(AlertEngineTests.Harness h, History history, bool seeded, TimeSpan? after = null)
    {
        h.Now += after ?? TimeSpan.FromDays(3);
        h.OpenFireReader = seeded ? history.Reader : null;
        return h.Build();
    }

    private static async Task SweepAsync(AlertEngineTests.Harness h, AlertEngine engine, History history, params (string Mount, double FreeMb)[] volumes)
    {
        h.Adapter.Volumes.Clear();
        foreach (var (mount, freeMb) in volumes)
        {
            h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = mount, TotalMb = 102400, FreeMb = freeMb });
        }

        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        history.Sync();
    }

    private static int Fires(AlertEngineTests.Harness h, string metric) => h.Deliverer.Outcomes.Count(o => o.MetricName == metric);

    private const double Critical = 1000;   /* about 1% of a 100 GB volume */
    private const double Warning = 8192;    /* 8% */

    [Theory]
    [InlineData(Critical)]
    [InlineData(Warning)]
    public async Task AStandingVolumeCondition_AtTheSameLevel_AfterARestart_FiresNothing(double freeMb)
    {
        var h = Armed();
        var history = new History(h);
        await SweepAsync(h, h.Build(), history, ("D:\\", freeMb));
        Assert.Equal(1, Fires(h, "Volume Free Space"));

        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history, ("D:\\", freeMb));
        await SweepAsync(h, engine, history, ("D:\\", freeMb));

        Assert.Equal(1, Fires(h, "Volume Free Space"));
    }

    [Fact]
    public async Task WithNoHistoryReader_TheSameRestart_AnnouncesTheStandingVolumeAgain()
    {
        /* The control: this is the defect, and why the tests above and below mean something. */
        var h = Armed();
        var history = new History(h);
        await SweepAsync(h, h.Build(), history, ("D:\\", Critical));

        await SweepAsync(h, Restart(h, history, seeded: false), history, ("D:\\", Critical));

        Assert.Equal(2, Fires(h, "Volume Free Space"));
    }

    [Fact]
    public async Task AWarningThatBecomesCriticalAfterTheRestart_FiresAtOnce_AsAnEscalation()
    {
        var h = Armed();
        var history = new History(h);
        await SweepAsync(h, h.Build(), history, ("D:\\", Warning));

        var engine = Restart(h, history, seeded: true, after: TimeSpan.FromMinutes(5));
        await SweepAsync(h, engine, history, ("D:\\", Critical));

        Assert.Equal(2, Fires(h, "Volume Free Space"));
        var second = h.Deliverer.Outcomes.Last(o => o.MetricName == "Volume Free Space");
        Assert.Equal(AlertSeverityLevel.Critical, second.Severity);
        Assert.True(second.Context!.EscalationBypassesCooldown);
    }

    [Fact]
    public async Task AWorseningLevelAfterTheRestart_FiresAgain()
    {
        var h = Armed();
        var history = new History(h);
        await SweepAsync(h, h.Build(), history, ("D:\\", 9000));

        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history, ("D:\\", 6000));

        Assert.Equal(2, Fires(h, "Volume Free Space"));
    }

    [Fact]
    public async Task AConditionThatClearedBeforeTheRestart_AndCameBackAfterIt_Fires()
    {
        var h = Armed();
        var history = new History(h);
        var first = h.Build();
        await SweepAsync(h, first, history, ("D:\\", Critical));
        h.Now += TimeSpan.FromMinutes(10);
        await SweepAsync(h, first, history, ("D:\\", 50000));   /* cleared: the engine writes the resolution */
        Assert.Contains(h.Resolutions, r => r.Title == EscalationSeed.VolumeFreeSpaceResolvedTitle);

        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history, ("D:\\", Critical));

        Assert.Equal(2, Fires(h, "Volume Free Space"));
    }

    [Fact]
    public async Task AConditionThatClearedWhileTheServiceWasDown_AndCameBackLater_Fires()
    {
        /* No resolution row exists (nothing was running to write it), but this process saw the volumes healthy first. */
        var h = Armed();
        var history = new History(h);
        await SweepAsync(h, h.Build(), history, ("D:\\", Critical));

        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history, ("D:\\", 50000));
        h.Now += TimeSpan.FromHours(2);
        await SweepAsync(h, engine, history, ("D:\\", Critical));

        Assert.Equal(2, Fires(h, "Volume Free Space"));
    }

    [Fact]
    public async Task ADifferentVolumeOnTheSameServer_AfterTheRestart_Fires()
    {
        var h = Armed();
        var history = new History(h);
        await SweepAsync(h, h.Build(), history, ("D:\\", Critical));

        var engine = Restart(h, history, seeded: true);
        await SweepAsync(h, engine, history, ("E:\\", Critical));

        Assert.Equal(2, Fires(h, "Volume Free Space"));
        Assert.StartsWith("E:", h.Deliverer.Outcomes.Last(o => o.MetricName == "Volume Free Space").CurrentValue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHistoryRead_ThatFails_FallsBackToAnnouncing_AndIsReadOnceNotEverySweep()
    {
        var h = Armed();
        var history = new History(h);
        await SweepAsync(h, h.Build(), history, ("D:\\", Critical));

        history.Throws = true;
        var engine = Restart(h, history, seeded: true);
        var asked = history.ReadsAsked;
        await SweepAsync(h, engine, history, ("D:\\", Critical));
        await SweepAsync(h, engine, history, ("D:\\", Critical));

        Assert.Equal(2, Fires(h, "Volume Free Space"));
        Assert.Equal(asked + 1, history.ReadsAsked);
    }

    [Fact]
    public async Task AStandingPvsCondition_AfterARestart_FiresNothing_ButANewDatabaseOrAClearedOneDoes()
    {
        var h = Armed();
        var history = new History(h);
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });
        var first = h.Build();
        await first.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        history.Sync();
        Assert.Equal(1, Fires(h, "Version Store (PVS)"));

        /* Standing: no new fire. */
        var engine = Restart(h, history, seeded: true);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Equal(1, Fires(h, "Version Store (PVS)"));

        /* A different database is a different incident. */
        h.Adapter.PvsDatabases.Clear();
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "ledger", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });
        engine = Restart(h, history, seeded: true);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        history.Sync();
        Assert.Equal(2, Fires(h, "Version Store (PVS)"));

        /* Worse after a restart. */
        h.Adapter.PvsDatabases[0].PvsSizeMb = 9216;
        engine = Restart(h, history, seeded: true);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        history.Sync();
        Assert.Equal(3, Fires(h, "Version Store (PVS)"));

        /* Cleared (a resolution row), then back after a restart. */
        h.Adapter.PvsDatabases.Clear();
        h.Now += TimeSpan.FromMinutes(5);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        history.Sync();
        Assert.Contains(h.Resolutions, r => r.Title == EscalationSeed.PvsResolvedTitle);
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "ledger", PvsSizeMb = 9216, DatabaseDataSizeMb = 10240 });
        engine = Restart(h, history, seeded: true);
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Equal(4, Fires(h, "Version Store (PVS)"));
    }
}

/// <summary>#7068: the same restart for the Capture Down self-alert, whose "already announced" stamp was in memory too.</summary>
public sealed class StandingCaptureDownRestartSeedTests
{
    private const int ServerId = 424242;
    private const string Name = "CAPTURE-SRV";
    private static readonly DateTime Start = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Rig
    {
        public Rig()
        {
            Evaluator = new PerformanceMonitor.Darling.Service.DarlingSelfAlertEvaluator(
                Settings, Deliverer, History, _ => false,
                logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => Now);
        }

        public DarlingSelfAlertTests.FakeHistoryStore History { get; } = new();
        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new() { CooldownMinutes = 5 };
        private AlertEngineTests.RecordingDeliverer Deliverer { get; } = new();
        public DateTime Now { get; set; } = Start;
        public PerformanceMonitor.Darling.Service.DarlingSelfAlertEvaluator Evaluator { get; }
        public int Fires => Deliverer.Outcomes.Count;

        public Task DownAsync() => Evaluator.ApplyCaptureDownAsync(ServerId, Name, new[] { "Blocking" }, Array.Empty<string>(), Ct);

        public Task UpAsync() => Evaluator.ApplyCaptureDownAsync(ServerId, Name, Array.Empty<string>(), Array.Empty<string>(), Ct);
    }

    [Fact]
    public async Task ACaptureStillDown_AfterARestart_WithAnOpenFireInTheHistory_IsNotAnnouncedAgain()
    {
        var rig = new Rig { Now = Start };
        rig.History.OpenFire = new AlertHistoryFire(Start.AddDays(-2), 0, null);

        await rig.DownAsync();
        await rig.DownAsync();

        Assert.Equal(0, rig.Fires);
        Assert.Contains("Capture Down", rig.History.OpenFireAsked);
    }

    [Fact]
    public async Task ACaptureDown_WithNoOpenFireInTheHistory_IsAnnounced_OnceAcrossSweeps()
    {
        var rig = new Rig();

        await rig.DownAsync();
        await rig.DownAsync();

        Assert.Equal(1, rig.Fires);
    }

    [Fact]
    public async Task ACaptureSeenRunningFirst_AndDownLater_IsAnnounced_WhateverTheHistoryHolds()
    {
        /* An open row nobody resolved (the service was down when the capture came back) is not this condition's. */
        var rig = new Rig();
        rig.History.OpenFire = new AlertHistoryFire(Start.AddDays(-2), 0, null);

        await rig.UpAsync();
        rig.Now += TimeSpan.FromHours(1);
        await rig.DownAsync();

        Assert.Equal(1, rig.Fires);
    }
}

