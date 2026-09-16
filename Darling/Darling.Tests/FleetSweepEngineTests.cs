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
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet sweep engine's compose path (#3466 lane 2), driven pure: <c>FleetSweepEngine.Compose</c>
/// takes the clock as an argument and every store read as a fixture, so these tests hold the sweep's
/// decisions — verdicts off the shared scorer, the diff against the previous sweep's rows, watch-item
/// transitions through the REAL state machine, the liveness judgments, the would-have-paged
/// derivation, and the settle-window suppressions — without a store, a worker, or a wall clock.
///
/// <para>The live half (the writer's transaction, the probe) is <c>FleetSweepStoreLivePostgresTests</c>'
/// job; the knob plumbing and the worker wiring pins are <c>FleetSweepCadenceKnobRungTests</c>'.</para>
/// </summary>
public sealed class FleetSweepEngineTests
{
    /* A fixed clock, and a service start comfortably OUTSIDE the settle window — every test that wants
       the window states its own start instant instead. */
    private static readonly DateTime Now = new(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime SpanStart = Now.AddHours(-1);
    private static readonly DateTime StartedLongAgo = Now.AddHours(-6);

    /* Later sweeps in a multi-sweep fixture must ADVANCE the pass counter — the engine judges a counter
       frozen across two alerts-on sweeps as dead alert instruments (the frozen-counter test proves
       exactly that), so a fixture reusing one figure would trip the very check it exists to hold. */
    private static FleetSweepInstrumentCounters SteadyInstruments(long passes = 500) =>
        new(StartedLongAgo, passes, AlertReadFailuresTotal: 0);

    private static FleetSweepServerReading Healthy(int id, string name) =>
        new(id, name, new DailyHealthSignals { HasData = true }, 0, null);

    private static FleetSweepServerReading CriticalDeadlocks(int id, string name, long deadlocks = 3) =>
        new(id, name, new DailyHealthSignals { HasData = true, Deadlocks = deadlocks }, 0, null);

    private static FleetSweepServerReading NoData(int id, string name) =>
        new(id, name, default, 0, null);

    private static FleetSweepComposition ComposeSimple(
        IReadOnlyList<FleetSweepServerReading> readings,
        bool alertsEnabled = true,
        FleetSweepRun? previousRun = null,
        IReadOnlyList<FleetSweepServerVerdict>? previousVerdicts = null,
        IReadOnlyList<FleetSweepWatchItem>? activeItems = null,
        FleetSweepInstrumentCounters? instruments = null,
        DateTime? now = null)
    {
        return FleetSweepEngine.Compose(
            now ?? Now,
            (now ?? Now).AddHours(-1),
            alertsEnabled,
            serversExpected: readings.Count,
            readings,
            previousRun,
            previousVerdicts ?? Array.Empty<FleetSweepServerVerdict>(),
            activeItems ?? Array.Empty<FleetSweepWatchItem>(),
            instruments ?? SteadyInstruments());
    }

    /* ─────────────────────── verdicts: the shared scorer, unforked ─────────────────────── */

    /// <summary>
    /// The verdicts ARE <see cref="DailyHealthBandCalculator"/>'s answers: the same signals fed to the
    /// shared scorer directly must produce the same band the sweep stored, and a non-Healthy card
    /// carries the scorer's own reason wording. Asserted against the calculator rather than against
    /// literals, so a threshold move cannot make the sweep and the day surfaces disagree — reusing the
    /// scorer is the lane's contract, and this is the pin that holds it.
    /// </summary>
    [Fact]
    public void Verdicts_ComeFromTheSharedScorer_WithItsOwnReasons()
    {
        var critical = CriticalDeadlocks(1, "server-a");
        var healthy = Healthy(2, "server-b");
        var composition = ComposeSimple(new[] { critical, healthy });

        var verdictA = composition.Verdicts.Single(v => v.ServerId == 1);
        var verdictB = composition.Verdicts.Single(v => v.ServerId == 2);

        Assert.Equal(
            DailyHealthBandCalculator.Label(DailyHealthBandCalculator.Classify(critical.Signals)),
            verdictA.Band);
        Assert.Equal("Critical", verdictA.Band);
        Assert.Equal(
            string.Join("; ", DailyHealthBandCalculator.BuildReasons(critical.Signals, 0)),
            verdictA.BandReason);

        /* Healthy needs no defending — the reason column's null contract. */
        Assert.Equal("Healthy", verdictB.Band);
        Assert.Null(verdictB.BandReason);

        /* Verdict beside its inputs: the evidence payload carries the signals, so a reader can
           disagree with the band rather than believe it. */
        Assert.NotNull(verdictA.VerdictJson);
        Assert.Contains("\"deadlocks\":3", verdictA.VerdictJson, StringComparison.Ordinal);
    }

    /* ─────────────────────── the diff: sweep N against sweep N−1's rows ─────────────────────── */

    /// <summary>
    /// Sweep 2's verdicts join sweep 1's by server: the previous band rides the row, a changed band is
    /// a transition in the document, and the run row names its diff anchor. Two composes, the second
    /// fed the first's outputs — the compare_analysis leg against sweep state.
    /// </summary>
    [Fact]
    public void TheSecondSweep_DiffsAgainstTheFirstsRows_AndRecordsTheTransition()
    {
        var first = ComposeSimple(new[] { Healthy(1, "server-a"), Healthy(2, "server-b") });
        Assert.Null(first.Run.PreviousSweepId);
        AssertReportFlag(first.Run.ReportJson, "no_previous_sweep", expected: true);
        Assert.All(first.Verdicts, v => Assert.Null(v.PreviousBand));

        var later = Now.AddHours(1);
        var second = ComposeSimple(
            new[] { CriticalDeadlocks(1, "server-a"), Healthy(2, "server-b") },
            previousRun: first.Run,
            previousVerdicts: first.Verdicts,
            instruments: SteadyInstruments(passes: 510),
            now: later);

        Assert.Equal(first.Run.SweepId, second.Run.PreviousSweepId);
        AssertReportFlag(second.Run.ReportJson, "no_previous_sweep", expected: false);

        var changed = second.Verdicts.Single(v => v.ServerId == 1);
        Assert.Equal("Healthy", changed.PreviousBand);
        Assert.Equal("Critical", changed.Band);

        var steady = second.Verdicts.Single(v => v.ServerId == 2);
        Assert.Equal("Healthy", steady.PreviousBand);

        using var report = JsonDocument.Parse(second.Run.ReportJson);
        var transitions = report.RootElement.GetProperty("changes").GetProperty("band_transitions")
            .EnumerateArray().ToList();
        var transition = Assert.Single(transitions);
        Assert.Equal("server-a", transition.GetProperty("server").GetString());
        Assert.Equal("Healthy", transition.GetProperty("from").GetString());
        Assert.Equal("Critical", transition.GetProperty("to").GetString());
    }

    /* ─────────────────────── watch items: the real state machine, full lifecycle ─────────────────────── */

    /// <summary>
    /// A four-sweep episode through the REAL state machine, each sweep fed the previous one's item
    /// images: first Critical sighting is pending, the second consecutive opens (with the opening
    /// sweep's id stamped), the first quiet sweep carries, the second closes (closing id stamped).
    /// This is the hysteresis contract end to end AT THE ENGINE — the machine's own boundaries are
    /// <c>FleetSweepStateRungTests</c>' pins; what this holds is that the engine feeds it honestly.
    /// </summary>
    [Fact]
    public void AWatchItem_LivesTheFullLifecycle_ThroughTheRealStateMachine()
    {
        /* Sweep 1: first Critical sighting → pending, not opened. */
        var sweep1 = ComposeSimple(new[] { CriticalDeadlocks(1, "server-a") });
        var item1 = Assert.Single(sweep1.WatchItems);
        Assert.Equal(FleetSweepEngine.BandCriticalItemKey, item1.ItemKey);
        Assert.Equal(FleetSweepWatchStateMachine.Pending, item1.State);
        Assert.Equal(1, item1.ConsecutiveHits);
        Assert.Null(item1.OpenedSweepId);
        Assert.NotNull(item1.EvidenceJson);

        /* Sweep 2: second consecutive Critical → open, stamped with THIS sweep's id. */
        var t2 = Now.AddHours(1);
        var sweep2 = ComposeSimple(
            new[] { CriticalDeadlocks(1, "server-a") },
            previousRun: sweep1.Run, previousVerdicts: sweep1.Verdicts,
            activeItems: sweep1.WatchItems, instruments: SteadyInstruments(passes: 510), now: t2);
        var item2 = Assert.Single(sweep2.WatchItems);
        Assert.Equal(FleetSweepWatchStateMachine.Open, item2.State);
        Assert.Equal(2, item2.ConsecutiveHits);
        Assert.Equal(sweep2.Run.SweepId, item2.OpenedSweepId);
        Assert.Equal(item1.FirstSeenSweepId, item2.FirstSeenSweepId);

        /* Sweep 3: quiet → carried with one standing miss; the opened stamp survives. */
        var t3 = Now.AddHours(2);
        var sweep3 = ComposeSimple(
            new[] { Healthy(1, "server-a") },
            previousRun: sweep2.Run, previousVerdicts: sweep2.Verdicts,
            activeItems: sweep2.WatchItems, instruments: SteadyInstruments(passes: 520), now: t3);
        var item3 = Assert.Single(sweep3.WatchItems);
        Assert.Equal(FleetSweepWatchStateMachine.Carried, item3.State);
        Assert.Equal(1, item3.ConsecutiveMisses);
        Assert.Equal(sweep2.Run.SweepId, item3.OpenedSweepId);
        Assert.Null(item3.ClosedSweepId);

        /* A miss carries NO fresh evidence — null, so the store's COALESCE keeps the standing
           evidence instead of overwriting what opened the item with a miss's nothing. */
        Assert.Null(item3.EvidenceJson);

        /* Sweep 4: second consecutive quiet → closed, stamped with THIS sweep's id. */
        var t4 = Now.AddHours(3);
        var sweep4 = ComposeSimple(
            new[] { Healthy(1, "server-a") },
            previousRun: sweep3.Run, previousVerdicts: sweep3.Verdicts,
            activeItems: sweep3.WatchItems, instruments: SteadyInstruments(passes: 530), now: t4);
        var item4 = Assert.Single(sweep4.WatchItems);
        Assert.Equal(FleetSweepWatchStateMachine.Closed, item4.State);
        Assert.Equal(sweep4.Run.SweepId, item4.ClosedSweepId);
    }

    /// <summary>An active item whose server was not sighted this sweep still advances — a miss is an
    /// evaluation, not an absence. The engine must feed every active item to the machine, or an open
    /// episode on a recovered server would never close.</summary>
    [Fact]
    public void AnActiveItem_NotSightedThisSweep_TakesAMiss()
    {
        var standing = new FleetSweepWatchItem(
            1, FleetSweepEngine.BandCriticalItemKey, "condition", FleetSweepWatchStateMachine.Open,
            2, 0, 100, 100, 100, null, SpanStart, SpanStart, "{}");

        var composition = ComposeSimple(new[] { Healthy(1, "server-a") }, activeItems: new[] { standing });

        var advanced = Assert.Single(composition.WatchItems);
        Assert.Equal(FleetSweepWatchStateMachine.Carried, advanced.State);
        Assert.Equal(1, advanced.ConsecutiveMisses);
    }

    /* ─────────────────────── liveness: quiet is not clean ─────────────────────── */

    /// <summary>
    /// A fleet-wide quiet span outside the settle window is DEAD INSTRUMENTS, not a healthy hour:
    /// <c>instruments_alive</c> goes false, the liveness block says which instrument (the collection
    /// rows), and the fleet-scope watch item opens its count. The spec's acceptance line — "with a
    /// data source down, the sweep reports the source down loudly rather than reporting quiet".
    /// </summary>
    [Fact]
    public void FleetWideQuiet_OutsideTheSettleWindow_IsDeadInstruments_NotAHealthySweep()
    {
        var composition = ComposeSimple(new[] { NoData(1, "server-a"), NoData(2, "server-b") });

        Assert.False(composition.Run.InstrumentsAlive);
        Assert.Contains("No server reported any collection", composition.Run.InstrumentLivenessJson, StringComparison.Ordinal);

        var fleetItem = composition.WatchItems.Single(w => w.ServerId == FleetSweepStore.FleetScopeServerId);
        Assert.Equal(FleetSweepEngine.InstrumentsDeadItemKey, fleetItem.ItemKey);
        Assert.Equal(FleetSweepWatchStateMachine.Pending, fleetItem.State);

        /* And each quiet server carries its own staleness item — the standing per-member condition. */
        Assert.Equal(2, composition.WatchItems.Count(w => w.ItemKey == FleetSweepEngine.CollectionStaleItemKey));
    }

    /// <summary>A per-server read fault is UNREADABLE, not quiet: the verdict says so, liveness goes
    /// red naming the count, and the server opens no staleness item (the sweep cannot know).</summary>
    [Fact]
    public void APerServerReadFault_IsUnreadableNotQuiet()
    {
        var faulted = new FleetSweepServerReading(1, "server-a", default, 0, "boom: socket reset");
        var composition = ComposeSimple(new[] { faulted, Healthy(2, "server-b") });

        Assert.False(composition.Run.InstrumentsAlive);
        Assert.Contains("Unreadable is not quiet", composition.Verdicts.Single(v => v.ServerId == 1).BandReason, StringComparison.Ordinal);
        Assert.Contains("boom: socket reset", composition.Run.InstrumentLivenessJson, StringComparison.Ordinal);

        /* No staleness item for a server the sweep could not read — a fault is not evidence of
           staleness, and fabricating a hit would open items off the sweep's own failure. */
        Assert.DoesNotContain(composition.WatchItems, w =>
            w.ServerId == 1 && w.ItemKey == FleetSweepEngine.CollectionStaleItemKey);
    }

    /// <summary>
    /// The frozen-counter reading, judged only when judgeable: a pass counter that did not move across
    /// two alerts-on sweeps with servers reporting is dead alert-path instruments (the #3464 shape);
    /// the SAME frozen counter under master-off is the gate doing its job and is stated, not alarmed
    /// on; and a restart since the previous sweep resets the baseline, so the comparison is declared
    /// unjudgeable rather than fabricated.
    /// </summary>
    [Fact]
    public void AFrozenPassCounter_FailsLiveness_OnlyWhenTheComparisonIsJudgeable()
    {
        FleetSweepRun PreviousRun(bool alertsEnabled) => new(
            100, Now.AddHours(-1), Now.AddHours(-2), Now.AddHours(-1), null, alertsEnabled, 1, 1, true,
            JsonSerializer.Serialize(new { alert_passes_total = 500 }), "{}");

        /* Judgeable and frozen: alerts on both sweeps, a server reporting, no restart → dead. */
        var frozen = ComposeSimple(
            new[] { Healthy(1, "server-a") },
            previousRun: PreviousRun(alertsEnabled: true),
            instruments: SteadyInstruments(passes: 500));
        Assert.False(frozen.Run.InstrumentsAlive);
        Assert.Contains("frozen", frozen.Run.InstrumentLivenessJson, StringComparison.Ordinal);

        /* Advancing: one more pass than the recorded baseline → alive. */
        var advancing = ComposeSimple(
            new[] { Healthy(1, "server-a") },
            previousRun: PreviousRun(alertsEnabled: true),
            instruments: SteadyInstruments(passes: 501));
        Assert.True(advancing.Run.InstrumentsAlive);

        /* Master-off freezes the counter BY DESIGN (#3464 stops the pass before RecordPass): stated in
           the block, and not a liveness failure. */
        var muted = ComposeSimple(
            new[] { Healthy(1, "server-a") },
            alertsEnabled: false,
            previousRun: PreviousRun(alertsEnabled: false),
            instruments: SteadyInstruments(passes: 500));
        Assert.True(muted.Run.InstrumentsAlive);
        Assert.Contains("expected under the alert master switch", muted.Run.InstrumentLivenessJson, StringComparison.Ordinal);

        /* A restart since the previous sweep resets the in-memory counter: baseline declared reset,
           comparison declared unjudgeable, liveness not failed on it. */
        var restarted = ComposeSimple(
            new[] { Healthy(1, "server-a") },
            previousRun: PreviousRun(alertsEnabled: true),
            instruments: new FleetSweepInstrumentCounters(Now.AddMinutes(-30), 5, 0));
        Assert.True(restarted.Run.InstrumentsAlive);
        Assert.Contains("baseline_reset\":true", restarted.Run.InstrumentLivenessJson, StringComparison.Ordinal);
    }

    /* ─────────────────────── master-off: the sweep produces, and delivers nothing ─────────────────────── */

    /// <summary>
    /// THE muted-mode pin: under <c>alerts_enabled: false</c> the sweep still composes everything —
    /// run, verdicts, watch items — the run row's header states the mute, and the would-have-paged
    /// ledger carries each Critical verdict's triggers with evidence. Under alerts-on the ledger is
    /// EMPTY: it is the mute's audit trail, not a second alert history.
    /// </summary>
    [Fact]
    public void UnderMasterOff_TheSweepProducesRows_AndTheLedgerCarriesTheWouldHavePaged()
    {
        var readings = new[]
        {
            new FleetSweepServerReading(1, "server-a", new DailyHealthSignals
            {
                HasData = true,
                Deadlocks = 2,
                HighCpuEvents = 10,
                BlockingEvents = 20,
            }, 1500, null),
            Healthy(2, "server-b"),
        };

        var muted = ComposeSimple(readings, alertsEnabled: false);

        Assert.False(muted.Run.AlertsEnabled);
        Assert.Equal(2, muted.Verdicts.Count);
        Assert.NotEmpty(muted.WatchItems);

        /* One row per (server, family): the deadlocks, the sustained CPU and the heavy blocking each
           crossed their own summary-scoring critical trigger. */
        var families = muted.WouldHavePaged.Where(w => w.ServerId == 1).Select(w => w.AlertFamily).OrderBy(f => f).ToArray();
        Assert.Equal(new[]
        {
            FleetSweepEngine.FamilyBlocking,
            FleetSweepEngine.FamilyDeadlocks,
            FleetSweepEngine.FamilyHighCpu,
        }, families);

        /* Evidence states its own derivation, so nobody reads the ledger as an alert-engine replay. */
        Assert.All(muted.WouldHavePaged, w =>
            Assert.Contains("summary-scoring critical trigger", w.EvidenceJson, StringComparison.Ordinal));

        /* And the same fleet under alerts-on writes NO ledger rows. */
        var unmuted = ComposeSimple(readings, alertsEnabled: true);
        Assert.Empty(unmuted.WouldHavePaged);
        Assert.True(unmuted.Run.AlertsEnabled);
    }

    /// <summary>
    /// The engine makes no delivery call — the structural half of the master-off contract, beside the
    /// behavioural pin above. <c>AlertMasterSwitchSurfaceTests</c>' census scans every production file
    /// for the delivery seams and fails on any un-censused caller, so the strong guarantee lives
    /// there; this pin is the local statement of intent that makes a future delivery call in this
    /// file a two-test conversation instead of a census surprise.
    /// </summary>
    [Fact]
    public void TheEngineSource_CallsNoDeliverySeam()
    {
        var engine = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "FleetSweepEngine.cs");

        foreach (var seam in new[] { ".DeliverAsync(", ".NotifyAsync(", ".TrySendAsync(", "SendFindingAlertAsync(" })
        {
            Assert.DoesNotContain(seam, engine, StringComparison.Ordinal);
        }

        /* And the worker launches it fire-and-track with the stacking guard — the backlog's shape, so
           a slow sweep costs its own slots and never the collection loop. */
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("_fleetSweep is null || _fleetSweep.IsCompleted", worker, StringComparison.Ordinal);
    }

    /* ─────────────────────── the post-restart settle window ─────────────────────── */

    /// <summary>
    /// Inside the settle window a stale fleet is startup transient, three-for-three on the measured
    /// install day: no-data verdicts say so, staleness opens NO watch item, an ACTIVE staleness item
    /// is omitted whole (its counters stand — a restart is not evidence the episode ended), and
    /// fleet-wide quiet does not fail liveness. Real data still bands normally — the window suppresses
    /// staleness judgments, not evidence.
    /// </summary>
    [Fact]
    public void InsideTheSettleWindow_StalenessIsStartupTransient_AndNothingAdvancesOnIt()
    {
        var justRestarted = new FleetSweepInstrumentCounters(Now.AddMinutes(-3), 2, 0);
        var standingStale = new FleetSweepWatchItem(
            1, FleetSweepEngine.CollectionStaleItemKey, "condition", FleetSweepWatchStateMachine.Open,
            3, 0, 100, 100, 100, null, SpanStart, SpanStart, "{}");

        var composition = ComposeSimple(
            new[] { NoData(1, "server-a"), CriticalDeadlocks(2, "server-b") },
            activeItems: new[] { standingStale },
            instruments: justRestarted);

        /* The no-data card names the window and the classification. */
        var stale = composition.Verdicts.Single(v => v.ServerId == 1);
        Assert.Equal("No Data", stale.Band);
        Assert.Contains("startup-transient", stale.BandReason, StringComparison.Ordinal);

        /* No staleness item opens, and the standing one is OMITTED — untouched, not missed. */
        Assert.DoesNotContain(composition.WatchItems, w => w.ItemKey == FleetSweepEngine.CollectionStaleItemKey);

        /* Fleet liveness is not failed by window staleness... */
        Assert.True(composition.Run.InstrumentsAlive);
        Assert.Contains("post_restart_window\":true", composition.Run.InstrumentLivenessJson, StringComparison.Ordinal);

        /* ...and real evidence still bands and still advances its own item — the window is not a
           holiday from data that exists. */
        Assert.Equal("Critical", composition.Verdicts.Single(v => v.ServerId == 2).Band);
        Assert.Contains(composition.WatchItems, w =>
            w.ServerId == 2 && w.ItemKey == FleetSweepEngine.BandCriticalItemKey);
    }

    /// <summary>The window's figure: 10 minutes — double the worst of the three ~4–5 minute
    /// measurements (2026-09-15), under the floor cadence so a real outage is loud by sweep two.</summary>
    [Fact]
    public void TheSettleWindow_IsTenMinutes_UnderTheFloorCadence()
    {
        Assert.Equal(TimeSpan.FromMinutes(10), FleetSweepEngine.PostRestartSettleWindow);
        Assert.True(FleetSweepEngine.PostRestartSettleWindow
            < TimeSpan.FromMinutes(FleetSweepCadence.IntervalMinutesFloor));
    }

    /* ─────────────────────── span arithmetic ─────────────────────── */

    /// <summary>The span anchors on the previous sweep's instant (no gap between documents), capped at
    /// the cadence ceiling after long downtime, and falls back to one interval on the first sweep.</summary>
    [Fact]
    public void TheSpan_AnchorsOnThePreviousSweep_CappedAtTheCeiling()
    {
        var interval = TimeSpan.FromMinutes(60);

        Assert.Equal(Now - interval, FleetSweepEngine.ComputeSpanStart(Now, interval, null));

        var recent = new FleetSweepRun(1, Now.AddMinutes(-90), Now.AddMinutes(-150), Now.AddMinutes(-90),
            null, true, 1, 1, true, "{}", "{}");
        Assert.Equal(recent.SweptAtUtc, FleetSweepEngine.ComputeSpanStart(Now, interval, recent));

        var ancient = new FleetSweepRun(1, Now.AddDays(-9), Now.AddDays(-9).AddHours(-1), Now.AddDays(-9),
            null, true, 1, 1, true, "{}", "{}");
        Assert.Equal(
            Now - TimeSpan.FromMinutes(FleetSweepCadence.IntervalMinutesCeiling),
            FleetSweepEngine.ComputeSpanStart(Now, interval, ancient));
    }

    /* ─────────────────────── helpers ─────────────────────── */

    private static void AssertReportFlag(string reportJson, string property, bool expected)
    {
        using var document = JsonDocument.Parse(reportJson);
        Assert.Equal(expected, document.RootElement.GetProperty(property).GetBoolean());
    }
}
