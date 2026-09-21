/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3466 lane 4: the fleet sweep's DAILY CHANNEL ROLLUP — the delivery half the engine deliberately does
/// not have, on the collector-cost digest's machinery and held to the digest suite's disciplines:
///
/// <para><b>Every silence pin has a firing twin.</b> "A quiet day posts nothing" is trivially satisfied by
/// a rollup that never posts, so each no-post assertion is paired with the same harness delivering the
/// moment the day has something to say — and the interval pins assert both that a post is not repeated and
/// that a skipped day did not consume the interval.</para>
///
/// <para><b>The rendered document's figures reconcile with each other</b>
/// (<see cref="TheRenderedRollupsFiguresReconcileWithEachOther"/>): the header's counts against the facts
/// they summarize, the ledger's total against its family lines, and the mute sentence against the sweep
/// count it claims — parsed from the SHIPPED string the deliverer received, because the failure a rollup
/// invites is a document whose sentences disagree with each other rather than a field with a wrong
/// value.</para>
///
/// <para><b>The delivery contract is the subject.</b> Master-off delivers nothing and consumes nothing;
/// the first post after re-enable covers its trailing day and STATES how many of its sweeps ran muted,
/// with the would-have-paged ledger rendered under that statement — the muted-mode contract's channel
/// half, which is the reason the whole feature exists.</para>
/// </summary>
public class FleetSweepRollupTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /* ---------------- fixtures ---------------- */

    private static readonly DateTime Day = new(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The ONE construction site for a run row in this file — the digest suite's factory rule,
    /// so a <see cref="FleetSweepRun"/> member change is a one-line merge here.</summary>
    private static FleetSweepRun Run(
        long sweepId, DateTime sweptAtUtc, bool alertsEnabled = true, bool instrumentsAlive = true,
        object? report = null, string? rawReport = null) => new(
        sweepId, sweptAtUtc, sweptAtUtc.AddHours(-1), sweptAtUtc, null, alertsEnabled, 3, 3, instrumentsAlive,
        "{\"alive\":" + (instrumentsAlive ? "true" : "false") + "}",
        rawReport ?? (report is null ? "{}" : JsonSerializer.Serialize(report)));

    /// <summary>A report document in the engine's own shape — the extraction parses what the engine
    /// persists, so the fixtures speak its vocabulary (changes.band_transitions, watch.opened/closed,
    /// fleet.bands).</summary>
    private static object Report(
        object[]? transitions = null, object[]? opened = null, object[]? closed = null,
        Dictionary<string, int>? bands = null) => new
        {
            changes = new { band_transitions = transitions ?? Array.Empty<object>() },
            watch = new { opened = opened ?? Array.Empty<object>(), closed = closed ?? Array.Empty<object>() },
            fleet = new { bands = bands ?? new Dictionary<string, int>() },
        };

    private static object Transition(string server, string from, string to, string? reason = null) =>
        new { server, from, to, reason };

    private static object WatchEvent(int serverId, string item, string condition = "c") =>
        new { server_id = serverId, item, condition };

    private static readonly Dictionary<int, string> Names = new()
    {
        [1] = "pm-server-1",
        [2] = "pm-server-2",
        [3] = "pm-server-3",
    };

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        /* #3580: DeliverAndReportAsync is REQUIRED on the seam rather than defaulted (CONTRIBUTING, Two-Store
           Parity), so every fake answers it by hand. This one reports nothing: null is "unreported", which the
           two daily documents treat as delivered, exactly as every fire before #3580 was. */
        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return null;
        }
    }

    private sealed class RecordingHistoryStore : IAlertHistoryStore
    {
        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
    }

    /// <summary>The digest suite's harness shape: the PRODUCT's own settings object over a default config,
    /// a recording deliverer, and a controllable clock.</summary>
    private sealed class Harness
    {
        public DarlingConfig Config { get; } = new();
        public RecordingDeliverer Deliverer { get; } = new();
        public RecordingHistoryStore History { get; } = new();
        public bool Muted { get; set; }
        public DateTime Now { get; set; } = Day;

        public DarlingSelfAlertEvaluator Build() => new(
            new DarlingAlertSettings(Config), Deliverer, History, _ => Muted,
            logger: null, utcNow: () => Now);
    }

    private static Task ApplyAsync(
        Harness h, DarlingSelfAlertEvaluator e,
        IReadOnlyList<FleetSweepRun> runs,
        IReadOnlyList<FleetSweepLedgerSpanEntry>? ledger = null,
        IReadOnlyDictionary<int, string>? names = null) =>
        e.ApplyFleetSweepRollupAsync(
            runs,
            ledger ?? Array.Empty<FleetSweepLedgerSpanEntry>(),
            names ?? Names,
            h.Now - DarlingSelfAlertEvaluator.FleetSweepRollupInterval,
            h.Now,
            Ct);

    /// <summary>A minimal reportable day: one sweep carrying one band transition.</summary>
    private static FleetSweepRun[] OneTransitionDay() => new[]
    {
        Run(1, Day.AddHours(-2), report: Report(
            transitions: new[] { Transition("pm-server-1", "Healthy", "Critical", "deadlocks in span") },
            bands: new Dictionary<string, int> { ["Critical"] = 1, ["Healthy"] = 2 })),
    };

    /* ---------------- the fire shape ---------------- */

    /// <summary>The rollup is a REPORT and fires like the digest: its own metric, no severity override (the
    /// per-metric map's declared INFO arm decides), the fleet-level store label, the sweep count as the
    /// whole-number value, and the honest "no threshold" string on the NOT NULL column.</summary>
    [Fact]
    public async Task TheRollup_FiresUnderItsOwnMetric_WithNoSeverityOverride()
    {
        var h = new Harness();
        var e = h.Build();

        await ApplyAsync(h, e, OneTransitionDay());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.FleetSweepRollupMetric, fired.MetricName);
        Assert.Equal("sweeprollup", fired.ServerKey);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Null(fired.Severity);
        Assert.Equal(1, fired.NumericCurrentValue);
        Assert.Equal(0, fired.NumericThresholdValue);
        Assert.Contains("no threshold", fired.ThresholdValue, StringComparison.Ordinal);
    }

    /// <summary>Once per interval — the ceiling the owner ruled, asserted in both directions on one clock:
    /// a second reportable day inside the interval delivers nothing, and past it the next rollup goes
    /// out.</summary>
    [Fact]
    public async Task TheRollup_DoesNotRepeatInsideItsInterval_AndDoesAfterIt()
    {
        var h = new Harness();
        var e = h.Build();

        await ApplyAsync(h, e, OneTransitionDay());
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.Add(DarlingSelfAlertEvaluator.FleetSweepRollupInterval).AddMinutes(-1);
        await ApplyAsync(h, e, OneTransitionDay());
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddMinutes(2);
        await ApplyAsync(h, e, OneTransitionDay());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    /* ---------------- the no-post days, each with its firing twin ---------------- */

    /// <summary>A day whose sweeps ran, proved their instruments, moved no bands, touched no watch items
    /// and never saw the mute posts NOTHING — not a one-line all-clear. The affirmative all-clear lives on
    /// the web feed at full cadence where every sweep carries its own liveness proof; a channel all-clear
    /// without that proof would be a green sweep over dead instruments, and one with it would be the
    /// hourly report card the delivery ruling exists to prevent. And the quiet day does not consume the
    /// interval, so the first day with something to say is not delayed by the quiet one evaluated
    /// first.</summary>
    [Fact]
    public async Task AQuietDay_PostsNothing_AndDoesNotConsumeTheInterval()
    {
        var h = new Harness();
        var e = h.Build();

        var quiet = new[]
        {
            Run(1, Day.AddHours(-3), report: Report(bands: new Dictionary<string, int> { ["Healthy"] = 3 })),
            Run(2, Day.AddHours(-2), report: Report(bands: new Dictionary<string, int> { ["Healthy"] = 3 })),
        };

        await ApplyAsync(h, e, quiet);
        Assert.Empty(h.Deliverer.Outcomes);

        /* The firing twin, same instant: the moment the day has content, it posts. */
        await ApplyAsync(h, e, OneTransitionDay());
        Assert.Single(h.Deliverer.Outcomes);
    }

    /// <summary>An empty day — no sweeps at all (the engine off, or a store still warming up) — posts
    /// nothing, vacuously through every reportable term.</summary>
    [Fact]
    public async Task AnEmptyDay_PostsNothing()
    {
        var h = new Harness();
        var e = h.Build();

        await ApplyAsync(h, e, Array.Empty<FleetSweepRun>());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    /* ---------------- the master gate, and what the first post after re-enable says ---------------- */

    /// <summary>Master-off delivers NOTHING — this apply is the one place the sweep feature touches a
    /// channel, so the gate here IS the feature's delivery contract — and the gate does not consume the
    /// interval. The firing twin is the catch-up contract: the first post after re-enable covers its
    /// trailing day and STATES how many of its sweeps ran muted, so the operator is told there is history
    /// to read on the web feed rather than left to discover it.</summary>
    [Fact]
    public async Task TheRollup_IsMasterGated_AndTheFirstPostAfterReEnable_NamesTheMutedSweeps()
    {
        var h = new Harness();
        h.Config.Alerts.Enabled = false;
        var e = h.Build();

        var day = new[]
        {
            Run(1, Day.AddHours(-3), alertsEnabled: false, report: Report(
                transitions: new[] { Transition("pm-server-1", "Healthy", "Critical") })),
            Run(2, Day.AddHours(-2), alertsEnabled: false, report: Report()),
            Run(3, Day.AddHours(-1), report: Report()),
        };

        await ApplyAsync(h, e, day);
        Assert.Empty(h.Deliverer.Outcomes);

        h.Config.Alerts.Enabled = true;
        await ApplyAsync(h, e, day, ledger: new[]
        {
            new FleetSweepLedgerSpanEntry(1, 1, FleetSweepEngine.FamilyDeadlocks),
        });

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("2 of the 3 covered sweeps ran with the alert master switch OFF", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Delivery was off while they ran", fired.DetailText, StringComparison.Ordinal);
        /* The trailing-day boundary is the whole catch-up contract: never a recap of older history. */
        Assert.Contains("the trailing day only", fired.DetailText, StringComparison.Ordinal);
    }

    /// <summary>Muted through the SHARED seam, not a private decision — the digest's rule, restated for
    /// the rollup: recorded, flagged muted, channels skipped by the deliverer.</summary>
    [Fact]
    public async Task TheRollup_HonoursTheSharedMuteSeam()
    {
        var h = new Harness { Muted = true };
        var e = h.Build();

        await ApplyAsync(h, e, OneTransitionDay());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(fired.Muted);
    }

    /// <summary>No resolution edge — a rollup is a measurement of a period, and a "Rollup Cleared" row
    /// after a quiet day would be the most pointless message this product could send.</summary>
    [Fact]
    public async Task TheRollup_WritesNoResolutionRow_WhenTheNextPeriodIsQuiet()
    {
        var h = new Harness();
        var e = h.Build();

        await ApplyAsync(h, e, OneTransitionDay());
        h.Now = h.Now.Add(DarlingSelfAlertEvaluator.FleetSweepRollupInterval);
        await ApplyAsync(h, e, Array.Empty<FleetSweepRun>());

        Assert.Single(h.Deliverer.Outcomes);
        Assert.DoesNotContain(h.History.Records, r => r.MetricName.Contains("Rollup", StringComparison.Ordinal));
    }

    /* ---------------- the rendered document ---------------- */

    private static async Task<AlertOutcome> FireAsync(
        FleetSweepRun[] runs, FleetSweepLedgerSpanEntry[] ledger)
    {
        var h = new Harness();
        var e = h.Build();
        await ApplyAsync(h, e, runs, ledger);
        return Assert.Single(h.Deliverer.Outcomes);
    }

    /// <summary>
    /// The identity pin, the digest suite's discipline: the SHIPPED document re-parsed and its printed
    /// figures held to each other —
    /// <list type="number">
    /// <item>the header's five counts against the facts they summarize (sweeps, transitions, opened,
    ///   closed, liveness incidents, muted sweeps) and against the LINES actually rendered;</item>
    /// <item>the mute sentence's "N of the M covered sweeps" against the header's own muted and sweep
    ///   counts;</item>
    /// <item>the ledger banner's row total against the sum of its family lines, and its family count
    ///   against the family lines rendered;</item>
    /// <item>chronological transition order — the fixture plants the transitions on out-of-order rows, so
    ///   a renderer trusting the read's newest-first order goes red.</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task TheRenderedRollupsFiguresReconcileWithEachOther()
    {
        var runs = new[]
        {
            /* Newest FIRST, the span read's order — extraction must re-order chronologically. */
            Run(3, Day.AddHours(-1), alertsEnabled: false, instrumentsAlive: false, report: Report(
                transitions: new[] { Transition("pm-server-2", "Warning", "Critical", "blocking events") },
                closed: new[] { WatchEvent(3, FleetSweepEngine.BandCriticalItemKey) },
                bands: new Dictionary<string, int> { ["Critical"] = 2, ["Healthy"] = 1 })),
            Run(1, Day.AddHours(-3), report: Report(
                transitions: new[] { Transition("pm-server-1", "Healthy", "Warning", "high-CPU samples") },
                opened: new[] { WatchEvent(1, FleetSweepEngine.BandCriticalItemKey, "banded Critical"), WatchEvent(0, FleetSweepEngine.InstrumentsDeadItemKey) })),
            Run(2, Day.AddHours(-2), alertsEnabled: false, report: Report(
                transitions: new[] { Transition("pm-server-1", "Warning", "Critical") })),
        };

        var ledger = new[]
        {
            new FleetSweepLedgerSpanEntry(2, 1, FleetSweepEngine.FamilyDeadlocks),
            new FleetSweepLedgerSpanEntry(3, 1, FleetSweepEngine.FamilyDeadlocks),
            new FleetSweepLedgerSpanEntry(3, 2, FleetSweepEngine.FamilyHighCpu),
            new FleetSweepLedgerSpanEntry(3, 9, FleetSweepEngine.FamilyHighCpu),
        };

        var fired = await FireAsync(runs, ledger);
        var detail = fired.DetailText ?? string.Empty;

        /* (1) the header, parsed from the shipped short message shape. */
        var header = Regex.Match(
            fired.ShortMessage ?? string.Empty,
            @"Fleet sweep rollup: (\d+) sweeps - (\d+) band transitions, (\d+) watch items opened, "
            + @"(\d+) closed, (\d+) liveness incidents, (\d+) muted sweeps");
        Assert.True(header.Success, fired.ShortMessage);
        int H(int group) => int.Parse(header.Groups[group].Value, CultureInfo.InvariantCulture);

        Assert.Equal(3, H(1));
        Assert.Equal(3, H(2));
        Assert.Equal(2, H(3));
        Assert.Equal(1, H(4));
        Assert.Equal(1, H(5));
        Assert.Equal(2, H(6));

        /* The header's counts against the LINES rendered (all under the caps here). */
        var transitionLines = Regex.Matches(detail, @"^- (?<server>\S+): (?<from>[\w ]+) -> (?<to>[\w ]+?)( \(.*\))?$", RegexOptions.Multiline);
        Assert.Equal(H(2), transitionLines.Count);
        Assert.Equal(H(3), Regex.Matches(detail, @"^- [\w-]+ on .*: .*$", RegexOptions.Multiline).Count
            - CountClosedLines(detail));

        /* The numeric value IS the header's sweep count — the whole-number the classifier renders. */
        Assert.Equal(H(1), fired.NumericCurrentValue);

        /* (2) the mute sentence against the header's own counts. */
        var mute = Regex.Match(detail, @"(\d+) of the (\d+) covered sweeps ran with the alert master switch OFF");
        Assert.True(mute.Success, detail);
        Assert.Equal(H(6), int.Parse(mute.Groups[1].Value, CultureInfo.InvariantCulture));
        Assert.Equal(H(1), int.Parse(mute.Groups[2].Value, CultureInfo.InvariantCulture));

        /* (3) the ledger banner against its family lines. */
        var banner = Regex.Match(detail, @"(\d+) rows across (\d+) servers and (\d+) families:");
        Assert.True(banner.Success, detail);
        var familyLines = Regex.Matches(detail, @"^- (?<family>[\w-]+): (?<rows>\d+) rows on (?<servers>.+)$", RegexOptions.Multiline);
        Assert.Equal(int.Parse(banner.Groups[3].Value, CultureInfo.InvariantCulture), familyLines.Count);
        Assert.Equal(
            int.Parse(banner.Groups[1].Value, CultureInfo.InvariantCulture),
            familyLines.Sum(m => int.Parse(m.Groups["rows"].Value, CultureInfo.InvariantCulture)));
        Assert.Equal(3, int.Parse(banner.Groups[2].Value, CultureInfo.InvariantCulture)); /* servers 1, 2, 9 */

        /* A server the span's verdicts never named renders as its id stated as such — honest, never
           invented — beside the names the verdicts did carry. */
        Assert.Contains("server id 9", detail, StringComparison.Ordinal);
        Assert.Contains("pm-server-1", detail, StringComparison.Ordinal);

        /* The liveness incident is stated in the quiet-is-not-clean vocabulary. */
        Assert.Contains("1 covered sweeps could NOT prove their instruments were alive", detail, StringComparison.Ordinal);

        /* (4) chronological: sweep 1's transition (planted second in the array) renders FIRST. */
        Assert.True(
            detail.IndexOf("pm-server-1: Healthy -> Warning", StringComparison.Ordinal)
            < detail.IndexOf("pm-server-2: Warning -> Critical", StringComparison.Ordinal),
            "transitions must render chronologically, not in the read's newest-first order");

        /* The fleet-as-of line is the NEWEST READABLE sweep's census - and says so. */
        Assert.Contains("The fleet as of the newest readable covered sweep: Critical 2, Healthy 1.", detail, StringComparison.Ordinal);

        /* The fleet-scope watch sentinel is named as the fleet. */
        Assert.Contains("instruments-dead on the fleet", detail, StringComparison.Ordinal);
    }

    private static int CountClosedLines(string detail)
    {
        var closedAt = detail.IndexOf("Watch items closed:", StringComparison.Ordinal);
        if (closedAt < 0)
        {
            return 0;
        }

        var tail = detail[closedAt..];
        var end = tail.IndexOf("\nEvery sweep in full", StringComparison.Ordinal);
        return Regex.Matches(end < 0 ? tail : tail[..end], @"^- ", RegexOptions.Multiline).Count;
    }

    /// <summary>"Muted, and nothing would have paged" is a statement the operator is owed — a muted day
    /// with an EMPTY ledger still posts, says the ledger was empty, and says the check was made.</summary>
    [Fact]
    public async Task AMutedDayWithAnEmptyLedger_StillPosts_AndSaysSo()
    {
        var fired = await FireAsync(
            new[] { Run(1, Day.AddHours(-2), alertsEnabled: false, report: Report()) },
            Array.Empty<FleetSweepLedgerSpanEntry>());

        Assert.Contains("EMPTY would-have-paged ledger", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("the check was made on every muted sweep", fired.DetailText, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3478's document-shape split cannot move the rollup, pinned rather than assumed: the extraction
    /// never consults the document's <c>would_have_paged</c> member — its ledger figures ride the
    /// STORE's rows (the ledger argument) — so all three vintages the store can hold extract identical
    /// facts: a muted document carrying the key (the gated writer's shape), an alerts-on document
    /// without it (post-gate), and a legacy alerts-on document carrying the fabricated <c>[]</c>
    /// (immutable, living out its retention). None lands on the unreadable counter, and the ledger
    /// totals are the rows' — the muted document's own single-row member does not leak in.
    /// </summary>
    [Fact]
    public void TheExtraction_ReadsTheLedgerFromTheStoresRows_WhateverVintageTheDocumentIs()
    {
        object Doc(object[]? ledgerMember) => ledgerMember is null
            ? Report(
                transitions: new[] { Transition("pm-server-1", "Healthy", "Critical") },
                bands: new Dictionary<string, int> { ["Critical"] = 1 })
            : new
            {
                changes = new { band_transitions = new[] { Transition("pm-server-1", "Healthy", "Critical") } },
                watch = new { opened = Array.Empty<object>(), closed = Array.Empty<object>() },
                fleet = new { bands = new Dictionary<string, int> { ["Critical"] = 1 } },
                would_have_paged = ledgerMember,
            };

        var runs = new[]
        {
            /* Muted, key present-and-populated — the gated writer's muted shape. */
            Run(1, Day.AddHours(-3), alertsEnabled: false, rawReport: JsonSerializer.Serialize(
                Doc(new object[] { new { server_id = 1, family = "deadlocks", evidence = "{}" } }))),
            /* Legacy alerts-on, the fabricated [] — what the store holds from before the gate. */
            Run(2, Day.AddHours(-2), rawReport: JsonSerializer.Serialize(Doc(Array.Empty<object>()))),
            /* Post-gate alerts-on: no key at all. */
            Run(3, Day.AddHours(-1), rawReport: JsonSerializer.Serialize(Doc(null))),
        };

        var ledger = new[]
        {
            new FleetSweepLedgerSpanEntry(1, 1, "deadlocks"),
            new FleetSweepLedgerSpanEntry(1, 2, "deadlocks"),
        };

        var facts = DarlingSelfAlertEvaluator.ExtractRollupFacts(runs, ledger, Names);

        Assert.Equal(3, facts.Sweeps);
        Assert.Equal(0, facts.UnreadableItems);   /* no vintage is a parse casualty */
        Assert.Equal(3, facts.Transitions.Count); /* the members it DOES read, read on every vintage */

        /* TWO rows from the store against ONE row inside the muted document: the figures are the
           rows', which is the tolerance — the key can come, go, or lie empty without moving these. */
        Assert.Equal(2, facts.LedgerRows);
        Assert.Equal(2, facts.LedgerServers);
        var family = Assert.Single(facts.Ledger);
        Assert.Equal(2, family.Rows);
    }

    /// <summary>An unreadable sweep document is COUNTED and the count is itself reportable — an unreadable
    /// day must not read as a quiet one, which is the quiet-is-not-clean rule applied to this feature's own
    /// artifacts.</summary>
    [Fact]
    public async Task AnUnreadableDocument_IsCounted_AndIsItselfReportable()
    {
        var fired = await FireAsync(
            new[] { Run(1, Day.AddHours(-2), rawReport: "not json at all") },
            Array.Empty<FleetSweepLedgerSpanEntry>());

        Assert.Contains("1 unreadable items across the covered sweeps' documents", fired.DetailText, StringComparison.Ordinal);
    }

    /// <summary>A band census carrying a count no int holds must not KILL the rollup tick — GetInt32
    /// answers one with FormatException/OverflowException, which the JsonException-only catch does not
    /// swallow, and an escaped tick is a day-long silent outage of the exact feature that promises an
    /// unreadable day cannot read as a quiet one. The corrupt census lands on the one unreadable counter,
    /// and the fleet-as-of line keeps the newest sweep whose census WAS readable — which is what the line
    /// says.</summary>
    [Fact]
    public async Task ABandCountNoIntHolds_IsCountedUnreadable_AndTheFleetAsOfLineKeepsTheNewestReadableCensus()
    {
        var fired = await FireAsync(
            new[]
            {
                Run(1, Day.AddHours(-3), report: Report(
                    bands: new Dictionary<string, int> { ["Healthy"] = 3 })),
                Run(2, Day.AddHours(-2), rawReport:
                    "{\"changes\":{\"band_transitions\":[]},\"watch\":{\"opened\":[],\"closed\":[]},"
                    + "\"fleet\":{\"bands\":{\"Healthy\":99999999999}}}"),
            },
            Array.Empty<FleetSweepLedgerSpanEntry>());

        Assert.Contains("1 unreadable items across the covered sweeps' documents", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains(
            "The fleet as of the newest readable covered sweep: Healthy 3.", fired.DetailText, StringComparison.Ordinal);
    }

    /// <summary>A watch entry missing its item, or carrying a server id no int holds, is counted on the
    /// SAME unreadable counter rather than dropped silently — a watch event the rollup cannot render is
    /// still a watch event the operator was owed, and one counter stated once keeps the honesty argument
    /// whole.</summary>
    [Fact]
    public async Task AWatchEntryMissingItsFields_IsCountedUnreadable_NotSilentlyDropped()
    {
        var fired = await FireAsync(
            new[]
            {
                Run(1, Day.AddHours(-2), rawReport:
                    "{\"changes\":{\"band_transitions\":[]},"
                    + "\"watch\":{\"opened\":[{\"server_id\":1},{\"item\":\"no-server\"}],\"closed\":[]},"
                    + "\"fleet\":{\"bands\":{}}}"),
            },
            Array.Empty<FleetSweepLedgerSpanEntry>());

        Assert.Contains("2 unreadable items across the covered sweeps' documents", fired.DetailText, StringComparison.Ordinal);
    }

    /* ---------------- the surfaces that read the metric name ---------------- */

    /// <summary>The metric name is a webhook automation key and two other assemblies carry their own
    /// literal of it — <c>AlertSeverity.ForMetric</c>'s INFO arm (pinned in <c>Lite.Tests</c>) and
    /// <c>AlertMetricClassifier</c>'s count arm. Pinning the const's VALUE here ties those literals to this
    /// one, the digest's exact arrangement.</summary>
    [Fact]
    public void TheRollupsMetricName_IsTheLiteralTheOtherAssembliesCarry()
    {
        Assert.Equal("Fleet Sweep Rollup", DarlingSelfAlertEvaluator.FleetSweepRollupMetric);
    }

    /// <summary>The rollup's value is a COUNT of covered sweeps — whole number, not a resolution.</summary>
    [Fact]
    public void TheRollupsCount_RendersAsAWholeNumber()
    {
        Assert.Equal("24", AlertMetricClassifier.FormatHistoryValue(
            DarlingSelfAlertEvaluator.FleetSweepRollupMetric, 24));
        Assert.False(AlertMetricClassifier.IsResolution(DarlingSelfAlertEvaluator.FleetSweepRollupMetric));
    }

    /// <summary>The interval is the owner's ruling in code: one day, the ceiling on channel presence and
    /// the covered span in one number, so what a post claims to cover and how often one can arrive cannot
    /// drift apart.</summary>
    [Fact]
    public void TheRollupsInterval_IsTheDailyCeilingTheRulingNames()
    {
        Assert.Equal(TimeSpan.FromDays(1), DarlingSelfAlertEvaluator.FleetSweepRollupInterval);
    }

    /* ---------------- #3834: the structured rows beside the prose ---------------- */

    /// <summary>
    /// The rollup's rows, and the property that makes them additive: one item per thing that HAPPENED, under
    /// a leading item carrying the window and its aggregates — those describe the SPAN rather than any row in
    /// it, and a surface rendering rows alone would otherwise lose "some covered sweeps could not prove their
    /// instruments", which is the one sentence in this document that must never be lost (quiet is not clean).
    /// The prose is asserted unchanged on both paths at the same time, for the digest suite's reason.
    /// </summary>
    [Fact]
    public async Task TheRollupFire_CarriesStructuredRows_AndLeavesTheProseIntact()
    {
        var runs = OneTransitionDay();
        var fired = await FireAsync(runs, Array.Empty<FleetSweepLedgerSpanEntry>());

        Assert.NotNull(fired.Context);
        var context = fired.Context;

        /* The frame first: the covered window in the structure as well as in the sentence (#2506's echo
           discipline), with the aggregates beside it. */
        var summary = Assert.Single(context.Details, d => d.Heading == "Rollup window");
        var summaryFields = summary.Fields.ToDictionary(f => f.Label, f => f.Value, StringComparer.Ordinal);
        Assert.Equal("1", summaryFields["Sweeps"]);
        Assert.Equal("1", summaryFields["Band transitions"]);
        Assert.Equal("0", summaryFields["Liveness incidents"]);
        Assert.Equal("0", summaryFields["Muted sweeps"]);
        Assert.Equal("0", summaryFields["Unreadable items"]);
        Assert.Contains("Window start (UTC)", summaryFields.Keys);
        Assert.Contains("Window end (UTC)", summaryFields.Keys);

        /* And the transition itself, as a row whose figures the prose also prints. */
        var transition = Assert.Single(context.Details,
            d => d.Heading.StartsWith("Band transition: ", StringComparison.Ordinal));
        var fields = transition.Fields.ToDictionary(f => f.Label, f => f.Value, StringComparer.Ordinal);
        Assert.Equal("pm-server-1", fields["Server"]);
        Assert.Equal("Healthy", fields["From"]);
        Assert.Equal("Critical", fields["To"]);
        Assert.Equal("deadlocks in span", fields["Reason"]);

        var prose = fired.DetailText ?? string.Empty;
        Assert.Contains("pm-server-1: Healthy -> Critical", prose, StringComparison.Ordinal);
        Assert.Contains("deadlocks in span", prose, StringComparison.Ordinal);

        /* The prose is the renderer's own output, and the delivery gate still passes it through: an essay is
           never textually equal to a flattened field list, so ProseForDelivery cannot suppress it. */
        var facts = DarlingSelfAlertEvaluator.ExtractRollupFacts(
            runs, Array.Empty<FleetSweepLedgerSpanEntry>(), Names);
        var h = new Harness();
        Assert.Equal(
            DarlingSelfAlertEvaluator.RenderFleetSweepRollup(
                facts, h.Now - DarlingSelfAlertEvaluator.FleetSweepRollupInterval, h.Now).Detail,
            fired.DetailText);
        Assert.NotEqual(AlertDetailText.Flatten(context), fired.DetailText);
        Assert.Equal(fired.DetailText, AlertDetailText.ProseForDelivery(fired.DetailText, context));

        /* A report is not an incident: no Incidents, so no per-event splitting and no delivery filter. */
        Assert.Null(context.Incidents);
    }
}
