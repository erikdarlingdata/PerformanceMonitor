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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One server's inputs to a sweep: the daily-summary signals aggregated over the sweep span,
/// or the fault that kept the sweep from reading them. <see cref="ReadFault"/> non-null means the
/// SWEEP's own instrument failed for this server — which must render as a dead instrument and a
/// <c>No Data</c>-banded card whose reason says "unreadable", never as a quiet server; the distinction
/// between "empty" and "unreadable" is the whole quiet-is-not-clean contract.</summary>
public sealed record FleetSweepServerReading(
    int ServerId,
    string ServerName,
    DailyHealthSignals Signals,
    long PeakBlockWaitMs,
    string? ReadFault);

/// <summary>The in-process counters the sweep cites as its instrument-liveness evidence: the service's
/// own start instant (the restart detector — the same instant <c>get_collection_health</c> reports as
/// <c>service.started_at</c> and <c>alert_read_health.counting_since</c>) and the alert path's
/// pass/failure totals, whose frozen-beside-a-delivering-path reading is how a real gating defect was
/// caught (#3464).</summary>
public sealed record FleetSweepInstrumentCounters(
    DateTime ServiceStartedUtc,
    long AlertPassesTotal,
    long AlertReadFailuresTotal);

/// <summary>Everything one sweep persists, composed pure so the tests drive it with a fixed clock and
/// fixture readings: the run row (document included), the per-server verdicts, the would-have-paged
/// ledger (non-empty only under master-off), and the full watch-item images the store upserts.</summary>
public sealed record FleetSweepComposition(
    FleetSweepRun Run,
    IReadOnlyList<FleetSweepServerVerdict> Verdicts,
    IReadOnlyList<FleetSweepWouldHavePagedEntry> WouldHavePaged,
    IReadOnlyList<FleetSweepWatchItem> WatchItems);

/// <summary>
/// The fleet sweep engine (#3466, lane 2 of 4): the scheduled, stateful whole-fleet evaluation that
/// writes lane 1's tables. Darling-only, like the store — a sweep is a statement about a fleet.
///
/// <para><b>The scoring path is REUSED, not forked.</b> Per-server verdicts come from the same two
/// pieces every day surface already bands with: <see cref="DailySummarySql.RangeSql"/> aggregates the
/// signals (through <c>DarlingHealthReader.GetWindowSignalsAsync</c>, the same statement bound to the
/// sweep's own span instead of a calendar day) and the shared <see cref="DailyHealthBandCalculator"/>
/// folds them into a band with <see cref="DailyHealthBandCalculator.BuildReasons"/> as the card's
/// stated reasons. So the sweep, the Performance Calendar and <c>get_daily_summary</c> cannot disagree
/// about what the same signals mean — and when the banding thresholds move, every surface moves
/// together. The one wording this file owns is the No-Data reason, because "no collection this day"
/// is the calendar's phrasing and a sweep span is not a day.</para>
///
/// <para><b>The diff is against persisted state, not memory.</b> Sweep N joins its verdicts to sweep
/// N−1's rows (by <c>previous_sweep_id</c>, the acceptance criteria's provable anchor): band
/// transitions, servers new to the sweep, servers that left. This is the <c>compare_analysis</c> leg
/// conceptually — two windows, per-key deltas, worse/better/stable — applied to sweep state rather
/// than analysis facts. No previous sweep persisted means the document SAYS so
/// (<c>no_previous_sweep: true</c>) rather than quietly restating absolutes.</para>
///
/// <para><b>Instrument liveness is computed before anything else is believed.</b> The sweep proves its
/// own data sources: which servers' signal reads succeeded, whether any server reported collection
/// inside the span, and whether the alert path's pass counter advanced since the previous sweep. Each
/// check that cannot be judged says WHY (service restarted since the previous sweep resets the
/// counter baseline; master-off freezes the counter by design — the #3464 gate stops the pass before
/// <c>RecordPass</c>, so a frozen counter under mute is expected and stated, not alarming). A judged
/// failure makes <c>instruments_alive</c> false, names the dead instrument in the liveness block, and
/// the schema's NOT NULL pair guarantees the answer is carried on every run row.</para>
///
/// <para><b>The would-have-paged ledger is derived from what the sweep itself can see, honestly.</b>
/// Under <c>alerts_enabled: false</c> the sweep decomposes each Critical verdict into the summary
/// scoring's own critical triggers — deadlocks, collection errors, severe memory pressure, sustained
/// high CPU, heavy blocking — one ledger row per (server, family) with the trigger's figures as
/// evidence. This is deliberately NOT a re-run of the alert engine: the alert families' thresholds,
/// cooldowns and mute rules are the engine's business, and a sweep that simulated them would drift
/// from them. What the ledger claims is exactly what it can prove: conditions the sweep's own scoring
/// banded Critical while the fleet was muted, which on the evidence day was the only surface that
/// carried an 18-minute CPU pin. The derivation is stated here so nobody reads the ledger as the
/// alert engine's counterfactual output. The DOCUMENT carries the ledger member exactly when the
/// check ran (#3478): present — including empty — under master-off, ABSENT under alerts-on, because
/// a stored <c>[]</c> on a sweep whose derivation never executed is a fabricated check, not an empty
/// result. Documents persisted before that gate carry the fabricated key immutably;
/// <see cref="FleetSweepPresentation.BuildRunNode"/> strips it at render for those.</para>
///
/// <para><b>Restart awareness is a gate, not a heuristic.</b> The service's own start instant is the
/// detector (<see cref="FleetSweepInstrumentCounters.ServiceStartedUtc"/>); inside
/// <see cref="PostRestartSettleWindow"/> a no-data server is classified startup-transient in its
/// reason, staleness opens no watch item, and fleet-wide quiet does not fail liveness — the
/// purge-before-collection-loop shape the spec measured three-for-three at ~4–5 minutes. The window
/// is 10 minutes: double the worst measurement, so a slow install day still lands inside it, and
/// short enough that a real post-restart outage is loud by the second sweep at any legal cadence.</para>
///
/// <para><b>Watch items go through lane 1's state machine, unforked.</b> What qualifies is
/// deliberately narrow in v1 — the spec's own contracts: a server whose verdict banded Critical
/// (<see cref="BandCriticalItemKey"/>), a server that reported nothing outside the settle window
/// (<see cref="CollectionStaleItemKey"/>), and the fleet-scope instruments-dead condition
/// (<see cref="InstrumentsDeadItemKey"/>, on the store's sentinel server id 0). Every active item is
/// evaluated every sweep — a miss is an evaluation — EXCEPT where the sweep cannot honestly judge:
/// a staleness item inside the settle window, or any item on a server whose read faulted, is OMITTED
/// from the upsert so its row and counters stand untouched rather than advancing on evidence that
/// does not exist. Known-noise consumption (mute lists as first-class data) rides the alert-count
/// signal, which already excludes dismissed rows; watch items keyed to muted families are a later
/// lane's refinement, stated here so the omission reads as a decision.</para>
///
/// <para><b>Master-off is when this engine matters most, and it delivers NOTHING.</b> The sweep
/// produces rows under <c>alerts_enabled: false</c> — that is the muted-mode contract — and makes no
/// delivery call anywhere: no deliverer, no notifier, no channel. Delivery is lane 4's daily rollup,
/// bounded by the owner's cadence ruling. <c>AlertMasterSwitchSurfaceTests</c>' source census scans
/// every production file for delivery-seam calls, so this file staying out of its census is the
/// structural proof, and <c>FleetSweepEngineTests</c> pins the master-off compose.</para>
/// </summary>
public static class FleetSweepEngine
{
    /// <summary>
    /// How long after the service's own start the sweep treats fleet staleness as startup-transient.
    /// The purge-before-collection-loop window was measured three-for-three at ~4–5 minutes on the
    /// 2026-09-15 install evidence; 10 minutes doubles the worst measurement so a slow install day
    /// still lands inside it, while staying under the floor cadence so a real outage is loud by the
    /// second sweep. Detected from the service's own start instant — the same
    /// <c>counting_since</c> every health surface reports — not from any per-server stamp, because
    /// the window is a property of THIS process's store, not of the monitored servers.
    /// </summary>
    public static readonly TimeSpan PostRestartSettleWindow = TimeSpan.FromMinutes(10);

    /// <summary>Watch item: this server's verdict banded Critical. Two consecutive Critical sweeps
    /// open it; two quiet sweeps close it — the state machine's bars.</summary>
    public const string BandCriticalItemKey = "band-critical";

    /// <summary>Watch item: this server reported no collection inside the sweep span, outside the
    /// post-restart settle window. The standing "instruments dead for one member" condition.</summary>
    public const string CollectionStaleItemKey = "collection-stale";

    /// <summary>Watch item, fleet scope (<see cref="FleetSweepStore.FleetScopeServerId"/>): the
    /// sweep's own instrument-liveness verdict was false.</summary>
    public const string InstrumentsDeadItemKey = "instruments-dead";

    /* The would-have-paged families — the summary scoring's critical triggers, named as families so
       the lane-4 rollup can count and rank them in SQL. These are the SWEEP's vocabulary, deliberately
       distinct from the alert engine's metric names: the ledger is derived from summary scoring, and
       borrowing the engine's spellings would claim a provenance the rows do not have. */
    public const string FamilyDeadlocks = "deadlocks";

    /// <summary>Retained as VOCABULARY for rows already in the ledger; no new row carries it. #3539 A2 made
    /// collection errors a Warning-ceiling share of the span's runs (the collector-health surface's own bar
    /// and tier), and the ledger is derived from CRITICAL triggers only — so the family has nothing left to
    /// fire on. Stored verdicts are immutable and their readers key on this spelling.</summary>
    public const string FamilyCollectionErrors = "collection-errors";
    public const string FamilyMemoryCritical = "memory-critical";
    public const string FamilyHighCpu = "high-cpu";
    public const string FamilyBlocking = "blocking";

    /* ─────────────────────────────── the pure core ─────────────────────────────── */

    /// <summary>
    /// Composes one sweep from its inputs — pure and clock-free (the caller supplies <paramref
    /// name="nowUtc"/>), so <c>FleetSweepEngineTests</c> drives every branch with fixtures. The sweep
    /// id is the instant's ticks: generator-issued and time-ordered, which is all the store asks of it
    /// (its reads ORDER BY the instant with the id as tiebreak rather than trusting this detail).
    /// </summary>
    public static FleetSweepComposition Compose(
        DateTime nowUtc,
        DateTime spanStartUtc,
        bool alertsEnabled,
        int serversExpected,
        IReadOnlyList<FleetSweepServerReading> readings,
        FleetSweepRun? previousRun,
        IReadOnlyList<FleetSweepServerVerdict> previousVerdicts,
        IReadOnlyList<FleetSweepWatchItem> activeWatchItems,
        FleetSweepInstrumentCounters instruments,
        DeadlockRateThresholds deadlockRateTiers)
    {
        ArgumentNullException.ThrowIfNull(readings);
        ArgumentNullException.ThrowIfNull(previousVerdicts);
        ArgumentNullException.ThrowIfNull(activeWatchItems);
        ArgumentNullException.ThrowIfNull(instruments);

        /* #3525: the deadlock-rate tiers travel INTO the shared scorer, so the sweep's verdicts band on the
           pair get_alert_settings reports — required rather than defaulted, the DeadlockSeverity discipline:
           a caller that kept the old call would compile and silently band on the shipped pair while the
           Overview card used the store's. Built once, because the banding thresholds must be one
           configuration for the whole sweep. */
        var banding = new DailyHealthThresholds { DeadlockRates = deadlockRateTiers };

        var sweepId = nowUtc.Ticks;
        var inSettleWindow = nowUtc - instruments.ServiceStartedUtc < PostRestartSettleWindow;
        var previousByServer = previousVerdicts.ToDictionary(v => v.ServerId);

        /* ---- per-server verdicts, off the shared scorer ---- */

        var verdicts = new List<FleetSweepServerVerdict>(readings.Count);
        var readFaults = new List<(string Server, string Fault)>();

        foreach (var reading in readings.OrderBy(r => r.ServerName, StringComparer.Ordinal).ThenBy(r => r.ServerId))
        {
            previousByServer.TryGetValue(reading.ServerId, out var previous);

            string band;
            string? reason;
            if (reading.ReadFault is { } fault)
            {
                /* Unreadable, not quiet: the band vocabulary has no fifth word, so No Data carries it
                   with a reason that says which it is — and the liveness block goes red below, so the
                   header cannot read green over this card. */
                band = DailyHealthBandCalculator.Label(DailyHealthBand.NoData);
                reason = "The sweep could not read this server's signals: " + fault
                    + ". Unreadable is not quiet — instrument liveness is red for this sweep.";
                readFaults.Add((reading.ServerName, fault));
            }
            else
            {
                var classified = DailyHealthBandCalculator.Classify(reading.Signals, banding);
                band = DailyHealthBandCalculator.Label(classified);
                reason = classified switch
                {
                    DailyHealthBand.Healthy => null,
                    DailyHealthBand.NoData when inSettleWindow =>
                        "No collection inside the sweep span — inside the post-restart settle window (service started "
                        + instruments.ServiceStartedUtc.ToString("o") + "), classified startup-transient, not an outage.",
                    DailyHealthBand.NoData => "No collection inside the sweep span.",
                    _ => string.Join("; ", DailyHealthBandCalculator.BuildReasons(reading.Signals, reading.PeakBlockWaitMs)),
                };
            }

            verdicts.Add(new FleetSweepServerVerdict(
                reading.ServerId,
                reading.ServerName,
                band,
                previous?.Band,
                reason,
                reading.ReadFault is null ? SerializeSignals(reading) : null));
        }

        var serversReported = readings.Count(r => r.ReadFault is null && r.Signals.HasData);

        /* ---- instrument liveness, judged before anything else is believed ---- */

        var liveness = JudgeInstrumentLiveness(
            alertsEnabled, serversExpected, serversReported, readFaults,
            previousRun, instruments, inSettleWindow);

        /* ---- the would-have-paged ledger, master-off only ---- */

        var wouldHavePaged = new List<FleetSweepWouldHavePagedEntry>();
        if (!alertsEnabled)
        {
            foreach (var reading in readings.Where(r => r.ReadFault is null))
            {
                if (DailyHealthBandCalculator.Classify(reading.Signals, banding) != DailyHealthBand.Critical)
                {
                    continue;
                }

                foreach (var (family, evidence) in DecomposeCriticalTriggers(reading, banding))
                {
                    wouldHavePaged.Add(new FleetSweepWouldHavePagedEntry(reading.ServerId, family, evidence));
                }
            }
        }

        /* ---- watch items, through lane 1's state machine ---- */

        var watchItems = AdvanceWatchItems(
            nowUtc, sweepId, verdicts, readings, activeWatchItems, liveness.Alive, inSettleWindow);

        /* ---- the document, then the run row that carries it ---- */

        var criticalLabel = DailyHealthBandCalculator.Label(DailyHealthBand.Critical);
        var transitions = verdicts
            .Where(v => v.PreviousBand is not null && !string.Equals(v.Band, v.PreviousBand, StringComparison.Ordinal))
            .ToList();
        var newServers = verdicts.Where(v => v.PreviousBand is null && previousRun is not null).Select(v => v.ServerName).ToList();
        var currentIds = readings.Select(r => r.ServerId).ToHashSet();
        var departedServers = previousVerdicts.Where(v => !currentIds.Contains(v.ServerId)).Select(v => v.ServerName).ToList();

        /* An ordered member list rather than one anonymous type, because one member is CONDITIONAL
           and an anonymous type cannot omit a member per instance (#3478). OrderedDictionary keeps
           the muted document byte-identical to the anonymous shape it replaced — same key order,
           same member spellings, same serializer — because stored documents are immutable and their
           readers pin the muted shape. */
        var report = new OrderedDictionary<string, object?>
        {
            /* The stored document's two ids stay JSON NUMBERS, decided at #3487 (which re-spelled
               them as strings on every WIRE payload, because tick-scale ids round in a double-based
               client's JSON.parse). The store is .NET-side and a long round-trips a bigint exactly,
               so the stored record never lies where it lives — the lie only ever happened at the JS
               boundary, and guarding that boundary is the presentation layer's job, which re-spells
               these two fields on the freshly parsed embed at render. Changing the stored spelling
               instead would buy nothing the render rule does not already provide, and would cost a
               permanent vintage split: stored documents are immutable, so every reader of the raw
               rows would need to handle both spellings for the store's whole retention. */
            ["sweep_id"] = sweepId,
            ["swept_at"] = nowUtc.ToString("o"),
            ["span_start"] = spanStartUtc.ToString("o"),
            ["span_end"] = nowUtc.ToString("o"),
            /* The mute header the spec requires on EVERY sweep, so the state cannot fade from
               operator memory. */
            ["alerts_enabled"] = alertsEnabled,
            ["previous_sweep_id"] = previousRun?.SweepId,
            /* Null anchor stated, never implied: a first sweep diffs against nothing and says so. */
            ["no_previous_sweep"] = previousRun is null,
            ["post_restart_window"] = inSettleWindow,
            ["instruments_alive"] = liveness.Alive,
            ["fleet"] = new
            {
                servers_expected = serversExpected,
                servers_reported = serversReported,
                bands = verdicts
                    .GroupBy(v => v.Band, StringComparer.Ordinal)
                    .OrderBy(g => g.Key, StringComparer.Ordinal)
                    .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal),
            },
            ["changes"] = new
            {
                band_transitions = transitions.Select(v => new
                {
                    server = v.ServerName,
                    from = v.PreviousBand,
                    to = v.Band,
                    reason = v.BandReason,
                }),
                new_servers = newServers,
                departed_servers = departedServers,
            },
            ["watch"] = new
            {
                opened = watchItems.Where(w => w.OpenedSweepId == sweepId)
                    .Select(w => new { server_id = w.ServerId, item = w.ItemKey, condition = w.Condition }),
                closed = watchItems.Where(w => w.ClosedSweepId == sweepId)
                    .Select(w => new { server_id = w.ServerId, item = w.ItemKey, condition = w.Condition }),
                carried = watchItems.Where(w => w.State == FleetSweepWatchStateMachine.Carried && w.ClosedSweepId != sweepId)
                    .Select(w => new
                    {
                        server_id = w.ServerId,
                        item = w.ItemKey,
                        consecutive_hits = w.ConsecutiveHits,
                        consecutive_misses = w.ConsecutiveMisses,
                    }),
            },
        };

        /* The document carries the ledger member exactly when the check ran (#3478). Under alerts-on
           the derivation above never executes, so a stored [] would not be an empty result — it would
           fabricate a check that was never made, the precise over-claim the presentation contract
           forbids and the live sighting caught. Present-and-empty under mute stays a statement:
           "muted, and nothing would have paged". */
        if (!alertsEnabled)
        {
            report["would_have_paged"] = wouldHavePaged.Select(w => new
            {
                server_id = w.ServerId,
                family = w.AlertFamily,
                evidence = w.EvidenceJson,
            });
        }

        report["critical_servers"] = verdicts.Where(v => string.Equals(v.Band, criticalLabel, StringComparison.Ordinal))
            .Select(v => new { server = v.ServerName, reason = v.BandReason });

        var run = new FleetSweepRun(
            sweepId,
            nowUtc,
            spanStartUtc,
            nowUtc,
            previousRun?.SweepId,
            alertsEnabled,
            serversExpected,
            serversReported,
            liveness.Alive,
            liveness.Json,
            JsonSerializer.Serialize(report));

        return new FleetSweepComposition(run, verdicts, wouldHavePaged, watchItems);
    }

    /// <summary>The read-side cadence clamp — the same named bounds the MCP write path and the
    /// Viewer's save gate enforce, so no accepted value is ever rewritten here.</summary>
    public static int ClampIntervalMinutes(int configured) =>
        Math.Clamp(configured, FleetSweepCadence.IntervalMinutesFloor, FleetSweepCadence.IntervalMinutesCeiling);

    /// <summary>
    /// The span this sweep summarizes: from the previous sweep's instant (the diff anchor, so no gap
    /// opens between consecutive documents) — capped at the cadence ceiling, so a sweep waking from
    /// long downtime never claims to have consulted a window its per-source retention tiers might not
    /// hold, and honestly bounded because <c>span_start</c> is a statement of what was read, not of
    /// how long the service was away. First sweep ever: one interval back.
    /// </summary>
    public static DateTime ComputeSpanStart(DateTime nowUtc, TimeSpan interval, FleetSweepRun? previousRun)
    {
        var floor = nowUtc - TimeSpan.FromMinutes(FleetSweepCadence.IntervalMinutesCeiling);
        if (previousRun is null)
        {
            return nowUtc - interval;
        }

        return previousRun.SweptAtUtc > floor ? previousRun.SweptAtUtc : floor;
    }

    /* ─────────────────────────────── liveness ─────────────────────────────── */

    private sealed record LivenessVerdict(bool Alive, string Json);

    /// <summary>
    /// The quiet-is-not-clean block: every instrument the sweep consulted, whether it was advancing,
    /// and — for every check that cannot be judged — why not, so a reader can disagree with the
    /// boolean rather than believe it. Unjudgeable is never failure: a frozen alert-pass counter under
    /// master-off is the #3464 gate doing its job (the pass returns before <c>RecordPass</c>), and a
    /// reset baseline after a restart is the restart, so each carries its stated cause instead of
    /// tripping the verdict. What DOES trip it: a per-server signal read the sweep itself could not
    /// perform, fleet-wide quiet outside the settle window, and a pass counter frozen across two
    /// alerts-on sweeps with servers reporting — the exact frozen-counter-beside-a-delivering-path
    /// reading that caught the evidence day's gating defect.
    /// </summary>
    private static LivenessVerdict JudgeInstrumentLiveness(
        bool alertsEnabled,
        int serversExpected,
        int serversReported,
        List<(string Server, string Fault)> readFaults,
        FleetSweepRun? previousRun,
        FleetSweepInstrumentCounters instruments,
        bool inSettleWindow)
    {
        var alive = true;
        var notes = new List<string>();

        if (readFaults.Count > 0)
        {
            alive = false;
            notes.Add($"{readFaults.Count} server signal read(s) failed — those servers are unreadable, not quiet.");
        }

        if (serversExpected > 0 && serversReported == 0)
        {
            if (inSettleWindow)
            {
                notes.Add("Fleet-wide quiet inside the post-restart settle window — startup transient, not judged.");
            }
            else
            {
                alive = false;
                notes.Add("No server reported any collection inside the span — the collection instruments are not advancing.");
            }
        }

        /* The pass-counter baseline is the PREVIOUS run's liveness block — the sweep's own persisted
           memory, which is the point of lane 1: the frozen-counter comparison needs a persisted prior
           reading, and the previous document carries one. */
        long? previousPasses = null;
        if (previousRun is not null)
        {
            previousPasses = TryReadPassesTotal(previousRun.InstrumentLivenessJson);
        }

        var baselineReset = previousRun is not null && instruments.ServiceStartedUtc > previousRun.SweptAtUtc;
        bool? advancing = null;
        if (previousPasses is { } baseline && !baselineReset)
        {
            advancing = instruments.AlertPassesTotal > baseline;
        }

        if (advancing == false)
        {
            if (alertsEnabled && previousRun is { AlertsEnabled: true } && serversReported > 0 && !inSettleWindow)
            {
                alive = false;
                notes.Add($"The alert-pass counter is frozen at {instruments.AlertPassesTotal} since the previous sweep "
                    + "while alerts are enabled and servers are reporting — the alert path's instruments are not advancing.");
            }
            else if (!alertsEnabled || previousRun is { AlertsEnabled: false })
            {
                notes.Add("The alert-pass counter did not advance — expected under the alert master switch, which stops "
                    + "the pass before it is counted; stated so the frozen counter reads as the mute, not as a dead path.");
            }
            else
            {
                notes.Add("The alert-pass counter did not advance; not judged (settle window or no reporting servers).");
            }
        }
        else if (baselineReset)
        {
            notes.Add("Service restarted since the previous sweep — the pass-counter baseline reset, so counter advance "
                + "is not judgeable this sweep.");
        }

        var json = JsonSerializer.Serialize(new
        {
            alive,
            service_started_at = instruments.ServiceStartedUtc.ToString("o"),
            post_restart_window = inSettleWindow,
            baseline_reset = baselineReset,
            alert_passes_total = instruments.AlertPassesTotal,
            alert_passes_previous = previousPasses,
            alert_passes_advancing = advancing,
            alert_read_failures_total = instruments.AlertReadFailuresTotal,
            servers_expected = serversExpected,
            servers_reported = serversReported,
            server_read_faults = readFaults.Select(f => new { server = f.Server, error = f.Fault }),
            notes,
        });

        return new LivenessVerdict(alive, json);
    }

    /// <summary>The previous sweep's <c>alert_passes_total</c>, or null when its liveness block does
    /// not carry one (a document from a build before this field, or hand-fed test state). Null means
    /// "no baseline", never zero — zero would judge the counter frozen against a number nobody
    /// recorded.</summary>
    private static long? TryReadPassesTotal(string livenessJson)
    {
        try
        {
            using var document = JsonDocument.Parse(livenessJson);
            if (document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty("alert_passes_total", out var passes)
                && passes.ValueKind == JsonValueKind.Number)
            {
                return passes.GetInt64();
            }
        }
        catch (JsonException)
        {
            /* An unparseable previous block is a missing baseline, not a sweep failure. */
        }

        return null;
    }

    /* ─────────────────────────────── watch items ─────────────────────────────── */

    /// <summary>
    /// Evaluates every active watch item plus every fresh sighting through
    /// <see cref="FleetSweepWatchStateMachine"/>, and returns the full row images the store upserts.
    /// A miss is an evaluation — an active item not sighted this sweep advances its exit counter —
    /// EXCEPT where the sweep cannot honestly judge, and those items are OMITTED (rows untouched)
    /// rather than fed a fabricated miss: staleness items inside the settle window (staleness there is
    /// the restart, not the server), and any item on a server whose signal read faulted (unreadable is
    /// neither hit nor quiet). One consequence of evaluating inside the settle window is decided, not
    /// overlooked: a NON-staleness pending item missed there has its entry count reset — and that is
    /// bounded, because the window is shorter than the floor cadence, so at most one sweep lands inside
    /// it, and a single miss cannot close an open item under the exit bar. The opened/closed stamps
    /// always describe the CURRENT episode — a resurrected item clears the old episode's stamps, whose
    /// history lives in the sweep documents that reported it.
    /// </summary>
    private static List<FleetSweepWatchItem> AdvanceWatchItems(
        DateTime nowUtc,
        long sweepId,
        IReadOnlyList<FleetSweepServerVerdict> verdicts,
        IReadOnlyList<FleetSweepServerReading> readings,
        IReadOnlyList<FleetSweepWatchItem> activeWatchItems,
        bool instrumentsAlive,
        bool inSettleWindow)
    {
        var criticalLabel = DailyHealthBandCalculator.Label(DailyHealthBand.Critical);
        var faultedServers = readings.Where(r => r.ReadFault is not null).Select(r => r.ServerId).ToHashSet();
        var readingsById = readings.Where(r => r.ReadFault is null).ToDictionary(r => r.ServerId);

        /* This sweep's hits: (server, item) -> (condition text, fresh evidence). */
        var hits = new Dictionary<(int ServerId, string ItemKey), (string Condition, string Evidence)>();

        foreach (var verdict in verdicts)
        {
            if (!string.Equals(verdict.Band, criticalLabel, StringComparison.Ordinal)
                || !readingsById.TryGetValue(verdict.ServerId, out var reading))
            {
                continue;
            }

            hits[(verdict.ServerId, BandCriticalItemKey)] = (
                "Server banded Critical by the sweep's summary scoring.",
                SerializeSignals(reading));
        }

        if (!inSettleWindow)
        {
            foreach (var reading in readings)
            {
                if (reading.ReadFault is null && !reading.Signals.HasData)
                {
                    hits[(reading.ServerId, CollectionStaleItemKey)] = (
                        "Server reported no collection inside the sweep span.",
                        JsonSerializer.Serialize(new { span_end = nowUtc.ToString("o"), rows_in_span = 0 }));
                }
            }
        }

        if (!instrumentsAlive)
        {
            hits[(FleetSweepStore.FleetScopeServerId, InstrumentsDeadItemKey)] = (
                "The sweep's instrument-liveness verdict was false — see the run's liveness block.",
                JsonSerializer.Serialize(new { sweep_id = sweepId }));
        }

        var results = new List<FleetSweepWatchItem>();
        var evaluated = new HashSet<(int, string)>();

        foreach (var item in activeWatchItems)
        {
            var key = (item.ServerId, item.ItemKey);
            evaluated.Add(key);

            /* The two cannot-judge carve-outs: omitted whole, so the row's counters stand. */
            if (item.ItemKey == CollectionStaleItemKey && inSettleWindow)
            {
                continue;
            }

            if (item.ServerId != FleetSweepStore.FleetScopeServerId && faultedServers.Contains(item.ServerId))
            {
                continue;
            }

            var held = hits.TryGetValue(key, out var hit);
            var advance = FleetSweepWatchStateMachine.Advance(
                item.State, held, item.ConsecutiveHits, item.ConsecutiveMisses);

            results.Add(BuildItemImage(item, advance, held ? hit : null, sweepId, nowUtc));
        }

        foreach (var (key, hit) in hits)
        {
            if (evaluated.Contains(key))
            {
                continue;
            }

            /* No active row: FirstSighting — which is also the resurrection path. A CLOSED row for
               this key may still exist; the upsert overwrites its mutable columns with this fresh
               episode's image while the birth record survives the conflict arm's carve-out. */
            var advance = FleetSweepWatchStateMachine.FirstSighting();
            var fresh = new FleetSweepWatchItem(
                key.ServerId, key.ItemKey, hit.Condition,
                advance.State, advance.ConsecutiveHits, advance.ConsecutiveMisses,
                sweepId, sweepId, null, null, nowUtc, nowUtc, hit.Evidence);

            results.Add(BuildItemImage(fresh, advance, hit, sweepId, nowUtc));
        }

        return results
            .OrderBy(w => w.ServerId)
            .ThenBy(w => w.ItemKey, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>One advanced row image. Evidence is fresh on a hit and NULL on a miss — the store's
    /// COALESCE keeps the standing evidence, so a carried item never cites nothing. The last-seen pair
    /// moves ONLY on a hit — last-seen means SIGHTING, lane 1's contract — because a miss is an
    /// evaluation, and stamping evaluations would make every active row read "seen just now" forever:
    /// a pending item that is never resighted stays pending, is re-evaluated every sweep, and a
    /// re-stamped <c>last_seen_at</c> would keep it ahead of the retention arm keyed on that column for
    /// the rest of the store's life, while the readers' <c>ORDER BY last_seen_at DESC</c> would rank
    /// "most recently evaluated" — every active row, every sweep — which ranks nothing. Carrying the
    /// stamps is safe for live episodes: an open item takes at most one standing miss before the exit
    /// bar closes it, so a row still being carried is never more than one sweep from its last sighting.
    /// The episode stamps follow the state: opened is set on the opening sweep and cleared when a fresh
    /// episode begins; closed is set on the closing sweep and cleared the moment the item is anything
    /// but closed.</summary>
    private static FleetSweepWatchItem BuildItemImage(
        FleetSweepWatchItem row,
        FleetSweepWatchStateMachine.WatchAdvance advance,
        (string Condition, string Evidence)? hit,
        long sweepId,
        DateTime nowUtc)
    {
        var opened = advance.JustOpened
            ? sweepId
            : advance.State == FleetSweepWatchStateMachine.Pending ? (long?)null : row.OpenedSweepId;

        var closed = advance.JustClosed
            ? sweepId
            : advance.State == FleetSweepWatchStateMachine.Closed ? row.ClosedSweepId : (long?)null;

        return new FleetSweepWatchItem(
            row.ServerId,
            row.ItemKey,
            hit?.Condition ?? row.Condition,
            advance.State,
            advance.ConsecutiveHits,
            advance.ConsecutiveMisses,
            row.FirstSeenSweepId,
            hit is null ? row.LastSeenSweepId : sweepId,
            opened,
            closed,
            row.FirstSeenAtUtc,
            hit is null ? row.LastSeenAtUtc : nowUtc,
            hit?.Evidence);
    }

    /* ─────────────────────────────── would-have-paged ─────────────────────────────── */

    /// <summary>
    /// One Critical verdict decomposed into the summary scoring's own critical triggers — the honest
    /// derivation the class doc states: what the sweep can SEE, not what the alert engine would have
    /// decided. Each row carries the trigger, the measured figure and the threshold it crossed, so an
    /// operator auditing a mute reads evidence rather than an assertion.
    /// </summary>
    private static IEnumerable<(string Family, string Evidence)> DecomposeCriticalTriggers(
        FleetSweepServerReading reading, DailyHealthThresholds thresholds)
    {
        var signals = reading.Signals;

        /* #3525: the deadlock family fires on the RATE the scorer banded Critical with, never on a bare
           count — the same DeadlockSeverity call Classify makes, so the ledger cannot page on a trigger the
           verdict did not band. A sub-hour span's unrateable arm maxes out at Warning, so it can never
           reach this. */
        if (ServerHealthClassifier.DeadlockSeverity(signals.Deadlocks, signals.Window, thresholds.DeadlockRates)
            == HealthSeverity.Critical)
        {
            /* Critical implies a rateable window (the unrateable arm returns Warning or Unknown), so the
               rate is present by construction. */
            var ratePerHour = ServerHealthClassifier.DeadlockRatePerHour(signals.Deadlocks, signals.Window)!.Value;
            yield return (FamilyDeadlocks, DeadlockRateEvidence(
                ratePerHour, thresholds.DeadlockRates.CriticalPerHour, signals.Deadlocks));
        }

        /* Collection errors are absent from this decomposition on purpose (#3539 A2): the arm is now a
           share of the span's runs with a Warning ceiling — see DailyHealthBandCalculator.CollectionErrorSeverity
           — so no Critical verdict can be attributed to it and FamilyCollectionErrors produces no new rows. */

        if (signals.MemoryCriticalEvents > 0)
        {
            yield return (FamilyMemoryCritical, Evidence("severe memory-pressure events", signals.MemoryCriticalEvents, 1));
        }

        /* #3539 A2: the CPU family fires on the arm the verdict banded with — the hot-sample count against
           the bar SCALED to this span (the greater of the excursion-scale minimum and the sustained-heat
           rate), never against the fixed 6 the pre-#3539 constant applied to every span. */
        if (DailyHealthBandCalculator.HighCpuSeverity(signals.HighCpuEvents, signals.Window, thresholds) == HealthSeverity.Critical)
        {
            yield return (FamilyHighCpu, Evidence(
                "high-CPU samples (>= 80% total host) against the span-scaled bar",
                signals.HighCpuEvents,
                thresholds.HighCpuCriticalSamplesFor(signals.Window)));
        }

        /* #3539 A2/A3: the blocking family fires on the same BlockingSeverity call Classify makes — the
           rate over the span, or the 60 s wait arm — so the ledger cannot page on a count the verdict did
           not band. Which arm decided is legible from the evidence: the rate row names the rate tier, the
           wait row names the wait bar. */
        /* The SIGNALS' peak, not the reading's: Classify sees only the signals, and the two must agree.
           The production read fills both from one MAX. */
        var peakBlockSeconds = signals.PeakBlockWaitMs / 1000.0;
        if (ServerHealthClassifier.BlockingSeverity(signals.BlockingEvents, peakBlockSeconds, signals.Window)
            == HealthSeverity.Critical)
        {
            yield return (FamilyBlocking, BlockingEvidence(signals.BlockingEvents, signals.Window, peakBlockSeconds));
        }
    }

    /// <summary>The blocking family's evidence (#3539 A3): the arm that banded is the one named. A 60 s
    /// block is the wait arm's Critical whatever the rate; otherwise the RATE is the value, with the raw
    /// count as its own member for the operator reconciling against the blocking grid.</summary>
    private static string BlockingEvidence(long count, TimeSpan window, double peakBlockSeconds)
    {
        if (peakBlockSeconds >= ServerHealthThresholds.BlockingCriticalWaitSeconds)
        {
            return JsonSerializer.Serialize(new
            {
                derivation = "summary-scoring critical trigger under alerts_enabled: false — not an alert-engine replay",
                trigger = "longest single block in the sweep span, seconds",
                value = peakBlockSeconds,
                threshold = ServerHealthThresholds.BlockingCriticalWaitSeconds,
                blocking_count = count,
            });
        }

        /* Critical without the wait arm implies a rateable window at or past the Critical tier, so the
           rate is present by construction. */
        var ratePerHour = ServerHealthClassifier.BlockingRatePerHour(count, window)!.Value;
        return JsonSerializer.Serialize(new
        {
            derivation = "summary-scoring critical trigger under alerts_enabled: false — not an alert-engine replay",
            trigger = "blocking events per hour over the sweep span",
            value = ratePerHour,
            threshold = ServerHealthThresholds.BlockingCriticalPerHour,
            blocking_count = count,
        });
    }

    private static string Evidence(string what, long value, double threshold) =>
        JsonSerializer.Serialize(new
        {
            derivation = "summary-scoring critical trigger under alerts_enabled: false — not an alert-engine replay",
            trigger = what,
            value,
            threshold,
        });

    /// <summary>The deadlock family's evidence (#3525): the shared shape with the RATE as the value —
    /// because the rate is what banded — plus the raw count as its own member, because the count is the
    /// countable fact an operator reconciles against the deadlock grid.</summary>
    private static string DeadlockRateEvidence(double ratePerHour, double criticalPerHour, long count) =>
        JsonSerializer.Serialize(new
        {
            derivation = "summary-scoring critical trigger under alerts_enabled: false — not an alert-engine replay",
            trigger = "deadlocks per hour over the sweep span",
            value = ratePerHour,
            threshold = criticalPerHour,
            deadlock_count = count,
        });

    private static string SerializeSignals(FleetSweepServerReading reading) =>
        JsonSerializer.Serialize(new
        {
            deadlocks = reading.Signals.Deadlocks,
            /* #3525: the rate the deadlock signal banded on, beside the count it was derived from (null on
               an unrateable span) — the card's own disclosure rule: evidence a reader can disagree with has
               to include the figure the band read. Additive members on NEW rows only; stored verdicts are
               immutable and their readers key on the members that were always here. */
            deadlock_rate_per_hour = ServerHealthClassifier.DeadlockRatePerHour(
                reading.Signals.Deadlocks, reading.Signals.Window),
            window_minutes = reading.Signals.Window.TotalMinutes,
            collection_errors = reading.Signals.CollectionErrors,
            /* #3539 A2/A3, additive on NEW rows: the denominator the error share bands on, and the blocking
               rate beside its count (null on an unrateable span) — the same disclosure rule as the deadlock
               rate above. */
            collection_runs = reading.Signals.CollectionRuns,
            high_cpu_events = reading.Signals.HighCpuEvents,
            blocking_events = reading.Signals.BlockingEvents,
            blocking_rate_per_hour = ServerHealthClassifier.BlockingRatePerHour(
                reading.Signals.BlockingEvents, reading.Signals.Window),
            memory_pressure_events = reading.Signals.MemoryPressureEvents,
            memory_critical_events = reading.Signals.MemoryCriticalEvents,
            alert_count = reading.Signals.AlertCount,
            peak_block_wait_ms = reading.PeakBlockWaitMs,
            has_data = reading.Signals.HasData,
        });

    /* ─────────────────────────────── the IO shell ─────────────────────────────── */

    /// <summary>
    /// One sweep, end to end: read the engine seam (throwing reads — a fault here fails the WHOLE
    /// sweep loudly, leaving the previous sweep as the newest complete one, which is the #2448
    /// discipline's read-end), read each server's signals (per-server faults are CAUGHT and become
    /// dead-instrument evidence, the loud-not-quiet path), compose, persist in the store's one
    /// transaction.
    ///
    /// <para><b>Never throws.</b> It is launched fire-and-track off the fleet loop (the
    /// oversized-plan backlog's shape), so an escaping fault would be an unobserved task exception —
    /// and a sweep failure must cost the fleet nothing but this sweep slot. Cancellation returns
    /// quietly; any other fault is one error line naming what was lost.</para>
    /// </summary>
    public static async Task RunAsync(
        NpgsqlDataSource postgres,
        IReadOnlyList<(int ServerId, string ServerName)> servers,
        TimeSpan interval,
        bool alertsEnabled,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(servers);
        ArgumentNullException.ThrowIfNull(logger);

        try
        {
            var previousRun = await FleetSweepStore.GetLatestSweepAsync(postgres, cancellationToken).ConfigureAwait(false);
            var previousVerdicts = previousRun is null
                ? new List<FleetSweepServerVerdict>()
                : await FleetSweepStore.GetServerVerdictsForEngineAsync(postgres, previousRun.SweepId, cancellationToken).ConfigureAwait(false);
            var activeItems = await FleetSweepStore.GetActiveWatchItemsAsync(postgres, cancellationToken).ConfigureAwait(false);

            /* #3525: the deadlock-rate tiers, read once per sweep off the fleet reader's own published SQL
               — an engine-seam read, so a fault here loudly costs this sweep slot rather than quietly
               banding the fleet on the shipped pair. */
            var deadlockRateTiers = await ReadDeadlockRateThresholdsAsync(postgres, cancellationToken).ConfigureAwait(false);

            var nowUtc = DateTime.UtcNow;
            var spanStartUtc = ComputeSpanStart(nowUtc, interval, previousRun);

            /* Sequential, deliberately: 100+ aggregate reads against the loopback store are cheap one
               at a time and a herd all at once — and the sweep is a background errand racing nothing. */
            var readings = new List<FleetSweepServerReading>(servers.Count);
            foreach (var (serverId, serverName) in servers.DistinctBy(s => s.ServerId))
            {
                cancellationToken.ThrowIfCancellationRequested();
                readings.Add(await ReadServerSignalsAsync(
                    postgres, serverId, serverName, spanStartUtc, nowUtc, cancellationToken).ConfigureAwait(false));
            }

            var composition = Compose(
                nowUtc, spanStartUtc, alertsEnabled, servers.Count, readings,
                previousRun, previousVerdicts, activeItems, ReadInstrumentCounters(), deadlockRateTiers);

            await FleetSweepStore.RecordSweepAsync(
                postgres, composition.Run, composition.Verdicts,
                composition.WouldHavePaged, composition.WatchItems, cancellationToken).ConfigureAwait(false);

            logger.LogInformation(
                "Fleet sweep {SweepId}: {Reported}/{Expected} servers reported, instruments_alive={Alive}, "
                + "{Verdicts} verdict(s), {WatchItems} watch item(s), {Ledger} would-have-paged row(s), alerts_enabled={AlertsEnabled}",
                composition.Run.SweepId, composition.Run.ServersReported, composition.Run.ServersExpected,
                composition.Run.InstrumentsAlive, composition.Verdicts.Count, composition.WatchItems.Count,
                composition.WouldHavePaged.Count, alertsEnabled);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown mid-sweep: nothing persisted (the store's transaction is all-or-nothing past
               its connection open), so the previous sweep stands as the newest complete one. */
        }
        catch (Exception ex)
        {
            /* One sweep lost, loudly; the next slot tries again. The previous sweep remains the
               newest complete document — stale, stamped with its own instant, incapable of
               misleading anyone. */
            logger.LogError("Fleet sweep failed; the previous sweep remains the newest complete one: {Message}", ex.Message);
        }
    }

    /// <summary>One server's signals over the span — the shared daily-summary aggregate, summed across
    /// the UTC-day buckets the statement returns (exact: every signal is an additive count over the
    /// same half-open window). A fault is CAUGHT into the reading, because for a per-server read the
    /// honest rendering is a dead instrument on that server's card, not a lost sweep.</summary>
    private static async Task<FleetSweepServerReading> ReadServerSignalsAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        DateTime spanStartUtc,
        DateTime spanEndUtc,
        CancellationToken cancellationToken)
    {
        try
        {
            var rows = await DarlingHealthReader.GetWindowSignalsAsync(
                postgres, serverId, spanStartUtc, spanEndUtc, cancellationToken).ConfigureAwait(false);

            var peakBlock = rows.Count == 0 ? 0L : rows.Max(r => r.MaxBlockDurationMs);

            var signals = new DailyHealthSignals
            {
                HasData = rows.Count > 0,
                Deadlocks = rows.Sum(r => r.DeadlockCount),
                CollectionErrors = rows.Sum(r => r.CollectionErrors),
                /* #3539 A2: runs sum exactly as the errors do (additive counts over one half-open window),
                   so the share the band reads is the span's, not the first day-bucket's. */
                CollectionRuns = rows.Sum(r => r.CollectionRuns),
                HighCpuEvents = rows.Sum(r => r.HighCpuEvents),
                BlockingEvents = rows.Sum(r => r.BlockingEvents),
                /* #3539 A2: the longest block across the span's day buckets — a MAX, not a sum, because it
                   is a magnitude; the blocking band's wait arm reads it. */
                PeakBlockWaitMs = peakBlock,
                MemoryPressureEvents = rows.Sum(r => r.MemoryPressureEvents),
                MemoryCriticalEvents = rows.Sum(r => r.MemoryCriticalEvents),
                AlertCount = rows.Sum(r => r.AlertCount),
                /* #3525: the sweep's own span, NOT a calendar day — the denominator the deadlock rate
                   bands on. At the floor cadence (15 min) this is sub-hour and the band's unrateable arm
                   applies: deadlocks read Warning, never a rate-multiplied Critical. */
                Window = spanEndUtc - spanStartUtc,
            };

            return new FleetSweepServerReading(serverId, serverName, signals, peakBlock, ReadFault: null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new FleetSweepServerReading(serverId, serverName, default, 0L, ex.Message);
        }
    }

    /// <summary>The deadlock band's tiers from the store's singleton settings row (#3368, V120) — the
    /// fleet reader's read, off its own published SQL, hoisted here once per sweep (#3525). A store with
    /// no row yet bands on the shipped pair, which is what such a store would seed anyway; values come
    /// back RAW and <see cref="DeadlockRateThresholds"/> clamps on read.</summary>
    private static async Task<DeadlockRateThresholds> ReadDeadlockRateThresholdsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(DarlingFleetReader.FleetDeadlockRateThresholdSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return new DeadlockRateThresholds(reader.GetDouble(0), reader.GetDouble(1));
        }

        return DeadlockRateThresholds.Default;
    }

    /// <summary>The in-process instrument counters, read at compose time: the process-global alert
    /// read-health counter's start instant (the restart detector), the pass total summed across every
    /// per-server bucket, and the instance-wide swallowed-read total.</summary>
    private static FleetSweepInstrumentCounters ReadInstrumentCounters()
    {
        var counter = PerformanceMonitor.Alerting.AlertReadFailureCounter.Shared;
        long passes = 0;
        foreach (var key in counter.ServerKeys())
        {
            passes += counter.ReadFor(key).ServerAlertPasses;
        }

        return new FleetSweepInstrumentCounters(
            DateTime.SpecifyKind(counter.CountingSince, DateTimeKind.Utc),
            passes,
            counter.ReadInstance().ReadFailures);
    }
}
