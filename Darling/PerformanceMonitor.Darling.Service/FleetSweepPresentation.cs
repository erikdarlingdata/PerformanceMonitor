/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The fleet sweep's ONE wire shape (#3466, lane 3 of 4): the builders that turn
/// <see cref="FleetSweepStore"/>'s rows into the JSON both presentation surfaces serve — the web
/// feed's <c>/api/sweeps</c> routes here, and lane 4's <c>get_sweep_reports</c> MCP tool against the
/// same builders. The store's reads stay row-shaped because the engine joins them relationally; the
/// RENDER reshaping (documents embedded as objects rather than escaped strings, server names joined
/// onto ledger rows, the hysteresis bars beside the counters they bound) happens once here, so the
/// two surfaces cannot drift into different opinions about what a sweep looks like — the
/// <c>DarlingWebEndpoints</c>/MCP zero-drift rule, applied to this feature's rows.
///
/// <para><b>The stored documents ride EMBEDDED, not re-escaped.</b> <c>report_json</c>,
/// <c>instrument_liveness_json</c> and the evidence payloads are JSON the service itself wrote; a
/// client that receives them as strings has to parse a document out of a document, and an agent
/// reading <c>get_sweep_reports</c> would burn its window on escape characters. They are parsed here
/// and embedded as objects (the <c>BuildFullRuleNode</c> definition-embedding precedent). A payload
/// that does not parse — hand-fed test state, a truncated row — is carried VERBATIM as a string
/// rather than dropped: a reader must see that the field held something unreadable, never an absence
/// that looks deliberate.</para>
///
/// <para><b>The honesty rules are the engine's, restated on the wire.</b> Every run node carries the
/// mute header (<c>alerts_enabled</c>) and the liveness verdict (<c>instruments_alive</c>)
/// unconditionally — the spec requires the mute stated on every sweep, and quiet-is-not-clean applies
/// to the render too. The detail shape carries <c>would_have_paged</c> whenever the sweep ran under
/// master-off, INCLUDING empty: "muted, and nothing would have paged" is a statement the operator is
/// owed, where an absent key reads as "nothing was checked". The EMBEDDED report obeys the same
/// contract (#3478): documents persisted before the engine's storage gate carry a fabricated
/// <c>would_have_paged: []</c> on alerts-on sweeps, immutably and for their whole retention, so
/// <see cref="BuildRunNode"/> strips the key from the alerts-on embed — the render's copy, never the
/// stored row — which also keeps the contract true against any future writer regression.</para>
/// </summary>
public static class FleetSweepPresentation
{
    /// <summary>
    /// The timeline payload: the runs inside the caller's span, newest first, each with its full
    /// document embedded. Both bounds are echoed back so the client renders the window it was
    /// actually served — the #2506 discipline's read-end: a response that does not say what window it
    /// covers invites the caller to assume the one it asked for.
    /// </summary>
    public static JsonObject BuildTimelineNode(
        IReadOnlyList<FleetSweepRun> runs, DateTime spanStartUtc, DateTime spanEndUtc)
    {
        ArgumentNullException.ThrowIfNull(runs);

        var sweeps = new JsonArray();
        foreach (var run in runs)
        {
            sweeps.Add(BuildRunNode(run));
        }

        return new JsonObject
        {
            ["span_start"] = spanStartUtc.ToString("o"),
            ["span_end"] = spanEndUtc.ToString("o"),
            ["count"] = runs.Count,
            ["sweeps"] = sweeps,
        };
    }

    /// <summary>
    /// One run row on the wire: the header facts plus the document and liveness block embedded as
    /// objects. The mute header and the liveness verdict are ALWAYS present — the two facts the spec
    /// requires stated on every sweep, so no client has to know a rule about their absence. The
    /// embedded report's <c>would_have_paged</c> obeys the class contract: absent on an alerts-on
    /// sweep, stripped here (#3478) because legacy documents carry it fabricated.
    /// </summary>
    public static JsonObject BuildRunNode(FleetSweepRun run)
    {
        ArgumentNullException.ThrowIfNull(run);

        var report = EmbedJson(run.ReportJson);

        /* The back-compat arm of #3478: every alerts-on sweep persisted before the engine's storage
           gate carries would_have_paged: [] — written unconditionally, never derived — and stored
           documents are immutable, living out their retention as-written. The strip operates on the
           freshly parsed embed (EmbedJson parses per call), NEVER the stored row, so the record keeps
           what was written while the wire keeps the contract: on an alerts-on sweep no would-have-paged
           check was made, and serving an empty ledger would claim one. The type guard keeps the
           unparseable-payload arm intact — a verbatim string is carried whole, not probed. */
        if (run.AlertsEnabled && report is JsonObject reportObject)
        {
            reportObject.Remove("would_have_paged");
        }

        return new JsonObject
        {
            ["sweep_id"] = run.SweepId,
            ["swept_at"] = run.SweptAtUtc.ToString("o"),
            ["span_start"] = run.SpanStartUtc.ToString("o"),
            ["span_end"] = run.SpanEndUtc.ToString("o"),
            ["previous_sweep_id"] = run.PreviousSweepId,
            ["alerts_enabled"] = run.AlertsEnabled,
            ["servers_expected"] = run.ServersExpected,
            ["servers_reported"] = run.ServersReported,
            ["instruments_alive"] = run.InstrumentsAlive,
            ["instrument_liveness"] = EmbedJson(run.InstrumentLivenessJson),
            ["report"] = report,
        };
    }

    /// <summary>
    /// One sweep in full: the run node plus its per-server verdicts and — whenever the sweep ran
    /// under master-off — the would-have-paged ledger, with server NAMES joined from the verdicts the
    /// same sweep wrote, because a ledger row keyed on a bare id would make the one table the
    /// muted-mode contract exists for the one table an operator cannot read without a second lookup.
    /// The key is present-and-empty on a muted sweep with nothing to report (that is a finding), and
    /// ABSENT on an alerts-on sweep: the engine writes no ledger there, and an empty array would
    /// claim a check that was never made.
    /// </summary>
    public static JsonObject BuildSweepDetailNode(
        FleetSweepRun run,
        IReadOnlyList<FleetSweepServerVerdict> verdicts,
        IReadOnlyList<FleetSweepWouldHavePagedEntry> wouldHavePaged)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(verdicts);
        ArgumentNullException.ThrowIfNull(wouldHavePaged);

        var node = BuildRunNode(run);

        var verdictArray = new JsonArray();
        var namesById = new Dictionary<int, string>();
        foreach (var verdict in verdicts)
        {
            namesById[verdict.ServerId] = verdict.ServerName;
            verdictArray.Add(BuildVerdictNode(verdict));
        }

        node["verdicts"] = verdictArray;

        if (!run.AlertsEnabled)
        {
            var ledger = new JsonArray();
            foreach (var entry in wouldHavePaged)
            {
                ledger.Add(new JsonObject
                {
                    ["server_id"] = entry.ServerId,
                    ["server"] = namesById.TryGetValue(entry.ServerId, out var name) ? name : null,
                    ["family"] = entry.AlertFamily,
                    ["evidence"] = EmbedJson(entry.EvidenceJson),
                });
            }

            node["would_have_paged"] = ledger;
        }

        return node;
    }

    /// <summary>One verdict row: the band beside the previous band with the transition pre-computed
    /// (<c>band_changed</c> — the diff is the feature's acceptance test, so the wire states it rather
    /// than leaving every client to re-derive string equality), the reason exactly as stored (null on
    /// a card that needs no defending), and the signals evidence embedded so a reader can disagree
    /// with the band rather than believe it.</summary>
    public static JsonObject BuildVerdictNode(FleetSweepServerVerdict verdict)
    {
        ArgumentNullException.ThrowIfNull(verdict);

        return new JsonObject
        {
            ["server_id"] = verdict.ServerId,
            ["server"] = verdict.ServerName,
            ["band"] = verdict.Band,
            ["previous_band"] = verdict.PreviousBand,
            ["band_changed"] = verdict.PreviousBand is not null
                && !string.Equals(verdict.Band, verdict.PreviousBand, StringComparison.Ordinal),
            ["reason"] = verdict.BandReason,
            ["signals"] = verdict.VerdictJson is null ? null : EmbedJson(verdict.VerdictJson),
        };
    }

    /// <summary>
    /// The watch-item payload: each row with its full hysteresis position — the standing counters AND
    /// the bars they count toward (<see cref="FleetSweepWatchStateMachine.EntryConsecutiveSweeps"/> /
    /// <see cref="FleetSweepWatchStateMachine.ExitConsecutiveSweeps"/>, spliced from the machine's own
    /// constants), because "1 consecutive miss" only means something beside "of 2 to close", and a
    /// client that hardcoded the bars would silently mis-render the day they become per-item data
    /// (the #3297 route the store doc reserves).
    /// </summary>
    public static JsonObject BuildWatchItemsNode(IReadOnlyList<FleetSweepWatchItem> items)
    {
        ArgumentNullException.ThrowIfNull(items);

        var array = new JsonArray();
        foreach (var item in items)
        {
            array.Add(new JsonObject
            {
                ["server_id"] = item.ServerId,
                /* The fleet-scope sentinel named on the wire, so no client has to know that 0 means
                   the fleet — the store constant is the one authority for the number. */
                ["fleet_scope"] = item.ServerId == FleetSweepStore.FleetScopeServerId,
                ["item"] = item.ItemKey,
                ["condition"] = item.Condition,
                ["state"] = item.State,
                ["consecutive_hits"] = item.ConsecutiveHits,
                ["consecutive_misses"] = item.ConsecutiveMisses,
                ["first_seen_sweep_id"] = item.FirstSeenSweepId,
                ["last_seen_sweep_id"] = item.LastSeenSweepId,
                ["opened_sweep_id"] = item.OpenedSweepId,
                ["closed_sweep_id"] = item.ClosedSweepId,
                ["first_seen_at"] = item.FirstSeenAtUtc.ToString("o"),
                ["last_seen_at"] = item.LastSeenAtUtc.ToString("o"),
                ["evidence"] = item.EvidenceJson is null ? null : EmbedJson(item.EvidenceJson),
            });
        }

        return new JsonObject
        {
            ["count"] = items.Count,
            ["entry_bar_sweeps"] = FleetSweepWatchStateMachine.EntryConsecutiveSweeps,
            ["exit_bar_sweeps"] = FleetSweepWatchStateMachine.ExitConsecutiveSweeps,
            ["items"] = array,
        };
    }

    /// <summary>
    /// A stored JSON payload embedded as a node, or — when it does not parse — carried verbatim as a
    /// string. The service wrote every payload this touches, so the fallback arm is for hand-fed test
    /// state and truncation, and it fails toward VISIBLE: a reader sees the field held something
    /// unreadable rather than an absence that looks deliberate.
    /// </summary>
    internal static JsonNode? EmbedJson(string json)
    {
        ArgumentNullException.ThrowIfNull(json);

        try
        {
            return JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            return JsonValue.Create(json);
        }
    }
}
