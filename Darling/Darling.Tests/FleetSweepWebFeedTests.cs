/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3466 lane 3: the fleet sweep web feed — the <c>/api/sweeps</c> routes' span/state binding, the
/// <see cref="FleetSweepPresentation"/> wire shapes both presentation surfaces serve (lane 4's
/// <c>get_sweep_reports</c> builds on the same builders), and the frontend parity pins.
///
/// <para>The JS is untested by convention (no JS runner in this repo), so the pins live where the
/// repo's other frontend guards live: the API payload SHAPES are pinned here in C#, and the page's
/// load-bearing vocabulary/branches are pinned by source scan — the
/// <see cref="AlertTrayStatusLoggedTests"/> idiom, including its strict object-literal parser so a
/// shape the parser cannot read fails loudly rather than comparing less.</para>
/// </summary>
public sealed class FleetSweepWebFeedTests
{
    /* ---- span binding: validated, refusing, and the same authority as the MCP surface ---------------- */

    [Fact]
    public void TheDefaultSpan_IsOneHour_TheOwnersRuling()
    {
        /* "configurable spans of time though, default hourly" — the ruling that split the surfaces.
           At the shipped hourly cadence the default view lands on the newest sweep. */
        Assert.Equal(1, DarlingFleetSweepEndpoints.DefaultSpanHours);
    }

    [Fact]
    public void AnAbsentSpan_DefaultsToOneHour_EndingNow()
    {
        var error = DarlingFleetSweepEndpoints.ValidateSpan(null, null, out var hours, out var endUtc);

        Assert.Null(error);
        Assert.Equal(DarlingFleetSweepEndpoints.DefaultSpanHours, hours);
        Assert.True((DateTime.UtcNow - endUtc).Duration() < TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void AReadableSpan_Binds_AndTheAnchorMovesTheEnd()
    {
        var error = DarlingFleetSweepEndpoints.ValidateSpan("24", "2026-01-02T03:04:05Z", out var hours, out var endUtc);

        Assert.Null(error);
        Assert.Equal(24, hours);
        Assert.Equal(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), endUtc);
    }

    /// <summary>
    /// An UNREADABLE span is refused, never defaulted — the #3287 filter discipline: a caller who
    /// asked for a window they mistyped must not receive a complete-looking answer to a different
    /// one. Out-of-range values and bad anchors are refused by <c>McpHelpers.ValidateWindow</c>, the
    /// SAME authority every windowed MCP read applies, so the two surfaces speak one refusal
    /// vocabulary (the prose says <c>hours_back</c>, the shared parser's spelling — the mute-rule
    /// endpoints' accepted cosmetic consequence, one surface over).
    /// </summary>
    [Theory]
    [InlineData("abc", null, "Invalid hours value 'abc'")]
    [InlineData("", null, "Invalid hours value ''")]  // the endpoint maps empty to absent; this pins the pure helper's own arm
    [InlineData("0", null, "Invalid hours_back value '0'")]
    [InlineData("-3", null, "Invalid hours_back value '-3'")]
    [InlineData("999", null, "exceeds maximum")]
    [InlineData("4", "not-a-time", "Invalid as_of value")]
    [InlineData("4", "2099-01-01T00:00:00Z", "in the future")]
    public void AnUnusableSpanOrAnchor_IsRefused_NotSubstituted(string? rawHours, string? asOf, string expectedFragment)
    {
        var error = DarlingFleetSweepEndpoints.ValidateSpan(rawHours, asOf, out _, out _);

        Assert.NotNull(error);
        /* Both arms answer the `invalid` envelope since #3739 (the endpoint unwraps it into its own {"error"} body). */
        Assert.True(McpHelpers.IsRefusalEnvelope(error), error);
        Assert.Contains(expectedFragment, McpHelpers.ErrorMessageOf(error!), StringComparison.Ordinal);
    }

    /* ---- the watch-state filter ------------------------------------------------------------------------ */

    [Fact]
    public void TheKnownWatchStates_AreExactlyTheStateMachines_InItsOrder()
    {
        Assert.Equal(
            new[]
            {
                FleetSweepWatchStateMachine.Pending,
                FleetSweepWatchStateMachine.Open,
                FleetSweepWatchStateMachine.Carried,
                FleetSweepWatchStateMachine.Closed,
            },
            DarlingFleetSweepEndpoints.KnownWatchStates);
    }

    [Fact]
    public void EveryMachineState_IsAccepted_AndAbsentMeansTheDefaultView()
    {
        Assert.Null(DarlingFleetSweepEndpoints.ValidateWatchState(null));
        foreach (var state in DarlingFleetSweepEndpoints.KnownWatchStates)
        {
            Assert.Null(DarlingFleetSweepEndpoints.ValidateWatchState(state));
        }
    }

    /// <summary>An unknown state is refused naming the legal values — it would match nothing, and an
    /// empty answer to a typo is indistinguishable from a clean worklist. Ordinal deliberately: the
    /// wire states are the machine's exact spellings, so <c>Open</c> is not <c>open</c>.</summary>
    [Theory]
    [InlineData("garbage")]
    [InlineData("Open")]
    [InlineData("OPEN")]
    public void AnUnknownState_IsRefused_NamingTheLegalValues(string state)
    {
        var error = DarlingFleetSweepEndpoints.ValidateWatchState(state);

        Assert.NotNull(error);
        /* The `invalid` envelope since #3739, hints.parameter naming the web feed's knob; the words are pinned
           on its sentence (the `+` alone would trip a raw-JSON Contains: the serializer escapes it). */
        Assert.Equal("state", JsonDocument.Parse(error!).RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        var sentence = McpHelpers.ErrorMessageOf(error!);
        Assert.Contains(FleetSweepWatchStateMachine.Carried, sentence, StringComparison.Ordinal);
        Assert.Contains("omit for open + carried", sentence, StringComparison.Ordinal);
    }

    /* ---- the wire shapes: FleetSweepPresentation ------------------------------------------------------- */

    private static FleetSweepRun Run(bool alertsEnabled = true, bool instrumentsAlive = true) => new(
        SweepId: 638600000000000000,
        SweptAtUtc: new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc),
        SpanStartUtc: new DateTime(2026, 9, 15, 11, 0, 0, DateTimeKind.Utc),
        SpanEndUtc: new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc),
        PreviousSweepId: 638599964000000000,
        AlertsEnabled: alertsEnabled,
        ServersExpected: 3,
        ServersReported: 2,
        InstrumentsAlive: instrumentsAlive,
        InstrumentLivenessJson: "{\"alive\":" + (instrumentsAlive ? "true" : "false") + ",\"notes\":[]}",
        ReportJson: "{\"fleet\":{\"bands\":{\"Healthy\":2,\"Critical\":1}},\"alerts_enabled\":" + (alertsEnabled ? "true" : "false") + "}");

    [Fact]
    public void TheRunNode_CarriesTheHeaderFacts_AndEmbedsTheDocumentsAsObjects()
    {
        var node = FleetSweepPresentation.BuildRunNode(Run(alertsEnabled: false, instrumentsAlive: false));

        foreach (var key in new[]
        {
            "sweep_id", "swept_at", "span_start", "span_end", "previous_sweep_id",
            "alerts_enabled", "servers_expected", "servers_reported",
            "instruments_alive", "instrument_liveness", "report",
        })
        {
            Assert.True(node.ContainsKey(key), "missing key: " + key);
        }

        /* The two facts the spec requires on EVERY sweep ride the header, and here at their loud
           values: the mute header and the liveness verdict. */
        Assert.False((bool)node["alerts_enabled"]!);
        Assert.False((bool)node["instruments_alive"]!);

        /* The stored documents are EMBEDDED objects, not escaped strings — the BuildFullRuleNode
           definition-embedding precedent, so no client parses a document out of a document. */
        var report = Assert.IsType<JsonObject>(node["report"]);
        Assert.Equal(2, (int)report["fleet"]!["bands"]!["Healthy"]!);
        var liveness = Assert.IsType<JsonObject>(node["instrument_liveness"]);
        Assert.False((bool)liveness["alive"]!);
    }

    /// <summary>
    /// THE #3487 pin: every tick-scale id in a serialized sweep payload is a JSON STRING. A sweep id
    /// is <c>DateTime.UtcNow.Ticks</c>-scale (~6.4e17), roughly 70x past JavaScript's
    /// <c>Number.MAX_SAFE_INTEGER</c> (2^53 ≈ 9.0e15), so a JSON number carrying one is rounded by
    /// every JS consumer's <c>JSON.parse</c> — ~128-tick granularity at this magnitude, measured
    /// live: 639251940075451830 parsed to 639251940075451800, and the Fleet Sweeps page's drill-down
    /// fetched a neighbor the store never held. The input side always took <c>sweep_id</c> as a
    /// string (exact-parse-or-refuse — pinned in <see cref="DarlingMcpFleetSweepToolsTests"/>); this
    /// pins the output side of the same rule, on the one builder both surfaces serve — the shared
    /// builders make it <c>get_sweep_reports</c>' pin too.
    /// </summary>
    [Fact]
    public void TheRunNode_SpellsItsTickScaleIds_AsJsonStrings()
    {
        var node = FleetSweepPresentation.BuildRunNode(Run());

        Assert.Equal(JsonValueKind.String, node["sweep_id"]!.GetValueKind());
        Assert.Equal("638600000000000000", node["sweep_id"]!.GetValue<string>());
        Assert.Equal(JsonValueKind.String, node["previous_sweep_id"]!.GetValueKind());
        Assert.Equal("638599964000000000", node["previous_sweep_id"]!.GetValue<string>());

        /* On the serialized wire, QUOTED — the byte shape the whole issue is about. */
        var json = node.ToJsonString();
        Assert.Contains("\"sweep_id\":\"638600000000000000\"", json, StringComparison.Ordinal);
        Assert.Contains("\"previous_sweep_id\":\"638599964000000000\"", json, StringComparison.Ordinal);

        /* The detail node wraps the same builder, so the sweep_id fetch serves the same spelling. */
        var detail = FleetSweepPresentation.BuildSweepDetailNode(
            Run(), new List<FleetSweepServerVerdict>(), new List<FleetSweepWouldHavePagedEntry>());
        Assert.Equal(JsonValueKind.String, detail["sweep_id"]!.GetValueKind());
    }

    /// <summary>A first sweep's null <c>previous_sweep_id</c> stays JSON null — an absence is not a
    /// spelling, and the page's <c>previous_sweep_id == null</c> branch (the "no previous sweep"
    /// badge) rests on it: the string <c>"null"</c> would be a truthy non-answer that renders as an
    /// id.</summary>
    [Fact]
    public void AFirstSweeps_NullPreviousSweepId_StaysNull_NotAStringSpellingOfNull()
    {
        var node = FleetSweepPresentation.BuildRunNode(Run() with { PreviousSweepId = null });

        Assert.True(node.ContainsKey("previous_sweep_id"), "the key must be PRESENT — a first sweep states its null, never omits it");
        Assert.Null(node["previous_sweep_id"]);
        Assert.Contains("\"previous_sweep_id\":null", node.ToJsonString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnparseablePayload_IsCarriedVerbatim_NeverDropped()
    {
        /* The fallback fails toward VISIBLE: a reader must see the field held something unreadable,
           never an absence that looks deliberate. */
        var embedded = FleetSweepPresentation.EmbedJson("not json at all");
        Assert.Equal("not json at all", embedded!.GetValue<string>());

        Assert.IsType<JsonObject>(FleetSweepPresentation.EmbedJson("{\"a\":1}"));
        Assert.IsType<JsonArray>(FleetSweepPresentation.EmbedJson("[1,2]"));
    }

    /// <summary>
    /// THE legacy-doc pin (#3478): every alerts-on sweep persisted before the engine's storage gate
    /// carries a fabricated <c>would_have_paged: []</c> — written unconditionally, never derived — and
    /// stored documents are immutable, living out their retention as-written. The run node strips the
    /// key from the EMBEDDED report on an alerts-on sweep, so the render contract (absent means no
    /// check was made) holds across the legacy store without a data migration — and against any future
    /// writer regression. Both surfaces and both read paths ride this one builder: the timeline embeds
    /// run nodes, and the sweep_id fetch's detail node wraps the same one.
    /// </summary>
    [Fact]
    public void TheRunNode_StripsTheFabricatedLedgerKey_FromALegacyAlertsOnDocument()
    {
        var legacy = Run(alertsEnabled: true) with
        {
            ReportJson = "{\"alerts_enabled\":true,\"would_have_paged\":[]}",
        };

        var node = FleetSweepPresentation.BuildRunNode(legacy);
        var report = Assert.IsType<JsonObject>(node["report"]);
        Assert.False(report.ContainsKey("would_have_paged"));

        /* The sweep_id fetch path serves the same stripped embed, and its top-level gate still holds —
           neither layer of an alerts-on detail claims the check. */
        var detail = FleetSweepPresentation.BuildSweepDetailNode(
            legacy, new List<FleetSweepServerVerdict>(), new List<FleetSweepWouldHavePagedEntry>());
        Assert.False(Assert.IsType<JsonObject>(detail["report"]).ContainsKey("would_have_paged"));
        Assert.False(detail.ContainsKey("would_have_paged"));

        /* The strip probes only a parsed OBJECT: an unparseable alerts-on payload still rides verbatim
           — the fail-toward-visible arm is not a casualty of the gate. */
        var unreadable = Run(alertsEnabled: true) with { ReportJson = "not json at all" };
        Assert.Equal("not json at all", FleetSweepPresentation.BuildRunNode(unreadable)["report"]!.GetValue<string>());
    }

    /// <summary>
    /// #3487's arm B, the #3478 render-rule shape one test up: the stored report document spells its
    /// two ids as JSON numbers — the engine's deliberate stored spelling (longs are exact .NET-side;
    /// the reasoning is on the record at its composition site) — and stored documents are immutable,
    /// so every persisted document carries numeric ids for its whole retention. The run node re-spells
    /// the two fields on the FRESHLY PARSED embed, never the stored row, so the embedded copy obeys
    /// the same string contract as the header fields beside it. The fixture's id is the live
    /// measurement's: the value below is exactly the one a JS consumer parsed to …800.
    /// </summary>
    [Fact]
    public void TheRunNode_RespellsTheEmbeddedDocumentsIds_AsStrings_OnTheParsedCopy()
    {
        var legacy = Run() with
        {
            ReportJson = "{\"sweep_id\":639251940075451830,\"previous_sweep_id\":639251903948800000,\"alerts_enabled\":true}",
        };

        var report = Assert.IsType<JsonObject>(FleetSweepPresentation.BuildRunNode(legacy)["report"]);
        Assert.Equal("639251940075451830", report["sweep_id"]!.GetValue<string>());
        Assert.Equal("639251903948800000", report["previous_sweep_id"]!.GetValue<string>());

        /* A first sweep's stored document carries previous_sweep_id: null — the re-spell probes only
           a NUMBER, so the null stays null beside its no_previous_sweep companion. */
        var first = Run() with
        {
            PreviousSweepId = null,
            ReportJson = "{\"sweep_id\":639251940075451830,\"previous_sweep_id\":null}",
        };
        var firstReport = Assert.IsType<JsonObject>(FleetSweepPresentation.BuildRunNode(first)["report"]);
        Assert.True(firstReport.ContainsKey("previous_sweep_id"));
        Assert.Null(firstReport["previous_sweep_id"]);

        /* An unparseable payload still rides verbatim — the re-spell probes only a parsed OBJECT,
           the same guard the #3478 strip stands behind. */
        var unreadable = Run() with { ReportJson = "not json at all" };
        Assert.Equal("not json at all", FleetSweepPresentation.BuildRunNode(unreadable)["report"]!.GetValue<string>());
    }

    /// <summary>The strip is alerts-on ONLY: a muted document's ledger key is the muted-mode contract's
    /// whole point and rides the embed untouched — including empty, because present-and-empty is the
    /// statement ("muted, and nothing would have paged"), not a shape accident.</summary>
    [Fact]
    public void TheRunNode_LeavesTheLedgerKey_OnAMutedDocument()
    {
        var muted = Run(alertsEnabled: false) with
        {
            ReportJson = "{\"alerts_enabled\":false,\"would_have_paged\":[]}",
        };

        var report = Assert.IsType<JsonObject>(FleetSweepPresentation.BuildRunNode(muted)["report"]);
        var rows = Assert.IsType<JsonArray>(report["would_have_paged"]);
        Assert.Empty(rows);
    }

    [Fact]
    public void TheDetailNode_UnderMasterOff_CarriesTheLedger_WithServerNamesJoined()
    {
        var verdicts = new List<FleetSweepServerVerdict>
        {
            new(7, "sql-a", "Critical", "Healthy", "deadlocks", "{\"deadlocks\":3}"),
        };
        var ledger = new List<FleetSweepWouldHavePagedEntry>
        {
            new(7, FleetSweepEngine.FamilyDeadlocks, "{\"value\":3,\"threshold\":1}"),
            new(9, FleetSweepEngine.FamilyHighCpu, "{\"value\":12,\"threshold\":8}"),
        };

        var node = FleetSweepPresentation.BuildSweepDetailNode(Run(alertsEnabled: false), verdicts, ledger);

        var rows = Assert.IsType<JsonArray>(node["would_have_paged"]);
        Assert.Equal(2, rows.Count);

        /* The name is joined from the verdicts the SAME sweep wrote — the ledger is the muted-mode
           contract's table, and a bare id would make it the one table an operator cannot read. */
        Assert.Equal("sql-a", (string?)rows[0]!["server"]);
        Assert.Equal(FleetSweepEngine.FamilyDeadlocks, (string?)rows[0]!["family"]);
        Assert.IsType<JsonObject>(rows[0]!["evidence"]);

        /* A server the verdict list does not carry keeps its id and an honest null name, never a
           fabricated one. */
        Assert.Equal(9, (int)rows[1]!["server_id"]!);
        Assert.Null(rows[1]!["server"]);
    }

    [Fact]
    public void TheDetailNode_UnderMasterOff_CarriesAnEMPTYLedger_BecauseNothingToReportIsAStatement()
    {
        var node = FleetSweepPresentation.BuildSweepDetailNode(
            Run(alertsEnabled: false), new List<FleetSweepServerVerdict>(), new List<FleetSweepWouldHavePagedEntry>());

        var rows = Assert.IsType<JsonArray>(node["would_have_paged"]);
        Assert.Empty(rows);
    }

    [Fact]
    public void TheDetailNode_WithAlertsOn_OmitsTheLedgerKey_BecauseNoCheckWasMade()
    {
        /* The engine writes no ledger under alerts-on; an empty array here would claim a check that
           was never made — the exact over-claim the derivation note in the evidence exists to stop. */
        var node = FleetSweepPresentation.BuildSweepDetailNode(
            Run(alertsEnabled: true), new List<FleetSweepServerVerdict>(), new List<FleetSweepWouldHavePagedEntry>());

        Assert.False(node.ContainsKey("would_have_paged"));
        Assert.True(node.ContainsKey("verdicts"));
    }

    [Theory]
    [InlineData(null, "Critical", false)]        // a first sweep has no previous band to change from
    [InlineData("Healthy", "Critical", true)]    // the transition the timeline exists to show
    [InlineData("Warning", "Warning", false)]    // held steady
    public void TheVerdictNode_PreComputesTheTransition(string? previous, string band, bool expectedChanged)
    {
        var node = FleetSweepPresentation.BuildVerdictNode(new FleetSweepServerVerdict(1, "s", band, previous, null, null));

        Assert.Equal(expectedChanged, (bool)node["band_changed"]!);
        Assert.Equal(band, (string?)node["band"]);
        Assert.Equal(previous, (string?)node["previous_band"]);
    }

    [Fact]
    public void TheWatchItemsNode_CarriesTheBars_NamesWhatItCan_AndNeverNamesTheFleetScope()
    {
        var now = new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);
        var items = new List<FleetSweepWatchItem>
        {
            new(FleetSweepStore.FleetScopeServerId, FleetSweepEngine.InstrumentsDeadItemKey, "cond",
                FleetSweepWatchStateMachine.Open, 2, 0, 1, 2, 2, null, now, now, "{\"sweep_id\":2}"),
            new(7, FleetSweepEngine.BandCriticalItemKey, "cond",
                FleetSweepWatchStateMachine.Carried, 0, 1, 1, 2, 1, null, now, now, null),
            new(9, FleetSweepEngine.BandCriticalItemKey, "cond",
                FleetSweepWatchStateMachine.Carried, 0, 1, 1, 2, 1, null, now, now, null),
        };

        /* The map deliberately carries the fleet-scope sentinel's id: no verdict row should, but a
           name for "the fleet" would be a fabrication however it reached the map, so the builder's
           guard — not the map's hygiene — is what the omission below rests on. */
        var names = new Dictionary<int, string>
        {
            [FleetSweepStore.FleetScopeServerId] = "a-name-that-must-not-render",
            [7] = "sql-a",
        };

        var node = FleetSweepPresentation.BuildWatchItemsNode(items, names);

        /* The bars ride the payload, spliced from the machine's own constants — "1 miss" only means
           something beside "of 2 to close", and a client that hardcoded 2 would silently mis-render
           the day per-item thresholds arrive as data (the #3297 route the store doc reserves). */
        Assert.Equal(FleetSweepWatchStateMachine.EntryConsecutiveSweeps, (int)node["entry_bar_sweeps"]!);
        Assert.Equal(FleetSweepWatchStateMachine.ExitConsecutiveSweeps, (int)node["exit_bar_sweeps"]!);

        var array = Assert.IsType<JsonArray>(node["items"]);

        /* The fleet-scope sentinel: flagged, and NO server field even with its id in the map — the
           fleet has no server name, and the fleet_scope flag is what a client renders from (#3482). */
        Assert.True((bool)array[0]!["fleet_scope"]!);   // the store's sentinel, named on the wire
        Assert.False(Assert.IsType<JsonObject>(array[0]).ContainsKey("server"));

        /* A member whose id resolves carries its display name — the field sweeps.js has preferred
           since lane 3, which the feed never sent (#3482: the worklist rendered raw ids). */
        Assert.False((bool)array[1]!["fleet_scope"]!);
        Assert.Equal("sql-a", (string?)array[1]!["server"]);

        /* A name the retained history no longer holds is OMITTED — not fabricated, not null-padded:
           the client's "server N" fallback is the honest degrade, exactly the pre-fix rendering. */
        Assert.Equal(9, (int)array[2]!["server_id"]!);
        Assert.False(Assert.IsType<JsonObject>(array[2]).ContainsKey("server"));

        Assert.IsType<JsonObject>(array[0]!["evidence"]);
        Assert.Null(array[1]!["evidence"]);             // a miss's null evidence stays null, not ""
        Assert.Equal(1, (int)array[1]!["consecutive_misses"]!);
    }

    /// <summary>The worklist's four sweep-id anchors ride as strings too (#3487) — they are exactly
    /// the ids an agent would feed back to the <c>sweep_id</c> fetch, and a rounded
    /// <c>opened_sweep_id</c> would send that drill-down to a sweep that never existed. The nullable
    /// arms hold: an item that has never opened or closed carries honest nulls, not spellings of
    /// null.</summary>
    [Fact]
    public void TheWatchItemsNode_SpellsItsSweepIdAnchors_AsJsonStrings()
    {
        var now = new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);
        var items = new List<FleetSweepWatchItem>
        {
            new(7, FleetSweepEngine.BandCriticalItemKey, "cond", FleetSweepWatchStateMachine.Open,
                2, 0, 638600000000000000, 638600036000000000, 638600036000000000, null, now, now, null),
        };

        var row = Assert.IsType<JsonArray>(
            FleetSweepPresentation.BuildWatchItemsNode(items, new Dictionary<int, string>())["items"])[0]!;

        Assert.Equal("638600000000000000", row["first_seen_sweep_id"]!.GetValue<string>());
        Assert.Equal("638600036000000000", row["last_seen_sweep_id"]!.GetValue<string>());
        Assert.Equal("638600036000000000", row["opened_sweep_id"]!.GetValue<string>());
        Assert.Null(row["closed_sweep_id"]);
    }

    [Fact]
    public void TheTimelineNode_EchoesTheWindowItServed()
    {
        var start = new DateTime(2026, 9, 15, 11, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);

        var node = FleetSweepPresentation.BuildTimelineNode(new List<FleetSweepRun> { Run() }, start, end);

        /* The #2506 discipline's read end: a response that does not say what window it covers invites
           the caller to assume the one it asked for. */
        Assert.Equal(start.ToString("o"), (string?)node["span_start"]);
        Assert.Equal(end.ToString("o"), (string?)node["span_end"]);
        Assert.Equal(1, (int)node["count"]!);
        Assert.Single(Assert.IsType<JsonArray>(node["sweeps"]));
    }

    /* ---- the frontend parity pins (the AlertTrayStatusLoggedTests idiom) ------------------------------- */

    /// <summary>
    /// The sweeps page's watch-state vocabulary is exactly the state machine's — key for key. JS
    /// cannot reference the C# constants, so this is the cross-language join, parsed strictly (a
    /// shape the parser cannot read fails loudly rather than comparing fewer keys).
    /// </summary>
    [Fact]
    public void TheSweepPagesWatchStateLabels_MatchTheStateMachinesConstants()
    {
        var labels = ParseObjectLiteral(FrontendSource("js/pages/sweeps.js"), "export const SWEEP_WATCH_STATE_LABELS = {");

        Assert.Equal(
            new[]
            {
                FleetSweepWatchStateMachine.Carried,
                FleetSweepWatchStateMachine.Closed,
                FleetSweepWatchStateMachine.Open,
                FleetSweepWatchStateMachine.Pending,
            }.OrderBy(s => s, StringComparer.Ordinal),
            labels.Keys.OrderBy(s => s, StringComparer.Ordinal));
    }

    /// <summary>
    /// Quiet-is-not-clean applies to the RENDER: a sweep with <c>instruments_alive: false</c> must
    /// LOOK different — a red <c>role="alert"</c> banner on the document and a distinct class on its
    /// timeline row — and the mute header must render the would-have-paged ledger whenever the
    /// document carries one. Source pins, because the branches are the page's load-bearing contract
    /// and there is no JS runner to execute them.
    /// </summary>
    [Fact]
    public void TheSweepPage_RendersDeadInstrumentsLoudly_AndTheLedgerWheneverCarried()
    {
        var src = FrontendSource("js/pages/sweeps.js");

        Assert.Contains("if (!d.instruments_alive)", src, StringComparison.Ordinal);
        Assert.Contains("instruments-dead", src, StringComparison.Ordinal);
        Assert.Contains("role: \"alert\"", src, StringComparison.Ordinal);

        Assert.Contains("if (!d.alerts_enabled)", src, StringComparison.Ordinal);
        Assert.Contains("Array.isArray(d.would_have_paged)", src, StringComparison.Ordinal);

        /* The hysteresis position renders against the bars the API carries, never a hardcoded 2. */
        Assert.Contains("entry_bar_sweeps", src, StringComparison.Ordinal);
        Assert.Contains("exit_bar_sweeps", src, StringComparison.Ordinal);
    }

    /// <summary>The worklist names its rows in the feed's preference order (#3482): the fleet-scope
    /// label for the sentinel FIRST (the fleet has no server name, so "server 0" would be the exact
    /// misleading rendering the issue is about), then the display name the feed joins from the
    /// retained verdict history, then the bare id as the honest degrade for a name the store no
    /// longer holds. Source pin, the file's frontend convention.</summary>
    [Fact]
    public void TheWorklistRender_PrefersTheName_AndNeverSaysServerZeroForTheFleet()
    {
        Assert.Contains(
            "w.fleet_scope ? \"Fleet\" : (w.server || \"server \" + w.server_id)",
            FrontendSource("js/pages/sweeps.js"),
            StringComparison.Ordinal);
    }

    /// <summary>The cadence knob is DISPLAY-ONLY on this page: it reads the same
    /// <c>get_alert_settings</c> every settings surface reads, and the page carries no write call at
    /// all — the Settings window and <c>update_alert_settings</c> own the knob (lane 2's V124).</summary>
    [Fact]
    public void TheSweepPage_ReadsTheCadence_AndWritesNothing()
    {
        var src = FrontendSource("js/pages/sweeps.js");

        Assert.Contains("readTool(\"get_alert_settings\"", src, StringComparison.Ordinal);
        Assert.Contains("fleet_sweep", src, StringComparison.Ordinal);
        Assert.DoesNotContain("apiSend", src, StringComparison.Ordinal);
        Assert.DoesNotContain("update_alert_settings", src, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Store host section (#4214 part 2): the sweeps page's own "is the STORE sized right" panel, below
    /// Watch items. Reads <c>get_store_host</c> through <c>/api/read</c> (never a raw fetch, never a second
    /// implementation of the verdict math #4214 part 1 already owns) and highlights every stale-* verdict —
    /// source pins, the file's own convention for a branch there is no JS runner to execute.
    /// </summary>
    [Fact]
    public void TheSweepPage_HasAStoreHostSection_AndHighlightsEveryStaleVerdict()
    {
        var src = FrontendSource("js/pages/sweeps.js");

        Assert.Contains("el(\"h3\", { class: \"section-title\", text: \"Store host\" })", src, StringComparison.Ordinal);
        Assert.Contains("readTool(\"get_store_host\", {})", src, StringComparison.Ordinal);

        /* Every stale-* verdict highlighted, not just the exact stale_after_hardware_change string — a
           future fifth verdict named "stale_something_else" must not silently fall through unhighlighted. */
        Assert.Contains("v.startsWith(\"stale\")", src, StringComparison.Ordinal);
    }

    /// <summary>The page is reachable: the shell carries the nav entry and the router routes the hash
    /// to the renderer — the two wiring points a new page can silently miss.</summary>
    [Fact]
    public void TheSweepPage_IsWiredIntoTheShellAndTheRouter()
    {
        Assert.Contains("data-route=\"sweeps\" href=\"#/sweeps\"", FrontendSource("index.html"), StringComparison.Ordinal);

        var app = FrontendSource("js/app.js");
        Assert.Contains("import { renderSweeps } from \"./pages/sweeps.js\";", app, StringComparison.Ordinal);
        /* renderSweeps(main, opts) (#4214 round-1 review), not renderSweeps(main) — opts carries the poll
           tick's { poll: true } through to the store host card, so it replays instead of re-fetching. */
        Assert.Contains("renderSweeps(main, opts)", app, StringComparison.Ordinal);
        Assert.Contains("#/sweeps", app, StringComparison.Ordinal);
    }

    /* ---- the logger seat: the service log, not the provider-less app factory ---------------------------- */

    /// <summary>
    /// The web host clears the dashboard app's logging providers (deliberate — framework noise has no
    /// seat in the service log), which makes <c>app.Logger</c> a logger with nowhere to write. These
    /// routes' log-and-degrade store reads used to log through it, so every web-path degradation line
    /// went nowhere — the same gap class the MCP host closed for <c>get_sweep_reports</c> with
    /// <c>AddSingleton&lt;ILogger&gt;(_logger)</c> (#3473 review). The fix is one thread: the host
    /// hands <c>_logger</c> to <c>MapAll</c> beside the providers it cleared (two halves of one
    /// decision, stated at the ClearProviders site), the sweep wiring takes that seat, and nothing in
    /// the endpoint file reaches for <c>app.Logger</c> again — a fresh handler that did would compile
    /// and silently regress, which is exactly what this source pin exists to catch.
    /// </summary>
    [Fact]
    public void TheSweepRoutes_LogThroughTheServiceLogger_NotTheProviderlessAppFactory()
    {
        var endpoints = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingFleetSweepEndpoints.cs");

        /* The seat: the wiring receives the service logger by parameter… */
        Assert.Contains(
            "internal static void Map(WebApplication app, NpgsqlDataSource postgres, ILogger logger)",
            endpoints, StringComparison.Ordinal);

        /* …and no handler in the file writes through the app's cleared factory. Scanned as CODE
           (comments and literals stripped): the doc prose above the wiring deliberately names
           app.Logger to teach the rule, and prose must stay free to do that while code cannot
           regress — the CSharpSourceWalker separation every code-shape pin in this project rides. */
        Assert.DoesNotContain("app.Logger", CSharpSourceWalker.StripCommentsAndStrings(endpoints), StringComparison.Ordinal);

        /* The host's half of the design: providers cleared AND its own logger handed to the seats. */
        var host = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingWebHostService.cs");
        Assert.Contains("builder.Logging.ClearProviders();", host, StringComparison.Ordinal);
        Assert.Contains(
            "DarlingWebEndpoints.MapAll(app, postgres, _collectorState, _logger, _baselineCache, postgresConfig, _readLatency);",
            host, StringComparison.Ordinal);
    }

    /* ---- #4283: the two sweep GETs carry no local catch; the #4281 backstop answers instead ------------- */

    /// <summary>
    /// #4283 supersedes #4286 round-1 review, Low 3: rather than <c>/api/sweeps/latest</c> and
    /// <c>/api/sweeps/{id}</c> each carrying its own <c>DarlingWebFailureLog</c> catch, #4283 removes every
    /// route's local catch project-wide, so the #4281 top-of-pipeline backstop
    /// (<c>ReadDispatchCatch_AnswersTheRuledBodyAndStatus_NotFormatError</c> in
    /// <see cref="DarlingWebFailureHandlingTests"/> pins the dispatcher's twin of this same backstop) answers
    /// every unhandled throw from these two GETs the same ruled way. Neither handler may reach for
    /// <c>ex.Message</c> on its own, because neither has an <c>ex</c> to reach for. Source pin, not a
    /// TestServer test: producing a genuine store fault needs a broken NpgsqlDataSource this file otherwise
    /// never builds.
    /// </summary>
    [Fact]
    public void TheSweepReads_HaveNoLocalCatch_NotExMessage()
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingFleetSweepEndpoints.cs");

        AssertNoLocalCatch(raw, "/api/sweeps", "\"/api/sweeps\"");
        AssertNoLocalCatch(raw, "/api/sweeps/latest", "\"/api/sweeps/latest\"");
        AssertNoLocalCatch(raw, "/api/sweeps/{id:long}", "\"/api/sweeps/{id:long}\"");
        AssertNoLocalCatch(raw, "/api/sweeps/watch-items", "\"/api/sweeps/watch-items\"");
    }

    /// <summary>Slices ONE MapGet's body out of the endpoint file (from its route literal to the next
    /// MapGet, or end of file for the last one) and asserts it has no local catch and never writes
    /// <c>ex.Message</c> to the wire -- the shape #4283 leaves so the #4281 backstop is the only thing that
    /// answers an unhandled throw here. Boundaries are found in RAW source, because the route literal itself
    /// is plain string CONTENT that <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> blanks; the
    /// slice is then stripped before the content assertions, so a doc comment mentioning "catch" or
    /// "ex.Message" (this file's own review-note comments do) cannot produce a false failure.</summary>
    private static void AssertNoLocalCatch(string raw, string routeForMessage, string routeLiteral)
    {
        var start = raw.IndexOf(routeLiteral, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{routeForMessage}'s MapGet was not found; this pin is reading nothing.");

        var nextMapGet = raw.IndexOf("app.MapGet(", start + routeLiteral.Length, StringComparison.Ordinal);
        var end = nextMapGet >= 0 ? nextMapGet : raw.Length;
        var body = CSharpSourceWalker.StripCommentsAndStrings(raw[start..end]);

        Assert.DoesNotContain("catch", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ex.Message", body, StringComparison.Ordinal);
    }

    /* ---- helpers --------------------------------------------------------------------------------------- */

    /// <summary>Parses a flat <c>key: "value",</c> object literal out of frontend source — the
    /// <see cref="AlertTrayStatusLoggedTests"/> parser's strictness: any line inside the literal this
    /// cannot read fails the test, so the parity check can never silently compare fewer keys.</summary>
    private static Dictionary<string, string> ParseObjectLiteral(string source, string opener)
    {
        var open = source.IndexOf(opener, StringComparison.Ordinal);
        Assert.True(open >= 0, "object literal not found: " + opener);

        var close = source.IndexOf("};", open, StringComparison.Ordinal);
        Assert.True(close > open, "object literal is not closed: " + opener);

        var body = source.Substring(open, close - open);
        body = body.Substring(body.IndexOf('{') + 1);

        var pairs = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var rawLine in body.Split('\n'))
        {
            var line = rawLine.Trim().TrimEnd('\r');
            if (line.Length == 0)
            {
                continue;
            }

            var match = Regex.Match(line, @"^""?(?<key>[A-Za-z0-9_+]+)""?\s*:\s*""(?<label>[^""]*)"",$");
            Assert.True(match.Success, "unreadable line in " + opener + " — the parity check would compare fewer keys: " + line);
            pairs[match.Groups["key"].Value] = match.Groups["label"].Value;
        }

        Assert.NotEmpty(pairs);
        return pairs;
    }

    private static string FrontendSource(string relPath, [CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Service", "wwwroot", relPath));
        Assert.True(File.Exists(path), $"{relPath} not found at {path} (did the frontend move?)");
        return File.ReadAllText(path);
    }
}
