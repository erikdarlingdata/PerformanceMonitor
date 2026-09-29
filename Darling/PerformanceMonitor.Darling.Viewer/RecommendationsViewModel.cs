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
using System.Text;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Canonical three-band severity for the viewer's Recommendations surface — the viewer-local
/// mirror of Lite's <c>LiteRecommendationSeverity</c> (the viewer reads only the engine producer;
/// there is no legacy critical_issues store here). The engine scores findings on a
/// <c>double</c> (0-~2.0); that scale maps onto this enum so one list sorts and renders
/// consistently. The ordinals are deliberately Critical &gt; Warning &gt; Info so a descending
/// enum sort matches a descending severity sort.
/// </summary>
public enum RecommendationSeverity
{
    Info = 0,
    Warning = 1,
    Critical = 2
}

/// <summary>
/// Which top-level state the Recommendations surface is in. The tab swaps a single visible region
/// per state. Mirrors Lite's <c>LiteRecommendationsState</c>, including <see cref="InsufficientData"/>:
/// Lite reaches that state through its in-app engine call (which reads back the insufficient-data
/// determination), whereas the Darling service runs analysis on its own cadence and PERSISTS the
/// determination to the V19 <c>analysis_state</c> marker (<c>DarlingObservability.WriteAnalysisStateAsync</c>).
/// The viewer reads that marker alongside the findings, so a zero-finding read on a young deployment
/// (the engine skipped for want of its 24h history) renders <see cref="InsufficientData"/> — "still
/// collecting" — instead of a false <see cref="Empty"/> all-clear.
/// </summary>
public enum RecommendationsState
{
    /// <summary>A read is in flight.</summary>
    Loading,

    /// <summary>
    /// The read produced zero recommendations AND the persisted analysis-state marker says the engine
    /// has not yet cleared its minimum-history window (the 24h data-span gate) — recommendations are not
    /// meaningful yet, so "still collecting" is shown rather than a false all-clear.
    /// </summary>
    InsufficientData,

    /// <summary>
    /// The read produced zero recommendations AND the persisted marker records a window-empty pass
    /// (#3524/#3551): the span gate passed on lifetime history but the analysis window itself collected
    /// zero facts — a dead collector or an unreachable target, not a healthy server, so a distinct
    /// "collection appears broken" notice is shown, never the all-clear.
    /// </summary>
    WindowEmpty,

    /// <summary>The read completed and produced zero recommendations — the all-clear.</summary>
    Empty,

    /// <summary>The read completed with one or more recommendations to render.</summary>
    Loaded
}

/// <summary>
/// A single advise-only recommendation row — the viewer-local mirror of Lite's
/// <see cref="PerformanceMonitorLite.Analysis.Recommendations.LiteRecommendationItem"/> shape
/// (referenced here only in prose; the viewer does not depend on the Lite assembly). A plain DTO
/// (no WPF dependency) built from a persisted <see cref="AnalysisFinding"/> via a
/// <see cref="ViewerFindingRow"/>.
///
/// <para>
/// The viewer is ADVISE-ONLY (Postgres reads only; SQL-side remediation is Dashboard-only per
/// project scope). A card offers the diagnosis, copy-paste T-SQL when the persisted remediation
/// action carries one, and an "Ask AI" MCP prompt — never an in-app Apply, and (mirroring the
/// re-skin brief) no mute affordance (mute lives on the Alert History surface).
/// </para>
/// </summary>
public sealed class RecommendationItem
{
    /// <summary>The three-band severity used for sorting and the badge glyph/colour.</summary>
    public RecommendationSeverity Severity { get; init; }

    /// <summary>The raw engine severity (0-~2.0), the secondary sort within a band.</summary>
    public double RawSeverity { get; init; }

    /// <summary>The affected database, or null for a server-scoped finding.</summary>
    public string? Database { get; init; }

    /// <summary>The one-line card heading.</summary>
    public string Title { get; init; } = string.Empty;

    /// <summary>
    /// Operator-facing advice prose, composed from <see cref="FactAdvice.GetComposedForFinding"/>
    /// (remediation line + investigation line). Settable so the co-fired cross-reference can be
    /// appended after mapping (mirrors Lite's reader).
    /// </summary>
    public string? AdviceText { get; set; }

    /// <summary>
    /// Copy-paste-ready T-SQL derived from the finding's PERSISTED
    /// <see cref="AnalysisFinding.Remediation"/> action — the same read-back source the Dashboard's
    /// RecommendationsReader uses (BuildCopyPasteFromAction). Null when the action carries no
    /// copy-paste target. NOTE: the viewer reads persisted findings, whose ephemeral drill-down is
    /// NOT stored, so <see cref="FactRemediation.GenerateForFinding"/> (Lite's live-path source)
    /// returns null here; the persisted action is the authoritative copy-paste source on read.
    /// </summary>
    public string? CopyPasteSql { get; init; }

    /// <summary>
    /// The finding's PERSISTED remediation action (or null) — the same object <see cref="CopyPasteSql"/>
    /// is derived from. Carried so the card can reconstruct the incident-vs-config-fix distinction the
    /// Dashboard draws from its RecommendationSetting taxonomy (<c>RecommendationsViewModel.IsIncident</c>).
    /// The viewer reads only engine findings (there is no legacy critical_issues RecommendationSetting
    /// store here), so a structured standing-config action — DB_CONFIG / RCSI / FILE_AUTOGROWTH_PERCENT /
    /// SERVER_CONFIG — is the only signal that a finding is a config fix rather than a time-bound incident,
    /// and it gates whether the "Open in Active Queries" deep-link is offered.
    /// </summary>
    public RemediationAction? Remediation { get; init; }

    /// <summary>
    /// The finding's incident id — the group key the surface collapses cards under so related
    /// findings render as one report. Empty for findings analyzed before incident_id existed; the
    /// view-model treats an empty id as a standalone single-card incident.
    /// </summary>
    public string IncidentId { get; init; } = string.Empty;

    /// <summary>The monitored server's display name (for the Ask-AI prompt).</summary>
    public string ServerName { get; init; } = string.Empty;

    /// <summary>
    /// The finding's monitored-server id (<see cref="AnalysisFinding.ServerId"/>) — drives which
    /// per-server tab the "Open in Active Queries" deep-link opens. A real server id is never 0, so the
    /// deep-link predicate treats 0 as "no server anchor".
    /// </summary>
    public int ServerId { get; init; }

    /// <summary>UTC start of the finding's time window (analysis TimeRangeStart), or null.</summary>
    public DateTime? WindowStartUtc { get; init; }

    /// <summary>UTC end of the finding's time window (analysis TimeRangeEnd), or null.</summary>
    public DateTime? WindowEndUtc { get; init; }
}

/// <summary>
/// A single recommendation rendered as a card. A plain DTO wrapping a <see cref="RecommendationItem"/>
/// plus the pre-computed display/visibility flags the XAML binds to, so the affordance model and the
/// Ask-AI prompt are unit-testable. Mirrors Lite's <c>LiteRecommendationCardViewModel</c>: ADVISE-ONLY
/// (no Apply, no mute); every card offers "Ask AI", and cards whose persisted action carries a
/// copy-paste statement also offer "Copy fix".
/// </summary>
public sealed class RecommendationCardViewModel
{
    private readonly int _utcOffsetMinutes;

    public RecommendationCardViewModel(RecommendationItem item, int utcOffsetMinutes = 0)
    {
        Item = item ?? throw new ArgumentNullException(nameof(item));
        _utcOffsetMinutes = utcOffsetMinutes;
    }

    /// <summary>The underlying advise-only recommendation row.</summary>
    public RecommendationItem Item { get; }

    /// <summary>The three-band severity (drives the badge glyph + colour).</summary>
    public RecommendationSeverity Severity => Item.Severity;

    /// <summary>Short uppercase severity label for the badge, e.g. "CRITICAL".</summary>
    public string SeverityLabel => Item.Severity switch
    {
        RecommendationSeverity.Critical => "CRITICAL",
        RecommendationSeverity.Warning => "WARNING",
        _ => "INFO"
    };

    /// <summary>A glyph for the badge (Segoe MDL2 Assets code point) per severity — Lite's glyphs.</summary>
    public string SeverityGlyph => Item.Severity switch
    {
        RecommendationSeverity.Critical => "", // error / critical
        RecommendationSeverity.Warning => "",  // warning triangle
        _ => ""                                 // info
    };

    /// <summary>The card heading.</summary>
    public string Title => Item.Title;

    /// <summary>
    /// The affected database wrapped in brackets for display, or empty for a server-scoped finding
    /// (so the database line collapses).
    /// </summary>
    public string DatabaseBracketed =>
        string.IsNullOrEmpty(Item.Database) ? string.Empty : $"[{Item.Database}]";

    /// <summary>Whether a database line should be shown at all.</summary>
    public bool HasDatabase => !string.IsNullOrEmpty(Item.Database);

    /// <summary>The operator-facing advice prose.</summary>
    public string? AdviceText => Item.AdviceText;

    /// <summary>Whether there is advice prose to render.</summary>
    public bool HasAdvice => !string.IsNullOrEmpty(Item.AdviceText);

    /// <summary>The copy-paste-ready fix T-SQL, if any.</summary>
    public string? CopyPasteSql => Item.CopyPasteSql;

    /// <summary>Whether "Copy fix" is shown — only when a copy-paste statement exists.</summary>
    public bool ShowCopyFix => !string.IsNullOrEmpty(Item.CopyPasteSql);

    /// <summary>
    /// Whether "Ask AI" is shown. Shown for every card — the Darling service exposes the same MCP
    /// investigation tools the prompt references, so any finding can be handed to AI.
    /// </summary>
    public bool ShowAskAi => true;

    // ---- "Open in Active Queries" deep-link (aggregate → per-server tab) ---------------

    /// <summary>The finding's monitored-server id, so the shell opens the right per-server tab.</summary>
    public int ServerId => Item.ServerId;

    /// <summary>Raw UTC start of the finding window (drives the deep-link read), or null.</summary>
    public DateTime? WindowStartUtc => Item.WindowStartUtc;

    /// <summary>Raw UTC end of the finding window (drives the deep-link read), or null.</summary>
    public DateTime? WindowEndUtc => Item.WindowEndUtc;

    /// <summary>
    /// Whether this row carries a STRUCTURED standing config-fix action — a persisted
    /// <see cref="RemediationAction"/> whose typed per-target lists map to a database- or server-level
    /// configuration change: DB_CONFIG safe settings (<see cref="RemediationAction.DbConfigTargets"/>),
    /// per-database RCSI (<see cref="RemediationAction.RcsiTargets"/> — a DB_CONFIG action can carry ONLY
    /// these, so they must be checked), percent-autogrowth files
    /// (<see cref="RemediationAction.FileGrowthTargets"/>), or server config MAXDOP/CTFP/memory
    /// (<see cref="RemediationAction.ServerConfigTargets"/>). Mirrors the Dashboard card's
    /// <c>HasStructuredFixAction</c>. Plan-regression (force-plan), clear-plan, and MISSING_INDEX actions
    /// are deliberately NOT structured config fixes — their target lists are empty here — so they remain
    /// incidents, matching the Dashboard (where MISSING_INDEX keeps the incident affordances).
    /// </summary>
    private bool HasStructuredFixAction =>
        Item.Remediation is { } r &&
        ((r.DbConfigTargets is { Count: > 0 }) ||
         (r.RcsiTargets is { Count: > 0 }) ||
         (r.FileGrowthTargets is { Count: > 0 }) ||
         (r.ServerConfigTargets is { Count: > 0 }));

    /// <summary>
    /// Whether this is a standing CONFIG-FIX finding (a structured config action) rather than a time-bound
    /// incident. The viewer reads only engine findings (no legacy critical_issues RecommendationSetting
    /// store), so — unlike the Dashboard, which also flags a config fix via a non-None Setting — the
    /// structured action is the only config-fix signal available here.
    /// </summary>
    public bool IsConfigFix => HasStructuredFixAction;

    /// <summary>
    /// Whether this is a time-bound INCIDENT finding (CPU/memory/blocking/waits/plan-regression/
    /// missing-index) — anything that is NOT a structured standing config fix. Mirrors the Dashboard's
    /// <c>IsIncident</c> (<c>Setting == None &amp;&amp; !HasStructuredFixAction</c>), reduced to the
    /// <c>!HasStructuredFixAction</c> half the viewer can observe. Incidents get the deep-link to Active
    /// Queries; config fixes do not.
    /// </summary>
    public bool IsIncident => !HasStructuredFixAction;

    /// <summary>
    /// Whether the "Open in Active Queries" deep-link is shown — an INCIDENT-type finding (not a standing
    /// config fix) that ALSO carries BOTH a time window (to scope the Active Queries read to when it fired)
    /// AND a server id (to open the right per-server tab). This is the Dashboard's incident affordance
    /// (<c>ShowOpenInActiveQueries => IsIncident</c>) ported to the viewer's advise-only model: a config-fix
    /// finding (e.g. AUTO_SHRINK, RCSI, autogrowth, MAXDOP) hides the deep-link even when it carries a
    /// window+server, because sending the operator to Active Queries makes no sense for a standing
    /// misconfiguration; a finding with no time window (or no server id) also hides it, because deep-linking
    /// needs both.
    /// </summary>
    public bool ShowOpenInActiveQueries =>
        IsIncident && Item.ServerId != 0 && Item.WindowStartUtc.HasValue && Item.WindowEndUtc.HasValue;

    /// <summary>
    /// The UTC window the deep-link scopes Active Queries to: the finding's own window when it carries a
    /// real range, widened to +/-<see cref="ViewerServerTab.DrillDownHalfWindowMinutes"/> around the point
    /// for a degenerate (start &gt;= end) window so the read is never empty. Reuses #1409's drill
    /// half-window for consistency with the chart/heatmap drills. Only meaningful when
    /// <see cref="ShowOpenInActiveQueries"/> is true; falls back to a "now"-anchored band otherwise.
    /// </summary>
    public (DateTime FromUtc, DateTime ToUtc) DeepLinkWindowUtc()
    {
        var from = Item.WindowStartUtc ?? DateTime.UtcNow;
        var to = Item.WindowEndUtc ?? from;
        if (to <= from)
        {
            from = from.AddMinutes(-ViewerServerTab.DrillDownHalfWindowMinutes);
            to = to.AddMinutes(ViewerServerTab.DrillDownHalfWindowMinutes);
        }

        return (from, to);
    }

    /// <summary>
    /// The MCP investigation prompt copied to the clipboard by "Ask AI". The window is rendered in
    /// the viewer machine's local time (UTC window + offset) for operator legibility.
    /// </summary>
    public string AskAiPrompt
    {
        get
        {
            var (from, to) = LocalWindow();
            return RecommendationsViewModel.BuildAskAiPrompt(Item.ServerName, Title, from, to);
        }
    }

    /// <summary>
    /// The finding window converted to local time via the passed offset, with a sensible fallback
    /// when the producer carried no window (a 2h band ending "now"). Tests pass offset 0 (UTC) for
    /// determinism; the tab passes the viewer machine's local offset.
    /// </summary>
    private (DateTime From, DateTime To) LocalWindow()
    {
        if (Item.WindowStartUtc is { } su && Item.WindowEndUtc is { } eu)
            return (su.AddMinutes(_utcOffsetMinutes), eu.AddMinutes(_utcOffsetMinutes));

        var now = DateTime.UtcNow.AddMinutes(_utcOffsetMinutes);
        return (now.AddHours(-2), now);
    }
}

/// <summary>
/// An incident group of cards, rendered as a collapsible section. Plain DTO so the grouping is
/// unit-testable. Mirrors Lite's <c>LiteRecommendationSectionViewModel</c>.
/// </summary>
public sealed class RecommendationSectionViewModel
{
    public RecommendationSectionViewModel(
        RecommendationSeverity severity, IReadOnlyList<RecommendationCardViewModel> cards, bool expanded,
        string header)
    {
        Severity = severity;
        Cards = cards ?? throw new ArgumentNullException(nameof(cards));
        IsExpanded = expanded;
        Header = header;
    }

    /// <summary>The severity of the section's primary (highest) finding.</summary>
    public RecommendationSeverity Severity { get; }

    /// <summary>The cards in this section, in the reader's severity-desc order.</summary>
    public IReadOnlyList<RecommendationCardViewModel> Cards { get; }

    /// <summary>How many cards the section holds.</summary>
    public int Count => Cards.Count;

    /// <summary>Whether the section's expander starts expanded.</summary>
    public bool IsExpanded { get; }

    /// <summary>The header label: primary finding + count (when &gt; 1) + severity.</summary>
    public string Header { get; }
}

/// <summary>
/// The pure, WPF-free core of the viewer's Recommendations surface. Maps the persisted
/// <see cref="ViewerFindingRow"/> list (already severity-sorted by
/// <see cref="ViewerDataService.MapFindings"/>) to advise-only items, appends the co-fired
/// cross-reference, and groups by INCIDENT into the collapsible sections the tab renders. Keeping
/// the mapping + grouping here (rather than in code-behind) makes it directly unit-testable.
/// Mirrors Lite's <c>LiteRecommendationsReader</c> (per-finding mapping) + <c>LiteRecommendationsViewModel</c>
/// (grouping), reconciled to the viewer's persisted-finding read.
///
/// <para>
/// This is a snapshot view-model: each load builds a fresh instance and the tab reassigns its bound
/// collections, so there is no <c>INotifyPropertyChanged</c> surface to reason about.
/// </para>
/// </summary>
public sealed class RecommendationsViewModel
{
    /// <summary>The incident sections, in severity order, or empty.</summary>
    public IReadOnlyList<RecommendationSectionViewModel> Sections { get; }

    /// <summary>The selected top-level state.</summary>
    public RecommendationsState State { get; }

    /// <summary>
    /// The message shown in the <see cref="RecommendationsState.InsufficientData"/> state — the engine's
    /// own persisted message, or <see cref="DefaultInsufficientDataMessage"/> when it supplied none. Empty
    /// in every other state.
    /// </summary>
    public string InsufficientDataMessage { get; }

    /// <summary>
    /// The message shown in the <see cref="RecommendationsState.WindowEmpty"/> state — the engine's own
    /// persisted message (or <see cref="DefaultWindowEmptyMessage"/> when it supplied none), always
    /// suffixed with the Collection Health pointer. Empty in every other state.
    /// </summary>
    public string WindowEmptyMessage { get; }

    /// <summary>Total card count across all sections.</summary>
    public int TotalCount => Sections.Sum(s => s.Count);

    private RecommendationsViewModel(
        IReadOnlyList<RecommendationSectionViewModel> sections, RecommendationsState state, string insufficientDataMessage,
        string windowEmptyMessage = "")
    {
        Sections = sections;
        State = state;
        InsufficientDataMessage = insufficientDataMessage;
        WindowEmptyMessage = windowEmptyMessage;
    }

    /// <summary>The default insufficient-data prose when the engine supplied no message (mirrors Lite's).</summary>
    public const string DefaultInsufficientDataMessage =
        "Still collecting — the engine needs about 24 hours of history before recommendations are " +
        "meaningful. Keep the collector running and check back later.";

    /// <summary>Builds the loading-state view-model (no data yet, read in flight).</summary>
    public static RecommendationsViewModel Loading() =>
        new(Array.Empty<RecommendationSectionViewModel>(), RecommendationsState.Loading, string.Empty);

    /// <summary>
    /// Builds the insufficient-data-state view-model from the persisted analysis-state marker's message
    /// (or the default when it is null/blank) — the viewer's mirror of Lite's
    /// <c>LiteRecommendationsViewModel.InsufficientData</c>, sourced from the V19 marker the Darling
    /// service writes rather than a live engine call.
    /// </summary>
    public static RecommendationsViewModel InsufficientData(string? message) =>
        new(
            Array.Empty<RecommendationSectionViewModel>(),
            RecommendationsState.InsufficientData,
            string.IsNullOrWhiteSpace(message) ? DefaultInsufficientDataMessage : message!);

    /// <summary>The default window-empty prose when the marker carried no message (mirrors Lite's).</summary>
    public const string DefaultWindowEmptyMessage =
        "Nothing was collected in this analysis window, so nothing was measured — this is not an all-clear.";

    /// <summary>
    /// Appended to every window-empty message so the operator lands on the surface that diagnoses a
    /// dead collector — the viewer's rendering of the same pointer the MCP <c>analyze_server</c> tool
    /// appends (<c>get_collection_health</c> there, the in-app tab here). Mirrors Lite's.
    /// </summary>
    public const string WindowEmptyCollectionHealthPointer =
        "Check the Collection Health tab to see when collectors last succeeded.";

    /// <summary>
    /// Builds the window-empty-state view-model (#3524/#3551) from the persisted marker's message (or
    /// the default when it is null/blank), suffixed with the Collection Health pointer — the viewer's
    /// mirror of Lite's <c>LiteRecommendationsViewModel.WindowEmpty</c>, sourced from the V19 marker's
    /// window-empty shape (<see cref="AnalysisStateMarker.WindowEmpty"/>) rather than a live engine call.
    /// </summary>
    public static RecommendationsViewModel WindowEmpty(string? message) =>
        new(
            Array.Empty<RecommendationSectionViewModel>(),
            RecommendationsState.WindowEmpty,
            string.Empty,
            (string.IsNullOrWhiteSpace(message) ? DefaultWindowEmptyMessage : message!) +
            " " + WindowEmptyCollectionHealthPointer);

    /// <summary>
    /// Builds a loaded/empty/insufficient-data/window-empty view-model from the persisted finding rows
    /// and the per-server analysis-state marker. Maps each row to an advise-only item, appends the co-fired
    /// cross-reference, and groups by incident. State selection:
    /// <list type="bullet">
    /// <item>one or more findings -> <see cref="RecommendationsState.Loaded"/> (findings always win);</item>
    /// <item>zero findings AND <paramref name="insufficientData"/> (the persisted marker says the engine
    /// has not cleared its 24h data-span gate) -> <see cref="RecommendationsState.InsufficientData"/>
    /// ("still collecting");</item>
    /// <item>zero findings AND <paramref name="windowEmpty"/> (the marker records a window-empty pass,
    /// #3524/#3551) -> <see cref="RecommendationsState.WindowEmpty"/> ("collection appears broken");</item>
    /// <item>zero findings and neither marker -> <see cref="RecommendationsState.Empty"/>
    /// (the genuine all-clear — enough data, facts measured, nothing to report).</item>
    /// </list>
    /// <paramref name="utcOffsetMinutes"/> is carried onto each card for the Ask-AI prompt's window. The
    /// rows arrive pre-sorted (severity band desc, raw desc, database, title) from the read, and grouping
    /// preserves that order. <paramref name="insufficientData"/> and <paramref name="windowEmpty"/> default
    /// false so the callers that carry no marker keep the prior loaded/empty behavior.
    /// </summary>
    public static RecommendationsViewModel FromFindings(
        IReadOnlyList<ViewerFindingRow> rows, string serverName, int utcOffsetMinutes = 0,
        bool insufficientData = false, string? insufficientDataMessage = null,
        bool windowEmpty = false, string? windowEmptyMessage = null)
    {
        if (rows is null || rows.Count == 0)
            return ZeroFindingState(insufficientData, insufficientDataMessage, windowEmpty, windowEmptyMessage);

        var items = new List<RecommendationItem>(rows.Count);
        foreach (var row in rows)
        {
            if (row is null)
                continue;
            items.Add(MapItem(row, serverName));
        }

        if (items.Count == 0)
            return ZeroFindingState(insufficientData, insufficientDataMessage, windowEmpty, windowEmptyMessage);

        AppendCoFired(items);
        return new(GroupByIncident(items, utcOffsetMinutes), RecommendationsState.Loaded, string.Empty);
    }

    /// <summary>
    /// Picks the state for a zero-finding read: <see cref="RecommendationsState.InsufficientData"/> when
    /// the persisted marker says the analysis pass has not cleared the 24h data-span gate (so the tab
    /// shows "still collecting" rather than a false all-clear), <see cref="RecommendationsState.WindowEmpty"/>
    /// when it records a window-empty pass instead (#3524/#3551 — "collection appears broken", also never
    /// the all-clear; the two marker shapes are mutually exclusive at the writer, and insufficient-data is
    /// checked first defensively), else <see cref="RecommendationsState.Empty"/> (a genuine all-clear).
    /// </summary>
    private static RecommendationsViewModel ZeroFindingState(
        bool insufficientData, string? insufficientMessage, bool windowEmpty, string? windowEmptyMessage)
    {
        if (insufficientData)
            return InsufficientData(insufficientMessage);
        if (windowEmpty)
            return WindowEmpty(windowEmptyMessage);
        return new(Array.Empty<RecommendationSectionViewModel>(), RecommendationsState.Empty, string.Empty);
    }

    /// <summary>
    /// Maps one persisted finding row to an advise-only <see cref="RecommendationItem"/>. Reuses the
    /// row's already-computed Title / RawSeverity / DatabaseName (so the card banding and title match
    /// the grid mapping exactly), and derives the advice prose + copy-paste SQL + incident/window from
    /// the carried <see cref="AnalysisFinding"/>. Advice comes from <see cref="FactAdvice"/> for the
    /// root fact key; copy-paste SQL comes from the PERSISTED remediation action
    /// (<see cref="BuildCopyPasteSql"/>). Exposed <c>internal</c> for tests.
    /// </summary>
    internal static RecommendationItem MapItem(ViewerFindingRow row, string serverName)
    {
        var finding = row.Finding;
        var advice = FactAdvice.GetComposedForFinding(finding);

        return new RecommendationItem
        {
            Severity = ToSeverity(row.RawSeverity),
            RawSeverity = row.RawSeverity,
            Database = string.IsNullOrEmpty(row.DatabaseName) ? null : row.DatabaseName,
            Title = row.Title,
            AdviceText = ComposeAdvice(advice),
            CopyPasteSql = BuildCopyPasteSql(finding.Remediation),
            Remediation = finding.Remediation,
            IncidentId = finding.IncidentId,
            ServerName = serverName ?? string.Empty,
            ServerId = finding.ServerId,
            WindowStartUtc = AsUtc(finding.TimeRangeStart),
            WindowEndUtc = AsUtc(finding.TimeRangeEnd)
        };
    }

    /// <summary>
    /// Maps an engine finding's <c>double</c> severity onto the canonical band with the SAME cutoffs
    /// both apps' recommendation readers use (and <see cref="ViewerDataService.SeverityBand"/>):
    /// &gt;= 1.5 Critical, &gt;= 0.75 Warning, else Info.
    /// </summary>
    internal static RecommendationSeverity ToSeverity(double severity)
    {
        if (severity >= 1.5)
            return RecommendationSeverity.Critical;
        if (severity >= 0.75)
            return RecommendationSeverity.Warning;
        return RecommendationSeverity.Info;
    }

    /// <summary>
    /// Composes the advice prose from an <see cref="AdviceBlock"/>: the remediation line, with the
    /// investigation line appended when present. Null when no advice matched the root fact key.
    /// Mirrors Lite's / the Dashboard's ComposeEngineAdvice.
    /// </summary>
    internal static string? ComposeAdvice(AdviceBlock? advice)
    {
        if (advice is null)
            return null;

        var sb = new StringBuilder();
        if (!string.IsNullOrEmpty(advice.Remediation))
            sb.Append(advice.Remediation);
        if (!string.IsNullOrEmpty(advice.Investigation))
        {
            if (sb.Length > 0)
                sb.Append(' ');
            sb.Append(advice.Investigation);
        }

        return sb.Length == 0 ? null : sb.ToString();
    }

    /// <summary>
    /// Rebuilds the copy-paste T-SQL for a card from the finding's PERSISTED
    /// <see cref="RemediationAction"/> by delegating to the shared
    /// <see cref="FactRemediation.RenderCopyPasteCommand"/> — the viewer reads persisted findings, whose
    /// ephemeral drill-down is NOT stored (so <see cref="FactRemediation.GenerateForFinding"/> returns
    /// null on read), and the built action IS persisted. Because the viewer has NO in-app remediation
    /// executor (advise-only, Postgres read-through), a runnable copy-paste command is the ONLY
    /// remediation surface, so the shared renderer covers ALL seven remediation shapes — the three
    /// always-safe ones (percent-autogrowth MODIFY FILE, missing-index CREATE, safe DB-config
    /// ALTER DATABASE … SET) render bare, and force-plan, server-config, RCSI, and clear-plan (the four
    /// that were previously copy-paste dead ends) now render too, the two destructive ones (RCSI,
    /// clear-plan) carrying their two-sided risk disclosure as a comment header. Kept as a thin
    /// <c>internal</c> seam (exercised directly by the viewer tests) over the shared renderer so the
    /// viewer and the Dashboard reader cannot drift. Null when the action carries no renderable target.
    /// </summary>
    internal static string? BuildCopyPasteSql(RemediationAction? action) =>
        FactRemediation.RenderCopyPasteCommand(action);

    /// <summary>
    /// Appends a "what else fired in this analysis window" cross-reference to each card's advice text
    /// (correlate-and-focus slice 1) so related findings link to each other. Render-time, not frozen;
    /// no-op for a single card. Mirrors Lite's / the Dashboard's reader so all surfaces cross-reference
    /// the same way.
    /// </summary>
    internal static void AppendCoFired(List<RecommendationItem> items)
    {
        if (items.Count <= 1)
            return;

        var windowTitles = new List<(string Title, double Severity)>(items.Count);
        foreach (var it in items)
            windowTitles.Add((it.Title, it.RawSeverity));

        foreach (var it in items)
        {
            var line = CoFiredSummary.Line(CoFiredSummary.OtherTitles(it.Title, windowTitles));
            if (line is null)
                continue;
            it.AdviceText = string.IsNullOrEmpty(it.AdviceText) ? line : it.AdviceText + " " + line;
        }
    }

    /// <summary>
    /// Groups the severity-sorted items into one collapsible section per INCIDENT: cards sharing an
    /// <see cref="RecommendationItem.IncidentId"/> form one group headed by their primary
    /// (highest-severity) finding, so related findings read as one report. A finding with no incident
    /// id is its own single-card group. Groups appear in severity order (the input is severity-desc
    /// sorted). A group expands unless it is Info-only. Mirrors Lite's GroupByIncident.
    /// </summary>
    private static List<RecommendationSectionViewModel> GroupByIncident(
        IReadOnlyList<RecommendationItem> list, int utcOffsetMinutes)
    {
        var order = new List<string>();
        var buckets = new Dictionary<string, List<RecommendationItem>>(StringComparer.Ordinal);
        var soloCount = 0;
        foreach (var item in list)
        {
            var key = string.IsNullOrEmpty(item.IncidentId) ? "__solo_" + soloCount++ : item.IncidentId;
            if (!buckets.TryGetValue(key, out var bucket))
            {
                bucket = new List<RecommendationItem>();
                buckets[key] = bucket;
                order.Add(key);
            }
            bucket.Add(item);
        }

        var sections = new List<RecommendationSectionViewModel>(order.Count);
        foreach (var key in order)
            sections.Add(BuildIncidentSection(buckets[key], utcOffsetMinutes));
        return sections;
    }

    /// <summary>
    /// Builds one incident section from its cards (kept in the reader's severity-desc order). The
    /// header names the primary (first) finding, the finding count, and the incident severity; the
    /// section expands unless the incident is Info-only. Mirrors Lite's BuildIncidentSection.
    /// </summary>
    private static RecommendationSectionViewModel BuildIncidentSection(
        IReadOnlyList<RecommendationItem> incidentItems, int utcOffsetMinutes)
    {
        var cards = incidentItems
            .Select(i => new RecommendationCardViewModel(i, utcOffsetMinutes))
            .ToList();
        var primary = cards[0]; // severity-desc sorted -> the first card is the incident primary
        var severity = primary.Severity;
        var label = severity switch
        {
            RecommendationSeverity.Critical => "CRITICAL",
            RecommendationSeverity.Warning => "WARNING",
            _ => "INFO"
        };
        var header = cards.Count > 1
            ? $"{primary.Title} · {cards.Count} findings · {label}"
            : $"{primary.Title} · {label}";
        return new RecommendationSectionViewModel(
            severity, cards, expanded: severity != RecommendationSeverity.Info, header);
    }

    /// <summary>
    /// Builds the MCP investigation prompt "Ask AI" copies to the clipboard for a finding. Pure (no
    /// WPF / clock) so the interpolation is unit-testable. The window times are formatted in whatever
    /// timezone the caller passed (the card passes viewer-local). Ported verbatim from Lite's prompt
    /// (RecommendationsTab AskAi_Click -> LiteRecommendationsViewModel.BuildAskAiPrompt).
    /// </summary>
    public static string BuildAskAiPrompt(string serverName, string title, DateTime from, DateTime to)
    {
        return string.Format(
            CultureInfo.InvariantCulture,
            "Using the PerformanceMonitor MCP tools, investigate this finding on server \"{0}\": " +
            "\"{1}\". It was flagged around {2:yyyy-MM-dd HH:mm}–{3:HH:mm}. Call analyze_server / " +
            "get_analysis_findings and the relevant wait/blocking/memory tools, then tell me the " +
            "likely cause and what to do.",
            serverName, title, from, to);
    }

    /// <summary>
    /// Stamps a nullable engine timestamp as UTC (the pipeline records TimeRangeStart/End in UTC; the
    /// PG read-back tags them Utc, but re-stamp defensively). Null passes through.
    /// </summary>
    private static DateTime? AsUtc(DateTime? value) =>
        value is null ? null : DateTime.SpecifyKind(value.Value, DateTimeKind.Utc);
}
