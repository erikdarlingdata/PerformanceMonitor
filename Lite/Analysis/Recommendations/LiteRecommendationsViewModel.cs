/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitorLite.Analysis.Recommendations;

/// <summary>
/// Which top-level state the Lite Recommendations surface is in. The control swaps a single
/// visible region per state; the view-model picks exactly one from the load inputs so the
/// selection is unit-testable without WPF. Mirrors the Dashboard's RecommendationsState.
/// </summary>
public enum LiteRecommendationsState
{
    /// <summary>A read is in flight — show the spinner.</summary>
    Loading,

    /// <summary>
    /// The engine has not yet collected its minimum history window
    /// (<c>AnalysisService.MinimumDataHours</c>); recommendations are not meaningful yet.
    /// </summary>
    InsufficientData,

    /// <summary>The read completed and produced zero recommendations — the all-clear.</summary>
    Empty,

    /// <summary>The read completed with one or more recommendations to render.</summary>
    Loaded
}

/// <summary>
/// A single recommendation rendered as a card. A plain DTO (no WPF dependency) wrapping a
/// <see cref="LiteRecommendationItem"/> plus the pre-computed display/visibility flags the XAML
/// binds to, so the affordance model and the Ask-AI prompt are unit-testable.
///
/// <para>
/// Lite is ADVISE-ONLY. There is NO Apply button, NO privileged remediation execution, and (unlike
/// the Dashboard) no "Open in Active Queries" deep-link — the Lite Recommendations surface is a
/// shared tab with a server selector, not a per-server tab that owns an Active Queries view, so that
/// affordance is omitted gracefully. Every card offers "Ask AI" (copies an MCP investigation
/// prompt); cards that carry a derivable copy-paste statement also offer "Copy fix".
/// </para>
/// </summary>
public sealed class LiteRecommendationCardViewModel
{
    public LiteRecommendationCardViewModel(LiteRecommendationItem item, int utcOffsetMinutes = 0)
    {
        Item = item ?? throw new ArgumentNullException(nameof(item));
        _utcOffsetMinutes = utcOffsetMinutes;
    }

    private readonly int _utcOffsetMinutes;

    /// <summary>The underlying advise-only recommendation row.</summary>
    public LiteRecommendationItem Item { get; }

    /// <summary>The three-band severity (drives the badge glyph + colour).</summary>
    public LiteRecommendationSeverity Severity => Item.Severity;

    /// <summary>Short uppercase severity label for the badge, e.g. "CRITICAL".</summary>
    public string SeverityLabel => Item.Severity switch
    {
        LiteRecommendationSeverity.Critical => "CRITICAL",
        LiteRecommendationSeverity.Warning => "WARNING",
        _ => "INFO"
    };

    /// <summary>A glyph for the badge (Segoe MDL2 Assets code point) per severity.</summary>
    public string SeverityGlyph => Item.Severity switch
    {
        LiteRecommendationSeverity.Critical => "", // error / critical
        LiteRecommendationSeverity.Warning => "",  // warning triangle
        _ => ""                                      // info
    };

    /// <summary>The card heading.</summary>
    public string Title => Item.Title;

    /// <summary>
    /// The affected database wrapped in brackets for display, or empty for a server-scoped
    /// finding (so the database line collapses).
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

    // ---- affordance model (advise-only) -----------------------------------

    /// <summary>
    /// Whether the "Copy fix" button is shown — only when a derivable copy-paste statement exists.
    /// </summary>
    public bool ShowCopyFix => !string.IsNullOrEmpty(Item.CopyPasteSql);

    /// <summary>
    /// Whether the "Ask AI" (copy MCP prompt) button is shown. Shown for every card — Lite exposes
    /// the same MCP investigation tools as the Dashboard, so any finding can be handed to AI.
    /// </summary>
    public bool ShowAskAi => true;

    // ---- Ask-AI prompt ----------------------------------------------------

    /// <summary>
    /// The MCP investigation prompt copied to the clipboard by "Ask AI". The window is rendered in
    /// the monitored server's local time (UTC window + offset) for operator legibility.
    /// </summary>
    public string AskAiPrompt
    {
        get
        {
            var (from, to) = ServerLocalWindow();
            return LiteRecommendationsViewModel.BuildAskAiPrompt(Item.ServerName, Title, from, to);
        }
    }

    /// <summary>
    /// The finding window converted to the monitored server's local time, with a sensible fallback
    /// when the producer carried no window (a 2h band ending "now" in server time).
    /// </summary>
    private (DateTime From, DateTime To) ServerLocalWindow()
    {
        if (Item.WindowStartUtc is { } su && Item.WindowEndUtc is { } eu)
            return (su.AddMinutes(_utcOffsetMinutes), eu.AddMinutes(_utcOffsetMinutes));

        var now = DateTime.UtcNow.AddMinutes(_utcOffsetMinutes);
        return (now.AddHours(-2), now);
    }
}

/// <summary>
/// A severity group (Critical / Warning / Info) of cards, rendered as a collapsible section.
/// Plain DTO so the grouping is unit-testable. Mirrors the Dashboard's section view-model.
/// </summary>
public sealed class LiteRecommendationSectionViewModel
{
    public LiteRecommendationSectionViewModel(
        LiteRecommendationSeverity severity, IReadOnlyList<LiteRecommendationCardViewModel> cards, bool expanded,
        string? header = null)
    {
        Severity = severity;
        Cards = cards ?? throw new ArgumentNullException(nameof(cards));
        IsExpanded = expanded;
        _header = header;
    }

    // When set (incident grouping), overrides the severity-derived header.
    private readonly string? _header;

    /// <summary>The severity this section groups.</summary>
    public LiteRecommendationSeverity Severity { get; }

    /// <summary>The cards in this section, in the order the reader returned them.</summary>
    public IReadOnlyList<LiteRecommendationCardViewModel> Cards { get; }

    /// <summary>How many cards the section holds.</summary>
    public int Count => Cards.Count;

    /// <summary>Whether the section's expander starts expanded.</summary>
    public bool IsExpanded { get; }

    /// <summary>
    /// The header label. For incident groups it's the supplied incident header (primary finding +
    /// count + severity); falls back to the severity label ("Critical (3)") when no header was set.
    /// </summary>
    public string Header => _header ?? (Severity switch
    {
        LiteRecommendationSeverity.Critical => $"Critical ({Count})",
        LiteRecommendationSeverity.Warning => $"Warning ({Count})",
        _ => $"Info ({Count})"
    });
}

/// <summary>
/// The pure, WPF-free core of the Lite Recommendations surface. Groups a flat
/// <see cref="LiteRecommendationItem"/> list (already severity-sorted by
/// <see cref="LiteRecommendationsReader"/>) into the Critical / Warning / Info sections the control
/// renders, and selects the single top-level <see cref="LiteRecommendationsState"/>. Keeping the
/// grouping + state logic here (rather than in code-behind) makes it directly unit-testable.
/// Mirrors the Dashboard's RecommendationsViewModel, minus the Apply/Mute/legacy concepts.
///
/// <para>
/// This is a snapshot view-model: each load builds a fresh instance and the control reassigns its
/// bound collections, so there is no <c>INotifyPropertyChanged</c> surface to reason about.
/// </para>
/// </summary>
public sealed class LiteRecommendationsViewModel
{
    /// <summary>The severity sections, Critical → Warning → Info, omitting empty ones.</summary>
    public IReadOnlyList<LiteRecommendationSectionViewModel> Sections { get; }

    /// <summary>The selected top-level state.</summary>
    public LiteRecommendationsState State { get; }

    /// <summary>
    /// The insufficient-data message to show in the <see cref="LiteRecommendationsState.InsufficientData"/>
    /// state (the engine's own message, or a default).
    /// </summary>
    public string InsufficientDataMessage { get; }

    /// <summary>Total card count across all sections.</summary>
    public int TotalCount => Sections.Sum(s => s.Count);

    private LiteRecommendationsViewModel(
        IReadOnlyList<LiteRecommendationSectionViewModel> sections,
        LiteRecommendationsState state,
        string insufficientDataMessage)
    {
        Sections = sections;
        State = state;
        InsufficientDataMessage = insufficientDataMessage;
    }

    /// <summary>The default insufficient-data prose when the engine supplied none.</summary>
    public const string DefaultInsufficientDataMessage =
        "Collecting data — recommendations appear after the engine has at least 24 hours of history. " +
        "Keep the collector running and check back later.";

    /// <summary>Builds the loading-state view-model (no data yet, read in flight).</summary>
    public static LiteRecommendationsViewModel Loading() =>
        new(Array.Empty<LiteRecommendationSectionViewModel>(), LiteRecommendationsState.Loading, string.Empty);

    /// <summary>
    /// Builds the insufficient-data-state view-model from the engine's message (or the default when
    /// it is null/blank).
    /// </summary>
    public static LiteRecommendationsViewModel InsufficientData(string? engineMessage) =>
        new(
            Array.Empty<LiteRecommendationSectionViewModel>(),
            LiteRecommendationsState.InsufficientData,
            string.IsNullOrWhiteSpace(engineMessage) ? DefaultInsufficientDataMessage : engineMessage!);

    /// <summary>
    /// Builds a loaded/empty view-model from the reader's flat, already-sorted list. Groups by
    /// <see cref="LiteRecommendationSeverity"/> into Critical / Warning / Info sections (empty
    /// sections are omitted), preserving the reader's intra-severity order. Critical and Warning
    /// start expanded; Info starts collapsed. The state is <see cref="LiteRecommendationsState.Empty"/>
    /// when the list is empty, else <see cref="LiteRecommendationsState.Loaded"/>.
    /// <paramref name="utcOffsetMinutes"/> is carried onto each card for the Ask-AI prompt's window.
    /// </summary>
    public static LiteRecommendationsViewModel FromItems(
        IEnumerable<LiteRecommendationItem> items, int utcOffsetMinutes = 0)
    {
        var list = items as IReadOnlyList<LiteRecommendationItem> ?? items?.ToList()
                   ?? (IReadOnlyList<LiteRecommendationItem>)Array.Empty<LiteRecommendationItem>();

        if (list.Count == 0)
            return new(Array.Empty<LiteRecommendationSectionViewModel>(), LiteRecommendationsState.Empty, string.Empty);

        return new(GroupByIncident(list, utcOffsetMinutes), LiteRecommendationsState.Loaded, string.Empty);
    }

    /// <summary>
    /// Groups the reader's flat, severity-sorted list into one collapsible section per INCIDENT
    /// (correlate-and-focus): cards sharing an <see cref="LiteRecommendationItem.IncidentId"/> form one
    /// group headed by their primary (highest-severity) finding, so related findings read as one report
    /// instead of a sea of cards. A finding with no incident id (from before incident_id existed) is its
    /// own single-card group. Groups appear in severity order (the input is already severity-desc
    /// sorted, so each group's primary card is in severity order). A group expands unless it is Info-only.
    /// Mirrors the Dashboard's GroupByIncident.
    /// </summary>
    private static List<LiteRecommendationSectionViewModel> GroupByIncident(
        IReadOnlyList<LiteRecommendationItem> list, int utcOffsetMinutes)
    {
        var order = new List<string>();
        var buckets = new Dictionary<string, List<LiteRecommendationItem>>(StringComparer.Ordinal);
        var soloCount = 0;
        foreach (var item in list)
        {
            var key = string.IsNullOrEmpty(item.IncidentId) ? "__solo_" + soloCount++ : item.IncidentId;
            if (!buckets.TryGetValue(key, out var bucket))
            {
                bucket = new List<LiteRecommendationItem>();
                buckets[key] = bucket;
                order.Add(key);
            }
            bucket.Add(item);
        }

        var sections = new List<LiteRecommendationSectionViewModel>(order.Count);
        foreach (var key in order)
            sections.Add(BuildIncidentSection(buckets[key], utcOffsetMinutes));
        return sections;
    }

    /// <summary>
    /// Builds one incident section from its cards (kept in the reader's severity-desc order). The
    /// header names the primary (first) finding, the finding count, and the incident severity; the
    /// section expands unless the incident is Info-only.
    /// </summary>
    private static LiteRecommendationSectionViewModel BuildIncidentSection(
        IReadOnlyList<LiteRecommendationItem> incidentItems, int utcOffsetMinutes)
    {
        var cards = incidentItems
            .Select(i => new LiteRecommendationCardViewModel(i, utcOffsetMinutes))
            .ToList();
        var primary = cards[0]; // reader sorted severity-desc -> the first card is the incident primary
        var severity = primary.Severity;
        var label = severity switch
        {
            LiteRecommendationSeverity.Critical => "CRITICAL",
            LiteRecommendationSeverity.Warning => "WARNING",
            _ => "INFO"
        };
        var header = cards.Count > 1
            ? $"{primary.Title} · {cards.Count} findings · {label}"
            : $"{primary.Title} · {label}";
        return new LiteRecommendationSectionViewModel(
            severity, cards, expanded: severity != LiteRecommendationSeverity.Info, header);
    }

    /// <summary>
    /// Builds the MCP investigation prompt "Ask AI" copies to the clipboard for a finding. Pure (no
    /// WPF / clock) so the interpolation is unit-testable. The window times are formatted in whatever
    /// timezone the caller passed (the card passes server-local). Mirrors the Dashboard's prompt.
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
}
