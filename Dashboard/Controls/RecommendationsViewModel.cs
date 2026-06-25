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
using PerformanceMonitorDashboard.Services.Recommendations;

namespace PerformanceMonitorDashboard.Controls
{
    /// <summary>
    /// Which top-level state the Recommendations surface is in. The control swaps a single
    /// visible region per state; the view-model picks exactly one from the load inputs so the
    /// selection is unit-testable without WPF (WS1b-1).
    /// </summary>
    public enum RecommendationsState
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
    /// <see cref="RecommendationItem"/> plus the pre-computed display/visibility flags the XAML
    /// binds to, so the affordance model and the Ask-AI prompt are unit-testable (WS1b-1).
    ///
    /// <para>
    /// The card affordances split on whether the finding is an <b>incident</b> (time-bound:
    /// CPU/memory/blocking/waits/plan-regression — <see cref="RecommendationSetting.None"/> with no
    /// structured standing-fix action) or a standing <b>config-fix</b> (either a flagged
    /// <see cref="RecommendationSetting"/> — AutoShrink/AutoClose/QueryStore/MAXDOP/RCSI — OR a
    /// structured per-target action such as FILE_AUTOGROWTH_PERCENT, which has Setting==None but
    /// still carries a typed MODIFY FILE fix):
    /// </para>
    /// <list type="bullet">
    ///   <item>Incidents: "Open in Active Queries" (deep-links to the time window) + "Ask AI"
    ///   (copies an MCP prompt) + Apply (when a remediation exists, e.g. clear-plan).</item>
    ///   <item>Config-fixes: Apply (when a remediation exists) + "Copy fix" (copies the ALTER).</item>
    /// </list>
    /// <para>
    /// Apply (the gated remediation + two-sided informed consent for destructive fixes) and Mute
    /// (engine rows) are wired in WS1b-2; "Open in Active Queries", "Ask AI" and "Copy fix" were
    /// wired in WS1b-1 (navigation + clipboard).
    /// </para>
    /// </summary>
    public sealed class RecommendationCardViewModel
    {
        public RecommendationCardViewModel(RecommendationItem item, string serverName = "", int utcOffsetMinutes = 0)
        {
            Item = item ?? throw new ArgumentNullException(nameof(item));
            ServerName = serverName ?? string.Empty;
            _utcOffsetMinutes = utcOffsetMinutes;
        }

        private readonly int _utcOffsetMinutes;

        /// <summary>The underlying unified recommendation row.</summary>
        public RecommendationItem Item { get; }

        /// <summary>The monitored server's display name (for the Ask-AI prompt).</summary>
        public string ServerName { get; }

        /// <summary>The three-band severity (drives the badge glyph + colour).</summary>
        public CanonicalSeverity Severity => Item.CanonicalSeverity;

        /// <summary>Short uppercase severity label for the badge, e.g. "CRITICAL".</summary>
        public string SeverityLabel => Item.CanonicalSeverity switch
        {
            CanonicalSeverity.Critical => "CRITICAL",
            CanonicalSeverity.Warning => "WARNING",
            _ => "INFO"
        };

        /// <summary>A glyph for the badge (Segoe MDL2 Assets code point) per severity.</summary>
        public string SeverityGlyph => Item.CanonicalSeverity switch
        {
            CanonicalSeverity.Critical => "", // error / critical
            CanonicalSeverity.Warning => "",  // warning triangle
            _ => ""                            // info
        };

        /// <summary>The card heading.</summary>
        public string Title => Item.Title;

        /// <summary>
        /// The affected database wrapped in brackets for display, or empty for a server-scoped
        /// rec (so the database line collapses).
        /// </summary>
        public string DatabaseBracketed =>
            string.IsNullOrEmpty(Item.Database) ? string.Empty : $"[{Item.Database}]";

        /// <summary>Whether a database line should be shown at all.</summary>
        public bool HasDatabase => !string.IsNullOrEmpty(Item.Database);

        /// <summary>The operator-facing advice prose.</summary>
        public string? AdviceText => Item.AdviceText;

        /// <summary>Whether there is advice prose to render.</summary>
        public bool HasAdvice => !string.IsNullOrEmpty(Item.AdviceText);

        /// <summary>The copy-paste-ready fix T-SQL (config-fixes only), if any.</summary>
        public string? CopyPasteSql => Item.CopyPasteSql;

        // ---- affordance model -------------------------------------------------

        /// <summary>
        /// Whether this is a time-bound INCIDENT finding (CPU/memory/blocking/waits/plan-regression):
        /// <see cref="RecommendationItem.Setting"/> == <see cref="RecommendationSetting.None"/> AND it
        /// does not carry a <see cref="HasStructuredFixAction"/> standing fix. Incidents send the
        /// operator to the dashboard / AI rather than showing a raw query.
        /// </summary>
        public bool IsIncident => Item.Setting == RecommendationSetting.None && !HasStructuredFixAction;

        /// <summary>
        /// Whether this is a standing CONFIG-FIX finding (a flagged
        /// <see cref="RecommendationSetting"/>: AutoShrink/AutoClose/QueryStore/RCSI/PageVerify).
        /// </summary>
        public bool IsConfigFix => Item.Setting != RecommendationSetting.None;

        /// <summary>
        /// Whether this row carries a STRUCTURED standing fix — a persisted action with typed
        /// per-target lists that map to a database-config-style ALTER (DB_CONFIG settings or
        /// FILE_AUTOGROWTH_PERCENT files). Such a row is a config fix you run against a standing
        /// setting (not a time-bound incident): it offers BOTH "Copy fix" AND Apply, and never the
        /// incident affordances (Open-in-Active-Queries / Ask-AI). Plan-regression / clear-plan
        /// actions are NOT structured fixes (their target lists are empty here) and remain
        /// incidents that happen to be Apply-able.
        /// </summary>
        private bool HasStructuredFixAction =>
            (Item.Remediation?.DbConfigTargets is { Count: > 0 }) ||
            (Item.Remediation?.FileGrowthTargets is { Count: > 0 }) ||
            // WS3 server-level config (MAXDOP/CTFP carry a typed ServerConfigTargets Apply action;
            // the advise-only memory cards carry no action but set IsServerConfigAdvisory so they
            // still read as Copy-only config fixes, not incidents).
            (Item.Remediation?.ServerConfigTargets is { Count: > 0 }) ||
            Item.IsServerConfigAdvisory;

        /// <summary>
        /// Whether the "Open in Active Queries" deep-link button is shown — incidents only.
        /// </summary>
        public bool ShowOpenInActiveQueries => IsIncident;

        /// <summary>Whether the "Ask AI" (copy MCP prompt) button is shown — incidents only.</summary>
        public bool ShowAskAi => IsIncident;

        /// <summary>
        /// Whether the "Copy fix" button is shown — config-fixes (a flagged setting), structured
        /// standing fixes (DB_CONFIG / FILE_AUTOGROWTH_PERCENT) that carry an ALTER statement, or a
        /// MISSING_INDEX advisory (WS4) carrying a SQL Server-suggested CREATE INDEX to copy. The
        /// missing-index case deliberately stays an incident (it keeps Open-in-Active-Queries / Ask-AI
        /// and shows no Apply), so it is gated on its own flag rather than reclassifying the card as a
        /// structured fix.
        /// </summary>
        public bool ShowCopyFix =>
            !string.IsNullOrEmpty(Item.CopyPasteSql) && (IsConfigFix || HasStructuredFixAction || Item.IsMissingIndexAdvisory);

        /// <summary>
        /// Whether the Apply button is shown for this card — whenever the row carries a built,
        /// persisted <see cref="RecommendationItem.Remediation"/> action (engine rows; mirrors the
        /// alert path's <c>Remediation != null</c> rule). Every Remediation that reaches a CARD has a
        /// registered handler (plan-regression, DB-config, RCSI, clear-plan, file-autogrowth,
        /// server-config); the one non-executable persisted action — the WS4 MISSING_INDEX advisory —
        /// is mapped by the reader to a copy-paste card with Remediation left null, so it never shows
        /// Apply. Shown for incidents (e.g. clear-plan / plan-regression), config-fixes (e.g. RCSI),
        /// and structured standing fixes (autogrowth).
        /// Drives the Apply button + (for destructive fixes) the two-sided informed-consent gate.
        /// </summary>
        public bool ShowApply => Item.Remediation != null;

        /// <summary>
        /// Whether the Mute button is shown. Mute is an engine-only concept (the legacy
        /// <c>config.critical_issues</c> store has no mute), so it shows only for
        /// <see cref="RecommendationSource.Engine"/> rows. Drives the Mute button (mutes the
        /// story pattern for this server), wired in WS1b-2.
        /// </summary>
        public bool ShowMute => Item.Source == RecommendationSource.Engine;

        // ---- deep-link window (raw UTC; the handler applies grace + tz) --------

        /// <summary>Raw UTC start of the finding window (drives the deep-link), or null.</summary>
        public DateTime? WindowStartUtc => Item.WindowStartUtc;

        /// <summary>Raw UTC end of the finding window (drives the deep-link), or null.</summary>
        public DateTime? WindowEndUtc => Item.WindowEndUtc;

        // ---- Ask-AI prompt ----------------------------------------------------

        /// <summary>
        /// The MCP investigation prompt copied to the clipboard by "Ask AI". The window is rendered
        /// in the monitored server's local time (UTC window + offset) for operator legibility.
        /// </summary>
        public string AskAiPrompt
        {
            get
            {
                var (from, to) = ServerLocalWindow();
                return RecommendationsViewModel.BuildAskAiPrompt(ServerName, Title, from, to);
            }
        }

        /// <summary>
        /// The finding window converted to the monitored server's local time, with a sensible
        /// fallback when the producer carried no window (a 2h band ending "now" in server time).
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
    /// Plain DTO so the grouping is unit-testable (WS1b-1).
    /// </summary>
    public sealed class RecommendationSectionViewModel
    {
        public RecommendationSectionViewModel(
            CanonicalSeverity severity, IReadOnlyList<RecommendationCardViewModel> cards, bool expanded,
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
        public CanonicalSeverity Severity { get; }

        /// <summary>The cards in this section, in the order the reader returned them.</summary>
        public IReadOnlyList<RecommendationCardViewModel> Cards { get; }

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
            CanonicalSeverity.Critical => $"Critical ({Count})",
            CanonicalSeverity.Warning => $"Warning ({Count})",
            _ => $"Info ({Count})"
        });
    }

    /// <summary>
    /// The pure, WPF-free core of the Recommendations surface (WS1b-1). Groups a flat
    /// <see cref="RecommendationItem"/> list (already de-duped + severity-sorted by
    /// <c>RecommendationsReader</c>) into the Critical / Warning / Info sections the control
    /// renders, and selects the single top-level <see cref="RecommendationsState"/>. Keeping the
    /// grouping + state logic here (rather than in code-behind) makes it directly unit-testable.
    ///
    /// <para>
    /// This is a snapshot view-model: each load builds a fresh instance and the control reassigns
    /// its bound collections, so there is no <c>INotifyPropertyChanged</c> surface to reason about.
    /// </para>
    /// </summary>
    public sealed class RecommendationsViewModel
    {
        /// <summary>The severity sections, Critical → Warning → Info, omitting empty ones.</summary>
        public IReadOnlyList<RecommendationSectionViewModel> Sections { get; }

        /// <summary>The selected top-level state.</summary>
        public RecommendationsState State { get; }

        /// <summary>
        /// The insufficient-data message to show in the <see cref="RecommendationsState.InsufficientData"/>
        /// state (the engine's own <c>InsufficientDataMessage</c>, or a default).
        /// </summary>
        public string InsufficientDataMessage { get; }

        /// <summary>Total card count across all sections.</summary>
        public int TotalCount => Sections.Sum(s => s.Count);

        private RecommendationsViewModel(
            IReadOnlyList<RecommendationSectionViewModel> sections,
            RecommendationsState state,
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

        /// <summary>
        /// Builds the loading-state view-model (no data yet, read in flight).
        /// </summary>
        public static RecommendationsViewModel Loading() =>
            new(Array.Empty<RecommendationSectionViewModel>(), RecommendationsState.Loading, string.Empty);

        /// <summary>
        /// Builds the insufficient-data-state view-model from the engine's message (or the default
        /// when it is null/blank).
        /// </summary>
        public static RecommendationsViewModel InsufficientData(string? engineMessage) =>
            new(
                Array.Empty<RecommendationSectionViewModel>(),
                RecommendationsState.InsufficientData,
                string.IsNullOrWhiteSpace(engineMessage) ? DefaultInsufficientDataMessage : engineMessage!);

        /// <summary>
        /// Builds a loaded/empty view-model from the reader's flat, already-sorted list. Groups by
        /// <see cref="CanonicalSeverity"/> into Critical / Warning / Info sections (empty sections
        /// are omitted), preserving the reader's intra-severity order. Critical and Warning start
        /// expanded; Info starts collapsed. The state is <see cref="RecommendationsState.Empty"/>
        /// when the list is empty, else <see cref="RecommendationsState.Loaded"/>.
        /// <paramref name="serverName"/> + <paramref name="utcOffsetMinutes"/> are carried onto each
        /// card for the Ask-AI prompt (the deep-link itself uses the raw UTC window).
        /// </summary>
        public static RecommendationsViewModel FromItems(
            IEnumerable<RecommendationItem> items, string serverName = "", int utcOffsetMinutes = 0)
        {
            var list = items as IReadOnlyList<RecommendationItem> ?? items?.ToList()
                       ?? (IReadOnlyList<RecommendationItem>)Array.Empty<RecommendationItem>();

            if (list.Count == 0)
                return new(Array.Empty<RecommendationSectionViewModel>(), RecommendationsState.Empty, string.Empty);

            return new(GroupByIncident(list, serverName, utcOffsetMinutes), RecommendationsState.Loaded, string.Empty);
        }

        /// <summary>
        /// Groups the reader's flat, severity-sorted list into one collapsible section per INCIDENT
        /// (correlate-and-focus): cards sharing an <see cref="RecommendationItem.IncidentId"/> form one
        /// group headed by their primary (highest-severity) finding, so related findings read as one
        /// report instead of a sea of cards. An item with no incident id (legacy rows; engine rows from
        /// before incident_id existed) is its own single-card group. Groups appear in severity order —
        /// the input is already severity-desc sorted, so each group's first-appearance (its primary
        /// card) is in severity-desc order. A group expands unless it is Info-only.
        /// </summary>
        private static List<RecommendationSectionViewModel> GroupByIncident(
            IReadOnlyList<RecommendationItem> list, string serverName, int utcOffsetMinutes)
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
                sections.Add(BuildIncidentSection(buckets[key], serverName, utcOffsetMinutes));
            return sections;
        }

        /// <summary>
        /// Builds one incident section from its cards (kept in the reader's severity-desc order). The
        /// header names the primary (first) finding, the finding count, and the incident severity; the
        /// section expands unless the incident is Info-only.
        /// </summary>
        private static RecommendationSectionViewModel BuildIncidentSection(
            IReadOnlyList<RecommendationItem> incidentItems, string serverName, int utcOffsetMinutes)
        {
            var cards = incidentItems
                .Select(i => new RecommendationCardViewModel(i, serverName, utcOffsetMinutes))
                .ToList();
            var primary = cards[0]; // reader sorted severity-desc -> the first card is the incident primary
            var severity = primary.Severity;
            var label = severity switch
            {
                CanonicalSeverity.Critical => "CRITICAL",
                CanonicalSeverity.Warning => "WARNING",
                _ => "INFO"
            };
            var header = cards.Count > 1
                ? $"{primary.Title} · {cards.Count} findings · {label}"
                : $"{primary.Title} · {label}";
            return new RecommendationSectionViewModel(
                severity, cards, expanded: severity != CanonicalSeverity.Info, header);
        }

        /// <summary>
        /// Builds the MCP investigation prompt "Ask AI" copies to the clipboard for an incident.
        /// Pure (no WPF / clock) so the interpolation is unit-testable. The window times are
        /// formatted in whatever timezone the caller passed (the card passes server-local).
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
}
