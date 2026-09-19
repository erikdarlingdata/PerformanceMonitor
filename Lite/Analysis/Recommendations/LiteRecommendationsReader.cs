/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Analysis.Recommendations;

/// <summary>
/// The data layer for the Lite Recommendations surface (WS1c). Reads the engine findings a
/// server persisted to DuckDB (via the existing <see cref="FindingStore.GetRecentFindingsAsync"/>
/// read path) and maps each to a <see cref="LiteRecommendationItem"/>. This mirrors the Dashboard's
/// <c>RecommendationsReader</c> logic — engine severity banding and latest-batch-only — but is
/// Lite-contained: there is no legacy critical_issues store in Lite and no de-dupe across producers.
/// Lite produces a COPYABLE remediation command (no in-app Apply/execute path) — the operator runs it
/// against their own direct SQL Server connection.
///
/// <para>
/// Advice prose comes from the SHARED <see cref="FactAdvice"/> table (keyed on the finding's root
/// fact key, which IS read back). The copy-paste T-SQL comes from the finding's PERSISTED
/// <see cref="PerformanceMonitor.Analysis.RemediationAction"/> (built at analysis time from the
/// drill-down, serialized into <c>remediation_action_json</c>, deserialized on read) rendered by the
/// SHARED <see cref="FactRemediation.RenderCopyPasteCommand"/> — the SAME source the Darling viewer's
/// Copy-fix delegates to, so Lite and Darling produce byte-identical commands for all seven remediation
/// shapes (the two destructive ones carry a two-sided risk-disclosure header). The live
/// <see cref="FactRemediation.GenerateForFinding"/> path is kept only as a fallback for a finding still
/// carrying its ephemeral drill-down (no persisted action). Findings with no execution shape carry a
/// null action and render advise-only.
/// </para>
///
/// <para>
/// The per-row mappers are <c>internal static</c> so the finding → card mapping (banding, advice
/// composition, sort, latest-batch trim) can be unit-tested directly without a database or UI.
/// </para>
/// </summary>
public sealed class LiteRecommendationsReader
{
    private readonly FindingStore _findingStore;

    public LiteRecommendationsReader(FindingStore findingStore)
    {
        _findingStore = findingStore ?? throw new ArgumentNullException(nameof(findingStore));
    }

    /// <summary>
    /// Reads the latest analysis batch for the server and returns the mapped advise-only cards,
    /// sorted by severity descending. <paramref name="hoursBack"/> bounds the read; only the most
    /// recent <c>analysis_time</c> within that window is surfaced (see <see cref="LatestBatchOnly"/>).
    /// <paramref name="serverName"/> is carried onto each card for the Ask-AI prompt.
    /// </summary>
    public async Task<List<LiteRecommendationItem>> GetRecommendationsAsync(
        int serverId, string serverName, int hoursBack = 24, int limit = 100)
    {
        var findings = await _findingStore.GetRecentFindingsAsync(serverId, hoursBack, limit);
        /* #3712: the two knobs the notification path applied when these findings fired, so the grid's
           not-paged marker and the gate's decision read the same settings. */
        var lens = new FindingRoutingLens(App.AnalysisNotifySeverity, App.AnalysisUncorroboratedRoute);
        return MapFindings(findings, serverName, lens);
    }

    /// <summary>
    /// Maps a finding list (already ordered <c>analysis_time DESC, severity DESC</c> by the store)
    /// to the sorted advise-only card list. Pure over the inputs — no DB, no UI — so the banding,
    /// advice composition, latest-batch trim, and sort are unit-testable directly. Exposed
    /// <c>internal</c> for tests; the public entry point is <see cref="GetRecommendationsAsync"/>.
    /// </summary>
    internal static List<LiteRecommendationItem> MapFindings(
        IReadOnlyList<AnalysisFinding> findings, string serverName, FindingRoutingLens? lens = null)
    {
        var latest = LatestBatchOnly(findings);

        var items = new List<LiteRecommendationItem>(latest.Count);
        foreach (var finding in latest)
        {
            if (finding is null)
                continue;
            items.Add(MapFinding(finding, serverName, lens));
        }

        items.Sort(CompareForDisplay);
        AppendCoFired(items);
        return items;
    }

    /// <summary>
    /// Appends a "what else fired in this analysis window" cross-reference to each card's advice text
    /// (correlate-and-focus slice 1, review §1d) so the operator can see related findings without
    /// hunting. Render-time, not frozen — always reflects the current batch. No-op for a single card.
    /// </summary>
    private static void AppendCoFired(List<LiteRecommendationItem> items)
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
    /// Keeps only the most-recent analysis run's findings. <see cref="FindingStore.GetRecentFindingsAsync"/>
    /// returns EVERY batch in the window (ordered <c>analysis_time DESC</c>), so on a server analyzed
    /// repeatedly each run would otherwise add another copy of every finding — one card per batch.
    /// Mirrors the Dashboard reader's latest-batch-only trim. Returns the input unchanged when it has
    /// zero or one rows.
    /// </summary>
    internal static IReadOnlyList<AnalysisFinding> LatestBatchOnly(IReadOnlyList<AnalysisFinding> findings)
    {
        if (findings is null || findings.Count <= 1)
            return findings ?? Array.Empty<AnalysisFinding>();

        var latest = findings[0].AnalysisTime; // ordered DESC -> [0] is the most recent batch
        var latestOnly = new List<AnalysisFinding>(findings.Count);
        foreach (var f in findings)
        {
            if (f is not null && f.AnalysisTime == latest)
                latestOnly.Add(f);
        }

        return latestOnly;
    }

    /// <summary>
    /// Maps one engine <see cref="AnalysisFinding"/> to a single <see cref="LiteRecommendationItem"/>.
    /// Advice comes from <see cref="FactAdvice"/> for the root fact key; the copy-paste command comes
    /// PRIMARILY from the finding's persisted <see cref="AnalysisFinding.Remediation"/> rendered by the
    /// shared <see cref="BuildCopyPasteSql"/> (the read-back path — the finding has no drill-down but
    /// carries the built action), falling back to the live <see cref="FactRemediation.GenerateForFinding"/>
    /// for a finding still carrying its ephemeral drill-down. Null when neither applies — the card then
    /// renders advise-only.
    /// </summary>
    /// <param name="lens">#3712: the notify floor and routing knob to judge the not-paged marker by; null
    /// (the pure-mapping tests' default) computes no marker.</param>
    internal static LiteRecommendationItem MapFinding(AnalysisFinding finding, string serverName, FindingRoutingLens? lens = null)
    {
        // Value-stated advice frozen into StoryText at analysis time (current MAXDOP/CTFP/etc.),
        // with the static block as the fallback for legacy findings. StoryText now holds advice
        // JSON, never the old (always-empty) story prose, so it is no longer a Title/text fallback.
        var advice = FactAdvice.GetComposedForFinding(finding);

        return new LiteRecommendationItem
        {
            Severity = SeverityBand(finding.Severity),
            RawSeverity = finding.Severity,
            Database = string.IsNullOrEmpty(finding.DatabaseName) ? null : finding.DatabaseName,
            Title = !string.IsNullOrEmpty(advice?.Headline) ? advice!.Headline : finding.RootFactKey,
            AdviceText = ComposeAdvice(advice),
            // Persisted-action render is the PRIMARY source (mirrors the Darling viewer); the live
            // drill-down generator is only a fallback when no action was persisted (see class remarks).
            CopyPasteSql = NullIfEmpty(BuildCopyPasteSql(finding.Remediation))
                        ?? NullIfEmpty(FactRemediation.GenerateForFinding(finding)),
            IncidentId = finding.IncidentId,
            ServerName = serverName ?? string.Empty,
            WindowStartUtc = AsUtc(finding.TimeRangeStart),
            WindowEndUtc = AsUtc(finding.TimeRangeEnd),
            /* #3712: read off the persisted fields (root key, confidence, chain length) through the shared
               lens; null when the finding paged, was below the floor, or no lens was supplied. */
            NotPagedReason = lens?.NotPagedReason(finding)
        };
    }

    /// <summary>
    /// Rebuilds the copy-paste T-SQL for a card from the finding's PERSISTED
    /// <see cref="RemediationAction"/> by delegating to the shared
    /// <see cref="FactRemediation.RenderCopyPasteCommand"/> — the SAME renderer the Darling viewer's
    /// Copy-fix uses, so a Lite card and a Darling card produce byte-identical commands for all seven
    /// remediation shapes (force-plan, clear-plan, server-config, RCSI, safe DB-config, file-growth,
    /// missing-index; the two destructive shapes carry a two-sided risk-disclosure comment header).
    /// Lite has NO in-app executor — this is a copyable command only. A thin <c>internal</c> seam over
    /// the shared renderer so the reader can be unit-tested directly and cannot drift from the viewer.
    /// Null when the action is null or carries no renderable target.
    /// </summary>
    internal static string? BuildCopyPasteSql(RemediationAction? action) =>
        FactRemediation.RenderCopyPasteCommand(action);

    /// <summary>
    /// Maps an engine finding's <c>double</c> severity (0–~2.0) onto the canonical band. Cutoffs
    /// are identical to the Dashboard reader's: <c>&gt;= 1.5</c> → Critical, <c>&gt;= 0.75</c> →
    /// Warning, else Info, so a finding bands the same way in both apps.
    /// </summary>
    internal static LiteRecommendationSeverity SeverityBand(double severity)
    {
        if (severity >= 1.5)
            return LiteRecommendationSeverity.Critical;
        if (severity >= 0.75)
            return LiteRecommendationSeverity.Warning;
        return LiteRecommendationSeverity.Info;
    }

    /// <summary>
    /// Composes the advice prose from a <see cref="AdviceBlock"/>: the remediation line, with the
    /// investigation line appended when present. Null when no advice matched the root fact key.
    /// Mirrors the Dashboard reader's <c>ComposeEngineAdvice</c>.
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
    /// Total display order: canonical severity desc, then raw severity desc, then database
    /// (ordinal, case-insensitive), then title — deterministic regardless of input order. Mirrors
    /// the Dashboard reader's <c>CompareForDisplay</c>.
    /// </summary>
    private static int CompareForDisplay(LiteRecommendationItem x, LiteRecommendationItem y)
    {
        // Higher band first (enum ordinals are Info<Warning<Critical, so descending).
        int bySeverity = ((int)y.Severity).CompareTo((int)x.Severity);
        if (bySeverity != 0)
            return bySeverity;

        int byRaw = y.RawSeverity.CompareTo(x.RawSeverity);
        if (byRaw != 0)
            return byRaw;

        int byDb = string.Compare(
            x.Database ?? string.Empty, y.Database ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        if (byDb != 0)
            return byDb;

        return string.Compare(x.Title, y.Title, StringComparison.Ordinal);
    }

    /// <summary>Returns null for a null/empty/whitespace string, else the trimmed-nothing original.</summary>
    private static string? NullIfEmpty(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value;

    /// <summary>
    /// Stamps a nullable engine timestamp as UTC (the analysis pipeline records
    /// <c>TimeRangeStart/End</c> in UTC; the DuckDB read-back can return Kind=Unspecified, so fix
    /// the Kind to avoid an ambiguous later conversion). Null passes through.
    /// </summary>
    private static DateTime? AsUtc(DateTime? value) =>
        value is null ? null : DateTime.SpecifyKind(value.Value, DateTimeKind.Utc);
}
