/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorLite.Analysis.Recommendations;

/// <summary>
/// Canonical, three-band severity for the Lite Recommendations surface. The analysis
/// engine scores findings on a <c>double</c> (0–~2.0); that scale is mapped onto this
/// single enum so one list can be sorted and rendered consistently. The numeric ordinals
/// are deliberately Critical &gt; Warning &gt; Info so a descending sort on the enum matches
/// a descending sort on severity. Lite-local mirror of the Dashboard's CanonicalSeverity
/// (Lite reads ONLY the engine producer — there is no legacy critical_issues store in Lite).
/// </summary>
public enum LiteRecommendationSeverity
{
    Info = 0,
    Warning = 1,
    Critical = 2
}

/// <summary>
/// A single Lite Recommendations row — the DTO the surface renders. A plain object with NO WPF
/// dependency so the reader's mapping is unit-testable without a UI or a database. Built by
/// <see cref="LiteRecommendationsReader"/> from an
/// <see cref="PerformanceMonitor.Analysis.AnalysisFinding"/>.
///
/// <para>
/// Lite produces a COPYABLE remediation command but has NO in-app Apply/execute path, permanently
/// (#2138). The reason is Lite's STORE, not a division of labour between SKUs: Lite's DuckDB file is
/// per-workstation, and a remediation journal kept there could not be the audit trail such a journal
/// exists to be — two operators acting on the same server would each hold half of it and neither could
/// see the other's forces. It also could not carry the auto-force bot's own-forces-only invariant, which
/// is a predicate plus a self-reference WITHIN one shared store. So the execute path belongs to the SKU
/// with a shared store, and no deprecation can move it back. A Lite card offers the diagnosis, the
/// copy-paste T-SQL rendered from the finding's persisted
/// <see cref="PerformanceMonitor.Analysis.RemediationAction"/> (the SAME shared renderer the Darling
/// viewer uses, so the commands are byte-identical), and an "Ask AI" MCP prompt — the operator runs the
/// command themselves against their own direct SQL Server connection.
/// </para>
/// </summary>
public sealed class LiteRecommendationItem
{
    /// <summary>The three-band severity used for sorting and the badge/icon.</summary>
    public LiteRecommendationSeverity Severity { get; set; }

    /// <summary>
    /// The raw engine severity (<c>double</c>, 0–~2.0) used as the secondary sort within a
    /// band so two findings in the same band still order by their underlying score.
    /// </summary>
    public double RawSeverity { get; set; }

    /// <summary>The affected database, or null/empty for a server-scoped finding.</summary>
    public string? Database { get; set; }

    /// <summary>The one-line title shown as the card heading.</summary>
    public string Title { get; set; } = string.Empty;

    /// <summary>
    /// Operator-facing advice prose, composed from <see cref="PerformanceMonitor.Analysis.FactAdvice"/>
    /// for the finding's root fact key (remediation line + investigation line). Falls back to the
    /// finding's own story text when no advice block matches. Null only when neither is available.
    /// </summary>
    public string? AdviceText { get; set; }

    /// <summary>
    /// Copy-paste-ready T-SQL the operator can run themselves, rendered from the finding's persisted
    /// <see cref="PerformanceMonitor.Analysis.RemediationAction"/> by the SHARED
    /// <see cref="PerformanceMonitor.Analysis.FactRemediation.RenderCopyPasteCommand"/> (the same
    /// renderer the Darling viewer's Copy-fix uses, so the commands are byte-identical). Null when the
    /// finding carries no execution shape — such cards render advise-only (advice + Ask AI).
    /// </summary>
    public string? CopyPasteSql { get; set; }

    /// <summary>
    /// The finding's incident id (correlate-and-focus) — the group key the surface collapses cards
    /// under, so related findings render as one report. Empty for findings analyzed before incident_id
    /// existed; the view-model treats an empty id as a standalone single-card incident.
    /// </summary>
    public string IncidentId { get; set; } = string.Empty;

    /// <summary>The monitored server's display name (for the Ask-AI prompt).</summary>
    public string ServerName { get; set; } = string.Empty;

    /// <summary>
    /// UTC start of the time window the finding pertains to (the analysis
    /// <c>TimeRangeStart</c>). Used to render the Ask-AI prompt's window. Null when the
    /// finding carried no time bound.
    /// </summary>
    public DateTime? WindowStartUtc { get; set; }

    /// <summary>UTC end of the finding time window (the analysis <c>TimeRangeEnd</c>). Null when absent.</summary>
    public DateTime? WindowEndUtc { get; set; }
}
