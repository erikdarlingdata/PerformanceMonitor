/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Analysis;

namespace PerformanceMonitorDashboard.Services.Recommendations
{
    /// <summary>
    /// Canonical, three-band severity for the unified Recommendations surface. The two
    /// producers score on different scales (engine findings carry a <c>double</c> 0–~2.0;
    /// the legacy <c>config.critical_issues</c> store carries the text CRITICAL/WARNING/INFO)
    /// — both are mapped onto this single enum so one list can be sorted and rendered
    /// consistently. The numeric ordinals are deliberately Critical &gt; Warning &gt; Info
    /// so a descending sort on the enum matches a descending sort on severity.
    /// </summary>
    public enum CanonicalSeverity
    {
        Info = 0,
        Warning = 1,
        Critical = 2
    }

    /// <summary>
    /// Which producer a <see cref="RecommendationItem"/> came from.
    /// <see cref="Engine"/> = <c>config.analysis_findings</c> (the fact/story engine; may
    /// carry an Apply-capable <see cref="RemediationAction"/>). <see cref="Legacy"/> =
    /// <c>config.critical_issues</c> (the frozen configuration-issues proc; advise +
    /// copy-paste only, never Apply).
    /// </summary>
    public enum RecommendationSource
    {
        Engine,
        Legacy
    }

    /// <summary>
    /// The canonical de-dupe key's setting component (C3). A (normalized database, setting)
    /// pair identifies the SAME underlying misconfiguration regardless of which producer
    /// surfaced it, so the de-dupe can collapse a duplicate that both stores report.
    /// <see cref="None"/> means the row is NOT a per-database config-setting rec (memory
    /// pressure, server-level MAXDOP/CTFP, dumps, plan/index advice, …) and therefore NEVER
    /// de-dupes — every <see cref="None"/> row passes through untouched.
    ///
    /// <para>
    /// Only <see cref="AutoShrink"/>, <see cref="AutoClose"/> and <see cref="QueryStore"/>
    /// can collide across the two stores today. <see cref="Rcsi"/> and
    /// <see cref="PageVerify"/> are engine-only (the legacy proc emits no per-DB rec for
    /// them), so they get their own values for clarity but never actually collide.
    /// </para>
    /// </summary>
    public enum RecommendationSetting
    {
        None = 0,
        AutoShrink,
        AutoClose,
        QueryStore,
        Rcsi,
        PageVerify
    }

    /// <summary>
    /// A single unified Recommendations row — the view-model the surface renders. This is a
    /// plain DTO with NO WPF dependency so the reader's mapping + the pure de-dupe function
    /// over it are unit-testable without a UI or a database.
    ///
    /// <para>
    /// Built by <see cref="RecommendationsReader"/> from either an
    /// <see cref="AnalysisFinding"/> (<see cref="RecommendationSource.Engine"/>) or a
    /// legacy critical-issue row (<see cref="RecommendationSource.Legacy"/>). The
    /// <see cref="Remediation"/> action is present ONLY for engine rows that carry one
    /// (it drives Apply + the two-sided consent gate, wired in a later WS); legacy rows
    /// are advise/copy-paste only and leave it null.
    /// </para>
    /// </summary>
    public sealed class RecommendationItem
    {
        /// <summary>The three-band severity used for sorting and the badge/icon.</summary>
        public CanonicalSeverity CanonicalSeverity { get; set; }

        /// <summary>
        /// The raw severity used as the secondary sort within a band. Engine rows carry the
        /// finding's <c>double</c> (0–~2.0); legacy rows carry a synthesized stand-in derived
        /// from the text band (2.0 / 1.0 / 0.0) so the two scales sort together sensibly.
        /// </summary>
        public double RawSeverity { get; set; }

        /// <summary>The affected database, or null/empty for a server-scoped rec.</summary>
        public string? Database { get; set; }

        /// <summary>The one-line title / problem area shown as the row heading.</summary>
        public string Title { get; set; } = string.Empty;

        /// <summary>
        /// The problem-area grouping (engine: category; legacy: <c>problem_area</c>).
        /// Kept distinct from <see cref="Title"/> for grouping/filtering.
        /// </summary>
        public string ProblemArea { get; set; } = string.Empty;

        /// <summary>
        /// Operator-facing advice prose (engine: <see cref="FactAdvice"/> for the root fact;
        /// legacy: the row's <c>message</c>). Null when no advice is available.
        /// </summary>
        public string? AdviceText { get; set; }

        /// <summary>
        /// Copy-paste-ready SQL the operator can run themselves (engine: the
        /// <c>ALTER DATABASE</c> statements rebuilt from the persisted action's DB-config
        /// targets; legacy: the row's <c>investigate_query</c>). Null when none applies.
        /// </summary>
        public string? CopyPasteSql { get; set; }

        /// <summary>
        /// The built, persisted remediation action for Apply (engine only; null for legacy
        /// and for engine rows with no executable shape). Drives the in-app Apply button +
        /// the informed-consent gate. Lives in PerformanceMonitor.Analysis.
        /// </summary>
        public RemediationAction? Remediation { get; set; }

        /// <summary>Which producer this row came from.</summary>
        public RecommendationSource Source { get; set; }

        /// <summary>
        /// The engine finding's <c>story_path_hash</c>, used to mute the pattern. Engine rows
        /// only; null for legacy rows (the legacy store has no mute concept).
        /// </summary>
        public string? StoryPathHash { get; set; }

        /// <summary>
        /// The engine finding's <c>story_path</c> — the human-readable pattern path persisted on
        /// the mute record alongside <see cref="StoryPathHash"/> (the hash is the mute key; the
        /// path is the operator-facing label for any future un-mute UI). Engine rows only; null
        /// for legacy rows (the legacy store has no mute concept).
        /// </summary>
        public string? StoryPath { get; set; }

        /// <summary>
        /// The engine finding's incident id (correlate-and-focus) — the group key the surface
        /// collapses cards under, so related findings render as one report. Empty for legacy rows and
        /// for engine rows analyzed before incident_id existed; the view-model treats an empty id as a
        /// standalone single-card incident.
        /// </summary>
        public string IncidentId { get; set; } = string.Empty;

        /// <summary>
        /// The canonical de-dupe setting (or <see cref="RecommendationSetting.None"/>). See
        /// <see cref="RecommendationSetting"/>. Drives the (database, setting) de-dupe key.
        /// </summary>
        public RecommendationSetting Setting { get; set; }

        /// <summary>
        /// True for the four SERVER-LEVEL config cards (WS3: MAXDOP / CTFP / max &amp; min server
        /// memory). These are server-scoped (<see cref="Setting"/> stays
        /// <see cref="RecommendationSetting.None"/> so they never de-dupe with legacy per-db rows),
        /// but they are standing CONFIG FIXES — Copy fix (and, for MAXDOP/CTFP, Apply) — NOT
        /// time-bound incidents. The view-model treats this flag as a structured-fix signal so the
        /// two advise-only memory cards (which carry no <see cref="Remediation"/>) still read as
        /// config fixes (Copy fix, no incident affordances) rather than incidents.
        /// </summary>
        public bool IsServerConfigAdvisory { get; set; }

        /// <summary>
        /// True for a MISSING_INDEX advisory card (WS4). Like the advise-only server-config memory
        /// cards it is COPY-PASTE ONLY — the SQL Server-suggested CREATE INDEX rides
        /// <see cref="CopyPasteSql"/> while <see cref="Remediation"/> stays null (creating an index is
        /// a judgement call, so there is deliberately no Apply button). The card stays a
        /// <see cref="RecommendationSetting.None"/> incident with no structured fix action, so it keeps
        /// its Open-in-Active-Queries / Ask-AI affordances; this flag only tells the view-model to ALSO
        /// surface "Copy fix" for the CREATE statement.
        /// </summary>
        public bool IsMissingIndexAdvisory { get; set; }

        /// <summary>
        /// UTC start of the time window the finding pertains to (engine: the analysis
        /// <c>TimeRangeStart</c>; legacy: the <c>critical_issues.log_date</c>). Raw - no grace
        /// window is applied here; the navigation handler widens it (+/- 1h) and converts to
        /// server-local time when deep-linking to Active Queries. Null when the producer carried
        /// no time bound.
        /// </summary>
        public System.DateTime? WindowStartUtc { get; set; }

        /// <summary>
        /// UTC end of the finding time window (engine: the analysis <c>TimeRangeEnd</c>;
        /// legacy: the <c>critical_issues.log_date</c>, i.e. equal to <see cref="WindowStartUtc"/>).
        /// Raw - see <see cref="WindowStartUtc"/>.
        /// </summary>
        public System.DateTime? WindowEndUtc { get; set; }
    }
}
