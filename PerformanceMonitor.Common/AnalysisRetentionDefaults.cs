/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The horizon analysis findings are kept for, named once so no edition carries it as a literal.
///
/// <para>This is NOT the parquet archive horizon. Lite's <c>RetentionService</c> owns the archive's
/// 3-month calendar-month rolling window (the figure the README quotes); this bounds
/// <c>analysis_findings</c> in the hot store, and the two are deliberately independent — the findings
/// sweep is gated on the DuckDB handle the cleanup needs, not on the archive being configured at all.
/// Reading the README's archive figure as this one is the confusion this paragraph exists to stop.</para>
///
/// <para>Why a name rather than the literal it replaced: nothing scheduled the findings cleanup at all,
/// so <c>analysis_findings</c> grew until a size-triggered <c>ArchiveAllAndResetAsync</c> incidentally
/// wiped the WHOLE DuckDB — losing every finding rather than only aged ones, because
/// <c>analysis_findings</c> is not in <c>ArchiveService.ArchivableTables</c>, so routine archival never
/// touched it. A horizon that prevents that is worth being unable to drift, and the literal it replaced
/// appeared three times down one call chain — the scheduler's call site and the two cleanup defaults
/// beneath it — where the copies could disagree and only the call site would decide.</para>
///
/// <para>It lives in Common rather than beside <c>CollectorScheduleDefaults</c> because findings are not a
/// collector: the Darling retention comments already treat "not a collector, so no
/// CollectorScheduleDefaults entry to carry its horizon" as the rule, and a findings row in a
/// per-collector cadence table would be a phantom collector. Common is also the only assembly that both
/// SKUs' consumers and both test projects reference and grant internals to, which is what lets the
/// cross-edition identity below be a compile-time assertion rather than a source-text scrape.</para>
/// </summary>
internal static class AnalysisRetentionDefaults
{
    /// <summary>
    /// 30 days, and deliberately cross-edition: the Darling service's daily purge rides the same horizon
    /// (<c>DarlingRetention.DataRetentionBaseDays</c>). That agreement is asserted rather than assumed —
    /// Darling.Tests is the one project that can see both symbols and pins them equal, so moving one
    /// alone fails a build instead of diverging quietly.
    ///
    /// <para>The window itself is the correlation window: a finding is worth keeping exactly as long as
    /// the metric data it was drawn from, because a finding that outlives its evidence can no longer be
    /// re-examined and one that expires first leaves the evidence unexplained.</para>
    /// </summary>
    internal const int FindingsRetentionDays = 30;
}
