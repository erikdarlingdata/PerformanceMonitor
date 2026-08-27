/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The SQL Server error numbers that mean "the monitoring login lacks a grant this collector needs",
/// in ONE place. Both runners and <c>ITargetProvider.Classify</c> ask this rather than carrying their
/// own copy of the list.
///
/// <para><b>Why this is a shared predicate and not six literals.</b> It already WAS six literals, and
/// they had already drifted — in three different directions. The SQL Server provider classified 916;
/// Darling's collector catch and Lite's <c>RunCollectorAsync</c> catch had 8189 but not 916; the two
/// failed-jobs msdb reads had 916 but not 8189; and Lite's XE-session catch had neither. So the engine
/// seam whose stated purpose is that "the runner and the provider cannot disagree about what an error
/// means" disagreed with itself four ways. Adding one number to six hand-maintained lists is how the
/// seventh site gets missed; deriving all of them from this set is how it cannot.</para>
///
/// <para><b>One set is right for all six because they all answer the same question</b> — "did the login
/// get told no?" — even though they act on it differently: the collector catches record PERMISSIONS
/// instead of ERROR, the failed-jobs reads return an empty list instead of failing the alert cycle, and
/// the XE catch marks the capture unavailable. What each one DOES with the answer is its own business;
/// what counts as a denial is not.</para>
///
/// <para><b>262 (#2512).</b> "%ls permission denied in database '%.*ls'" — the denial a database-scoped
/// DMV read raises when the login does not hold the permission IN the named database. It arrived with the
/// #2150 field report as "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'" on an
/// Azure SQL Database elastic pool, 11x consecutive, and was classified <c>Unclassified</c> — which
/// means ERROR, every cycle, forever. That is the collection-health pollution PERMISSIONS exists to
/// absorb: a least-privilege login that cannot read a source is an operator-actionable state, not a
/// monitoring fault. <see cref="TempDbStatsCollector"/> used to be gated off the whole Azure SQL Database
/// tier to avoid it; the gate is gone and this is what makes that safe.</para>
///
/// <para><b>Deliberately NOT here:</b> 208 (invalid object name) is
/// <see cref="CollectorTargetFault.ObjectMissing"/> — the remedy is install or upgrade, not grant —
/// and 1222 is a lock-timeout yield. 297 appears here even though it can ALSO mean "the Extended Events
/// session is gone": the worker raises its own exception type for that case before any classification
/// runs, so the ambiguity never reaches this set.</para>
/// </summary>
public static class SqlServerPermissionErrors
{
    /// <summary>
    /// True when <paramref name="sqlErrorNumber"/> is a permission denial rather than a fault.
    /// <list type="bullet">
    /// <item>229 — EXECUTE/SELECT permission denied on an object.</item>
    /// <item>262 — permission denied IN a database (the #2150/#2512 tempdb DMV case).</item>
    /// <item>297 — the user does not have permission to perform this action.</item>
    /// <item>300 — VIEW SERVER STATE denied; on Azure SQL Database a service-objective limit rather
    /// than a missing grant, which is what <c>AzureDmvPermissionHint</c> exists to say.</item>
    /// <item>916 — the principal cannot access the database under the current security context.</item>
    /// <item>8189 — sys.traces' own denial (ALTER TRACE missing), a legitimate least-privilege
    /// choice (#1823).</item>
    /// </list>
    /// </summary>
    public static bool IsPermissionDenied(int sqlErrorNumber)
        => sqlErrorNumber is 229 or 262 or 297 or 300 or 916 or 8189;
}
