/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// #6565: decides whether two analysis findings that share a <c>root_fact_key</c> and a database are the same root cause, for the
/// investigation brief's <c>same_root_cause</c> mark. Both stores' brief readers call it after their SQL returns the candidate and the
/// request's persisted <c>remediation_action_json</c>, so Darling and Lite compare one way.
/// </summary>
public static class InvestigationRootFactMatch
{
    /// <summary>The root fact key of a plan regression. It is server-wide: the key names no query, so key and database alone match
    /// every regressed query in the database.</summary>
    public const string PlanRegressionRootFactKey = "PLAN_REGRESSION";

    /// <summary>
    /// True when the finding behind a candidate earlier report is the same root cause as the request's finding. The SQL has already matched
    /// the root fact key and database. For every key but <see cref="PlanRegressionRootFactKey"/> that is the whole test by design (the
    /// key is not always a per-subject identity: some keys name a kind of fact, so the same key in the same database counts as the same
    /// root cause), and this returns true without reading the actions. A plan regression matches only when both findings share a force-plan target: the same database
    /// and the same Query Store query id. A finding with no force targets (or an unreadable action) matches nothing.
    /// </summary>
    public static bool IsSameRootCause(string? rootFactKey, string? requestRemediationJson, string? candidateRemediationJson)
    {
        if (!string.Equals(rootFactKey, PlanRegressionRootFactKey, StringComparison.Ordinal))
            return true;

        var mine = AlertContextSerializer.DeserializeAction(requestRemediationJson)?.Targets;
        var theirs = AlertContextSerializer.DeserializeAction(candidateRemediationJson)?.Targets;
        if (mine is null || theirs is null || mine.Count == 0 || theirs.Count == 0)
            return false;

        return mine.Any(m => theirs.Any(t =>
            m.QueryId == t.QueryId && string.Equals(m.Database, t.Database, StringComparison.OrdinalIgnoreCase)));
    }
}
