/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The spellings and grading the forced-plan-failure alert reasons about (#2157), fixed here so the
/// engine, both apps' stores, and the mute-rule surface reference one string each — the same discipline
/// <see cref="DatabaseStateTokens"/> exists for.
/// </summary>
public static class ForcePlanTokens
{
    /// <summary>The metric name every forced-plan-failure alert fires under (the AlertSeverity map key).</summary>
    public const string MetricName = "Forced Plan Failing";

    /// <summary>
    /// Per-object alert key prefix. The alerting unit is one FORCED PLAN, not one server: two plans
    /// failing on the same database are two independent conditions that resolve independently, so the
    /// cooldown and active-set keys have to carry the plan identity.
    /// </summary>
    public const string KeyPrefix = "forceplan:";

    /// <summary>The forcing types Query Store reports, both of which this alert covers.</summary>
    public const string Manual = "MANUAL";
    public const string Auto = "AUTO";

    /// <summary>
    /// Query Store's sentinel for "no failure recorded" in <c>last_force_failure_reason_desc</c>. Treated
    /// as no reason rather than a reason named NONE — a row can carry it while the counter still rose, and
    /// rendering "Reason: NONE" would read as though the engine declined to say.
    /// </summary>
    public const string NoFailureReason = "NONE";

    /// <summary>
    /// Severity for a failure rise. Deliberately WARNING for every rise, with no Critical tier:
    ///
    /// <para>A failing force is not an outage — the query still runs, on the optimizer's plan — so the
    /// honest grading is "somebody's mitigation is silently not working", which is a warning. A Critical
    /// tier would need evidence about which reasons or rates actually correlate with harm, and inventing
    /// thresholds without that evidence is how alert streams become noise nobody reads. If field data
    /// later shows a class that IS urgent (a MANUAL force failing on a hot query, say), grade it then and
    /// say what the data was.</para>
    /// </summary>
    public static AlertSeverityLevel SeverityFor(ForcePlanFailureInfo _) => AlertSeverityLevel.Warning;

    /// <summary>A human-friendly rendering of a failure reason (NO_INDEX → "NO INDEX"), or "unspecified"
    /// when Query Store recorded none.</summary>
    public static string HumanizeReason(string? reason)
    {
        var trimmed = reason?.Trim();
        if (string.IsNullOrEmpty(trimmed)
            || string.Equals(trimmed, NoFailureReason, System.StringComparison.OrdinalIgnoreCase))
        {
            return "unspecified";
        }

        return trimmed.Replace('_', ' ');
    }

    /// <summary>
    /// The per-plan identity used in alert keys and mute contexts. Includes the database because
    /// <c>query_id</c>/<c>plan_id</c> are only unique within one database's Query Store.
    /// </summary>
    public static string PlanKey(string databaseName, long queryId, long planId) =>
        $"{KeyPrefix}{databaseName}:{queryId}:{planId}";
}
