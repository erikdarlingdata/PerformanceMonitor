/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The investigation family of an alert (#5632, ruling Q9): the grain an agent investigation is grouped on, one open
/// request per server, investigation family and 30-minute window. Finer than the notification families of
/// <see cref="AlertFamily"/> (which split the monitor's own health and the daily reports from everything else): blocking
/// and a slow query on one server are two incidents and start two agents.
/// <para>Six families from the issue's draft (blocking, query performance, resources, availability, jobs, self-monitor)
/// plus <see cref="Other"/> for custom and external alerts and for anything the map does not name. Reports and the
/// recoveries the product delivers as alerts never start an investigation: <see cref="Of"/> answers null for them.
/// Analysis findings take their family from their category (<see cref="AnalysisCategoryFamilies"/>).</para>
/// <para>The spellings are the store's: <c>investigation_requests.family</c> has a CHECK on exactly <see cref="All"/>.</para>
/// </summary>
public static class InvestigationFamily
{
    /// <summary>Blocking, lock waits and deadlocks.</summary>
    public const string Blocking = "blocking";

    /// <summary>Slow, regressed or failing queries and plans.</summary>
    public const string QueryPerformance = "query_performance";

    /// <summary>CPU, memory, I/O, tempdb and space.</summary>
    public const string Resources = "resources";

    /// <summary>Database state, availability groups, replication, reachability.</summary>
    public const string Availability = "availability";

    /// <summary>The <see cref="AlertFamily.AgentJobs"/> family.</summary>
    public const string Jobs = "jobs";

    /// <summary>The <see cref="AlertFamily.SelfMonitor"/> family: the monitor's own health.</summary>
    public const string SelfMonitor = "self_monitor";

    /// <summary>Custom and external alerts, analysis categories the map does not name, and unclassified metrics.</summary>
    public const string Other = "other";

    /// <summary>Every family, in the store CHECK's order.</summary>
    public static readonly IReadOnlyList<string> All = new[] { Blocking, QueryPerformance, Resources, Availability, Jobs, SelfMonitor, Other };

    /// <summary>
    /// The investigation family of every metric in the <see cref="AlertFamily.Performance"/> family of
    /// <see cref="AlertFamily.MetricFamilies"/>, by name. A null value is a recovery the product delivers as an alert:
    /// it never starts an investigation. The family test fails when a performance metric is added to
    /// <see cref="AlertFamily.MetricFamilies"/> without an entry here, so a new alert gets a deliberate family.
    /// The other notification families map whole (<see cref="Of"/>).
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string?> PerformanceMetricFamilies = new Dictionary<string, string?>(StringComparer.Ordinal)
    {
        ["Blocking Detected"] = Blocking,
        ["Blocking Wait Time"] = Blocking,
        ["Deadlocks Detected"] = Blocking,
        ["PostgreSQL Vacuum Horizon Blocked"] = Blocking,

        ["Long-Running Query"] = QueryPerformance,
        ["Forced Plan Failing"] = QueryPerformance,
        /* Not in the issue's draft: a client timeout is a query that ran past its caller's limit. */
        [AlertFamily.ClientTimeoutsMetric] = QueryPerformance,

        ["High CPU"] = Resources,
        ["Poison Wait"] = Resources,
        /* Not in the draft: the scheduler queue is CPU pressure, read the other way round. */
        ["Runnable Queue"] = Resources,
        ["tempdb Space"] = Resources,
        ["Version Store (PVS)"] = Resources,
        ["Volume Free Space"] = Resources,
        ["Database File Growth"] = Resources,

        ["Database State"] = Availability,
        /* Not in the draft: a missing backup or CHECKDB is a restorability gap, which sits beside Database State. */
        ["Backup Overdue"] = Availability,
        ["Log Backup Overdue"] = Availability,
        ["CHECKDB Overdue"] = Availability,
        ["AG Failover"] = Availability,
        ["AG Replica Disconnected"] = Availability,
        ["AG Sync Fell Behind"] = Availability,
        ["AG Database Suspended"] = Availability,
        ["PostgreSQL Wraparound Risk"] = Availability,
        /* Not in the draft: a slot that holds WAL is a replication health problem before it is a disk one. */
        ["PostgreSQL Replication Slot Retention"] = Availability,
        /* Not in the draft: the target cannot be reached. */
        ["Server Unreachable"] = Availability,

        /* The two recoveries the product delivers as alerts (AlertFamily.RecoveryPairs): never an investigation. */
        ["Server Restored"] = null,
        ["AG Replica Reconnected"] = null,
    };

    /// <summary>
    /// The investigation family of an analysis finding, by its category (<c>Analysis: {category} [{hash8}]</c>). The
    /// categories are the analysis graphs' edge and story categories (SQL Server <c>RelationshipGraph</c> and the
    /// PostgreSQL target graph). A category not named here is <see cref="Other"/>. Matched case-insensitively.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> AnalysisCategoryFamilies = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["blocking"] = Blocking,
        ["lock_contention"] = Blocking,
        ["deadlocking"] = Blocking,

        ["queries"] = QueryPerformance,
        ["query_performance"] = QueryPerformance,
        ["plans"] = QueryPerformance,
        ["plan_regression"] = QueryPerformance,
        ["parallelism"] = QueryPerformance,
        ["memory_grants"] = QueryPerformance,
        ["temp_spill"] = QueryPerformance,

        ["cpu_pressure"] = Resources,
        ["cpu_spike"] = Resources,
        ["cpu_burn"] = Resources,
        ["thread_exhaustion"] = Resources,
        ["memory_pressure"] = Resources,
        ["host_memory"] = Resources,
        ["io_pressure"] = Resources,
        ["log_io"] = Resources,
        ["latch_contention"] = Resources,
        ["tempdb_pressure"] = Resources,
        ["bloat_growth"] = Resources,
        ["object_growth"] = Resources,
        /* An anomaly is a spike against the server's own baseline in CPU, waits, I/O or latency, so resources is the
           closest family. One caused by blocking or a query story is still reported by the agent that gets it. */
        ["anomaly"] = Resources,
        /* A story's category is its root fact's source (InferenceEngine: rootFact.Source), so these SQL Server fact sources are
           the categories a live run emits. The edge categories above (cpu_pressure and the rest) only match a story whose
           category happens to equal one. config, database_config, coverage, bad_actor, tempdb and waits stay other. */
        ["cpu"] = Resources,
        ["memory"] = Resources,
        ["io"] = Resources,
        ["disk"] = Resources,

        ["jobs"] = Jobs,

        ["ag_health"] = Availability,
        ["replication_risk"] = Availability,
        /* Vacuum that cannot keep up ends in wraparound, which the draft puts in availability. */
        ["vacuum_starvation"] = Availability,
    };

    /// <summary>
    /// The investigation family of a delivered metric name, or null when the alert never starts an investigation (a
    /// report, or a recovery delivered as an alert). Resolution rows are not alerts and are the caller's to skip.
    /// </summary>
    public static string? Of(string metricName)
    {
        ArgumentNullException.ThrowIfNull(metricName);

        if (AlertFamily.RecoveryPairs.ContainsKey(metricName))
        {
            return null;
        }

        /* #6003: the alert about a request no agent took never opens another one, or an agent route matching every metric
           would expire, alert and open again without end. */
        if (string.Equals(metricName, AlertFamily.InvestigationUnansweredMetric, StringComparison.Ordinal))
        {
            return null;
        }

        /* #6551: a change watch's request (metric "Change Watch", opened by the change-watch pass and not by an alert) is a question about
           what a recorded change did to plans and query measures, so it is query performance. A forced-plan change is the common case; a
           configuration change is read through the same query measures. Without this it fell to `other`, next to custom and external
           alerts, and an operator that routes families to agents sent it to the agent that works those. The name is not in the notification
           census (it is not an alert), so it is mapped here and not in PerformanceMetricFamilies. */
        if (string.Equals(metricName, InvestigationTriggerKinds.ChangeWatchMetric, StringComparison.Ordinal))
        {
            return QueryPerformance;
        }

        if (PerformanceMetricFamilies.TryGetValue(metricName, out var named))
        {
            return named;
        }

        if (AlertFamily.TryGetAnalysisCategory(metricName, out var category))
        {
            return AnalysisCategoryFamilies.TryGetValue(category, out var fromCategory) ? fromCategory : Other;
        }

        if (!AlertFamily.MetricFamilies.TryGetValue(metricName, out var notificationFamily))
        {
            /* Custom ("Custom:"), external ("External:"), an analysis name without its hash, and any metric the
               notification census does not name. */
            return Other;
        }

        return notificationFamily switch
        {
            AlertFamily.Reports => null,
            AlertFamily.AgentJobs => Jobs,
            AlertFamily.SelfMonitor => SelfMonitor,
            _ => Other,
        };
    }

    /// <summary>Whether an alert with this metric name can start an investigation (<see cref="Of"/> is not null).</summary>
    public static bool StartsInvestigation(string metricName) => Of(metricName) is not null;
}
