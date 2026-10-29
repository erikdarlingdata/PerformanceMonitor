/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The closed taxonomy of alert FAMILIES (#3598) — the user-facing grain a notification route matches on —
/// and the census that assigns every metric name the product delivers to exactly one of them.
///
/// <para><b>Why this exists as data rather than convention.</b> The four audiences were always there: a
/// "Compression Job Stuck" is about the monitor, a "Collector Cost Digest" is prose nobody should be paged
/// by, a "Failed Agent Job" is a job owner's problem and a "Deadlocks Detected" is the reason the channel
/// exists. Nothing in code said so — the send path consulted the metric name for its severity colour and for
/// nothing else — so a reader triaging one channel re-derived the audience on every post. Writing the
/// taxonomy down is design point 1 of the issue: the routing key is the metric name (already in hand at every
/// call site), and a family is a named set of metric names. Membership is pinned by a census test in
/// <c>Darling.Tests</c> that walks every metric constant the engines declare and every severity arm
/// <see cref="AlertSeverity.ForMetric"/> carries, so a new alert cannot ship without an author deciding
/// which audience it belongs to.</para>
///
/// <para><b>The fall-through is <see cref="Performance"/>, deliberately.</b> A name this census does not
/// know — a metric added after it, or a spelling the census missed — resolves to the pages family, which is
/// the family whose destination is the parent <c>config_notification</c> row's own channel set: the place
/// every alert went before routes existed. So an unclassified alert lands exactly where it landed yesterday,
/// never in a report channel someone stopped watching. The two dynamic name shapes are classified by prefix
/// for the same reason: <c>Custom:&lt;id&gt;</c> rules and <c>Analysis: &lt;category&gt; [&lt;hash&gt;]</c>
/// findings are user-authored predicates and engine findings over a monitored server's performance data,
/// both of which carry a severity and a remedy, so they are pages and not reports.</para>
///
/// <para><b>Recoveries pair with their firing</b> (design point 4). The shared engine's condition-cleared
/// notices ("Blocking Cleared", "CPU Resolved") never reach a channel — <c>AlertResolution</c> is a
/// history row and a log line by design — so the only recoveries a route can see are the ones the
/// self-alert evaluator FIRES as alerts under their own names: "Server Restored" and "AG Replica
/// Reconnected". <see cref="Canonical"/> maps each to the firing it clears so an exact-metric route on the
/// firing catches its recovery too, and a channel that saw the fire sees the clear.</para>
/// </summary>
public static class AlertFamily
{
    /// <summary>Alerts about the monitoring tool itself — its store, its collectors, its own jobs and
    /// certificates. The person who cares may not be the person watching production.</summary>
    public const string SelfMonitor = "self-monitor";

    /// <summary>Scheduled prose — the collector-cost digest and the fleet sweep rollup. Reports to read,
    /// never pages; nobody should be woken by one.</summary>
    public const string Reports = "reports";

    /// <summary>SQL Server Agent: failed and anomalously long jobs, and the Agent service itself being down.
    /// Real signal, routinely lower priority than a performance page, and famously chatty during maintenance
    /// windows.</summary>
    public const string AgentJobs = "agent-jobs";

    /// <summary>Everything about a monitored server's health that the on-call person is paged for:
    /// blocking, deadlocks, CPU, long-running queries, space, availability groups, connection loss, custom
    /// rules and analysis findings. The fall-through family for any name the census does not know.</summary>
    public const string Performance = "performance";

    /// <summary>Alerts another service raised through <c>config.submit_external_alert</c> (#5654), named
    /// <c>External:&lt;source&gt;:&lt;key&gt;</c>. Its own family so a route can send everything an outside system
    /// raises to its own channel; with no route it goes where every other alert goes, to the parent channels.</summary>
    public const string External = "external";

    /// <summary>The metric-name prefix of an external alert (#5654): <c>External:&lt;source&gt;:&lt;key&gt;</c>.</summary>
    public const string ExternalMetricPrefix = "External:";
    /// <summary>
    /// The Client Timeouts alert's metric name (#5640): the ONE declaration. The engine's evaluator
    /// (<c>AttentionEvaluator.MetricName</c>) takes it from here, because this assembly sits below the engine, and the
    /// severity map in this assembly uses it too. Only <c>AlertMetricClassifier</c> (a leaf assembly that cannot see
    /// this one) spells it as a literal, and a test pins that literal to this constant.
    /// </summary>
    public const string ClientTimeoutsMetric = "Client Timeouts";

    /// <summary>Every family, in the order the settings surfaces list them.</summary>
    public static readonly IReadOnlyList<string> All = new[] { SelfMonitor, Reports, AgentJobs, Performance, External };

    /// <summary>#6003, D8: the self-alert for an investigation request that expired with no agent on it and nowhere else to go.
    /// A WEBHOOK AUTOMATION KEY like every metric name, so it must stay stable across releases.</summary>
    public const string InvestigationUnansweredMetric = "Investigation Not Picked Up";

    /// <summary>
    /// The exact metric NAME → family census. Every entry is a string the engines deliver verbatim — the
    /// <c>AlertEngine</c>/<c>PostgresAlertEvaluator</c>/<c>AgAlertPolicy</c> constants and the self-alert
    /// evaluator's — spelled here as literals only because this assembly sits below the ones that declare
    /// them; the census test asserts each literal equals its declaring constant.
    /// </summary>

    public static readonly IReadOnlyDictionary<string, string> MetricFamilies = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        /* ---- performance: the shared engine's SQL Server alerts ---- */
        ["High CPU"] = Performance,
        ["Blocking Detected"] = Performance,
        ["Blocking Wait Time"] = Performance,
        ["Deadlocks Detected"] = Performance,
        ["Poison Wait"] = Performance,
        ["Runnable Queue"] = Performance,
        [ClientTimeoutsMetric] = Performance,
        ["Long-Running Query"] = Performance,
        ["tempdb Space"] = Performance,
        ["Volume Free Space"] = Performance,
        ["Version Store (PVS)"] = Performance,
        ["Database File Growth"] = Performance,
        ["Database State"] = Performance,
        ["Identity Capacity"] = Performance,
        ["Forced Plan Failing"] = Performance,

        /* ---- performance: the PostgreSQL-only alerts ---- */
        ["PostgreSQL Wraparound Risk"] = Performance,
        ["PostgreSQL Vacuum Horizon Blocked"] = Performance,
        ["PostgreSQL Replication Slot Retention"] = Performance,

        /* ---- performance: the Availability Group family (#991) ---- */
        ["AG Failover"] = Performance,
        ["AG Replica Disconnected"] = Performance,
        ["AG Replica Reconnected"] = Performance,
        ["AG Sync Fell Behind"] = Performance,
        ["AG Database Suspended"] = Performance,

        /* ---- performance: connection loss. About the TARGET being unreachable, which the on-call for that
           server wants paged for, even though the observer is the monitor. ---- */
        ["Server Unreachable"] = Performance,
        ["Server Restored"] = Performance,

        /* ---- agent-jobs ---- */
        ["Failed Agent Job"] = AgentJobs,
        ["Long-Running Job"] = AgentJobs,
        ["Agent Not Running"] = AgentJobs,
        ["Agent Alert Gaps"] = AgentJobs,

        /* ---- self-monitor: the monitor's own health (DarlingSelfAlertEvaluator) ---- */
        ["Collection Stopped"] = SelfMonitor,
        ["Capture Down"] = SelfMonitor,
        ["Compression Job Stuck"] = SelfMonitor,
        ["Store Disk Pressure"] = SelfMonitor,
        ["Store Runtime Upgrade"] = SelfMonitor,
        /* #5450: raised once per service start when the store shows the service was not collecting. */
        ["Collection Gap At Start"] = SelfMonitor,
        /* #5450: the daily retained-history audit: hours of a past day the store's rollups hold under half of usual. */
        ["Collection Gaps In History"] = SelfMonitor,
        ["Store Job Over Cadence"] = SelfMonitor,
        ["Retention Held"] = SelfMonitor,
        ["Custom Alert Rules Unhealthy"] = SelfMonitor,
        /* #5654: external alerts the drain could not deliver (expired, interrupted or failed rows). Its
           "External Alerts Delivered Again" recovery is a resolution row, like "Custom Alert Rules Recovered". */
        ["External Alerts Undelivered"] = SelfMonitor,
        /* #6003, D8: an investigation request expired with no agent on it, and its agent route has no fallback route and no
           human channel. Never an investigation itself (InvestigationFamily.Of). */
        [InvestigationUnansweredMetric] = SelfMonitor,
        ["Stale Mute Rules"] = SelfMonitor,
        ["Web TLS Certificate Expiring"] = SelfMonitor,
        /* #5288: the MCP endpoint's twin of the web certificate alert, its own metric so a mute or route for
           one never covers the other. */
        ["MCP TLS Certificate Expiring"] = SelfMonitor,
        /* #4732: collection skipped a share of the slots that came due over the last hour. */
        ["Collection Falling Behind"] = SelfMonitor,
        /* #4215: a managed store's darling-managed.conf fell back to the last-good copy, is
           hand-edited and kept in force, or PostgreSQL rejected an owned setting outright. */
        ["Store Settings Need Attention"] = SelfMonitor,
        ["Collector Cost Regression"] = SelfMonitor,
        /* #4299: the raw-retention over-horizon self-alert. */
        ["Raw Purge Over Horizon"] = SelfMonitor,
        /* #5867: hours of the Query Store compose rollup whose build is retried only once a day. */
        ["Compose Rollup Hours Stuck"] = SelfMonitor,
        /* #4750: a webhook channel that has failed three times in a row. */
        ["Notification Channel Failing"] = SelfMonitor,
        /* #3783: the store's own TOAST slack and checkpointer pressure — informational conditions about the
           monitor's store, so self-monitor, not reports: they are entered and left and write a resolution. */
        ["Store TOAST Slack"] = SelfMonitor,
        ["Store Checkpointer Pressure"] = SelfMonitor,
        /* #3816: the store's background-job self-heal covers every policy family, and each family pages
           under its own name so a mute or a route on one is not silently a mute on the others. Additions
           beside "Compression Job Stuck", which keeps its exact string — a metric name is the identity every
           history row, mute rule and route is keyed on, so renaming it would orphan all three. */
        ["Refresh Job Stuck"] = SelfMonitor,
        ["Retention Job Stuck"] = SelfMonitor,
        ["Store Job Failing"] = SelfMonitor,

        /* ---- reports: the three daily documents ---- */
        ["Collector Cost Digest"] = Reports,
        ["Fleet Sweep Rollup"] = Reports,
        /* #3712: the once-a-day copy of the analysis findings the corroboration gate routed away from a page. */
        ["Analysis Singles Digest"] = Reports,
    };

    /// <summary>
    /// The recoveries the product DELIVERS as alerts, each paired with the firing it clears. Only these two
    /// reach a channel under a name other than their firing's; every other recovery is a history row
    /// (<c>AlertResolution</c>, never routed). An exact-metric route on the firing catches the recovery.
    /// <para>The VALUE side is the incident's name, which is also the whole grain the pair is keyed on
    /// everywhere the lifecycle is named: the PagerDuty dedup key reads the value for the firing edge
    /// too (the firing's own name must equal the closing edge's, or the pair is two incidents again).</para>
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> RecoveryPairs = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["Server Restored"] = "Server Unreachable",
        ["AG Replica Reconnected"] = "AG Replica Disconnected",
    };

    /// <summary>
    /// The closing edge of a FIRING metric of a pair (<see cref="RecoveryPairs"/> read from its value side):
    /// "Server Unreachable" gives "Server Restored". False for a closing edge or any metric outside the pairs.
    /// </summary>
    public static bool TryGetRecoveryOf(string metricName, out string recovery)
    {
        foreach (var pair in RecoveryPairs)
        {
            if (string.Equals(pair.Value, metricName, StringComparison.Ordinal))
            {
                recovery = pair.Key;
                return true;
            }
        }

        recovery = "";
        return false;
    }

    /// <summary>
    /// The OTHER edge of the pair <paramref name="metricName"/> belongs to (#5469): the closing edge for a
    /// firing metric, the firing metric for a closing edge. False for a metric outside the pairs.
    /// </summary>
    public static bool TryGetPairedEdge(string metricName, out string other)
    {
        if (RecoveryPairs.TryGetValue(metricName, out var firing))
        {
            other = firing;
            return true;
        }

        return TryGetRecoveryOf(metricName, out other);
    }

    /// <summary>
    /// Whether <paramref name="metricName"/> belongs to the paired lifecycle on EITHER side: a closing edge
    /// (<see cref="RecoveryPairs"/> key) or a firing whose close re-keys onto it (a value). A future pair
    /// joins the checks that read this by naming its value — no second census, so the two directions of
    /// one pair cannot drift apart.
    /// </summary>
    public static bool IsPairedEdge(string metricName) =>
        RecoveryPairs.ContainsKey(metricName)
        || RecoveryPairs.Values.Any(value => string.Equals(value, metricName, StringComparison.Ordinal));

    /// <summary>The metric-name prefixes of the dynamic alert shapes, each with its family.</summary>
    public static readonly IReadOnlyList<(string Prefix, string Family)> PrefixFamilies = new[]
    {
        /* CustomAlertEvaluator.MetricNameFor: "Custom:" + rule id. */
        ("Custom:", Performance),
        /* ExternalAlertDrain.MetricNameFor (#5654): "External:" + source + ":" + key. */
        (ExternalMetricPrefix, External),
        /* FindingMessageFormatter.MetricName: "Analysis: {category} [{hash}]". */
        (AnalysisMetricPrefix, Performance),
    };

    /// <summary>The prefix of an analysis finding's metric name, <c>Analysis: {category} [{hash8}]</c>
    /// (<c>FindingMessageFormatter.MetricName</c>).</summary>
    public const string AnalysisMetricPrefix = "Analysis: ";

    /// <summary>
    /// The finding category out of an analysis metric name of the shape <c>Analysis: {category} [{hash8}]</c> (#5632).
    /// False for any other name, including a name that has the prefix but no bracketed hash suffix, so a digest
    /// title or a route's own <c>Analysis: {category}</c> match is never read as a finding.
    /// </summary>
    public static bool TryGetAnalysisCategory(string? metricName, out string category)
    {
        category = string.Empty;
        if (metricName is null
            || !metricName.StartsWith(AnalysisMetricPrefix, StringComparison.Ordinal)
            || !metricName.EndsWith(']'))
        {
            return false;
        }

        var bracket = metricName.LastIndexOf(" [", StringComparison.Ordinal);
        if (bracket < AnalysisMetricPrefix.Length)
        {
            return false;
        }

        var found = metricName[AnalysisMetricPrefix.Length..bracket].Trim();
        if (found.Length == 0)
        {
            return false;
        }

        category = found;
        return true;
    }

    /// <summary>Whether <paramref name="name"/> spells a family (case-insensitively, trimmed) — the test a
    /// route's <c>metric_match</c> is read through to decide whether it is a family route or an exact one.</summary>
    public static bool IsFamily(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        var trimmed = name.Trim();
        foreach (var family in All)
        {
            if (string.Equals(family, trimmed, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The family's canonical spelling for a name <see cref="IsFamily"/> accepts, or null.</summary>
    public static string? NormalizeFamily(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var trimmed = name.Trim();
        foreach (var family in All)
        {
            if (string.Equals(family, trimmed, StringComparison.OrdinalIgnoreCase))
            {
                return family;
            }
        }

        return null;
    }

    /// <summary>
    /// The firing metric a delivered name routes AS: a recovery's paired firing, else the name itself. A
    /// route on "Server Unreachable" catches "Server Restored" through this, which is what keeps a channel
    /// from seeing fires without clears (design point 4).
    /// </summary>
    public static string Canonical(string metricName)
    {
        if (metricName is null)
        {
            throw new ArgumentNullException(nameof(metricName));
        }

        return RecoveryPairs.TryGetValue(metricName, out var firing) ? firing : metricName;
    }

    /// <summary>
    /// The family of a delivered metric name: the census entry for its <see cref="Canonical"/> firing, else
    /// the prefix rule for the two dynamic shapes, else <see cref="Performance"/> — see the class summary for
    /// why the fall-through is the pages family and not an error.
    /// </summary>
    public static string Of(string metricName)
    {
        var canonical = Canonical(metricName);
        if (MetricFamilies.TryGetValue(canonical, out var family))
        {
            return family;
        }

        foreach (var (prefix, prefixFamily) in PrefixFamilies)
        {
            if (canonical.StartsWith(prefix, StringComparison.Ordinal))
            {
                return prefixFamily;
            }
        }

        return Performance;
    }

    /// <summary>
    /// A SQL boolean expression over <paramref name="column"/> that is true exactly when <see cref="Of"/> answers
    /// <paramref name="family"/> for the stored metric name (#6074), so the alert history read filters by family in
    /// the database, ahead of its row cap, rather than trimming a page after the cap. Spelled with the census
    /// as quoted LITERALS because the census is compiled-in code, never caller input, and so the one fragment
    /// serves both stores (PostgreSQL and DuckDB both have <c>starts_with</c>). A census entry decides first, then
    /// a prefix rule, then the fall-through, the order <see cref="Of"/> reads: the fall-through family
    /// (<see cref="Performance"/>) takes every name the census and the prefixes do not know. A recovery name
    /// (<c>Server Restored</c>) is in its firing's family. <paramref name="family"/> must be a value
    /// <see cref="NormalizeFamily"/> returned; any other value yields a predicate that matches nothing.
    /// </summary>
    public static string SqlFamilyPredicate(string column, string family)
    {
        if (column is null)
        {
            throw new ArgumentNullException(nameof(column));
        }

        if (NormalizeFamily(family) is not { } canonicalFamily)
        {
            return "(1 = 0)";
        }

        var census = MetricFamilies.Keys.Concat(RecoveryPairs.Keys).Distinct(StringComparer.Ordinal).ToList();
        var inFamily = census.Where(name => string.Equals(Of(name), canonicalFamily, StringComparison.Ordinal))
            .OrderBy(name => name, StringComparer.Ordinal).Select(SqlLiteral).ToList();
        var allCensus = census.OrderBy(name => name, StringComparer.Ordinal).Select(SqlLiteral).ToList();
        var prefixesIn = PrefixFamilies.Where(p => p.Family == canonicalFamily).Select(p => p.Prefix).Distinct(StringComparer.Ordinal).ToList();
        var prefixesAny = PrefixFamilies.Select(p => p.Prefix).Distinct(StringComparer.Ordinal).ToList();

        string Starts(IEnumerable<string> prefixes) =>
            "(" + string.Join(" OR ", prefixes.Select(prefix => "starts_with(" + column + ", " + SqlLiteral(prefix) + ")")) + ")";

        var arms = new List<string>();
        if (inFamily.Count > 0)
        {
            arms.Add(column + " IN (" + string.Join(", ", inFamily) + ")");
        }

        var uncensused = column + " NOT IN (" + string.Join(", ", allCensus) + ")";
        if (prefixesIn.Count > 0)
        {
            arms.Add("(" + uncensused + " AND " + Starts(prefixesIn) + ")");
        }

        if (string.Equals(canonicalFamily, Performance, StringComparison.Ordinal))
        {
            arms.Add("(" + uncensused + " AND NOT " + Starts(prefixesAny) + ")");
        }

        return arms.Count == 0 ? "(1 = 0)" : "(" + string.Join(" OR ", arms) + ")";
    }

    /// <summary>The trimmed <c>metric_name</c> and <c>family</c> inputs of an alert history read (#6074), null when blank.
    /// A family that spells no family keeps its trimmed text, and <see cref="SqlFamilyPredicate"/> matches nothing for it,
    /// so an unknown family reads as an EMPTY page whose status sentence names the filter rather than as an error.</summary>
    public static (string? MetricName, string? Family) NormalizeHistoryFilter(string? metricName, string? family)
    {
        var metric = string.IsNullOrWhiteSpace(metricName) ? null : metricName.Trim();
        var fam = string.IsNullOrWhiteSpace(family) ? null : (NormalizeFamily(family) ?? family.Trim());
        return (metric, fam);
    }

    /// <summary>The clause an empty alert history page appends to say which filters were on, e.g.
    /// <c> for metric_name 'High CPU' and family 'performance'</c>; empty when neither was set. A family that names no family
    /// says so, with the list of the real ones.</summary>
    public static string DescribeHistoryFilter(string? metricName, string? family)
    {
        var parts = new List<string>();
        if (metricName is not null)
        {
            parts.Add("metric_name '" + metricName + "'");
        }

        if (family is not null)
        {
            parts.Add("family '" + family + "'" + (IsFamily(family) ? string.Empty : " (not a family; the families are " + string.Join(", ", All) + ")"));
        }

        return parts.Count == 0 ? string.Empty : " for " + string.Join(" and ", parts);
    }

    private static string SqlLiteral(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    /// <summary>Whether the census names <paramref name="metricName"/> (after canonicalization) explicitly —
    /// by entry or by prefix — as opposed to answering through the fall-through.</summary>
    public static bool IsClassified(string metricName)
    {
        var canonical = Canonical(metricName);
        if (MetricFamilies.ContainsKey(canonical))
        {
            return true;
        }

        foreach (var (prefix, _) in PrefixFamilies)
        {
            if (canonical.StartsWith(prefix, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
