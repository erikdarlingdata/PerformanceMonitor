/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The per-alert triage page's API (#2710, option 2): <c>GET /api/triage</c> assembles, ON READ, the context
/// an on-call person needs for one alert firing — the matching <c>config_alert_log</c> row(s), the recent
/// collection log, and the alert-type-relevant collected data — from what the store already holds. Nothing is
/// written anywhere when an alert links here (the webhook channels compute the URL from server + metric +
/// firing instant + dedup key, because the history row does not exist yet at payload-build time), so there is
/// no per-alert artifact to provision and nothing to GC; a link older than retention degrades to a page of
/// honest empties rather than a 404.
///
/// <para><b>Sections reuse the <c>/api/read</c> dispatch, never re-implement it.</b> Each alert-type-relevant
/// read runs through the SAME <see cref="DarlingWebEndpoints.BuildReadDispatch"/> handler the read surface
/// serves (bound from a synthetic query string), so there is zero SQL/projection drift between a triage
/// section and the corresponding read endpoint — the #1562 no-drift rule applied to this surface. The
/// window is anchored AT the firing instant via each tool's own <c>as_of</c> anchor (#2495), so a link
/// clicked hours later still shows the data around the incident, not around the click.</para>
///
/// <para><b>Degrade, never 500.</b> Every fallible step — server resolution, the alert-history match, each
/// section's read — is caught per-step and reported inside the page body (<c>notes</c> / a section
/// <c>error</c>), because the whole point of the link is to be useful when something is wrong. An alert type
/// with no mapping (a self-alert, a future metric) falls back to <see cref="DefaultSections"/>; an alert
/// whose context carried no Database/InvolvedObjects loses nothing here, because the page is keyed on
/// server + metric + time, not on incident fields. A read that does not apply to the server's engine answers
/// with its own not-collected/empty envelope, which the page renders as an honest empty.</para>
///
/// <para><b>Exposure.</b> Same posture as the rest of the web surface: the auth middleware (token→cookie +
/// CIDR, loopback exempt from CIDR only) runs before this route, and everything served here is already
/// reachable via <c>/api/read/*</c> — this endpoint adds assembly, not reach.</para>
/// </summary>
internal static class DarlingTriageEndpoint
{
    /// <summary>One triage section: a display title plus the <c>/api/read</c> read it runs and the fixed
    /// query params it binds (wire keys, exactly what the dispatch lambda reads — <c>hours</c>, <c>limit</c>,
    /// <c>top</c>, ...). The server and the <c>as_of</c> anchor are injected per request, EXCEPT on a
    /// <paramref name="FleetLevel"/> section: those reads take no server at all (#2768), so injecting one
    /// would bind a parameter the tool does not read and imply a scope the section does not have.</summary>
    internal sealed record TriageSection(
        string Title, string Read, IReadOnlyDictionary<string, string> Params, bool FleetLevel = false);

    private static TriageSection S(string title, string read, params (string Key, string Value)[] parameters) =>
        Section(title, read, fleetLevel: false, parameters);

    /// <summary>A FLEET-LEVEL section (#2768): a read that answers about the monitoring store or the whole
    /// fleet and therefore takes no <c>server</c>. Used by the store self-alert family, whose alerts fire
    /// under the synthetic <see cref="DarlingSelfAlertEvaluator.StoreServerLabel"/> (or the opted-in
    /// <c>peers.storeName</c> — #3500; either way, not a registered target) and have no per-server scope to
    /// drill into.</summary>
    private static TriageSection F(string title, string read, params (string Key, string Value)[] parameters) =>
        Section(title, read, fleetLevel: true, parameters);

    private static TriageSection Section(
        string title, string read, bool fleetLevel, params (string Key, string Value)[] parameters)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in parameters)
        {
            map[key] = value;
        }

        return new TriageSection(title, read, map, fleetLevel);
    }

    /// <summary>
    /// The resolution-title → firing-metric aliases (review catch on this PR): the active→inactive edge
    /// records <c>resolution.Title</c> — not the firing metric — into <c>config_alert_log.metric_name</c>
    /// (<c>DarlingSelfAlertEvaluator</c>'s resolution write), and the Alert History page's Triage link makes
    /// every one of those rows a reachable entry point. Without these keys a "CPU Resolved" row landed on the
    /// thin fallback, losing exactly the CPU drill-down that would confirm the recovery. Each alias maps to
    /// the SAME section list as its firing metric — confirming a recovery asks the same questions as
    /// investigating the firing, one window later. The AG and Server Unreachable/Restored families need no
    /// entry here: their recovery edges deliver under their own metric names, mapped directly below; Failed
    /// Agent Job has no resolution edge at all. Declared ABOVE <see cref="SectionsByMetric"/> deliberately:
    /// static field initializers run in declaration order, and the map builder reads this list.
    /// </summary>
    internal static readonly IReadOnlyList<(string Alias, string Canonical)> ResolutionAliases = new[]
    {
        ("CPU Resolved", "High CPU"),
        ("Blocking Cleared", "Blocking Detected"),
        ("Blocking Wait Cleared", "Blocking Wait Time"),
        ("Deadlocks Cleared", "Deadlocks Detected"),
        ("Poison Waits Cleared", "Poison Wait"),
        ("Long-Running Queries Cleared", "Long-Running Query"),
        ("tempdb Space Resolved", "tempdb Space"),
        ("Volume Free Space Resolved", "Volume Free Space"),
        ("Version Store (PVS) Resolved", "Version Store (PVS)"),
        ("Database File Growth Resolved", "Database File Growth"),
        ("Long-Running Jobs Cleared", "Long-Running Job"),
        ("Database State Resolved", "Database State"),
        ("Forced Plan Failing Resolved", "Forced Plan Failing"),
        /* The store family's recovery edges (#2768). Store Runtime Upgrade has no resolution edge — an
           in-place major upgrade is a one-shot event, not a condition that clears. */
        (DarlingSelfAlertEvaluator.DiskPressureResolvedMetric, DarlingSelfAlertEvaluator.DiskPressureMetric),
        ("Store Job Cadence Recovered", DarlingSelfAlertEvaluator.JobCadenceMetric),
        ("Compression Job Recovered", DarlingSelfAlertEvaluator.CompressionJobMetric),
        /* #3816: the other two policy families' recovery edges. The total_failures arm has none — it reports
           an EVENT ("N more failures since the previous sample"), not a condition that clears. */
        (DarlingSelfAlertEvaluator.RefreshJobRecoveredMetric, DarlingSelfAlertEvaluator.RefreshJobStuckMetric),
        (DarlingSelfAlertEvaluator.RetentionJobRecoveredMetric, DarlingSelfAlertEvaluator.RetentionJobStuckMetric),
        (DarlingSelfAlertEvaluator.StaleMuteResolvedMetric, DarlingSelfAlertEvaluator.StaleMuteMetric),
        (DarlingSelfAlertEvaluator.WebTlsCertRenewedMetric, DarlingSelfAlertEvaluator.WebTlsCertExpiryMetric),
        /* The store families that landed AFTER #2768 (#3833). Their resolution titles are triage entry
           points exactly like the five edges above — the history row records resolution.Title into
           metric_name — and each was falling to the per-server fallback because nothing folded it onto its
           firing. Their canonicals have no SectionsByMetric entry ON PURPOSE: the fleet-level arm of
           SectionsFor answers them through this same fold, which is what keeps this the last list that has
           to grow (a NEW store family needs its alias here, but no section mapping). */
        (DarlingSelfAlertEvaluator.RetentionHoldClearedMetric, DarlingSelfAlertEvaluator.RetentionHoldMetric),
        (DarlingSelfAlertEvaluator.CustomRuleHealthResolvedMetric, DarlingSelfAlertEvaluator.CustomRuleHealthMetric),
        (DarlingSelfAlertEvaluator.ToastSlackClearedMetric, DarlingSelfAlertEvaluator.ToastSlackMetric),
        (DarlingSelfAlertEvaluator.CheckpointerPressureRecoveredMetric, DarlingSelfAlertEvaluator.CheckpointerPressureMetric),
        (DarlingSelfAlertEvaluator.StoreSettingsResolvedMetric, DarlingSelfAlertEvaluator.StoreSettingsMetric),
        (DarlingSelfAlertEvaluator.RawPurgeOverHorizonClearedMetric, DarlingSelfAlertEvaluator.RawPurgeOverHorizonMetric),
    };

    /// <summary>
    /// The store self-alerts that fire under a MONITORED SERVER's name rather than the store's label — the
    /// self-monitor family's per-server members. The condition is about the monitor (a collector that stopped,
    /// a capture session that is gone, a collector whose own cost regressed) but it is scoped to ONE server,
    /// the alert carries that server's real name, and the per-server reads are exactly the drill-down an
    /// operator wants. They are named here because they are the EXCEPTION to the family rule in
    /// <see cref="IsFleetLevelStoreMetric"/>, and the exception list is the half that does not grow: the
    /// population that keeps growing is the fleet-level one (#3833), and nothing has to be added here when it
    /// does. Declared ABOVE <see cref="SectionsByMetric"/> because the map's alias loop calls
    /// <see cref="IsFleetLevelStoreMetric"/> during type initialization, and static fields initialize in
    /// declaration order.
    /// </summary>
    internal static readonly IReadOnlyList<string> PerServerSelfMonitorMetrics = new[]
    {
        "Collection Stopped",
        "Capture Down",
        "Collector Cost Regression",
    };

    /// <summary>
    /// The alert-type → relevant-reads mapping, keyed by the EXACT <c>MetricName</c> the alert engine fires
    /// (the same string the history row and the mute rules key on — <c>AlertEngine</c> /
    /// <c>DarlingSelfAlertEvaluator</c> / <c>PostgresAlertEvaluator</c> literals), plus the
    /// <see cref="ResolutionAliases"/> — see there for why a RESOLUTION row needs its own key. Hours are
    /// lookback BEFORE the firing instant (the per-request <c>as_of</c> anchors each window's END there),
    /// sized per signal: short for point-in-time state (active queries), a day for trends whose shape is the
    /// finding. A metric not listed here — a self-alert, or a metric added later — falls back to
    /// <see cref="DefaultSections"/> rather than failing, and the pinned test only guards that every read
    /// named HERE really dispatches.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, IReadOnlyList<TriageSection>> SectionsByMetric =
        BuildSectionsByMetric();

    private static Dictionary<string, IReadOnlyList<TriageSection>> BuildSectionsByMetric()
    {
        var map = new Dictionary<string, IReadOnlyList<TriageSection>>(StringComparer.OrdinalIgnoreCase)
        {
            /* "High CPU" fires for BOTH engines since #2719 (the PG alert reuses the same metric name and
               threshold), so the section list carries both engines' CPU reads: the wrong-engine one answers
               with its own not-collected envelope, which the page renders as an honest empty. */
            ["High CPU"] = new[]
            {
                S("CPU utilization", "get_cpu_utilization", ("hours", "4")),
                S("Instance CPU (Performance Insights)", "get_pg_cpu_utilization", ("hours", "4")),
                S("Top queries by CPU", "get_top_queries_by_cpu", ("hours", "2"), ("top", "10")),
                S("Scheduler pressure", "get_cpu_scheduler_pressure"),
            },
            ["Blocking Detected"] = new[]
            {
                S("Blocking chains", "get_blocking", ("hours", "4"), ("limit", "20")),
                S("Blocking per minute", "get_blocking_stats", ("hours", "4")),
                S("Active blocking queries", "get_active_queries", ("hours", "1"), ("blocking_only", "true"), ("limit", "25")),
            },
            ["Blocking Wait Time"] = new[]
            {
                S("Blocking chains", "get_blocking", ("hours", "4"), ("limit", "20")),
                S("Lock-wait trend", "get_lock_wait_trend", ("hours", "24")),
                S("Active blocking queries", "get_active_queries", ("hours", "1"), ("blocking_only", "true"), ("limit", "25")),
            },
            ["Deadlocks Detected"] = new[]
            {
                S("Deadlocks", "get_deadlocks", ("hours", "4"), ("limit", "10")),
                S("Deadlock detail", "get_deadlock_detail", ("hours", "4"), ("limit", "3")),
                S("Deadlock trend", "get_deadlock_trend", ("hours", "24")),
            },
            /* "Poison Wait" fires for BOTH engines since #2711 (PostgresAlertEvaluator.PoisonWaitMetric reuses
               the name and thresholds), so this carries both engines' wait reads - the #2719 High CPU shape. */
            ["Poison Wait"] = new[]
            {
                S("Top waits", "get_wait_stats", ("hours", "4"), ("limit", "20")),
                S("PostgreSQL waits", "get_pg_wait_stats", ("hours", "4"), ("limit", "20")),
                S("Waiting tasks", "get_waiting_tasks", ("hours", "1"), ("limit", "25")),
            },
            ["Long-Running Query"] = new[]
            {
                S("Active queries", "get_active_queries", ("hours", "1"), ("limit", "25")),
                S("Completed long queries", "get_long_query_completions", ("hours", "4"), ("limit", "20")),
            },
            ["tempdb Space"] = new[]
            {
                S("tempdb usage trend", "get_tempdb_trend", ("hours", "24")),
                S("Active queries", "get_active_queries", ("hours", "1"), ("limit", "25")),
            },
            ["Volume Free Space"] = new[]
            {
                S("Database sizes", "get_database_sizes"),
                S("File IO stats", "get_file_io_stats"),
            },
            ["Version Store (PVS)"] = new[]
            {
                S("PVS state", "get_pvs_stats", ("trend_hours_back", "24")),
                S("Active queries", "get_active_queries", ("hours", "1"), ("limit", "25")),
            },
            ["Database File Growth"] = new[]
            {
                S("Default trace events", "get_default_trace_events", ("hours", "24"), ("limit", "50")),
                S("Database sizes", "get_database_sizes"),
            },
            ["Long-Running Job"] = new[]
            {
                S("Running jobs", "get_running_jobs"),
                S("Active queries", "get_active_queries", ("hours", "1"), ("limit", "25")),
            },
            ["Failed Agent Job"] = new[]
            {
                S("Running jobs", "get_running_jobs"),
                S("Default trace events", "get_default_trace_events", ("hours", "24"), ("limit", "50")),
            },
            ["Forced Plan Failing"] = new[]
            {
                S("Plan corrections", "get_plan_corrections", ("hours", "24"), ("limit", "25")),
                S("Query Store regressions", "get_query_store_regressions", ("hours", "24"), ("limit", "20")),
            },
            ["Database State"] = new[]
            {
                S("Server summary", "get_server_summary"),
                S("Database sizes", "get_database_sizes"),
            },
            /* The AG family (#991): one shape for the whole family — the topology answers all of them. */
            ["AG Replica Disconnected"] = AgSections(),
            ["AG Replica Reconnected"] = AgSections(),
            ["AG Database Suspended"] = AgSections(),
            ["AG Data Movement Resumed"] = AgSections(),
            ["AG Failover"] = AgSections(),
            ["AG Sync Fell Behind"] = AgSections(),
            ["AG Sync Recovered"] = AgSections(),
            /* The connect edge (V20): collection health tells whether it is one collector or the box. */
            ["Server Unreachable"] = new[]
            {
                S("Collection health", "get_collection_health"),
                S("Server summary", "get_server_summary"),
            },
            ["Server Restored"] = new[]
            {
                S("Collection health", "get_collection_health"),
                S("Server summary", "get_server_summary"),
            },
            /* The store self-alert family (#2768). These fire under the synthetic
               DarlingSelfAlertEvaluator.StoreServerLabel, which is not a registered target, so the per-server
               reads the fallback used to run could never apply — they rendered three resolver errors on every
               store alert. One shape for the whole family, the AG precedent: get_store_metrics answers all of
               them (total_bytes + daily_growth for disk pressure, the objects[] rows' last_run_duration_ms /
               schedule_interval_ms / duration_vs_cadence_percent for job cadence — the alert's own detail text
               points at that same series — and per-object compression ratios for a stuck compression job),
               and collector cost is what DRIVES the volume all three are downstream of. */
            [DarlingSelfAlertEvaluator.DiskPressureMetric] = StoreSections(),
            [DarlingSelfAlertEvaluator.StoreUpgradeMetric] = StoreSections(),
            [DarlingSelfAlertEvaluator.JobCadenceMetric] = StoreSections(),
            [DarlingSelfAlertEvaluator.CompressionJobMetric] = StoreSections(),
            /* #3816: the same shape for the two families the self-heal now covers and for the failure arm —
               get_store_metrics answers all three (the objects[] background_job rows carry each job's
               last_run_duration_ms, schedule_interval_ms, total_runs and total_failures, which is the series
               the failure alert's own detail text points at), and collector cost is what drives the volume
               they are all downstream of. */
            [DarlingSelfAlertEvaluator.RefreshJobStuckMetric] = StoreSections(),
            [DarlingSelfAlertEvaluator.RetentionJobStuckMetric] = StoreSections(),
            [DarlingSelfAlertEvaluator.PolicyJobFailingMetric] = StoreSections(),

            /* #3306: the stale-mute alert is the one store-family member whose subject is the CONFIGURATION
               rather than the store's volume, so get_store_metrics answers nothing about it. The rule list is
               the drill-down, with enabled_only=false so a rule that lapsed or was disabled between the
               firing and the click is still visible rather than looking deleted. The alert history is the
               second half: what a stale mute costs is the alerts it suppressed, and a muted alert is still
               RECORDED, so the history is where the suppressed signal actually is. */
            [DarlingSelfAlertEvaluator.StaleMuteMetric] = new[]
            {
                F("Mute rules in force", "get_mute_rules", ("enabled_only", "false")),
                /* 168 hours because that is StaleMuteAge, and it is also this read's own ceiling. */
                F("Recent alerts (a muted alert is still recorded here)", "get_alert_history", ("hours", "168"), ("limit", "50")),
            },

            /* #3514: the web-dashboard TLS certificate expiry alert is config/host-shaped like the stale-mute
               one — get_store_metrics answers nothing about it, and renewing the certificate is an out-of-band
               step on the service host. The actionable facts (subject, thumbprint, expiry — or, for the #3517
               not-yet-valid arm of the same metric, the date the window opens) are in the alert detail; the
               history is the firing trail, so the operator can see when the warning began. */
            [DarlingSelfAlertEvaluator.WebTlsCertExpiryMetric] = new[]
            {
                F("Recent alerts (the certificate's subject, thumbprint and validity dates are in the alert detail)", "get_alert_history", ("hours", "168"), ("limit", "50")),
            },

            /* PostgreSQL alert family (PostgresAlertEvaluator). */
            ["PostgreSQL Wraparound Risk"] = new[]
            {
                S("Wraparound headroom", "get_pg_wraparound_risk", ("hours", "24")),
                S("Autovacuum health", "get_pg_autovacuum_health", ("hours", "24"), ("limit", "20")),
            },
            ["PostgreSQL Vacuum Horizon Blocked"] = new[]
            {
                S("xmin horizon holders", "get_pg_xmin_horizon", ("hours", "24")),
                S("Session states", "get_pg_session_states", ("hours", "24"), ("limit", "25")),
            },
            ["PostgreSQL Replication Slot Retention"] = new[]
            {
                S("Replication slots", "get_pg_replication_slots", ("hours", "24")),
                S("Replication stats", "get_pg_replication_stats", ("hours", "24"), ("limit", "25")),
            },
        };

        /* Alias AFTER the literals so each resolution title shares its firing metric's exact list. A
           canonical with no entry above is legitimate exactly when the fleet-level arm answers it (#3833) —
           SectionsFor folds the alias through IsFleetLevelStoreMetric and lands on the store sections, so
           copying nothing here still renders the pair identically. A canonical that is NEITHER mapped nor
           fleet-level is a construction error — a typo, or a per-server alias nobody wired — and failing the
           process at type initialization is louder than any test. */
        foreach (var (alias, canonical) in ResolutionAliases)
        {
            if (map.TryGetValue(canonical, out var sections))
            {
                map[alias] = sections;
            }
            else if (!IsFleetLevelStoreMetric(canonical))
            {
                throw new InvalidOperationException(
                    $"ResolutionAliases: '{alias}' folds to '{canonical}', which has no section mapping and " +
                    "is not a fleet-level store metric. Map it, fix the name, or add its family exception.");
            }
        }

        return map;
    }

    private static TriageSection[] AgSections() => new[]
    {
        S("Availability Group health", "get_ag_health"),
        S("Server summary", "get_server_summary"),
    };

    /// <summary>The store self-alert family's sections (#2768), all FLEET-LEVEL, so none binds a server.
    /// <c>get_store_metrics</c> twice: its summary (the store's size and growth, its largest objects first),
    /// and its background jobs as a list of their own, because since #3903 the summary carries the jobs in a
    /// nested list the card's table does not render, and the job-family alerts are about exactly those rows.
    /// <c>get_collector_cost</c> is what drives the ingest volume those numbers move with.</summary>
    private static TriageSection[] StoreSections() => new[]
    {
        F("Store size and growth", "get_store_metrics", ("days_back", "30")),
        F("Background jobs (failing first, then closest to their cadence)", "get_store_metrics",
            ("days_back", "30"), ("object_kind", StoreSelfMetrics.BackgroundJobObjectKind), ("limit", "25")),
        F("Collector cost (what drives store volume)", "get_collector_cost", ("days_back", "7")),
    };

    /// <summary>The fallback for a metric with no mapping — self-alerts, and any metric added after this map.
    /// A summary plus collection health is thin but always valid; the standing collection-log section below
    /// rides alongside on every page.</summary>
    internal static readonly IReadOnlyList<TriageSection> DefaultSections = new[]
    {
        S("Server summary", "get_server_summary"),
        S("Collection health", "get_collection_health"),
    };

    /// <summary>The standing section every triage page ends with, whatever the alert type — what the service
    /// itself was doing around the firing (gaps, YIELDED lock-timeouts, errors) is triage context for all of them.</summary>
    internal static readonly TriageSection CollectionLogSection =
        S("Recent collection log", "get_collection_log", ("hours", "2"), ("limit", "100"));

    /// <summary>
    /// Does this metric NAME say the alert is fleet-level — about the monitoring store itself rather than a
    /// monitored server (#3833)?
    ///
    /// <para><b>Why the name and not the request's server.</b> #2768 fixed this page for the store family by
    /// adding four names to <see cref="SectionsByMetric"/>, and every self-alert family that landed after it
    /// — the retention hold, the collector-cost digest, the fleet sweep rollup, the custom-rule health check,
    /// TOAST slack, checkpointer pressure, the analysis singles digest — arrived without an entry and
    /// silently reopened the original defect for its own metric: the page emitted the fleet-level note and
    /// then rendered two per-server sections under it, each reading "Could not resolve server". A list that
    /// has to be maintained in step with a growing taxonomy keeps falling behind; a rule does not. So the
    /// decision is derived from the <see cref="AlertFamily"/> census — the taxonomy a new alert cannot ship
    /// without being added to, because <c>NotificationRoutingTests</c> reds when it is not — and the four-name
    /// list is no longer what makes a store alert render correctly.</para>
    ///
    /// <para><b>Both store-fired families count.</b> <see cref="AlertFamily.SelfMonitor"/> is the monitor's own
    /// health and <see cref="AlertFamily.Reports"/> is the three daily documents the store composes about
    /// ITSELF and the fleet (the collector-cost digest, the fleet sweep rollup, the analysis singles digest) —
    /// all fired under the store's label, and the digest is the metric the #3833 report was filed against.
    /// <see cref="AlertFamily.Canonical"/> folds a delivered recovery name onto its firing, and
    /// <see cref="ResolutionAliases"/> covers the resolution TITLES the store writes to history, so an
    /// all-clear row ("Retention Hold Cleared", "Store TOAST Slack Cleared") is fleet-level exactly like the
    /// firing it clears — the #2768 lesson that "Store Disk Pressure" was mapped while its own resolution was
    /// not.</para>
    ///
    /// <para>PURE, and keyed on the metric alone: the arm is testable without a request, which a predicate
    /// reading the request-scoped server label would not be.</para>
    /// </summary>
    internal static bool IsFleetLevelStoreMetric(string? metricName)
    {
        if (string.IsNullOrWhiteSpace(metricName))
        {
            return false;
        }

        var trimmed = metricName.Trim();

        /* A resolution TITLE is not a delivered metric name, so the census does not know it; the alias table
           already states which firing each one clears, and that firing is what the family is read for. */
        foreach (var (alias, canonical) in ResolutionAliases)
        {
            if (string.Equals(alias, trimmed, StringComparison.OrdinalIgnoreCase))
            {
                trimmed = canonical;
                break;
            }
        }

        foreach (var perServer in PerServerSelfMonitorMetrics)
        {
            if (string.Equals(perServer, trimmed, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        var family = AlertFamily.Of(trimmed);
        return string.Equals(family, AlertFamily.SelfMonitor, StringComparison.Ordinal)
            || string.Equals(family, AlertFamily.Reports, StringComparison.Ordinal);
    }

    /// <summary>The sections for one metric: the exact-name mapping, else the store's own reads when the
    /// metric NAME says the alert is fleet-level (<see cref="IsFleetLevelStoreMetric"/> — #3833), else
    /// <see cref="DefaultSections"/>. The mapping stays an OVERRIDE layer rather than the gate: the six
    /// entries with tailored reads (disk pressure's store metrics, stale-mute's rule list, the TLS
    /// certificate's history) keep them, and every other store metric gets correct sections instead of the
    /// per-server fallback's two resolver errors. Null/blank (a hand-built URL) still falls back rather than
    /// erroring.</summary>
    internal static IReadOnlyList<TriageSection> SectionsFor(string? metricName)
    {
        if (string.IsNullOrWhiteSpace(metricName))
        {
            return DefaultSections;
        }

        if (SectionsByMetric.TryGetValue(metricName.Trim(), out var sections))
        {
            return sections;
        }

        return IsFleetLevelStoreMetric(metricName) ? s_fleetLevelStoreSections : DefaultSections;
    }

    /// <summary>The fleet-level fallback, held as ONE instance so a caller can compare identity the way the
    /// pins compare against <see cref="DefaultSections"/>.</summary>
    private static readonly IReadOnlyList<TriageSection> s_fleetLevelStoreSections = StoreSections();

    /// <summary>
    /// Is this link's <c>server</c> the label the fleet-level store self-alerts fire under (#2768)? Those
    /// alerts are ABOUT the monitoring store, which is not a monitored SQL Server and is not in the
    /// registry, so resolving it is guaranteed to fail. Recognising it lets the page skip resolution
    /// entirely — no page-level "Could not resolve server" warning for a server that was never supposed to
    /// resolve — and skip the standing per-server collection-log section, which needs a server to mean
    /// anything. Compared case-insensitively and trimmed, matching how the history filter and
    /// <see cref="SectionsFor"/> treat their inputs, because the label arrives back through a URL.
    ///
    /// <para>No longer strictly pure since #3500: an opted-in <c>peers.storeName</c> is a second spelling of
    /// the same fleet-level fact, read from the ambient <see cref="DarlingPeerDirectory"/> snapshot the
    /// worker and the MCP host both publish from the same file — the established channel for exactly this
    /// block, rather than a second config load. BOTH spellings stay recognised on an opted-in store on
    /// purpose: a channel still holds links minted before the opt-in, and a link is a promise. The
    /// documented cost of the ambient read is the field's stated non-goal — a storeName that shadows a
    /// monitored server's display name makes that server's links read fleet-level too, which is why the
    /// config comment says not to reuse one.</para>
    /// </summary>
    internal static bool IsFleetLevelStoreServer(string? server)
    {
        if (string.IsNullOrWhiteSpace(server))
        {
            return false;
        }

        var trimmed = server.Trim();
        if (string.Equals(trimmed, DarlingSelfAlertEvaluator.StoreServerLabel, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var storeName = DarlingPeerDirectory.Current.StoreName;
        return storeName.Length > 0 && string.Equals(trimmed, storeName, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>How far past the firing instant each section's window END sits, so the firing itself — and
    /// its immediate aftermath — is inside the window rather than being its exclusive upper bound.</summary>
    internal static readonly TimeSpan AnchorSlack = TimeSpan.FromMinutes(15);

    /// <summary>How far back from the anchor the alert-history match looks. Generous, because the link's
    /// timestamp is the DELIVERY instant and per-event splits can deliver a batch minutes after the sweep.
    /// Widened to <c>internal</c> so <see cref="AlertNotebookEndpoint"/> (#4222) reuses the SAME family
    /// lookback for its window math rather than copying the constant.</summary>
    internal static readonly TimeSpan AlertMatchLookback = TimeSpan.FromHours(24);

    /// <summary>
    /// PURE: resolves the link's <c>at</c> instant into (the anchor the page is ABOUT, the <c>as_of</c> value
    /// the section reads are anchored on). A missing/unparseable <c>at</c> anchors at now with a null
    /// <c>as_of</c> (each tool's own "window ends now" default). A parseable one is clamped to
    /// [now − slack ceiling handled by the tools themselves] going forward: the read anchor is
    /// <c>min(at + slack, now)</c>, and when that lands within a minute of now the <c>as_of</c> is omitted
    /// entirely — sending "now" as an explicit anchor buys nothing and risks the tools' future-anchor refusal
    /// on a skewed clock.
    /// </summary>
    internal static (DateTime AnchorUtc, string? AsOf) ResolveAnchor(string? at, DateTime nowUtc)
    {
        if (string.IsNullOrWhiteSpace(at)
            || !DateTime.TryParse(
                at, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
        {
            return (nowUtc, null);
        }

        var anchor = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        if (anchor > nowUtc)
        {
            anchor = nowUtc;
        }

        var readEnd = anchor + AnchorSlack;
        if (readEnd >= nowUtc - TimeSpan.FromMinutes(1))
        {
            return (anchor, null);
        }

        return (anchor, readEnd.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Maps <c>GET /api/triage</c>. Called once from <see cref="DarlingWebEndpoints.MapAll"/>, after the auth
    /// middleware like every sibling route; <paramref name="analysis"/> is the same shared instance the read
    /// dispatch receives (none of the mapped sections currently need it, but the dispatch signature does).
    /// </summary>
    public static void Map(WebApplication app, NpgsqlDataSource postgres, DarlingAnalysisService analysis, ILogger logger)
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();

        app.MapGet("/api/triage", async (HttpContext context) =>
        {
            var serverQuery = Query(context, "server");
            var metric = Query(context, "metric");
            var dedup = Query(context, "dedup");
            var now = DateTime.UtcNow;
            var (anchor, asOf) = ResolveAnchor(Query(context, "at"), now);

            var notes = new JsonArray();

            /* #2768: a store self-alert's server is the store's label — the synthetic StoreServerLabel, or
               #3500's opted-in peers.storeName — which cannot resolve by design either way. Recognise it up
               front and skip resolution rather than reporting a failure the operator can do nothing about —
               the sections this page then runs are fleet-level and take no server.
               Since #3833 the metric NAME is the second, equal arm: it is what decides the sections below, so
               reading it here too keeps the note, the skipped collection log and the section choice on ONE
               answer — and a store alert whose server param was lost or rewritten in transit through a
               channel still renders as the fleet-level alert it is. */
            var fleetLevelStore = IsFleetLevelStoreServer(serverQuery) || IsFleetLevelStoreMetric(metric);

            /* Server resolution — a failure is a NOTE, not a 500: the page still renders the alert-history
               match (fleet-wide) and whatever sections can answer without a resolvable server. */
            int? serverId = null;
            string? serverName = serverQuery;
            if (!fleetLevelStore && !string.IsNullOrWhiteSpace(serverQuery))
            {
                var resolveStopwatch = Stopwatch.StartNew();
                try
                {
                    var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverQuery);
                    if (error is null)
                    {
                        serverId = resolved.ServerId;
                        serverName = resolved.ServerName;
                    }
                    else if (error.StartsWith(DarlingServerResolver.RegistryReadFaultPrefix, StringComparison.Ordinal))
                    {
                        /* #4283 H2: the resolver's OWN store-fault sentence, not the caller's refusal — this is
                           the ONE branch below that can carry ex.Message (via LoadEnabledOrFaultAsync's catch), so
                           it is logged once and answered with the fixed generic/timeout note, never shown raw. */
                        DarlingWebFailureLog.Report(logger, "/api/triage:resolve-server", resolveStopwatch.ElapsedMilliseconds, error);
                        notes.Add((JsonNode)(DarlingWebFailureLog.IsStatementTimeoutSentence(error)
                            ? DarlingWebFailureLog.TimeoutMessage
                            : DarlingWebFailureLog.GenericMessage));
                    }
                    else
                    {
                        /* The resolver's miss is the `invalid` envelope since #3739; a note on this page is TEXT,
                           so the sentence is read back out of it rather than the JSON being shown as prose. This
                           is a client-correctable refusal, not a caught exception, so it is NOT #4283's target —
                           the resolver's own validator sentence, never ex.Message. (The resolver's OWN
                           store-fault sentence — the one that DOES carry ex.Message — is handled by the branch
                           above: logged once, never shown raw.) */
                        notes.Add((JsonNode)McpHelpers.ErrorMessageOf(error));
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    /* #4283: a fixed note, never ex.Message — the real text still reaches the service log. This
                       page answers 200 regardless (a degraded note, not a failed response), so only the TEXT
                       changes, not the status. */
                    DarlingWebFailureLog.Report(logger, "/api/triage:resolve-server", resolveStopwatch.ElapsedMilliseconds, ex);
                    notes.Add((JsonNode)"Server resolution failed. The service log names what failed.");
                }
            }

            /* The alert-history match: same-metric rows around the anchor, nearest first. The link's `at` is
               the delivery instant, so the nearest row at the top IS this firing whenever the row survived. */
            JsonNode? alert = null;
            var related = new JsonArray();
            var alertHistoryStopwatch = Stopwatch.StartNew();
            try
            {
                var until = anchor + AnchorSlack;
                if (until > now)
                {
                    until = now;
                }

                var rows = await DarlingAlertReader.GetAlertHistoryAsync(
                    postgres, anchor - AlertMatchLookback, until, serverId, 200, context.RequestAborted);

                var matched = new List<DarlingAlertReader.AlertHistoryReadRow>();
                foreach (var row in rows)
                {
                    if (string.IsNullOrWhiteSpace(metric)
                        || string.Equals(row.MetricName, metric.Trim(), StringComparison.OrdinalIgnoreCase))
                    {
                        matched.Add(row);
                    }
                }

                matched.Sort((a, b) =>
                    Math.Abs((a.AlertTime - anchor).Ticks).CompareTo(Math.Abs((b.AlertTime - anchor).Ticks)));

                for (var i = 0; i < matched.Count && i < 6; i++)
                {
                    var node = AlertRowNode(matched[i]);
                    if (i == 0)
                    {
                        alert = node;
                    }
                    else
                    {
                        related.Add(node);
                    }
                }

                if (alert is null)
                {
                    notes.Add((JsonNode)(
                        "No matching alert-history row was found near this instant - the row may have aged " +
                        "past retention, or the link predates delivery logging. The sections below still " +
                        "cover the window."));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* #4283: fixed note, real text to the service log — same reasoning as the resolver catch above. */
                DarlingWebFailureLog.Report(logger, "/api/triage:alert-history", alertHistoryStopwatch.ElapsedMilliseconds, ex);
                notes.Add((JsonNode)"Alert-history lookup failed. The service log names what failed.");
            }

            /* The alert-type-relevant sections + the standing collection log, each through the SAME
               /api/read dispatch handler the read surface serves, each failure captured per-section. */
            var sections = new JsonArray();
            foreach (var section in SectionsFor(metric))
            {
                sections.Add(await RunSectionAsync(section, dispatch, context, postgres, analysis, serverName, asOf, logger));
            }

            /* The standing per-server collection log rides along on every per-server page. It is SKIPPED for a
               fleet-level store alert (#2768): get_collection_log is keyed on a server, so with the synthetic
               label it could only ever answer with the resolver error this fix exists to remove. */
            if (!fleetLevelStore)
            {
                sections.Add(await RunSectionAsync(CollectionLogSection, dispatch, context, postgres, analysis, serverName, asOf, logger));
            }
            else
            {
                notes.Add((JsonNode)(
                    "This is a fleet-level alert about the monitoring store itself, not about a monitored " +
                    "server, so the per-server sections do not apply and are replaced by the store's own " +
                    "size, growth, background-job and collector-cost reads."));
            }

            var body = new JsonObject
            {
                ["metric"] = metric,
                ["server"] = serverName,
                ["server_id"] = serverId,
                ["fleet_level"] = fleetLevelStore,
                ["at"] = anchor.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
                ["as_of"] = asOf,
                ["dedup_key"] = dedup,
                ["alert"] = alert,
                ["related_alerts"] = related,
                ["notes"] = notes,
                ["sections"] = sections,
            };

            return Results.Text(body.ToJsonString(), "application/json");
        });
    }

    /// <summary>Runs one section through its <c>/api/read</c> dispatch handler (a synthetic query string over
    /// the REAL binding + tool code), returning <c>{title, read, data}</c> on success — <c>data</c> is the
    /// tool's own JSON, miss envelope included — or <c>{title, read, error}</c> when the tool refused the
    /// request (its <c>{"status":"invalid", ...}</c> envelope since #3739) or answered with a bare message,
    /// each reduced to its OWN sentence here because <c>error</c> on this page is TEXT the card renders. A tool
    /// that caught an exception (its <c>{"status":"error", ...}</c> envelope since #3653 Q11) or a
    /// binding-layer throw instead gets a fixed sentence (#4283: never <c>ex.Message</c>), and the real text
    /// goes to the service log once through <see cref="DarlingWebFailureLog.Report(ILogger,string,long,string)"/>.
    /// Never throws: a broken section is one card on the page, not a dead page.</summary>
    private static async Task<JsonObject> RunSectionAsync(
        TriageSection section,
        IReadOnlyDictionary<string, DarlingWebEndpoints.ReadToolHandler> dispatch,
        HttpContext requestContext,
        NpgsqlDataSource postgres,
        DarlingAnalysisService analysis,
        string? serverName,
        string? asOf,
        ILogger logger)
    {
        var result = new JsonObject { ["title"] = section.Title, ["read"] = section.Read };

        if (!dispatch.TryGetValue(section.Read, out var handler))
        {
            /* Unreachable while the pinned map↔dispatch test holds; reported honestly if it ever regresses. */
            result["error"] = $"Read '{section.Read}' is not served by this host.";
            return result;
        }

        var stopwatch = Stopwatch.StartNew();
        var route = "/api/triage:" + section.Read;
        try
        {
            var toolContext = new DefaultHttpContext
            {
                RequestAborted = requestContext.RequestAborted,
            };
            toolContext.Request.QueryString = new QueryString(BuildSectionQuery(section, serverName, asOf));

            var raw = await handler(toolContext, postgres, analysis);
            switch (DarlingWebEndpoints.ClassifyToolResponse(raw))
            {
                case DarlingWebEndpoints.ToolResponseKind.JsonPassthrough:
                    result["data"] = JsonNode.Parse(raw);
                    break;
                case DarlingWebEndpoints.ToolResponseKind.ServerError:
                    /* #4283: the tool caught its own exception; the envelope's sentence carries ex.Message and
                       is never shown on this card — logged once instead, same fixed wording ToHttpResult's
                       ServerError arm answers with (this page still answers 200 overall; only the card's text
                       degrades). */
                    var sentence = McpHelpers.ErrorMessageOf(raw);
                    DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, sentence);
                    result["error"] = DarlingWebFailureLog.IsStatementTimeoutSentence(sentence)
                        ? DarlingWebFailureLog.TimeoutMessage
                        : DarlingWebFailureLog.GenericMessage;
                    break;
                default:
                    /* Refusal / ClientError: a validator's or resolver's own sentence, client-correctable and
                       never ex.Message — shown as-is, same as the read surface's 400 body. True now specifically
                       because #4283 H2's ClassifyToolResponse reclassifies the resolver's OWN registry-read-fault
                       sentence as ServerError before this arm ever sees it; only a genuine refusal/miss sentence
                       reaches here. */
                    result["error"] = McpHelpers.ErrorMessageOf(raw);
                    break;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #4283: the binding-layer backstop (a throw, not a tool's own catch) — the real Exception is
               still here, so reported and answered from it directly rather than through the sentence
               classifier, exactly as the /api/read/* loop's own binding-layer catch does. */
            DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex);
            result["error"] = DarlingWebFailureLog.IsStatementTimeout(ex)
                ? DarlingWebFailureLog.TimeoutMessage
                : DarlingWebFailureLog.GenericMessage;
            return result;
        }

        return result;
    }

    /// <summary>PURE: the synthetic query string one section binds — its fixed params plus the injected
    /// <c>server</c> and <c>as_of</c> (each omitted when absent, so the tool sees its own default). A
    /// <see cref="TriageSection.FleetLevel"/> section never gets the server (#2768): its read does not take
    /// one, and on a store alert the only server available is a label that resolves to nothing.</summary>
    internal static string BuildSectionQuery(TriageSection section, string? serverName, string? asOf)
    {
        var builder = new StringBuilder(64);
        void Append(string key, string value)
        {
            builder.Append(builder.Length == 0 ? '?' : '&')
                .Append(Uri.EscapeDataString(key)).Append('=').Append(Uri.EscapeDataString(value));
        }

        if (!section.FleetLevel && !string.IsNullOrWhiteSpace(serverName))
        {
            Append("server", serverName);
        }

        if (!string.IsNullOrEmpty(asOf))
        {
            Append("as_of", asOf);
        }

        foreach (var (key, value) in section.Params)
        {
            Append(key, value);
        }

        return builder.Length == 0 ? "?" : builder.ToString();
    }

    /// <summary>One alert-history row in the SAME wire shape <c>get_alert_history</c> serves, so the page and
    /// any automation parse one shape whichever surface they read.</summary>
    private static JsonObject AlertRowNode(DarlingAlertReader.AlertHistoryReadRow row) => new()
    {
        ["alert_time"] = row.AlertTime.ToString("o", CultureInfo.InvariantCulture),
        ["server_id"] = row.ServerId,
        ["server_name"] = row.ServerName,
        ["metric_name"] = row.MetricName,
        ["current_value"] = row.CurrentValue,
        ["threshold_value"] = row.ThresholdValue,
        ["alert_sent"] = row.AlertSent,
        ["notification_type"] = row.NotificationType,
        ["send_error"] = row.SendError,
        ["muted"] = row.Muted,
        ["detail_text"] = row.DetailText,
    };

    private static string? Query(HttpContext context, string key)
    {
        var value = context.Request.Query[key].ToString();
        return string.IsNullOrEmpty(value) ? null : value;
    }
}
