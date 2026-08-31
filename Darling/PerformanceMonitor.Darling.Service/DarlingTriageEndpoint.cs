/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;

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
    /// <c>top</c>, ...). The server and the <c>as_of</c> anchor are injected per request.</summary>
    internal sealed record TriageSection(string Title, string Read, IReadOnlyDictionary<string, string> Params);

    private static TriageSection S(string title, string read, params (string Key, string Value)[] parameters)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in parameters)
        {
            map[key] = value;
        }

        return new TriageSection(title, read, map);
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
            ["High CPU"] = new[]
            {
                S("CPU utilization", "get_cpu_utilization", ("hours", "4")),
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
            ["Poison Wait"] = new[]
            {
                S("Top waits", "get_wait_stats", ("hours", "4"), ("limit", "20")),
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

        /* Alias AFTER the literals so each resolution title shares its firing metric's exact list — a
           canonical named here but absent above is a construction error, and failing the process at type
           initialization is louder than any test. */
        foreach (var (alias, canonical) in ResolutionAliases)
        {
            map[alias] = map[canonical];
        }

        return map;
    }

    private static TriageSection[] AgSections() => new[]
    {
        S("Availability Group health", "get_ag_health"),
        S("Server summary", "get_server_summary"),
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

    /// <summary>The sections for one metric: the exact-name mapping, else <see cref="DefaultSections"/>.
    /// Null/blank (a hand-built URL) also falls back rather than erroring.</summary>
    internal static IReadOnlyList<TriageSection> SectionsFor(string? metricName) =>
        !string.IsNullOrWhiteSpace(metricName) && SectionsByMetric.TryGetValue(metricName.Trim(), out var sections)
            ? sections
            : DefaultSections;

    /// <summary>How far past the firing instant each section's window END sits, so the firing itself — and
    /// its immediate aftermath — is inside the window rather than being its exclusive upper bound.</summary>
    private static readonly TimeSpan AnchorSlack = TimeSpan.FromMinutes(15);

    /// <summary>How far back from the anchor the alert-history match looks. Generous, because the link's
    /// timestamp is the DELIVERY instant and per-event splits can deliver a batch minutes after the sweep.</summary>
    private static readonly TimeSpan AlertMatchLookback = TimeSpan.FromHours(24);

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
    public static void Map(WebApplication app, NpgsqlDataSource postgres, DarlingAnalysisService analysis)
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

            /* Server resolution — a failure is a NOTE, not a 500: the page still renders the alert-history
               match (fleet-wide) and whatever sections can answer without a resolvable server. */
            int? serverId = null;
            string? serverName = serverQuery;
            if (!string.IsNullOrWhiteSpace(serverQuery))
            {
                try
                {
                    var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverQuery);
                    if (error is null)
                    {
                        serverId = resolved.ServerId;
                        serverName = resolved.ServerName;
                    }
                    else
                    {
                        notes.Add((JsonNode)error);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    notes.Add((JsonNode)$"Server resolution failed: {ex.Message}");
                }
            }

            /* The alert-history match: same-metric rows around the anchor, nearest first. The link's `at` is
               the delivery instant, so the nearest row at the top IS this firing whenever the row survived. */
            JsonNode? alert = null;
            var related = new JsonArray();
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
                notes.Add((JsonNode)$"Alert-history lookup failed: {ex.Message}");
            }

            /* The alert-type-relevant sections + the standing collection log, each through the SAME
               /api/read dispatch handler the read surface serves, each failure captured per-section. */
            var sections = new JsonArray();
            foreach (var section in SectionsFor(metric))
            {
                sections.Add(await RunSectionAsync(section, dispatch, context, postgres, analysis, serverName, asOf));
            }

            sections.Add(await RunSectionAsync(CollectionLogSection, dispatch, context, postgres, analysis, serverName, asOf));

            var body = new JsonObject
            {
                ["metric"] = metric,
                ["server"] = serverName,
                ["server_id"] = serverId,
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
    /// tool's own JSON, envelope included — or <c>{title, read, error}</c> when the tool answered with a bare
    /// message or threw. Never throws: a broken section is one card on the page, not a dead page.</summary>
    private static async Task<JsonObject> RunSectionAsync(
        TriageSection section,
        IReadOnlyDictionary<string, DarlingWebEndpoints.ReadToolHandler> dispatch,
        HttpContext requestContext,
        NpgsqlDataSource postgres,
        DarlingAnalysisService analysis,
        string? serverName,
        string? asOf)
    {
        var result = new JsonObject { ["title"] = section.Title, ["read"] = section.Read };

        if (!dispatch.TryGetValue(section.Read, out var handler))
        {
            /* Unreachable while the pinned map↔dispatch test holds; reported honestly if it ever regresses. */
            result["error"] = $"Read '{section.Read}' is not served by this host.";
            return result;
        }

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
                default:
                    result["error"] = raw;
                    break;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            result["error"] = $"Error during {section.Read}: {ex.Message}";
        }

        return result;
    }

    /// <summary>PURE: the synthetic query string one section binds — its fixed params plus the injected
    /// <c>server</c> and <c>as_of</c> (each omitted when absent, so the tool sees its own default).</summary>
    internal static string BuildSectionQuery(TriageSection section, string? serverName, string? asOf)
    {
        var builder = new StringBuilder(64);
        void Append(string key, string value)
        {
            builder.Append(builder.Length == 0 ? '?' : '&')
                .Append(Uri.EscapeDataString(key)).Append('=').Append(Uri.EscapeDataString(value));
        }

        if (!string.IsNullOrWhiteSpace(serverName))
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
