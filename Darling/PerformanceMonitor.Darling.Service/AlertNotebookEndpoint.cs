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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// <c>GET /api/alert-notebook</c> (#4222 slice C): the server half of the alert -&gt; notebook MVP. Turns an
/// alert link's <c>server</c> + <c>metric</c> + <c>at</c> + <c>dedup</c> into a bound, read-only notebook
/// definition — the mechanical conversion of <see cref="DarlingTriageEndpoint.SectionsFor"/>'s per-metric
/// reads into <c>{type:"read"}</c> cells, plus the alert this link is ABOUT (dedup-matched, not just
/// nearest-in-time) and a four-arm status answering whether the condition is still live.
///
/// <para><b>Reuses <see cref="DarlingTriageEndpoint"/>, never copies it.</b> The anchor math
/// (<see cref="DarlingTriageEndpoint.ResolveAnchor"/>), the resolution-title aliasing
/// (<see cref="DarlingTriageEndpoint.ResolutionAliases"/>) and the per-metric read list
/// (<see cref="DarlingTriageEndpoint.SectionsFor"/> / <see cref="DarlingTriageEndpoint.DefaultSections"/>) are
/// the SAME members that page already exposed as <c>internal</c>; this endpoint widens nothing new beyond the
/// two lookback constants below, both already declared on that type.</para>
///
/// <para><b>Templates are mechanical for every metric in this slice.</b> The authored blocking/deadlock
/// templates (forensic multi-read layouts) are a LATER slice; every metric here gets <c>mechanical/&lt;metric&gt;</c>
/// v1 — a header cell, a status cell, then one read cell per <see cref="DarlingTriageEndpoint.SectionsFor"/>
/// entry (or the fallback), each carrying <c>server</c>, <c>as_of</c> and an explicit <c>hours</c>/<c>limit</c>.
/// No composed cells are emitted here, so no cell carries an absolute <c>range</c> yet — that is this slice's
/// honest gap, not an oversight (composed panels aren't part of <see cref="DarlingTriageEndpoint.TriageSection"/>
/// at all).</para>
///
/// <para><b>Degrade, never error (#2710).</b> A stale or empty firing — no matching history row, no sections
/// with data — still answers 200 with honest empty cells and a note, exactly like the triage page it sits
/// beside.</para>
/// </summary>
internal static class AlertNotebookEndpoint
{
    /// <summary>The template id/version pair every mechanical conversion carries. Bumped only if the SHAPE of
    /// the mechanical conversion below changes; a new metric added to <see cref="DarlingTriageEndpoint.SectionsByMetric"/>
    /// does not bump it, because the conversion rule — not the per-metric read list — is what "mechanical/…"
    /// versions.</summary>
    internal const int MechanicalTemplateVersion = 1;

    /// <summary>Maps <c>GET /api/alert-notebook</c>. Called once from <see cref="DarlingWebEndpoints.MapAll"/>,
    /// after the auth middleware like every sibling route.</summary>
    public static void Map(WebApplication app, NpgsqlDataSource postgres, DarlingAnalysisService analysis, ILogger logger)
    {
        app.MapGet("/api/alert-notebook", async (HttpContext context) =>
        {
            var serverQuery = Query(context, "server");
            var metric = Query(context, "metric");
            var dedup = Query(context, "dedup");
            var now = DateTime.UtcNow;
            var (anchor, asOf) = DarlingTriageEndpoint.ResolveAnchor(Query(context, "at"), now);

            var notes = new JsonArray();

            var fleetLevelStore = DarlingTriageEndpoint.IsFleetLevelStoreServer(serverQuery)
                || DarlingTriageEndpoint.IsFleetLevelStoreMetric(metric);

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
                        DarlingWebFailureLog.Report(logger, "/api/alert-notebook:resolve-server", resolveStopwatch.ElapsedMilliseconds, error);
                        notes.Add((JsonNode)(DarlingWebFailureLog.IsStatementTimeoutSentence(error)
                            ? DarlingWebFailureLog.TimeoutMessage
                            : DarlingWebFailureLog.GenericMessage));
                    }
                    else
                    {
                        notes.Add((JsonNode)McpHelpers.ErrorMessageOf(error));
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    DarlingWebFailureLog.Report(logger, "/api/alert-notebook:resolve-server", resolveStopwatch.ElapsedMilliseconds, ex);
                    notes.Add((JsonNode)"Server resolution failed. The service log names what failed.");
                }
            }

            /* window_end = min(at + 15 min, now); window_start = min(Incident Since, at) - family lookback.
               Incident Since is only known once the alert match below picks a row, so window_start is
               finalized after that match — anchor - AlertMatchLookback is the provisional value used to
               scope the match query itself (identical to the triage endpoint's own window). */
            var windowEnd = anchor + DarlingTriageEndpoint.AnchorSlack;
            if (windowEnd > now)
            {
                windowEnd = now;
            }

            JsonNode? alertNode = null;
            AlertIncident? matchedIncident = null;
            DarlingAlertReader.AlertHistoryReadRow? matchedRow = null;
            var alertHistoryStopwatch = Stopwatch.StartNew();
            List<DarlingAlertReader.AlertHistoryReadRow> historyRows = new();
            try
            {
                historyRows = await DarlingAlertReader.GetAlertHistoryAsync(
                    postgres, anchor - DarlingTriageEndpoint.AlertMatchLookback, windowEnd, serverId, 200, context.RequestAborted);

                var sameMetric = new List<DarlingAlertReader.AlertHistoryReadRow>();
                foreach (var row in historyRows)
                {
                    if (string.IsNullOrWhiteSpace(metric)
                        || string.Equals(row.MetricName, metric.Trim(), StringComparison.OrdinalIgnoreCase))
                    {
                        sameMetric.Add(row);
                    }
                }

                /* Dedup wins over nearest-in-time: scan every same-metric row's incidents for the link's key
                   before falling back to the nearest row. */
                if (!string.IsNullOrWhiteSpace(dedup))
                {
                    foreach (var row in sameMetric)
                    {
                        if (!AlertContextSerializer.TryDeserialize(row.ContextJson, out var ctx)
                            || ctx.Incidents is not { Count: > 0 })
                        {
                            continue;
                        }

                        foreach (var incident in ctx.Incidents)
                        {
                            if (string.Equals(incident.DedupKey, dedup.Trim(), StringComparison.Ordinal))
                            {
                                matchedRow = row;
                                matchedIncident = incident;
                                break;
                            }
                        }

                        if (matchedRow is not null)
                        {
                            break;
                        }
                    }
                }

                if (matchedRow is null && sameMetric.Count > 0)
                {
                    sameMetric.Sort((a, b) =>
                        Math.Abs((a.AlertTime - anchor).Ticks).CompareTo(Math.Abs((b.AlertTime - anchor).Ticks)));
                    matchedRow = sameMetric[0];
                    if (AlertContextSerializer.TryDeserialize(matchedRow.ContextJson, out var ctx) && ctx.Incidents is { Count: > 0 })
                    {
                        matchedIncident = ctx.Incidents[0];
                    }
                }

                if (matchedRow is not null)
                {
                    alertNode = AlertRowNode(matchedRow, matchedIncident);
                }
                else
                {
                    notes.Add((JsonNode)(
                        "No matching alert-history row was found near this instant - the row may have aged " +
                        "past retention, or the link predates delivery logging. The cells below still cover " +
                        "the window."));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/alert-notebook:alert-history", alertHistoryStopwatch.ElapsedMilliseconds, ex);
                notes.Add((JsonNode)"Alert-history lookup failed. The service log names what failed.");
            }

            var incidentSince = matchedIncident?.IncidentStartedUtc ?? matchedRow?.AlertTime;
            var windowStart = anchor - DarlingTriageEndpoint.AlertMatchLookback;
            if (incidentSince is DateTime since && since < anchor)
            {
                windowStart = since - DarlingTriageEndpoint.AlertMatchLookback;
            }

            var status = await ResolveStatusAsync(
                postgres, serverId, metric, anchor, now, historyRows, matchedRow, logger, context.RequestAborted);

            var lookbackHours = FamilyLookbackHours(metric);
            var sections = DarlingTriageEndpoint.SectionsFor(metric);
            var cells = new JsonArray
            {
                HeaderCell(metric, serverName, matchedIncident, matchedRow),
                StatusCell(status),
            };

            foreach (var section in sections)
            {
                cells.Add(ReadCell(section, serverName, asOf, lookbackHours));
            }

            var body = new JsonObject
            {
                ["alert"] = alertNode,
                ["status"] = status,
                ["notes"] = notes,
                ["template"] = new JsonObject
                {
                    ["id"] = "mechanical/" + (string.IsNullOrWhiteSpace(metric) ? "default" : metric.Trim()),
                    ["version"] = MechanicalTemplateVersion,
                },
                ["definition"] = new JsonObject
                {
                    ["kind"] = "notebook",
                    ["cells"] = cells,
                },
            };

            return Results.Text(body.ToJsonString(), "application/json");
        });
    }

    /// <summary>The header cell every mechanical template opens with: a markdown-shaped read-only summary of
    /// the alert this notebook is bound to. Not a <c>read</c> cell — it carries nothing
    /// <see cref="DarlingWebEndpoints.BuildReadDispatch"/> would recognise — so the SPA render slice renders
    /// it directly rather than dispatching it.</summary>
    private static JsonObject HeaderCell(
        string? metric, string? serverName, AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row)
    {
        var cell = new JsonObject
        {
            ["type"] = "header",
            ["title"] = string.IsNullOrWhiteSpace(metric) ? "Alert" : metric,
            ["server"] = serverName,
        };

        if (row is not null)
        {
            cell["alert_time"] = row.AlertTime.ToString("o", CultureInfo.InvariantCulture);
        }

        if (incident is not null)
        {
            cell["incident_since"] = incident.IncidentStartedUtc?.ToString("o", CultureInfo.InvariantCulture);
            cell["involved_objects"] = string.Join(", ", incident.InvolvedObjects);
            cell["database"] = incident.Database;
            cell["total_occurrences"] = incident.TotalOccurrences;
        }

        return cell;
    }

    /// <summary>The status cell: same shape as <see cref="HeaderCell"/> — a directly-rendered cell, not a
    /// <c>read</c> cell.</summary>
    private static JsonObject StatusCell(string status) => new()
    {
        ["type"] = "status",
        ["title"] = "Status",
        ["status"] = status,
    };

    /// <summary>Converts one <see cref="DarlingTriageEndpoint.TriageSection"/> into a <c>{type:"read"}</c> cell
    /// — the mechanical conversion every metric gets in this slice. <c>hours</c> is the SectionsFor entry's own
    /// declared value where it has one, else the family lookback; <c>limit</c> is the entry's own value where
    /// declared, else a fixed default, because every read cell must carry an explicit limit (the #4222 budget
    /// pin).</summary>
    private static JsonObject ReadCell(
        DarlingTriageEndpoint.TriageSection section, string? serverName, string? asOf, string lookbackHours)
    {
        var parameters = new JsonObject();
        if (!section.FleetLevel && !string.IsNullOrWhiteSpace(serverName))
        {
            parameters["server"] = serverName;
        }

        if (!string.IsNullOrEmpty(asOf))
        {
            parameters["as_of"] = asOf;
        }

        var hasHours = false;
        var hasLimit = false;
        foreach (var (key, value) in section.Params)
        {
            parameters[key] = value;
            hasHours |= string.Equals(key, "hours", StringComparison.Ordinal);
            hasLimit |= string.Equals(key, "limit", StringComparison.Ordinal);
        }

        if (!hasHours)
        {
            parameters["hours"] = lookbackHours;
        }

        if (!hasLimit)
        {
            parameters["limit"] = DefaultMechanicalCellLimit;
        }

        return new JsonObject
        {
            ["type"] = "read",
            ["read"] = section.Read,
            ["params"] = parameters,
            ["viz"] = "table",
            ["title"] = section.Title,
        };
    }

    /// <summary>The fixed limit a mechanical read cell gets when its <see cref="DarlingTriageEndpoint.TriageSection"/>
    /// declared none — generous for a drill-down table, bounded against an unbounded read on a busy SQL
    /// Server.</summary>
    private const string DefaultMechanicalCellLimit = "50";

    /// <summary>The family lookback (hours, as the wire string a read cell's <c>hours</c> param takes) used
    /// when a section declares none of its own. One flat value across every metric in this slice — the
    /// per-metric tuning <see cref="DarlingTriageEndpoint.SectionsByMetric"/> already carries on most entries
    /// is respected first; this is only the mechanical conversion's own fallback.</summary>
    private static string FamilyLookbackHours(string? metric) => "24";

    /// <summary>
    /// The four status arms (#4222): a resolution row for this metric after <c>at</c> -&gt; "Resolved at T";
    /// else a later same-metric firing -&gt; "Fired again at T"; else, if the metric's collector has run since
    /// <c>at</c> -&gt; "No resolution recorded"; else "Unknown (not collected since T)". NEVER "ongoing" — an
    /// absence of rows is not evidence while the instrument is down.
    /// </summary>
    private static async Task<string> ResolveStatusAsync(
        NpgsqlDataSource postgres, int? serverId, string? metric, DateTime anchor, DateTime now,
        List<DarlingAlertReader.AlertHistoryReadRow> historyRows,
        DarlingAlertReader.AlertHistoryReadRow? matchedRow, ILogger logger, System.Threading.CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(metric))
        {
            return "Unknown (not collected since " + anchor.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) + ")";
        }

        var trimmedMetric = metric.Trim();

        /* Arm 1: a resolution row for THIS metric after `at`. ResolutionAliases maps a resolution TITLE to
           its firing metric; a resolution row's own metric_name is the alias, not the canonical name, so the
           search is over every alias that folds onto this metric plus the metric's own resolved-title
           siblings the alert engine may write directly. */
        DateTime? resolvedAt = null;
        foreach (var row in historyRows)
        {
            if (row.AlertTime <= anchor)
            {
                continue;
            }

            foreach (var (alias, canonical) in DarlingTriageEndpoint.ResolutionAliases)
            {
                if (string.Equals(canonical, trimmedMetric, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(row.MetricName, alias, StringComparison.OrdinalIgnoreCase))
                {
                    if (resolvedAt is null || row.AlertTime < resolvedAt)
                    {
                        resolvedAt = row.AlertTime;
                    }
                }
            }
        }

        if (resolvedAt is DateTime resolved)
        {
            return "Resolved at " + resolved.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        }

        /* Arm 2: a later same-metric firing. */
        DateTime? refiredAt = null;
        foreach (var row in historyRows)
        {
            if (row.AlertTime <= anchor)
            {
                continue;
            }

            if (string.Equals(row.MetricName, trimmedMetric, StringComparison.OrdinalIgnoreCase)
                && (matchedRow is null || row.AlertTime != matchedRow.AlertTime))
            {
                if (refiredAt is null || row.AlertTime < refiredAt)
                {
                    refiredAt = row.AlertTime;
                }
            }
        }

        if (refiredAt is DateTime refired)
        {
            return "Fired again at " + refired.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        }

        /* Arms 3/4: collector freshness since `at`. get_collection_log's own store (v_collection_log) is read
           directly here — a fleet-level store metric has no server, so a null serverId reads "not collected"
           honestly rather than faking a server scope. */
        var anchorStamp = anchor.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        if (serverId is null)
        {
            return "Unknown (not collected since " + anchorStamp + ")";
        }

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(
                "SELECT collection_time FROM v_collection_log WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 1", connection);
            command.Parameters.AddWithValue(serverId.Value);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime lastCollected && lastCollected >= anchor)
            {
                return "No resolution recorded";
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            DarlingWebFailureLog.Report(logger, "/api/alert-notebook:collector-freshness", 0, ex);
        }

        return "Unknown (not collected since " + anchorStamp + ")";
    }

    /// <summary>One alert-history row plus its dedup-matched incident's facts, in the SAME wire shape
    /// <see cref="DarlingTriageEndpoint"/>'s own row node uses, extended with the incident facts the spec
    /// requires (Incident Since, Involved Objects, Database, Total Occurrences).</summary>
    private static JsonObject AlertRowNode(DarlingAlertReader.AlertHistoryReadRow row, AlertIncident? incident)
    {
        var node = new JsonObject
        {
            ["alert_time"] = row.AlertTime.ToString("o", CultureInfo.InvariantCulture),
            ["server_id"] = row.ServerId,
            ["server_name"] = row.ServerName,
            ["metric_name"] = row.MetricName,
            ["current_value"] = row.CurrentValue,
            ["threshold_value"] = row.ThresholdValue,
            ["detail_text"] = row.DetailText,
        };

        if (incident is not null)
        {
            node["incident_since"] = incident.IncidentStartedUtc?.ToString("o", CultureInfo.InvariantCulture);
            node["involved_objects"] = string.Join(", ", incident.InvolvedObjects);
            node["database"] = incident.Database;
            node["total_occurrences"] = incident.TotalOccurrences;
        }

        return node;
    }

    private static string? Query(HttpContext context, string key)
    {
        var value = context.Request.Query[key].ToString();
        return string.IsNullOrEmpty(value) ? null : value;
    }
}
