/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

internal static partial class AlertNotebookEndpoint
{
    /// <summary>PostgreSQL Wraparound Risk template version (#4223). Bumped only if this template's SHAPE
    /// changes.</summary>
    internal const int PgWraparoundTemplateVersion = 1;

    /// <summary>PostgreSQL Vacuum Horizon Blocked template version (#4223).</summary>
    internal const int PgXminHorizonTemplateVersion = 1;

    /// <summary>PostgreSQL Replication Slot Retention template version (#4223).</summary>
    internal const int PgReplicationSlotTemplateVersion = 1;

    /// <summary>PostgreSQL Wraparound Risk: header, status, <c>get_pg_wraparound_risk</c>, <c>get_pg_autovacuum_health</c>
    /// (the mechanical map's second read for this family), then a 24h trend of the LEAST XID headroom remaining
    /// (<c>pg_xids_remaining</c>, aggregated Min — the least runway to a write outage is the signal, same
    /// as the measure's own default).</summary>
    private static JsonArray BuildPgWraparoundCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        return new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            AuthoredReadCell("get_pg_wraparound_risk", "Wraparound headroom", serverName, asOf,
                ("hours", "24")),
            AuthoredReadCell("get_pg_autovacuum_health", "Autovacuum health", serverName, asOf,
                ("hours", "24"), ("limit", "20")),
            PgGaugeTimelinePanel(
                "XIDs remaining to write outage", "pg_wraparound_stats", "pg_xids_remaining", "min",
                windowStart, windowEnd, databaseFilter: incident?.Database),
        };
    }

    /// <summary>PostgreSQL Vacuum Horizon Blocked: header, status, <c>get_pg_xmin_horizon</c> (what is
    /// holding the horizon back, by cause), <c>get_pg_session_states</c> (whether a held-open session
    /// actually pins the horizon), then a 24h trend of the horizon's own age (<c>pg_xmin_age</c>).</summary>
    private static JsonArray BuildPgXminHorizonCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        return new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            AuthoredReadCell("get_pg_xmin_horizon", "xmin horizon holders", serverName, asOf,
                ("hours", "24")),
            AuthoredReadCell("get_pg_session_states", "Session states", serverName, asOf,
                ("hours", "24"), ("limit", "25")),
            PgGaugeTimelinePanel(
                "Xmin horizon age", "pg_xmin_horizon", "pg_xmin_age", "max",
                windowStart, windowEnd, databaseFilter: null),
        };
    }

    /// <summary>PostgreSQL Replication Slot Retention: header, status, <c>get_pg_replication_slots</c>,
    /// <c>get_pg_replication_stats</c> (the mechanical map's second read — the connected-replica counterpart
    /// of the slot read), then a 24h trend of retained WAL per slot (<c>pg_slot_retained_wal_bytes</c>).</summary>
    private static JsonArray BuildPgReplicationSlotCells(
        string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
        AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status)
    {
        return new JsonArray
        {
            HeaderCell(metric, serverName, incident, row),
            StatusCell(status),
            AuthoredReadCell("get_pg_replication_slots", "Replication slots", serverName, asOf,
                ("hours", "24")),
            AuthoredReadCell("get_pg_replication_stats", "Replication stats", serverName, asOf,
                ("hours", "24"), ("limit", "25")),
            PgGaugeTimelinePanel(
                "Slot retained WAL", "pg_replication_slot_stats", "pg_slot_retained_wal_bytes", "max",
                windowStart, windowEnd, databaseFilter: incident?.Database),
        };
    }

    /// <summary>A 24h trend panel for a GAUGE measure (spec §1 binding: absolute <c>range</c> equal to the
    /// computed window, and — where the incident carries a database and the source has that dimension — a
    /// bound <c>database_name</c> filter). Distinct from <see cref="TimelinePanel"/>, which hard-codes
    /// <c>aggregate: "count"</c> for a per-event measure and always carries an annotation overlay; none of
    /// the three PostgreSQL gauge sources here have a per-event count or a catalog annotation source to
    /// overlay, so this carries the caller's own aggregate and no <c>annotations</c> key.</summary>
    private static JsonObject PgGaugeTimelinePanel(
        string title, string source, string measure, string aggregate, DateTime windowStart, DateTime windowEnd,
        string? databaseFilter)
    {
        var cell = new JsonObject
        {
            ["type"] = "panel",
            ["title"] = title,
            ["source"] = source,
            ["measure"] = measure,
            ["aggregate"] = aggregate,
            ["viz"] = "line",
            ["timeBucket"] = "hour",
            ["range"] = new JsonObject
            {
                ["windowStart"] = windowStart.ToString("o", CultureInfo.InvariantCulture),
                ["windowEnd"] = windowEnd.ToString("o", CultureInfo.InvariantCulture),
            },
        };

        if (!string.IsNullOrWhiteSpace(databaseFilter))
        {
            cell["filters"] = new JsonArray
            {
                new JsonObject
                {
                    ["dimension"] = "database_name",
                    ["op"] = "eq",
                    ["value"] = databaseFilter,
                },
            };
        }

        return cell;
    }
}
