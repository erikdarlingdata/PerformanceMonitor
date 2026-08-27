/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for PostgreSQL freeze headroom, paired with the <c>pg_wraparound_stats</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgWraparoundTools
{
    /// <summary>
    /// Severity from the documented escalation ladder rather than from a round number. Each boundary is
    /// a real PostgreSQL behaviour change, which is what makes the label actionable instead of decorative.
    /// </summary>
    internal static string Classify(
        double pctTowardWraparound,
        double pctTowardEmergencyVacuum,
        double pctTowardMultixactWraparound = 0,
        double pctTowardMultixactEmergency = 0)
    {
        /* BOTH counters, graded on the same ladder and the worse label winning. This used to take the XID
           percentage only, so a database at 80% on MultiXacts and 3% on XIDs was labelled "ok" — while the
           tool's own description promises "a server can look fine on transaction IDs and be in trouble on
           MultiXacts". MultiXact exhaustion stops writes exactly as XID exhaustion does, and is burned much
           faster by SELECT FOR UPDATE and foreign-key-heavy workloads, which is precisely the workload that
           gets there first. */
        var xid = Grade(pctTowardWraparound, pctTowardEmergencyVacuum);
        var multi = Grade(pctTowardMultixactWraparound, pctTowardMultixactEmergency);

        /* Rank by how bad the label is, not by which counter it came from. */
        var worst = Rank(multi) > Rank(xid) ? multi : xid;

        /* Name the counter when MultiXacts are the reason, because the remedy differs and the reader would
           otherwise go looking at transaction IDs. */
        return worst != "ok" && Rank(multi) > Rank(xid) ? worst + "_multixact" : worst;
    }

    private static string Grade(double pctTowardWraparound, double pctTowardEmergencyVacuum) =>
        pctTowardWraparound switch
        {
            /* PostgreSQL starts its own "must be vacuumed within N transactions" warnings at
               wrapLimit - 100M for BOTH counters (varsup.c SetTransactionIdLimit, multixact.c
               SetMultiXactIdLimit) = ~95.3% of 2^31 — an earlier draft said 98% ("~40M left"),
               which put this rung ~57M ids AFTER the server began warning. The tool should never
               grade calmer than the engine. */
            >= 95.3 => "critical_wraparound_imminent",
            /* vacuum_failsafe_age (1.6B) — cost limits and index cleanup are abandoned to catch up. */
            >= 74.5 => "critical_failsafe_range",
            >= 50.0 => "warning",
            _ => pctTowardEmergencyVacuum >= 100.0 ? "info_anti_wraparound_vacuum_expected" : "ok",
        };

    private static int Rank(string label) => label switch
    {
        "critical_wraparound_imminent" => 4,
        "critical_failsafe_range" => 3,
        "warning" => 2,
        "info_anti_wraparound_vacuum_expected" => 1,
        _ => 0,
    };

    [McpServerTool(Name = "get_pg_wraparound_risk"), Description("Gets PostgreSQL transaction ID and MultiXact ID freeze headroom per database - how close the server is to a write outage. This is the highest-consequence PostgreSQL signal and has no SQL Server equivalent. PostgreSQL transaction IDs are 32-bit and wrap; if the oldest unfrozen ID gets too old the server stops accepting new write transactions entirely, and no failover helps because every replica shares the condition. Reports two independent counters, because MultiXact IDs exhaust separately and are burned much faster by SELECT FOR UPDATE and foreign-key-heavy workloads - a server can look fine on transaction IDs and be in trouble on MultiXacts. Also reports whether autovacuum is winning: compare the current age against the window peak. Works on any PostgreSQL target, not only Aurora.")]
    public static async Task<string> GetPgWraparoundRisk(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze, used for the peak comparison. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingPgWraparoundReader.GetPgWraparoundAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            if (rows.Count == 0)
            {
                /* This collector has no AppliesTo gate at all, so the capability answer above catches
                   every engine the store can classify (#2532). What is left is a PostgreSQL target that has
                   not collected yet, or a row whose engine_kind is NULL — naming "the server is SQL Server"
                   here would repeat a branch that can no longer reach this line. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_wraparound_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No PostgreSQL freeze-headroom data for this server and window. This collector runs on "
                        + "any PostgreSQL target, so either pg_wraparound_stats has not collected yet, or the "
                        + "store has not recorded this server's engine — and a target it cannot classify may "
                        + "not be a PostgreSQL one at all. Check list_servers.");
            }

            var databases = rows
                .Select(r => new
                {
                    database_name = r.DatabaseName,
                    severity = Classify(
                        r.PctTowardWraparound,
                        r.PctTowardEmergencyVacuum,
                        r.PctTowardMultixactWraparound,
                        r.PctTowardMultixactEmergency),
                    measured_at = r.MeasuredAt,
                    /* Transaction IDs. */
                    frozen_xid_age = r.FrozenXidAge,
                    xids_remaining = r.XidsRemaining,
                    pct_toward_wraparound = Math.Round(r.PctTowardWraparound, 2),
                    pct_toward_emergency_vacuum = Math.Round(r.PctTowardEmergencyVacuum, 1),
                    autovacuum_freeze_max_age = r.AutovacuumFreezeMaxAge,
                    /* MultiXacts - the independently fatal second counter. */
                    min_multixid_age = r.MinMultiXidAge,
                    multixids_remaining = r.MultiXidsRemaining,
                    pct_toward_multixact_wraparound = Math.Round(r.PctTowardMultixactWraparound, 2),
                    pct_toward_multixact_emergency = Math.Round(r.PctTowardMultixactEmergency, 1),
                    /* Is autovacuum winning? A current age below the window peak means freezing has
                       clawed age back at least once — the healthy sawtooth. Equal to the peak means it
                       has only ever climbed within this window, which is the shape that ends badly. */
                    window_peak_frozen_xid_age = r.WindowPeakFrozenXidAge,
                    freezing_is_keeping_up = r.FrozenXidAge < r.WindowPeakFrozenXidAge,
                    allows_connections = r.AllowsConnections,
                })
                .OrderByDescending(d => Math.Max(d.pct_toward_wraparound, d.pct_toward_multixact_wraparound))
                .ToList();

            var worst = databases[0];

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* Fleet-triage summary first: the worst database is the server's real state, since one
                   database hitting the wall stops writes for the whole instance. */
                worst_database = worst.database_name,
                worst_severity = worst.severity,
                worst_pct_toward_wraparound = worst.pct_toward_wraparound,
                thresholds = new
                {
                    anti_wraparound_vacuum_forced_at_pct_of_freeze_max_age = 100,
                    failsafe_engages_around_pct = 74.5,
                    server_warnings_begin_around_pct = 95.3,
                    writes_stop_at_pct = 99.86,
                },
                databases,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL freeze headroom failed: {ex.Message}");
        }
    }
}
