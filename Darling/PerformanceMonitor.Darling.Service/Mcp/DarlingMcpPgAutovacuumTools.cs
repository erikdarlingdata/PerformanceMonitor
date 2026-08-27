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
/// The MCP surface for autovacuum health, paired with the <c>pg_autovacuum_stats</c> collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgAutovacuumTools
{
    /// <summary>
    /// Classifies a table by how far past its OWN threshold it is, not by its dead-tuple count. The same
    /// count is routine on a large table and urgent on a small one, so the ratio is the only comparable
    /// figure — and whether the pile is growing separates autovacuum losing a race from autovacuum not
    /// running at all.
    /// </summary>
    internal static string Classify(bool autovacuumDisabled, double? thresholdRatio, bool deadTuplesGrowing)
    {
        /* A configuration finding, and the one case where the count is beside the point: this table will
           never be vacuumed by autovacuum no matter how bad it gets, and it holds back the whole
           database's freeze horizon while it sits there. */
        if (autovacuumDisabled)
        {
            return "critical_autovacuum_disabled_on_table";
        }

        if (thresholdRatio is null)
        {
            return "unknown_no_threshold";
        }

        return thresholdRatio switch
        {
            /* Ten times past the line is not a busy table, it is a stuck one — most often autovacuum
               being cancelled repeatedly by conflicting locks, or starved of workers. */
            >= 10 => "critical_far_past_threshold",
            >= 2 when deadTuplesGrowing => "warning_past_threshold_and_growing",
            >= 2 => "warning_past_threshold",
            >= 1 => "info_at_threshold",
            _ => "ok",
        };
    }

    [McpServerTool(Name = "get_pg_autovacuum_health"), Description("Gets PostgreSQL per-table autovacuum health: which tables are behind on vacuum or analyze, ranked by how far past each table's OWN trigger threshold it is. This ratio is the point of the tool - a dead-tuple count alone is not actionable, because autovacuum fires at autovacuum_vacuum_threshold + scale_factor * reltuples, so 500,000 dead tuples is routine on a 50-million-row table and urgent on a 10,000-row one. Thresholds honour per-table reloptions overrides, not just the server settings, since ALTER TABLE SET (autovacuum_*) is common on exactly the big hot tables where the global default is wrong. Also reports tables with autovacuum switched off entirely, whether dead tuples are still growing (autovacuum losing a race) or flat (autovacuum blocked or not running), and the analyze backlog that drives bad row estimates. Works on any PostgreSQL target; collected per database.")]
    public static async Task<string> GetPgAutovacuumHealth(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze, used for the dead-tuple growth comparison. Default 24.")] int hours_back = 24,
        [Description("Maximum tables to return, worst first. Default 20.")] int limit = 20,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingPgAutovacuumReader.GetPgAutovacuumAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit);

            if (rows.Count == 0)
            {
                /* "No table on this server has dead tuples" is the healthy answer for a PostgreSQL target
                   and a fabricated one for a SQL Server target — the collector has never run there. Ask the
                   engine before making the claim (#2532). */
                var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_autovacuum_stats");
                if (gated != null)
                {
                    return gated;
                }

                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    status = "no_pending_maintenance",
                    finding = "No table on this server has dead tuples, pending analyze work, inserts since "
                            + "its last vacuum, or autovacuum disabled. The collector records only tables with "
                            + "pending work, so an empty result is the healthy case rather than missing data.",
                }, McpHelpers.JsonOptions);
            }

            var tables = rows.Select(r =>
            {
                /* -1 is the collector's not-applicable sentinel, and a 0 threshold happens on a table that
                   has never been analyzed. Neither can produce a ratio, and inventing one would rank a
                   table we know nothing about above tables we have measured. */
                double? ratio = r.VacuumThreshold > 0
                    ? Math.Round((double)r.DeadTuples / r.VacuumThreshold, 2)
                    : null;
                double? analyzeRatio = r.AnalyzeThreshold > 0
                    ? Math.Round((double)r.ModsSinceAnalyze / r.AnalyzeThreshold, 2)
                    : null;
                var growing = r.DeadTuples > r.FirstDeadTuples;

                return new
                {
                    database_name = r.DatabaseName,
                    table_name = $"{r.SchemaName}.{r.TableName}",
                    severity = Classify(r.AutovacuumDisabled, ratio, growing),
                    dead_tuples = r.DeadTuples,
                    vacuum_threshold = r.VacuumThreshold >= 0 ? r.VacuumThreshold : (long?)null,
                    /* The headline number: 1.0 means autovacuum should be triggering right now. */
                    threshold_ratio = ratio,
                    dead_tuples_growing = growing,
                    dead_tuple_change = r.DeadTuples - r.FirstDeadTuples,
                    live_tuples = r.LiveTuples,
                    /* The analyze half. Stale statistics produce bad row estimates and bad plans, which is
                       a different symptom from bloat and gets missed because both come from one process. */
                    mods_since_analyze = r.ModsSinceAnalyze,
                    analyze_threshold = r.AnalyzeThreshold >= 0 ? r.AnalyzeThreshold : (long?)null,
                    analyze_threshold_ratio = analyzeRatio,
                    /* The append-only path, PG13+: a table with no dead tuples is invisible to the vacuum
                       rule, and a table never vacuumed is never frozen either. */
                    inserts_since_vacuum = r.InsertsSinceVacuum >= 0 ? r.InsertsSinceVacuum : (long?)null,
                    insert_vacuum_threshold = r.InsertVacuumThreshold >= 0 ? r.InsertVacuumThreshold : (long?)null,
                    autovacuum_disabled = r.AutovacuumDisabled,
                    total_bytes = r.TotalBytes >= 0 ? r.TotalBytes : (long?)null,
                    total_gb = r.TotalBytes >= 0 ? Math.Round(r.TotalBytes / 1024.0 / 1024.0 / 1024.0, 2) : (double?)null,
                    last_autovacuum = r.LastAutovacuum,
                    last_vacuum = r.LastVacuum,
                    last_autoanalyze = r.LastAutoanalyze,
                    last_analyze = r.LastAnalyze,
                    autovacuum_count = r.AutovacuumCount,
                    /* Never autovacuumed AND past threshold is the strongest single signal that something
                       is preventing it, rather than that it has not got round to this table yet. */
                    never_autovacuumed = r.LastAutovacuum is null && r.AutovacuumCount == 0,
                    measured_at = r.MeasuredAt,
                };
            })
            .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "tables_with_pending_maintenance",
                table_count = tables.Count,
                autovacuum_disabled_count = tables.Count(t => t.autovacuum_disabled),
                past_threshold_count = tables.Count(t => t.threshold_ratio >= 1),
                growing_count = tables.Count(t => t.dead_tuples_growing),
                worst_table = tables[0].table_name,
                worst_severity = tables[0].severity,
                tables,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL autovacuum health failed: {ex.Message}");
        }
    }
}
