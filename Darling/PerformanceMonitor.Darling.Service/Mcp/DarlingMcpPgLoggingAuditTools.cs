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
/// <c>get_pg_logging_audit</c> (#3607): whether a PostgreSQL target's logging settings are producing the lines
/// they could, setting by setting, with the remedy for each in the hosting flavour's own syntax.
///
/// <para>The judgment is <see cref="DarlingPgLoggingAudit"/>'s and the rows are
/// <see cref="DarlingPgLoggingAuditReader"/>'s; this class is the wire. It sits beside
/// <c>get_pg_plan_capture_readiness</c> deliberately: that read judges plan capture's own preconditions
/// and this one judges the rest of the logging surface, and between them target onboarding has one "is this
/// target telling us everything it could" answer, which is the issue's ask. The two do not overlap —
/// the plan-capture settings appear here as observed values with a pointer, never as a second verdict.</para>
///
/// <para><b>No <c>hours_back</c> and no <c>as_of</c> PARAMETER, and that is the <c>get_pg_server_config</c>
/// convention rather than an omission.</b> Configuration is a state, not a window: the audit is of the
/// NEWEST snapshot, and the response carries that snapshot's collection time as <c>as_of</c> so the reader
/// knows how old the state is. A windowed form would answer "no logging configuration" about a server
/// whose hourly collector last ran just outside the window.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgLoggingAuditTools
{
    [McpServerTool(Name = "get_pg_logging_audit"), Description("Audits a PostgreSQL target's LOGGING settings - log_min_duration_statement, log_lock_waits, log_temp_files, log_autovacuum_min_duration, log_checkpoints, log_connections, log_disconnections - and says, per setting, whether it is producing the lines it could, what telemetry those lines unlock, the recommended value WITH its cost, and the remedy in the syntax this server's hosting needs (ALTER SYSTEM plus a reload where the server is yours to administer; a parameter group on RDS/Aurora, decided from rds.* parameters in the stored snapshot rather than guessed). It reads the STORED configuration snapshot pg_server_config already collects hourly, never the live server, and reports that snapshot's time as as_of. Read it at onboarding and whenever a target-side log read comes back empty: a target with every one of these off looks identical to a fully instrumented one from every counter-based read, and the difference shows up at incident time when the log somebody reaches for holds nothing. Verdicts are instrumented, partial, off or unknown - partial means a THRESHOLD is filtering (statements faster than N ms, temp files under N kB) and the row says what falls below it; for log_min_duration_statement the threshold IS the recommended posture, and its cost_note says so, because 0 logs every statement the server runs. unknown means the setting is not in the snapshot and nothing is inferred. Every facet names the Darling family that would consume its lines and says PLANNED where that consumer does not ship yet (#3601/#3602/#3603) - the counter reads that exist today are named beside it with what they cannot see. Plan capture's own settings (auto_explain, log_line_prefix %Q, lc_messages) are LISTED as observed for completeness but judged by get_pg_plan_capture_readiness, which owns their traps; lc_messages decides whether any of these lines are written in the English the parsers match. PostgreSQL-only.")]
    public static async Task<string> GetPgLoggingAudit(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var snapshot = await DarlingPgLoggingAuditReader.GetNewestSnapshotAsync(postgres, resolved.ServerId);

            if (snapshot.Count == 0)
            {
                /* The same miss vocabulary as get_pg_server_config, because it is the same absence: the
                   collector this reads never ran here (engine gate), or has not run YET. Neither is an audit
                   result, and an audit of an empty snapshot would say 'unknown' seven times and look like one. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_server_config")
                    ?? McpHelpers.Status(
                        "empty",
                        $"No configuration snapshot has been collected for {resolved.ServerName} yet, so there "
                        + "is nothing to audit. pg_server_config runs hourly; a server registered in the last "
                        + "hour has not reached its first collection. This is not a verdict about the "
                        + "server's logging - it is the absence of the evidence.");
            }

            return BuildAuditJson(resolved.ServerName, DarlingPgLoggingAudit.Audit(snapshot));
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading the PostgreSQL logging audit failed: {ex.Message}");
        }
    }

    /// <summary>
    /// The response body, split out so the WIRE SHAPE can be asserted without a live store — the reason
    /// <c>BuildReadinessJson</c> is separate on the plan tools.
    /// </summary>
    internal static string BuildAuditJson(string serverName, DarlingPgLoggingAudit.Result audit)
    {
        var facets = audit.Facets;

        /* The counts are over ALL facets, never a page - there is no limit on this read, the facet list is
           fixed and small, so the #2629 cap-versus-window trap does not arise and the summary is a fact about
           the server. off and unknown are NAMED because they are the actionable ones; partial is not
           listed as a to-do because for two settings it is the recommendation. */
        var off = facets.Where(f => f.Verdict == DarlingPgLoggingAudit.Off).Select(f => f.Setting).ToArray();
        var unknown = facets.Where(f => f.Verdict == DarlingPgLoggingAudit.Unknown).Select(f => f.Setting).ToArray();

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            status = "logging_audit",
            /* The snapshot's collection time, on the wire under the name the other latest-snapshot reads
               use (get_pvs_stats). It is how old this state is, not a window. */
            as_of = audit.AsOf.ToString("o"),
            source = "pg_server_config, newest snapshot - stored configuration, not the live server",
            hosting = audit.Managed ? "managed (RDS/Aurora)" : "self-hosted",
            hosting_evidence = audit.HostingEvidence,
            total = facets.Count,
            instrumented_count = facets.Count(f => f.Verdict == DarlingPgLoggingAudit.Instrumented),
            partial_count = facets.Count(f => f.Verdict == DarlingPgLoggingAudit.Partial),
            off_count = off.Length,
            unknown_count = unknown.Length,
            off_settings = off,
            unknown_settings = unknown,
            note = "One facet per logging setting, in the order an operator reaches for them - statements, "
                 + "locks, spills, maintenance, checkpoints, connections - not a causal order; nothing here "
                 + "gates anything else. verdict describes the LINES: instrumented writes every line the "
                 + "setting can, partial has a threshold filtering and the row says what falls below it, off "
                 + "writes nothing, unknown is not in the snapshot and nothing is inferred. partial is the "
                 + "recommended posture for log_min_duration_statement and can be for log_temp_files - read "
                 + "cost_note before changing a partial row. consumer names the Darling family that reads the "
                 + "lines and says PLANNED where it does not ship yet; the setting is still worth turning on "
                 + "first, because the log it fills is the one somebody opens at incident time. remedy is "
                 + "worded for this server's hosting (see hosting_evidence). judged_by_readiness lists plan "
                 + "capture's own settings as observed in the same snapshot; " + DarlingPgLoggingAudit.ReadinessTool
                 + " judges them, and its message_locale facet decides whether ANY of these lines are written "
                 + "in the English the parsers match.",
            facets = facets.Select(f => new
            {
                setting = f.Setting,
                verdict = f.Verdict,
                /* Verbatim, so a reader sees what the server said rather than this tool's reading of it. */
                value = f.Value,
                unit = f.Unit,
                default_value = f.DefaultValue,
                source = f.Source,
                change_needs = f.ChangeNeeds,
                unlocks = f.Unlocks,
                consumer = f.Consumer,
                recommended = f.Recommended,
                cost_note = f.CostNote,
                remedy = f.Remedy,
                scope_note = f.ScopeNote,
            }),
            judged_by_readiness = new
            {
                tool = DarlingPgLoggingAudit.ReadinessTool,
                note = "Shown as observed in this snapshot for completeness and NOT judged here: "
                     + DarlingPgLoggingAudit.ReadinessTool + " owns each of these with its own trap - a loaded "
                     + "auto_explain at -1 captures nothing, an auto_explain.* value on a server that never "
                     + "loaded the module is a placeholder, a log_line_prefix without %Q orphans every plan, "
                     + "and a translated lc_messages blinds every log read here. A null value means the "
                     + "setting is not in the snapshot, which for auto_explain.log_min_duration usually means "
                     + "the library is not loaded.",
                settings = audit.JudgedByReadiness.Select(s => new
                {
                    setting = s.Setting,
                    value = s.Value,
                    source = s.Source,
                    readiness_facet = s.ReadinessFacet,
                }),
            },
        }, McpHelpers.JsonOptions);
    }
}
