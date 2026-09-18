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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// PostgreSQL server-log events (#3601) — the classified pipeline's read surface: the SQL Server DBA's
/// "check the error log", answered from what <c>pg_log_events</c> stored off the target's log.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgLogEventTools
{
    [McpServerTool(Name = "get_pg_log_events"), Description("Gets the events PostgreSQL wrote to its server log in the window, newest first, classified by FAMILY: error (anything at WARNING or worse - a cancelled statement, a FATAL connection refusal, a constraint violation, a deadlock's ERROR line), connection (log_connections / log_disconnections: received, authorized with user, database and application_name, disconnection with session time), lock_wait (log_lock_waits: 'process N still waiting for ShareLock on transaction M after 1000 ms', and the 'acquired' line that ends the same wait - the engine's own blocked-process report, written rather than sampled), plus temp_file, autovacuum and checkpoint, which are recognised and stored with their message today and gain structured tables in later work. This is the SQL Server DBA's 'check the error log': errors, spills, autovacuum runs and connection churn land in this log and nowhere else, and between two counter samples they were invisible. Filter with family and min_severity; min_severity ranks by SERIOUSNESS - LOG/INFO < NOTICE < WARNING < ERROR < FATAL < PANIC - which is NOT log_min_messages' order (there LOG sits above ERROR, because that setting ranks by how hard a message is to suppress). Each row is one DISTINCT log entry; times_seen counts how often the collector saw the SAME entry, which on a target whose log is read by pg_read_file climbs while the entry stays in the re-read window and on RDS/Aurora is normally 1 - never a count of occurrences. message and detail are REDACTED (quoted literals and unique-violation key values stripped) and the statement text is never stored: statement_fingerprint is a hash of the redacted statement, so the same statement shape recurs to one fingerprint and get_pg_top_queries is where the shape's normalised text lives. sqlstate is null unless the target's log_line_prefix carries %e; database_name and user_name are null unless the prefix carries %u@%d or the message names them. WHAT THE TARGET HAS TO HAVE ON for each family to carry anything: error needs only log_min_messages at WARNING or lower (the default); connection needs log_connections / log_disconnections; lock_wait needs log_lock_waits; temp_file needs log_temp_files; autovacuum needs log_autovacuum_min_duration; checkpoint needs log_checkpoints. A family that is off on the target is EMPTY here and this tool does not audit the settings - get_pg_server_config carries them. Three preconditions are the same ones every target-side log read has: the log must be readable (pg_read_server_files plus GRANT EXECUTE ON pg_read_file self-hosted; the RDS log API on Aurora/RDS), stamped in a zero-offset zone (log_timezone = UTC - a non-UTC log is REFUSED whole rather than shifted), and written in English (lc_messages - a translated severity label matches nothing). The page is bounded by limit and says so: events_returned is the page, truncated is observed (one more row was fetched than shown), oldest_returned_at / newest_returned_at bound the page, and total_events is the WINDOW's distinct-event count under the same family and severity filters on the same statement - so a 25-row page of a 4,000-event hour reads as 25 of 4,000, not as the hour.")]
    public static async Task<string> GetPgLogEvents(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("One family to return: error, connection, lock_wait, temp_file, autovacuum, checkpoint. Omit for every family.")] string? family = null,
        [Description("Lowest severity to return, by seriousness: LOG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC. Default LOG (everything). WARNING is the 'errors only' setting; connection and lock_wait lines are LOG and are excluded by anything above it.")] string? min_severity = null,
        [Description("Maximum events to return. Default 50. The page is truncated when the window holds more; total_events is the window's count.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return McpHelpers.Status("error", limitError);

        /* The family filter is a closed vocabulary and a typo in it must not read as "no events of that
           kind" — an unknown family is the caller's mistake, said as one. */
        var familyFilter = string.IsNullOrWhiteSpace(family) ? null : family.Trim().ToLowerInvariant();
        if (familyFilter is not null && !PgLogFamilies.IsKnown(familyFilter))
        {
            return McpHelpers.Status("error",
                $"family '{family}' is not one this pipeline classifies. Known families: "
                + string.Join(", ", PgLogFamilies.All.Where(f => f != PgLogFamilies.Other)) + ".");
        }

        /* The severity floor is a label, ranked by PgLogEntry.RankOf — the same ranking the reader's SQL
           CASE spells, pinned to agree. An unknown label is refused for the family filter's reason. */
        var minRank = 0;
        var severityLabel = string.IsNullOrWhiteSpace(min_severity) ? "LOG" : min_severity.Trim().ToUpperInvariant();
        minRank = PgLogEntry.RankOf(severityLabel);
        if (minRank == 0)
        {
            return McpHelpers.Status("error",
                $"min_severity '{min_severity}' is not a PostgreSQL severity label. Use one of LOG, INFO, "
                + "NOTICE, WARNING, ERROR, FATAL, PANIC (ranked by seriousness in that order).");
        }

        try
        {
            /* limit + 1 so truncation is OBSERVED from the extra row rather than inferred from the count reaching
               the cap (#3594). */
            var page = await DarlingPgLogEventReader.GetEventsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd,
                familyFilter, minRank, limit + 1);

            if (page.Rows.Count == 0)
            {
                /* Capability first, then what the collector's own last run recorded (#3410): a denied
                   pg_read_file grant, a missing file, a logging_collector that is off, a non-UTC
                   log_timezone — each lands in collection_log as a named skip, and quoting it here is what
                   turns "no events" into not-collected WITH THE REASON. Then the honest quiet answer, which
                   names the settings a family depends on, because an all-off target is the likeliest
                   reason a log has nothing for a family and nothing else on this surface says so. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_log_events")
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_log_events")
                    ?? McpHelpers.Status(
                        "no_events",
                        $"No log event{(familyFilter is null ? string.Empty : $" in family '{familyFilter}'")}"
                        + $"{(minRank > 1 ? $" at {severityLabel} or worse" : string.Empty)} was stored for "
                        + $"{resolved.ServerName} in the last {hours_back} hour(s). Four things produce that: "
                        + "the server wrote none, which is the healthy answer for the error and lock_wait "
                        + "families; the setting that family depends on is off on the target "
                        + "(log_connections / log_disconnections for connection, log_lock_waits for "
                        + "lock_wait, log_temp_files for temp_file, log_autovacuum_min_duration for "
                        + "autovacuum, log_checkpoints for checkpoint - get_pg_server_config carries them, "
                        + "and a family whose setting is off is empty here by construction); the log could "
                        + "not be read (get_pg_plan_capture_readiness judges readability, because plan "
                        + "capture reads the same file the same way); or the log was read and PostgreSQL "
                        + "did not write it in English (lc_messages translates the severity label, so a "
                        + "FEHLER: line matches nothing - the readiness read's message_locale facet). "
                        + "pg_stat_database's counters in get_pg_database_stats are the independent check "
                        + "for the error and temp_file families: if they moved and nothing is here, the log "
                        + "is the problem rather than the server.");
            }

            var truncated = page.Rows.Count > limit;
            var rows = truncated ? page.Rows.Take(limit).ToList() : page.Rows.ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                family = familyFilter,
                min_severity = severityLabel,
                status = "events",
                events_returned = rows.Count,
                /* The WINDOW's distinct-event count under the same filters, off the same statement (#3613) —
                   never the page's count under a total's name (#3594). */
                total_events = page.WindowTotal,
                truncated,
                order = "newest first",
                /* Under newest-first the oldest stamp IS the reach of the read: events before it were not
                   shown, and total_events says how many the window held. */
                oldest_returned_at = rows[^1].OccurredAtUtc,
                newest_returned_at = rows[0].OccurredAtUtc,
                note = "occurred_at is when PostgreSQL wrote the line, not when the collector found it. "
                     + "times_seen counts sightings of the SAME entry, never occurrences: a target read with "
                     + "pg_read_file re-reads an overlapping log tail every cycle, so the count climbs while "
                     + "the entry stays in the window; on RDS and Aurora the log API is consume-once and it "
                     + "is normally 1. message and detail are redacted; the statement is never stored, only "
                     + "fingerprinted. min_severity ranks by seriousness (LOG/INFO < NOTICE < WARNING < ERROR "
                     + "< FATAL < PANIC), not by log_min_messages' suppression order. total_events is the "
                     + "window's distinct-event count under these filters; events_returned is this page.",
                events = rows.Select(r => new
                {
                    occurred_at = r.OccurredAtUtc,
                    family = r.Family,
                    severity = r.Severity,
                    sqlstate = r.SqlState,
                    database_name = r.DatabaseName,
                    user_name = r.UserName,
                    application_name = r.ApplicationName,
                    pid = r.Pid,
                    message = r.Message,
                    detail = r.Detail,
                    statement_fingerprint = r.StatementFingerprint,
                    raw_line_hash = r.RawLineHash,
                    times_seen = r.TimesSeen,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.Status("error", $"Reading PostgreSQL log events failed: {ex.Message}");
        }
    }
}
