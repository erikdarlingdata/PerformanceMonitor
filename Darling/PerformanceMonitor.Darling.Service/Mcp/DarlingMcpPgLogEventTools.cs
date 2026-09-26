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
using System.Threading;
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
    [McpServerTool(Name = "get_pg_log_events"), Description("Gets PostgreSQL server-log events in the window ending at as_of, newest first, by family (error, connection, lock_wait, temp_file, autovacuum, checkpoint). Each family is gated by its own logging setting; an off family is EMPTY here, unaudited. Also needs log_timezone = UTC (else refused whole) and English lc_messages (else empty) - an all-family empty can mean either, not 'no events'. min_severity ranks by SERIOUSNESS, not log_min_messages' order. total_events is the window's count; events_returned is the limit-bounded page. times_seen is a sighting count, not an occurrence count. <<GUIDE>> Gets the events PostgreSQL wrote to its server log in the window, newest first, classified by FAMILY: error (anything at WARNING or worse - a cancelled statement, a FATAL connection refusal, a constraint violation, a deadlock's ERROR line), connection (log_connections / log_disconnections: received, authorized with user, database and application_name, disconnection with session time), lock_wait (log_lock_waits: 'process N still waiting for ShareLock on transaction M after 1000 ms', and the 'acquired' line that ends the same wait - the engine's own blocked-process report, written rather than sampled), temp_file (log_temp_files: one event per spilled file with its exact bytes and when it spilled - THIS execution at 03:07 wrote 4.2 GB, which pg_stat_database's temp_bytes delta and pg_stat_statements' temp_blks_written can only aggregate; a spill-storm alert - N events or X bytes in a window - is a custom-alert rule to write over this family, not something this tool judges), autovacuum (log_autovacuum_min_duration: one event per completed automatic vacuum or analyze with relation_name as schema.table, is_analyze, duration_ms, pages_removed / pages_remaining, tuples_removed / tuples_remaining, buffer_hits / buffer_misses / buffer_dirtied and wal_records / wal_bytes lifted from the report - what a run COST, where get_pg_autovacuum_health's catalog sample says only whether it ran; that tool carries the same runs per table as recent_runs), and checkpoint, which is recognised and stored with its message and nothing lifted. The lifted fields appear on an event only when the line carried them: an autoanalyze has no pages or tuples clause, a PostgreSQL 16/17 autoanalyze no WAL clause, and a row stored before the parsers landed has none. This is the SQL Server DBA's 'check the error log': errors, spills, autovacuum runs and connection churn land in this log and nowhere else, and between two counter samples they were invisible. Filter with family and min_severity; min_severity ranks by SERIOUSNESS - LOG/INFO < NOTICE < WARNING < ERROR < FATAL < PANIC - which is NOT log_min_messages' order (there LOG sits above ERROR, because that setting ranks by how hard a message is to suppress). WARNING is the 'errors only' setting; connection and lock_wait lines are LOG and are excluded by anything above it. Each row is one DISTINCT log entry; times_seen counts how often the collector saw the SAME entry, which on a target whose log is read by pg_read_file climbs while the entry stays in the re-read window and on RDS/Aurora is normally 1 - never a count of occurrences. Messages are shown as PostgreSQL wrote them; SQL text is normalized with literals replaced by ? (a deadlock's queries in detail, a function's statement in context). The statement text is never stored, and neither is any value this tool returns derived from it; get_pg_top_queries is where a statement shape's normalised text lives. sqlstate is null unless the target's log_line_prefix carries %e; database_name and user_name are null unless the prefix carries %u@%d or the message names them. WHAT THE TARGET HAS TO HAVE ON for each family to carry anything: error needs only log_min_messages at WARNING or lower (the default); connection needs log_connections / log_disconnections; lock_wait needs log_lock_waits; temp_file needs log_temp_files; autovacuum needs log_autovacuum_min_duration; checkpoint needs log_checkpoints. A family that is off on the target is EMPTY here and this tool does not audit the settings - get_pg_server_config carries them. Three preconditions are the same ones every target-side log read has: the log must be readable (pg_read_server_files plus GRANT EXECUTE ON pg_read_file self-hosted; the RDS log API on Aurora/RDS), stamped in a zero-offset zone (log_timezone = UTC - a non-UTC log is REFUSED whole rather than shifted), and written in English (lc_messages - a translated severity label matches nothing). The page is bounded by limit and says so: events_returned is the page, truncated is observed (one more row was fetched than shown), oldest_returned_at / newest_returned_at bound the page, and total_events is the WINDOW's distinct-event count under the same family and severity filters on the same statement - so a 25-row page of a 4,000-event hour reads as 25 of 4,000, not as the hour.")]
    public static async Task<string> GetPgLogEvents(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("One family to return: error, connection, lock_wait, temp_file, autovacuum, checkpoint. Omit for every family.")] string? family = null,
        [Description("Lowest severity to return, by seriousness: LOG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC. Default LOG (everything). See the tool's reading guide.")] string? min_severity = null,
        [Description("Maximum events to return. Default 50. The page is truncated when the window holds more; total_events is the window's count.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        /* The family filter is a closed vocabulary and a typo in it must not read as "no events of that
           kind" — an unknown family is the caller's mistake, said as one: the `invalid` envelope (#3739), not the
           `error` word this and the min_severity refusal below wore until then, which the web surface read as
           a server fault and answered 500 for a typo. */
        var familyFilter = string.IsNullOrWhiteSpace(family) ? null : family.Trim().ToLowerInvariant();
        if (familyFilter is not null && !PgLogFamilies.IsKnown(familyFilter))
        {
            return McpHelpers.Refusal("family",
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
            return McpHelpers.Refusal("min_severity",
                $"min_severity '{min_severity}' is not a PostgreSQL severity label. Use one of LOG, INFO, "
                + "NOTICE, WARNING, ERROR, FATAL, PANIC (ranked by seriousness in that order).");
        }

        try
        {
            /* limit + 1 so truncation is OBSERVED from the extra row rather than inferred from the count reaching
               the cap (#3594). */
            var page = await DarlingPgLogEventReader.GetEventsAsync(
                postgres, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd,
                familyFilter, minRank, limit + 1, cancellationToken);

            if (page.Rows.Count == 0)
            {
                /* Capability first, then what the collector's own last run recorded (#3410): a denied
                   pg_read_file grant, a missing file, a logging_collector that is off, a non-UTC
                   log_timezone — each lands in collection_log as a named skip, and quoting it here is what
                   turns "no events" into not-collected WITH THE REASON. Then the honest quiet answer, which
                   names the settings a family depends on, because an all-off target is the likeliest
                   reason a log has nothing for a family and nothing else on this surface says so. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_log_events", cancellationToken)
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_log_events", cancellationToken)
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
                     + "is normally 1. message, detail and context are as PostgreSQL wrote them, with the SQL "
                     + "in them normalized; the statement is never stored, only fingerprinted. min_severity "
                     + "ranks by seriousness (LOG/INFO < NOTICE < WARNING < ERROR "
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
                    context = r.Context,
                    /* Neither stored identity is returned (#3996's review, #4004): raw_line_hash and statement_fingerprint
                       hash text a reader can mostly rebuild, so a surface that returned them would hand out the thing a
                       guess is tested against. Both are keyed with the store's own secret now, and they still stay
                       inside the store: the reads dedupe on raw_line_hash, and times_seen is what that dedupe says. */
                    times_seen = r.TimesSeen,
                    /* V130 (#3602, #3603): the family's numbers, PRESENT only where the line carried them —
                       a nested object rather than thirteen nullable members, because a connection event
                       carrying thirteen nulls would be noise on every row of the volume family, and a null
                       here already has three meanings (not this family; this version did not print the
                       clause; stored before the parser landed) that the description spells out once. */
                    temp_file = r.Family == PgLogFamilies.TempFile && r.Metrics.Bytes is not null
                        ? new { bytes = r.Metrics.Bytes, mb = Math.Round(r.Metrics.Bytes.Value / 1024.0 / 1024.0, 2) }
                        : null,
                    autovacuum = r.Family == PgLogFamilies.Autovacuum && r.Metrics.RelationName is not null
                        ? AutovacuumRunShape(r.Metrics)
                        : null,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_log_events", ex);
        }
    }

    /// <summary>
    /// One autovacuum run's lifted figures as the JSON shape both this tool and
    /// <c>get_pg_autovacuum_health</c>'s <c>recent_runs</c> publish (#3603) — one spelling, so a reader
    /// moving between the two surfaces meets the same keys. Nulls are kept INSIDE this object: here the
    /// family is known to be autovacuum, so a null means "this line did not carry the clause" (an
    /// autoanalyze has no pages or tuples; 16/17 print no WAL clause on an analyze) and saying so beats
    /// omitting the key and letting a reader wonder whether it exists. <c>kind</c> is the word a DBA uses;
    /// <c>is_analyze</c> is the column.
    /// </summary>
    internal static object AutovacuumRunShape(DarlingPgLogEventReader.PgLogEventMetricsRow m) => new
    {
        relation_name = m.RelationName,
        kind = m.IsAnalyze == true ? "analyze" : "vacuum",
        duration_ms = m.DurationMs,
        pages_removed = m.PagesRemoved,
        pages_remaining = m.PagesRemaining,
        tuples_removed = m.TuplesRemoved,
        tuples_remaining = m.TuplesRemaining,
        buffer_hits = m.BufferHits,
        buffer_misses = m.BufferMisses,
        buffer_dirtied = m.BufferDirtied,
        wal_records = m.WalRecords,
        wal_bytes = m.WalBytes,
        /* Blocks read and dirtied as megabytes at the DEFAULT 8 kB block_size — the unit disks are quoted
           in, and the same constant PostgreSQL's own `avg read rate` line uses. A target built with another
           BLCKSZ (rare; pg_server_config carries block_size) is off by that ratio here and exact in the
           block counts beside it. Null where the clause was absent, never a fabricated zero. */
        read_mb = m.BufferMisses is null ? null : (double?)Math.Round(m.BufferMisses.Value * 8192.0 / 1024.0 / 1024.0, 2),
        written_mb = m.BufferDirtied is null ? null : (double?)Math.Round(m.BufferDirtied.Value * 8192.0 / 1024.0 / 1024.0, 2),
    };
}
