// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// PostgreSQL deadlocks (#2661) — the reports themselves, read out of the server log, rather than the count
/// <c>pg_stat_database</c> keeps.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgDeadlockTools
{
    /* #4735: the three texts an MCP caller reads in place of the tool guide are members of their own, so a test can
       read what the RESPONSE says rather than only what the guide says. The guide is served on tools/list and the
       response is what a caller that never opened the guide is left with; the two had drifted apart. */

    /// <summary>The <c>note</c> the "deadlocks" response carries.</summary>
    internal const string DeadlocksNote =
        "occurred_at is when PostgreSQL wrote the report, not when the collector found it. "
        + "times_seen counts how often the collector saw the SAME report, never how many times "
        + "the deadlock happened - a genuine repeat appears as its own row with different "
        + "process IDs. What a given value MEANS depends on the transport: reading the log file "
        + "directly re-reads an overlapping tail, so times_seen climbs while the report stays "
        + "in the window and is a property of that read window. On RDS and Aurora the log API "
        + "is consume-once, so times_seen is normally 1 and a low value there is the ordinary "
        + "state rather than a partial count. It is not guaranteed to be 1 there either: the "
        + "collector saves its resume position, so a restart resumes where the last read "
        + "stopped, except for a deadlock report split across two reads, where the saved "
        + "position waits and a restart reads that part again; a window whose write did not "
        + "land is offered again too.";

    /// <summary>The "no_deadlocks" status text: why an empty window is not proof of a quiet server.</summary>
    internal static string NoDeadlocksText(string serverName, int hoursBack) =>
        $"No deadlock was reported on {serverName} in the last {hoursBack} "
        + "hour(s). FOUR different things produce that and they are worth telling apart: "
        + "the server had no deadlocks, which is the healthy answer; the log could not be "
        + "read; the log was read and PostgreSQL did not write it in English; or, on a target "
        + "that logs in the stderr format, the log was read but its line prefix is one the "
        + "reader cannot parse. The pg_plan_capture_readiness collector judges all four, "
        + "because plan capture reads the same file the same way - the English one as its "
        + "message_locale facet (#3061) and the prefix one as its log_line_prefix_readable "
        + "facet (#4735) - and get_pg_plan_capture_readiness is the read that returns those "
        + "facets with the remedy for each. "
        + "The last two are the causes nothing else hints at: lc_messages translates "
        + "PostgreSQL's own messages including the severity label, so a target writing "
        + "FEHLER: rather than ERROR: matches nothing, and a log_line_prefix that does not "
        + "start with a timestamp and carry the process id alone in brackets (as in "
        + "'%m [%p] ') has every line written under it dropped - either one looks exactly "
        + "like a quiet server. get_pg_server_config carries the target's lc_messages. "
        + "pg_stat_database's cumulative deadlock counter, in get_pg_database_stats, is "
        + "the independent check for all four - if it moved and nothing is here, the log "
        + "is the problem rather than the server.";

    /// <summary>The "empty" status text of <c>get_pg_deadlock_detail</c> when no hash was given.</summary>
    internal static string NoDeadlockGraphText(string serverName) =>
        $"No deadlock graph is stored for {serverName}. Either the server had no "
        + "deadlocks, which is the healthy answer, or its log could not be read, or it "
        + "was read in a language this does not match - PostgreSQL translates its own "
        + "messages under a non-English lc_messages, severity label included (#3061) - or "
        + "it was read on a stderr log target whose log_line_prefix the reader cannot parse, "
        + "so every line was dropped (the log_line_prefix_readable facet of "
        + "get_pg_plan_capture_readiness, #4735). "
        + "get_pg_database_stats carries pg_stat_database's cumulative deadlock counter, "
        + "which tells the healthy case from the others.";

    [McpServerTool(Name = "get_pg_deadlocks"), Description("Gets PostgreSQL deadlocks reported in the window, newest first: victim, cycle size, lock modes/resources, normalized statement. An empty answer does not mean none occurred: log_error_verbosity=terse drops the DETAIL field this reads, and a non-English lc_messages (its severity label is translated too) matches nothing - both look like a quiet server. Check both, or get_pg_database_stats' counter, first. times_seen is a sighting count, not a deadlock count: it depends on transport - self-hosted climbs on re-reads, RDS/Aurora normally stays 1 (normal there, not partial). <<GUIDE>> Gets PostgreSQL deadlocks that were reported in the window, newest first, with the victim process, how many sessions were in the cycle, the lock modes and resources involved, and the victim's statement, normalized: every literal is ?, and a statement that cannot be read to its end (PostgreSQL cuts each at track_activity_query_size) is withheld. PostgreSQL writes a complete deadlock report to its server log at default settings and needs nothing ENABLED on the target, unlike plan capture - but that is not the same as unsuppressable. log_error_verbosity = terse drops the DETAIL field, which is where the wait graph and every participant's SQL live, so the ERROR line is still logged and this tool still returns nothing. If this comes back empty, check log_error_verbosity on the target (and log_min_messages) before concluding the server does not deadlock; get_pg_database_stats' cumulative deadlock counter is the independent test. There is a third precondition and it is the least visible: lc_messages. PostgreSQL translates its own messages under a non-English locale, the SEVERITY LABEL INCLUDED, so a target writing FEHLER: rather than ERROR: matches nothing here and returns the same empty result a quiet server does. get_pg_server_config carries the target's lc_messages (pass include_defaults if it does not appear, because the compiled default is the empty value - and empty is itself inconclusive rather than safe, since the server then takes its language from its own environment, which no query can see). The pg_plan_capture_readiness collector judges it as the message_locale facet. There is a fourth precondition where the target logs in the stderr format: the log line prefix must also be readable, or the read finds nothing and looks like a quiet server. The log_line_prefix_readable facet judges it, and get_pg_plan_capture_readiness reports both facets. Each row is one DISTINCT deadlock, and a deadlock that genuinely recurred appears as a separate row because the participating process IDs differ. times_seen counts how often the collector saw that SAME report, and what a value means depends on the transport. Where the collector reads the log file itself it re-reads an overlapping tail every cycle on purpose, so one report is seen several times and times_seen climbs while it stays in the window. On RDS and Aurora the log API is consume-once, so a report is normally seen once and times_seen normally stays 1: there a low value is the ordinary state and NOT a partial count, so do not read 1 as 'seen once so far, expect more'. It is not guaranteed to be 1 there either - the collector saves its resume position, so a restart resumes where the last read stopped, except for a deadlock report split across two reads, where the saved position waits and a restart reads that part again; a window whose write did not land is offered again too - so treat times_seen as a sighting count whose meaning depends on the transport, and never as a count of deadlocks on either. Use get_pg_deadlock_detail with a deadlock_hash for the full wait graph and every participant's SQL. A report stored before its SQL was normalized at capture has a deadlock_hash that starts at-, which get_pg_deadlock_detail takes the same way. This is what bounds the page - read truncated to know whether the window held more distinct deadlocks than were returned; it is observed by fetching one row past this cap, never inferred from a full page.")]
    public static async Task<string> GetPgDeadlocks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum deadlocks to return. Default 25. See the tool's reading guide.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);
            /* #3653 (the #3541 A3 class, the residue #3679 named): the caller's limit + 1 as the fetch, the
               extra row as the OBSERVED truncation signal. This used to publish `rows.Count >= limit`, which
               says "more" for a window holding exactly `limit` distinct deadlocks - the one case an
               inference cannot tell from a busier window - and on the default limit of 25 that is a
               plausible shape for a bad afternoon, not a corner. McpHelpers.BoundPage trims the page back to
               `limit`, so deadlock_count below stays a count of what is returned. */
            var fetched = await DarlingPgDeadlockReader.GetDeadlocksAsync(
                postgres, resolved.ServerId, windowStart, windowEnd, limit + 1, cancellationToken);
            var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);

            if (rows.Count == 0)
            {
                /* Capability first — a permanent engine gap outranks a fixable precondition — then what the
                   collector's own last run recorded (#3410): a denied pg_read_file grant, the file it could
                   not open, or a logging_collector that is off all land in collection_log as a named
                   non-fatal skip, and quoting that sentence here is what turns "no deadlocks" into
                   not-collected WITH THE REASON. get_pg_plans already asks #2546's question for
                   pg_plan_capture; the deadlock read reads the same file the same way and had never asked. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_deadlocks", cancellationToken)
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_deadlocks", cancellationToken)
                    /* #4966: not_collected and the precondition stay bare; a quiet answer carries the window keys under hints. */
                    ?? McpHelpers.Status(
                        "no_deadlocks",
                        NoDeadlocksText(resolved.ServerName, hours_back),
                        (await DarlingMcpWindowNotice.ReadEventAsync(
                            () => DarlingMcpWindowNotice.Probe(postgres, "pg_deadlocks", resolved.ServerName, windowStart, windowEnd, cancellationToken),
                            null, windowStart, windowEnd, "pg_deadlocks", emptyAnswer: true, logger: logger, cancellationToken: cancellationToken)).AsHints());
            }

            /* #4966: the notice is always COVERAGE, whether or not the cap cut the page (the cap is reported by truncated). A sparse event
               list windowed on the event's own time: the floor is the earlier of the coverage probe (the schedule's retention edge and the
               server's first collection, never an oldest row) and the oldest event shown (a first run stores events from before itself). */
            var notice = await DarlingMcpWindowNotice.ReadEventAsync(
                () => DarlingMcpWindowNotice.Probe(postgres, "pg_deadlocks", resolved.ServerName, windowStart, windowEnd, cancellationToken),
                rows.Min(r => r.OccurredAtUtc), windowStart, windowEnd, "pg_deadlocks", logger: logger, cancellationToken: cancellationToken);

            var json = JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                status = "deadlocks",
                deadlock_count = rows.Count,
                truncated,
                note = DeadlocksNote,
                deadlocks = rows.Select(r => new
                {
                    occurred_at = r.OccurredAtUtc,
                    deadlock_hash = r.DeadlockHash,
                    /* The session PostgreSQL cancelled. It is the end whose application saw an error, which
                       is usually the only end anybody noticed. */
                    victim_pid = r.VictimPid,
                    participant_count = r.ParticipantCount,
                    lock_modes = r.LockModes,
                    resources = r.Resources,
                    victim_statement = r.VictimStatement,
                    times_seen = r.TimesSeen,
                }),
            }, McpHelpers.JsonOptions);

            /* A failed probe costs the notice, never the rows (see DarlingMcpWindowNotice.ReadAsync). */
            return notice.IsUnavailable ? DarlingMcpWindowNotice.WithoutKeys(json) : json;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_deadlocks", ex);
        }
    }

    [McpServerTool(Name = "get_pg_deadlock_detail"), Description("Gets PostgreSQL deadlock graphs in full: the complete wait graph as the server wrote it, naming every participant, the lock each was waiting for, who blocked whom, and each participant's statement, normalized: every literal is ?, and a statement that cannot be read to its end is withheld. Pass a deadlock_hash from get_pg_deadlocks for one specific report, or omit it to get the most recent graphs. This carries MORE than a SQL Server deadlock graph does - PostgreSQL names the SQL of every session in the cycle, where the SQL Server graph often leaves the non-victim side as a handle. The graph is stored as written rather than reassembled, apart from its statements' literals, so a lock type the parser does not break out separately is still readable here.")]
    public static async Task<string> GetPgDeadlockDetail(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("A deadlock_hash from get_pg_deadlocks. Omit for the most recent graphs.")] string? deadlock_hash = null,
        [Description("Maximum graphs to return when no hash is given. Default 5.")] int limit = 5,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        try
        {
            var rows = await DarlingPgDeadlockReader.GetDeadlockDetailAsync(
                postgres, resolved.ServerId, deadlock_hash, limit, cancellationToken);

            if (rows.Count == 0)
            {
                return string.IsNullOrWhiteSpace(deadlock_hash)
                    ? await DarlingEngineCapability.NotCollectedStatusAsync(
                          postgres, resolved.ServerId, resolved.ServerName, "pg_deadlocks", cancellationToken)
                      ?? McpHelpers.Status(
                          "empty",
                          NoDeadlockGraphText(resolved.ServerName))
                    : McpHelpers.Status(
                          "empty",
                          $"No deadlock with hash '{deadlock_hash}' is stored for {resolved.ServerName}. A "
                          + "hash identifies one report on ONE server, so one from a different server will "
                          + "not resolve here - and a report can age out of retention while a hash you are "
                          + "holding does not.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                status = "deadlock_detail",
                graph_count = rows.Count,
                note = "graph is PostgreSQL's own DETAIL block, as written apart from stripped tab indenting and "
                     + "the statements' literals, each replaced by ?; a statement that cannot be read to its end "
                     + "is withheld. It reads as: one line per wait edge naming who waits for what and who "
                     + "blocks them, then each participant's process ID followed by its statement.",
                deadlocks = rows.Select(r => new
                {
                    deadlock_hash = r.DeadlockHash,
                    occurred_at = r.OccurredAtUtc,
                    victim_pid = r.VictimPid,
                    participant_count = r.ParticipantCount,
                    lock_modes = r.LockModes,
                    resources = r.Resources,
                    graph = r.GraphText,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_deadlock_detail", ex);
        }
    }
}
