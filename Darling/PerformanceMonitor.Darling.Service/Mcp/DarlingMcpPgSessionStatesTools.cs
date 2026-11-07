/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
/// The MCP surface for session states, paired with the <c>pg_session_states</c> collector (#2540).
/// <para>The tool's job is the half the collector deliberately does not do: deciding what a long
/// idle-in-transaction session MEANS. The collector stores durations and a horizon age; the judgement that
/// separates "this is starving your vacuum" from "this is a connection pool doing nothing harmful" lives
/// here, and it is gated on <c>peak_horizon_age</c> rather than on the duration.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgSessionStatesTools
{
    /// <summary>
    /// The duration past which an idle-in-transaction session is worth a human's attention even when it
    /// pins nothing.
    /// <para>Five minutes. It holds no snapshot and no transaction id, so it is costing vacuum nothing — but
    /// it IS holding a connection and any locks the transaction took, and at five minutes it has stopped
    /// looking like application latency and started looking like a code path that forgot to commit. The
    /// finding says exactly that rather than borrowing the vacuum argument, which does not apply.</para>
    /// </summary>
    internal const long IdleWithoutHorizonAttentionMs = 5 * 60 * 1000;

    /// <summary>
    /// What this session was actually doing to the database, and the one place in this feature where the
    /// causal claim is made or withheld.
    ///
    /// <para><b><c>peak_horizon_age</c> decides, not the duration.</b> Four idle-in-transaction shapes were
    /// measured on a live PostgreSQL 16.15 instance and two of them pin nothing: a READ COMMITTED
    /// transaction that only read released its snapshot when the statement ended, and one whose UPDATE
    /// matched zero rows never got a transaction id at all. Both sat idle in transaction indefinitely with
    /// <c>backend_xmin</c> and <c>backend_xid</c> NULL. A tool that reasoned from the state string and the
    /// clock would tell someone to kill those sessions to save their vacuum, and killing them would change
    /// nothing about the horizon.</para>
    /// </summary>
    internal static string HorizonFinding(DarlingPgSessionStatesReader.PgSessionStateRow r)
    {
        /* Redaction first, and it outranks everything: if the state columns were redacted then the inputs
           to every other branch below are NULL, and a finding derived from them would be an artefact of a
           missing GRANT rather than an observation about the database. */
        if (r.StateWasRedacted)
        {
            return "CANNOT SAY - the monitoring login lacks pg_monitor on this target, so PostgreSQL "
                 + "redacted this row. Only pid, application_name, database and user survive - state, "
                 + "state_change, xact_start, query_start, backend_start, wait_event, backend_type, "
                 + "client_addr and query_id all came back NULL and the query text was replaced. "
                 + "backend_xmin and backend_xid are NOT redacted, which is why an xid age can still "
                 + "appear above: the horizon still reads as pinned and nothing here can say by what. "
                 + "Grant pg_monitor to the monitoring role and this becomes answerable.";
        }

        var sustained = DarlingPgSessionStatesReader.IsSustainedHolder(r);

        if (r.HorizonHolderSamples > 0 && sustained)
        {
            /* #8092: the age is the evidence, so the sentence states it beside the sample share. */
            return "PINS THE XMIN HORIZON at a peak age of "
                 + r.PeakHorizonAge.ToString("N0", CultureInfo.InvariantCulture)
                 + " transactions, and did so across most of the samples that saw it "
                 + $"({r.HorizonHolderSamples} of {r.SampleCount}). While this transaction stays open, "
                 + "VACUUM cannot reclaim any row version newer than it on ANY table in the cluster - which "
                 + "is why bloat grows on tables this session never touched, and why autovacuum keeps "
                 + "running and reporting success while reclaiming nothing. Cross-check the same window in "
                 + "get_pg_xmin_horizon: its 'session' row should name this pid. The fix is to make the "
                 + "transaction end, which is an application change, not a database one - terminating the "
                 + "backend releases the horizon immediately but the code path that opened it will do it "
                 + "again.";
        }

        if (r.HorizonHolderSamples > 0 && r.PeakHorizonAge > 0 && DarlingPgSessionStatesReader.HoldsAcrossSamples(r))
        {
            /* #8092: the oldest holder in enough samples to count, but at an age too small to have held
               anything back: a short writer caught at each sample, which is what a maintenance job running a
               brief transaction every few minutes looks like. Said with the age and the floor so the row
               explains its own band, and so a reader can disagree with the floor rather than trust it. */
            var brief = "Was the oldest open transaction on the instance in "
                      + $"{r.HorizonHolderSamples} of {r.SampleCount} sample(s), but its horizon age peaked at "
                      + r.PeakHorizonAge.ToString("N0", CultureInfo.InvariantCulture)
                      + " transaction(s), under the "
                      + DarlingPgSessionStatesReader.MinSustainedHolderHorizonAge.ToString("N0", CultureInfo.InvariantCulture)
                      + " that makes a hold: at most that many transactions' row versions waited behind it, "
                      + "which is the shape of a short writer seen at each sample rather than a transaction "
                      + "VACUUM was stuck behind. It becomes a finding if the age climbs.";

            return brief + LongIdleOtherAxis(r,
                ", though what it pinned was small. A forgotten commit is still a forgotten commit.");
        }

        if (r.HorizonHolderSamples > 0 && r.PeakHorizonAge == 0)
        {
            /* #7381: the oldest open transaction on the instance, but at age 0 it held the newest xid or
               snapshot, so no row version waited behind it. On a quiet instance the oldest is simply
               whatever ran. Said plainly so a count like 5 of 5 does not read as a hold. */
            var zero = "Was the oldest open transaction on the instance in "
                     + $"{r.HorizonHolderSamples} of {r.SampleCount} sample(s), but its horizon age never rose "
                     + "above 0: it held the newest xid or snapshot, so no row version waited behind it and "
                     + "VACUUM lost nothing. On a quiet instance the oldest transaction is just whatever ran.";

            /* Same long-idle check as the passing branch below, and for the same reason: a forgotten
               idle-in-transaction writer on a quiet target is age 0 here AND Warning in SessionSeverity,
               so returning only the sentence above put "lost nothing" and a Warning in one JSON row. */
            return zero + LongIdleOtherAxis(r,
                ", though at age 0 it pinned nothing for VACUUM to wait behind. A forgotten commit is "
                + "still a forgotten commit on a quiet instance.");
        }

        if (r.HorizonHolderSamples > 0)
        {
            var passing = "Held the oldest xmin in "
                        + $"{r.HorizonHolderSamples} of {r.SampleCount} sample(s) - a passing sighting "
                        + "rather than a sustained hold. Every write transaction is briefly the oldest "
                        + "holder, so the hold ON ITS OWN is normal traffic. It becomes a finding if the "
                        + "share climbs.";

            /* But the hold is not the only thing this row can be. A session that is ALSO idle in
               transaction past the attention floor is a real finding whatever its share of the holds -
               and returning only the sentence above for it was a genuine bug: the severity band said
               Warning while this text said "not a finding", in the same JSON object. The two now agree,
               and the reason they nearly did not is that is_horizon_holder means OLDEST rather than
               HOLDS: several long transactions on one instance take turns being oldest, so none of them
               reaches the sustained threshold and every one of them still needs saying. */
            return passing + LongIdleOtherAxis(r,
                ", and it did pin the horizon while it did so - just not as the oldest holder for most of "
                + "the window. Being the oldest is a property of the INSTANCE, not of this "
                + "session: on a busy one several long transactions take turns.");
        }

        if (r.PeakHorizonAge < 0 && r.IdleInTransactionSamples > 0)
        {
            var note = "Idle in transaction, but pinned NOTHING - it held neither a snapshot "
                     + "(backend_xmin) nor a transaction id (backend_xid) in any sample. This is the "
                     + "ordinary shape of a READ COMMITTED transaction that has only read, or whose write "
                     + "matched no rows: the snapshot is released at the end of each statement. It is "
                     + "costing VACUUM nothing, so killing it will not help bloat or wraparound.";

            return r.PeakStateDurationMs >= IdleWithoutHorizonAttentionMs
                ? note + " It is still worth looking at on its own terms: at "
                       + FormatDuration(r.PeakStateDurationMs) + " idle inside a transaction it is holding a "
                       + "connection and any locks the transaction already took, which is a forgotten "
                       + "commit rather than a vacuum problem."
                : note;
        }

        if (r.PeakHorizonAge < 0)
        {
            return "Pinned nothing - no snapshot and no transaction id in any sample. Held a transaction "
                 + "open, but not one that VACUUM had to wait behind.";
        }

        return "Held a transaction id or snapshot at a peak age of "
             + r.PeakHorizonAge.ToString("N0", CultureInfo.InvariantCulture)
             + " transactions, but was never the OLDEST holder on the instance, so something else was "
             + "setting the horizon. See get_pg_xmin_horizon for what.";
    }

    /// <summary>
    /// The long-idle-in-transaction sentence both holder branches of <see cref="HorizonFinding"/> share, so
    /// the text agrees with <see cref="SessionSeverity"/>'s Warning arm (same condition) whichever branch the
    /// row lands in. Empty when the row is not long idle. <paramref name="tail"/> is what is particular to
    /// the branch, and starts with its own punctuation.
    /// </summary>
    private static string LongIdleOtherAxis(DarlingPgSessionStatesReader.PgSessionStateRow r, string tail)
        => r.IdleInTransactionSamples > 0 && r.PeakStateDurationMs >= IdleWithoutHorizonAttentionMs
            ? " It is a finding on the OTHER axis though: at " + FormatDuration(r.PeakStateDurationMs)
              + " idle inside a transaction it is holding a connection and any locks the transaction took"
              + tail
            : "";

    /// <summary>
    /// Severity band, server-computed so the browser and the WPF row styles paint the same thing from the
    /// same decision rather than each re-deriving it.
    /// <para>The four words are the HOUSE vocabulary — <c>Healthy</c>, <c>Warning</c>, <c>Critical</c>,
    /// <c>Unknown</c>, capitalised — the same set <c>IndexSeverity</c> and <c>BloatSeverity</c> return, and
    /// the only set the shared <c>sev-*</c> CSS defines. A private vocabulary here would be worse than
    /// wrong: <c>sev-info</c> and <c>sev-none</c> match no rule, so the badge would render unstyled rather
    /// than failing, which is the kind of defect that ships.</para>
    /// </summary>
    internal static string SessionSeverity(DarlingPgSessionStatesReader.PgSessionStateRow r)
    {
        if (r.StateWasRedacted)
        {
            /* Its own band, above every severity, for the same reason pg_table_bloat_stats gives
               estimate_unavailable one: this row carries no trustworthy state at all, and painting it as
               healthy or as critical would both be inventions. */
            return "Unknown";
        }

        var sustained = DarlingPgSessionStatesReader.IsSustainedHolder(r);

        if (r.HorizonHolderSamples > 0 && sustained)
        {
            return "Critical";
        }

        /* The Warning band is LONG IDLE IN TRANSACTION, and it is deliberately not conditioned on
           whether the session pinned anything. Review suggested adding "&& PeakHorizonAge < 0" here to
           match the viewer; the agreement was real and the direction was wrong. is_horizon_holder means
           OLDEST on the instance rather than HOLDS, so on a busy instance several genuinely long idle
           transactions each take turns being oldest, none of them clears the sustained threshold, and
           that gate would have painted every one of them Healthy - the dangerous direction. The viewer
           and the finding text were changed to agree with THIS instead. */
        if (r.IdleInTransactionSamples > 0 && r.PeakStateDurationMs >= IdleWithoutHorizonAttentionMs)
        {
            return "Warning";
        }

        /* A passing sighting collapses into Healthy rather than getting a band of its own. Every write
           transaction is briefly the oldest xmin holder, so a distinct band there would paint ordinary
           traffic and teach people to ignore the column. The sample counts are on the row for anyone who
           wants to see the difference. */
        return "Healthy";
    }

    /// <summary>#7528: the reader answers -1 for a duration the collector did not measure (an autovacuum worker in
    /// state <c>starting</c> has no query start). The <c>*_duration</c> text twins already read "not measured"; the
    /// numeric <c>_ms</c> field answers null so -1 cannot be read as a measured duration. Same treatment as the
    /// replication slot sizes in <c>get_pg_replication_slots</c>. peak_horizon_age and the xmin/xid ages keep their
    /// documented -1 (pinned nothing).</summary>
    internal static long? MeasuredMs(long milliseconds) => milliseconds >= 0 ? milliseconds : null;

    internal static string FormatDuration(long milliseconds)
    {
        if (milliseconds < 0)
        {
            return "not measured";
        }

        var span = TimeSpan.FromMilliseconds(milliseconds);
        if (span.TotalMinutes < 1)
        {
            return span.TotalSeconds.ToString("0.#", CultureInfo.InvariantCulture) + "s";
        }

        return span.TotalHours < 1
            ? span.TotalMinutes.ToString("0.#", CultureInfo.InvariantCulture) + "m"
            : span.TotalHours.ToString("0.#", CultureInfo.InvariantCulture) + "h";
    }

    [McpServerTool(Name = "get_pg_session_states")]
    [Description("Gets PostgreSQL sessions holding a transaction open: how long, and WHETHER they are pinning the xmin horizon. peak_horizon_age carries that, not duration; -1 means pinned NOTHING, not a small age. Critical (PINS THE XMIN HORIZON) needs a hold across the samples AND a peak_horizon_age of at least 10,000 transactions; a short writer seen at every sample at an age of 1 or 2 is named as such, not graded Critical. THIS IS A SAMPLE at the collection interval - a transaction that opened and closed between samples is invisible; captures_in_window is the denominator, and captures_in_window = 0 means unavailable, not a clean bill. Requires pg_monitor; without it a row's state columns come back NULL (state_was_redacted) rather than refused. last_query_text is the statement text. Window ends at as_of. <<GUIDE>> PostgreSQL sessions holding a transaction open - who is idle in transaction, for how long, and WHETHER THAT SESSION IS ACTUALLY PINNING THE XMIN HORIZON. That last part is the whole point and it is not the same question as the first two: measured on a live instance, an idle-in-transaction session under READ COMMITTED that has only read, or whose UPDATE matched zero rows, holds neither a snapshot nor a transaction id and starves VACUUM of nothing at all. Only a transaction that has written (holding backend_xid) or one under REPEATABLE READ (holding backend_xmin) pins anything. So peak_horizon_age, not the duration, is what supports a causal claim, and a peak_horizon_age of -1 means the session pinned NOTHING rather than something small. Pairs with get_pg_xmin_horizon, which names the CLASS of holder; this names the session. Rolled up per backend across the window, horizon holders first, then longest transaction. THIS IS A SAMPLE at the collection interval, not an event log - PostgreSQL records nothing about session state unless something asks, so a transaction that opened and closed between samples is invisible; the capture counts are reported so 'nothing found' is distinguishable from 'nobody looked'. No raw query text is stored: pg_stat_activity.query carries literal parameter values, so the normalised query_id and a whitelisted command keyword are stored instead - last_query_text is that statement's normalised text with placeholders (a 400-character preview, last_query_text_truncated says it was cut, full_text: true returns it whole), null until the hourly statement capture reaches the id and always null for query id 0 and a null id; join last_query_id to get_pg_top_queries for its call counts. Requires pg_monitor on the target; without it PostgreSQL silently returns rows with every state column NULL, which the read reports rather than hides. This is what bounds the page - read truncated to know whether the window held more sessions than were returned; it is observed by fetching one row past this cap, never inferred from a full page.")]
    public static async Task<string> GetPgSessionStates(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum sessions to return, horizon holders first then longest transaction. Default 25. See the tool's reading guide.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Return each row's full last_query_text instead of a 400-character preview. Default false.")] bool full_text = false,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorForAnswerAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);
            /* #3653 (one vocabulary): the page cut is OBSERVED off a limit + 1 fetch through McpHelpers.BoundPage
               (the #3594 dialect), replacing `limit_reached = sessions.Count >= limit` — which read a window of
               exactly `limit` sessions as a cut page. */
            var fetched = await DarlingPgSessionStatesReader.GetPgSessionStatesAsync(
                postgres, resolved.ServerId, start, end, limit + 1, cancellationToken);
            var (rows, truncated) = McpHelpers.BoundPage(fetched, limit);

            /* Fetched whether or not there are rows. On a surface where zero rows is the healthy answer, the
               denominator is not an error path - it is what makes a healthy answer believable. */
            var captures = await DarlingPgSessionStatesReader.GetPgSessionStatesCaptureCountsAsync(
                postgres, resolved.ServerId, start, end, cancellationToken);

            if (rows.Count == 0)
            {
                return await EmptyAsync(postgres, resolved.ServerId, resolved.ServerName, resolved.DisplayName, hours_back, captures, start, end, logger, cancellationToken);
            }

            /* #4966: decided after the empty branch, whose not-collected answer carries no notice. */
            var notice = await DarlingMcpWindowNotice.ReadForToolAsync(
                postgres, "get_pg_session_states", resolved.ServerName, start, end, emptyAnswer: false, logger, cancellationToken);

            var holders = rows.Count(r => r.HorizonHolderSamples > 0 && !r.StateWasRedacted);
            var idleHolders = rows.Count(r => r.HorizonHolderSamples > 0 && r.IdleInTransactionSamples > 0
                                              && !r.StateWasRedacted);
            var idleWithoutHorizon = rows.Count(r => r.IdleInTransactionSamples > 0 && r.PeakHorizonAge < 0
                                                     && !r.StateWasRedacted);
            var redacted = rows.Count(r => r.StateWasRedacted);
            /* The collector's per-capture cap, a SOURCE-side fact carried per row as capture_was_truncated — a
               different cut from the page's `truncated` above, and named apart from it on purpose. */
            var captureWasTruncated = rows.Any(r => r.CaptureWasTruncated);

            var sessions = rows.Select(r => new
            {
                pid = r.Pid,
                /* The collector's synthetic (backend_start, pid) identity, and the reason a pid alone is not
                   the key: pids are reused, and this id is comparable to the one on get_pg_blocking's rows
                   for the same backend. */
                backend_id = r.BackendId.ToString(CultureInfo.InvariantCulture),
                severity = SessionSeverity(r),
                database = r.DatabaseName,
                username = r.Username,
                application_name = r.ApplicationName,
                client_addr = r.ClientAddr,
                backend_type = r.BackendType,
                last_state = r.LastState,
                last_wait_event_type = r.LastWaitEventType,
                last_wait_event = r.LastWaitEvent,
                /* The leading SQL keyword, whitelisted at collection. Not a truncation of the statement -
                   see the collector for why a substring would be a data-leak vector. */
                last_command_tag = r.LastCommandTag,
                /* Normalised statement identity. Join it to get_pg_top_queries for the text with $1
                   placeholders where the literals were. */
                last_query_id = r.LastQueryId,
                /* #7428: the statement behind the id, a preview unless full_text, null until the hourly capture reaches
                   that id (the same rules as get_pg_top_queries), and always null for query id 0 and a null id. */
                last_query_text = DarlingMcpPgStatementTools.StatementTextFor(r.LastQueryText, full_text).Text,
                last_query_text_truncated = DarlingMcpPgStatementTools.StatementTextFor(r.LastQueryText, full_text).Truncated,
                query_id_note = r.LastQueryId is null
                    ? "No query_id. PostgreSQL 13 does not have the column at all; on 14+ it is NULL when "
                    + "compute_query_id is off, and on a redacted row it is NULL with everything else "
                    + "privileged. Check the target's version before reading anything into this."
                    : null,
                peak_xact_duration_ms = MeasuredMs(r.PeakXactDurationMs),
                peak_xact_duration = FormatDuration(r.PeakXactDurationMs),
                peak_state_duration_ms = MeasuredMs(r.PeakStateDurationMs),
                peak_state_duration = FormatDuration(r.PeakStateDurationMs),
                peak_query_duration_ms = MeasuredMs(r.PeakQueryDurationMs),
                /* How long the BACKEND has existed, against how long it has held its transaction. The pair
                   separates two different bugs that look identical in the transaction duration alone: a
                   connection created ten minutes ago that has been idle in transaction for all ten is a
                   pool handing out a session nobody finished with, while a three-day-old worker that has
                   held one for ten minutes is a code path that forgot to commit. */
                backend_age_ms = MeasuredMs(r.PeakBackendDurationMs),
                backend_age = FormatDuration(r.PeakBackendDurationMs),
                /* -1 means pinned NOTHING. Not a small age - no age. */
                peak_horizon_age = r.PeakHorizonAge,
                peak_xmin_age = r.PeakXminAge,
                peak_xid_age = r.PeakXidAge,
                pinned_the_horizon = r.PeakHorizonAge >= 0,
                horizon_holder_samples = r.HorizonHolderSamples,
                idle_in_transaction_samples = r.IdleInTransactionSamples,
                sample_count = r.SampleCount,
                first_seen_at = r.FirstSeenAt.ToString("o"),
                last_seen_at = r.LastSeenAt.ToString("o"),
                state_was_redacted = r.StateWasRedacted,
                /* Capture context from this backend's most recent sample: two idle-in-transaction sessions
                   out of six connections is a different instance from two out of four thousand. */
                sessions_on_instance = r.TotalSessions,
                active_sessions_on_instance = r.ActiveSessions,
                idle_in_transaction_on_instance = r.IdleInTransactionSessions,
                reportable_sessions_on_instance = r.ReportableSessions,
                capture_was_truncated = r.CaptureWasTruncated,
                finding = HorizonFinding(r),
            })
            .ToList();

            return DarlingMcpWindowNotice.Finish(JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* #4966: the window floor, right after hours_back. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                status = "session_states",
                /* The page's count under the page's name (#3594). */
                sessions_returned = sessions.Count,
                horizon_holder_count = holders,
                idle_in_transaction_holder_count = idleHolders,
                /* Reported as its own number because it is the correction this tool exists to make: these
                   sessions look exactly like the harmful ones and are not. */
                idle_in_transaction_pinning_nothing = idleWithoutHorizon,
                /* The denominator, always, on a sampled surface. */
                captures_in_window = captures.CapturesTotal,
                captures_with_sessions = captures.CapturesWithSessions,
                first_capture_at = captures.FirstCaptureAt?.ToString("o"),
                last_capture_at = captures.LastCaptureAt?.ToString("o"),
                redacted_row_count = redacted,
                truncated,
                note = "This is a SAMPLE taken every collection cycle, not an event log. PostgreSQL records "
                     + "nothing about session state unless something asks it, so a transaction that opened "
                     + "and closed between two samples left no trace - "
                     + $"{captures.CapturesWithSessions} of {captures.CapturesTotal} capture(s) in this "
                     + "window found any reportable session at all. Duration alone is NOT evidence that a "
                     + "session is starving VACUUM: read peak_horizon_age, where -1 means the session pinned "
                     + "nothing."
                     + (idleWithoutHorizon > 0
                         ? $" {idleWithoutHorizon} session(s) here were idle in transaction and pinned "
                         + "NOTHING - they held no snapshot and no transaction id, so terminating them "
                         + "would not reclaim a single dead row."
                         : string.Empty)
                     + (redacted > 0
                         ? $" {redacted} row(s) are REDACTED: the monitoring login lacks pg_monitor on this "
                         + "target, so PostgreSQL returned the rows with every state column NULL rather "
                         + "than refusing the read. Those rows say nothing about session state and their "
                         + "severity is reported as unknown."
                         : string.Empty)
                     + (captureWasTruncated
                         ? " At least one capture hit the collector's per-capture row cap, so the stored "
                         + "rows for it are a worst-first sample of a larger set - compare "
                         + "reportable_sessions_on_instance against the rows returned."
                         : string.Empty)
                     + (truncated
                         ? $" TRUNCATED: the window held more sessions than the {limit} returned, so the counts "
                         + "above cover only the sessions returned. Raise limit for the full picture."
                         : string.Empty),
                sessions,
            }, McpHelpers.JsonOptions), notice);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_pg_session_states", ex);
        }
    }

    /// <summary>
    /// Which KIND of nothing an empty result is. The engine question is asked FIRST (#2532): "no long
    /// transactions" said about a SQL Server target is not a weak answer but a false one.
    /// <para>The denominator comes from <c>collection_log</c> and not from the data, because this is an
    /// EXCEPTION surface like <c>pg_blocking_edges</c>: the collector stores nothing when every session is
    /// behaving, so an absence of rows is the HEALTHY state and probing the table itself would report a
    /// perfectly monitored server as uncollected (#2508).</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, string displayName, int hoursBack,
        DarlingPgSessionStatesReader.PgSessionStatesCaptureCounts captures,
        DateTime start, DateTime end, ILogger? logger, CancellationToken cancellationToken = default)
    {
        var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
            postgres, serverId, displayName, "pg_session_states", cancellationToken);
        if (gated != null)
        {
            return gated;
        }

        var hints = new
        {
            server = displayName,
            hours_back = hoursBack,
            captures_in_window = captures.CapturesTotal,
            captures_with_sessions = captures.CapturesWithSessions,
            first_capture_at = captures.FirstCaptureAt?.ToString("o"),
            last_capture_at = captures.LastCaptureAt?.ToString("o"),
        };

        if (captures.CapturesTotal > 0)
        {
            /* #4966: the all-clear carries the window floor beside its own hints; the unavailable answer below stays bare. */
            var notice = await DarlingMcpWindowNotice.ReadForToolAsync(
                postgres, "get_pg_session_states", serverName, start, end, emptyAnswer: true, logger, cancellationToken);
            return await DarlingServerOfflineNote.EmptyAsync(postgres, serverId, displayName, end, McpHelpers.Status(
                "empty",
                McpHelpers.QuietUnlessCut(
                    notice.WindowTruncated, notice.EffectiveStart,
                    factual: $"No session held a transaction open past the collector's floor on {displayName} in the last "
                        + $"{hoursBack} hour(s), across {captures.CapturesTotal} capture(s)",
                    coveredClaim: ". This is the healthy "
                        + "state and a real all-clear rather than missing data - the collector stores nothing when "
                        + "every transaction is short.",
                    tail: " One caveat that is not a hedge: this samples at the "
                        + "collection interval, so a transaction that opened and closed between two samples is "
                        + "genuinely invisible here."),
                notice.IsUnavailable ? hints : new
                {
                    hints.server,
                    hints.hours_back,
                    hints.captures_in_window,
                    hints.captures_with_sessions,
                    hints.first_capture_at,
                    hints.last_capture_at,
                    effective_start = notice.EffectiveStart,
                    window_truncated = notice.WindowTruncated,
                    truncation_note = notice.TruncationNote,
                }), cancellationToken, start);
        }

        return await DarlingServerOfflineNote.UnavailableAsync(
            postgres, serverId, displayName,
            $"No captures at all for {displayName} in the last {hoursBack} hour(s), so the window says "
            + "nothing either way - this is NOT an all-clear. Check that collection is running and enabled "
            + "for this server with get_collection_health, or widen hours_back.",
            hints, cancellationToken);
    }
}
