/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the stored session-state samples (<c>pg_session_states</c>), rolled up per BACKEND rather than per
/// sample — one row per session that held a transaction open in the window, however many captures saw it.
/// <para>The rollup is the point. A session parked idle in transaction for two hours appears in a hundred and
/// twenty captures, and a read that returned them raw would report one problem a hundred and twenty times and
/// push every other session under the row limit. Grouping on the collector's synthetic backend id — which
/// packs <c>backend_start</c> with the pid — rather than on the pid means a pid reused by a second backend
/// inside the window is two findings and not one merged average.</para>
/// </summary>
public static class DarlingPgSessionStatesReader
{
    public sealed record PgSessionStateRow(
        long BackendId,
        int Pid,
        string? DatabaseName,
        string? Username,
        string? ApplicationName,
        string? ClientAddr,
        string? BackendType,
        string? LastState,
        string? LastWaitEventType,
        string? LastWaitEvent,
        string? LastCommandTag,
        long? LastQueryId,
        DateTime FirstSeenAt,
        DateTime LastSeenAt,
        int SampleCount,
        int IdleInTransactionSamples,
        int HorizonHolderSamples,
        long PeakStateDurationMs,
        long PeakXactDurationMs,
        long PeakQueryDurationMs,
        long PeakBackendDurationMs,
        long PeakHorizonAge,
        long PeakXminAge,
        long PeakXidAge,
        bool StateWasRedacted,
        int TotalSessions,
        int ActiveSessions,
        int IdleInTransactionSessions,
        int ReportableSessions,
        bool CaptureWasTruncated);

    /// <summary>
    /// One row per backend seen in the window, with its peaks and its identity from the most recent sample.
    ///
    /// <para><b><c>horizon_holder_samples</c> is the column that answers the question this surface exists
    /// for</b>, and it is a COUNT rather than a flag on purpose. A session that was the oldest xmin holder in
    /// one capture out of a hundred was momentarily at the front of a queue everything passes through; a
    /// session that was the holder in ninety-eight of them is the reason vacuum is not reclaiming anything.
    /// Those are opposite findings and a boolean cannot separate them.</para>
    ///
    /// <para><b><c>peak_horizon_age</c> of <c>-1</c> means the session pinned NOTHING</b>, not that it pinned
    /// something small — the sentinel the collector writes, carried through <c>max()</c> unchanged because
    /// -1 loses to every real age. This is the distinction the whole feature turns on: an idle-in-transaction
    /// session under READ COMMITTED that only read, or whose write matched no rows, holds neither a snapshot
    /// nor a transaction id and starves vacuum of exactly nothing. Both were measured on a live instance.
    /// Reporting a long idle-in-transaction duration WITHOUT this column beside it is how a monitoring tool
    /// talks somebody into killing a harmless session.</para>
    ///
    /// <para><b>Peaks, not averages.</b> The finding is how far this went, and an average over a hundred
    /// samples of a transaction that grew monotonically reports roughly half of what actually happened. The
    /// first and last sample times and the sample count travel alongside so the peak can be placed in time
    /// rather than floating free.</para>
    ///
    /// <para><b>Horizon holders sort first, then the longest transaction</b> — worst-first, not newest-first,
    /// the same ordering argument <c>DarlingPgBlockingReader</c> makes. A newest-first read under a row limit
    /// would return whatever happened at the end of the window and could omit the incident entirely. Holders
    /// lead because theirs is the only row carrying a proven causal claim about the horizon.</para>
    ///
    /// <para><b><c>capture_was_truncated</c> compares the collector's pre-limit count against the rows it
    /// actually stored</b>, per capture. The collector caps rows per capture, so on an instance parking
    /// thousands of sessions the stored set is a worst-first sample of a much larger population, and a read
    /// that did not say so would silently under-report the scale of the very thing it is describing.</para>
    ///
    /// <para><b>Joins use <c>IS NOT DISTINCT FROM</c> where a column is nullable</b>, because <c>=</c> is
    /// NULL for a NULL operand and would silently drop the row rather than match it.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 row limit.</para>
    /// </summary>
    public const string PgSessionStatesSql = """
        WITH samples AS (
            SELECT
                collection_id,
                collection_time,
                backend_id,
                pid,
                database_name,
                username,
                application_name,
                client_addr,
                backend_type,
                state,
                wait_event_type,
                wait_event,
                command_tag,
                query_id,
                state_duration_ms,
                xact_duration_ms,
                query_duration_ms,
                backend_duration_ms,
                xmin_age,
                xid_age,
                horizon_age,
                is_idle_in_transaction,
                is_horizon_holder,
                state_is_redacted,
                total_sessions,
                active_sessions,
                idle_in_transaction_sessions,
                reportable_sessions
            FROM pg_session_states
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        capture_shape AS (
            SELECT
                collection_id,
                count(*)::bigint                                    AS rows_stored,
                max(reportable_sessions)                            AS reportable_sessions
            FROM samples
            GROUP BY collection_id
        ),
        latest AS (
            SELECT DISTINCT ON (backend_id)
                backend_id,
                collection_time                                     AS last_sample_at,
                pid,
                database_name,
                username,
                application_name,
                client_addr,
                backend_type,
                state,
                wait_event_type,
                wait_event,
                command_tag,
                query_id,
                total_sessions,
                active_sessions,
                idle_in_transaction_sessions,
                reportable_sessions
            FROM samples
            ORDER BY backend_id, collection_time DESC
        ),
        rolled AS (
            SELECT
                s.backend_id,
                min(s.collection_time)                              AS first_seen_at,
                max(s.collection_time)                              AS last_seen_at,
                count(*)::int                                       AS sample_count,
                count(*) FILTER (WHERE s.is_idle_in_transaction)::int AS idle_in_transaction_samples,
                count(*) FILTER (WHERE s.is_horizon_holder)::int     AS horizon_holder_samples,
                max(s.state_duration_ms)                            AS peak_state_duration_ms,
                max(s.xact_duration_ms)                             AS peak_xact_duration_ms,
                max(s.query_duration_ms)                            AS peak_query_duration_ms,
                max(s.backend_duration_ms)                          AS peak_backend_duration_ms,
                max(s.horizon_age)                                  AS peak_horizon_age,
                max(s.xmin_age)                                     AS peak_xmin_age,
                max(s.xid_age)                                      AS peak_xid_age,
                bool_or(coalesce(s.state_is_redacted, false))       AS state_was_redacted,
                bool_or(c.reportable_sessions > c.rows_stored)      AS capture_was_truncated
            FROM samples AS s
            JOIN capture_shape AS c
              ON  c.collection_id IS NOT DISTINCT FROM s.collection_id
            GROUP BY s.backend_id
        )
        SELECT
            coalesce(l.backend_id, 0)                               AS backend_id,
            coalesce(l.pid, 0)                                      AS pid,
            l.database_name                                         AS database_name,
            l.username                                              AS username,
            l.application_name                                      AS application_name,
            l.client_addr                                           AS client_addr,
            l.backend_type                                          AS backend_type,
            l.state                                                 AS last_state,
            l.wait_event_type                                       AS last_wait_event_type,
            l.wait_event                                            AS last_wait_event,
            l.command_tag                                           AS last_command_tag,
            l.query_id                                              AS last_query_id,
            r.first_seen_at                                         AS first_seen_at,
            r.last_seen_at                                          AS last_seen_at,
            r.sample_count                                          AS sample_count,
            r.idle_in_transaction_samples                           AS idle_in_transaction_samples,
            r.horizon_holder_samples                                AS horizon_holder_samples,
            coalesce(r.peak_state_duration_ms, -1)                  AS peak_state_duration_ms,
            coalesce(r.peak_xact_duration_ms, -1)                   AS peak_xact_duration_ms,
            coalesce(r.peak_query_duration_ms, -1)                  AS peak_query_duration_ms,
            coalesce(r.peak_backend_duration_ms, -1)                AS peak_backend_duration_ms,
            coalesce(r.peak_horizon_age, -1)                        AS peak_horizon_age,
            coalesce(r.peak_xmin_age, -1)                           AS peak_xmin_age,
            coalesce(r.peak_xid_age, -1)                            AS peak_xid_age,
            coalesce(r.state_was_redacted, false)                   AS state_was_redacted,
            coalesce(l.total_sessions, 0)                           AS total_sessions,
            coalesce(l.active_sessions, 0)                          AS active_sessions,
            coalesce(l.idle_in_transaction_sessions, 0)             AS idle_in_transaction_sessions,
            coalesce(l.reportable_sessions, 0)                      AS reportable_sessions,
            coalesce(r.capture_was_truncated, false)                AS capture_was_truncated
        FROM rolled AS r
        JOIN latest AS l
          ON  l.backend_id IS NOT DISTINCT FROM r.backend_id
        ORDER BY
            (r.horizon_holder_samples > 0) DESC,
            r.peak_xact_duration_ms DESC,
            r.peak_state_duration_ms DESC,
            l.backend_id
        LIMIT $4
        """;

    public static async Task<List<PgSessionStateRow>> GetPgSessionStatesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgSessionStateRow>();
        await using var command = postgres.CreateCommand(PgSessionStatesSql);
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND. Npgsql does not reject Kind=Utc - it infers timestamptz, and
           PostgreSQL then resolves the comparison against these NAIVE timestamp columns by converting them
           at the store session's TimeZone, so east of UTC the window slides off the data and the read
           returns nothing at all. Same convention as every other PostgreSQL read here. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(MapRow(reader));
        }

        return rows;
    }

    /// <summary>
    /// Maps one row by ORDINAL. The projection above and this method are a positional contract: a column
    /// inserted in one without the other shifts every field after it and produces rows that are wrong
    /// rather than rows that throw.
    /// </summary>
    internal static PgSessionStateRow MapRow(System.Data.Common.DbDataReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return new PgSessionStateRow(
            reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            /* Stays NULL rather than collapsing to a sentinel. 0 is a legal query_id, and all three reasons
               this is absent - PostgreSQL 13, compute_query_id off, or a redacted row - mean "not measured",
               which is what NULL means. */
            reader.IsDBNull(11) ? null : reader.GetInt64(11),
            reader.GetDateTime(12),
            reader.GetDateTime(13),
            reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
            reader.IsDBNull(15) ? 0 : reader.GetInt32(15),
            reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
            /* -1 for every unmeasured duration and age, matching the collector's sentinel. 0 would read as
               "started this instant" for a duration and as "holds the newest possible xid" for an age; both
               are claims, where -1 is visibly not a measurement. */
            reader.IsDBNull(17) ? -1 : reader.GetInt64(17),
            reader.IsDBNull(18) ? -1 : reader.GetInt64(18),
            reader.IsDBNull(19) ? -1 : reader.GetInt64(19),
            reader.IsDBNull(20) ? -1 : reader.GetInt64(20),
            reader.IsDBNull(21) ? -1 : reader.GetInt64(21),
            reader.IsDBNull(22) ? -1 : reader.GetInt64(22),
            reader.IsDBNull(23) ? -1 : reader.GetInt64(23),
            /* Defaults TRUE on an absent value, deliberately the cautious direction: state_was_redacted is
               the flag that tells a reader the state columns cannot be trusted, so an unknown must not
               manufacture confidence. Every other boolean here defaults false. */
            reader.IsDBNull(24) || reader.GetBoolean(24),
            reader.IsDBNull(25) ? 0 : reader.GetInt32(25),
            reader.IsDBNull(26) ? 0 : reader.GetInt32(26),
            reader.IsDBNull(27) ? 0 : reader.GetInt32(27),
            reader.IsDBNull(28) ? 0 : reader.GetInt32(28),
            !reader.IsDBNull(29) && reader.GetBoolean(29));
    }

    /// <summary>
    /// How many captures in the window recorded any reportable session at all, and how many recorded none.
    /// <para>The honest-empty denominator, and this surface needs it more than most. <c>pg_session_states</c>
    /// is an EXCEPTION table like <c>pg_blocking_edges</c> and unlike <c>pg_index_usage_stats</c>: the
    /// collector writes nothing at all when no session had a transaction open past the floor, so an absent
    /// capture and a capture that found a perfectly healthy instance are byte-identical in the stored rows.
    /// <c>collection_log</c> records a SUCCESS with zero rows, so the two really are distinguishable — but
    /// only by looking there, which is what this does.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgSessionStatesCaptureCountsSql = """
        SELECT
            count(*) FILTER (WHERE l.rows_collected > 0)            AS captures_with_sessions,
            count(*)                                                AS captures_total,
            min(l.collection_time)                                  AS first_capture_at,
            max(l.collection_time)                                  AS last_capture_at
        FROM collection_log AS l
        WHERE l.server_id = $1
        AND   l.collector_name = 'pg_session_states'
        AND   l.status = 'SUCCESS'
        AND   l.collection_time >= $2
        AND   l.collection_time <= $3
        """;

    public sealed record PgSessionStatesCaptureCounts(
        long CapturesWithSessions,
        long CapturesTotal,
        DateTime? FirstCaptureAt,
        DateTime? LastCaptureAt);

    public sealed record LongRunningSessionRow(
        long BackendId,
        int Pid,
        string? DatabaseName,
        string? Username,
        string? ApplicationName,
        string? CommandTag,
        long QueryDurationMs);

    /// <summary>
    /// The single most recent <c>pg_session_states</c> capture for this server, filtered to sessions whose
    /// CURRENT query has been running at least <paramref name="thresholdMs"/> — the Postgres read behind the
    /// #2711 Long-Running Query alert.
    ///
    /// <para><b>Why the single latest capture, not a window rollup.</b> <see cref="GetPgSessionStatesAsync"/>
    /// above answers "what happened in this window" with peaks-per-backend, which is right for the Viewer's
    /// own display but wrong for "is a long-running query happening RIGHT NOW": a session whose peak
    /// <c>query_duration_ms</c> crossed the threshold ten minutes ago and has since finished must not still
    /// read as active. Restricting to rows sharing the single most recent <c>collection_time</c> is what makes
    /// this a live-state check rather than a history search — a still-running session reappears in EVERY
    /// cycle with a monotonically growing duration, so it is present in that latest capture by construction;
    /// a finished one is not.</para>
    ///
    /// <para><b><paramref name="recencyMinutes"/> exists because this table is an EXCEPTION table</b> (see the
    /// collector's own doc comment) — a healthy instance with nothing over the collector's own 30s/10s floors
    /// produces ZERO rows for a cycle, which is correct and must not be confused with "the collector stopped
    /// running". Bounding "most recent" to a real window (rather than an unqualified <c>MAX(collection_time)</c>
    /// that could resolve to a capture from hours ago) is what keeps an honest empty from silently going stale.
    /// 15 minutes, matching the fleet's own <c>OfflineThreshold</c> staleness convention
    /// (<see cref="PerformanceMonitor.Common.ServerHealthBands"/>) rather than a tight multiple of the
    /// collector's 1-minute configured cadence — this fleet's delivered sweep cadence has been measured
    /// running well behind its configured interval under load, and a recency bound tighter than the fleet's
    /// own staleness definition would make the alert silently never fire on exactly the servers busy enough to
    /// need it.</para>
    ///
    /// <para>No query text: this collector deliberately stores none (see its class remarks) to avoid
    /// accumulating literal parameter values for a condition an ordinary application can cross, so the alert
    /// this backs identifies a session by pid/database/command tag rather than a statement preview.</para>
    ///
    /// <para><b>Excludes idle-in-transaction sessions.</b> Per PostgreSQL's own semantics for
    /// <c>pg_stat_activity.query_start</c> ("time when the currently active query was started, OR IF STATE IS
    /// NOT ACTIVE, when the last query was started"), <c>query_duration_ms</c> for a session sitting
    /// <c>idle in transaction</c> measures how long ago its LAST query started, not how long a query has
    /// actually been running — that query already finished. Without this filter a session that ran a 5ms
    /// UPDATE and has sat idle-in-transaction for 40 minutes since (the common "forgot to COMMIT" shape) would
    /// read identically to one whose UPDATE has genuinely been executing for 40 minutes, and the two are
    /// different incidents needing different fixes (an app connection-pool bug vs. a slow statement). This
    /// matches the SQL Server equivalent (<c>AlertEngine.CheckLongRunningQueriesAsync</c>), which reads
    /// <c>sys.dm_exec_requests</c> — a table of requests actually executing, where an idle session has no
    /// row at all.</para>
    ///
    /// <para>$1 server_id, $2 threshold (ms), $3 recency floor (naive UTC), $4 row limit.</para>
    /// </summary>
    public const string CurrentLongRunningSessionsSql = """
        WITH recent AS (
            SELECT max(collection_time) AS latest_capture
            FROM pg_session_states
            WHERE server_id = $1
            AND   collection_time >= $3
        )
        SELECT
            s.backend_id,
            s.pid,
            s.database_name,
            s.username,
            s.application_name,
            s.command_tag,
            s.query_duration_ms
        FROM pg_session_states AS s
        JOIN recent AS r
          ON  s.collection_time = r.latest_capture
        WHERE s.server_id = $1
        AND   s.query_duration_ms >= $2
        AND   s.is_idle_in_transaction = false
        ORDER BY s.query_duration_ms DESC, s.pid
        LIMIT $4
        """;

    public static async Task<List<LongRunningSessionRow>> GetCurrentLongRunningSessionsAsync(
        NpgsqlDataSource postgres, int serverId, long thresholdMs, DateTime nowUtc, int recencyMinutes, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<LongRunningSessionRow>();
        await using var command = postgres.CreateCommand(CurrentLongRunningSessionsSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(thresholdMs);
        /* SpecifyKind(Unspecified) at the BIND — see GetPgSessionStatesAsync's identical comment for why
           Kind=Utc would silently shift this comparison against the naive columns it is measured against. */
        command.Parameters.AddWithValue(
            DateTime.SpecifyKind(nowUtc.AddMinutes(-recencyMinutes), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new LongRunningSessionRow(
                reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? -1 : reader.GetInt64(6)));
        }

        return rows;
    }

    public static async Task<PgSessionStatesCaptureCounts> GetPgSessionStatesCaptureCountsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var command = postgres.CreateCommand(PgSessionStatesCaptureCountsSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new PgSessionStatesCaptureCounts(
                reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3));
        }

        return new PgSessionStatesCaptureCounts(0, 0, null, null);
    }
}
