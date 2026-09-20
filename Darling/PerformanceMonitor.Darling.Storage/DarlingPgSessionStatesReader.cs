/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
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
    /// The caller passes the fleet's own <c>OfflineThreshold</c> staleness convention (30 minutes via the
    /// shared <c>CollectionStoppedMinutesDefault</c>, #2794)
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
    /// <para><b>The noise opt-outs, and which SQL Server sibling each mirrors (#3539).</b> SQL Server's
    /// <c>CheckLongRunningQueriesAsync</c> reads <c>sys.dm_exec_requests</c> through five switchable noise
    /// filters plus an unconditional <c>session_id &gt; 50</c>; this read had none, so <c>autovacuum</c> at
    /// minute 31, a nightly <c>pg_dump</c>, or a manual <c>VACUUM</c> on a large relation paged with a mute as
    /// the only remedy — and the mute is weaker here than on SQL Server, because this table stores no query
    /// text (see the collector) and so a mute rule cannot match a statement. What the row DOES carry is
    /// <c>backend_type</c>, <c>application_name</c> and the whitelisted <c>command_tag</c>, which are the
    /// three handles below.</para>
    /// <list type="bullet">
    /// <item><b>Non-client backends</b> (unconditional): <c>backend_type &lt;&gt; 'client backend'</c> —
    /// autovacuum workers, walsenders (streaming replication, <c>pg_basebackup</c>), logical replication
    /// workers, background workers. Mirrors SQL Server's unconditional <c>session_id &gt; 50</c>: a system
    /// process is not a query. NULL-safe in the INCLUDING direction — <c>backend_type</c> is in the
    /// privileged column set and comes back NULL without <c>pg_monitor</c>, and dropping every row on a
    /// redacted target would make the alert silently never fire exactly where the collector has already
    /// stamped <c>state_is_redacted</c>.</item>
    /// <item><b>Maintenance statements</b> (unconditional): <c>command_tag</c> in <c>VACUUM</c>,
    /// <c>ANALYZE</c>, <c>REINDEX</c>, <c>CLUSTER</c> — a manual vacuum of a large table runs for an hour by
    /// design, and the alert asks about QUERIES. SQL Server has no statement-shape sibling because it needs
    /// none: its row carries the text and an operator mutes <c>ALTER INDEX</c> by pattern; here the tag is
    /// the only handle, so the exclusion has to live in the read. <c>CREATE</c> is deliberately NOT in the
    /// list: the tag cannot tell <c>CREATE INDEX CONCURRENTLY</c> (maintenance) from <c>CREATE TABLE AS
    /// SELECT</c> (a query), and the honest side of that ambiguity is to report — the incident line shows
    /// the tag so a reader can see which it was.</item>
    /// <item><b>Dump and restore utilities</b> (the <c>{0}</c> placeholder, on the SHARED
    /// <c>longRunningQueryExcludeBackups</c> switch): <c>application_name</c> in <c>pg_dump</c>,
    /// <c>pg_dumpall</c>, <c>pg_restore</c>, <c>pg_basebackup</c> — the names libpq's
    /// <c>fallback_application_name</c> gives those tools, so a dump's <c>COPY ... TO STDOUT</c> sessions
    /// carry them without operator configuration. Mirrors <c>BackupsFilter</c> (<c>BACKUPTHREAD</c> /
    /// <c>BACKUPIO</c>) on the SAME knob, the way <c>longRunningQueryEnabled</c> and the threshold are already
    /// shared: "do not page me for backups" is one preference, not one per engine. <c>psql</c> is NOT
    /// excluded — an operator's ad-hoc statement running long is precisely a long-running query, and SQL
    /// Server does not exclude SSMS either.</item>
    /// <item><b>Idle in transaction</b> (unconditional, pre-existing): its own condition — see the
    /// paragraph above.</item>
    /// </list>
    /// <para>The other three SQL Server switches have no honest PostgreSQL reading and are not faked:
    /// <c>sp_server_diagnostics</c> and <c>XE_LIVE_TARGET_TVF</c> name SQL Server internals with no
    /// counterpart, and <c>WAITFOR</c>'s twin (<c>pg_sleep</c>) is invisible here because the row carries
    /// no text and <c>SELECT pg_sleep(...)</c> tags as <c>SELECT</c>. CDC's nearest relative — logical
    /// replication workers — is already out through <c>backend_type</c>. <c>excludedDatabases</c> is the
    /// <c>candidates</c> CTE's third flag (<c>{3}</c>, below), applied ahead of the cap like the knob.</para>
    ///
    /// <para><b>The opt-out knob (#3653 A5, Q5; #3743 for this twin).</b> <c>{1}</c> and <c>{2}</c> are the
    /// Long-Running Query knob's two arm expressions — the SHARED
    /// <c>LongRunningQueryExclusions.BuildSqlPredicates</c> text (the literal <c>FALSE</c> for an arm with no
    /// entries), built by the host over <see cref="ExclusionProgramNameColumn"/> and
    /// <see cref="ExclusionLoginNameColumn"/> with operands binding from <see cref="ExclusionFirstParameterOrdinal"/>
    /// onward, program prefixes first. <c>application_name</c> is the <c>program_name</c> twin and
    /// <c>username</c> (<c>pg_stat_activity.usename</c>) the <c>login_name</c> twin, so an entry means the same
    /// thing on both engines: prefix for programs, whole name for logins, case-insensitive, <c>%</c> / <c>_</c>
    /// escaped, a NULL column matching nothing. The builder lives in the alerting assembly, which this storage
    /// assembly does not (and should not) reference, so the host hands the rendered text and its operands
    /// down rather than the knob itself — the translation is still spelled once, in the builder.</para>
    ///
    /// <para><b>Why a <c>candidates</c> CTE with two flags rather than one more <c>AND NOT</c></b> — the SQL
    /// Server read's shape (<c>DarlingAlertReadAdapter.LongRunningQueriesSqlTemplate</c>), mirrored so the two
    /// reads agree row-for-row in meaning. The knob is applied AHEAD of <c>LIMIT $4</c>: the sessions it names
    /// are the longest-running on the server by construction (a permanent ETL or replication worker under a
    /// service role), so a post-read filter would let them fill the cap on every sweep and blind the alert.
    /// And the fire payload wants to know how many it removed and WHICH ARM did it — a setting whose only
    /// effect is a page not arriving is one an operator cannot verify — so every over-threshold row is
    /// flagged once per arm, the outer query keeps the unflagged rows under the cap, and the two counts are
    /// uncorrelated scalars over the same CTE: one statement, one capture, rows and counts describing the same
    /// instant. The login count is taken <c>AND NOT</c> the program flag so a session matching both arms counts
    /// once, under the program prefix, and the two counts sum to the sessions removed. The counts are
    /// <c>count(*)</c> rather than the SQL Server read's <c>COUNT(DISTINCT session_id)</c>: there a MARS session
    /// can hold two request rows, here <c>pg_stat_activity</c> has exactly one row per backend and the capture
    /// stores one row per backend, so the row IS the session — and <c>backend_id</c> / <c>pid</c> are nullable
    /// on a redacted target, where a DISTINCT over them would drop the very rows the receipt is for. The outer
    /// alias stays <c>s</c> like the inner's so the pins on this text keep reading.</para>
    ///
    /// <para><b>#3742: <c>excludedDatabases</c> is the third flag, for the same reason.</b> Until #3742 this
    /// read applied the shared database list in C#, AFTER <c>LIMIT $4</c> — the very shape the knob's paragraph
    /// above refuses — and its helper's doc called that "the same known shape on both engines". It was: the
    /// two SQL Server reads had it too, and #3772 fixed them first. On a server where an excluded reporting
    /// database's ETL held the five longest sessions, the page filled with rows the filter then threw away and
    /// the alert came back short or empty while un-excluded long-running sessions existed — the list meant to
    /// make the alert accurate had switched it off for every other database, silently. The list now rides the
    /// same CTE as <c>{3} AS excluded_by_database</c>, built by the SAME shared builder's five-argument overload
    /// over <see cref="ExclusionDatabaseNameColumn"/> (exact, case-insensitive — the rule the C# filter always
    /// applied — one <c>COALESCE(col, '') ILIKE $n ESCAPE '\'</c> term per entry, so a row with no database name
    /// is still kept), its operands bound AFTER the login operands so no ordinal that existed before this arm
    /// moves. The outer <c>WHERE</c> drops it with the knob's two, and its count is taken <c>AND NOT</c> both
    /// knob flags — the database arm is LAST, so a session the knob would have removed anyway is the knob's
    /// whichever database it ran in, and the three counts sum to the sessions removed (the builder's doc says
    /// why last and not first: the knob is the specific instrument whose card lists its entries; the database
    /// list is the blunt shared one the blocking and deadlock arms also honour).</para>
    ///
    /// <para>$1 server_id, $2 threshold (ms), $3 recency floor (naive UTC), $4 row limit, $5… the knob's
    /// operands then the database operands; <c>{0}</c> is the switchable filter block. A PROPERTY rather than a
    /// string field on purpose: the shipped-read parse census (<c>DarlingPgReadSqlParsesLiveTests</c>)
    /// parse-checks every static string field on a reader, and a template with a placeholder in it cannot
    /// parse. The two RENDERINGS below are the fields — both with the knob AND the database list EMPTY
    /// (<c>FALSE</c> arms), the only shape this assembly can render without the builder — so the texts that
    /// reach the store on an untouched knob are the ones parse-checked; the set shapes are parse-checked and
    /// executed by the alert tests' live class instead.</para>
    /// </summary>
    public static string CurrentLongRunningSessionsSqlTemplate => """
        WITH recent AS (
            SELECT max(collection_time) AS latest_capture
            FROM pg_session_states
            WHERE server_id = $1
            AND   collection_time >= $3
        ),
        candidates AS (
            SELECT
                s.backend_id,
                s.pid,
                s.database_name,
                s.username,
                s.application_name,
                s.command_tag,
                s.query_duration_ms,
                {1} AS excluded_by_program_prefix,
                {2} AS excluded_by_login,
                {3} AS excluded_by_database
            FROM pg_session_states AS s
            JOIN recent AS r
              ON  s.collection_time = r.latest_capture
            WHERE s.server_id = $1
            AND   s.query_duration_ms >= $2
            AND   s.is_idle_in_transaction = false
            AND   coalesce(s.backend_type, 'client backend') = 'client backend'
            AND   coalesce(s.command_tag, '') NOT IN ('VACUUM', 'ANALYZE', 'REINDEX', 'CLUSTER')
            {0}
        )
        SELECT
            s.backend_id,
            s.pid,
            s.database_name,
            s.username,
            s.application_name,
            s.command_tag,
            s.query_duration_ms,
            CAST((SELECT count(*) FROM candidates AS x WHERE x.excluded_by_program_prefix) AS integer) AS excluded_by_program_prefix_count,
            CAST((SELECT count(*) FROM candidates AS x WHERE x.excluded_by_login AND NOT x.excluded_by_program_prefix) AS integer) AS excluded_by_login_count,
            CAST((SELECT count(*) FROM candidates AS x WHERE x.excluded_by_database AND NOT (x.excluded_by_program_prefix OR x.excluded_by_login)) AS integer) AS excluded_by_database_count
        FROM candidates AS s
        WHERE NOT (s.excluded_by_program_prefix OR s.excluded_by_login OR s.excluded_by_database)
        ORDER BY s.query_duration_ms DESC, s.pid
        LIMIT $4
        """;

    /// <summary>The <c>program_name</c> twin the knob's prefix arm is built over — libpq's
    /// <c>application_name</c>, the same column the backups opt-out filters on. Qualified with the
    /// <c>candidates</c> CTE's inner alias because that is where the flags are computed.</summary>
    public const string ExclusionProgramNameColumn = "s.application_name";

    /// <summary>The <c>login_name</c> twin the knob's exact arm is built over — <c>pg_stat_activity.usename</c>,
    /// stored as <c>username</c> by the collector.</summary>
    public const string ExclusionLoginNameColumn = "s.username";

    /// <summary>The <c>database_name</c> column the shared <c>excludedDatabases</c> arm is built over (#3742) —
    /// the collector's copy of <c>pg_stat_activity.datname</c>, qualified with the <c>candidates</c> CTE's inner
    /// alias like the knob's two columns because that is where the flags are computed.</summary>
    public const string ExclusionDatabaseNameColumn = "s.database_name";

    /// <summary>The <c>$n</c> the knob's first operand binds as: after the four fixed parameters
    /// (<c>$1</c> server, <c>$2</c> threshold, <c>$3</c> recency floor, <c>$4</c> limit), program prefixes
    /// first, then logins, then (#3742) the excluded databases — the builder's order is the binding order.</summary>
    public const int ExclusionFirstParameterOrdinal = 5;

    /// <summary>The arm text an EMPTY arm renders as — the builder's own spelling of "no entries", the same for
    /// the knob's two arms and the database arm, spliced here so the two field renderings below (which cannot
    /// call the builder) are the very text the host sends on an untouched knob and an empty database list. Not
    /// a statement: a fragment, verified where it is spliced.</summary>
    public const string NoExclusionPredicate = "FALSE";

    /// <summary>The switchable dump/restore opt-out — the PostgreSQL reading of
    /// <c>longRunningQueryExcludeBackups</c>. A constant rather than inline so a test can pin the list and
    /// the read can be asserted to include it exactly when the switch is on.</summary>
    public const string BackupUtilitiesFilter =
        "AND   coalesce(s.application_name, '') NOT IN ('pg_dump', 'pg_dumpall', 'pg_restore', 'pg_basebackup')";

    /// <summary>The read as it runs with the backups opt-out ON, the knob EMPTY and no excluded databases —
    /// the shipped default before #3743's knob, and what the pre-#3539 constant name meant. Kept under the old
    /// name so pins on the shape keep pointing at the text that actually executes on an untouched store. A
    /// <c>static readonly</c> field, not a property, so the parse census sees it;
    /// <see cref="BackupUtilitiesFilter"/> is a <c>const</c>, so this initializer cannot read it before it
    /// exists.</summary>
    public static readonly string CurrentLongRunningSessionsSql = BuildCurrentLongRunningSessionsSql(excludeBackups: true);

    /// <summary>The read as it runs with the backups opt-out OFF, the knob EMPTY and no excluded databases —
    /// the other text that can reach the store without the builder, held as a field for the same parse-census
    /// reason. The host does not read this; it calls
    /// <see cref="BuildCurrentLongRunningSessionsSql(bool, string, string, string)"/> with the settings.</summary>
    public static readonly string CurrentLongRunningSessionsSqlBackupsIncluded = BuildCurrentLongRunningSessionsSql(excludeBackups: false);

    /// <summary>Renders <see cref="CurrentLongRunningSessionsSqlTemplate"/> for one setting of the shared
    /// backups switch with the opt-out knob EMPTY and no excluded databases (all three arms
    /// <see cref="NoExclusionPredicate"/>) — the pre-#3743 rows, byte-for-byte the text the host sends when
    /// both knob lists and the database list are empty.</summary>
    public static string BuildCurrentLongRunningSessionsSql(bool excludeBackups) =>
        BuildCurrentLongRunningSessionsSql(excludeBackups, NoExclusionPredicate, NoExclusionPredicate, NoExclusionPredicate);

    /// <summary>Renders <see cref="CurrentLongRunningSessionsSqlTemplate"/> for one setting of the shared
    /// backups switch and the three arm expressions (the shared builder's five-argument overload's
    /// <c>ProgramPrefixPredicate</c> / <c>LoginPredicate</c> / <c>DatabasePredicate</c>, built over
    /// <see cref="ExclusionProgramNameColumn"/>, <see cref="ExclusionLoginNameColumn"/> and
    /// <see cref="ExclusionDatabaseNameColumn"/> from <see cref="ExclusionFirstParameterOrdinal"/>). Public so
    /// the host's call and a test's pin are the same text. Four arguments and no three-argument sibling on
    /// purpose: a caller cannot forget the database arm and quietly ship a read that ignores the list.</summary>
    public static string BuildCurrentLongRunningSessionsSql(
        bool excludeBackups, string programPrefixPredicate, string loginPredicate, string databasePredicate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(programPrefixPredicate);
        ArgumentException.ThrowIfNullOrWhiteSpace(loginPredicate);
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePredicate);
        return CurrentLongRunningSessionsSqlTemplate
            .Replace("{0}", excludeBackups ? BackupUtilitiesFilter : "")
            .Replace("{1}", programPrefixPredicate)
            .Replace("{2}", loginPredicate)
            .Replace("{3}", databasePredicate);
    }

    /// <summary>
    /// What <see cref="GetCurrentLongRunningSessionsAsync"/> hands back since #3743: the sessions the alert
    /// evaluates (longest first, capped) and how many over-threshold sessions the read removed from the same
    /// capture ahead of the cap, split by the arm that removed them — the storage-side twin of the alerting
    /// assembly's <c>LongRunningQueryReadResult</c>, which this assembly cannot reference. The three counts sum
    /// to the sessions removed: a session matching both knob arms is counted under the program prefix only, and
    /// one the knob removed is the knob's whichever database it ran in (see the SQL's doc comment).
    /// </summary>
    /// <param name="Sessions">The evaluated sessions.</param>
    /// <param name="ExcludedByProgramPrefix">Over-threshold sessions in the capture whose <c>application_name</c>
    /// matched a configured prefix — including any that ALSO matched a login (counted once, here).</param>
    /// <param name="ExcludedByLogin">Over-threshold sessions whose <c>username</c> matched a configured login and
    /// whose <c>application_name</c> did NOT match a prefix.</param>
    /// <param name="ExcludedByDatabase">Over-threshold sessions whose <c>database_name</c> is on the shared
    /// <c>excludedDatabases</c> list and that NEITHER knob arm matched (#3742).</param>
    public sealed record LongRunningSessionsReadResult(
        List<LongRunningSessionRow> Sessions, int ExcludedByProgramPrefix, int ExcludedByLogin, int ExcludedByDatabase);

    /// <param name="excludeBackups">The shared <c>longRunningQueryExcludeBackups</c> switch — drops the
    /// dump/restore utilities' sessions (see the SQL's doc comment).</param>
    /// <param name="programPrefixPredicate">The knob's program arm as SQL — the shared builder's
    /// <c>ProgramPrefixPredicate</c> over <see cref="ExclusionProgramNameColumn"/>, or
    /// <see cref="NoExclusionPredicate"/> for an empty arm.</param>
    /// <param name="loginPredicate">The knob's login arm as SQL — the builder's <c>LoginPredicate</c> over
    /// <see cref="ExclusionLoginNameColumn"/>, or <see cref="NoExclusionPredicate"/>.</param>
    /// <param name="databasePredicate">The shared <c>excludedDatabases</c> list's arm as SQL (#3742) — the
    /// builder's five-argument overload's <c>DatabasePredicate</c> over <see cref="ExclusionDatabaseNameColumn"/>
    /// (exact, case-insensitive; a row with no database name is kept), or <see cref="NoExclusionPredicate"/>
    /// for an empty list. Applied IN the read, ahead of <c>LIMIT $4</c>, never after it.</param>
    /// <param name="exclusionOperands">The builder's operands in binding order (program prefixes, then logins,
    /// then databases), bound from <see cref="ExclusionFirstParameterOrdinal"/> onward. Empty when every arm
    /// is empty.</param>
    public static async Task<LongRunningSessionsReadResult> GetCurrentLongRunningSessionsAsync(
        NpgsqlDataSource postgres, int serverId, long thresholdMs, DateTime nowUtc, int recencyMinutes, int limit,
        bool excludeBackups,
        string programPrefixPredicate, string loginPredicate, string databasePredicate, IReadOnlyList<string> exclusionOperands,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(exclusionOperands);

        var rows = new List<LongRunningSessionRow>();
        var excludedByProgramPrefix = 0;
        var excludedByLogin = 0;
        var excludedByDatabase = 0;
        await using var command = postgres.CreateCommand(
            BuildCurrentLongRunningSessionsSql(excludeBackups, programPrefixPredicate, loginPredicate, databasePredicate));
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(thresholdMs);
        /* SpecifyKind(Unspecified) at the BIND — see GetPgSessionStatesAsync's identical comment for why
           Kind=Utc would silently shift this comparison against the naive columns it is measured against. */
        command.Parameters.AddWithValue(
            DateTime.SpecifyKind(nowUtc.AddMinutes(-recencyMinutes), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        /* #3743: the knob's operands follow the four fixed parameters, in the builder's order — which is the
           order the predicates number them from ExclusionFirstParameterOrdinal. #3742's database operands are
           in the same list, after the login operands, so the loop binds all three arms without knowing where
           one ends — the builder's one operand list in one binding order is the whole point. */
        foreach (var operand in exclusionOperands)
        {
            command.Parameters.AddWithValue(operand);
        }

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
            /* The same three scalars on every row — read once is enough; a read with no rows has nothing to
               fire and therefore nothing to render the counts beside (the SQL Server read's rule). */
            excludedByProgramPrefix = reader.IsDBNull(7) ? 0 : reader.GetInt32(7);
            excludedByLogin = reader.IsDBNull(8) ? 0 : reader.GetInt32(8);
            excludedByDatabase = reader.IsDBNull(9) ? 0 : reader.GetInt32(9);
        }

        /* #3742: no post-read filter. The rows ARE the page — every exclusion, the database list included,
           was applied inside the statement ahead of LIMIT $4, so a row that reaches here is one the alert
           evaluates. The C# FilterExcludedDatabases helper that used to run here (and could leave the page
           short or empty while matches existed) is gone, not disabled. */
        return new LongRunningSessionsReadResult(rows, excludedByProgramPrefix, excludedByLogin, excludedByDatabase);
    }

    public static async Task<PgSessionStatesCaptureCounts> GetPgSessionStatesCaptureCountsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var command = postgres.CreateCommand(PgSessionStatesCaptureCountsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
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
