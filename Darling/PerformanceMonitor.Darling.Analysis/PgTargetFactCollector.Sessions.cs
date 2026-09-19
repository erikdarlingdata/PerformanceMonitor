/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The window's session picture from <c>pg_session_states</c>, in one read: the PEAK capture (the one whose
    /// denormalised <c>total_sessions</c> was highest; ties to the newest), the NEWEST capture, and the window's
    /// shape — how many captures stored rows and how many of those rows were <c>state_is_redacted</c>. <c>$1</c>
    /// server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    ///
    /// <para><b>Why the denormalised totals and not a count of rows.</b> <c>pg_session_states</c> is an EXCEPTION
    /// table (the collector's own remarks; <c>DarlingPgSessionStatesReader.PgSessionStatesCaptureCountsSql</c>'s
    /// doc): a capture stores one row per session that had a transaction open past the collector's floors, capped
    /// at a hundred, and the instance-wide counts — <c>total_sessions</c>, <c>active_sessions</c>,
    /// <c>idle_in_transaction_sessions</c> — repeat on every stored row precisely so that any one row answers
    /// "out of how many" (the V86 design comment). <c>MAX()</c> per <c>collection_time</c> is therefore a pick,
    /// not an aggregate: every row of a capture carries the same four integers. Counting the stored rows would
    /// report the reportable subset, never the pool.</para>
    ///
    /// <para><b>What the exception shape costs this family, stated rather than hidden.</b> A capture in which no
    /// session was over a floor stores NOTHING, so a quiet minute is absent from this series and the peak is the
    /// peak over the captures that had something to report. On a pool filled by short OLTP sessions with nothing
    /// idle past ten seconds or open past thirty, the collector may store no capture at all and this family says
    /// nothing — <c>captures_with_rows</c> rides the fact so the advice can say over how many captures the peak
    /// was seen. A rung adding <c>numbackends</c> to <c>pg_database_stats</c> (the universal one-minute series)
    /// would give the ratio a numerator every minute; that is a schema decision, not this lane's.</para>
    ///
    /// <para><b><c>total_sessions</c> counts every backend <c>pg_stat_activity</c> reports</b>, PostgreSQL's own
    /// background processes included (checkpointer, walwriter, background writer, autovacuum launcher and
    /// workers, logical replication launcher — a handful on a stock build), minus the collector's own session and
    /// minus parallel workers. The collector could not filter to <c>client backend</c> because <c>backend_type</c>
    /// is a privileged column that redacts to NULL without <c>pg_read_all_stats</c>. Background processes do
    /// NOT consume <c>max_connections</c> slots, so the ratio this read feeds runs a few points HIGH of the true
    /// client share — the safe direction for a cliff detector (a false alarm at the margin, never a missed
    /// refusal) — and the advice says so beside the number.</para>
    ///
    /// <para><b>The redacted share is over ROWS, not captures</b>, because redaction is per-backend and
    /// ownership-based (measured, V86: the unprivileged login sees its own backends whole and every other
    /// role's blank), so a login without the grant redacts essentially every stored row and one with it none;
    /// a per-capture vote would say the same thing with a coarser denominator. The <c>coalesce(…, false)</c> is
    /// the reader's own NULL rule for a boolean flag that a pre-flag row cannot carry.</para>
    ///
    /// <para>Every <c>FROM</c> / <c>JOIN</c> names the collector table or a CTE; <c>collection_log</c> — which
    /// the reader's capture-count SQL uses for the honest-empty denominator — is not an analysis table and is
    /// not read here.</para>
    /// </summary>
    public const string PgTargetSessionPeakSql = @"
WITH captures AS (
    SELECT
        collection_time,
        MAX(total_sessions)                                        AS total_sessions,
        MAX(active_sessions)                                       AS active_sessions,
        MAX(idle_in_transaction_sessions)                          AS idle_in_transaction_sessions,
        COUNT(*)                                                   AS rows_stored,
        COUNT(*) FILTER (WHERE coalesce(state_is_redacted, false)) AS rows_redacted
    FROM pg_session_states
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY collection_time
),
peak AS (
    SELECT collection_time, total_sessions, active_sessions, idle_in_transaction_sessions
    FROM captures
    ORDER BY total_sessions DESC NULLS LAST, collection_time DESC
    LIMIT 1
),
latest AS (
    SELECT collection_time, total_sessions, active_sessions, idle_in_transaction_sessions
    FROM captures
    ORDER BY collection_time DESC
    LIMIT 1
),
window_shape AS (
    SELECT
        COUNT(*)                        AS captures_with_rows,
        CAST(SUM(rows_stored) AS bigint)   AS rows_stored,
        CAST(SUM(rows_redacted) AS bigint) AS rows_redacted
    FROM captures
)
SELECT
    p.total_sessions                AS peak_total_sessions,
    p.active_sessions               AS peak_active_sessions,
    p.idle_in_transaction_sessions  AS peak_idle_in_transaction_sessions,
    p.collection_time               AS peak_at,
    l.total_sessions                AS latest_total_sessions,
    l.active_sessions               AS latest_active_sessions,
    l.idle_in_transaction_sessions  AS latest_idle_in_transaction_sessions,
    l.collection_time               AS latest_at,
    w.captures_with_rows,
    w.rows_stored,
    w.rows_redacted
FROM window_shape AS w
CROSS JOIN peak AS p
CROSS JOIN latest AS l";

    /// <summary>
    /// The window's idle-in-transaction HOLDERS from <c>pg_session_states</c> (v2 — #3691 lane 14, design §3.10):
    /// every stored row that was <c>is_idle_in_transaction</c> with <c>xact_duration_ms</c> at or over the floor
    /// <c>$4</c> (the scorer's WARNING bar, passed rather than repeated so the SQL and the constant cannot drift),
    /// grouped by HOLDER IDENTITY — <c>application_name</c> + <c>username</c> + <c>database_name</c> — with how many
    /// captures each identity was seen over the floor in, its longest transaction, the largest <c>horizon_age</c> it
    /// carried and when it was last seen; the window's shape (captures with a holder, holder rows, the most holders
    /// in one capture) and the peak capture ride every row. Longest holder first; at most 25 identities. <c>$1</c>
    /// server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    ///
    /// <para><b>Identity, not pid.</b> The V86 table stores no query text by design, and a pid is one backend's
    /// lifetime — the chronic shape this fact exists for is a CODE PATH that parks a transaction every time it
    /// runs, from the same application, as the same role, in the same database, on a fresh connection each time.
    /// Grouping by the three names is what makes "seen in 6 of 48 captures" mean recurrence rather than one
    /// session's six sightings; <c>COUNT(DISTINCT collection_time)</c> is the recurrence, so three parked sessions
    /// in one capture count once. Each name is <c>coalesce</c>d to the empty string so a NULL <c>application_name</c>
    /// (a client that set none) groups as one identity rather than never grouping at all.</para>
    ///
    /// <para><b>Redacted rows are excluded by the flag, not by accident.</b> Under redaction <c>state</c> is NULL and
    /// <c>is_idle_in_transaction</c> is not set, so such rows could not cross the floor anyway; the explicit
    /// <c>NOT coalesce(state_is_redacted, false)</c> says so, and the caller does not run this read at all when the
    /// window's rows are majority-redacted — that window stamps the permissions advisory instead of reading zero
    /// holders off blank rows.</para>
    ///
    /// <para><b><c>horizon_age</c> keeps the collector's <c>-1</c> sentinel</b> (pins nothing — a READ COMMITTED
    /// reader, an UPDATE that matched no rows, V86) through <c>coalesce(…, -1)</c> and <c>MAX()</c>: an identity
    /// whose every sighting pinned nothing reads <c>-1</c>, one that ever pinned reads the largest age it pinned.
    /// The scorer escalates on <c>&gt; 0</c> only.</para>
    ///
    /// <para>Every <c>FROM</c> / <c>JOIN</c> names the collector table or a CTE.</para>
    /// </summary>
    public const string PgTargetIdleInTransactionSql = @"
WITH holders AS (
    SELECT
        collection_time,
        coalesce(application_name, '')     AS application_name,
        coalesce(username, '')             AS username,
        coalesce(database_name, '')        AS database_name,
        xact_duration_ms,
        coalesce(horizon_age, -1)          AS horizon_age,
        coalesce(is_horizon_holder, false) AS is_horizon_holder
    FROM pg_session_states
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   coalesce(is_idle_in_transaction, false)
    AND   NOT coalesce(state_is_redacted, false)
    AND   xact_duration_ms >= $4
),
per_capture AS (
    SELECT
        collection_time,
        COUNT(*)              AS holders_in_capture,
        MAX(xact_duration_ms) AS max_xact_duration_ms
    FROM holders
    GROUP BY collection_time
),
window_shape AS (
    SELECT
        COUNT(*)                                              AS captures_with_holders,
        CAST(coalesce(SUM(holders_in_capture), 0) AS bigint)  AS holder_rows,
        coalesce(MAX(holders_in_capture), 0)                  AS peak_concurrent_holders
    FROM per_capture
),
peak_capture AS (
    SELECT collection_time, holders_in_capture
    FROM per_capture
    ORDER BY holders_in_capture DESC, max_xact_duration_ms DESC, collection_time DESC
    LIMIT 1
),
identities AS (
    SELECT
        application_name,
        username,
        database_name,
        COUNT(DISTINCT collection_time) AS captures_seen,
        MAX(xact_duration_ms)           AS max_xact_duration_ms,
        MAX(horizon_age)                AS max_horizon_age,
        bool_or(is_horizon_holder)      AS is_horizon_holder,
        MAX(collection_time)            AS last_seen_at
    FROM holders
    GROUP BY application_name, username, database_name
)
SELECT
    i.application_name,
    i.username,
    i.database_name,
    i.captures_seen,
    i.max_xact_duration_ms,
    i.max_horizon_age,
    i.is_horizon_holder,
    i.last_seen_at,
    w.captures_with_holders,
    w.holder_rows,
    w.peak_concurrent_holders,
    p.collection_time      AS peak_at,
    p.holders_in_capture   AS peak_holders
FROM identities AS i
CROSS JOIN window_shape AS w
CROSS JOIN peak_capture AS p
ORDER BY i.max_xact_duration_ms DESC, i.captures_seen DESC, i.application_name, i.username, i.database_name
LIMIT 25";

    /// <summary>
    /// <c>PG_CONNECTION_SATURATION</c> from <c>pg_session_states</c>' denormalised totals over the ceiling lane 2's
    /// config family emitted a moment ago, or <c>PG_MONITORING_PERMISSIONS</c> when the <c>state_is_redacted</c>
    /// share says the monitoring login could not see session state (filled by lane 3 — #3542 step 3, design
    /// §3.6); and, since lane 14 of #3691, <c>PG_IDLE_IN_TRANSACTION</c> from the same captures' rows over the
    /// duration floor (<see cref="PgTargetIdleInTransactionSql"/>, design §3.10). Two reads on one connection
    /// (<see cref="PgTargetSessionPeakSql"/> first — it decides whether the second may be trusted), at most two
    /// facts: the idle fact and EITHER the saturation ratio or the permissions advisory, never both of those.
    ///
    /// <para><b>The idle-in-transaction fact does not need the ceiling.</b> A duration is graded on its own bar, so
    /// the second read runs after the redaction gate and BEFORE the ceiling lookup, and a window with no config
    /// snapshot (no saturation fact) still states its parked transactions. Under a redacted majority the read is
    /// skipped — <c>state</c> and the duration columns are NULL for every backend the login does not own, so
    /// "no row over the floor" would be blindness read as an all-clear — and the permissions advisory carries
    /// <see cref="PgTargetScorer.IdleInTransactionUnobservableKey"/> <c>= 1</c> so a reader of
    /// <c>get_analysis_facts</c> sees WHY this family said nothing about parked transactions.</para>
    ///
    /// <para><b>The ceiling is composed here, at collect time, from the in-memory fact list</b> —
    /// <c>facts.Find(CONFIG_PG_MAX_CONNECTIONS)</c> and <c>facts.Find(CONFIG_PG_SUPERUSER_RESERVED)</c>, the two
    /// context facts <c>PgTargetFactCollector.Config.cs</c> emits at base 0 for exactly this reader (emission
    /// order: Config before Sessions, the same seam Buffer and Write use for <c>shared_buffers</c> and
    /// <c>max_wal_size</c>). This family never reads <c>pg_server_config</c> itself: the plan's resolution of the
    /// two lanes' file overlap is that step 3 depends on step 2 through the KEYS alone, and the base-severity seam
    /// (<c>ScoreSessionsFact(Fact)</c>) receives one fact, not a lookup, so the ratio must be on the fact before
    /// scoring. The three numbers ride the fact as metadata so the advice states them and a reader of
    /// <c>get_analysis_facts</c> can redo the division.</para>
    ///
    /// <para><b>usable = max_connections − superuser_reserved_connections − reserved_connections</b>, engine-defined:
    /// PostgreSQL refuses the connection that would take the last <c>superuser_reserved_connections</c> slots
    /// unless the role is a superuser, so an ordinary application sees the cliff there, not at
    /// <c>max_connections</c>. PostgreSQL 16 added <c>reserved_connections</c> (slots held for members of
    /// <c>pg_use_reserved_connections</c>, default 0) as a second carve-out ahead of the superuser one, and an
    /// ordinary role hits that cliff first; the snapshot name list carries it since the between-waves pass, so
    /// the third context fact is subtracted when present and 0 when absent (a pre-16 target has no such setting,
    /// which is the same arithmetic as the 16+ default).</para>
    ///
    /// <para><b>Precedence: redaction first.</b> A majority-redacted window emits the permissions advisory and
    /// returns — no saturation fact, no ratio — because under redaction the state columns and the collector's own
    /// duration floors are NULL for every backend the login does not own, so which captures stored rows and what
    /// they say about the pool is the login's blindness, not the server's state (the V86 comment's measured
    /// case: four idle-in-transaction sessions under the privileged role, zero of the same nine backends under
    /// the unprivileged one). <c>total_sessions</c> is a bare <c>count(*)</c> and survives redaction, so the
    /// advisory still states the peak count it saw — as a count, never as a share of the ceiling.</para>
    ///
    /// <para><b>A window with no ceiling emits nothing.</b> No config snapshot at or before the window's end, or
    /// a snapshot whose two rows did not normalise, means there is no denominator; a fact whose value could not
    /// be the ratio would change what <see cref="Fact.Value"/> means from one pass to the next. Logged at debug
    /// with the peak it would have graded, and the config family's own coverage describes the missing
    /// snapshot.</para>
    /// </summary>
    private async partial Task CollectSessionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        /* Not a rate — nothing here divides by observed time — but an unobserved window has no peak worth
           stating either, and the pass is "unavailable" on the coverage witness regardless. */
        if (context.ObservedDurationMs <= 0)
            return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetSessionPeakSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            long peakTotal, peakActive, peakIdleInTransaction, latestTotal, latestActive, latestIdleInTransaction, capturesWithRows, rowsStored, rowsRedacted;
            DateTime? peakAt, latestAt;
            /* The reader is closed before the second read runs — Npgsql allows one open reader per connection. */
            using (var reader = await cmd.ExecuteReaderAsync(context.CancellationToken))
            {
                /* The CROSS JOIN against the two LIMIT 1 CTEs yields no row at all when the window stored no capture —
                   the exception table's honest empty, and this family's. */
                if (!await reader.ReadAsync(context.CancellationToken))
                    return;

                peakTotal = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
                peakActive = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
                peakIdleInTransaction = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
                peakAt = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
                latestTotal = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
                latestActive = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
                latestIdleInTransaction = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
                latestAt = reader.IsDBNull(7) ? (DateTime?)null : reader.GetDateTime(7);
                capturesWithRows = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
                rowsStored = reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9));
                rowsRedacted = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));
            }

            if (rowsStored <= 0 || peakTotal <= 0)
                return;

            var windowEnd = AsNaive(context.TimeRangeEnd);
            var redactedShare = rowsRedacted / (double)rowsStored;

            /* ── Redaction first: the login's blindness is the finding, and no ratio is built on blank rows. ── */
            if (redactedShare >= PgTargetScorer.RedactedShareMajority)
            {
                var advisory = new Fact
                {
                    Source = PgTargetSources.SessionsSource,
                    Key = PgTargetFactKeys.MonitoringPermissions,
                    Value = redactedShare,
                    ServerId = context.ServerId,
                    Metadata =
                    {
                        ["rows_redacted_share"] = redactedShare,
                        ["rows_redacted"] = rowsRedacted,
                        ["rows_stored"] = rowsStored,
                        ["captures_with_rows"] = capturesWithRows,
                        /* A count, not a share of anything: count(*) is not a privileged read. */
                        ["peak_total_sessions"] = peakTotal,
                        /* The idle-in-transaction read is NOT run on blank rows: the family's silence on parked
                           transactions this pass is the login's, and the stamp says so instead of a false zero. */
                        [PgTargetScorer.IdleInTransactionUnobservableKey] = 1,
                    },
                };
                if (peakAt is { } redactedPeakAt)
                    advisory.Metadata["peak_age_s"] = Math.Max(0, (windowEnd - AsNaive(redactedPeakAt)).TotalSeconds);
                facts.Add(advisory);
                return;
            }

            /* ── The parked transactions, on the same connection: graded on duration alone, so they need no ceiling
               and are stated before the ceiling lookup can return early. ── */
            await ReadIdleInTransactionAsync(connection, context, facts, capturesWithRows, redactedShare, windowEnd);

            /* ── The ceiling, off lane 2's context facts (emission order: Config before Sessions). The third is
               PostgreSQL 16+'s reserved_connections (pg_use_reserved_connections); absent on a pre-16 snapshot and
               then 0, so the two mandatory facts alone still make a ceiling. ── */
            var maxConnections = facts.Find(f => f.Key == PgTargetFactKeys.ConfigMaxConnections);
            var reserved = facts.Find(f => f.Key == PgTargetFactKeys.ConfigSuperuserReserved);
            var reservedForRole = facts.Find(f => f.Key == PgTargetFactKeys.ConfigReservedConnections)?.Value ?? 0;
            if (maxConnections is null || reserved is null)
            {
                _logger?.LogDebug(
                    "[PgTargetFactCollector] CollectSessionFactsAsync on server {ServerId} ({ServerName}) saw a peak of {PeakSessions} sessions over {Captures} captures but no max_connections / superuser_reserved_connections context fact to grade it against (no pg_server_config snapshot at or before the window's end); no saturation fact this pass.",
                    context.ServerId, context.ServerName, peakTotal, capturesWithRows);
                return;
            }

            var usable = maxConnections.Value - reserved.Value - reservedForRole;
            /* PostgreSQL refuses to start with superuser_reserved_connections + reserved_connections >= max_connections,
               so a non-positive ceiling is a snapshot that does not describe a running server; no claim. */
            if (usable <= 0)
                return;

            var ratio = peakTotal / usable;
            var fact = new Fact
            {
                Source = PgTargetSources.SessionsSource,
                Key = PgTargetFactKeys.ConnectionSaturation,
                Value = ratio,
                ServerId = context.ServerId,
                Metadata =
                {
                    ["saturation_ratio"] = ratio,
                    ["peak_total_sessions"] = peakTotal,
                    ["peak_active_sessions"] = peakActive,
                    ["peak_idle_in_transaction_sessions"] = peakIdleInTransaction,
                    /* Neither active nor idle-in-transaction: idle client sessions AND PostgreSQL's own background
                       processes, which this series cannot tell apart (backend_type redacts). */
                    ["peak_other_sessions"] = Math.Max(0, peakTotal - peakActive - peakIdleInTransaction),
                    ["peak_idle_in_transaction_share"] = peakIdleInTransaction / (double)peakTotal,
                    ["latest_total_sessions"] = latestTotal,
                    ["latest_active_sessions"] = latestActive,
                    ["latest_idle_in_transaction_sessions"] = latestIdleInTransaction,
                    ["max_connections"] = maxConnections.Value,
                    ["superuser_reserved_connections"] = reserved.Value,
                    ["reserved_connections"] = reservedForRole,
                    ["usable_connections"] = usable,
                    ["max_connections_pending_restart"] = maxConnections.Metadata.GetValueOrDefault("pending_restart"),
                    ["config_snapshot_age_s"] = maxConnections.Metadata.GetValueOrDefault("snapshot_age_s"),
                    ["captures_with_rows"] = capturesWithRows,
                    ["rows_redacted_share"] = redactedShare,
                },
            };
            if (peakAt is { } at)
                fact.Metadata["peak_age_s"] = Math.Max(0, (windowEnd - AsNaive(at)).TotalSeconds);
            if (latestAt is { } lastAt)
                fact.Metadata["latest_age_s"] = Math.Max(0, (windowEnd - AsNaive(lastAt)).TotalSeconds);

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_session_states arrived in V86; a pre-migration store raises 42P01 here, which the reporter
               classifies quiet. Degrades to "no facts" so one unavailable input cannot cost this server its
               other facts, and WHY is reported, not assumed (#2826). An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// <c>PG_IDLE_IN_TRANSACTION</c> from <see cref="PgTargetIdleInTransactionSql"/>: no rows over the floor, no fact
    /// (the exception table's honest empty — and, under the floor the scorer's WARNING bar sets, an absence that
    /// means "nothing parked past a minute", stated by the saturation card's state breakdown rather than by a
    /// zero-valued fact here). Otherwise ONE fact for the window, named for the LONGEST holder identity
    /// (<see cref="Fact.ObjectName"/> <c>application as role</c>, <see cref="Fact.DatabaseName"/> its database —
    /// the two string seams the doubles-only metadata cannot carry), with <see cref="Fact.Value"/> that holder's
    /// longest transaction in SECONDS and the rest as metadata: the milliseconds the scorer grades, the holder's
    /// horizon claim and recurrence, the most captures ANY identity recurred in (the amplifier's witness — the
    /// chronic path may not be the longest one), how many distinct identities were over the floor, the window's
    /// shape, and the peak capture (how many sessions were parked at once, and when).
    ///
    /// <para><b>Why one fact and not one per identity.</b> A finding row is keyed by fact key; the story that
    /// roots here is "parked transactions on this server", and the advice names the longest holder and says how
    /// many others there were. Per-identity facts would be N cards saying the same thing with a different
    /// application_name, muted one at a time. The 25-identity cap bounds the read; <c>holder_identities</c>
    /// states the count the read saw so a truncated list is visible as one.</para>
    ///
    /// <para>Runs inside the caller's <c>try</c>: the shared three-outcome degrade covers it, and an abandonment
    /// propagates. Ages are measured from the window's end the caller asked for — no clock of its own.</para>
    /// </summary>
    private async Task ReadIdleInTransactionAsync(
        NpgsqlConnection connection, AnalysisContext context, List<Fact> facts,
        long capturesWithRows, double redactedShare, DateTime windowEnd)
    {
        using var cmd = new NpgsqlCommand(PgTargetIdleInTransactionSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        /* The scorer's WARNING bar IS the read floor (measured, 2026-09-19 — see the constant): a row under it is
           not a holder this fact speaks of, and the constant is passed so the SQL cannot carry a second copy. */
        cmd.Parameters.AddWithValue((long)PgTargetScorer.IdleInTransactionWarningMs);

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken))
            return;

        /* Row 1 is the longest holder (ORDER BY max_xact_duration_ms DESC); the window shape and the peak
           capture repeat on every row, so they are read once, here. */
        var applicationName = reader.GetString(0);
        var username = reader.GetString(1);
        var databaseName = reader.GetString(2);
        var holderCapturesSeen = ToInt64(reader.GetValue(3));
        var holderMaxMs = ToInt64(reader.GetValue(4));
        var holderHorizonAge = reader.IsDBNull(5) ? -1L : ToInt64(reader.GetValue(5));
        var holderIsHorizonHolder = !reader.IsDBNull(6) && reader.GetBoolean(6);
        var holderLastSeenAt = reader.IsDBNull(7) ? (DateTime?)null : reader.GetDateTime(7);
        var capturesWithHolders = ToInt64(reader.GetValue(8));
        var holderRows = ToInt64(reader.GetValue(9));
        var peakConcurrentHolders = ToInt64(reader.GetValue(10));
        var peakAt = reader.IsDBNull(11) ? (DateTime?)null : reader.GetDateTime(11);

        if (holderMaxMs <= 0)
            return;

        /* Every identity: the recurrence witness is the MOST captures any one of them was seen in, and how many
           pinned the horizon at some sighting — the chronic path need not be the longest holder. */
        var identities = 1L;
        var recurringCaptures = holderCapturesSeen;
        var identitiesPinningHorizon = holderHorizonAge > 0 ? 1L : 0L;
        while (await reader.ReadAsync(context.CancellationToken))
        {
            identities++;
            recurringCaptures = Math.Max(recurringCaptures, ToInt64(reader.GetValue(3)));
            if (!reader.IsDBNull(5) && ToInt64(reader.GetValue(5)) > 0)
                identitiesPinningHorizon++;
        }

        var fact = new Fact
        {
            Source = PgTargetSources.SessionsSource,
            Key = PgTargetFactKeys.IdleInTransaction,
            Value = holderMaxMs / 1_000.0,
            ServerId = context.ServerId,
            DatabaseName = string.IsNullOrEmpty(databaseName) ? null : databaseName,
            /* The identity the advice names; application_name is what the client set and is operator-facing. */
            ObjectName = $"{(string.IsNullOrEmpty(applicationName) ? "(no application_name)" : applicationName)} as {(string.IsNullOrEmpty(username) ? "(unknown role)" : username)}",
            Metadata =
            {
                [PgTargetScorer.IdleInTransactionDurationMsKey] = holderMaxMs,
                [PgTargetScorer.IdleInTransactionHolderHorizonAgeKey] = holderHorizonAge,
                ["holder_is_horizon_holder"] = holderIsHorizonHolder ? 1 : 0,
                ["holder_captures_seen"] = holderCapturesSeen,
                [PgTargetScorer.IdleInTransactionRecurringCapturesKey] = recurringCaptures,
                ["holder_identities"] = identities,
                ["holder_identities_pinning_horizon"] = identitiesPinningHorizon,
                ["captures_with_holders"] = capturesWithHolders,
                ["captures_with_rows"] = capturesWithRows,
                ["holder_rows"] = holderRows,
                ["peak_concurrent_holders"] = peakConcurrentHolders,
                ["floor_ms"] = PgTargetScorer.IdleInTransactionWarningMs,
                ["rows_redacted_share"] = redactedShare,
            },
        };
        if (holderLastSeenAt is { } lastSeen)
            fact.Metadata["holder_last_seen_age_s"] = Math.Max(0, (windowEnd - AsNaive(lastSeen)).TotalSeconds);
        if (peakAt is { } at)
            fact.Metadata["peak_age_s"] = Math.Max(0, (windowEnd - AsNaive(at)).TotalSeconds);

        facts.Add(fact);
    }
}
