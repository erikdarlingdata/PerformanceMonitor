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
    /// <c>PG_CONNECTION_SATURATION</c> from <c>pg_session_states</c>' denormalised totals over the ceiling lane 2's
    /// config family emitted a moment ago, or <c>PG_MONITORING_PERMISSIONS</c> when the <c>state_is_redacted</c>
    /// share says the monitoring login could not see session state (filled by lane 3 — #3542 step 3, design
    /// §3.6). One read (<see cref="PgTargetSessionPeakSql"/>), at most one fact.
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
    /// <para><b>usable = max_connections − superuser_reserved_connections</b>, engine-defined: PostgreSQL refuses
    /// the connection that would take the last <c>superuser_reserved_connections</c> slots unless the role is a
    /// superuser, so an ordinary application sees the cliff there, not at <c>max_connections</c>. PostgreSQL 16
    /// added <c>reserved_connections</c> (slots for <c>pg_use_reserved_connections</c> members, default 0) as a
    /// second carve-out; lane 2's snapshot name list does not carry it yet, so on a target that set it this
    /// ceiling reads that many slots HIGH — noted, not compensated.</para>
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

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            /* The CROSS JOIN against the two LIMIT 1 CTEs yields no row at all when the window stored no capture —
               the exception table's honest empty, and this family's. */
            if (!await reader.ReadAsync(context.CancellationToken))
                return;

            var peakTotal = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var peakActive = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var peakIdleInTransaction = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var peakAt = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
            var latestTotal = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var latestActive = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var latestIdleInTransaction = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));
            var latestAt = reader.IsDBNull(7) ? (DateTime?)null : reader.GetDateTime(7);
            var capturesWithRows = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
            var rowsStored = reader.IsDBNull(9) ? 0L : ToInt64(reader.GetValue(9));
            var rowsRedacted = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));

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
                    },
                };
                if (peakAt is { } redactedPeakAt)
                    advisory.Metadata["peak_age_s"] = Math.Max(0, (windowEnd - AsNaive(redactedPeakAt)).TotalSeconds);
                facts.Add(advisory);
                return;
            }

            /* ── The ceiling, off lane 2's two context facts (emission order: Config before Sessions). ── */
            var maxConnections = facts.Find(f => f.Key == PgTargetFactKeys.ConfigMaxConnections);
            var reserved = facts.Find(f => f.Key == PgTargetFactKeys.ConfigSuperuserReserved);
            if (maxConnections is null || reserved is null)
            {
                _logger?.LogDebug(
                    "[PgTargetFactCollector] CollectSessionFactsAsync on server {ServerId} ({ServerName}) saw a peak of {PeakSessions} sessions over {Captures} captures but no max_connections / superuser_reserved_connections context fact to grade it against (no pg_server_config snapshot at or before the window's end); no saturation fact this pass.",
                    context.ServerId, context.ServerName, peakTotal, capturesWithRows);
                return;
            }

            var usable = maxConnections.Value - reserved.Value;
            /* PostgreSQL refuses to start with superuser_reserved_connections >= max_connections, so a
               non-positive ceiling is a snapshot that does not describe a running server; no claim. */
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
}
