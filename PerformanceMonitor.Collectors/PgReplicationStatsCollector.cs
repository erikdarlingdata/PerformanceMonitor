/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Connected standbys and how far behind each one is — <c>pg_stat_replication</c> (#2544, the replication
/// slice).
///
/// <para><b>Not the same as <see cref="PgReplicationSlotsCollector"/>.</b> A slot is a promise to RETAIN
/// WAL, and it exists whether or not anybody is connected — an abandoned slot is the classic way to fill a
/// disk with nothing attached to it. This is the live CONNECTION: who is streaming right now, in what state,
/// and how far behind. A server can have a slot with no standby, a standby with no slot, or both, and the
/// two collectors answer different halves of "is replication healthy".</para>
///
/// <para><b>BOTH the byte distance and the time lag are collected, because the time lag badly understates a
/// replay stall.</b> Measured twice against a real standby holding <c>pg_wal_replay_pause()</c>: 26 MB
/// behind while <c>replay_lag</c> read 209 ms, and 33.7 MB behind while it read 2.8 seconds. The lag columns
/// do grow — the earlier claim that they stop was wrong — but they measure the round-trip of the most
/// recently replayed record rather than the size of the backlog, so they scale with nothing a reader can
/// use. Two point-eight seconds sounds survivable; 33 MB of unapplied WAL is the actual state. The byte
/// distance is the proportionate measure and the one an alert should be built on.</para>
///
/// <para><b>The four distances localise the fault, which is the other reason not to keep just one.</b> In
/// that same stalled run <c>sent</c>, <c>write</c> and <c>flush</c> were all <b>zero</b> and only
/// <c>replay</c> was behind — so the WAL had been shipped, written and fsynced, and the problem was purely
/// apply. Meanwhile <c>state</c> still read <c>streaming</c>. Any single column would have missed
/// that.</para>
///
/// <para><b>Distance is measured from the primary's CURRENT WAL position</b>, not from <c>sent_lsn</c>.
/// Measuring replay against what was sent hides a sender that has itself fallen behind; against
/// <c>pg_current_wal_lsn()</c> the three columns decompose the total distance — how much has not been sent,
/// not been flushed, not been replayed — which is what separates a network problem from an apply
/// problem.</para>
///
/// <para><b>Zero rows on a standby is CORRECT, not a fault.</b> <c>pg_stat_replication</c> is the
/// primary-side view; a replica has no downstream of its own unless it is cascading. Measured: 0 rows on a
/// server whose <c>pg_is_in_recovery()</c> is true. The read has to say "this server is a replica" rather
/// than "replication is not happening".</para>
///
/// <para>Readable by <c>pg_monitor</c> with no extra grant — verified, including the client address and the
/// lag columns, which are restricted from ordinary roles.</para>
/// </summary>
public sealed class PgReplicationStatsCollector : PostgresCollectorDefinitionBase<PgReplicationStatsCollector.Row>
{
    public static PgReplicationStatsCollector Instance { get; } = new();

    private PgReplicationStatsCollector()
    {
    }

    /// <param name="State"><c>streaming</c> is healthy; <c>catchup</c>, <c>backup</c> and <c>startup</c> are
    /// transient; anything else persisting is a finding.</param>
    /// <param name="SyncState"><c>async</c>, <c>sync</c>, <c>quorum</c> or <c>potential</c>. A
    /// <c>sync</c> standby falling behind blocks commits on the primary, which makes its lag a
    /// completely different severity from an async one's.</param>
    /// <param name="ReplayBytesBehind">Distance from the primary's current WAL position to what this
    /// standby has REPLAYED. The measure that scales with the size of the backlog — build alerts on this
    /// one, not on the time lag. See the type header.</param>
    /// <param name="ReplayLagMs">The time equivalent. It does move when replay stalls, but it measures the
    /// round-trip of the most recently replayed record rather than the backlog, so it understates badly:
    /// 2.8 seconds reported against 33.7 MB of unapplied WAL, measured. Stored alongside the byte distance
    /// because the gap between the two is itself informative.</param>
    public readonly record struct Row(
        string? ApplicationName,
        string? ClientAddr,
        string? State,
        string? SyncState,
        int? SyncPriority,
        long? SentBytesBehind,
        long? WriteBytesBehind,
        long? FlushBytesBehind,
        long? ReplayBytesBehind,
        double? WriteLagMs,
        double? FlushLagMs,
        double? ReplayLagMs,
        DateTime? BackendStart);

    /* pg_current_wal_lsn() is the reference for every distance, not sent_lsn - see the type header.

       It is only callable on a PRIMARY: on a standby it raises 25P01 (recovery in progress), where the
       equivalent is pg_last_wal_receive_lsn(). That does not need guarding here because the whole view is
       empty on a standby, so the function is never reached with a row to compute - but a future change that
       made this run on a replica would fail on that call rather than returning an honest empty, which is why
       it is written down.

       The lag columns are INTERVALs, converted to milliseconds at the source. Storing an interval would
       force every consumer to parse it, and EXTRACT(EPOCH ...) is exact for the sub-second values these
       actually carry.

       client_addr is inet; cast to text so the store column is a plain string rather than depending on a
       provider mapping for a PostgreSQL-specific type. NULL when the standby connects over a Unix socket,
       which is a real configuration and not a missing value. */
    private const string QueryText = @"
SELECT
    r.application_name::text                                       AS application_name,
    host(r.client_addr)::text                                      AS client_addr,
    r.state::text                                                  AS state,
    r.sync_state::text                                             AS sync_state,
    r.sync_priority                                                AS sync_priority,
    pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), r.sent_lsn)::bigint   AS sent_bytes_behind,
    pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), r.write_lsn)::bigint  AS write_bytes_behind,
    pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), r.flush_lsn)::bigint  AS flush_bytes_behind,
    pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), r.replay_lsn)::bigint AS replay_bytes_behind,
    EXTRACT(EPOCH FROM r.write_lag) * 1000                         AS write_lag_ms,
    EXTRACT(EPOCH FROM r.flush_lag) * 1000                         AS flush_lag_ms,
    EXTRACT(EPOCH FROM r.replay_lag) * 1000                        AS replay_lag_ms,
    (r.backend_start AT TIME ZONE 'UTC')                           AS backend_start
FROM pg_catalog.pg_stat_replication AS r
/* Worst first: the standby furthest from the primary's current position is the one to look at, and replay
   distance is the end of the chain - a standby that has received everything and applied nothing is still
   useless for reads and still cannot be failed over to without waiting. */
ORDER BY replay_bytes_behind DESC NULLS LAST, r.application_name";

    public override string Name => "pg_replication_stats";

    public override string TargetTable => "pg_replication_stats";

    /// <summary>
    /// Every PostgreSQL target, INCLUDING standbys. A standby returns zero rows unless it is cascading to a
    /// downstream of its own — and a cascading replica's downstream is exactly as worth watching as a
    /// primary's. Gating this to primaries would hide that, and would also require knowing recovery state at
    /// dispatch time, which is a fact that changes on failover without the gate noticing.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("application_name", CollectorColumnType.Varchar),
        new CollectorColumn("client_addr", CollectorColumnType.Varchar),
        new CollectorColumn("state", CollectorColumnType.Varchar),
        /* A SYNC standby falling behind blocks commits on the primary. Same lag, entirely different
           severity from an async one's, so this column is not decoration. */
        new CollectorColumn("sync_state", CollectorColumnType.Varchar),
        new CollectorColumn("sync_priority", CollectorColumnType.Integer),
        /* Four distances rather than one, because they decompose the problem: not-yet-sent is the sender or
           the network, sent-but-not-flushed is the standby's disk, flushed-but-not-replayed is its apply. */
        new CollectorColumn("sent_bytes_behind", CollectorColumnType.BigInt),
        new CollectorColumn("write_bytes_behind", CollectorColumnType.BigInt),
        new CollectorColumn("flush_bytes_behind", CollectorColumnType.BigInt),
        new CollectorColumn("replay_bytes_behind", CollectorColumnType.BigInt),
        /* The TIME view of the same three stages. Kept alongside the bytes because they disagree when
           replay stalls, and the disagreement is itself the finding - see the type header. */
        new CollectorColumn("write_lag_ms", CollectorColumnType.Double),
        new CollectorColumn("flush_lag_ms", CollectorColumnType.Double),
        new CollectorColumn("replay_lag_ms", CollectorColumnType.Double),
        /* When this standby's connection began. A backend_start that keeps moving is a standby that keeps
           reconnecting, which reads as healthy in every other column. */
        new CollectorColumn("backend_start", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                ApplicationName: reader.IsDBNull(0) ? null : reader.GetString(0),
                ClientAddr: reader.IsDBNull(1) ? null : reader.GetString(1),
                State: reader.IsDBNull(2) ? null : reader.GetString(2),
                SyncState: reader.IsDBNull(3) ? null : reader.GetString(3),
                SyncPriority: reader.IsDBNull(4) ? null : reader.GetInt32(4),
                SentBytesBehind: Long(reader, 5),
                WriteBytesBehind: Long(reader, 6),
                FlushBytesBehind: Long(reader, 7),
                ReplayBytesBehind: Long(reader, 8),
                /* NULL and not 0. The lag columns are genuinely NULL until the standby has reported a
                   round-trip, and on a brand-new connection that is a real state - 0 would claim a measured
                   zero lag on a standby nobody has heard from yet. */
                WriteLagMs: Double(reader, 9),
                FlushLagMs: Double(reader, 10),
                ReplayLagMs: Double(reader, 11),
                BackendStart: reader.IsDBNull(12) ? null : reader.GetDateTime(12)));
        }

        return rows;

        static long? Long(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetInt64(ordinal);
        static double? Double(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? null : r.GetDouble(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Every column is instantaneous state; the history is what makes a standby that drifts
           away and catches up repeatedly distinguishable from one that is simply far behind now. */
        writer
            .Value(row.ApplicationName)
            .Value(row.ClientAddr)
            .Value(row.State)
            .Value(row.SyncState)
            .Value(row.SyncPriority)
            .Value(row.SentBytesBehind)
            .Value(row.WriteBytesBehind)
            .Value(row.FlushBytesBehind)
            .Value(row.ReplayBytesBehind)
            .Value(row.WriteLagMs)
            .Value(row.FlushLagMs)
            .Value(row.ReplayLagMs)
            .Value(row.BackendStart);
    }
}
