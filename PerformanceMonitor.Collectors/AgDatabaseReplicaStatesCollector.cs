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
/// Availability Group DATABASE health — one row per database per replica, from
/// sys.dm_hadr_database_replica_states joined to sys.availability_replicas,
/// sys.availability_groups and sys.databases (#991). The database-grain half of the AG surface;
/// <see cref="AgReplicaStatesCollector"/> carries the replica grain.
///
/// <para>Metric surface follows Hannah Vernon's SqlServerAgMonitor (MIT),
/// https://github.com/HannahVernon/SqlServerAgMonitor.</para>
///
/// <para>The queue sizes and rates are INSTANTANEOUS GAUGES, not cumulative counters — the DMV
/// recomputes each from the current backlog, so no delta-framework involvement (this is the trap
/// that would otherwise make them look like <see cref="WaitStatsCollector"/>'s counters). Zero rows
/// is the NORMAL result on a server with no Availability Groups.</para>
///
/// <para>last_hardened_lsn / last_commit_lsn are <c>numeric(25, 0)</c> — far wider than BIGINT — so
/// they are converted to strings server-side and stored as text. Only last_commit_lsn is a real LSN;
/// last_hardened_lsn is a log-block id padded with zeroes, so neither is safe to do arithmetic on.
/// Deriving a byte distance between them is ANALYSIS, deliberately left to a reader.</para>
///
/// <para>THE RULE FOR EVERY COLUMN HERE, and the only safe one: while a replica is SUSPENDED, a reading
/// may RAISE an alarm but may never CLEAR one. That asymmetry holds under both the documented and the
/// measured behavior, so nothing downstream has to bet on which is true — which matters, because they
/// disagree.</para>
///
/// <para>secondary_lag_seconds is 2016+ and the repo floor IS 2016, so it is referenced directly
/// with no version branch. MS Learn documents it reading <c>0</c> while data movement is SUSPENDED
/// — DO NOT BUILD ON THAT SENTENCE, it is contradicted by measurement. On 16.0.4265.3 it reads 0
/// while movement is ACTIVE and caught up, and ACCRUES once suspended. What it reports on a
/// suspended row is roughly how stale the secondary's last hardened log is
/// (<c>now - last_hardened_time</c>), NOT time since suspension: under write load that starts near
/// zero and climbs (measured 0→15→31→46→62 s and 30→45→60→75 s on two loaded runs), but on an IDLE
/// group it starts at however long since the last write and can be thousands of seconds immediately.
/// It also does not latch the moment movement stops — a suspended row can still report 0 for the
/// first sample or two. So the magnitude is STALENESS, not volume at risk (log_send_queue_size would
/// be the volume measure, and it is NULL while suspended). is_suspended is collected alongside so
/// that reading is possible. Evidence: tools/ag-fixture/VALIDATION.md.</para>
///
/// <para>The rest of the SUSPENDED surface, all measured, all pointing the same way — STALE, NOT CURRENT:
/// log_send_queue_size goes NULL rather than growing; redo_queue_size FREEZES at its last value; and all
/// four *_time columns FREEZE at their last pre-suspension instant. So a commit-time delta computed across
/// replicas stops growing exactly when replication has stopped, understating the problem at the moment it
/// is worst — the opposite direction from secondary_lag_seconds, which is why the two must never be
/// averaged or cross-checked against each other without reading is_suspended first.</para>
///
/// <para>The drain estimates inherit that freeze and are the sharpest edge of it: est_redo_completion_time_min
/// is queue ÷ rate, and with BOTH frozen it holds a small, static, reassuring value (measured 0.0144 min
/// held flat across a 45 s suspension) when the honest answer is "never, movement is stopped".
/// est_send_drain_time_min at least reads NULL, because its queue goes NULL. Threshold the redo estimate
/// on its own and a suspended replica looks healthy; that is precisely the alarm a suspended row must
/// never be allowed to clear.</para>
///
/// <para>last_received_time read NULL in every sample on that fixture, healthy and suspended alike, so
/// treat it as optional rather than expected. last_commit_time and last_redone_time also sit still on an
/// IDLE database — they are "time of last commit", not a heartbeat — so <c>now - last_commit_time</c> is
/// not a lag measure: on a quiet, perfectly healthy replica it grows without bound.</para>
///
/// <para>GRAIN WARNING: on a SECONDARY, sys.dm_hadr_database_replica_states carries only the LOCAL
/// replica's rows, so this INNER JOIN narrows to a one-row self-view even though
/// sys.availability_replicas holds every replica. A complete AG picture requires collecting from the
/// PRIMARY; monitoring only a secondary yields that replica's own view and nothing about its peers.</para>
///
/// <para>synchronization_state_desc's values are space-separated (<c>NOT SYNCHRONIZING</c>), unlike
/// the replica-grain synchronization_health_desc's underscores (<c>NOT_HEALTHY</c>) — stored
/// verbatim as the DMV reports them.</para>
///
/// <para>The four *_time columns are the commit/hardened/redone/received timestamps the reference
/// project skips; they are what the canonical primary-vs-secondary commit-time lag math needs, and
/// unlike secondary_lag_seconds they are directly comparable across replicas. The two
/// est_*_time_min columns are drain-time ESTIMATES computed server-side: queue ÷ rate ÷ 60. Both
/// guard the two ways the raw expression misbehaves — <c>* 1.0</c> stops BIGINT ÷ BIGINT integer
/// division, and <c>NULLIF(rate, 0)</c> stops the divide-by-zero an idle or suspended replica would
/// otherwise raise. A NULL estimate therefore means "no drain rate" (idle, suspended, or caught up),
/// which is the honest answer; it is deliberately never coerced to 0, which would read as "drains
/// instantly" — the exact opposite of the truth.</para>
///
/// <para>PERMISSIONS: this query joins the same two AG catalog views, which require VIEW ANY
/// DEFINITION and hide rows rather than erroring without it — see
/// <see cref="AgReplicaStatesCollector"/> for the full trap and the fingerprint that identifies it.</para>
/// </summary>
public sealed class AgDatabaseReplicaStatesCollector : CollectorDefinitionBase<AgDatabaseReplicaStatesCollector.Row>
{
    public static AgDatabaseReplicaStatesCollector Instance { get; } = new();

    private AgDatabaseReplicaStatesCollector()
    {
    }

    public readonly record struct Row(
        string? AgName,
        string? DatabaseName,
        string? ReplicaServerName,
        bool? IsLocal,
        string? SynchronizationStateDesc,
        string? LastHardenedLsn,
        string? LastCommitLsn,
        long? LogSendQueueSize,
        long? RedoQueueSize,
        long? LogSendRate,
        long? RedoRate,
        bool? IsSuspended,
        string? SuspendReasonDesc,
        string? AvailabilityModeDesc,
        long? SecondaryLagSeconds,
        DateTime? LastCommitTime,
        DateTime? LastHardenedTime,
        DateTime? LastRedoneTime,
        DateTime? LastReceivedTime,
        double? EstRedoCompletionTimeMin,
        double? EstSendDrainTimeMin,
        string? GroupId);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    ag_name = ag.name,
    database_name = d.name,
    replica_server_name = ar.replica_server_name,
    is_local = hdrs.is_local,
    synchronization_state_desc = hdrs.synchronization_state_desc,
    last_hardened_lsn = CONVERT(nvarchar(40), hdrs.last_hardened_lsn),
    last_commit_lsn = CONVERT(nvarchar(40), hdrs.last_commit_lsn),
    log_send_queue_size = hdrs.log_send_queue_size,
    redo_queue_size = hdrs.redo_queue_size,
    log_send_rate = hdrs.log_send_rate,
    redo_rate = hdrs.redo_rate,
    is_suspended = hdrs.is_suspended,
    suspend_reason_desc = hdrs.suspend_reason_desc,
    availability_mode_desc = ar.availability_mode_desc,
    secondary_lag_seconds = hdrs.secondary_lag_seconds,
    last_commit_time = hdrs.last_commit_time,
    last_hardened_time = hdrs.last_hardened_time,
    last_redone_time = hdrs.last_redone_time,
    last_received_time = hdrs.last_received_time,
    est_redo_completion_time_min = CONVERT(float, (hdrs.redo_queue_size * 1.0 / NULLIF(hdrs.redo_rate, 0)) / 60.0),
    est_send_drain_time_min = CONVERT(float, (hdrs.log_send_queue_size * 1.0 / NULLIF(hdrs.log_send_rate, 0)) / 60.0),
    group_id = CONVERT(nvarchar(36), ag.group_id)
FROM sys.dm_hadr_database_replica_states AS hdrs
JOIN sys.availability_replicas AS ar
  ON  hdrs.replica_id = ar.replica_id
  AND hdrs.group_id = ar.group_id
JOIN sys.availability_groups AS ag
  ON hdrs.group_id = ag.group_id
JOIN sys.databases AS d
  ON hdrs.database_id = d.database_id
WHERE COALESCE(ag.is_distributed, 0) = 0
ORDER BY
    ag.name,
    ar.replica_server_name,
    d.name
OPTION(RECOMPILE);";

    public override string Name => "ag_database_replica_states";

    public override string TargetTable => "ag_database_replica_states";

    public override string? WatermarkColumn => null;

    /// <summary>
    /// Gated off for Azure SQL Database only, matching <see cref="AgReplicaStatesCollector"/> —
    /// see that definition for the reasoning.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("ag_name", CollectorColumnType.Varchar),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("replica_server_name", CollectorColumnType.Varchar),
        new CollectorColumn("is_local", CollectorColumnType.Boolean),
        new CollectorColumn("synchronization_state_desc", CollectorColumnType.Varchar),
        new CollectorColumn("last_hardened_lsn", CollectorColumnType.Varchar),
        new CollectorColumn("last_commit_lsn", CollectorColumnType.Varchar),
        new CollectorColumn("log_send_queue_size", CollectorColumnType.BigInt),
        new CollectorColumn("redo_queue_size", CollectorColumnType.BigInt),
        new CollectorColumn("log_send_rate", CollectorColumnType.BigInt),
        new CollectorColumn("redo_rate", CollectorColumnType.BigInt),
        new CollectorColumn("is_suspended", CollectorColumnType.Boolean),
        new CollectorColumn("suspend_reason_desc", CollectorColumnType.Varchar),
        new CollectorColumn("availability_mode_desc", CollectorColumnType.Varchar),
        new CollectorColumn("secondary_lag_seconds", CollectorColumnType.BigInt),
        /* Appended, never inserted (#991 addendum): an upgraded store gets these via ALTER TABLE ADD
           COLUMN, which appends physically, so a fresh store's generated shape only matches if they
           are last here too. */
        new CollectorColumn("last_commit_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_hardened_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_redone_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_received_time", CollectorColumnType.Timestamp),
        new CollectorColumn("est_redo_completion_time_min", CollectorColumnType.Double),
        new CollectorColumn("est_send_drain_time_min", CollectorColumnType.Double),
        /* Appended LAST (#4475's group-count fallback rung), same shape and reasoning as the replica-grain
           twin in AgReplicaStatesCollector: sys.availability_groups.group_id, stored as text. */
        new CollectorColumn("group_id", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                AgName: reader.IsDBNull(0) ? null : reader.GetString(0),
                DatabaseName: reader.IsDBNull(1) ? null : reader.GetString(1),
                ReplicaServerName: reader.IsDBNull(2) ? null : reader.GetString(2),
                IsLocal: reader.IsDBNull(3) ? null : reader.GetBoolean(3),
                SynchronizationStateDesc: reader.IsDBNull(4) ? null : reader.GetString(4),
                LastHardenedLsn: reader.IsDBNull(5) ? null : reader.GetString(5),
                LastCommitLsn: reader.IsDBNull(6) ? null : reader.GetString(6),
                LogSendQueueSize: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                RedoQueueSize: reader.IsDBNull(8) ? null : reader.GetInt64(8),
                LogSendRate: reader.IsDBNull(9) ? null : reader.GetInt64(9),
                RedoRate: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                IsSuspended: reader.IsDBNull(11) ? null : reader.GetBoolean(11),
                SuspendReasonDesc: reader.IsDBNull(12) ? null : reader.GetString(12),
                AvailabilityModeDesc: reader.IsDBNull(13) ? null : reader.GetString(13),
                SecondaryLagSeconds: reader.IsDBNull(14) ? null : reader.GetInt64(14),
                LastCommitTime: reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                LastHardenedTime: reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                LastRedoneTime: reader.IsDBNull(17) ? null : reader.GetDateTime(17),
                LastReceivedTime: reader.IsDBNull(18) ? null : reader.GetDateTime(18),
                EstRedoCompletionTimeMin: reader.IsDBNull(19) ? null : reader.GetDouble(19),
                EstSendDrainTimeMin: reader.IsDBNull(20) ? null : reader.GetDouble(20),
                GroupId: reader.IsDBNull(21) ? null : reader.GetString(21)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.AgName)                    /* ag_name VARCHAR */
            .Value(row.DatabaseName)              /* database_name VARCHAR */
            .Value(row.ReplicaServerName)         /* replica_server_name VARCHAR */
            .Value(row.IsLocal)                   /* is_local BOOLEAN */
            .Value(row.SynchronizationStateDesc)  /* synchronization_state_desc VARCHAR */
            .Value(row.LastHardenedLsn)           /* last_hardened_lsn VARCHAR */
            .Value(row.LastCommitLsn)             /* last_commit_lsn VARCHAR */
            .Value(row.LogSendQueueSize)          /* log_send_queue_size BIGINT (KB) */
            .Value(row.RedoQueueSize)             /* redo_queue_size BIGINT (KB) */
            .Value(row.LogSendRate)               /* log_send_rate BIGINT (KB/s) */
            .Value(row.RedoRate)                  /* redo_rate BIGINT (KB/s) */
            .Value(row.IsSuspended)               /* is_suspended BOOLEAN */
            .Value(row.SuspendReasonDesc)         /* suspend_reason_desc VARCHAR */
            .Value(row.AvailabilityModeDesc)      /* availability_mode_desc VARCHAR */
            .Value(row.SecondaryLagSeconds)       /* secondary_lag_seconds BIGINT (s) */
            .Value(row.LastCommitTime)            /* last_commit_time TIMESTAMP */
            .Value(row.LastHardenedTime)          /* last_hardened_time TIMESTAMP */
            .Value(row.LastRedoneTime)            /* last_redone_time TIMESTAMP */
            .Value(row.LastReceivedTime)          /* last_received_time TIMESTAMP */
            .Value(row.EstRedoCompletionTimeMin)  /* est_redo_completion_time_min DOUBLE (min) */
            .Value(row.EstSendDrainTimeMin)       /* est_send_drain_time_min DOUBLE (min) */
            .Value(row.GroupId);                  /* group_id VARCHAR (uuid text, #4475) */
    }
}
