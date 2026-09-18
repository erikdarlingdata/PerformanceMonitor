/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The <c>next_tools</c> table for the PostgreSQL-target fact vocabulary (#3542) — the <c>PG_</c> /
/// <c>CONFIG_PG_</c> / <c>ANOMALY_PG_</c> arm <see cref="ToolRecommendations.GetForStoryPath"/> consults
/// before its SQL Server prefix arms, so a PostgreSQL story's recommended reads name <c>get_pg_*</c> tools and
/// never <c>get_wait_stats</c>. Keyed by the constants in <see cref="PgTargetFactKeys"/>, with the two dynamic
/// families (<see cref="PgTargetFactKeys.WaitKeyPrefix"/>, <see cref="PgTargetFactKeys.BadActorKeyPrefix"/>)
/// resolved by prefix. A key no row claims yields null and the caller skips it, as the SQL Server table does.
///
/// <para>Pre-filled with each family's obvious read in the plumbing lane rather than left for the content
/// lanes to fill, for the same reason <see cref="PgTargetFactKeys.ConfigAdvisoryRoots"/> is: one shared file
/// edited by eight parallel lanes is a merge conflict, and which <c>get_pg_*</c> tool reads a family's source
/// table is a fact about the product, not a threshold. A lane that adds a read (a drill-down tool, a trend
/// tool) adds its row here in its own PR.</para>
/// </summary>
internal static class PgTargetToolRecommendations
{
    private static readonly Dictionary<string, List<ToolRecommendation>> ByFactKey = new(StringComparer.Ordinal)
    {
        [PgTargetFactKeys.ServerMajorVersion] =
        [
            new("get_pg_extensions", "Which extensions this target has installed and available — the readiness picture behind the facts"),
        ],
        [PgTargetFactKeys.ConfigSharedBuffers] = ConfigReads(),
        [PgTargetFactKeys.ConfigMaxWalSize] = ConfigReads(),
        [PgTargetFactKeys.ConfigEffectiveCacheSize] = ConfigReads(),
        [PgTargetFactKeys.ConfigRandomPageCost] = ConfigReads(),
        [PgTargetFactKeys.ConfigTrackIoTiming] = ConfigReads(),
        [PgTargetFactKeys.ConfigCheckpointTimeout] = ConfigReads(),
        [PgTargetFactKeys.ConfigWalCompression] = ConfigReads(),
        [PgTargetFactKeys.ConfigAutovacuumOff] = ConfigReads(),
        [PgTargetFactKeys.ConfigMaxConnections] = ConfigReads(),
        [PgTargetFactKeys.ConfigSuperuserReserved] = ConfigReads(),
        [PgTargetFactKeys.ConfigWorkMem] = ConfigReads(),
        [PgTargetFactKeys.ConfigMaintWorkMem] = ConfigReads(),
        [PgTargetFactKeys.ConfigStatStatementsMissing] =
        [
            new("get_pg_extensions", "Whether pg_stat_statements is installed, available, or one CREATE EXTENSION away"),
            new("get_pg_server_config", "shared_preload_libraries and the settings that gate statement tracking"),
        ],
        [PgTargetFactKeys.PostureFsync] = ConfigReads(),
        [PgTargetFactKeys.PostureFullPageWrites] = ConfigReads(),
        [PgTargetFactKeys.PostureSynchronousCommit] = ConfigReads(),
        [PgTargetFactKeys.CheckpointPressure] =
        [
            new("get_pg_write_stats", "Checkpoint counts, WAL bytes and bgwriter activity over the window"),
            new("get_pg_server_config", "max_wal_size, checkpoint_timeout and checkpoint_completion_target as configured"),
        ],
        [PgTargetFactKeys.WalVolumeShift] =
        [
            new("get_pg_write_stats", "WAL bytes per interval over the window"),
        ],
        [PgTargetFactKeys.BufferCachePressure] =
        [
            new("get_pg_database_stats", "Per-database block hits and reads — the hit-ratio arm"),
            new("get_pg_io_stats", "Evictions and reads by backend type and context (PG 16+)"),
            new("get_pg_buffer_usage", "What the buffer cache holds right now, by relation"),
        ],
        [PgTargetFactKeys.Tps] = DatabaseReads(),
        [PgTargetFactKeys.HitRatio] = DatabaseReads(),
        [PgTargetFactKeys.DeadlockRate] =
        [
            new("get_pg_database_stats", "The engine's per-database deadlock counter the rate is derived from"),
            new("get_pg_deadlocks", "The deadlock graphs the log captured — exemplars, not the count"),
            new("get_pg_deadlock_detail", "One captured deadlock's full graph and statements"),
        ],
        [PgTargetFactKeys.TempSpill] =
        [
            new("get_pg_database_stats", "Per-database temp files and temp bytes over the window"),
            new("get_pg_top_queries", "The statements writing the most temp blocks"),
            new("get_pg_server_config", "work_mem as configured"),
        ],
        [PgTargetFactKeys.ConnectionSaturation] =
        [
            new("get_pg_session_states", "Sessions by state over the window, and the redacted share"),
            new("get_pg_server_config", "max_connections and superuser_reserved_connections as configured"),
        ],
        [PgTargetFactKeys.MonitoringPermissions] =
        [
            new("get_pg_session_states", "How much of pg_stat_activity the monitoring role could see"),
        ],
        [PgTargetFactKeys.IdleInTransaction] =
        [
            new("get_pg_session_states", "Idle-in-transaction sessions and how long they have held"),
        ],
        [PgTargetFactKeys.AutovacuumBacklog] =
        [
            new("get_pg_autovacuum_health", "Per-table dead tuples against each table's own threshold, and when autovacuum last ran"),
            new("get_pg_table_bloat", "How much space the backlog has already cost"),
        ],
        [PgTargetFactKeys.WraparoundTrend] =
        [
            new("get_pg_wraparound_risk", "XID and MultiXact age per database against autovacuum_freeze_max_age"),
            new("get_pg_autovacuum_health", "Whether the freeze work is keeping up"),
        ],
        [PgTargetFactKeys.XminHold] =
        [
            new("get_pg_xmin_horizon", "What is holding the horizon back, by source"),
            new("get_pg_replication_slots", "Slot xmin and retained WAL, when the holder is a slot"),
            new("get_pg_session_states", "The idle-in-transaction session, when the holder is a backend"),
        ],
        [PgTargetFactKeys.CpuPercent] =
        [
            new("get_pg_cpu_utilization", "Instance CPU over the window (Aurora / Performance Insights)"),
            new("get_pg_top_queries", "The statements consuming the most execution time"),
        ],
        [PgTargetFactKeys.AnomalyTps] = DatabaseReads(),
        [PgTargetFactKeys.AnomalySessionSpike] =
        [
            new("get_pg_session_states", "Session counts over the window against the usual level for this hour"),
        ],
        [PgTargetFactKeys.AnomalyCpuSpike] =
        [
            new("get_pg_cpu_utilization", "Instance CPU over the window against the usual level for this hour"),
        ],
        [PgTargetFactKeys.AnomalyDeadlockRate] =
        [
            new("get_pg_database_stats", "The deadlock counter deltas the ratio is computed from"),
            new("get_pg_deadlocks", "The captured deadlock graphs"),
        ],
        [PgTargetFactKeys.AnomalyWaitProfile] = WaitReads(),
    };

    /// <summary>The reads for a story-path key, or null when no row and no prefix claims it.</summary>
    public static List<ToolRecommendation>? GetForKey(string key)
    {
        if (ByFactKey.TryGetValue(key, out var recommendations))
            return recommendations;

        if (key.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal))
            return WaitReads();

        if (key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
        {
            return
            [
                new("get_pg_top_queries", "This statement's deltas beside the rest of the window's top statements"),
                new("get_pg_plans", "Captured plans for the statement, when plan capture is configured"),
            ];
        }

        return null;
    }

    private static List<ToolRecommendation> ConfigReads() =>
    [
        new("get_pg_server_config", "The current setting, its source and whether a restart is pending"),
        new("get_pg_server_config_changes", "When the setting last changed and from what"),
    ];

    private static List<ToolRecommendation> DatabaseReads() =>
    [
        new("get_pg_database_stats", "Per-database transaction, block and temp counters over the window"),
        new("get_pg_database_trend", "The same counters as a series"),
    ];

    private static List<ToolRecommendation> WaitReads() =>
    [
        new("get_pg_wait_stats", "Wait events over the window (Aurora deltas)"),
        new("get_pg_wait_sampling", "Sampled wait events over the window (stock PostgreSQL)"),
        new("get_pg_wait_trend", "The wait profile as a series"),
    ];
}
