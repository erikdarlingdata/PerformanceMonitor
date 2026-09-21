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
        [PgTargetFactKeys.ConfigReservedConnections] = ConfigReads(),
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
            /* Lane 29 of #3691: the composition drill-down names the relation that holds the pool; this is the read for what reads it. */
            new("get_pg_top_queries", "The heaviest statements with their shared_blks_read beside shared_blks_hit — which of them is missing the cache"),
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
        /* #3691 between waves (lane 14's report): the idle fact names two of three chains its advice sends the operator
           to — the xmin claim and the blocking leaf — so the reads for both follow the session read. */
        [PgTargetFactKeys.IdleInTransaction] =
        [
            new("get_pg_session_states", "Idle-in-transaction sessions and how long they have held"),
            new("get_pg_xmin_horizon", "Whether the parked transaction is the session holding the xmin horizon back"),
            new("get_pg_blocking", "Whether the parked transaction is the root of a captured blocking chain"),
        ],
        [PgTargetFactKeys.AutovacuumBacklog] =
        [
            new("get_pg_autovacuum_health", "Per-table dead tuples against each table's own threshold, and when autovacuum last ran"),
            new("get_pg_table_bloat", "How much space the backlog has already cost"),
        ],
        /* #3691 step 22: the per-table reloption card reads the same two tools as the backlog it sits beside — the
           per-table list (disabled tables first, so the operator sees the rest of the set) and what it has cost. */
        [PgTargetFactKeys.ConfigAutovacuumDisabled] =
        [
            new("get_pg_autovacuum_health", "Every disabled table past its own line, with dead tuples, threshold and when autovacuum last ran"),
            new("get_pg_table_bloat", "How much space the unvacuumed table has already cost"),
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
        /* lane 24 (#3691): the stock SAMPLED wait profile reads the same three wait tools; get_pg_wait_sampling is the one
           that carries its rows. */
        [PgTargetFactKeys.AnomalySampledWaitProfile] = WaitReads(),

        /* v2 (#3691), pre-filled by the v2 plumbing as the v1 rows were: which get_pg_* read a family's table is
           a fact about the product, not a threshold, and PgTargetMcpSurfaceTests requires a row for every key
           the day it is declared. The content lanes (11 I/O, 12 replication, 13 bloat, 15 WAL) may sharpen a
           reason in their own PR; the tool names are the tables' reads. */
        [PgTargetFactKeys.IoReadLatencyMs] = IoReads(),
        [PgTargetFactKeys.IoWriteLatencyMs] = IoReads(),
        [PgTargetFactKeys.AnomalyIoLatency] =
        [
            new("get_pg_io_trend", "Read latency over the window against the usual level for this hour"),
            new("get_pg_io_stats", "Reads, read time and evictions by backend type and context (PG 16+)"),
        ],
        [PgTargetFactKeys.ReplicationLag] =
        [
            new("get_pg_replication_stats", "Per-standby write, flush and replay lag over the window"),
            new("get_pg_write_stats", "How much WAL the primary is producing for the standbys to replay"),
        ],
        [PgTargetFactKeys.SlotRetention] =
        [
            new("get_pg_replication_slots", "Each slot's retained WAL, activity and safe_wal_size"),
            new("get_pg_server_config", "max_slot_wal_keep_size and wal_keep_size as configured"),
        ],
        [PgTargetFactKeys.SlotXmin] =
        [
            new("get_pg_replication_slots", "Each slot's xmin and catalog_xmin, and whether it is active"),
            new("get_pg_xmin_horizon", "The horizon by holder — the slot against the other candidates"),
        ],
        [PgTargetFactKeys.AnomalyReplicationLag] =
        [
            new("get_pg_replication_stats", "Replay lag over the window against the usual level for this hour"),
        ],
        [PgTargetFactKeys.BloatTrend] =
        [
            new("get_pg_table_bloat", "Estimated table bloat by relation, sampled hourly"),
            new("get_pg_autovacuum_health", "Whether autovacuum is keeping up on the growing tables"),
        ],
        [PgTargetFactKeys.IndexBloatTrend] =
        [
            new("get_pg_index_bloat", "Estimated index bloat by index, sampled daily"),
            new("get_pg_index_usage", "Whether the bloated index is read at all"),
        ],
        [PgTargetFactKeys.AnomalyWalVolume] =
        [
            new("get_pg_write_stats", "WAL bytes per interval over the window against the usual level for this hour"),
        ],
        /* Wave 2: declared, nothing emits it yet; the read is the autovacuum health one it will hang on. */
        [PgTargetFactKeys.MaintenanceShapeShift] =
        [
            new("get_pg_autovacuum_health", "Autovacuum worker activity by table over the window"),
        ],
        /* Wave 3 (#3691, between waves): the blocking family's rows, declared with its stubs — PgTargetMcpSurfaceTests
           requires a row the day a key is declared, and which get_pg_* read a family's table is a fact about the
           product, not a threshold. Lane 17 may reorder or extend them when the advice names its first question. */
        [PgTargetFactKeys.BlockingChain] = BlockingReads(),
        [PgTargetFactKeys.LockWaitEvents] =
        [
            new("get_pg_log_events", "The lock_wait family: log_lock_waits' own 'still waiting' lines, written by the engine rather than sampled"),
            new("get_pg_blocking", "The sampled chains over the same window, root blocker attributed"),
        ],
        [PgTargetFactKeys.LongRunningQuery] =
        [
            new("get_pg_session_states", "Active sessions and how long each statement has been running"),
            new("get_pg_blocking", "Whether the long runner is the root of a captured blocking chain"),
        ],
        [PgTargetFactKeys.AnomalyBlocking] = BlockingReads(),
        /* v3 (#3691 plumbing): the plan, kernel and memory families' rows, declared with their stubs — the same rule as
           wave 3's. Every tool named here was verified registered (rg over the Mcp folder); the plan read is
           get_pg_plans (there is no get_pg_plan_captures), and the host-memory columns ride pg_cpu_utilization's row
           (V136), so the host read is get_pg_cpu_utilization — no memory tool exists or is needed. The content lanes
           may reorder or extend. */
        [PgTargetFactKeys.PlanRegression] = PlanReads(),
        [PgTargetFactKeys.ParameterSensitivity] =
        [
            new("get_pg_plans", "Every plan_hash captured for the statement over the window, and when each appeared"),
            new("get_pg_column_stats", "The predicate columns' top_value_frequency and n_distinct — the skew behind the flip"),
            new("get_pg_top_queries", "The statement's calls and mean time beside its neighbours"),
        ],
        [PgTargetFactKeys.SeqScanAdvisory] =
        [
            new("get_pg_plans", "The captured plan with the Seq Scan node, its Filter (literals redacted), its row estimate and actual rows"),
            new("get_pg_predicate_stats", "Which columns the predicate names, how often it was evaluated and how selective it was (pg_qualstats) — the only source an index column is named from"),
            new("get_pg_top_queries", "The statement's text, calls and mean time — what the scan costs per hour"),
            new("get_pg_table_bloat", "The relation's heap size and dead-tuple share — the size gate and the write side of the index's cost"),
            new("get_pg_column_stats", "The scanned columns' distribution — whether the selective predicate is selective for every value or only the common one"),
        ],
        [PgTargetFactKeys.AnomalyPlanRegression] = PlanReads(),
        [PgTargetFactKeys.CpuBurnCores] = KernelReads(),
        [PgTargetFactKeys.CpuDecomposition] =
        [
            new("get_pg_kernel_stats", "User and system CPU time by statement over the window"),
            new("get_pg_wait_stats", "The wait time the CPU time is decomposed against"),
        ],
        [PgTargetFactKeys.AnomalyCpuBurn] = KernelReads(),
        /* lane 34 (#3691): the bad actor's own-normal deviation reads what its statement's card reads — the trend
           question first (mean stepped or calls did), the statement beside the window's others, the captured plan. */
        [PgTargetFactKeys.AnomalyBadActorShare] = BadActorReads(),
        [PgTargetFactKeys.ConfigMemoryOvercommit] =
        [
            new("get_pg_server_config", "shared_buffers, max_connections, work_mem, maintenance_work_mem and autovacuum_max_workers as set"),
            new("get_pg_cpu_utilization", "The host's memory_total_bytes the sum is measured against (the V136 memory columns ride the capacity row)"),
            new("get_pg_session_states", "How many of max_connections are in use — the ceiling's distance from the load"),
        ],
        [PgTargetFactKeys.HostMemoryPressure] =
        [
            new("get_pg_cpu_utilization", "Host memory free, cached and total per capture over the window — the pressure reading itself"),
            new("get_pg_server_config", "The memory knobs the host's free-plus-cached share is measured against"),
            new("get_pg_buffer_usage", "What the shared cache holds while the host is short"),
        ],
        /* lane 38 (#3691): the object-growth family's rows. There is no get_pg_database_size tool yet (verified by rg over
           the Mcp folder — pg_database_size_stats has no served read; ServerPageTabsTests carries the sequencing exemption),
           so the first read is the fact itself through get_analysis_facts, and the bloat read answers the question the
           card asks first. */
        [PgTargetFactKeys.DatabaseGrowth] = GrowthReads(),
        [PgTargetFactKeys.AnomalyDatabaseGrowth] = GrowthReads(),
    };

    private static List<ToolRecommendation> GrowthReads() =>
    [
        new("get_analysis_facts", "source=pg_growth — the trend fact itself: size at both ends of the lookback, growth per day, days-to-double, the top databases by name and the instance total"),
        new("get_pg_table_bloat", "Whether the new bytes are live rows or dead ones — the bloat question the card asks first, and the database's tables by measured heap size"),
        new("get_pg_autovacuum_health", "Whether autovacuum is keeping up on the tables that grew"),
    ];

    private static List<ToolRecommendation> PlanReads() =>
    [
        new("get_pg_plans", "The captured plans for the statement — both plan_hash values and when the flip was captured"),
        new("get_pg_top_queries", "The statement's mean time and calls over the window against its neighbours"),
        new("get_pg_query_duration_trend", "The statement's duration over time — where the step is"),
    ];

    private static List<ToolRecommendation> KernelReads() =>
    [
        new("get_pg_kernel_stats", "User and system CPU time by statement (pg_stat_kcache) — who burned the cores"),
        new("get_pg_top_queries", "The same statements by calls and mean time"),
        new("get_pg_extensions", "Whether pg_stat_kcache is installed and loaded — the reading exists only where it is"),
    ];

    private static List<ToolRecommendation> BlockingReads() =>
    [
        new("get_pg_blocking", "Captured blocking chains with the root blocker attributed and its recurrence across captures"),
        new("get_pg_lock_stats", "Which lock modes and relations were contended over the window"),
        new("get_pg_log_events", "The engine's written lock_wait events between the samples"),
    ];

    private static List<ToolRecommendation> IoReads() =>
    [
        new("get_pg_io_stats", "Reads, writes and their timings by backend type and context (PG 16+)"),
        new("get_pg_io_trend", "Read and write latency over the window"),
        new("get_pg_server_config", "track_io_timing — without it the timings are zero, not fast"),
    ];

    /// <summary>The reads for a story-path key, or null when no row and no prefix claims it.</summary>
    public static List<ToolRecommendation>? GetForKey(string key)
    {
        if (ByFactKey.TryGetValue(key, out var recommendations))
            return recommendations;

        if (key.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal))
            return WaitReads();

        if (key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
            return BadActorReads();

        return null;
    }

    /// <summary>The queries family's reads, for a <c>PG_BAD_ACTOR_*</c> card and (lane 34) for the own-normal anomaly
    /// that walks into it. The advice (PgTargetAdvice.Queries.cs) ends on "which is the first question": whether the
    /// mean STEPPED (a plan change) or the calls did (a workload change). The read that answers it leads.</summary>
    private static List<ToolRecommendation> BadActorReads() =>
    [
        new("get_pg_query_duration_trend", "Whether this statement's mean execution time stepped or its call count did — the first question"),
        new("get_pg_top_queries", "This statement's deltas beside the rest of the window's top statements"),
        new("get_pg_plans", "Captured plans for the statement, when plan capture is configured"),
    ];

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
