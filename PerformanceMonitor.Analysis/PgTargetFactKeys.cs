/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The <see cref="Fact.Source"/> values a PostgreSQL-TARGET analysis pass emits (#3542, design decision
/// D2) — every one <c>pg_</c>-prefixed, so that no SQL Server amplifier, tuning-class cap or
/// <see cref="FactAdvice"/> arm can match a PostgreSQL fact by accident. A shared source name
/// (<c>"waits"</c>, <c>"cpu"</c>, <c>"config"</c>) would let <c>PG_CPU_PERCENT</c> ride
/// <c>CPU_SQL_PERCENT</c>'s co-fire arm, or a PostgreSQL wait fall into <c>GetWaitThresholds</c>; the
/// prefix makes that cross-fire structurally impossible rather than merely unlikely.
///
/// <para><b>Why every constant's name ends in <c>Source</c>.</b> <c>FactSourceRegistryTests</c> sweeps the
/// three fact-collector assemblies for <c>Source = "…"</c> and asserts the swept set equals
/// <see cref="FactScorer.KnownSources"/> exactly. The sweep's own doc names the one blessed shape for a
/// source declared as a constant rather than stamped inline — <c>WindowCoverage.FactSource = "coverage"</c>
/// — and these follow it, so the PostgreSQL vocabulary is registered the day it is declared, BEFORE any
/// row persists, and a lane that stamps <c>Source = PgTargetSources.ConfigSource</c> can never emit a
/// source the registry does not list. That is the D2 argument made concrete: the source and key names are
/// the one decision that becomes expensive to reverse the moment the first PostgreSQL finding row lands in
/// <c>analysis_findings.root_fact_key</c> or a mute rule.</para>
///
/// <para><c>coverage</c> is deliberately NOT here: the coverage witness fact is engine-neutral and keeps
/// <see cref="WindowCoverage.FactSource"/>. PostgreSQL anomaly facts keep <c>"anomaly"</c> too — their
/// KEYS carry the <c>ANOMALY_PG_</c> prefix, and the shared anomaly scorer routes on the key.</para>
/// </summary>
public static class PgTargetSources
{
    /// <summary>Server-setting facts (<c>CONFIG_PG_*</c>) and the point-in-time registry facts.</summary>
    public const string ConfigSource = "pg_config";

    /// <summary>Durability posture (<c>fsync</c>, <c>full_page_writes</c>, <c>synchronous_commit</c>) — D6:
    /// posture-only, never a performance argument, structurally isolated from every amplifier and edge.</summary>
    public const string PostureSource = "pg_posture";

    /// <summary>Checkpoint / WAL pressure from <c>pg_write_stats</c>.</summary>
    public const string WriteSource = "pg_write";

    /// <summary>Buffer-cache pressure composite (hit ratio, evictions, bgwriter).</summary>
    public const string BufferSource = "pg_buffer";

    /// <summary>Connection saturation and session-state facts from <c>pg_session_states</c>.</summary>
    public const string SessionsSource = "pg_sessions";

    /// <summary>Autovacuum backlog, wraparound trend and xmin hold.</summary>
    public const string VacuumSource = "pg_vacuum";

    /// <summary>Wait-profile facts — Aurora deltas or the stock sampling estimate (<c>is_sampled</c>).</summary>
    public const string WaitsSource = "pg_waits";

    /// <summary>Temp-file spill from <c>pg_database_stats</c>.</summary>
    public const string TempSource = "pg_temp";

    /// <summary>Top statements (<c>PG_BAD_ACTOR_*</c>) from <c>pg_statement_stats</c>.</summary>
    public const string QueriesSource = "pg_queries";

    /// <summary>Per-database counter facts (TPS, hit ratio, deadlock rate) from <c>pg_database_stats</c>.</summary>
    public const string DatabaseSource = "pg_database";

    /// <summary>Instance CPU from <c>pg_cpu_utilization</c> — Aurora / Performance Insights only.</summary>
    public const string CpuSource = "pg_cpu";

    /* ── v2 (#3691) sources, declared by the v2 plumbing lane and filled by lanes 11–13. ── */

    /// <summary>Data-file I/O latency facts from <c>pg_io_stats</c> (<c>pg_stat_io</c>, PostgreSQL 16+; lane 11).</summary>
    public const string IoSource = "pg_io";

    /// <summary>Replication lag and slot-retention facts from <c>pg_replication_stats</c> /
    /// <c>pg_replication_slot_stats</c> (lane 12).</summary>
    public const string ReplicationSource = "pg_replication";

    /// <summary>Table and index bloat TRENDS from <c>pg_table_bloat_stats</c> (hourly) / <c>pg_index_bloat</c>
    /// (daily) — sparser cadences than the one-minute witness, so every fact here states its own sample count
    /// (lane 13).</summary>
    public const string BloatSource = "pg_bloat";

    /* ── wave 3 (#3691) source, declared by the between-waves batch and filled by lane 17. ── */

    /// <summary>Blocking-chain, lock-wait-event and long-running-query facts from <c>pg_blocking_edges</c> (the
    /// sampled chain edges, <c>BlockingChainReconstructor</c>'s input), <c>pg_log_events</c> (the <c>lock_wait</c>
    /// family — the engine's own written blocked-process report) and <c>pg_session_states</c> (design §2a; lane 17).</summary>
    public const string BlockingSource = "pg_blocking";

    /* ── v3 (#3691) sources, declared by the v3 plumbing lane and filled by lanes 27 / 28 / 32. ── */

    /// <summary>Plan-shape facts from <c>pg_plan_capture</c> (auto_explain captures keyed by <c>query_id</c> and
    /// <c>plan_hash</c>) joined to <c>pg_statement_stats</c> (the per-statement counters the step-change is read
    /// from) and, for the predicate side, <c>pg_column_stats</c> / <c>pg_predicate_stats</c> (design §6; lane 27,
    /// with lane 30's Seq-Scan advisory in the same family).</summary>
    public const string PlansSource = "pg_plans";

    /// <summary>Kernel-time facts from <c>pg_kernel_stats</c> — <c>pg_stat_kcache</c>'s per-statement user and
    /// system CPU time, the self-hosted CPU PROXY where the Aurora capacity percent does not exist; the extension's
    /// presence is read from <c>pg_extension_availability</c>, never inferred from the table (lane 28).</summary>
    public const string KernelSource = "pg_kernel";

    /// <summary>Memory-composition and host-memory facts: the §4b arithmetic over <c>pg_server_config</c>
    /// (<c>shared_buffers</c>, <c>max_connections</c>, <c>work_mem</c>, <c>maintenance_work_mem</c>,
    /// <c>autovacuum_max_workers</c>) against the host's <c>memory_total_bytes</c>, which rides
    /// <c>pg_cpu_utilization</c>'s row since V136 (lane R5: six host-memory columns on the existing hypertable, the
    /// V130 columns-on-the-row precedent — there is NO <c>pg_host_memory</c> table), and the host's free-plus-cached
    /// share on Aurora (lane 32).</summary>
    public const string MemorySource = "pg_memory";

    /// <summary>The prefix every PostgreSQL-target source carries; the shared scorer routes on it.</summary>
    public const string Prefix = "pg_";

    /// <summary>Every PostgreSQL-target source, sorted ordinally — the shape <see cref="FactScorer.KnownSources"/>
    /// keeps, so the two lists can be compared without re-sorting.</summary>
    public static readonly IReadOnlyList<string> All = new[]
    {
        BloatSource, BlockingSource, BufferSource, ConfigSource, CpuSource, DatabaseSource, IoSource, KernelSource, MemorySource,
        PlansSource, PostureSource, QueriesSource, ReplicationSource, SessionsSource, TempSource, VacuumSource, WaitsSource,
        WriteSource,
    };

    /// <summary>Whether <paramref name="source"/> is a PostgreSQL-target source.</summary>
    public static bool IsPgSource(string? source) =>
        source is not null && source.StartsWith(Prefix, StringComparison.Ordinal);
}

/// <summary>
/// The whole PostgreSQL-target fact vocabulary (#3542 D2 for v1; #3691 for v2), declared once and BEFORE any content lane
/// emits a row, so every collector partial, scorer arm, advice block and graph edge references a constant
/// and a renamed key fails to compile instead of orphaning persisted <c>root_fact_key</c> values.
///
/// <para>Three prefixes, and the shared code routes on them: <c>PG_</c> for measured facts,
/// <c>CONFIG_PG_</c> for setting checks, <c>ANOMALY_PG_</c> for baseline deviations. Nothing here may
/// collide with a SQL Server key by prefix — <c>ANOMALY_PG_CPU_SPIKE</c> does not start with
/// <c>ANOMALY_CPU</c>, so the SQL Server tool-recommendation prefix arms and <c>IsDeviationScoredAnomalyKey</c>
/// literals cannot claim it; the reverse is guaranteed by the <c>PG_</c> infix.</para>
///
/// <para>The comment beside each key names the lane that fills its collector / scorer / advice, per the
/// validated v1 build plan. Keys the design places in v2 but which a v1 graph edge or advice block already
/// names (<c>PG_IDLE_IN_TRANSACTION</c>, <c>PG_WAL_VOLUME_SHIFT</c>) are declared so the edge can be written
/// against a constant and simply stay inert until the fact exists.</para>
/// </summary>
public static class PgTargetFactKeys
{
    /* ── Plumbing (this lane): the one point-in-time registry fact the skeleton emits. ── */

    /// <summary>The target's PostgreSQL major, from <c>servers.postgres_major_version</c> — the twin of
    /// <c>SERVER_MAJOR_VERSION</c>. Value = major; metadata <c>is_aurora</c> = 0/1 from the registry kind.
    /// Base severity 0: context only. Emitted by the plumbing skeleton so a PostgreSQL pass has at least
    /// one fact to score before the content lanes land, and so the D6 flavour disclosure has a fact to
    /// hang on.</summary>
    public const string ServerMajorVersion = "PG_SERVER_MAJOR_VERSION";

    /* ── Lane 2 — the two knobs and their workload co-fires. ── */

    public const string ConfigSharedBuffers = "CONFIG_PG_SHARED_BUFFERS";
    public const string ConfigMaxWalSize = "CONFIG_PG_MAX_WAL_SIZE";
    public const string ConfigEffectiveCacheSize = "CONFIG_PG_EFFECTIVE_CACHE_SIZE";
    public const string ConfigRandomPageCost = "CONFIG_PG_RANDOM_PAGE_COST";
    public const string ConfigTrackIoTiming = "CONFIG_PG_TRACK_IO_TIMING";
    public const string ConfigCheckpointTimeout = "CONFIG_PG_CHECKPOINT_TIMEOUT";
    public const string ConfigWalCompression = "CONFIG_PG_WAL_COMPRESSION";
    public const string ConfigStatStatementsMissing = "CONFIG_PG_STAT_STATEMENTS_MISSING";
    /// <summary>Context facts (value-bearing, base 0) lane 3's saturation fact reads at SCORE time.</summary>
    public const string ConfigMaxConnections = "CONFIG_PG_MAX_CONNECTIONS";
    public const string ConfigSuperuserReserved = "CONFIG_PG_SUPERUSER_RESERVED";
    public const string BufferCachePressure = "PG_BUFFER_CACHE_PRESSURE";
    public const string CheckpointPressure = "PG_CHECKPOINT_PRESSURE";
    /// <summary>The window's WAL volume against its baseline, as lane 15 of #3691 shipped it: a CONTEXT fact (base
    /// severity 0) carrying the measured rates for the advice to state. The JUDGMENT is <see cref="AnomalyWalVolume"/>,
    /// which folds onto <see cref="CheckpointPressure"/> and is what every co-fire reads (the checkpoint trigger
    /// amplifier, the replication family's two WAL amplifiers); no graph edge leaves this key or the anomaly — a
    /// base-0 fact is never in the fired set, so an edge from it can never open (the #3691 between-waves lesson,
    /// pinned in <c>PgTargetWriteTests</c> and <c>PgTargetReplicationTests</c>).</summary>
    public const string WalVolumeShift = "PG_WAL_VOLUME_SHIFT";
    /// <summary>The one <c>pg_database_stats</c> read (lane 2's <c>Database.cs</c>) emits these four.</summary>
    public const string Tps = "PG_TPS";
    public const string HitRatio = "PG_HIT_RATIO";
    public const string DeadlockRate = "PG_DEADLOCK_RATE";
    public const string TempSpill = "PG_TEMP_SPILL";

    /* ── Lane 3 — connection saturation. ── */

    public const string ConnectionSaturation = "PG_CONNECTION_SATURATION";
    /// <summary>Emitted INSTEAD of a saturation number when the monitoring role could not see session state
    /// (<c>state_is_redacted</c> share ≥ ½ — the documented silent-failure mode).</summary>
    public const string MonitoringPermissions = "PG_MONITORING_PERMISSIONS";
    /// <summary>v2 fact; declared for the saturation and vacuum-chain edges that name it.</summary>
    public const string IdleInTransaction = "PG_IDLE_IN_TRANSACTION";

    /* ── Lane 4 — the vacuum family (D3), constants shared with the Tier-0 alerts (D9). ── */

    public const string AutovacuumBacklog = "PG_AUTOVACUUM_BACKLOG";
    public const string WraparoundTrend = "PG_WRAPAROUND_TREND";
    public const string XminHold = "PG_XMIN_HOLD";
    public const string ConfigAutovacuumOff = "CONFIG_PG_AUTOVACUUM_OFF";
    /// <summary>Evidence-gated on <see cref="AutovacuumBacklog"/> (D5); NOT an advisory root.</summary>
    public const string ConfigMaintWorkMem = "CONFIG_PG_MAINT_WORK_MEM";
    /// <summary>
    /// A table whose <c>autovacuum_enabled</c> reloption is off AND which sits persistently past its own
    /// trigger line (design §3.1, #3691 step 22). Source <c>pg_vacuum</c>, not <c>pg_config</c>: the value is
    /// read per table from <c>pg_autovacuum_stats</c>, never from <c>pg_settings</c>, and the fact exists only
    /// when the backlog does — a disabled table nobody needs vacuumed is not a finding. The workload co-fire
    /// is therefore built into the emission, which is why its base sits in the 0.9 band rather than at D5's
    /// 0.4 advisory base; it is NOT in <see cref="ConfigAdvisoryRoots"/> (it clears the incident line on its
    /// own). The <c>CONFIG_PG_</c> prefix is kept because the finding IS a configuration — one an operator set
    /// with <c>ALTER TABLE … SET (autovacuum_enabled = off)</c> — so it routes through the config prefix arms
    /// of the shared switches; its base score comes through its SOURCE (<c>ScoreVacuumFact</c>), and the
    /// vacuum family names it in the advice dispatcher the way it names <see cref="ConfigAutovacuumOff"/>.
    /// </summary>
    public const string ConfigAutovacuumDisabled = "CONFIG_PG_AUTOVACUUM_DISABLED";

    /* ── Lane 5 — wait profile. Dynamic keys under one prefix; see WaitKey. ── */

    /// <summary>Every wait fact's key starts with this; the type rollup is <c>PG_WAIT_LOCK</c>, a named
    /// standout <c>PG_WAIT_LOCK_RELATION</c>.</summary>
    public const string WaitKeyPrefix = "PG_WAIT_";

    /* ── Lane 6 — temp spill's config co-fire. ── */

    /// <summary>Evidence-gated on <see cref="TempSpill"/> (D5); NOT an advisory root.</summary>
    public const string ConfigWorkMem = "CONFIG_PG_WORK_MEM";

    /* ── Lane 7 — top statements. Dynamic keys under one prefix; see BadActorKey. ── */

    public const string BadActorKeyPrefix = "PG_BAD_ACTOR_";

    /* ── Between-waves shared-vocabulary block (#3542, after wave A). Additions here are delimited so a lane
       landing concurrently can rebase around one block rather than a scatter. ── */

    /// <summary>
    /// The stable ALIAS an edge names when its destination is "the bad actor" — whichever
    /// <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> fact the pass emitted. Never a fact's key: no collector stamps it, and
    /// <see cref="BadActorKeyPrefix"/> does not match it (the alias lacks the trailing underscore), so the
    /// queries scorer, advice and tool-recommendation arms cannot claim it by prefix. It exists because
    /// <see cref="RelationshipGraph.AddEdge"/> takes an exact destination string and this family's keys are
    /// dynamic (lane 7's note in <c>PgTargetRelationshipGraph.Query.cs</c>): the families whose symptom a
    /// statement explains (<c>PG_TEMP_SPILL</c>, <c>PG_CPU_PERCENT</c>) declare their edge INTO this alias, and
    /// <see cref="PgTargetRelationshipGraph.GetActiveEdges"/> resolves it at story-build time to the
    /// highest-severity bad actor present, or drops the edge when none is. One alias, one resolution rule,
    /// declared once so lanes 6 and 9 do not each invent one.
    /// </summary>
    public const string BadActorFamily = "PG_BAD_ACTOR";

    /// <summary>The PostgreSQL 16+ second carve-out of connection slots (<c>reserved_connections</c>, for
    /// members of <c>pg_use_reserved_connections</c>; default 0; absent before 16). Context, base 0, read at
    /// collect time by lane 3's ceiling beside <see cref="ConfigMaxConnections"/> and
    /// <see cref="ConfigSuperuserReserved"/>: <c>usable = max_connections − superuser_reserved_connections −
    /// reserved_connections</c>.</summary>
    public const string ConfigReservedConnections = "CONFIG_PG_RESERVED_CONNECTIONS";

    /* ── Lane 8 — durability posture (D6): posture-only, isolated from every amplifier and edge. ── */

    public const string PostureFsync = "PG_POSTURE_FSYNC";
    public const string PostureFullPageWrites = "PG_POSTURE_FULL_PAGE_WRITES";
    public const string PostureSynchronousCommit = "PG_POSTURE_SYNCHRONOUS_COMMIT";

    /* ── Lane 9 — baselines and anomalies. ── */

    /// <summary>Instance CPU percent — Aurora / Performance Insights only; absent on stock.</summary>
    public const string CpuPercent = "PG_CPU_PERCENT";
    public const string AnomalyTps = "ANOMALY_PG_TPS";
    public const string AnomalySessionSpike = "ANOMALY_PG_SESSION_SPIKE";
    public const string AnomalyCpuSpike = "ANOMALY_PG_CPU_SPIKE";
    public const string AnomalyDeadlockRate = "ANOMALY_PG_DEADLOCK_RATE";
    public const string AnomalyWaitProfile = "ANOMALY_PG_WAIT_PROFILE";

    /* ── Lane 24 (#3691) — stock PostgreSQL's SAMPLED wait profile, the sibling of the Aurora one above. ── */

    /// <summary>The stock wait profile — the <c>pg_wait_sampling</c> ESTIMATE, rated per second the sampler was
    /// actually watching (<c>sampled_ms</c>, V133) — against its own baseline (<c>pg_sampled_wait_ms_per_sec</c>).
    /// Its OWN key, metric and bar, never <see cref="AnomalyWaitProfile"/>'s: a per-backend-sample count quantised
    /// at the sampling period has a different noise distribution from a measured microsecond sum, and a bucket that
    /// mixed the two would be the unit error #3689 §5 refused. Not a STATIC <see cref="AnomalyToFamilies"/> entry: it
    /// folds per story onto its dominant contributor through <see cref="WaitProfileFamilies"/>, exactly as the Aurora
    /// one does (<see cref="IsWaitProfileAnomaly"/> is what the reconciler and the scorer route on). Lane 24.</summary>
    public const string AnomalySampledWaitProfile = "ANOMALY_PG_SAMPLED_WAIT_PROFILE";

    /// <summary>Whether <paramref name="key"/> is one of the two wait-profile anomalies — the Aurora
    /// <see cref="AnomalyWaitProfile"/> or the stock <see cref="AnomalySampledWaitProfile"/>. The two share one
    /// SHAPE (a ratio / modified-z family with <c>contrib_Type:event</c> metadata, folding onto the dominant wait,
    /// the extremity escape on the same statistic) and differ in instrument, metric name and bar; the shared
    /// switches that route on the shape ask this predicate so a third profile instrument would join here and
    /// nowhere else.</summary>
    public static bool IsWaitProfileAnomaly(string? key) =>
        key is AnomalyWaitProfile or AnomalySampledWaitProfile;

    /* ── v2 (#3691) vocabulary, declared once by the v2 plumbing lane so lanes 11–16 reference constants and
       never edit this file. The comment beside each key names the lane that fills its collector / scorer /
       advice and the table it reads; a key declared for a later wave says so and has no stub. ── */

    /* Lane 11 — data-file I/O latency (design §3.9), from pg_io_stats (pg_stat_io, PostgreSQL 16+). */

    /// <summary>Mean data-file READ latency (ms per read) over the window, from the reset-aware
    /// <c>pg_io_stats</c> counter differences; absent below PostgreSQL 16. Lane 11.</summary>
    public const string IoReadLatencyMs = "PG_IO_READ_LATENCY_MS";
    /// <summary>Mean data-file WRITE latency (ms per write) over the window, the same read. Lane 11.</summary>
    public const string IoWriteLatencyMs = "PG_IO_WRITE_LATENCY_MS";
    /// <summary>Read latency against its own per-server baseline (<c>pg_io_read_latency</c>) — z-score shape,
    /// graded by the shared deviation ramp. Lane 11.</summary>
    public const string AnomalyIoLatency = "ANOMALY_PG_IO_LATENCY";

    /* Lane 12 — replication (design §3.10), from pg_replication_stats and pg_replication_slot_stats. */

    /// <summary>Replay lag on the standbys (bytes, from <c>pg_replication_stats</c>); the fact names the worst
    /// standby. Lane 12.</summary>
    public const string ReplicationLag = "PG_REPLICATION_LAG";
    /// <summary>WAL retained by an inactive or lagging replication slot (<c>pg_replication_slot_stats</c>) — the
    /// disk-fill path. Lane 12.</summary>
    public const string SlotRetention = "PG_SLOT_RETENTION";
    /// <summary>A slot's <c>xmin</c> / <c>catalog_xmin</c> holding back vacuum — the replication-side twin of
    /// <see cref="XminHold"/>, kept a separate key so the two causes are never conflated. Lane 12.</summary>
    public const string SlotXmin = "PG_SLOT_XMIN";
    /// <summary>Replay lag against its own baseline (<c>pg_replay_lag_bytes</c>) — z-score shape. Lane 12.</summary>
    public const string AnomalyReplicationLag = "ANOMALY_PG_REPLICATION_LAG";

    /* Lane 13 — bloat trends (design §3.12), from pg_table_bloat_stats (hourly) and pg_index_bloat (daily). */

    /// <summary>Table bloat GROWING across the window's hourly samples — a trend, never a point estimate; the
    /// fact states its own sample count. Lane 13.</summary>
    public const string BloatTrend = "PG_BLOAT_TREND";
    /// <summary>Index bloat growing across the daily samples, the same shape. Lane 13.</summary>
    public const string IndexBloatTrend = "PG_INDEX_BLOAT_TREND";

    /* Lane 15 — the WAL-volume detector behind the v1-declared WalVolumeShift fact (design §3.11). */

    /// <summary>WAL bytes per second against its own baseline (<c>pg_wal_bytes_per_sec</c>) — the detector
    /// behind <see cref="WalVolumeShift"/>, z-score shape; folds onto <see cref="CheckpointPressure"/>
    /// because WAL volume is the leading edge of checkpoint pressure (§3.11). Lane 15.</summary>
    public const string AnomalyWalVolume = "ANOMALY_PG_WAL_VOLUME";

    /* ── Wave 3 (#3691) vocabulary, declared by the between-waves batch so lane 17 references constants and never
       edits this file (the v2 plumbing's shape, #3715). Design §2a: active-query facts; blocking facts plus the
       BlockingChainReconstructor port. Every stub the family fills carries the "filled by lane 17" marker. ── */

    /* Lane 17 — blocking / active queries, from pg_blocking_edges, pg_log_events (lock_wait) and pg_session_states. */

    /// <summary>A blocking chain reconstructed from the window's sampled <c>pg_blocking_edges</c> — the root blocker
    /// attributed (pid, state, application, statement fingerprint), the sessions behind it, the depth; the fact
    /// names the root and states how many captures it was the root of, so one stuck session and a recurring pattern
    /// grade differently. A SAMPLE, never an event log (the collector's own caveat). Lane 17.</summary>
    public const string BlockingChain = "PG_BLOCKING_CHAIN";
    /// <summary>The <c>lock_wait</c> family of <c>pg_log_events</c> (#3601: <c>log_lock_waits</c>' "still waiting for
    /// … after N ms" lines) at EVENT grain — written by the engine, not sampled, so it sees what the chain sample
    /// between two captures cannot; a rate over observed time, with the statement fingerprint. Lane 17.</summary>
    public const string LockWaitEvents = "PG_LOCK_WAIT_EVENTS";
    /// <summary>An active statement running far longer than the window's norm for it (<c>pg_session_states</c>'s
    /// active rows by <c>query_start</c>), named by fingerprint — the active-query half of §2a, distinct from the
    /// idle holder <see cref="IdleInTransaction"/> names. Lane 17.</summary>
    public const string LongRunningQuery = "PG_LONG_RUNNING_QUERY";
    /// <summary>Blocked sessions per capture against their own baseline (<c>pg_blocked_sessions</c>) — z-score
    /// shape; folds onto <see cref="BlockingChain"/>. Lane 17.</summary>
    public const string AnomalyBlocking = "ANOMALY_PG_BLOCKING";

    /* ── v3 (#3691) vocabulary, declared by the v3 plumbing lane so lanes 27 / 28 / 29 / 30 / 32 reference constants
       and never edit this file (the v2 plumbing's shape, #3715; the wave-3 batch's, #3737). Design §6 (plans), the
       kernel-stats CPU decomposition and self-hosted CPU proxy, and §4b (composition checks). Every stub a family
       fills carries its "filled by lane N" marker. NOT declared, deliberately: a max_wal_size-vs-disk composition
       key — disk free is not collected, and a key nothing can measure is a lie waiting for a lane. Lane 29 (buffer
       composition) declares nothing either: it is a drill-down on BufferCachePressure over pg_buffer_usage, lane
       16's deadlock shape. ── */

    /* Lane 27 — plans, from pg_plan_capture × pg_statement_stats (regression), pg_column_stats (sensitivity); lane 30
       adds the Seq-Scan advisory from plan_json × pg_predicate_stats in the same family. */

    /// <summary>A statement whose per-call cost STEP-CHANGED across the window (<c>pg_statement_stats</c>' reset-aware
    /// mean-ms difference) at the same moment its captured <c>plan_hash</c> flipped (<c>pg_plan_capture</c>) — the
    /// two readings together are a plan regression; either alone is not. Named by <c>query_id</c> through the
    /// <c>ObjectName</c> seam; the advice must say <c>queryid</c> is not stable across major upgrades. Lane 27.</summary>
    public const string PlanRegression = "PG_PLAN_REGRESSION";
    /// <summary>A statement captured under SEVERAL <c>plan_hash</c> values in the window whose predicate columns show
    /// a skewed <c>top_value_frequency</c> in <c>pg_column_stats</c> — the PostgreSQL reading of parameter
    /// sensitivity (no plan cache to force, so the remedy is the statement's shape, never a forced plan). Lane 27.</summary>
    public const string ParameterSensitivity = "PG_PARAMETER_SENSITIVITY";
    /// <summary>A <c>plan_json</c> Seq Scan over a large relation under a selective predicate
    /// (<c>pg_predicate_stats</c>' evaluation count and selectivity beside it) — EVIDENCE ONLY: predicate, rows,
    /// selectivity, estimate error. Never a <c>CREATE INDEX</c> statement anywhere in the fact or its advice (D8,
    /// the standing no-missing-index-folklore rule; the maintainer's decision on #3691 gates the lane). Lane 30.</summary>
    public const string SeqScanAdvisory = "PG_SEQ_SCAN_ADVISORY";
    /// <summary>A statement's mean execution ms against its own baseline (<c>pg_statement_mean_ms</c>) — z-score
    /// shape; folds onto <see cref="PlanRegression"/>, the regular fact that names the plan flip behind the
    /// deviation. The detector is lane 27's.</summary>
    public const string AnomalyPlanRegression = "ANOMALY_PG_PLAN_REGRESSION";

    /* Lane 28 — kernel, from pg_kernel_stats (pg_stat_kcache; availability from pg_extension_availability). */

    /// <summary>User-plus-system CPU seconds per WALL second over the window — cores busy — from the reset-aware
    /// <c>pg_kernel_stats</c> differences summed across statements: the self-hosted CPU PROXY that stands where
    /// <see cref="CpuPercent"/> (Aurora's capacity percent) does not exist, and the second confirmer a stock
    /// target's load storm needs to cross the page line. Rated over <c>ObservedDurationMs</c>, never the nominal
    /// window. Lane 28.</summary>
    public const string CpuBurnCores = "PG_CPU_BURN_CORES";
    /// <summary>Burning versus waiting: the window's kernel CPU time against its measured wait time, so the operator
    /// learns whether the box is out of CPU or the backends are parked — the decomposition that decides whether a
    /// wait card or a CPU card is the story. A context reading beside <see cref="CpuBurnCores"/>. Lane 28.</summary>
    public const string CpuDecomposition = "PG_CPU_DECOMPOSITION";
    /// <summary>Cores busy against their own baseline (<c>pg_cpu_burn_cores</c>) — z-score shape; folds onto
    /// <see cref="CpuBurnCores"/>. Lane 28.</summary>
    public const string AnomalyCpuBurn = "ANOMALY_PG_CPU_BURN";

    /* Lane 32 — memory composition (§4b) and host memory, from pg_server_config × pg_cpu_utilization's V136 memory columns
       (memory_total_bytes, memory_free_bytes, memory_cached_bytes, memory_buffers_bytes, memory_active_bytes,
       configured_memory_bytes — lane R5; no separate table). */

    /// <summary>The §4b composition check: <c>shared_buffers + max_connections × work_mem</c> (plus
    /// <c>maintenance_work_mem × autovacuum_max_workers</c>) against the host's <c>memory_total_bytes</c> — a
    /// configuration that CAN exceed the box, stated with the values read from the facts. A CONFIG advisory: 0.4 on
    /// its own (it is in <see cref="ConfigAdvisoryRoots"/>), and ≥ 0.5 only when a workload co-fire —
    /// <see cref="HostMemoryPressure"/> or <see cref="TempSpill"/> — says the arithmetic is being felt (D5; the
    /// content lane decides which and pins it). Lane 32.</summary>
    public const string ConfigMemoryOvercommit = "CONFIG_PG_MEMORY_OVERCOMMIT";
    /// <summary>The host's free-plus-cached share of total memory over the window (<c>pg_cpu_utilization</c>'s
    /// <c>memory_free_bytes + memory_cached_bytes</c> over <c>memory_total_bytes</c>, V136; Aurora only,
    /// where the host metrics exist) — the measured half of the composition story, and the co-fire that lifts
    /// <see cref="ConfigMemoryOvercommit"/> past its advisory base. Lane 32.</summary>
    public const string HostMemoryPressure = "PG_HOST_MEMORY_PRESSURE";

    /* Lane 34 (#3691, ruled 2026-09-20) — the bad actor graded against its OWN normal. The one root edit the lane was
       granted: this key, its AnomalyToFamilies entry below, and the deviation-scored membership in PgTargetScorer.Anomaly.cs. */

    /// <summary>ONE statement's share of the window's execution time against that statement's OWN hour-of-week
    /// share baseline (<c>pg_statement_share</c>, keyed by <c>queryid</c> through lane 33's per-key seam) — the
    /// z-score shape on the #3653 PAIR gate (the window's peak per-collection share AND its mean must both clear the
    /// cutoff), emitted at most ONCE per pass for the candidate statement furthest beyond its own normal, its
    /// <c>queryid</c> on <see cref="Fact.ObjectName"/> (lane 27's string seam; a 64-bit id is not exact in a double).
    /// Folds onto the statement's <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> card through the <see cref="BadActorFamily"/>
    /// alias: the graph edge <c>ANOMALY_PG_BAD_ACTOR_SHARE → PG_BAD_ACTOR</c> resolves at story-build time to the
    /// statement the anomaly names, so the two are one story when the anomaly outranks and one INCIDENT (graph
    /// connectivity) when the card does. The card's own grade is a CONTEXT band since this lane; the deviation is
    /// the grade (Erik's ruling, 2026-09-20: own-baseline deviation, absolute share as context). Lane 34.</summary>
    public const string AnomalyBadActorShare = "ANOMALY_PG_BAD_ACTOR_SHARE";

    /* Wave 2 — declared so the name is settled; no stub, no lane in this wave. */

    /// <summary>The autovacuum worker profile changing shape against its baseline
    /// (<c>pg_autovacuum_workers</c>). Declared for wave 2; nothing emits, scores or composes it yet.</summary>
    public const string MaintenanceShapeShift = "PG_MAINTENANCE_SHAPE_SHIFT";

    /* ── Prefixes and predicates the shared switches route on. ── */

    public const string MeasuredPrefix = "PG_";
    public const string ConfigPrefix = "CONFIG_PG_";
    public const string AnomalyPrefix = "ANOMALY_PG_";

    /// <summary>Whether <paramref name="key"/> belongs to the PostgreSQL-target vocabulary — any of the
    /// three prefixes. Ordinal: the keys are declared upper-case and stamped from these constants.</summary>
    public static bool IsPgKey(string? key) =>
        key is not null
        && (key.StartsWith(MeasuredPrefix, StringComparison.Ordinal)
            || key.StartsWith(ConfigPrefix, StringComparison.Ordinal)
            || key.StartsWith(AnomalyPrefix, StringComparison.Ordinal));

    /// <summary>Whether <paramref name="key"/> is a PostgreSQL-target anomaly.</summary>
    public static bool IsPgAnomalyKey(string? key) =>
        key is not null && key.StartsWith(AnomalyPrefix, StringComparison.Ordinal);

    /// <summary>
    /// The wait fact key for a PostgreSQL wait: the type rollup when <paramref name="waitEvent"/> is null or
    /// empty (<c>PG_WAIT_LOCK</c>), the named standout otherwise (<c>PG_WAIT_LOCK_RELATION</c>). Upper-cased
    /// invariantly and every non-alphanumeric run collapsed to one underscore, because Aurora renamed wait
    /// events between majors while keeping their identity, and a key that differs only in case or in a
    /// colon would split one wait's occurrence history in two.
    /// </summary>
    public static string WaitKey(string waitType, string? waitEvent)
    {
        var builder = new StringBuilder(WaitKeyPrefix, 48);
        AppendNormalised(builder, waitType);
        if (!string.IsNullOrWhiteSpace(waitEvent))
        {
            builder.Append('_');
            AppendNormalised(builder, waitEvent);
        }

        return builder.ToString();
    }

    /// <summary>The per-statement bad-actor key, keyed on <c>pg_stat_statements.queryid</c> the way the SQL
    /// Server family keys on <c>query_hash</c>. The advice must say that <c>queryid</c> is not stable across
    /// major upgrades, so occurrence tracking restarts at one.</summary>
    public static string BadActorKey(long queryId) =>
        BadActorKeyPrefix + queryId.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// The <c>CONFIG_PG_*</c> and posture keys that root a standalone advisory card at ANY positive severity
    /// — the PostgreSQL arm of <c>InferenceEngine.ConfigAdvisoryRootKeys</c>, consulted through
    /// <see cref="IsConfigAdvisoryRoot"/>. Membership follows D5: a CONVENTION check (a knob at its shipped
    /// default, an extension missing, a durability setting off) may root a card on a quiet server; an
    /// EVIDENCE-gated check (<c>work_mem</c>, <c>maintenance_work_mem</c>) may not — it scores above zero
    /// only when its workload co-fire exists, and then the co-fire is the root. The context facts
    /// (<c>max_connections</c>, <c>superuser_reserved_connections</c>, <c>reserved_connections</c>) score 0 and
    /// are not here either.
    ///
    /// <para>Pre-declared here rather than left for the lanes so that four parallel content lanes do not
    /// each edit this one shared file. A lane whose design moves a key between the two classes edits this
    /// list in its own PR and says why.</para>
    /// </summary>
    public static readonly IReadOnlyList<string> ConfigAdvisoryRoots = new[]
    {
        ConfigSharedBuffers,
        ConfigMaxWalSize,
        ConfigEffectiveCacheSize,
        ConfigRandomPageCost,
        ConfigTrackIoTiming,
        ConfigCheckpointTimeout,
        ConfigWalCompression,
        ConfigStatStatementsMissing,
        ConfigAutovacuumOff,
        PostureFsync,
        PostureFullPageWrites,
        PostureSynchronousCommit,
        /* v3 (#3691) plumbing: the §4b composition check is a CONVENTION reading of five knobs against the host —
           it may root a card on a quiet server at the 0.4 advisory base, and only a workload co-fire lifts it (D5).
           Lane 32 fills the bar; membership is routing, decided here so the lane never edits this file. */
        ConfigMemoryOvercommit,
    };

    private static readonly HashSet<string> s_configAdvisoryRoots = new(ConfigAdvisoryRoots, StringComparer.Ordinal);

    /// <summary>Whether <paramref name="key"/> is in <see cref="ConfigAdvisoryRoots"/>.</summary>
    public static bool IsConfigAdvisoryRoot(string? key) =>
        key is not null && s_configAdvisoryRoots.Contains(key);

    /// <summary>
    /// The PostgreSQL arm of <c>AnomalyIncidentReconciler.AnomalyToFamilies</c>: which REGULAR fact family
    /// an <c>ANOMALY_PG_*</c> story folds into when a same-run parent exists, in priority order. An anomaly
    /// absent from this map stays a solo card — <see cref="AnomalyTps"/> has no regular twin (throughput is
    /// context, not a symptom) and neither wait profile (<see cref="AnomalyWaitProfile"/>, and since lane 24 the
    /// stock <see cref="AnomalySampledWaitProfile"/>) is a STATIC entry: each resolves per story from its
    /// dominant contributor through <see cref="WaitProfileFamilies"/>, routed by <see cref="IsWaitProfileAnomaly"/>.
    ///
    /// <para>v2 (#3691): <see cref="AnomalyIoLatency"/> folds onto the read-latency fact it deviates from;
    /// <see cref="AnomalyReplicationLag"/> onto the lag fact; <see cref="AnomalyWalVolume"/> onto
    /// <see cref="CheckpointPressure"/>, not onto <see cref="WalVolumeShift"/> — WAL volume is the LEADING EDGE
    /// of checkpoint pressure (design §3.11), so the incident the operator sees is the checkpoint one, with the
    /// volume shift as its early evidence rather than a second card. Wave 3: <see cref="AnomalyBlocking"/> folds
    /// onto <see cref="BlockingChain"/> — more sessions blocked than this hour usually sees is the statistical
    /// reading of the chain the regular fact names. v3: <see cref="AnomalyPlanRegression"/> folds onto
    /// <see cref="PlanRegression"/> (the deviation is the statistical reading of the plan flip the regular fact
    /// names) and <see cref="AnomalyCpuBurn"/> onto <see cref="CpuBurnCores"/>.</para>
    ///
    /// <para>Lane 34: <see cref="AnomalyBadActorShare"/> names the <see cref="BadActorFamily"/> ALIAS, not a fact's
    /// key — the family it belongs to has dynamic keys and the map is static. Stated plainly: the reconciler's fold
    /// looks a family up by the exact keys on a regular story's path, and no path carries the alias, so THIS entry
    /// documents the relationship and satisfies the census; it does not itself fold. What puts the anomaly and its
    /// statement's card into one incident is the graph — <c>PgTargetRelationshipGraph.Query.cs</c> declares the
    /// alias edge, <c>GetActiveEdges</c> resolves it to the statement the anomaly names, and
    /// <c>InferenceEngine.ClusterIntoIncidents</c> unions across that active edge whichever of the two rooted.
    /// A reconciler arm resolving the alias per story from the anomaly's <c>queryid</c> (as the wait profile resolves
    /// from its contributors) is the follow-up the lane filed; it needs the reconciler root.</para>
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string[]> AnomalyToFamilies =
        new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            [AnomalyDeadlockRate] = [DeadlockRate],
            [AnomalyCpuSpike] = [CpuPercent],
            [AnomalySessionSpike] = [ConnectionSaturation],
            [AnomalyIoLatency] = [IoReadLatencyMs],
            [AnomalyReplicationLag] = [ReplicationLag],
            [AnomalyWalVolume] = [CheckpointPressure],
            [AnomalyBlocking] = [BlockingChain],
            [AnomalyPlanRegression] = [PlanRegression],
            [AnomalyCpuBurn] = [CpuBurnCores],
            [AnomalyBadActorShare] = [BadActorFamily],
        };

    /// <summary>The metadata prefix the wait-profile detector stamps its top contributors under —
    /// <c>contrib_Type:event</c> (value = milliseconds), the SQL Server profile's <c>contrib_TYPE</c> shape with
    /// the event appended (<c>PgTargetAnomalyDetector</c>, lane 9).</summary>
    internal const string WaitContributorMetadataPrefix = "contrib_";

    /// <summary>
    /// The regular wait fact(s) a wait-profile anomaly story (<see cref="AnomalyWaitProfile"/>, or the sampled
    /// <see cref="AnomalySampledWaitProfile"/> — both stamp <c>contrib_Type:event</c>) folds into, in priority order, resolved
    /// per story from the anomaly's own metadata — the PostgreSQL arm of what
    /// <c>AnomalyIncidentReconciler.ResolveFamilies</c> does for the literal <c>ANOMALY_WAIT_PROFILE</c>
    /// (its dominant <c>contrib_TYPE</c> mapped through <c>WaitFamilyKey</c>). Here the dominant
    /// (largest-ms) <c>contrib_Type:event</c> entry names BOTH keys the wait family may have emitted for that
    /// wait: the named standout first (<c>PG_WAIT_LOCK_RELATION</c>), the type rollup second
    /// (<c>PG_WAIT_LOCK</c>) — because the wait scorer grades one wait once (a rollup scores 0 whenever its
    /// standout fired), so exactly one of the two can be the fired parent, and the reconciler folds into the
    /// first candidate that has one. A contributor without an event (<c>contrib_Lock</c>) yields the rollup
    /// alone. Ties break on the ordinal contributor name so the same metadata always resolves the same way;
    /// empty when the story carries no contributor metadata (the anomaly then stays a solo card, as before).
    ///
    /// <para>v1 residue closed by the v2 plumbing (#3691): in v1 the PostgreSQL profile had no entry in either
    /// map and every profile story stayed solo beside the very wait card it was about.</para>
    /// </summary>
    public static string[] WaitProfileFamilies(IReadOnlyDictionary<string, double>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return Array.Empty<string>();

        string? dominant = null;
        var dominantValue = double.NegativeInfinity;
        foreach (var (metaKey, value) in metadata)
        {
            if (!metaKey.StartsWith(WaitContributorMetadataPrefix, StringComparison.Ordinal))
                continue;

            var name = metaKey.Substring(WaitContributorMetadataPrefix.Length);
            if (name.Length == 0)
                continue;

            if (dominant is null
                || value > dominantValue
                || (value == dominantValue && string.CompareOrdinal(name, dominant) < 0))
            {
                dominantValue = value;
                dominant = name;
            }
        }

        if (dominant is null)
            return Array.Empty<string>();

        var colon = dominant.IndexOf(':');
        if (colon <= 0 || colon == dominant.Length - 1)
            return [WaitKey(colon <= 0 ? dominant : dominant[..colon], null)];

        var waitType = dominant[..colon];
        var waitEvent = dominant[(colon + 1)..];
        return [WaitKey(waitType, waitEvent), WaitKey(waitType, null)];
    }

    private static void AppendNormalised(StringBuilder builder, string text)
    {
        var pendingUnderscore = false;
        foreach (var c in text)
        {
            if (char.IsLetterOrDigit(c))
            {
                if (pendingUnderscore && builder.Length > WaitKeyPrefix.Length && builder[^1] != '_')
                    builder.Append('_');
                pendingUnderscore = false;
                builder.Append(char.ToUpperInvariant(c));
            }
            else
            {
                pendingUnderscore = true;
            }
        }
    }
}
