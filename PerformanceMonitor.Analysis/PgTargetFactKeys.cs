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

    /// <summary>The prefix every PostgreSQL-target source carries; the shared scorer routes on it.</summary>
    public const string Prefix = "pg_";

    /// <summary>Every PostgreSQL-target source, sorted ordinally — the shape <see cref="FactScorer.KnownSources"/>
    /// keeps, so the two lists can be compared without re-sorting.</summary>
    public static readonly IReadOnlyList<string> All = new[]
    {
        BloatSource, BufferSource, ConfigSource, CpuSource, DatabaseSource, IoSource, PostureSource, QueriesSource,
        ReplicationSource, SessionsSource, TempSource, VacuumSource, WaitsSource, WriteSource,
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
    /// <summary>v2 fact; declared for the write-chain edge into <see cref="CheckpointPressure"/>.</summary>
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
    };

    private static readonly HashSet<string> s_configAdvisoryRoots = new(ConfigAdvisoryRoots, StringComparer.Ordinal);

    /// <summary>Whether <paramref name="key"/> is in <see cref="ConfigAdvisoryRoots"/>.</summary>
    public static bool IsConfigAdvisoryRoot(string? key) =>
        key is not null && s_configAdvisoryRoots.Contains(key);

    /// <summary>
    /// The PostgreSQL arm of <c>AnomalyIncidentReconciler.AnomalyToFamilies</c>: which REGULAR fact family
    /// an <c>ANOMALY_PG_*</c> story folds into when a same-run parent exists, in priority order. An anomaly
    /// absent from this map stays a solo card — <see cref="AnomalyTps"/> has no regular twin (throughput is
    /// context, not a symptom) and <see cref="AnomalyWaitProfile"/> is not a STATIC entry: it resolves per
    /// story from its dominant contributor through <see cref="WaitProfileFamilies"/>.
    ///
    /// <para>v2 (#3691): <see cref="AnomalyIoLatency"/> folds onto the read-latency fact it deviates from;
    /// <see cref="AnomalyReplicationLag"/> onto the lag fact; <see cref="AnomalyWalVolume"/> onto
    /// <see cref="CheckpointPressure"/>, not onto <see cref="WalVolumeShift"/> — WAL volume is the LEADING EDGE
    /// of checkpoint pressure (design §3.11), so the incident the operator sees is the checkpoint one, with the
    /// volume shift as its early evidence rather than a second card.</para>
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
        };

    /// <summary>The metadata prefix the wait-profile detector stamps its top contributors under —
    /// <c>contrib_Type:event</c> (value = milliseconds), the SQL Server profile's <c>contrib_TYPE</c> shape with
    /// the event appended (<c>PgTargetAnomalyDetector</c>, lane 9).</summary>
    internal const string WaitContributorMetadataPrefix = "contrib_";

    /// <summary>
    /// The regular wait fact(s) an <see cref="AnomalyWaitProfile"/> story folds into, in priority order, resolved
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
