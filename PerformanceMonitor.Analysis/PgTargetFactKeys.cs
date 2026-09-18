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

    /// <summary>The prefix every PostgreSQL-target source carries; the shared scorer routes on it.</summary>
    public const string Prefix = "pg_";

    /// <summary>Every PostgreSQL-target source, sorted ordinally — the shape <see cref="FactScorer.KnownSources"/>
    /// keeps, so the two lists can be compared without re-sorting.</summary>
    public static readonly IReadOnlyList<string> All = new[]
    {
        BufferSource, ConfigSource, CpuSource, DatabaseSource, PostureSource, QueriesSource,
        SessionsSource, TempSource, VacuumSource, WaitsSource, WriteSource,
    };

    /// <summary>Whether <paramref name="source"/> is a PostgreSQL-target source.</summary>
    public static bool IsPgSource(string? source) =>
        source is not null && source.StartsWith(Prefix, StringComparison.Ordinal);
}

/// <summary>
/// The whole v1 PostgreSQL-target fact vocabulary (#3542 D2), declared once and BEFORE any content lane
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
    /// (<c>max_connections</c>, <c>superuser_reserved_connections</c>) score 0 and are not here either.
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
    /// context, not a symptom) and <see cref="AnomalyWaitProfile"/> resolves from its dominant contributor,
    /// which lane 5 / lane 9 own.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string[]> AnomalyToFamilies =
        new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            [AnomalyDeadlockRate] = [DeadlockRate],
            [AnomalyCpuSpike] = [CpuPercent],
            [AnomalySessionSpike] = [ConnectionSaturation],
        };

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
