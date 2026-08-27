/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What a definition may need to know about the monitored server to build its query.
/// Grown deliberately as the sweep demands (engine edition today; version gates arrive with
/// the collectors that need them) — every added field is parity-critical target logic.
/// <para><b>A field a SQL Server <see cref="AppliesTo"/> gate reads must also be swept by
/// <see cref="CollectorEngineCapability.TargetsWithEngineEdition"/>.</b> That sweep is how
/// "can this collector EVER run on this engine edition" is derived, and a fact it never varies
/// sits at its CLR default in every shape it produces — so a gate written on it matches none of
/// them and the derivation announces a permanent, unfixable engine gap for a collector that runs
/// perfectly well. Adding a field that nothing gates on is fine; adding the GATE without extending
/// the sweep is what breaks, and <c>EveryFactASqlServerGateReads_IsVariedBySweepOrFixedByEdition</c>
/// fails the build when it happens rather than leaving it to be noticed (#2518).</para>
/// <para><b>The same rule holds one engine over</b> (#2532): a field a PostgreSQL
/// <see cref="AppliesTo"/> gate reads must be swept by
/// <see cref="CollectorEngineCapability.TargetsWithEngineKind"/>, or fixed by the engine kind the
/// way <see cref="IsAurora"/> is. <c>EveryFactAPostgresGateReads_IsVariedBySweepOrFixedByKind</c> is
/// the twin guard.</para>
/// </summary>
public sealed class CollectorTargetInfo
{
    /// <summary>
    /// Which database engine this target actually is. Defaults to
    /// <see cref="CollectorTargetEngine.SqlServer"/>, so every target the probes classify today —
    /// and every bare <c>new CollectorTargetInfo()</c> in a test — keeps its present behaviour.
    /// <para>A definition is only dispatched when its <see cref="ICollectorSchemaInfo.TargetEngine"/>
    /// matches this; see <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>.
    /// The SQL Server hosting flags below (<see cref="IsAzureSqlDb"/> and friends) are meaningful
    /// only when this is <see cref="CollectorTargetEngine.SqlServer"/>.</para>
    /// </summary>
    public CollectorTargetEngine Engine { get; init; } = CollectorTargetEngine.SqlServer;

    /// <summary>True when the target is Azure SQL Database (engine edition 5).</summary>
    public bool IsAzureSqlDb { get; init; }

    /// <summary>True when the target is Azure SQL Managed Instance (engine edition 8).</summary>
    public bool IsAzureManagedInstance { get; init; }

    /// <summary>
    /// True when the target is an Amazon RDS for SQL Server instance (detected via
    /// <c>DB_ID('rdsadmin') IS NOT NULL</c>). RDS does not expose the underlying OS, so DMVs that
    /// read OS/service state — notably <c>sys.dm_server_services</c> (used by agent_status) — and the
    /// restricted msdb surface running_jobs needs (<c>msdb.dbo.syssessions</c>) are unavailable there.
    /// Definitions gate those collectors off via <see cref="AppliesTo"/> so both hosts skip them.
    /// </summary>
    public bool IsAwsRds { get; init; }

    /// <summary>
    /// SQL Server major version (13 = 2016 … 17 = 2025); 0 when unknown. Definitions gate
    /// version-specific columns on this (database_config treats 0 as "assume newest" to match
    /// the original collector).
    /// </summary>
    public int SqlMajorVersion { get; init; }

    /// <summary>
    /// True when the monitored login can read msdb (<c>HAS_DBACCESS('msdb') = 1</c>), as probed at
    /// connect by both hosts (Lite's ServerManager and Darling's DarlingServerConnector, verbatim
    /// <c>HAS_DBACCESS(N'msdb')</c>).
    ///
    /// <para><b>This is reported, not dispatched on (#2559).</b> The three SQL-Agent collectors —
    /// running_jobs, job_history, agent_status — used to gate off via <see cref="AppliesTo"/> when it was
    /// false. The trouble is that msdb access is a GRANT rather than an engine capability, and this value
    /// is probed once and cached for the connection's life. So the sequence a user actually follows was
    /// broken: read our advice, run the <c>GRANT</c>, and nothing happens until the service restarts,
    /// with no indication why.</para>
    ///
    /// <para>The collectors now attempt and fail into <c>PERMISSIONS</c>, which is a first-class outcome
    /// rather than a defect: error 916 is already in <see cref="SqlServerPermissionErrors"/>, so the run
    /// is classified as a permission denial and never as an ERROR, and <c>CollectorHealthClassifier</c>
    /// bands a collector that has only ever been denied as <c>NO_PERMISSIONS</c> — checked BEFORE
    /// FAILING/STALE, so a server that will never have the grant does not read as broken and raises no
    /// alert. The cost is three fast-failing statements per cycle on such a server, which is a compile-time
    /// permission check with no execution behind it, and the grant now takes effect on the next cycle
    /// instead of the next restart.</para>
    ///
    /// <para>Defaults to <c>true</c>, still matching the probe's NULL-means-assume-access default. It no
    /// longer changes dispatch, but it remains the honest value to report on a connection surface.</para>
    /// </summary>
    public bool HasMsdbAccess { get; init; } = true;

    /* ---- PostgreSQL facts. Meaningful only when Engine is PostgreSql; the SQL Server flags above
       are correspondingly meaningless on a Postgres target. Kept flat alongside them for now
       because there are few; if this list grows much further it wants its own sub-object rather
       than more parallel properties. ---- */

    /// <summary>
    /// PostgreSQL major version (16, 17); 0 when unknown. Derived from
    /// <c>server_version_num / 10000</c> rather than parsing <c>version()</c> text, whose formatting
    /// has changed across releases.
    /// <para>Definitions gate on this for the real 16→17 breaks: <c>pg_stat_bgwriter</c> loses five
    /// columns to <c>pg_stat_checkpointer</c> and deletes two, <c>pg_stat_statements</c> renames
    /// <c>blk_*_time</c> to <c>shared_blk_*_time</c>, and <c>pg_stat_progress_vacuum</c> renames two
    /// columns. A fleet spanning both majors hits all of them.</para>
    /// </summary>
    public int PostgresMajorVersion { get; init; }

    /// <summary>
    /// The full <c>server_version_num</c> (e.g. 160011, 170007), for the minor-version gates a major
    /// alone cannot express — <c>aurora_stat_resource_usage()</c> needs Aurora 16.9+/17.5+ and is
    /// absent on 17.4, so a major-only check would call a function that is not there.
    /// </summary>
    public int PostgresVersionNum { get; init; }

    /// <summary>
    /// True when the target is Amazon Aurora PostgreSQL, detected by the presence of
    /// <c>aurora_version</c> in <c>pg_proc</c>.
    /// <para>This gates a large proprietary surface that stock PostgreSQL does not have at all, most
    /// importantly <c>aurora_stat_system_waits()</c> — cumulative wait counters, which core PostgreSQL
    /// simply does not provide in any version.</para>
    /// </summary>
    public bool IsAurora { get; init; }

    /// <summary>
    /// True when the target is in recovery, i.e. a read replica (<c>pg_is_in_recovery()</c>).
    /// <para>Not a routing hint: on Aurora every reader is a separate instance with its own
    /// statistics — its own <c>pg_stat_statements</c> contents and its own wait profile — so a reader
    /// is a distinct monitoring identity worth collecting from, not a shadow of the writer. Some
    /// surfaces are writer-only and gate off this.</para>
    /// </summary>
    public bool IsInRecovery { get; init; }
}
