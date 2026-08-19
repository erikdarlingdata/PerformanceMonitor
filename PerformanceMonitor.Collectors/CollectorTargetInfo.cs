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
    /// True when the monitored login can read msdb (<c>HAS_DBACCESS('msdb') = 1</c>). The SQL-Agent
    /// collectors — running_jobs, job_history, agent_status — read <c>msdb.dbo.sysjobs</c>,
    /// <c>sysjobhistory</c>, <c>sysjobschedules</c>, etc., so each gates off via <see cref="AppliesTo"/>
    /// when this is false; a login without msdb access would otherwise fail every cycle (error 229/916)
    /// and pollute collection-health. Both hosts probe this (Lite's ServerManager and Darling's
    /// DarlingServerConnector, verbatim <c>HAS_DBACCESS(N'msdb')</c>) and wire it in here.
    /// <para>Defaults to <c>true</c> so a target the probe never classified (the SqlMajorVersion == 0 /
    /// unknown path, and every bare <c>new CollectorTargetInfo()</c>) still attempts the Agent
    /// collectors — matching the probe's own NULL-means-assume-access default, so "unknown" never
    /// silently gates collection off.</para>
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
