/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Server-level facts an analyzer rule can use alongside a parsed plan (#4530/#4535/#4597). Ported from
/// erikdarlingdata/PerformanceStudio dev (85492a1) <c>src/PlanViewer.Core/Models/ServerMetadata.cs:5-37</c>.
/// <see cref="Database"/> is PS's <c>DatabaseMetadata</c> shape verbatim; PM's readers (#4597) fill the
/// members the store already carries for a database (name, compat level, collation, RCSI, stats options,
/// parameterization) and leave the rest at PS's defaults — see <see cref="DatabaseMetadata"/>.
/// </summary>
public class ServerMetadata
{
    // Instance
    public string? ServerName { get; set; }
    public string? ProductVersion { get; set; }
    public string? ProductLevel { get; set; }
    public string? Edition { get; set; }
    public bool IsAzure { get; set; }

    // Hardware
    public int CpuCount { get; set; }
    public long PhysicalMemoryMB { get; set; }

    /// <summary>
    /// <c>SERVERPROPERTY('EngineEdition')</c> of the stored properties row; null when it was not read. 5 is an Azure SQL
    /// Database, whose Hardware row names its vCores and no RAM (see <see cref="ServerContextCard"/>).
    /// </summary>
    public int? EngineEdition { get; set; }

    /// <summary>
    /// The vCores parsed from an Azure SQL Database's service objective; null for any other engine, and for an objective that
    /// names none (a DTU-model objective or an elastic pool).
    /// </summary>
    public int? VcoreCount { get; set; }

    // Instance settings
    public int MaxDop { get; set; }
    public int CostThresholdForParallelism { get; set; }
    public long MaxServerMemoryMB { get; set; }

    // Database-level (#4597): the plan's database when the entry point knows it. Null when the caller has
    // no database name (a pasted plan XML) or the store has no database_config row for it.
    public DatabaseMetadata? Database { get; set; }

    /// <summary>
    /// Whether sys.database_scoped_configurations is available (SQL 2016+ or Azure).
    /// </summary>
    public bool SupportsScopedConfigs =>
        IsAzure || (int.TryParse(ProductVersion?.Split('.')[0], out var major) && major >= 13);

    /// <summary>
    /// Whether sys.query_store_wait_stats is available (SQL 2017+ or Azure).
    /// </summary>
    public bool SupportsQueryStoreWaitStats =>
        IsAzure || (int.TryParse(ProductVersion?.Split('.')[0], out var major) && major >= 14);
}

/// <summary>
/// The plan's database. Ported from PS's <c>DatabaseMetadata</c> verbatim (names, members, defaults).
/// PM's readers (#4597) fill <c>Name</c>, <c>CompatibilityLevel</c>, <c>CollationName</c>,
/// <c>IsReadCommittedSnapshotOn</c>, <c>IsAutoCreateStatsOn</c>, <c>IsAutoUpdateStatsOn</c>,
/// <c>IsAutoUpdateStatsAsyncOn</c> and <c>IsParameterizationForced</c> from <c>database_config</c>, which
/// carries all of them with matching types. <c>SnapshotIsolationState</c> stays at PS's default 0: PS reads
/// it as the raw <c>sys.databases</c> tinyint, but PM's collector stores it as the engine's text
/// description (<c>OFF</c>/<c>ON</c>/<c>SNAPSHOT</c>), so filling it needs a name-to-code mapping this port
/// doesn't add. <c>NonDefaultScopedConfigs</c> stays empty: PM has no per-database
/// <c>database_scoped_configurations</c> store table today.
/// </summary>
public class DatabaseMetadata
{
    public string Name { get; set; } = "";
    public int CompatibilityLevel { get; set; }
    public string CollationName { get; set; } = "";

    // Isolation — notable if on
    public int SnapshotIsolationState { get; set; }
    public bool IsReadCommittedSnapshotOn { get; set; }

    // Stats — notable if off
    public bool IsAutoCreateStatsOn { get; set; }
    public bool IsAutoUpdateStatsOn { get; set; }

    // Stats async — notable if on
    public bool IsAutoUpdateStatsAsyncOn { get; set; }

    // Parameterization — notable if on
    public bool IsParameterizationForced { get; set; }

    // Database-scoped configs (2016+/Azure) — always empty in PM today (no store table).
    public System.Collections.Generic.List<ScopedConfigItem> NonDefaultScopedConfigs { get; set; } = new();
}

/// <summary>
/// One non-default <c>sys.database_scoped_configurations</c> row. Ported from PS verbatim; PM has no
/// store reader that fills this today (#4597), so <see cref="DatabaseMetadata.NonDefaultScopedConfigs"/>
/// is always empty.
/// </summary>
public class ScopedConfigItem
{
    public string Name { get; set; } = "";
    public string Value { get; set; } = "";
    public string? ValueForSecondary { get; set; }
}
