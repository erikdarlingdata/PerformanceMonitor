/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Which database engine a definition's query dialect targets, and which engine a monitored server
/// actually is. The gate that keeps the two from being crossed is
/// <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/> — a definition
/// only runs when its <see cref="ICollectorSchemaInfo.TargetEngine"/> equals the target's
/// <see cref="CollectorTargetInfo.Engine"/>.
/// <para>Both sides default to <see cref="SqlServer"/>, so every existing definition and every
/// existing target keep their present behaviour exactly: this enum adds a dimension, it does not
/// change a single dispatch decision until something opts into a different engine.</para>
/// <para>This is deliberately about SQL <em>dialect and catalog surface</em>, not about hosting.
/// Azure SQL DB, Managed Instance, and RDS for SQL Server are all <see cref="SqlServer"/> — their
/// differences are already carried as flags on <see cref="CollectorTargetInfo"/>, because they run
/// the same T-SQL against the same DMVs. A different value here means the query text itself would
/// not parse on the other engine.</para>
/// </summary>
public enum CollectorTargetEngine
{
    /// <summary>
    /// Microsoft SQL Server in any of its hosted shapes (box, Azure SQL DB, Azure SQL Managed
    /// Instance, AWS RDS for SQL Server). The default for definitions and targets alike.
    /// </summary>
    SqlServer = 0,

    /// <summary>
    /// PostgreSQL, including Amazon Aurora PostgreSQL. Definitions marked with this read
    /// <c>pg_stat_*</c> / <c>pg_catalog</c> surfaces and are never dispatched at a SQL Server target.
    /// </summary>
    PostgreSql = 1,
}
