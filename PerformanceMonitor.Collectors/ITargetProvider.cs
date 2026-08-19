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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Everything engine-specific about *executing* a collector: opening a connection, building a
/// command with the collector's parameters, and naming a driver exception. One implementation per
/// <see cref="CollectorTargetEngine"/>.
/// <para>This interface deliberately lives in the dependency-free collector library and speaks only
/// <see cref="System.Data.Common"/>, so that library keeps its zero-PackageReference property. The
/// implementations live where the drivers already are (the Darling service references both
/// Microsoft.Data.SqlClient and Npgsql), which is why nothing here mentions either.</para>
/// <para>Collector definitions themselves need no changes to work across engines — they already
/// return query text plus parameters and read through <see cref="DbDataReader"/>. What was missing
/// was this: a way for the runner to obtain a connection and a command without naming a provider.</para>
/// </summary>
public interface ITargetProvider
{
    /// <summary>Which engine this provider talks to.</summary>
    CollectorTargetEngine Engine { get; }

    /// <summary>
    /// Opens nothing — just constructs the connection. The caller owns it and is responsible for
    /// <c>OpenAsync</c> and disposal, matching how the runner already works.
    /// </summary>
    DbConnection CreateConnection(string connectionString);

    /// <summary>
    /// Builds a command for one collector query, mapping each <see cref="CollectorParameter"/> to the
    /// provider's own parameter type. A provider MUST throw on a parameter type it cannot map rather
    /// than silently sending a default — a wrong parameter type is a wrong result set, which is worse
    /// than a failure.
    /// </summary>
    DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds);

    /// <summary>
    /// Names a driver exception in engine-neutral terms. Returns
    /// <see cref="CollectorTargetFault.Unclassified"/> for anything not recognized, so an unexpected
    /// failure stays loud.
    /// <para><paramref name="yieldsOnLockTimeout"/> is passed in rather than inferred because whether a
    /// lock timeout is a yield or an error is a property of the COLLECTOR, not of the engine: only a
    /// definition that deliberately sets a short lock timeout may treat one as a yield.</para>
    /// </summary>
    CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout);

    /// <summary>
    /// Rewrites <paramref name="connectionString"/> to point at a different database on the same
    /// server, leaving every other setting alone.
    /// <para>This is what makes per-database fan-out possible on PostgreSQL at all. SQL Server has two
    /// ways to reach another database — switch the connection's catalog, or stay put and prefix the
    /// query (<c>EXECUTE [db].sys.sp_executesql</c>) — and the shared runner uses the first. PostgreSQL
    /// has only the first: a connection is bound to one database for its lifetime and there is no
    /// cross-database query at all. So the one shape both engines support is the one exposed here.</para>
    /// </summary>
    string WithDatabase(string connectionString, string databaseName);

    /// <summary>
    /// Where to ask for the fan-out database list, and what to ask. Returns the connection string the
    /// enumeration should run on plus the query to run — one decision, because the two are coupled: SQL
    /// Server reads <c>sys.databases</c> from <c>master</c>, while PostgreSQL reads <c>pg_database</c>,
    /// which is a shared catalog readable from whichever database is already connected.
    /// <para>What to do when enumeration FAILS is deliberately not here. On Azure SQL DB an
    /// inaccessible master has a meaningful fallback (collect from the one connected database) and a
    /// re-probe throttle; on PostgreSQL a login that cannot read <c>pg_database</c> cannot monitor the
    /// server at all, so there is nothing to fall back to. That policy stays with the runner.</para>
    /// </summary>
    (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
        string connectionString, IReadOnlyList<string>? excludedDatabases);
}
