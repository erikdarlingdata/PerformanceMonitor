/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The SQL Server implementation of <see cref="ITargetProvider"/>. This is a lift of the connection,
/// command, and parameter-mapping code that was inline in <c>DarlingCollectorRunner</c> — the
/// behaviour is deliberately unchanged, including the throw on an unmapped parameter type.
/// <para><see cref="Classify"/> reproduces the error numbers the runner's catch filters already use,
/// so the two cannot disagree. It does not replace those filters in this change; it exists so the
/// same decisions are expressible for a non-SQL-Server target.</para>
/// </summary>
public sealed class SqlServerTargetProvider : ITargetProvider
{
    public static readonly SqlServerTargetProvider Instance = new();

    public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

    public DbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);

    public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds)
    {
        if (connection is not SqlConnection sqlConnection)
        {
            throw new ArgumentException(
                $"SQL Server provider requires a SqlConnection, got {connection?.GetType().Name ?? "null"}",
                nameof(connection));
        }

        var command = new SqlCommand(query.Text, sqlConnection) { CommandTimeout = commandTimeoutSeconds };

        foreach (var parameter in query.Parameters)
        {
            command.Parameters.Add(ToSqlParameter(parameter));
        }

        return command;
    }

    /// <summary>
    /// Maps a collector parameter to its SQL Server type. Throws rather than defaulting on an
    /// unmapped type: a silently wrong parameter type yields a wrong result set, which is worse than
    /// a failure.
    /// </summary>
    private static SqlParameter ToSqlParameter(CollectorParameter parameter) => parameter.Type switch
    {
        CollectorParameterType.DateTime2 => new SqlParameter(parameter.Name, SqlDbType.DateTime2) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.NVarChar128 => new SqlParameter(parameter.Name, SqlDbType.NVarChar, 128) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.NVarChar260 => new SqlParameter(parameter.Name, SqlDbType.NVarChar, 260) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.Int32 => new SqlParameter(parameter.Name, SqlDbType.Int) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.BigInt => new SqlParameter(parameter.Name, SqlDbType.BigInt) { Value = parameter.Value ?? DBNull.Value },
        _ => throw new ArgumentOutOfRangeException(nameof(parameter), parameter.Type, "Unmapped collector parameter type"),
    };

    /// <summary>
    /// The error numbers below are the ones the runner and worker already branch on.
    /// <para><see cref="CollectorTargetFault.SessionMissing"/> is deliberately NOT produced here.
    /// Whether a 297 means "the XE session is gone" or "permission denied" depends on which collector
    /// asked and how the worker wrapped the call — the worker raises its own exception type for the XE
    /// case before any classification happens, and that type is private to it. Keeping that decision
    /// in the worker is correct: it is collector context, not engine semantics.</para>
    /// </summary>
    public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout)
    {
        if (exception is not SqlException sql)
        {
            return CollectorTargetFault.Unclassified;
        }

        /* Class 20+ is a fatal, connection-level error; -2 is a command timeout. Both force the
           caller to drop and re-probe the connection rather than just failing one collector. */
        if (sql.Class >= 20)
        {
            return CollectorTargetFault.ConnectionFatal;
        }

        if (sql.Number == -2)
        {
            return CollectorTargetFault.CommandTimeout;
        }

        /* 1222 is a lock-request timeout. It is a YIELD only for a collector that deliberately set a
           short LOCK_TIMEOUT; from any other collector it is a genuine error. */
        if (sql.Number == 1222)
        {
            return yieldsOnLockTimeout ? CollectorTargetFault.LockTimeoutYield : CollectorTargetFault.Unclassified;
        }

        /* #2512: the numbers live in SqlServerPermissionErrors now, so this and the two runner catch
           filters cannot disagree about what a denial is — they already had (916 was here and in
           neither of them). */
        if (SqlServerPermissionErrors.IsPermissionDenied(sql.Number))
        {
            return CollectorTargetFault.Permissions;
        }

        if (sql.Number == 208)
        {
            return CollectorTargetFault.ObjectMissing;
        }

        return CollectorTargetFault.Unclassified;
    }

    public string WithDatabase(string connectionString, string databaseName)
        => new SqlConnectionStringBuilder(connectionString) { InitialCatalog = databaseName }.ConnectionString;

    /// <summary>
    /// Enumeration runs against <c>master</c>, which is why the plan carries its own connection string:
    /// on an Azure SQL DB logical server the configured entry points at one user database, and
    /// <c>sys.databases</c> there lists only itself.
    /// <para><c>database_id &gt; 0</c> drops the resource database and <c>state_desc = 'ONLINE'</c> drops
    /// anything a per-database connection would fail on anyway (restoring, offline, recovery pending).
    /// This is the query Lite has always used, kept identical so both editions fan out over the same
    /// set — a difference here would show up as one edition silently monitoring fewer databases.</para>
    /// </summary>
    public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
        string connectionString, IReadOnlyList<string>? excludedDatabases)
    {
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(excludedDatabases, "name");

        return (
            WithDatabase(connectionString, "master"),
            new CollectorQuery(
                $"SELECT name FROM sys.databases WHERE state_desc = N'ONLINE' AND database_id > 0 {exclusionClause} ORDER BY name;",
                exclusionParameters));
    }
}
