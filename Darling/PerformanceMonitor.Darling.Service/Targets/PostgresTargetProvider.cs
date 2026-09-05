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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The PostgreSQL implementation of <see cref="ITargetProvider"/>, covering Amazon Aurora PostgreSQL.
/// Npgsql is already a dependency of this project — it is the store driver — so monitoring a Postgres
/// target needs no new package, only this.
/// <para>Classification is by SQLSTATE, not by message text. PostgreSQL's SQLSTATEs are stable across
/// versions and locales; its error messages are neither.</para>
/// </summary>
public sealed class PostgresTargetProvider : ITargetProvider
{
    public static readonly PostgresTargetProvider Instance = new();

    public CollectorTargetEngine Engine => CollectorTargetEngine.PostgreSql;

    public DbConnection CreateConnection(string connectionString) => new NpgsqlConnection(connectionString);

    public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds)
    {
        if (connection is not NpgsqlConnection npgsqlConnection)
        {
            throw new ArgumentException(
                $"PostgreSQL provider requires an NpgsqlConnection, got {connection?.GetType().Name ?? "null"}",
                nameof(connection));
        }

        var command = new NpgsqlCommand(query.Text, npgsqlConnection) { CommandTimeout = commandTimeoutSeconds };

        foreach (var parameter in query.Parameters)
        {
            command.Parameters.Add(ToNpgsqlParameter(parameter));
        }

        return command;
    }

    /// <summary>
    /// Maps a collector parameter to its PostgreSQL type.
    /// <para>Two deliberate choices. First, the <c>NVarChar128</c>/<c>NVarChar260</c> lengths are
    /// dropped: those exist to match SQL Server column widths, and PostgreSQL's <c>text</c> has no
    /// length to declare — carrying a bogus length would imply a constraint the engine does not
    /// have. Second, <c>DateTime2</c> maps to <c>timestamp</c> WITHOUT time zone, matching the store's
    /// own naive-UTC convention; mapping it to <c>timestamptz</c> would make Npgsql reject a
    /// <c>DateTimeKind.Unspecified</c> value, and every timestamp in this product is Unspecified.</para>
    /// </summary>
    private static NpgsqlParameter ToNpgsqlParameter(CollectorParameter parameter) => parameter.Type switch
    {
        CollectorParameterType.DateTime2 => new NpgsqlParameter(parameter.Name, NpgsqlDbType.Timestamp) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.NVarChar128 => new NpgsqlParameter(parameter.Name, NpgsqlDbType.Text) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.NVarChar260 => new NpgsqlParameter(parameter.Name, NpgsqlDbType.Text) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.Int32 => new NpgsqlParameter(parameter.Name, NpgsqlDbType.Integer) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.BigInt => new NpgsqlParameter(parameter.Name, NpgsqlDbType.Bigint) { Value = parameter.Value ?? DBNull.Value },
        _ => throw new ArgumentOutOfRangeException(nameof(parameter), parameter.Type, "Unmapped collector parameter type"),
    };

    /// <summary>
    /// SQLSTATE-based classification. Every code here was observed on our own Aurora fleet while
    /// probing which monitoring sources are readable, not taken from documentation:
    /// <list type="bullet">
    /// <item>42501 — <c>pg_monitor</c> lacks the grant (e.g. <c>aurora_stat_logical_wal_cache()</c>,
    /// which needs <c>rds_replication</c>).</item>
    /// <item>42P01 / 42883 — the relation or function is not there, which is how a version-gated
    /// feature or an uncreated extension presents (<c>pg_stat_statements</c> in a database where the
    /// view was never created; <c>apg_plan_mgmt.dba_plans</c> without the extension).</item>
    /// <item>0A000 — <c>feature_not_supported</c>. Aurora returns this for <c>pg_stat_wal</c>.</item>
    /// <item>55000 / 55P03 — the object is not in the right state, or a lock could not be taken.
    /// Aurora raises a 55-class error for <c>aurora_stat_optimized_reads_cache()</c> when the feature
    /// is disabled, which a naive collector logs as a failure every single cycle.</item>
    /// <item>57014 — <c>query_canceled</c>, which is what <c>statement_timeout</c> produces.</item>
    /// <item>08* — connection exceptions; 57P01/57P02/57P03 — shutdown and unavailability.</item>
    /// </list>
    /// </summary>
    public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout)
    {
        /* Unwrapped OR wrapped. Npgsql surfaces a command timeout as an NpgsqlException whose INNER
           exception is the TimeoutException — the bare shape is what a test constructs, not what the driver
           throws. Checking only the outer type sent every real command timeout down the NpgsqlException arm
           below and classified it ConnectionFatal, so a slow query forced a reconnect: precisely the
           reconnect storm the SQLSTATE arm is careful to avoid. */
        if (exception is TimeoutException
            || exception.InnerException is TimeoutException
            || (exception is NpgsqlException && exception.GetBaseException() is TimeoutException))
        {
            return CollectorTargetFault.CommandTimeout;
        }

        if (exception is not PostgresException pg)
        {
            /* A connection-level Npgsql failure that never reached the server carries no SQLSTATE. */
            return exception is NpgsqlException ? CollectorTargetFault.ConnectionFatal : CollectorTargetFault.Unclassified;
        }

        var state = pg.SqlState ?? string.Empty;

        if (state.StartsWith("08", StringComparison.Ordinal)
            || state is "57P01" or "57P02" or "57P03")
        {
            return CollectorTargetFault.ConnectionFatal;
        }

        return state switch
        {
            "42501" => CollectorTargetFault.Permissions,
            "42P01" or "42883" => CollectorTargetFault.ObjectMissing,
            "0A000" => CollectorTargetFault.FeatureDisabled,
            "55000" or "55006" => CollectorTargetFault.FeatureDisabled,
            "55P03" => yieldsOnLockTimeout ? CollectorTargetFault.LockTimeoutYield : CollectorTargetFault.Unclassified,
            "57014" => CollectorTargetFault.CommandTimeout,
            _ => CollectorTargetFault.Unclassified,
        };
    }

    public string WithDatabase(string connectionString, string databaseName)
        => new NpgsqlConnectionStringBuilder(connectionString) { Database = databaseName }.ConnectionString;

    /// <summary>
    /// Enumerates from wherever the service is already connected — <c>pg_database</c> is a shared
    /// catalog, so unlike SQL Server there is no equivalent of hopping to <c>master</c> first.
    /// <para>All three filters are load-bearing, and each screens a different kind of database that
    /// cannot be collected from. <c>datistemplate</c> excludes <c>template0</c> and <c>template1</c>;
    /// <c>template0</c> in particular is frozen and rejects connections outright, so including it would
    /// guarantee one failed connection per collection cycle forever. <c>datallowconn</c> excludes any
    /// database an administrator has deliberately closed — most often one mid-restore or being retired,
    /// exactly the databases where an extra connection attempt is least welcome.</para>
    /// <para><c>rdsadmin</c> is the managed-platform maintenance database, and it needs its own screen
    /// because the other two cannot see it: RDS leaves <c>datallowconn = true</c> and
    /// <c>datistemplate = false</c> on it, so it enumerates like a user database, and then
    /// <c>pg_hba.conf</c> rejects the connection (SQLSTATE 28000) for every principal a customer can
    /// hold. Excluded by name rather than behind a managed-mode gate, matching the SQL Server screen
    /// (#1565), which lists the vendor management databases unconditionally on the same reasoning: the
    /// names are vendor-controlled, so they cannot collide with real customer data, and no target flag
    /// has to be threaded down here to be trusted. Self-hosted PostgreSQL has no such database, so the
    /// filter removes nothing there.</para>
    /// <para>A name literal, not a parameter, precisely because it is not operator input — the
    /// <c>excludedDatabases</c> parameters carry configured names, and keeping this out of that set
    /// leaves an empty exclusion list producing no parameters at all.</para>
    /// </summary>
    public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
        string connectionString, IReadOnlyList<string>? excludedDatabases)
    {
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(excludedDatabases, "datname");

        return (connectionString, new CollectorQuery(
            $@"
SELECT datname
FROM pg_database
WHERE datallowconn
AND   NOT datistemplate
AND   datname <> '{ManagedMaintenanceDatabase}'
{exclusionClause}
ORDER BY datname",
            exclusionParameters));
    }

    /// <summary>
    /// The managed-platform maintenance database screened out of per-database fan-out.
    /// <para>Named rather than inlined so the exclusion is assertable by identity: a test that retypes
    /// the string proves the transcription, not the screen. Deliberately one name and not a list — the
    /// field evidence (#2995) is that the failure is always exactly this database, and the other
    /// providers' equivalents (<c>cloudsqladmin</c>, <c>azure_maintenance</c>) have never been measured
    /// on a monitored target, so adding them would be excluding databases nobody has seen.</para>
    /// </summary>
    internal const string ManagedMaintenanceDatabase = "rdsadmin";
}
