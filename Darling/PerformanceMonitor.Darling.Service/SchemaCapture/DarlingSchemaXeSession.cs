/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The lifecycle of the schema-changes session (#7643), over one connection: on premises the connection is to the server, on Azure
/// SQL Database to the database the session lives in. Probe, then start the session that exists or create and start the one that
/// does not. The statements come from <see cref="SchemaChangesXeSession"/>; failures are classified like the other captures'
/// (<see cref="DarlingXeSessions.CaptureFaultFor"/>) and handed back, never thrown, so a login that cannot create the session
/// leaves the pass a fault to record instead of a crash.
/// </summary>
public static class DarlingSchemaXeSession
{
    private const string CaptureName = "schema change";

    public static async Task<SchemaSessionState> EnsureAsync(
        DbConnection connection, string name, bool databaseScoped, ILogger? logger, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(connection);
        try
        {
            var state = await ProbeAsync(connection, name, databaseScoped, ct);
            if (!state.Exists)
            {
                await ExecuteTolerantAsync(connection, SchemaChangesXeSession.BuildCreateSql(name, databaseScoped), ct);
                logger?.LogInformation("Created the schema-changes XE session {Session}", name);
            }

            if (!state.Exists || !state.Running)
            {
                await ExecuteTolerantAsync(connection, SchemaChangesXeSession.BuildStartSql(name, databaseScoped), ct);
                logger?.LogInformation("Started the schema-changes XE session {Session}", name);
            }

            var after = await ProbeAsync(connection, name, databaseScoped, ct);
            return after.Running
                ? new SchemaSessionState(true, after.CreateTimeUtc, null, false)
                : new SchemaSessionState(false, null, "The schema-changes XE session did not run after it was started.", false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var fault = DarlingXeSessions.CaptureFaultFor(ex, CaptureName);
            logger?.LogWarning("The schema-changes XE session {Session} could not be ensured: {Message}", name, fault.Message);
            return new SchemaSessionState(false, null, fault.Message, fault.IsPermission);
        }
    }

    public static async Task DropAsync(DbConnection connection, string name, bool databaseScoped, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(connection);
        await using var command = connection.CreateCommand();
        command.CommandText = SchemaChangesXeSession.BuildDropSql(name, databaseScoped);
        command.CommandTimeout = 60;
        await command.ExecuteNonQueryAsync(ct);
    }

    private sealed record Probe(bool Exists, bool Running, DateTime? CreateTimeUtc);

    private static async Task<Probe> ProbeAsync(DbConnection connection, string name, bool databaseScoped, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = SchemaChangesXeSession.BuildStateProbeSql(databaseScoped);
        command.CommandTimeout = 60;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@session_name";
        parameter.DbType = DbType.String;
        parameter.Size = 128;
        parameter.Value = name;
        command.Parameters.Add(parameter);

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return new Probe(false, false, null);
        }

        var exists = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture) == 1;
        var running = Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture) == 1;
        DateTime? createTime = reader.IsDBNull(2)
            ? null
            : DateTime.SpecifyKind(Convert.ToDateTime(reader.GetValue(2), CultureInfo.InvariantCulture), DateTimeKind.Utc);
        return new Probe(exists, running, createTime);
    }

    /* #1251: on Azure SQL Database the catalogs the probe reads are visibility-scoped per principal, so a CREATE or START can answer
       "already there" when the probe saw nothing. Those two answers confirm the session is up, so they are success. */
    private static async Task ExecuteTolerantAsync(DbConnection connection, string sql, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 60;
        try
        {
            await command.ExecuteNonQueryAsync(ct);
        }
        catch (SqlException ex) when (DarlingXeSessions.IsBenignXeSessionAlreadyPresent(ex))
        {
            return;
        }
    }
}
