/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /* The client-timeout session's name lives in the shared builder (#5640) so the attention read and this lifecycle can
       never disagree on it. */
    private const string AttentionXeSessionName = AlwaysOnXeSessions.AttentionXeSessionName;

    /// <summary>
    /// Ensures the client-timeout XE session (the attention event, #5640) exists and is running. The session comes
    /// from the shared builder (<see cref="AlwaysOnXeSessions"/>), so there is no Lite copy of its DDL: the same statement
    /// Darling sends. Server-scoped for on-prem, Azure MI and AWS RDS; on Azure SQL DB a database-scoped session in EVERY
    /// monitored database through the shared always-on driver, as the deadlock and blocked-process sessions are (#1535, #4961).
    /// Skipped when the attention collector is disabled for the server. A server that refuses raises
    /// <see cref="XeSessionEnsureException"/> before the read, so a failed ensure never records a zero-row SUCCESS.
    /// </summary>
    public async Task EnsureAttentionXeSessionAsync(ServerConnection server, int engineEdition = 0, CancellationToken cancellationToken = default)
    {
        /* Skip if the attention collector is disabled */
        var schedule = _scheduleManager.GetScheduleForServer(server.Id, "attention");
        if (schedule == null || !schedule.Enabled)
        {
            return;
        }

        if (engineEdition == 5)
        {
            await EnsureDatabaseScopedXeSessionsAsync(
                server, "client timeout", AttentionXeSessionName,
                AlwaysOnArmEnsureNotUsed, cancellationToken,
                (databaseName, token) => EnsureAlwaysOnXeSessionInDatabaseAsync(server, AlwaysOnXeSessionKind.Attention, "client timeout", databaseName, token));
            return;
        }

        /* On-prem, Azure MI, and AWS RDS: create the server-scoped session */
        await EnsureServerScopedXeSessionAsync(
            server, "client timeout", AttentionXeSessionName,
            (connection, token) => EnsureAttentionXeSessionOnPremAsync(connection, server, token), cancellationToken);
    }

    /// <summary>
    /// On-prem / Azure MI / AWS RDS: starts the server-scoped client-timeout session when it is stopped, and creates and starts
    /// it when it is missing, with the statement the shared builder makes.
    /// </summary>
    private async Task EnsureAttentionXeSessionOnPremAsync(SqlConnection connection, ServerConnection server, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerProbeSql("PerformanceMonitorLite"), connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = AttentionXeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    /* Session exists but is stopped - start it */
                    try
                    {
                        using var startCmd = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerStartSql(), connection);
                        startCmd.CommandTimeout = CommandTimeoutSeconds;
                        await startCmd.ExecuteNonQueryAsync(cancellationToken);
                        AppLogger.Info("XeSession", $"[{server.DisplayName}] Started client timeout XE session");
                    }
                    catch (SqlException ex)
                    {
                        AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to start client timeout XE session: {ex.Message}");
                        throw;
                    }
                }
                else
                {
                    AppLogger.Debug("XeSession", $"Client timeout XE session is running on '{server.DisplayName}'");
                }
                return;
            }
        }

        try
        {
            using var createCmd = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerCreateSql(), connection);
            createCmd.CommandTimeout = CommandTimeoutSeconds;
            await createCmd.ExecuteNonQueryAsync(cancellationToken);
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started client timeout XE session");
        }
        catch (SqlException ex)
        {
            /* Warn rather than Error when the server simply said no: a denied XE session is a least-privilege
               posture (#1823), classified as PERMISSIONS upstream. Genuine failures still log at Error. */
            if (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
            {
                AppLogger.Warn("XeSession", $"[{server.DisplayName}] Failed to create client timeout XE session: {ex.Message}");
            }
            else
            {
                AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to create client timeout XE session: {ex.Message}");
            }
            throw;
        }
    }
}
