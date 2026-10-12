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

    /* #5640 (L4): the servers whose client-timeout session this run of the app has already dropped and created again for another
       definition. A second mismatch on the same server is left alone, so a probe that cannot be satisfied never drops every cycle. */
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, bool> _attentionDefinitionReplaced = new();

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
    /// it when it is missing, with the statement the shared builder makes. A session of that name with another definition (an
    /// older definition of our own session, L4) is dropped and created again, once per run of the app.
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
                /* #5640 (L4): the name is ours, so another definition under it is an older one of our own session. Without the
                   event_counter the count is NULL on every read, and without the ring buffer the read returns zero rows as SUCCESS
                   forever. Dropped and created again once per run of the app, logged once; if the new session still reads as another
                   definition, it is left alone. */
                if (result is int mismatch && mismatch == AlwaysOnXeSessions.AttentionDefinitionMismatch)
                {
                    if (!_attentionDefinitionReplaced.TryAdd(server.Id, true))
                    {
                        AppLogger.Debug("XeSession", $"[{server.DisplayName}] The client timeout XE session still has another definition after it was replaced; left as it is");
                        return;
                    }

                    AppLogger.Warn("XeSession", $"[{server.DisplayName}] The client timeout XE session {AttentionXeSessionName} has a definition other than this version's (events, actions or targets): dropping it and creating it again");
                    using var dropCmd = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerDropSql(), connection);
                    dropCmd.CommandTimeout = CommandTimeoutSeconds;
                    await dropCmd.ExecuteNonQueryAsync(cancellationToken);
                }
                else
                {
                    if (result is int isRunning && isRunning == 0)
                    {
                        /* Session exists but is stopped - start it. A failure is not logged here: it propagates to the ensure's catch, which applies the #4964
                           log-once rule (Warn or Error on the first failing cycle, Debug after it). A line here would repeat at full level every cycle (#5640). */
                        using var startCmd = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerStartSql(), connection);
                        startCmd.CommandTimeout = CommandTimeoutSeconds;
                        await startCmd.ExecuteNonQueryAsync(cancellationToken);
                        AppLogger.Info("XeSession", $"[{server.DisplayName}] Started client timeout XE session");
                    }
                    else
                    {
                        AppLogger.Debug("XeSession", $"Client timeout XE session is running on '{server.DisplayName}'");
                    }
                    return;
                }
            }
        }

        /* No catch here: a failure propagates to the ensure's catch, which logs it once at Warn or Error and then at Debug (#4964). */
        using var createCmd = new SqlCommand(AlwaysOnXeSessions.BuildAttentionServerCreateSql(), connection);
        createCmd.CommandTimeout = CommandTimeoutSeconds;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started client timeout XE session");
    }
}
