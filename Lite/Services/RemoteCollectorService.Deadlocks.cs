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
    /* The session name lives in the shared definition so the ring-buffer reader and this
       lifecycle code can never disagree on it. */
    private const string DeadlockXeSessionName = DeadlocksCollector.XeSessionName;

    /// <summary>
    /// Ensures the deadlock XE session exists and is running.
    /// Creates a ring_buffer session for ALL platforms (on-prem, MI, Azure SQL DB, AWS RDS).
    /// Server-scoped session for on-prem/MI/RDS; on Azure SQL DB a database-scoped session in
    /// EVERY monitored database (#1535 — a single session only ever captured the connection's own
    /// database, so deadlocks in the logical server's other databases never appeared).
    /// </summary>
    public async Task EnsureDeadlockXeSessionAsync(ServerConnection server, int engineEdition = 0, CancellationToken cancellationToken = default)
    {
        /* Skip if the deadlock collector is disabled */
        var schedule = _scheduleManager.GetScheduleForServer(server.Id, "deadlocks");
        if (schedule == null || !schedule.Enabled)
        {
            return;
        }

        if (engineEdition == 5)
        {
            /* Azure SQL DB: one database-scoped session per monitored database, matching the
               per-database ring-buffer read (DeadlocksCollector.RunsPerDatabase). The shared driver
               skips master, honors ExcludedDatabases via the shared database list, self-heals
               sessions the reader can't see, and only surfaces unhealthy when NO database could be
               ensured. */
            await EnsureDatabaseScopedXeSessionsAsync(
                server, "deadlock", DeadlockXeSessionName,
                EnsureDeadlockXeSessionAzureSqlDbAsync, cancellationToken);
            return;
        }

        try
        {
            using var connection = await CreateConnectionAsync(server, cancellationToken);

            /* On-prem, Azure MI, and AWS RDS: create server-scoped session with ring_buffer */
            await EnsureDeadlockXeSessionOnPremAsync(connection, server, cancellationToken);
        }
        catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
        {
            /* Session already present + running -- see IsBenignXeSessionAlreadyPresent (#1251). */
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Deadlock XE session already present (benign, #1251)");
        }
        catch (SqlException ex)
        {
            /* Warn rather than Error when the server simply said no: a denied XE session is a least-privilege
               posture (#1823), classified as PERMISSIONS upstream and retried no further this session.
               Genuine failures still log at Error. */
            if (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
            {
                AppLogger.Warn("XeSession", $"[{server.DisplayName}] Failed to ensure deadlock XE session: {ex.Message}");
            }
            else
            {
                AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to ensure deadlock XE session: {ex.Message}");
            }

            /* Propagate so RunCollectorAsync marks the collector unhealthy instead
               of letting a zero-row ring-buffer read record SUCCESS (#1086) */
            throw new XeSessionEnsureException("deadlock", ex);
        }
    }

    /// <summary>
    /// On-prem / Azure MI / AWS RDS: creates or ensures server-scoped XE session with ring_buffer target.
    /// </summary>
    private async Task EnsureDeadlockXeSessionOnPremAsync(SqlConnection connection, ServerConnection server, CancellationToken cancellationToken)
    {
        /* Check if our XE session already exists */
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = DeadlockXeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    /* Session exists but is stopped - start it */
                    try
                    {
                        using var startCmd = new SqlCommand(
                            $"ALTER EVENT SESSION [{DeadlockXeSessionName}] ON SERVER STATE = START;", connection);
                        startCmd.CommandTimeout = CommandTimeoutSeconds;
                        await startCmd.ExecuteNonQueryAsync(cancellationToken);
                        AppLogger.Info("XeSession", $"[{server.DisplayName}] Started deadlock XE session");
                    }
                    catch (SqlException ex)
                    {
                        AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to start deadlock XE session: {ex.Message}");
                        throw;
                    }
                }
                else
                {
                    AppLogger.Debug("XeSession", $"Deadlock XE session is running on '{server.DisplayName}'");
                }
                return;
            }
        }

        /* Create and start server-scoped session with ring_buffer
           Using MEMORY_PARTITION_MODE = NONE for AWS RDS compatibility */
        try
        {
            using var createCmd = new SqlCommand($@"
CREATE EVENT SESSION [{DeadlockXeSessionName}]
ON SERVER
ADD EVENT sqlserver.xml_deadlock_report
ADD TARGET package0.ring_buffer
(
    SET max_memory = 4096
)
WITH
(
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    MEMORY_PARTITION_MODE = NONE,
    STARTUP_STATE = ON
);

ALTER EVENT SESSION [{DeadlockXeSessionName}] ON SERVER STATE = START;", connection);
            createCmd.CommandTimeout = CommandTimeoutSeconds;
            await createCmd.ExecuteNonQueryAsync(cancellationToken);
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started deadlock XE session");
        }
        catch (SqlException ex)
        {
            /* Warn rather than Error when the server simply said no: a denied XE session is a least-privilege
               posture (#1823), classified as PERMISSIONS upstream and retried no further this session.
               Genuine failures still log at Error. */
            if (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
            {
                AppLogger.Warn("XeSession", $"[{server.DisplayName}] Failed to create deadlock XE session: {ex.Message}");
            }
            else
            {
                AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to create deadlock XE session: {ex.Message}");
            }
            throw;
        }
    }

    /// <summary>
    /// Azure SQL DB: creates database-scoped XE session with ring_buffer target.
    /// File targets are not supported in Azure SQL DB.
    /// </summary>
    private async Task EnsureDeadlockXeSessionAzureSqlDbAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        /* Check if database-scoped session already exists and uses the correct event */
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    has_correct_event = CASE
        WHEN EXISTS
        (
            SELECT 1/0
            FROM sys.database_event_session_events AS dese
            JOIN sys.database_event_sessions AS des
              ON des.event_session_id = dese.event_session_id
            WHERE des.name = @session_name
            AND   dese.name = N'database_xml_deadlock_report'
        )
        THEN 1
        WHEN EXISTS
        (
            SELECT 1/0
            FROM sys.database_event_sessions AS des
            WHERE des.name = @session_name
        )
        THEN 0
        ELSE NULL
    END;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = DeadlockXeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result is int hasCorrectEvent)
            {
                if (hasCorrectEvent == 0)
                {
                    /* Session exists but uses wrong event (xml_deadlock_report instead of database_xml_deadlock_report).
                       Drop it so we can recreate with the correct event. */
                    try
                    {
                        using var dropCmd = new SqlCommand(
                            $"DROP EVENT SESSION [{DeadlockXeSessionName}] ON DATABASE;", connection);
                        dropCmd.CommandTimeout = CommandTimeoutSeconds;
                        await dropCmd.ExecuteNonQueryAsync(cancellationToken);
                        AppLogger.Info("XeSession", $"[Azure SQL DB:{connection.Database}] Dropped deadlock XE session with incorrect event, will recreate");
                    }
                    catch (SqlException ex)
                    {
                        AppLogger.Error("XeSession", $"[Azure SQL DB:{connection.Database}] Failed to drop old deadlock XE session: {ex.Message}");
                    }
                    /* Fall through to create with correct event */
                }
                else
                {
                    /* Session exists with correct event - ensure it's started */
                    using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{DeadlockXeSessionName}'
)
BEGIN
    ALTER EVENT SESSION [{DeadlockXeSessionName}] ON DATABASE STATE = START;
END;", connection);
                    startCmd.CommandTimeout = CommandTimeoutSeconds;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);

                    /* Debug, not Info: this fires once per monitored database per cycle (#1535). */
                    AppLogger.Debug("XeSession", $"[Azure SQL DB:{connection.Database}] Deadlock XE session verified (database-scoped)");
                    return;
                }
            }
        }

        /* Create and start database-scoped session.
           Azure SQL DB uses database_xml_deadlock_report instead of xml_deadlock_report. */
        using (var cmd = new SqlCommand($@"
CREATE EVENT SESSION [{DeadlockXeSessionName}]
ON DATABASE
ADD EVENT sqlserver.database_xml_deadlock_report
ADD TARGET package0.ring_buffer
(
    SET max_memory = 4096
)
WITH
(
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    STARTUP_STATE = ON
);

ALTER EVENT SESSION [{DeadlockXeSessionName}] ON DATABASE STATE = START;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        AppLogger.Info("XeSession", $"[Azure SQL DB:{connection.Database}] Created and started deadlock XE session (database-scoped)");
    }

    /// <summary>
    /// Collects deadlocks via the shared <see cref="DeadlocksCollector"/> definition (the
    /// server- vs database-scoped ring-buffer reads, the deadlock_time watermark, and the
    /// victim-inputbuf extraction live there — the cross-SKU parity contract). The XE session
    /// lifecycle stays here; a missing/inaccessible session is tolerated as zero rows, exactly
    /// as before.
    /// </summary>
    private async Task<int> CollectDeadlocksAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            return await RunCollectorDefinitionAsync(DeadlocksCollector.Instance, server, cancellationToken);
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Number == 15151 || ex.Message.Contains("XE session"))
        {
            /* XE session not found or not accessible */
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Deadlock XE session not available: {ex.Message}");
            return 0;
        }
    }
}
