/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /* We create and manage our own XE session to avoid conflicts with user's existing sessions */
    private const string DeadlockXeSessionName = "PerformanceMonitor_Deadlock";

    /// <summary>
    /// Ensures the deadlock XE session exists and is running.
    /// Creates a ring_buffer session for ALL platforms (on-prem, MI, Azure SQL DB, AWS RDS).
    /// Uses server-scoped session for on-prem/MI/RDS, database-scoped for Azure SQL DB.
    /// </summary>
    public async Task EnsureDeadlockXeSessionAsync(ServerConnection server, int engineEdition = 0, CancellationToken cancellationToken = default)
    {
        /* Skip if the deadlock collector is disabled */
        var schedule = _scheduleManager.GetSchedule("deadlocks");
        if (schedule == null || !schedule.Enabled)
        {
            return;
        }

        bool isAzureSqlDb = engineEdition == 5;

        try
        {
            using var connection = await CreateConnectionAsync(server, cancellationToken);

            if (isAzureSqlDb)
            {
                /* Azure SQL DB: create database-scoped session with ring_buffer */
                await EnsureDeadlockXeSessionAzureSqlDbAsync(connection, cancellationToken);
            }
            else
            {
                /* On-prem, Azure MI, and AWS RDS: create server-scoped session with ring_buffer */
                await EnsureDeadlockXeSessionOnPremAsync(connection, server, cancellationToken);
            }
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning("Failed to ensure deadlock XE session on '{Server}': {Message}",
                server.DisplayName, ex.Message);
            AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to ensure deadlock XE session: {ex.Message}");
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

SELECT
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue("@session_name", DeadlockXeSessionName);
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
                        _logger?.LogInformation("Started deadlock XE session on '{Server}'", server.DisplayName);
                        AppLogger.Info("XeSession", $"[{server.DisplayName}] Started deadlock XE session");
                    }
                    catch (SqlException ex)
                    {
                        _logger?.LogWarning("Failed to start deadlock XE session on '{Server}': {Message}",
                            server.DisplayName, ex.Message);
                        AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to start deadlock XE session: {ex.Message}");
                    }
                }
                else
                {
                    _logger?.LogDebug("Deadlock XE session is running on '{Server}'", server.DisplayName);
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
            _logger?.LogInformation("Created and started deadlock XE session on '{Server}'", server.DisplayName);
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started deadlock XE session");
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning("Failed to create deadlock XE session on '{Server}': {Message}",
                server.DisplayName, ex.Message);
            AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to create deadlock XE session: {ex.Message}");
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

SELECT
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
            cmd.Parameters.AddWithValue("@session_name", DeadlockXeSessionName);
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
                        AppLogger.Info("XeSession", $"[Azure SQL DB] Dropped deadlock XE session with incorrect event, will recreate");
                    }
                    catch (SqlException ex)
                    {
                        AppLogger.Error("XeSession", $"[Azure SQL DB] Failed to drop old deadlock XE session: {ex.Message}");
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

                    _logger?.LogDebug("Deadlock XE session already exists (database-scoped, Azure SQL DB)");
                    AppLogger.Info("XeSession", $"[Azure SQL DB] Deadlock XE session verified (database-scoped)");
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
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS
);

ALTER EVENT SESSION [{DeadlockXeSessionName}] ON DATABASE STATE = START;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger?.LogInformation("Created and started deadlock XE session (database-scoped, Azure SQL DB)");
        AppLogger.Info("XeSession", $"[Azure SQL DB] Created and started deadlock XE session (database-scoped)");
    }

    /// <summary>
    /// Collects deadlock information from the PerformanceMonitor_Deadlock extended event session.
    /// For on-prem/MI/RDS: reads from server-scoped session.
    /// For Azure SQL DB: reads from database-scoped session.
    /// </summary>
    private async Task<int> CollectDeadlocksAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus.SqlEngineEdition == 5;

        string query;
        if (isAzureSqlDb)
        {
            /* Azure SQL DB: read from ring_buffer (database-scoped session)
               Azure SQL DB uses database_xml_deadlock_report event instead of xml_deadlock_report.
               Use .query() to get XML with structure intact, then CONVERT to nvarchar(max) */
            query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @PerformanceMonitor_Deadlock TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @PerformanceMonitor_Deadlock
(
    ring_buffer
)
SELECT
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM sys.dm_xe_database_session_targets AS xet
JOIN sys.dm_xe_database_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{DeadlockXeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    deadlock_time = evt.value('(@timestamp)[1]', 'datetime2'),
    victim_process_id = evt.value('(data[@name=""xml_report""]/value/deadlock/victim-list/victimProcess/@id)[1]', 'varchar(50)'),
    deadlock_graph_xml = CONVERT(nvarchar(max), evt.query('data[@name=""xml_report""]/value/deadlock'))
FROM
(
    SELECT
        pmd.ring_buffer
    FROM @PerformanceMonitor_Deadlock AS pmd
) AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""database_xml_deadlock_report""]') AS q(evt)
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
OPTION(RECOMPILE);";
        }
        else
        {
            /* On-prem / Azure MI / AWS RDS: read from ring_buffer (server-scoped session)
               Use .query() to get XML with structure intact, then CONVERT to nvarchar(max) */
            query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @PerformanceMonitor_Deadlock TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @PerformanceMonitor_Deadlock
(
    ring_buffer
)
SELECT
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{DeadlockXeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    deadlock_time = evt.value('(@timestamp)[1]', 'datetime2'),
    victim_process_id = evt.value('(data[@name=""xml_report""]/value/deadlock/victim-list/victimProcess/@id)[1]', 'varchar(50)'),
    deadlock_graph_xml = CONVERT(nvarchar(max), evt.query('data[@name=""xml_report""]/value/deadlock'))
FROM
(
    SELECT
        pmd.ring_buffer
    FROM @PerformanceMonitor_Deadlock AS pmd
) AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""xml_deadlock_report""]') AS q(evt)
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
OPTION(RECOMPILE);";
        }

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        /* Query the most recent deadlock_time we already have for this server.
           Pass it to SQL Server so we only fetch events newer than what we've collected.
           This prevents the same deadlock from being inserted multiple times as it
           lingers in the ring buffer across collection cycles. */
        DateTime? lastCollectedTime = null;
        try
        {
            using var duckConn = _duckDb.CreateConnection();
            await duckConn.OpenAsync(cancellationToken);
            using var cmd = duckConn.CreateCommand();
            cmd.CommandText = "SELECT MAX(deadlock_time) FROM deadlocks WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
                lastCollectedTime = dt;
        }
        catch
        {
            /* If DuckDB query fails, fall back to default 10-minute window */
        }

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        /* Use the most recent timestamp from DuckDB as the cutoff, or fall back to 10-minute window */
        command.Parameters.AddWithValue("@cutoff_time",
            lastCollectedTime ?? DateTime.UtcNow.AddMinutes(-10));

        try
        {
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            sqlSw.Stop();
            _lastSqlMs = sqlSw.ElapsedMilliseconds;

            var duckSw = Stopwatch.StartNew();
            using var duckConnection = _duckDb.CreateConnection();
            await duckConnection.OpenAsync(cancellationToken);

            using var appender = duckConnection.CreateAppender("deadlocks");

            while (await reader.ReadAsync(cancellationToken))
            {
                var victimProcessId = reader.IsDBNull(1) ? null : reader.GetString(1);
                var graphXml = reader.IsDBNull(2) ? null : reader.GetString(2);
                var victimSqlText = ExtractVictimSqlText(graphXml, victimProcessId);

                var row = appender.CreateRow();
                row.AppendValue(GenerateCollectionId())
                   .AppendValue(collectionTime)
                   .AppendValue(serverId)
                   .AppendValue(server.ServerName)
                   .AppendValue(reader.IsDBNull(0) ? (DateTime?)null : reader.GetDateTime(0))
                   .AppendValue(victimProcessId)
                   .AppendValue(victimSqlText)
                   .AppendValue(graphXml)
                   .EndRow();

                rowsCollected++;
            }
            duckSw.Stop();
            _lastDuckDbMs = duckSw.ElapsedMilliseconds;
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Number == 15151 || ex.Message.Contains("XE session"))
        {
            /* XE session not found or not accessible */
            _logger?.LogDebug("Deadlock XE session not available on '{Server}': {Message}",
                server.DisplayName, ex.Message);
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Deadlock XE session not available: {ex.Message}");
            return 0;
        }

        _logger?.LogDebug("Collected {RowCount} deadlocks for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }

    /// <summary>
    /// Extracts victim SQL text from a deadlock graph XML fragment.
    /// </summary>
    private static string? ExtractVictimSqlText(string? graphXml, string? victimProcessId)
    {
        if (string.IsNullOrEmpty(graphXml))
        {
            return null;
        }

        try
        {
            var doc = XElement.Parse(graphXml);

            /* Find all process nodes in the deadlock graph */
            var processes = doc.Descendants("process").ToList();

            /* If we have a victim ID, find that specific process */
            if (!string.IsNullOrEmpty(victimProcessId))
            {
                var victim = processes.FirstOrDefault(p =>
                    string.Equals(p.Attribute("id")?.Value, victimProcessId, StringComparison.OrdinalIgnoreCase));

                if (victim != null)
                {
                    return victim.Element("inputbuf")?.Value?.Trim();
                }
            }

            /* Fallback: return the first process inputbuf */
            return processes.FirstOrDefault()?.Element("inputbuf")?.Value?.Trim();
        }
        catch
        {
            return null;
        }
    }
}
