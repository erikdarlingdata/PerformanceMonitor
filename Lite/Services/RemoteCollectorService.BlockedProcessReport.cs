/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
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
    private const string BlockedProcessXeSessionName = "PerformanceMonitor_BlockedProcess";

    /// <summary>
    /// Ensures the blocked process XE session exists and is running.
    /// Creates a ring_buffer session for ALL platforms (on-prem, MI, Azure SQL DB).
    /// Uses server-scoped session for on-prem/MI, database-scoped for Azure SQL DB.
    /// </summary>
    public async Task EnsureBlockedProcessXeSessionAsync(ServerConnection server, int engineEdition = 0, CancellationToken cancellationToken = default)
    {
        /* Skip if the blocked_process_report collector is disabled */
        var schedule = _scheduleManager.GetSchedule("blocked_process_report");
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
                await EnsureBlockedProcessXeSessionAzureSqlDbAsync(connection, cancellationToken);
            }
            else
            {
                /* On-prem and Azure MI: create server-scoped session with ring_buffer */
                await EnsureBlockedProcessXeSessionOnPremAsync(connection, server, cancellationToken);
            }
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning("Failed to ensure blocked process XE session on '{Server}': {Message}",
                server.DisplayName, ex.Message);
            AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to ensure blocked process XE session: {ex.Message}");
        }
    }

    /// <summary>
    /// On-prem / Azure MI / AWS RDS: creates or ensures server-scoped XE session with ring_buffer target.
    /// Also ensures the blocked process threshold is configured (skipped on RDS where sp_configure is not available).
    /// </summary>
    private async Task EnsureBlockedProcessXeSessionOnPremAsync(SqlConnection connection, ServerConnection server, CancellationToken cancellationToken)
    {
        /* Check blocked process threshold and configure if needed.
           Wrapped in try/catch because sp_configure is not available on AWS RDS
           (threshold must be set via RDS parameter groups instead). */
        try
        {
            using var thresholdCmd = new SqlCommand(@"
DECLARE
    @threshold integer;

SELECT
    @threshold = CONVERT(integer, c.value_in_use)
FROM sys.configurations AS c
WHERE c.name = N'blocked process threshold (s)';

IF @threshold = 0
BEGIN
    EXECUTE sys.sp_configure
        N'show advanced options',
        1;

    RECONFIGURE;

    EXECUTE sys.sp_configure
        N'blocked process threshold (s)',
        5;

    RECONFIGURE;
END;

SELECT @threshold;", connection);
            thresholdCmd.CommandTimeout = CommandTimeoutSeconds;
            var result = await thresholdCmd.ExecuteScalarAsync(cancellationToken);
            var threshold = result as int? ?? 0;

            if (threshold == 0)
            {
                _logger?.LogInformation("Configured blocked process threshold to 5 seconds on '{Server}'", server.DisplayName);
                AppLogger.Info("XeSession", $"[{server.DisplayName}] Configured blocked process threshold to 5 seconds");
            }
        }
        catch (SqlException ex)
        {
            /* sp_configure not available (e.g. AWS RDS) — threshold must be set via platform config */
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Cannot set blocked process threshold via sp_configure (may require platform config): {ex.Message}");
        }

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
            cmd.Parameters.AddWithValue("@session_name", BlockedProcessXeSessionName);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    /* Session exists but is stopped - start it */
                    try
                    {
                        using var startCmd = new SqlCommand(
                            $"ALTER EVENT SESSION [{BlockedProcessXeSessionName}] ON SERVER STATE = START;", connection);
                        startCmd.CommandTimeout = CommandTimeoutSeconds;
                        await startCmd.ExecuteNonQueryAsync(cancellationToken);
                        _logger?.LogInformation("Started blocked process XE session on '{Server}'", server.DisplayName);
                        AppLogger.Info("XeSession", $"[{server.DisplayName}] Started blocked process XE session");
                    }
                    catch (SqlException ex)
                    {
                        _logger?.LogWarning("Failed to start blocked process XE session on '{Server}': {Message}",
                            server.DisplayName, ex.Message);
                        AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to start blocked process XE session: {ex.Message}");
                    }
                }
                else
                {
                    _logger?.LogDebug("Blocked process XE session is running on '{Server}'", server.DisplayName);
                }
                return;
            }
        }

        /* Create and start server-scoped session with ring_buffer */
        try
        {
            using var createCmd = new SqlCommand($@"
CREATE EVENT SESSION [{BlockedProcessXeSessionName}]
ON SERVER
ADD EVENT sqlserver.blocked_process_report
ADD TARGET package0.ring_buffer
(
    SET max_memory = 4096
)
WITH
(
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    STARTUP_STATE = ON
);

ALTER EVENT SESSION [{BlockedProcessXeSessionName}] ON SERVER STATE = START;", connection);
            createCmd.CommandTimeout = CommandTimeoutSeconds;
            await createCmd.ExecuteNonQueryAsync(cancellationToken);
            _logger?.LogInformation("Created and started blocked process XE session on '{Server}'", server.DisplayName);
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started blocked process XE session");
        }
        catch (SqlException ex)
        {
            _logger?.LogWarning("Failed to create blocked process XE session on '{Server}': {Message}",
                server.DisplayName, ex.Message);
            AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to create blocked process XE session: {ex.Message}");
        }
    }

    /// <summary>
    /// Azure SQL DB: creates database-scoped XE session with ring_buffer target.
    /// File targets are not supported in Azure SQL DB.
    /// </summary>
    private async Task EnsureBlockedProcessXeSessionAzureSqlDbAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        /* Check if database-scoped session already exists */
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.AddWithValue("@session_name", BlockedProcessXeSessionName);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                /* Session exists - ensure it's started (database-scoped sessions can stop on reconnect) */
                using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{BlockedProcessXeSessionName}'
)
BEGIN
    ALTER EVENT SESSION [{BlockedProcessXeSessionName}] ON DATABASE STATE = START;
END;", connection);
                startCmd.CommandTimeout = CommandTimeoutSeconds;
                await startCmd.ExecuteNonQueryAsync(cancellationToken);

                _logger?.LogDebug("Blocked process XE session already exists (database-scoped, Azure SQL DB)");
                AppLogger.Info("XeSession", $"[Azure SQL DB] Blocked process XE session verified (database-scoped)");
                return;
            }
        }

        /* Create and start database-scoped session */
        using (var cmd = new SqlCommand($@"
CREATE EVENT SESSION [{BlockedProcessXeSessionName}]
ON DATABASE
ADD EVENT sqlserver.blocked_process_report
ADD TARGET package0.ring_buffer
(
    SET max_memory = 4096
)
WITH
(
    MAX_DISPATCH_LATENCY = 5 SECONDS
);

ALTER EVENT SESSION [{BlockedProcessXeSessionName}] ON DATABASE STATE = START;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger?.LogInformation("Created and started blocked process XE session (database-scoped, Azure SQL DB)");
        AppLogger.Info("XeSession", $"[Azure SQL DB] Created and started blocked process XE session (database-scoped)");
    }

    /// <summary>
    /// Collects blocked process reports from the XE session ring_buffer.
    /// For on-prem/MI: reads from server-scoped session.
    /// For Azure SQL DB: reads from database-scoped session.
    /// </summary>
    private async Task<int> CollectBlockedProcessReportsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        bool isAzureSqlDb = serverStatus.SqlEngineEdition == 5;

        string query;
        if (isAzureSqlDb)
        {
            /* Azure SQL DB: read from ring_buffer (database-scoped session)
               Use .query() to get XML with structure intact, then CONVERT to nvarchar(max) */
            query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @PerformanceMonitor_BlockedProcess TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @PerformanceMonitor_BlockedProcess
(
    ring_buffer
)
SELECT
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM sys.dm_xe_database_session_targets AS xet
JOIN sys.dm_xe_database_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{BlockedProcessXeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    event_time = evt.value('(@timestamp)[1]', 'datetime2'),
    blocked_process_report_xml = CONVERT(nvarchar(max), evt.query('data[@name=""blocked_process""]/value/blocked-process-report'))
FROM
(
    SELECT
        pmd.ring_buffer
    FROM @PerformanceMonitor_BlockedProcess AS pmd
) AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""blocked_process_report""]') AS q(evt)
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
OPTION(RECOMPILE);";
        }
        else
        {
            /* On-prem / Azure MI: read from ring_buffer (server-scoped session)
               Use .query() to get XML with structure intact, then CONVERT to nvarchar(max) */
            query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @PerformanceMonitor_BlockedProcess TABLE
(
    ring_buffer xml NOT NULL
);

INSERT
    @PerformanceMonitor_BlockedProcess
(
    ring_buffer
)
SELECT
    ring_xml = TRY_CAST(xet.target_data AS xml)
FROM sys.dm_xe_session_targets AS xet
JOIN sys.dm_xe_sessions AS xes
  ON xes.address = xet.event_session_address
WHERE xes.name = N'{BlockedProcessXeSessionName}'
AND   xet.target_name = N'ring_buffer'
OPTION(RECOMPILE);

SELECT
    event_time = evt.value('(@timestamp)[1]', 'datetime2'),
    blocked_process_report_xml = CONVERT(nvarchar(max), evt.query('data[@name=""blocked_process""]/value/blocked-process-report'))
FROM
(
    SELECT
        pmd.ring_buffer
    FROM @PerformanceMonitor_BlockedProcess AS pmd
) AS rb
CROSS APPLY rb.ring_buffer.nodes('RingBufferTarget/event[@name=""blocked_process_report""]') AS q(evt)
WHERE evt.value('(@timestamp)[1]', 'datetime2') > @cutoff_time
OPTION(RECOMPILE);";
        }

        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        /* Query the most recent event_time we already have for this server.
           Pass it to SQL Server so we only fetch events newer than what we've collected.
           This prevents the same blocked process report from being inserted multiple times
           as it lingers in the ring buffer across collection cycles. */
        DateTime? lastCollectedTime = null;
        try
        {
            using var duckConn = _duckDb.CreateConnection();
            await duckConn.OpenAsync(cancellationToken);
            using var cmd = duckConn.CreateCommand();
            cmd.CommandText = "SELECT MAX(event_time) FROM blocked_process_reports WHERE server_id = $1";
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

            using var appender = duckConnection.CreateAppender("blocked_process_reports");

            while (await reader.ReadAsync(cancellationToken))
            {
                var eventTime = reader.IsDBNull(0) ? (DateTime?)null : reader.GetDateTime(0);
                var reportXml = reader.IsDBNull(1) ? null : reader.GetString(1);

                if (string.IsNullOrEmpty(reportXml))
                {
                    continue;
                }

                /* Parse the blocked process report XML in C# */
                var parsed = ParseBlockedProcessReportXml(reportXml, eventTime);
                if (parsed == null)
                {
                    continue;
                }

                var row = appender.CreateRow();
                row.AppendValue(GenerateCollectionId())
                   .AppendValue(collectionTime)
                   .AppendValue(serverId)
                   .AppendValue(server.ServerName)
                   .AppendValue(parsed.EventTime)
                   .AppendValue(parsed.DatabaseName)
                   .AppendValue(parsed.BlockedSpid)
                   .AppendValue(parsed.BlockedEcid)
                   .AppendValue(parsed.BlockingSpid)
                   .AppendValue(parsed.BlockingEcid)
                   .AppendValue(parsed.WaitTimeMs)
                   .AppendValue(parsed.WaitResource)
                   .AppendValue(parsed.LockMode)
                   .AppendValue(parsed.BlockedStatus)
                   .AppendValue(parsed.BlockedIsolationLevel)
                   .AppendValue(parsed.BlockedLogUsed)
                   .AppendValue(parsed.BlockedTransactionCount)
                   .AppendValue(parsed.BlockedClientApp)
                   .AppendValue(parsed.BlockedHostName)
                   .AppendValue(parsed.BlockedLoginName)
                   .AppendValue(parsed.BlockedSqlText)
                   .AppendValue(parsed.BlockingStatus)
                   .AppendValue(parsed.BlockingIsolationLevel)
                   .AppendValue(parsed.BlockingClientApp)
                   .AppendValue(parsed.BlockingHostName)
                   .AppendValue(parsed.BlockingLoginName)
                   .AppendValue(parsed.BlockingSqlText)
                   .AppendValue(parsed.BlockedTransactionName)
                   .AppendValue(parsed.BlockingTransactionName)
                   .AppendValue(parsed.BlockedLastTranStarted)
                   .AppendValue(parsed.BlockingLastTranStarted)
                   .AppendValue(parsed.BlockedLastBatchStarted)
                   .AppendValue(parsed.BlockingLastBatchStarted)
                   .AppendValue(parsed.BlockedLastBatchCompleted)
                   .AppendValue(parsed.BlockingLastBatchCompleted)
                   .AppendValue(parsed.BlockedPriority)
                   .AppendValue(parsed.BlockingPriority)
                   .AppendValue(reportXml)
                   .EndRow();

                rowsCollected++;
            }
            duckSw.Stop();
            _lastDuckDbMs = duckSw.ElapsedMilliseconds;
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Number == 15151 || ex.Message.Contains("XE session"))
        {
            /* XE session not found or not accessible */
            _logger?.LogDebug("Blocked process XE session not available on '{Server}': {Message}",
                server.DisplayName, ex.Message);
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Blocked process XE session not available: {ex.Message}");
            return 0;
        }

        _logger?.LogDebug("Collected {RowCount} blocked process reports for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }

    /// <summary>
    /// Parses a blocked-process-report XML fragment into a structured object.
    /// XML structure: &lt;blocked-process-report&gt;&lt;blocked-process&gt;&lt;process ...&gt;&lt;inputbuf&gt;...
    ///                 &lt;blocking-process&gt;&lt;process ...&gt;&lt;inputbuf&gt;...
    /// </summary>
    private static ParsedBlockedProcessReport? ParseBlockedProcessReportXml(string xml, DateTime? eventTime)
    {
        try
        {
            var doc = XElement.Parse(xml);

            var blockedProcess = doc.Element("blocked-process")?.Element("process");
            var blockingProcess = doc.Element("blocking-process")?.Element("process");

            if (blockedProcess == null)
            {
                return null;
            }

            return new ParsedBlockedProcessReport
            {
                EventTime = eventTime,
                DatabaseName = blockedProcess.Attribute("currentdbname")?.Value,
                BlockedSpid = int.TryParse(blockedProcess.Attribute("spid")?.Value, out var bs) ? bs : 0,
                BlockedEcid = int.TryParse(blockedProcess.Attribute("ecid")?.Value, out var be) ? be : 0,
                BlockingSpid = int.TryParse(blockingProcess?.Attribute("spid")?.Value, out var bks) ? bks : 0,
                BlockingEcid = int.TryParse(blockingProcess?.Attribute("ecid")?.Value, out var bke) ? bke : 0,
                WaitTimeMs = long.TryParse(blockedProcess.Attribute("waittime")?.Value, out var wt) ? wt : 0,
                WaitResource = blockedProcess.Attribute("waitresource")?.Value,
                LockMode = blockedProcess.Attribute("lockMode")?.Value,
                BlockedStatus = blockedProcess.Attribute("status")?.Value,
                BlockedIsolationLevel = blockedProcess.Attribute("isolationlevel")?.Value,
                BlockedLogUsed = long.TryParse(blockedProcess.Attribute("logused")?.Value, out var lu) ? lu : 0,
                BlockedTransactionCount = int.TryParse(blockedProcess.Attribute("trancount")?.Value, out var tc) ? tc : 0,
                BlockedClientApp = blockedProcess.Attribute("clientapp")?.Value,
                BlockedHostName = blockedProcess.Attribute("hostname")?.Value,
                BlockedLoginName = blockedProcess.Attribute("loginname")?.Value,
                BlockedSqlText = blockedProcess.Element("inputbuf")?.Value?.Trim(),
                BlockedTransactionName = blockedProcess.Attribute("transactionname")?.Value,
                BlockedLastTranStarted = DateTime.TryParse(blockedProcess.Attribute("lasttranstarted")?.Value, out var blts) ? blts : null,
                BlockedLastBatchStarted = DateTime.TryParse(blockedProcess.Attribute("lastbatchstarted")?.Value, out var blbs) ? blbs : null,
                BlockedLastBatchCompleted = DateTime.TryParse(blockedProcess.Attribute("lastbatchcompleted")?.Value, out var blbc) ? blbc : null,
                BlockedPriority = int.TryParse(blockedProcess.Attribute("priority")?.Value, out var bp) ? bp : 0,
                BlockingStatus = blockingProcess?.Attribute("status")?.Value,
                BlockingIsolationLevel = blockingProcess?.Attribute("isolationlevel")?.Value,
                BlockingClientApp = blockingProcess?.Attribute("clientapp")?.Value,
                BlockingHostName = blockingProcess?.Attribute("hostname")?.Value,
                BlockingLoginName = blockingProcess?.Attribute("loginname")?.Value,
                BlockingSqlText = blockingProcess?.Element("inputbuf")?.Value?.Trim(),
                BlockingTransactionName = blockingProcess?.Attribute("transactionname")?.Value,
                BlockingLastTranStarted = DateTime.TryParse(blockingProcess?.Attribute("lasttranstarted")?.Value, out var bklts) ? bklts : null,
                BlockingLastBatchStarted = DateTime.TryParse(blockingProcess?.Attribute("lastbatchstarted")?.Value, out var bklbs) ? bklbs : null,
                BlockingLastBatchCompleted = DateTime.TryParse(blockingProcess?.Attribute("lastbatchcompleted")?.Value, out var bklbc) ? bklbc : null,
                BlockingPriority = int.TryParse(blockingProcess?.Attribute("priority")?.Value, out var bkp) ? bkp : 0
            };
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Internal model for parsed blocked process report XML data.
    /// </summary>
    private class ParsedBlockedProcessReport
    {
        public DateTime? EventTime { get; set; }
        public string? DatabaseName { get; set; }
        public int BlockedSpid { get; set; }
        public int BlockedEcid { get; set; }
        public int BlockingSpid { get; set; }
        public int BlockingEcid { get; set; }
        public long WaitTimeMs { get; set; }
        public string? WaitResource { get; set; }
        public string? LockMode { get; set; }
        public string? BlockedStatus { get; set; }
        public string? BlockedIsolationLevel { get; set; }
        public long BlockedLogUsed { get; set; }
        public int BlockedTransactionCount { get; set; }
        public string? BlockedClientApp { get; set; }
        public string? BlockedHostName { get; set; }
        public string? BlockedLoginName { get; set; }
        public string? BlockedSqlText { get; set; }
        public string? BlockingStatus { get; set; }
        public string? BlockingIsolationLevel { get; set; }
        public string? BlockingClientApp { get; set; }
        public string? BlockingHostName { get; set; }
        public string? BlockingLoginName { get; set; }
        public string? BlockingSqlText { get; set; }
        public string? BlockedTransactionName { get; set; }
        public string? BlockingTransactionName { get; set; }
        public DateTime? BlockedLastTranStarted { get; set; }
        public DateTime? BlockingLastTranStarted { get; set; }
        public DateTime? BlockedLastBatchStarted { get; set; }
        public DateTime? BlockingLastBatchStarted { get; set; }
        public DateTime? BlockedLastBatchCompleted { get; set; }
        public DateTime? BlockingLastBatchCompleted { get; set; }
        public int BlockedPriority { get; set; }
        public int BlockingPriority { get; set; }
    }
}
