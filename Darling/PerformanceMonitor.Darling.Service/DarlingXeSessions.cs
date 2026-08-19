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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Creates and starts the app-managed XE ring-buffer sessions the deadlock and blocked-process
/// collectors read — ported from Lite's Ensure* lifecycle (session names come from the shared
/// definitions so the reader and this lifecycle can never disagree). Server-scoped sessions on
/// on-prem/MI/RDS; on Azure SQL DB a database-scoped session in EVERY monitored database (#1535
/// — a single session only ever captured the connection's own database), matching the
/// collectors' per-database ring-buffer reads. The blocked-process threshold sp_configure
/// bootstrap self-guards for platforms without sp_configure (AWS RDS). Ensure failures are
/// logged and tolerated — the collectors read zero rows until the session exists (#1251 benign
/// "already present" errors are success once the session passes the reader-visibility check),
/// and the SESSION_MISSING classification + Capture Down self-alert own the capture-is-down
/// story. Runs once per connect: a database created mid-run gets its sessions at the next
/// reconnect (its reads tolerate the missing session as zero rows until then).
/// </summary>
public static class DarlingXeSessions
{
    public static async Task EnsureAllAsync(ServerRuntime server, DarlingCollectorRunner runner, ILogger? logger, CancellationToken cancellationToken)
    {
        /* Same self-gate as ReconcileLongQueryCompletionsAsync, same reason: this method constructs a
           SqlConnection from the engine-ambiguous connection string, so it enforces its own precondition
           rather than trusting the caller's gate (DarlingWorker's connect path) to be the only entry
           forever. XE does not exist on PostgreSQL. */
        if (server.Target.Engine != PerformanceMonitor.Collectors.CollectorTargetEngine.SqlServer)
        {
            return;
        }

        if (server.Target.IsAzureSqlDb)
        {
            await EnsureDatabaseScopedAsync(server, runner, logger, cancellationToken);
            return;
        }

        try
        {
            using var connection = new SqlConnection(server.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            /* Each capture ensures under its own filter — benign AND hard: a benign "already
               exists" (or any hard failure) from the deadlock ensure used to abort the whole
               batch, silently skipping the blocked-process ensure until the next reconnect. */
            try
            {
                await EnsureDeadlockOnPremAsync(connection, server, logger, cancellationToken);
            }
            catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
            {
                logger?.LogInformation("[{Server}] Deadlock XE session already present (benign, #1251)", server.Config.DisplayName);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogError("[{Server}] Failed to ensure deadlock XE session: {Message} — deadlock collection will read zero rows until resolved",
                    server.Config.DisplayName, ex.Message);
            }

            try
            {
                await EnsureBlockedProcessOnPremAsync(connection, server, logger, cancellationToken);
            }
            catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
            {
                logger?.LogInformation("[{Server}] Blocked process XE session already present (benign, #1251)", server.Config.DisplayName);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogError("[{Server}] Failed to ensure blocked process XE session: {Message} — blocked-process collection will read zero rows until resolved",
                    server.Config.DisplayName, ex.Message);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogError("[{Server}] Failed to ensure XE sessions: {Message} — deadlock/blocked-process collection will read zero rows until resolved",
                server.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// Azure SQL DB: one database-scoped session pair per monitored database (#1535), using the
    /// runner's shared database list (master enumeration + fallback + ExcludedDatabases) and
    /// per-database connections. Master is skipped — database-scoped sessions can't be created in
    /// logical master. Failure isolation is per database AND per capture: one broken database (or
    /// one broken session) never blocks the rest.
    /// </summary>
    private static async Task EnsureDatabaseScopedAsync(ServerRuntime server, DarlingCollectorRunner runner, ILogger? logger, CancellationToken cancellationToken)
    {
        List<string> databases;
        try
        {
            databases = await runner.GetAzureDatabaseListAsync(server, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogError("[{Server}] Failed to enumerate databases for XE sessions: {Message} — deadlock/blocked-process collection will read zero rows until resolved",
                server.Config.DisplayName, ex.Message);
            return;
        }

        foreach (var databaseName in databases)
        {
            cancellationToken.ThrowIfCancellationRequested();

            /* Logical master can't host database-scoped event sessions. */
            if (string.Equals(databaseName, "master", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                using var connection = await runner.OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);

                await EnsureOneDatabaseScopedAsync(
                    connection, server, databaseName, "deadlock", DeadlocksCollector.XeSessionName,
                    c => EnsureDeadlockAzureAsync(c, server, logger, cancellationToken), logger, cancellationToken);

                await EnsureOneDatabaseScopedAsync(
                    connection, server, databaseName, "blocked process", BlockedProcessReportCollector.XeSessionName,
                    c => EnsureBlockedProcessAzureAsync(c, server, logger, cancellationToken), logger, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning("[{Server}] [{Database}] Failed to open a connection for XE session ensure: {Message}",
                    server.Config.DisplayName, databaseName, ex.Message);
            }
        }
    }

    /// <summary>
    /// One capture's database-scoped ensure, with the #1251 benign path grown a read-back check
    /// (#1535, verbatim semantics from Lite): a benign "already exists"/"already started" proves
    /// the session is there, but NOT that this principal can see it — the ring-buffer reader joins
    /// <c>sys.dm_xe_database_sessions</c>, and a session invisible there (created by another
    /// principal, or present-but-stopped) reads zero rows forever while collection reports
    /// SUCCESS. So after a benign error the session is probed in the reader's own DMV, and an
    /// invisible one is dropped and recreated under this principal. Hard failures are warn-logged
    /// by the caller's per-capture catch here.
    /// </summary>
    private static async Task EnsureOneDatabaseScopedAsync(
        SqlConnection connection,
        ServerRuntime server,
        string databaseName,
        string captureName,
        string sessionName,
        Func<SqlConnection, Task> ensureAsync,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        try
        {
            try
            {
                await ensureAsync(connection);
            }
            catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
            {
                if (!await IsDatabaseScopedSessionVisibleAsync(connection, sessionName, cancellationToken))
                {
                    using (var dropCmd = new SqlCommand($"DROP EVENT SESSION [{sessionName}] ON DATABASE;", connection))
                    {
                        dropCmd.CommandTimeout = 60;
                        await dropCmd.ExecuteNonQueryAsync(cancellationToken);
                    }

                    await ensureAsync(connection);

                    /* Read back once: recreated under THIS principal and still invisible means the
                       reader cannot see this database's capture at all — say so at Error rather than
                       announcing a recreate that didn't help. Ensure runs once per connect, so there
                       is no per-cycle churn to bound here (Lite's per-cycle driver keeps a give-up
                       set for the same case). */
                    if (await IsDatabaseScopedSessionVisibleAsync(connection, sessionName, cancellationToken))
                    {
                        logger?.LogInformation("[{Server}] [{Database}] {Capture} XE session existed but was not visible to the ring-buffer reader — dropped and recreated (#1535)",
                            server.Config.DisplayName, databaseName, captureName);
                    }
                    else
                    {
                        logger?.LogError("[{Server}] [{Database}] {Capture} XE session is still not visible in sys.dm_xe_database_sessions after recreating it — the ring-buffer reader cannot see this database's capture",
                            server.Config.DisplayName, databaseName, captureName);
                    }
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("[{Server}] [{Database}] Failed to ensure {Capture} XE session: {Message}",
                server.Config.DisplayName, databaseName, captureName, ex.Message);
        }
    }

    /// <summary>
    /// Whether the database-scoped session is visible to THIS principal in
    /// <c>sys.dm_xe_database_sessions</c> — the reader's-eye view (false: the reader would see zero
    /// rows regardless of captured events).
    /// </summary>
    private static async Task<bool> IsDatabaseScopedSessionVisibleAsync(SqlConnection connection, string sessionName, CancellationToken cancellationToken)
    {
        using var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    is_visible =
        CASE
            WHEN EXISTS
            (
                SELECT
                    1/0
                FROM sys.dm_xe_database_sessions AS xes
                WHERE xes.name = @session_name
            )
            THEN 1
            ELSE 0
        END;", connection);
        cmd.CommandTimeout = 60;
        cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is int isVisible && isVisible == 1;
    }

    private static async Task EnsureDeadlockOnPremAsync(SqlConnection connection, ServerRuntime server, ILogger? logger, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = DeadlocksCollector.XeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    using var startCmd = new SqlCommand(
                        $"ALTER EVENT SESSION [{DeadlocksCollector.XeSessionName}] ON SERVER STATE = START;", connection);
                    startCmd.CommandTimeout = 60;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);
                    logger?.LogInformation("[{Server}] Started deadlock XE session", server.Config.DisplayName);
                }
                return;
            }
        }

        /* MEMORY_PARTITION_MODE = NONE for AWS RDS compatibility (mirrors Lite). */
        using var createCmd = new SqlCommand($@"
CREATE EVENT SESSION [{DeadlocksCollector.XeSessionName}]
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

ALTER EVENT SESSION [{DeadlocksCollector.XeSessionName}] ON SERVER STATE = START;", connection);
        createCmd.CommandTimeout = 60;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        logger?.LogInformation("[{Server}] Created and started deadlock XE session", server.Config.DisplayName);
    }

    private static async Task EnsureDeadlockAzureAsync(SqlConnection connection, ServerRuntime server, ILogger? logger, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
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
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = DeadlocksCollector.XeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result is int hasCorrectEvent)
            {
                if (hasCorrectEvent == 0)
                {
                    /* Wrong event — drop and recreate (mirrors Lite). */
                    try
                    {
                        using var dropCmd = new SqlCommand(
                            $"DROP EVENT SESSION [{DeadlocksCollector.XeSessionName}] ON DATABASE;", connection);
                        dropCmd.CommandTimeout = 60;
                        await dropCmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                    catch (SqlException ex)
                    {
                        logger?.LogError("[{Server}] Failed to drop old deadlock XE session: {Message}", server.Config.DisplayName, ex.Message);
                    }
                }
                else
                {
                    using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{DeadlocksCollector.XeSessionName}'
)
BEGIN
    ALTER EVENT SESSION [{DeadlocksCollector.XeSessionName}] ON DATABASE STATE = START;
END;", connection);
                    startCmd.CommandTimeout = 60;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);
                    return;
                }
            }
        }

        using (var cmd = new SqlCommand($@"
CREATE EVENT SESSION [{DeadlocksCollector.XeSessionName}]
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

ALTER EVENT SESSION [{DeadlocksCollector.XeSessionName}] ON DATABASE STATE = START;", connection))
        {
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        logger?.LogInformation("[{Server}] Created and started deadlock XE session (database-scoped)", server.Config.DisplayName);
    }

    private static async Task EnsureBlockedProcessOnPremAsync(SqlConnection connection, ServerRuntime server, ILogger? logger, CancellationToken cancellationToken)
    {
        /* Blocked process threshold bootstrap — sp_configure is unavailable on AWS RDS
           (parameter groups there), hence the tolerant catch (mirrors Lite). */
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
            thresholdCmd.CommandTimeout = 60;
            var result = await thresholdCmd.ExecuteScalarAsync(cancellationToken);
            if ((result as int? ?? 0) == 0)
            {
                logger?.LogInformation("[{Server}] Configured blocked process threshold to 5 seconds", server.Config.DisplayName);
            }
        }
        catch (SqlException ex)
        {
            logger?.LogInformation("[{Server}] Cannot set blocked process threshold via sp_configure (may require platform config): {Message}",
                server.Config.DisplayName, ex.Message);
        }

        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = BlockedProcessReportCollector.XeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    using var startCmd = new SqlCommand(
                        $"ALTER EVENT SESSION [{BlockedProcessReportCollector.XeSessionName}] ON SERVER STATE = START;", connection);
                    startCmd.CommandTimeout = 60;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);
                    logger?.LogInformation("[{Server}] Started blocked process XE session", server.Config.DisplayName);
                }
                return;
            }
        }

        using var createCmd = new SqlCommand($@"
CREATE EVENT SESSION [{BlockedProcessReportCollector.XeSessionName}]
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

ALTER EVENT SESSION [{BlockedProcessReportCollector.XeSessionName}] ON SERVER STATE = START;", connection);
        createCmd.CommandTimeout = 60;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        logger?.LogInformation("[{Server}] Created and started blocked process XE session", server.Config.DisplayName);
    }

    private static async Task EnsureBlockedProcessAzureAsync(SqlConnection connection, ServerRuntime server, ILogger? logger, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = BlockedProcessReportCollector.XeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{BlockedProcessReportCollector.XeSessionName}'
)
BEGIN
    ALTER EVENT SESSION [{BlockedProcessReportCollector.XeSessionName}] ON DATABASE STATE = START;
END;", connection);
                startCmd.CommandTimeout = 60;
                await startCmd.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
        }

        using (var cmd = new SqlCommand($@"
CREATE EVENT SESSION [{BlockedProcessReportCollector.XeSessionName}]
ON DATABASE
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

ALTER EVENT SESSION [{BlockedProcessReportCollector.XeSessionName}] ON DATABASE STATE = START;", connection))
        {
            cmd.CommandTimeout = 60;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        logger?.LogInformation("[{Server}] Created and started blocked process XE session (database-scoped)", server.Config.DisplayName);
    }

    /// <summary>
    /// Reconciles the OPT-IN long-query completion XE session (#1496) to the collector's enabled flag —
    /// Erik's dedicated switch, default OFF. ENABLED: create/start the session (server-scoped on
    /// on-prem/MI/RDS; database-scoped in every monitored database on Azure SQL DB, #1535). DISABLED:
    /// DROP the session on the monitored server(s) — the server-side session is the actual busy-server
    /// cost, so merely skipping collection is not enough. Unlike <see cref="EnsureAllAsync"/> (which is
    /// unconditional create-only, run once per connect), this is called from the per-server sweep and
    /// its lifecycle FOLLOWS the flag both ways; the DarlingWorker state-tracks the last-applied value so
    /// this only opens a connection when the desired state actually changes. Failures propagate to the
    /// caller (the worker logs + leaves the applied state unchanged so the next sweep retries).
    /// </summary>
    public static async Task ReconcileLongQueryCompletionsAsync(ServerRuntime server, DarlingCollectorRunner runner, bool enabled, ILogger? logger, CancellationToken cancellationToken)
    {
        /* Belt to the worker's braces: the caller gates on engine (a PostgreSQL target has no XE to
           reconcile), but this method constructs a SqlConnection from the engine-ambiguous connection
           string below, so it enforces its own precondition rather than trusting every present and future
           caller — the exact trust that put "Keyword not supported: 'host'" in the sweep log once a minute. */
        if (server.Target.Engine != PerformanceMonitor.Collectors.CollectorTargetEngine.SqlServer)
        {
            return;
        }

        if (server.Target.IsAzureSqlDb)
        {
            await ReconcileLongQueryCompletionsAzureAsync(server, runner, enabled, logger, cancellationToken);
            return;
        }

        using var connection = new SqlConnection(server.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        if (enabled)
        {
            await EnsureLongQueryCompletionsOnPremAsync(connection, server, logger, cancellationToken);
        }
        else
        {
            await DropLongQueryCompletionsAsync(connection, databaseScoped: false, cancellationToken);
            logger?.LogInformation("[{Server}] Long-query completion XE session reconciled OFF (collector disabled)", server.Config.DisplayName);
        }
    }

    private static async Task EnsureLongQueryCompletionsOnPremAsync(SqlConnection connection, ServerRuntime server, ILogger? logger, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = LongQueryCompletionsCollector.XeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    using var startCmd = new SqlCommand(LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: false), connection);
                    startCmd.CommandTimeout = 60;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);
                    logger?.LogInformation("[{Server}] Started long-query completion XE session", server.Config.DisplayName);
                }
                return;
            }
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(databaseScoped: false, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: false), connection);
        createCmd.CommandTimeout = 60;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        logger?.LogInformation("[{Server}] Created and started long-query completion XE session", server.Config.DisplayName);
    }

    private static async Task ReconcileLongQueryCompletionsAzureAsync(ServerRuntime server, DarlingCollectorRunner runner, bool enabled, ILogger? logger, CancellationToken cancellationToken)
    {
        List<string> databases;
        try
        {
            databases = await runner.GetAzureDatabaseListAsync(server, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("[{Server}] Failed to enumerate databases for the long-query completion XE session: {Message}", server.Config.DisplayName, ex.Message);
            return;
        }

        foreach (var databaseName in databases)
        {
            cancellationToken.ThrowIfCancellationRequested();

            /* Logical master can't host database-scoped event sessions. */
            if (string.Equals(databaseName, "master", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                using var connection = await runner.OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);

                if (enabled)
                {
                    try
                    {
                        await EnsureLongQueryCompletionsAzureAsync(connection, server, databaseName, logger, cancellationToken);
                    }
                    catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
                    {
                        /* Already present per the engine — the reader tolerates it (#1251). */
                    }
                }
                else
                {
                    await DropLongQueryCompletionsAsync(connection, databaseScoped: true, cancellationToken);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning("[{Server}] [{Database}] Failed to reconcile the long-query completion XE session: {Message}",
                    server.Config.DisplayName, databaseName, ex.Message);
            }
        }
    }

    private static async Task EnsureLongQueryCompletionsAzureAsync(SqlConnection connection, ServerRuntime server, string databaseName, ILogger? logger, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = LongQueryCompletionsCollector.XeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{LongQueryCompletionsCollector.XeSessionName}'
)
BEGIN
    ALTER EVENT SESSION [{LongQueryCompletionsCollector.XeSessionName}] ON DATABASE STATE = START;
END;", connection);
                startCmd.CommandTimeout = 60;
                await startCmd.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(databaseScoped: true, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: true), connection);
        createCmd.CommandTimeout = 60;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        logger?.LogInformation("[{Server}] [{Database}] Created and started long-query completion XE session (database-scoped)", server.Config.DisplayName, databaseName);
    }

    private static async Task DropLongQueryCompletionsAsync(SqlConnection connection, bool databaseScoped, CancellationToken cancellationToken)
    {
        using var cmd = new SqlCommand(LongQueryCompletionsCollector.BuildDropSessionSql(databaseScoped), connection);
        cmd.CommandTimeout = 60;
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// True when every error is a benign "the session is already there" extended-events error:
    /// 25631 (already exists) or 25705 (already started) — on Azure SQL DB the XE existence
    /// catalogs are visibility-scoped per principal, so CREATE/START can report these even when
    /// the pre-check came back empty; they confirm the session is up (#1251, verbatim from Lite).
    /// </summary>
    internal static bool IsBenignXeSessionAlreadyPresent(SqlException ex)
    {
        if (ex.Errors.Count == 0)
        {
            return false;
        }

        foreach (SqlError error in ex.Errors)
        {
            if (error.Number != 25631 && error.Number != 25705)
            {
                return false;
            }
        }

        return true;
    }
}
