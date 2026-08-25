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
    private const string BlockedProcessXeSessionName = BlockedProcessReportCollector.XeSessionName;

    /// <summary>
    /// Ensures the blocked process XE session exists and is running.
    /// Creates a ring_buffer session for ALL platforms (on-prem, MI, Azure SQL DB).
    /// Server-scoped session for on-prem/MI; on Azure SQL DB a database-scoped session in EVERY
    /// monitored database (#1535 — a single session only ever captured the connection's own
    /// database, so blocking in the logical server's other databases never appeared).
    /// </summary>
    public async Task EnsureBlockedProcessXeSessionAsync(ServerConnection server, int engineEdition = 0, CancellationToken cancellationToken = default)
    {
        /* Skip if the blocked_process_report collector is disabled */
        var schedule = _scheduleManager.GetScheduleForServer(server.Id, "blocked_process_report");
        if (schedule == null || !schedule.Enabled)
        {
            return;
        }

        if (engineEdition == 5)
        {
            /* Azure SQL DB: one database-scoped session per monitored database, matching the
               per-database ring-buffer read (BlockedProcessReportCollector.RunsPerDatabase). The
               shared driver skips master, honors ExcludedDatabases via the shared database list,
               self-heals sessions the reader can't see, and only surfaces unhealthy when NO
               database could be ensured. */
            await EnsureDatabaseScopedXeSessionsAsync(
                server, "blocked process", BlockedProcessXeSessionName,
                EnsureBlockedProcessXeSessionAzureSqlDbAsync, cancellationToken);
            return;
        }

        try
        {
            using var connection = await CreateConnectionAsync(server, cancellationToken);

            /* On-prem and Azure MI: create server-scoped session with ring_buffer */
            await EnsureBlockedProcessXeSessionOnPremAsync(connection, server, cancellationToken);
        }
        catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
        {
            /* The session is already present + running -- see IsBenignXeSessionAlreadyPresent (#1251). */
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Blocked process XE session already present (benign, #1251)");
        }
        catch (SqlException ex)
        {
            /* Warn rather than Error when the server simply said no: a denied XE session is a least-privilege
               posture (#1823), classified as PERMISSIONS upstream and retried no further this session.
               Genuine failures still log at Error. */
            if (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
            {
                AppLogger.Warn("XeSession", $"[{server.DisplayName}] Failed to ensure blocked process XE session: {ex.Message}");
            }
            else
            {
                AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to ensure blocked process XE session: {ex.Message}");
            }

            /* Propagate so RunCollectorAsync marks the collector unhealthy instead
               of letting a zero-row ring-buffer read record SUCCESS (#1086) */
            throw new XeSessionEnsureException("blocked process", ex);
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

SELECT /* PerformanceMonitorLite */
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = BlockedProcessXeSessionName });
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
                        AppLogger.Info("XeSession", $"[{server.DisplayName}] Started blocked process XE session");
                    }
                    catch (SqlException ex)
                    {
                        AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to start blocked process XE session: {ex.Message}");
                        throw;
                    }
                }
                else
                {
                    AppLogger.Debug("XeSession", $"Blocked process XE session is running on '{server.DisplayName}'");
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
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started blocked process XE session");
        }
        catch (SqlException ex)
        {
            /* Warn rather than Error when the server simply said no: a denied XE session is a least-privilege
               posture (#1823), classified as PERMISSIONS upstream and retried no further this session.
               Genuine failures still log at Error. */
            if (SqlServerPermissionErrors.IsPermissionDenied(ex.Number))
            {
                AppLogger.Warn("XeSession", $"[{server.DisplayName}] Failed to create blocked process XE session: {ex.Message}");
            }
            else
            {
                AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to create blocked process XE session: {ex.Message}");
            }
            throw;
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

SELECT /* PerformanceMonitorLite */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = BlockedProcessXeSessionName });
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

                /* Debug, not Info: this fires once per monitored database per cycle (#1535). */
                AppLogger.Debug("XeSession", $"[Azure SQL DB:{connection.Database}] Blocked process XE session verified (database-scoped)");
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
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    STARTUP_STATE = ON
);

ALTER EVENT SESSION [{BlockedProcessXeSessionName}] ON DATABASE STATE = START;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        AppLogger.Info("XeSession", $"[Azure SQL DB:{connection.Database}] Created and started blocked process XE session (database-scoped)");
    }

    /// <summary>
    /// True when every error is a benign "the session is already there" extended-events error:
    /// 25631 (event session already exists) or 25705 (already started). On Azure SQL DB the XE
    /// existence catalogs (sys.database_event_sessions / sys.dm_xe_database_sessions) are visibility-
    /// scoped per principal and can come back empty even when the session is present + running, so the
    /// CREATE/START path reports these -- they confirm the session is up, not a failure to surface (#1251).
    /// Shared by the blocked-process and deadlock ensure paths (same partial class).
    /// </summary>
    private static bool IsBenignXeSessionAlreadyPresent(SqlException ex)
    {
        if (ex.Errors.Count == 0)
        {
            return false;
        }

        foreach (Microsoft.Data.SqlClient.SqlError error in ex.Errors)
        {
            /* 25631 = event session already exists; 25705 = already started. */
            if (error.Number != 25631 && error.Number != 25705)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Azure SQL DB ensure driver, shared by the deadlock and blocked-process paths (#1535): one
    /// database-scoped XE session per monitored database, matching the collectors' per-database
    /// ring-buffer reads. Master is skipped (database-scoped sessions can't be created in logical
    /// master); ExcludedDatabases is honored by <see cref="GetAzureDatabaseListAsync"/>.
    ///
    /// <para><b>The #1251 benign path grew a read-back check.</b> A benign "already exists"/"already
    /// started" from the engine proves the session is there, but NOT that this principal can see it:
    /// the ring-buffer reader joins <c>sys.dm_xe_database_sessions</c>, and a session that is
    /// invisible there (created by another principal, or present-but-stopped) reads zero rows forever
    /// while the collector records SUCCESS — the exact silent-empty shape of #1346/#1535. So after a
    /// benign error the session is probed in the reader's own DMV, and an invisible one is dropped
    /// and recreated under this principal. On-prem keeps the plain benign log: its server-scoped
    /// catalogs need the same VIEW SERVER STATE every collector already requires.</para>
    ///
    /// <para><b>Failure isolation:</b> one database failing to ensure must not kill capture for the
    /// other databases, so per-database failures are warn-logged and the cycle proceeds — but if NO
    /// database could be ensured, capture is fully dead and this throws (SqlException failures as
    /// <see cref="XeSessionEnsureException"/> for the #1086 health surface) instead of letting a
    /// zero-row read record SUCCESS.</para>
    /// </summary>
    private async Task EnsureDatabaseScopedXeSessionsAsync(
        ServerConnection server,
        string captureName,
        string sessionName,
        Func<SqlConnection, CancellationToken, Task> ensureAsync,
        CancellationToken cancellationToken)
    {
        List<string> databases;
        try
        {
            databases = await GetAzureDatabaseListAsync(server, cancellationToken);
        }
        catch (SqlException ex)
        {
            AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to enumerate databases for {captureName} XE sessions: {ex.Message}");
            throw new XeSessionEnsureException(captureName, ex);
        }

        int attempted = 0;
        int healthy = 0;
        Exception? firstFailure = null;

        foreach (var databaseName in databases)
        {
            cancellationToken.ThrowIfCancellationRequested();

            /* Logical master can't host database-scoped event sessions. */
            if (string.Equals(databaseName, "master", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            attempted++;

            try
            {
                using var connection = await OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
                var readable = true;

                try
                {
                    await ensureAsync(connection, cancellationToken);
                }
                catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
                {
                    var recreateKey = $"{server.Id}:{databaseName}:{sessionName}";

                    if (await IsDatabaseScopedXeSessionVisibleAsync(connection, sessionName, cancellationToken))
                    {
                        AppLogger.Debug("XeSession", $"[{server.DisplayName}] [{databaseName}] {captureName} XE session already present (benign, #1251)");
                    }
                    else if (_xeSessionRecreateGaveUp.ContainsKey(recreateKey))
                    {
                        /* Recreate already proven not to fix visibility here — don't churn the
                           session every cycle (each DROP wipes captured-but-unread events).
                           Counted unhealthy, quietly (the give-up itself was logged at Error). */
                        AppLogger.Debug("XeSession", $"[{server.DisplayName}] [{databaseName}] {captureName} XE session still not readable; recreate previously didn't help — skipping (#1535)");
                        readable = false;
                        firstFailure ??= ex;
                    }
                    else
                    {
                        /* Exists per the engine, invisible to the reader's DMV — reclaim it.
                           Failures here fall to the per-database catch below. */
                        await RecreateDatabaseScopedXeSessionAsync(connection, sessionName, ensureAsync, cancellationToken);

                        if (await IsDatabaseScopedXeSessionVisibleAsync(connection, sessionName, cancellationToken))
                        {
                            AppLogger.Info("XeSession", $"[{server.DisplayName}] [{databaseName}] {captureName} XE session existed but was not visible to the ring-buffer reader — dropped and recreated (#1535)");
                        }
                        else
                        {
                            /* Recreated under THIS principal and still invisible — recreating
                               again next cycle can't help and would only churn events away. */
                            _xeSessionRecreateGaveUp[recreateKey] = 1;
                            AppLogger.Error("XeSession", $"[{server.DisplayName}] [{databaseName}] {captureName} XE session is still not visible in sys.dm_xe_database_sessions after recreating it — the ring-buffer reader cannot see this database's capture; giving up on recreates until the app restarts (#1535)");
                            readable = false;
                            firstFailure ??= ex;
                        }
                    }
                }

                if (readable)
                {
                    healthy++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                firstFailure ??= ex;
                AppLogger.Warn("XeSession", $"[{server.DisplayName}] [{databaseName}] Failed to ensure {captureName} XE session: {ex.Message}");
            }
        }

        if (attempted > 0 && healthy == 0 && firstFailure is not null)
        {
            AppLogger.Error("XeSession", $"[{server.DisplayName}] Failed to ensure the {captureName} XE session in all {attempted} database(s)");

            if (firstFailure is SqlException sqlFailure)
            {
                throw new XeSessionEnsureException(captureName, sqlFailure);
            }

            /* Non-SQL failure (e.g. a connection-open fault) — surface it raw with its original
               stack; RunCollectorAsync's general handler classifies it ERROR, which is still not a
               silent SUCCESS. */
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstFailure).Throw();
        }
    }

    /* Databases where a drop+recreate demonstrably did NOT make the session visible to the reader
       (see the give-up branch above): keyed server:database:session, in-memory so an app restart
       retries once. Prevents a per-cycle DROP/CREATE ping-pong that would wipe captured-but-unread
       ring-buffer events every cycle in a pathological-permissions database. */
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _xeSessionRecreateGaveUp = new();

    /// <summary>
    /// Whether the database-scoped session is visible to THIS principal in
    /// <c>sys.dm_xe_database_sessions</c> — the very DMV the ring-buffer reader joins, so this is
    /// the reader's-eye view: false means the reader would see zero rows regardless of captured
    /// events (session stopped, or running but created by a principal whose sessions we can't see).
    /// </summary>
    private async Task<bool> IsDatabaseScopedXeSessionVisibleAsync(SqlConnection connection, string sessionName, CancellationToken cancellationToken)
    {
        using var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
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
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is int isVisible && isVisible == 1;
    }

    /// <summary>
    /// Drops the (engine-confirmed-present) database-scoped session and re-runs the ensure so it is
    /// recreated under this principal, visible to the reader. The DROP works by name even when the
    /// catalogs hide the session — the same store the CREATE collided with resolves it.
    /// </summary>
    private static async Task RecreateDatabaseScopedXeSessionAsync(
        SqlConnection connection,
        string sessionName,
        Func<SqlConnection, CancellationToken, Task> ensureAsync,
        CancellationToken cancellationToken)
    {
        using (var dropCmd = new SqlCommand($"DROP EVENT SESSION [{sessionName}] ON DATABASE;", connection))
        {
            dropCmd.CommandTimeout = CommandTimeoutSeconds;
            await dropCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await ensureAsync(connection, cancellationToken);
    }

    /// <summary>
    /// Collects blocked process reports via the shared <see cref="BlockedProcessReportCollector"/>
    /// definition (the server- vs database-scoped ring-buffer reads, the wait_resource →
    /// contentious-object resolution, the event_time watermark, and the report-XML parse live
    /// there — the cross-SKU parity contract). The XE session lifecycle stays here; a
    /// missing/inaccessible session is tolerated as zero rows, exactly as before.
    /// </summary>
    private async Task<int> CollectBlockedProcessReportsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            return await RunCollectorDefinitionAsync(BlockedProcessReportCollector.Instance, server, cancellationToken);
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Number == 15151 || ex.Message.Contains("XE session"))
        {
            /* XE session not found or not accessible */
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Blocked process XE session not available: {ex.Message}");
            return 0;
        }
    }
}
