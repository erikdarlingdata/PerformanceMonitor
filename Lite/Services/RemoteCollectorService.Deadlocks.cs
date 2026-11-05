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
               skips master, honors ExcludedDatabases via the shared database list, and only
               surfaces unhealthy when NO database could be ensured. The per-database routine
               passed here never drops a shared session (#4961): it starts a stopped one, and
               falls back to this install's own session when the shared one stays unusable. */
            await EnsureDatabaseScopedXeSessionsAsync(
                server, "deadlock", DeadlockXeSessionName,
                AlwaysOnArmEnsureNotUsed, cancellationToken,
                (databaseName, token) => EnsureAlwaysOnXeSessionInDatabaseAsync(server, AlwaysOnXeSessionKind.Deadlock, "deadlock", databaseName, token));
            return;
        }

        /* On-prem, Azure MI, and AWS RDS: create server-scoped session with ring_buffer */
        await EnsureServerScopedXeSessionAsync(
            server, "deadlock", DeadlockXeSessionName,
            (connection, token) => EnsureDeadlockXeSessionOnPremAsync(connection, server, token), cancellationToken);
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
                    /* Session exists but is stopped - start it. A failure is not logged here: it propagates to the ensure's catch, which applies the #4964
                       log-once rule (Warn or Error on the first failing cycle, Debug after it). A line here would repeat at full level every cycle (#5640). */
                    using var startCmd = new SqlCommand(
                        $"ALTER EVENT SESSION [{DeadlockXeSessionName}] ON SERVER STATE = START;", connection);
                    startCmd.CommandTimeout = CommandTimeoutSeconds;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);
                    AppLogger.Info("XeSession", $"[{server.DisplayName}] Started deadlock XE session");
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
        /* No catch here: a failure propagates to the ensure's catch, which logs it once at Warn or Error and then at Debug (#4964). */
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

    /// <summary>
    /// Collects deadlocks via the shared <see cref="DeadlocksCollector"/> definition (the
    /// server- vs database-scoped ring-buffer reads, the deadlock_time watermark, and the
    /// victim-inputbuf extraction live there — the cross-SKU parity contract). The XE session
    /// lifecycle stays here; a missing/inaccessible session is NOT tolerated as zero rows (#4731): the
    /// read raises <see cref="XeSessionEnsureException"/> like the ensure does (see
    /// <see cref="ReadXeSessionAsync"/>), so the run records PERMISSIONS or ERROR with the XE session
    /// flagged unavailable, and never a SUCCESS over a source it could not read.
    /// </summary>
    private async Task<int> CollectDeadlocksAsync(ServerConnection server, XeSessionEnsureException? ensureFailure, CancellationToken cancellationToken)
    {
        if (ensureFailure is null)
        {
            var rows = await ReadXeSessionAsync(
                "deadlock",
                () => RunCollectorDefinitionAsync(DeadlocksCollector.Instance, server, cancellationToken));

            /* #7639: some databases were refused their session while others were ensured. The run still succeeded, and its note says
               which databases are not in it and why, so a short or empty grid is not read as a quiet server. */
            ApplyXeEnsurePartialNote(server, XeSessionEnsureWarnedKey(server, DeadlockXeSessionName));
            return rows;
        }

        /* #7639: the Azure SQL Database ensure failed in every database, so no database-scoped session was made. The read goes
           ahead anyway: the master item's durable telemetry arm needs no session (#2641), and a database that already holds the
           shared session is still read. The Azure loop skips each database it cannot read on its own, and whatever the read
           stored stays stored. The run then records the ensure's failure, the same PERMISSIONS or ERROR with the same message
           as before, so the grant that fixes it reaches the collection log. A read failure of its own is dropped: the ensure's
           is the cause, and the one a person can act on. */
        try
        {
            await ReadXeSessionAsync(
                "deadlock",
                () => RunCollectorDefinitionAsync(DeadlocksCollector.Instance, server, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            AppLogger.Debug("XeSession", $"[{server.DisplayName}] Deadlock read after a failed ensure: {ex.Message}");
        }

        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ensureFailure).Throw();
        return 0;
    }
}
