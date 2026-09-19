/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /* The session name + DDL live in the shared definition so the ring-buffer reader and this
       lifecycle can never disagree on them (#1496). */
    private const string LongQueryXeSessionName = LongQueryCompletionsCollector.XeSessionName;

    /* Per-server last-applied enabled state for the long-query trace's XE session, so the reconcile
       does not open a connection every cycle for a server whose state has not changed. Keyed by
       server id; unset = not yet reconciled. In-memory (cleared on app restart, which then re-
       reconciles once). true = the session is being ensured (enabled); false = confirmed dropped
       (disabled). */
    private readonly ConcurrentDictionary<string, bool> _longQueryTraceApplied = new();

    /* #3754: the ENABLE failure the reconcile below caught, per server, kept until a later reconcile
       succeeds. The reconcile runs from the per-server collection loop, OUTSIDE the collector's run - so
       when the session could not be created (every monitored database refused it on Azure SQL DB; the
       one CREATE refused on-prem) the run still went ahead, its tolerant ring-buffer read found no
       session, returned zero rows exactly as a quiet session would, and the cycle recorded SUCCESS.
       The blocked-process and deadlock collectors do not have this hole because their ensure runs
       INSIDE RunCollectorAsync and throws XeSessionEnsureException straight into its classification;
       this slot is how the long-query collector's out-of-run ensure reaches the same arm. The
       exception is kept whole, not its message, because that arm classifies on the type and the inner
       SqlException's number (PERMISSIONS for a denied ALTER ANY EVENT SESSION, ERROR otherwise). */
    private readonly ConcurrentDictionary<string, Exception> _longQueryTraceFault = new();

    /// <summary>
    /// Reconciles the long-query completion XE session to the collector's enabled flag — Erik's
    /// dedicated switch (#1496), default OFF. ENABLED: ensure the session exists + running (re-ensured
    /// every cycle, so a session dropped out from under us self-heals, and an Azure database-scoped
    /// session that stopped on reconnect is restarted — mirrors the blocked-process ensure). DISABLED:
    /// DROP the session on the monitored server(s) — the session itself is the busy-server cost, so
    /// merely skipping collection is not enough. The drop runs once per disabled server (tracked in
    /// <see cref="_longQueryTraceApplied"/>) rather than every cycle, since a disabled collector is the
    /// default for every server and re-checking a confirmed-absent session each cycle would open a
    /// connection to every server forever. Called unconditionally from the per-server collection loop
    /// (NOT gated by the enabled flag), because a disabled collector is never dispatched and so the drop
    /// has nowhere else to run.
    /// </summary>
    public async Task ReconcileLongQueryCompletionsXeSessionAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        var schedule = _scheduleManager.GetScheduleForServer(server.Id, "long_query_completions");
        var enabled = schedule?.Enabled ?? false;

        try
        {
            if (enabled)
            {
                await EnsureLongQueryCompletionsXeSessionAsync(server, cancellationToken);
                _longQueryTraceApplied[server.Id] = true;

                /* #3754: the session exists (everywhere it could) - a fault from an earlier cycle is over. */
                _longQueryTraceFault.TryRemove(server.Id, out _);
            }
            else if (_longQueryTraceApplied.GetValueOrDefault(server.Id, true))
            {
                /* Disabled and either never reconciled or previously enabled: drop once, then remember
                   it is gone so the next cycles skip the connection entirely. */
                await DropLongQueryCompletionsXeSessionAsync(server, cancellationToken);
                _longQueryTraceApplied[server.Id] = false;

                /* #3754: nothing to be honest about while disabled - the collector is not dispatched - and a
                   fault left here would classify the first run after re-enabling before its reconcile ran. */
                _longQueryTraceFault.TryRemove(server.Id, out _);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* Leave the applied state unchanged so the next cycle retries; a failed reconcile must
               never break the collection loop. */
            AppLogger.Warn("XeSession", $"[{server.DisplayName}] Failed to reconcile long-query completion XE session: {ex.Message}");

            /* #3754: and while ENABLING, remember it, so this cycle's run of the collector is classified
               from this exception instead of reading an absent session as a quiet one. A DISABLING failure
               records nothing: no run is dispatched while disabled, and the retry the unchanged applied
               state already buys is the whole remedy. */
            if (enabled)
            {
                _longQueryTraceFault[server.Id] = ex;
            }
        }
    }

    /// <summary>
    /// Ensures the long-query completion XE session exists and is running. Server-scoped on
    /// on-prem/MI/RDS; on Azure SQL DB a database-scoped session in EVERY monitored database (#1535),
    /// matching the per-database ring-buffer read (<see cref="LongQueryCompletionsCollector.RunsPerDatabase"/>).
    /// </summary>
    public async Task EnsureLongQueryCompletionsXeSessionAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        var engineEdition = _serverManager.GetConnectionStatus(server.Id).SqlEngineEdition;

        if (engineEdition == 5)
        {
            await EnsureDatabaseScopedXeSessionsAsync(
                server, "long query completions", LongQueryXeSessionName,
                EnsureLongQueryCompletionsXeSessionAzureSqlDbAsync, cancellationToken);
            return;
        }

        using var connection = await CreateConnectionAsync(server, cancellationToken);
        await EnsureLongQueryCompletionsXeSessionOnPremAsync(connection, server, cancellationToken);
    }

    private async Task EnsureLongQueryCompletionsXeSessionOnPremAsync(SqlConnection connection, ServerConnection server, CancellationToken cancellationToken)
    {
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
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = LongQueryXeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    using var startCmd = new SqlCommand(LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: false), connection);
                    startCmd.CommandTimeout = CommandTimeoutSeconds;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);
                    AppLogger.Info("XeSession", $"[{server.DisplayName}] Started long-query completion XE session");
                }
                return;
            }
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(databaseScoped: false, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: false), connection);
        createCmd.CommandTimeout = CommandTimeoutSeconds;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started long-query completion XE session (duration >= {LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds} us)");
    }

    private async Task EnsureLongQueryCompletionsXeSessionAzureSqlDbAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = LongQueryXeSessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{LongQueryXeSessionName}'
)
BEGIN
    ALTER EVENT SESSION [{LongQueryXeSessionName}] ON DATABASE STATE = START;
END;", connection);
                startCmd.CommandTimeout = CommandTimeoutSeconds;
                await startCmd.ExecuteNonQueryAsync(cancellationToken);
                AppLogger.Debug("XeSession", $"[Azure SQL DB:{connection.Database}] Long-query completion XE session verified (database-scoped)");
                return;
            }
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(databaseScoped: true, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(databaseScoped: true), connection);
        createCmd.CommandTimeout = CommandTimeoutSeconds;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        AppLogger.Info("XeSession", $"[Azure SQL DB:{connection.Database}] Created and started long-query completion XE session (database-scoped)");
    }

    /// <summary>
    /// Drops the long-query completion XE session (the opt-out path — disabling the collector removes
    /// the server-side session, the actual busy-server cost). Idempotent: the shared DROP DDL is
    /// guarded by an existence check, so a server that never had the session is a clean no-op. On Azure
    /// SQL DB the drop runs per monitored database (master skipped), matching where the sessions live.
    /// </summary>
    public async Task DropLongQueryCompletionsXeSessionAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        var engineEdition = _serverManager.GetConnectionStatus(server.Id).SqlEngineEdition;

        if (engineEdition == 5)
        {
            List<string> databases;
            try
            {
                databases = await GetAzureDatabaseListAsync(server, cancellationToken);
            }
            catch (SqlException ex)
            {
                AppLogger.Warn("XeSession", $"[{server.DisplayName}] Could not enumerate databases to drop the long-query completion XE session: {ex.Message}");
                return;
            }

            foreach (var databaseName in databases)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (string.Equals(databaseName, "master", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                try
                {
                    using var connection = await OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
                    using var dropCmd = new SqlCommand(LongQueryCompletionsCollector.BuildDropSessionSql(databaseScoped: true), connection);
                    dropCmd.CommandTimeout = CommandTimeoutSeconds;
                    await dropCmd.ExecuteNonQueryAsync(cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    AppLogger.Debug("XeSession", $"[{server.DisplayName}] [{databaseName}] Could not drop the long-query completion XE session: {ex.Message}");
                }
            }

            return;
        }

        using var conn = await CreateConnectionAsync(server, cancellationToken);
        using var cmd = new SqlCommand(LongQueryCompletionsCollector.BuildDropSessionSql(databaseScoped: false), conn);
        cmd.CommandTimeout = CommandTimeoutSeconds;
        await cmd.ExecuteNonQueryAsync(cancellationToken);
        AppLogger.Info("XeSession", $"[{server.DisplayName}] Long-query completion XE session reconciled OFF (collector disabled)");
    }

    /// <summary>
    /// Collects long-query completions via the shared <see cref="LongQueryCompletionsCollector"/>
    /// definition. The session lifecycle stays in the reconcile above; a missing/inaccessible session
    /// is tolerated here as zero rows, exactly like the blocked-process reader — EXCEPT (#3754) when the
    /// reconcile has already recorded that the session could not be created: then the read is not
    /// attempted and the reconcile's own exception is rethrown into <c>RunCollectorAsync</c>'s
    /// classification, the way the blocked-process and deadlock ensures throw into it from inside the
    /// run. Zero rows off a session that does not exist is not a collection; recording it as SUCCESS was
    /// the defect.
    /// </summary>
    private async Task<int> CollectLongQueryCompletionsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        if (_longQueryTraceFault.TryGetValue(server.Id, out var ensureFailure))
        {
            /* The original exception, with its original stack: RunCollectorAsync classifies an
               XeSessionEnsureException on its inner SqlException's number (PERMISSIONS / ERROR) and anything
               else through its general arms, so the type has to survive - a message alone would land every
               refusal as ERROR. */
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ensureFailure).Throw();
        }

        try
        {
            return await RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, server, cancellationToken);
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Number == 15151 || ex.Message.Contains("XE session"))
        {
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Long-query completion XE session not available: {ex.Message}");
            return 0;
        }
    }
}
