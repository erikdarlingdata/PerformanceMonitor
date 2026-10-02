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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;

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
///
/// <para>The opt-in long-query completion session (<see cref="ReconcileLongQueryCompletionsAsync"/>) is
/// the exception to "logged and tolerated" since #3754: its ensure failures are ACCOUNTED and handed back
/// to the worker, because that collector has no self-alert of its own and a tolerated-and-forgotten
/// ensure failure left its runs recording SUCCESS on a server where the session existed nowhere.</para>
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

        /* A test replaces the whole ensure of one server. Null in production. */
        if (runner.XeEnsureOverrideForTests is { } ensureOverride)
        {
            await ensureOverride(server, cancellationToken);
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
            /* No #3477 scope here, deliberately: this list provisions the always-on SESSIONS for two
               collectors (deadlocks, blocked_process_report), each of which may carry a DIFFERENT
               scope — the scope is a COLLECTION predicate on each collector's own read fan-out, while
               the session inventory stays server-shaped. An unread session's ring buffer is bounded
               server-side; a session dropped because ONE collector was scoped would blind the other.
               The opt-in long-query trace is the other rule: it follows its own collector's scope
               (ReconcileLongQueryCompletionsAzureAsync). */
            databases = runner.AzureDatabaseListOverrideForTests is { } listOverride
                ? await listOverride(server, cancellationToken)
                : await runner.GetAzureDatabaseListAsync(server, databaseScope: null, cancellationToken);
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

            SqlConnection? connection = null;
            try
            {
                IAlwaysOnXeDatabase database;
                if (runner.AlwaysOnXeDatabaseForTests is { } open)
                {
                    database = await open(server, databaseName, cancellationToken);
                }
                else
                {
                    connection = await runner.OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
                    database = new DarlingAlwaysOnXeSessions.Database(connection);
                }

                /* #4961: the shared session when it is usable, this install's own when it is not, and back again. */
                await DarlingAlwaysOnXeSessions.EnsureAsync(
                    database, runner, server, databaseName, AlwaysOnXeSessionKind.Deadlock, "deadlock", logger, cancellationToken);
                await DarlingAlwaysOnXeSessions.EnsureAsync(
                    database, runner, server, databaseName, AlwaysOnXeSessionKind.BlockedProcess, "blocked process", logger, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning("[{Server}] [{Database}] Failed to open a connection for XE session ensure: {Message}",
                    server.Config.DisplayName, databaseName, ex.Message);
            }
            finally
            {
                connection?.Dispose();
            }
        }
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
            /* Threshold could not be set: the login lacks ALTER SETTINGS, or sp_configure
               is unavailable on the platform (AWS RDS / Azure SQL DB). Tolerated either way. */
            logger?.LogInformation("[{Server}] Could not auto-configure 'blocked process threshold (s)' to 5 seconds. This is expected when the monitoring login lacks ALTER SETTINGS, or on AWS RDS / Azure SQL DB where it is set via platform config. It is benign: blocking is still captured by the always-on DMV blocking snapshot; only the richer blocked-process-report XE stays off until the threshold is set. Detail: {Message}",
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
    ///
    /// <para><b>Returns the run-record note the reconcile owes the collector (#3754), or null.</b> On Azure
    /// SQL DB the session is per database, so "reconciled" has a middle state a server-scoped session
    /// cannot have: created in SOME monitored databases and refused in others. Before #3754 the Azure arm
    /// caught every per-database failure, logged it, and returned as if it had succeeded — so the worker
    /// latched the state as applied and never retried, and the collector's run then read the databases
    /// whose session did not exist, found nothing (the tolerant ring-buffer read returns zero rows for an
    /// absent session, by design), and recorded SUCCESS. On the issue's server that was EVERY database,
    /// every sweep: the CREATE was rejected in each one and <c>get_collection_health</c> reported the
    /// collector HEALTHY. Now: every database refused THROWS the first failure, exactly as the
    /// server-scoped arm has always thrown its one CREATE failure and as the runners' per-database loops
    /// throw when all databases fail (#2623), so the worker does not latch and the run is classified
    /// <c>SESSION_MISSING</c>; some databases refused returns the #2623 partial note naming them, which the
    /// worker merges onto the run's row. Null is the clean reconcile, and the disabled (DROP) arm's answer.
    /// A drop failure is not a capture outage, so it is not scored; on Azure SQL DB it throws
    /// <see cref="LongQueryTraceDropException"/> after every database was tried, and on every other engine the drop of the
    /// server's session throws it too, and the worker retries it with a cap, as Lite does.</para>
    /// <para><paramref name="pass"/>: which part runs (<see cref="LongQueryTracePass"/>). It changes only the Azure
    /// SQL DB arm, because the server-scoped arm has no drop while enabled.</para>
    /// <para><paramref name="createFailureWarned"/>: a create that already logged its failure at Warning (#4964). The
    /// Azure arm then logs the create side's failures at Debug, so a create that fails on every sweep warns once.</para>
    /// <para><paramref name="instanceGuard"/>: for a server's own session (every engine but Azure SQL Database), says whether
    /// another registration of this install keeps the trace on the same instance (#4961). It is called only when the trace is
    /// off and the drop is about to run, and a positive match leaves the session in place. Null means no registration keeps it.</para>
    /// </summary>
    public static async Task<string?> ReconcileLongQueryCompletionsAsync(
        ServerRuntime server,
        DarlingCollectorRunner runner,
        bool enabled,
        LongQueryTracePass pass,
        IReadOnlyList<LongQueryTraceRegistration> registrations,
        IReadOnlyList<string> serverSeparatelyMonitored,
        bool createFailureWarned,
        ILogger? logger,
        CancellationToken cancellationToken,
        Func<Task<LongQueryTraceInstanceGuard>>? instanceGuard = null)
    {
        /* Belt to the worker's braces: the caller gates on engine (a PostgreSQL target has no XE to
           reconcile), but this method constructs a SqlConnection from the engine-ambiguous connection
           string below, so it enforces its own precondition rather than trusting every present and future
           caller — the exact trust that put "Keyword not supported: 'host'" in the sweep log once a minute. */
        if (server.Target.Engine != PerformanceMonitor.Collectors.CollectorTargetEngine.SqlServer)
        {
            return null;
        }

        /* #4961: no install id, no session. Nothing is created and the session is never named from the legacy constant: an
           enabled trace throws, so the worker records the fault and the run reads SESSION_MISSING; a disabled one has no
           session of this install's to drop. */
        var sessionName = runner.LongQuerySessionName();
        if (sessionName is null)
        {
            if (!enabled)
            {
                return null;
            }

            throw new InvalidOperationException("The long-query trace was not created: this install has no id to name its Extended Events session.");
        }

        if (server.Target.IsAzureSqlDb)
        {
            return await ReconcileLongQueryCompletionsAzureAsync(server, runner, sessionName, enabled, pass, registrations, serverSeparatelyMonitored, createFailureWarned, logger, cancellationToken);
        }

        /* #4961: the session older versions shared between installs is dropped once, whether the trace is on or off, and the
           drop is recorded. Its failure waits until the per-install work has run, then takes the retry cap. */
        var legacy = runner.LegacyLongQuery;
        var legacyPass = await legacy.BeginPassAsync(server, pass, logger, cancellationToken);
        Task DropLegacyOnServer(string legacyName, CancellationToken token) => DropSessionOnServerAsync(server, runner, legacyName, token);

        if (!enabled)
        {
            await legacyPass.DropAsync(string.Empty, DropLegacyOnServer, cancellationToken);

            /* #4961: two registrations of this install can reach one instance under different host names, and one drop
               stops the trace the other keeps. A positive match leaves the session, and the reconcile counts as done, so
               no later sweep connects for it: the registration that keeps it drops it when it turns its own trace off. A
               name that is not known matches nothing, and the drop runs as it always did. The guard is resolved here, not
               before, so a server that never reaches this drop reads nothing. The legacy session was dropped above either way. */
            var keptByAnother = instanceGuard is not null && (await instanceGuard()).Kept;
            if (keptByAnother)
            {
                logger?.LogInformation(
                    "[{Server}] Long-query completion XE session left in place: another registration of this install keeps it on the same instance",
                    server.Config.DisplayName);
            }
            else
            {
                await DropLongQueryCompletionsOnServerAsync(server, runner, sessionName, cancellationToken);
            }

            if (legacyPass.ToException(Array.Empty<string>(), createNote: null, onServer: true) is { } legacyDropFailure)
            {
                throw legacyDropFailure;
            }

            if (!keptByAnother)
            {
                logger?.LogInformation("[{Server}] Long-query completion XE session reconciled OFF (collector disabled)", server.Config.DisplayName);
            }

            return null;
        }

        await legacyPass.DropAsync(string.Empty, DropLegacyOnServer, cancellationToken);

        /* A test replaces the server-scoped create, called with no database name. Null in production. */
        if (runner.LongQueryTraceDatabaseOverrideForTests is { } createOnServer)
        {
            await createOnServer(server, string.Empty, true, sessionName, cancellationToken);
            if (runner.LegacyLongQueryPresentForTests is { } presentOnServer && await presentOnServer(server, string.Empty, cancellationToken))
            {
                legacy.NoteFound(server, string.Empty, logger);
            }
        }
        else if (runner.LongQueryTraceStepOverrideForTests is { } stepOnServer)
        {
            /* Below it, a test replaces the open and the work, and sees the registration's own connection string (#4961). */
            await stepOnServer(server, string.Empty, server.ConnectionString, LongQueryTraceStep.CreateAndStart, sessionName, cancellationToken);
        }
        else
        {
            using var connection = new SqlConnection(server.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            await EnsureLongQueryCompletionsOnPremAsync(connection, server, sessionName, legacy, logger, cancellationToken);
        }

        /* One server-scoped session: it either exists now or the CREATE above threw. There is no partial. A failed legacy
           drop is the one thing left to report, after the create it did not stop. */
        if (legacyPass.ToException(Array.Empty<string>(), createNote: null, onServer: true) is { } legacyCreateFailure)
        {
            throw legacyCreateFailure;
        }

        return null;
    }

    /// <summary>
    /// The connection string for one database on an Azure SQL Database server: the registration's own, so a registration
    /// with read-only intent opens a read-only connection (#4961).
    /// </summary>
    private static string LongQueryTraceConnectionString(ServerRuntime server, string databaseName) =>
        SqlServerTargetProvider.Instance.WithDatabase(server.ConnectionString, databaseName);

    /// <summary>
    /// The steps one Azure SQL Database database's ensure takes for a registration, in order, with the connection string
    /// each one opens (#4961). Pure, so the production ensure and the test seam read the same plan.
    /// </summary>
    internal static IReadOnlyList<(LongQueryTraceStep Step, string ConnectionString)> LongQueryTraceStepsFor(string ownConnectionString)
    {
        var own = new SqlConnectionStringBuilder(ownConnectionString);
        if (own.ApplicationIntent != ApplicationIntent.ReadOnly)
        {
            return new[] { (LongQueryTraceStep.CreateAndStart, ownConnectionString) };
        }

        /* A session cannot be created on a read-only replica, and the definition replicates from the primary. So the
           definition is created over a connection without the intent, and not started there; the session is started over
           the registration's own connection, because run state is per replica. */
        own.ApplicationIntent = ApplicationIntent.ReadWrite;
        return new[]
        {
            (LongQueryTraceStep.CreateDefinition, own.ConnectionString),
            (LongQueryTraceStep.Start, ownConnectionString),
        };
    }

    /// <summary>
    /// Whether a failed create is a read-only database's refusal (error 3906): the error itself, or the one an exception
    /// wraps (#4961).
    /// </summary>
    internal static bool IsReadOnlyDatabaseRefusal(Exception? ex) =>
        LongQueryTraceDatabases.IsReadOnlyDatabaseRefusal(ErrorNumbersOf(ex));

    /// <summary>
    /// The numbers of the SQL errors a failure carries: its own and those of the exceptions it wraps. The shared project has
    /// no SqlClient, so each app reads them off its own exception (#4961).
    /// </summary>
    internal static List<int> ErrorNumbersOf(Exception? ex)
    {
        var numbers = new List<int>();
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is SqlException sql)
            {
                numbers.AddRange(sql.Errors.Cast<SqlError>().Select(e => e.Number));
            }
        }

        return numbers;
    }

    /// <summary>
    /// The Azure SQL Database create for a registration with read-only intent in one database (#4961): the definition over
    /// a connection without the intent, which does not start it, then the start over the registration's own read-only
    /// connection.
    /// </summary>
    private static async Task EnsureLongQueryCompletionsReadOnlyIntentAsync(
        ServerRuntime server, string databaseName, string sessionName, ILogger? logger, CancellationToken cancellationToken)
    {
        foreach (var (step, connectionString) in LongQueryTraceStepsFor(LongQueryTraceConnectionString(server, databaseName)))
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            try
            {
                if (step == LongQueryTraceStep.CreateDefinition)
                {
                    await CreateLongQueryCompletionsDefinitionAzureAsync(connection, server, databaseName, sessionName, logger, cancellationToken);
                }
                else
                {
                    await StartLongQueryCompletionsAzureAsync(connection, sessionName, cancellationToken);
                }
            }
            catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
            {
                /* Already present per the engine, or already started: the reader tolerates it (#1251). */
            }
        }
    }

    private static async Task CreateLongQueryCompletionsDefinitionAzureAsync(
        SqlConnection connection, ServerRuntime server, string databaseName, string sessionName, ILogger? logger, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            if (await cmd.ExecuteScalarAsync(cancellationToken) != null)
            {
                return;
            }
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(sessionName, databaseScoped: true, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds), connection);
        createCmd.CommandTimeout = 60;
        await ExecuteLongQueryAzureDdlAsync(createCmd, cancellationToken);
        logger?.LogInformation("[{Server}] [{Database}] Created the long-query completion XE session's definition over a connection without read-only intent (database-scoped)", server.Config.DisplayName, databaseName);
    }

    private static async Task StartLongQueryCompletionsAzureAsync(SqlConnection connection, string sessionName, CancellationToken cancellationToken)
    {
        using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{sessionName}'
)
BEGIN
    {LongQueryCompletionsCollector.BuildStartSessionSql(sessionName, databaseScoped: true)}
END;", connection);
        startCmd.CommandTimeout = 60;
        await ExecuteLongQueryAzureDdlAsync(startCmd, cancellationToken);
    }

    /// <summary>
    /// #4961: marks the failure of one create or start of the long-query session in an Azure SQL Database, so the failure line,
    /// the all-refused rethrow's message and the fault the run records carry the caps sentence
    /// (<see cref="AlwaysOnXeSessions.AzureCapsSentence"/>), as the deadlock and blocked-process sessions' do. The engine's
    /// "already there" is no failure, and a read-only database's refusal says its own reason, so neither is marked. Called only
    /// on the Azure SQL Database arm: no cap limits the server-scoped session of any other engine.
    /// </summary>
    private static void MarkLongQueryAzureFailure(Exception ex)
    {
        if (ex is SqlException sql && IsBenignXeSessionAlreadyPresent(sql))
        {
            return;
        }

        AlwaysOnXeSessions.MarkAzureCapsFailure(ex, ErrorNumbersOf(ex));
    }

    /// <summary>Runs one create or start statement of the Azure per-database ensure, and marks its failure (<see cref="MarkLongQueryAzureFailure"/>).</summary>
    private static async Task ExecuteLongQueryAzureDdlAsync(SqlCommand command, CancellationToken cancellationToken)
    {
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MarkLongQueryAzureFailure(ex);
            throw;
        }
    }

    /// <summary>
    /// Drops the server-scoped long-query session, on every engine but Azure SQL Database. A failure, the connection's
    /// included, throws <see cref="LongQueryTraceDropException"/> (<see cref="LongQueryTraceDropException.ForServer"/>) like
    /// the Azure arm's drops do, so the worker's cap applies to it (#4964). Left as it was, it reached the worker's general
    /// catch, which warns on every sweep with no end while the trace is off.
    /// </summary>
    private static async Task DropLongQueryCompletionsOnServerAsync(ServerRuntime server, DarlingCollectorRunner runner, string sessionName, CancellationToken cancellationToken)
    {
        try
        {
            await DropSessionOnServerAsync(server, runner, sessionName, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw LongQueryTraceDropException.ForServer(ex);
        }
    }

    /// <summary>Drops one session on the server, and lets the failure through as it is.</summary>
    private static async Task DropSessionOnServerAsync(ServerRuntime server, DarlingCollectorRunner runner, string sessionName, CancellationToken cancellationToken)
    {
        /* A test replaces the server-scoped drop, called with no database name. Null in production. */
        if (runner.LongQueryTraceDatabaseOverrideForTests is { } dropOnServer)
        {
            await dropOnServer(server, string.Empty, false, sessionName, cancellationToken);
            return;
        }

        using var connection = new SqlConnection(server.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await DropLongQueryCompletionsAsync(connection, sessionName, databaseScoped: false, cancellationToken);
    }

    private static async Task EnsureLongQueryCompletionsOnPremAsync(SqlConnection connection, ServerRuntime server, string sessionName, DarlingLegacyLongQuerySession legacy, ILogger? logger, CancellationToken cancellationToken)
    {
        int? isRunning = null;
        var legacyPresent = false;

        /* The second SELECT asks whether an older install created the legacy session again, in the same batch (#4961). */
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END
FROM sys.server_event_sessions AS ses
LEFT JOIN sys.dm_xe_sessions AS dxs
  ON dxs.name = ses.name
WHERE ses.name = @session_name;

SELECT /* PerformanceMonitorDarling */
    legacy_present = CASE WHEN EXISTS (SELECT 1/0 FROM sys.server_event_sessions AS les WHERE les.name = @legacy_name) THEN 1 ELSE 0 END;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            cmd.Parameters.Add(new SqlParameter("@legacy_name", SqlDbType.NVarChar, 128) { Value = DarlingLegacyLongQuerySession.SessionName });
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                isRunning = reader.GetInt32(0);
            }

            if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
            {
                legacyPresent = reader.GetInt32(0) == 1;
            }
        }

        if (legacyPresent)
        {
            legacy.NoteFound(server, string.Empty, logger);
        }

        if (isRunning is not null)
        {
            if (isRunning == 0)
            {
                using var startCmd = new SqlCommand(LongQueryCompletionsCollector.BuildStartSessionSql(sessionName, databaseScoped: false), connection);
                startCmd.CommandTimeout = 60;
                await startCmd.ExecuteNonQueryAsync(cancellationToken);
                logger?.LogInformation("[{Server}] Started long-query completion XE session", server.Config.DisplayName);
            }

            return;
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(sessionName, databaseScoped: false, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(sessionName, databaseScoped: false), connection);
        createCmd.CommandTimeout = 60;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        logger?.LogInformation("[{Server}] Created and started long-query completion XE session", server.Config.DisplayName);
    }

    /// <summary>
    /// The Azure SQL DB arm: one database-scoped session per monitored database, in the databases
    /// <see cref="LongQueryTraceDatabases.Plan"/> names (shared with Lite). ENABLED: created in each monitored
    /// database, then dropped from each listed database outside that set. DISABLED: dropped from every listed
    /// database, exclusions and scope included. Neither touches a database monitored as its own server, and neither
    /// drops the session from a database where another registration of the server keeps it
    /// (<see cref="LongQueryTraceDatabases.KeptElsewhere"/>). Returns the #2623 partial note when the ENABLE was refused in some databases, throws the first failure
    /// when it was refused in all of them, and returns null otherwise — see
    /// <see cref="ReconcileLongQueryCompletionsAsync"/> for why the middle state exists only here and what each
    /// answer makes the worker do (#3754). A failed listing throws too. A failed drop throws
    /// <see cref="LongQueryTraceDropException"/> after every database was tried, carrying the partial note, so
    /// the worker retries it up to <see cref="LongQueryTraceDatabases.DropAttemptCap"/> times in a row, then once an
    /// hour. A <see cref="LongQueryTracePass.CreateOnly"/> pass stops after the create side.
    /// <para>The create side has no cap on its attempts, on purpose. Its listing failure rethrows as it is, so the worker
    /// leaves the trace unapplied and a login that cannot read master on a logical server retries the create on every
    /// sweep: the fault is recorded again, and the run reads <c>SESSION_MISSING</c>. The CREATE path where every database
    /// refuses has the same cadence. What is capped is the log: the first failed pass logs its lines at Warning, and the
    /// passes after it log them at Debug (<paramref name="createFailureWarned"/>, #4964) until a create succeeds or the
    /// server reconnects. The drop side has its own cap, because its failures, the listing included, arrive as
    /// <see cref="LongQueryTraceDropException"/>.</para>
    /// </summary>
    private static async Task<string?> ReconcileLongQueryCompletionsAzureAsync(
        ServerRuntime server,
        DarlingCollectorRunner runner,
        string sessionName,
        bool enabled,
        LongQueryTracePass pass,
        IReadOnlyList<LongQueryTraceRegistration> registrations,
        IReadOnlyList<string> serverSeparatelyMonitored,
        bool createFailureWarned,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        /* The hourly attempt after the cap logs each failed drop at Debug, so the cap's one warning is not repeated. */
        var afterTheCap = pass == LongQueryTracePass.RetryAfterCap;

        /* #4964: the level of every failure the create side logs in this pass. A create that already warned logs its
           repeats at Debug; the retry and the fault the worker records do not change. */
        var createFailureLevel = createFailureWarned ? LogLevel.Debug : LogLevel.Warning;

        /* Two lifecycle rules live in this class, on purpose. The always-on deadlock and blocked-process
           sessions (EnsureDatabaseScopedAsync) follow the inventory: every database the server lists, with no
           #3477 scope, because they are cheap, the alerts read them, and one list provisions both while each
           collector may carry its own scope. This opt-in trace follows the monitored set: the inventory narrowed by the scope its
           own read loop resolves (DarlingCollectorRunner.DatabaseScopeFor), minus the databases monitored as
           their own servers, whose registrations own those sessions. The trace costs overhead, so it must not
           run in a database the user left out. */
        var separatelyMonitored = runner.SeparatelyMonitoredDatabasesFor(server);

        /* The databases where another registration of this logical server keeps the session: a drop here would
           remove it for that registration too, since the session is one object per database. */
        IReadOnlyList<string> KeptElsewhere(IEnumerable<string> candidates) =>
            LongQueryTraceDatabases.KeptElsewhere(
                server.ServerId.ToString(CultureInfo.InvariantCulture), server.Config.Host, candidates, registrations, serverSeparatelyMonitored);

        /* #4961: the legacy session is dropped once in each database, then recorded (DarlingLegacyLongQuerySession). */
        var legacyPass = await runner.LegacyLongQuery.BeginPassAsync(server, pass, logger, cancellationToken);

        /* Every listed database but master and the databases monitored as their own servers, which own their own drops. */
        IReadOnlyList<string> LegacyDatabases(IEnumerable<string> listed) =>
            LongQueryTraceDatabases.Plan(enabled: false, listed, Array.Empty<string>(), separatelyMonitored, keptElsewhere: Array.Empty<string>()).Drop;

        if (!enabled)
        {
            var listed = await ListEveryDatabaseForTheTraceAsync(server, runner, createNote: null, cancellationToken);
            var off = LongQueryTraceDatabases.Plan(
                enabled: false,
                listed,
                Array.Empty<string>(),
                separatelyMonitored,
                KeptElsewhere(listed));

            /* #4961: the legacy session goes from every listed database but master, whoever else keeps a session there: it
               is not the per-install session. The same one listing serves both drops. */
            var legacyOff = LegacyDatabases(listed);
            foreach (var legacyDatabase in legacyOff)
            {
                await legacyPass.DropAsync(legacyDatabase, (legacyName, token) => DropSessionInDatabaseAsync(server, runner, legacyName, legacyDatabase, token), cancellationToken);
            }

            LongQueryTraceDropException? offFailure = null;
            try
            {
                await DropLongQueryTraceInEachAsync(server, runner, sessionName, off.Drop, createNote: null, afterTheCap, logger, cancellationToken);
            }
            catch (LongQueryTraceDropException ex)
            {
                offFailure = ex;
            }

            if (LegacyLongQueryDropPass.Merge(offFailure, legacyPass.ToException(legacyOff, createNote: null, onServer: false)) is { } offFailed)
            {
                throw offFailed;
            }

            return null;
        }

        List<string> monitored;
        try
        {
            monitored = await runner.ListLongQueryTraceDatabasesAsync(
                server,
                allDatabases: false,
                runner.DatabaseScopeFor(LongQueryCompletionsCollector.Instance.Name, server.ServerId),
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Thrown, not returned: a return here made the worker count the trace applied with no session
               created, until the next reconnect. Now the next sweep tries again. */
            logger?.Log(createFailureLevel, "[{Server}] Failed to enumerate databases for the long-query completion XE session: {Message}", server.Config.DisplayName, ex.Message);
            throw;
        }

        /* #3754: the per-database account, in the shape both runners' Azure loops keep (#2623) - attempted
           against failed, the names, the first exception whole. Only the ENABLE side is scored: a database
           whose session could not be created is a database whose completions will never be captured,
           which is the fact the collector's run record has to carry. */
        var attempted = 0;
        var failed = 0;
        var failedDatabases = new List<string>();
        Exception? firstFailure = null;

        /* Every database the server lists, read once for both drops of this pass: the legacy session's, in each database before
           that database's create, and the per-install session's outside the monitored set after the creates. A listing that
           fails does not stop the creates. It fails the pass after them, as it always has. */
        List<string>? listedForTheDrop = null;
        Exception? listFailure = null;
        if (pass != LongQueryTracePass.CreateOnly)
        {
            try
            {
                listedForTheDrop = await runner.ListLongQueryTraceDatabasesAsync(server, allDatabases: true, databaseScope: null, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                listFailure = ex;
            }
        }

        var legacyDatabases = listedForTheDrop is null ? Array.Empty<string>() : LegacyDatabases(listedForTheDrop);
        var legacyBeforeCreate = new HashSet<string>(legacyDatabases, StringComparer.OrdinalIgnoreCase);
        var created = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var databaseName in LongQueryTraceDatabases.Plan(enabled: true, Array.Empty<string>(), monitored, separatelyMonitored, keptElsewhere: Array.Empty<string>()).Create)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempted++;
            created.Add(databaseName);

            /* The legacy drop comes first, so it frees the slot the create needs. Its failure does not stop the create. */
            if (legacyBeforeCreate.Contains(databaseName))
            {
                await legacyPass.DropAsync(databaseName, (legacyName, token) => DropSessionInDatabaseAsync(server, runner, legacyName, databaseName, token), cancellationToken);
            }

            try
            {
                if (runner.LongQueryTraceDatabaseOverrideForTests is { } inDatabase)
                {
                    await inDatabase(server, databaseName, true, sessionName, cancellationToken);
                    if (runner.LegacyLongQueryPresentForTests is { } presentInDatabase && await presentInDatabase(server, databaseName, cancellationToken))
                    {
                        runner.LegacyLongQuery.NoteFound(server, databaseName, logger);
                    }

                    continue;
                }

                /* Below it, a test replaces each step's open and work, and sees the connection string the step would have
                   opened (#4961). */
                if (runner.LongQueryTraceStepOverrideForTests is { } stepInDatabase)
                {
                    foreach (var (kind, connectionString) in LongQueryTraceStepsFor(LongQueryTraceConnectionString(server, databaseName)))
                    {
                        try
                        {
                            await stepInDatabase(server, databaseName, connectionString, kind, sessionName, cancellationToken);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            /* The step stands for one create or start statement, so its failure is marked like the statement's. */
                            MarkLongQueryAzureFailure(ex);
                            throw;
                        }
                    }

                    continue;
                }

                /* #4961: a registration with read-only intent creates the definition over a connection without it, and starts
                   the session over its own. */
                if (new SqlConnectionStringBuilder(LongQueryTraceConnectionString(server, databaseName)).ApplicationIntent == ApplicationIntent.ReadOnly)
                {
                    await EnsureLongQueryCompletionsReadOnlyIntentAsync(server, databaseName, sessionName, logger, cancellationToken);
                    continue;
                }

                using var connection = await runner.OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);

                try
                {
                    await EnsureLongQueryCompletionsAzureAsync(connection, server, databaseName, sessionName, runner.LegacyLongQuery, logger, cancellationToken);
                }
                catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
                {
                    /* Already present per the engine — the reader tolerates it (#1251). */
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* Still tolerated per database - one database that cannot host the session must not stop
                   the others from getting theirs - and still logged here, at the database, because this is
                   the one line that names which database refused. #3754 adds the ACCOUNT beside the log so
                   the failure reaches the run record and not only the service log. The database name rides
                   on the exception (#2997) for the all-failed rethrow: the worker composes the run's
                   SESSION_MISSING message from it, and its only other source of a name is the runtime's
                   connected database, which is master here and never the one that refused. */
                failed++;
                failedDatabases.Add(databaseName);
                CollectorFaultDatabase.Stamp(ex, databaseName);
                firstFailure ??= ex;

                /* #4961: a read-only database gets the one message that says why and what to change, in place of the
                   server's own. */
                if (IsReadOnlyDatabaseRefusal(ex))
                {
                    logger?.Log(createFailureLevel, "[{Server}] [{Database}] {Message}",
                        server.Config.DisplayName, databaseName, LongQueryTraceDatabases.ReadOnlyDatabaseMessage());
                }
                else
                {
                    logger?.Log(createFailureLevel, "[{Server}] [{Database}] Failed to reconcile the long-query completion XE session: {Message}",
                        server.Config.DisplayName, databaseName, AlwaysOnXeSessions.DescribeFailure(ex));
                }
            }
        }

        /* Every database refused: capture is dead on this server, and returning normally here is what let the
           worker latch "applied" and the collector record SUCCESS. Surface the first failure raw - its type
           and number are what the worker's fault arms classify on - after the one summary line the runners'
           all-failed arms also write. Mirrors Lite's EnsureDatabaseScopedXeSessionsAsync, which throws on
           healthy == 0 for the same reason. */
        if (attempted > 0 && failed == attempted && firstFailure is not null)
        {
            logger?.Log(IsReadOnlyDatabaseRefusal(firstFailure) ? LogLevel.Debug : createFailureLevel, "[{Server}] long_query_completions XE session could not be ensured in all {Count} database(s); surfacing the first failure",
                server.Config.DisplayName, attempted);
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstFailure).Throw();
        }

        /* Some refused: the session exists where it could, and the run that reads those databases is a real
           read - SUCCESS is right for it - but its row must say that the others are not in it. The shared
           #2623 composer, so this loss is worded exactly as the runners word theirs; null when nothing
           failed, which is the ordinary sweep. */
        var partialNote = EnumeratedCollectorDriver.BuildPartialFailureNote(failed, attempted, failedDatabases, firstFailure?.Message);

        /* The worker's hourly pass while the trace is on stops here: it brings back a session dropped from outside,
           and leaves the drop side to a connect, a change of the state key, or an attempt after the cap. */
        if (pass == LongQueryTracePass.CreateOnly)
        {
            return partialNote;
        }

        /* Then drop the session from each listed database outside the monitored set: a database excluded, or
           taken out of the scope, since the session was created there. */
        if (listedForTheDrop is null)
        {
            throw new LongQueryTraceDropException(Array.Empty<string>(), listFailure!, partialNote);
        }

        var outside = LongQueryTraceDatabases.Plan(
            enabled: true,
            listedForTheDrop,
            monitored,
            separatelyMonitored,
            KeptElsewhere(listedForTheDrop));

        /* The legacy drops the creates did not reach: the databases outside the monitored set. */
        foreach (var legacyDatabase in legacyDatabases.Where(database => !created.Contains(database)))
        {
            await legacyPass.DropAsync(legacyDatabase, (legacyName, token) => DropSessionInDatabaseAsync(server, runner, legacyName, legacyDatabase, token), cancellationToken);
        }

        LongQueryTraceDropException? outsideFailure = null;
        try
        {
            await DropLongQueryTraceInEachAsync(server, runner, sessionName, outside.Drop, partialNote, afterTheCap, logger, cancellationToken);
        }
        catch (LongQueryTraceDropException ex)
        {
            outsideFailure = ex;
        }

        if (LegacyLongQueryDropPass.Merge(outsideFailure, legacyPass.ToException(legacyDatabases, partialNote, onServer: false)) is { } cleanupFailure)
        {
            throw cleanupFailure;
        }

        return partialNote;
    }

    /// <summary>
    /// Every online database, with no exclusions and no scope, for a drop. A failure to list them throws
    /// <see cref="LongQueryTraceDropException"/> with no names, so the worker retries it like a failed drop.
    /// </summary>
    private static async Task<List<string>> ListEveryDatabaseForTheTraceAsync(ServerRuntime server, DarlingCollectorRunner runner, string? createNote, CancellationToken cancellationToken)
    {
        try
        {
            return await runner.ListLongQueryTraceDatabasesAsync(server, allDatabases: true, databaseScope: null, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new LongQueryTraceDropException(Array.Empty<string>(), ex, createNote);
        }
    }

    /// <summary>
    /// Drops the session in each database, tries every one, and logs a warning that names each database where the
    /// drop failed (<see cref="LongQueryTraceDatabases.DropEachAsync"/>). The hourly attempt after the cap logs it at
    /// Debug instead.
    /// </summary>
    private static Task DropLongQueryTraceInEachAsync(ServerRuntime server, DarlingCollectorRunner runner, string sessionName, IReadOnlyList<string> databases, string? createNote, bool afterTheCap, ILogger? logger, CancellationToken cancellationToken) =>
        LongQueryTraceDatabases.DropEachAsync(
            databases,
            (databaseName, token) => DropSessionInDatabaseAsync(server, runner, sessionName, databaseName, token),
            (databaseName, ex) => logger?.Log(
                afterTheCap ? LogLevel.Debug : LogLevel.Warning,
                "[{Server}] [{Database}] Failed to drop the long-query completion XE session: {Message}",
                server.Config.DisplayName, databaseName, ex.Message),
            createNote,
            cancellationToken);

    /// <summary>Drops one session in one database, and lets the failure through as it is.</summary>
    private static async Task DropSessionInDatabaseAsync(ServerRuntime server, DarlingCollectorRunner runner, string sessionName, string databaseName, CancellationToken cancellationToken)
    {
        if (runner.LongQueryTraceDatabaseOverrideForTests is { } inDatabase)
        {
            await inDatabase(server, databaseName, false, sessionName, cancellationToken);
            return;
        }

        using var connection = await runner.OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
        await DropLongQueryCompletionsAsync(connection, sessionName, databaseScoped: true, cancellationToken);
    }

    private static async Task EnsureLongQueryCompletionsAzureAsync(SqlConnection connection, ServerRuntime server, string databaseName, string sessionName, DarlingLegacyLongQuerySession legacy, ILogger? logger, CancellationToken cancellationToken)
    {
        string? existing = null;
        var legacyPresent = false;

        /* The second SELECT asks whether an older install created the legacy session again, in the same batch (#4961). */
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;

SELECT /* PerformanceMonitorDarling */
    legacy_present = CASE WHEN EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS les WHERE les.name = @legacy_name) THEN 1 ELSE 0 END;", connection))
        {
            cmd.CommandTimeout = 60;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            cmd.Parameters.Add(new SqlParameter("@legacy_name", SqlDbType.NVarChar, 128) { Value = DarlingLegacyLongQuerySession.SessionName });
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                existing = reader.GetString(0);
            }

            if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
            {
                legacyPresent = reader.GetInt32(0) == 1;
            }
        }

        if (legacyPresent)
        {
            legacy.NoteFound(server, databaseName, logger);
        }

        if (existing != null)
        {
            using var startCmd = new SqlCommand($@"
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.dm_xe_database_sessions AS xes
    WHERE xes.name = N'{sessionName}'
)
BEGIN
    ALTER EVENT SESSION [{sessionName}] ON DATABASE STATE = START;
END;", connection);
            startCmd.CommandTimeout = 60;
            await ExecuteLongQueryAzureDdlAsync(startCmd, cancellationToken);
            return;
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(sessionName, databaseScoped: true, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(sessionName, databaseScoped: true), connection);
        createCmd.CommandTimeout = 60;
        await ExecuteLongQueryAzureDdlAsync(createCmd, cancellationToken);
        logger?.LogInformation("[{Server}] [{Database}] Created and started long-query completion XE session (database-scoped)", server.Config.DisplayName, databaseName);
    }

    private static async Task DropLongQueryCompletionsAsync(SqlConnection connection, string sessionName, bool databaseScoped, CancellationToken cancellationToken)
    {
        using var cmd = new SqlCommand(LongQueryCompletionsCollector.BuildDropSessionSql(sessionName, databaseScoped), connection);
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

/// <summary>
/// Which part of the long-query trace's reconcile runs (<see cref="DarlingXeSessions.ReconcileLongQueryCompletionsAsync"/>).
/// The worker's latch decides, in <c>DarlingWorker.ReconcileLongQueryTraceAsync</c>.
/// </summary>
public enum LongQueryTracePass
{
    /// <summary>The whole reconcile: after a connect, or after a change of the state key.</summary>
    Full,

    /// <summary>
    /// The hourly pass while the trace is on: the create side alone, so a session dropped from outside comes back.
    /// </summary>
    CreateOnly,

    /// <summary>
    /// The hourly attempt after the cleanup's cap (<see cref="LongQueryTraceDropRetry"/>): the whole reconcile, with
    /// each failed drop logged at Debug.
    /// </summary>
    RetryAfterCap,
}
