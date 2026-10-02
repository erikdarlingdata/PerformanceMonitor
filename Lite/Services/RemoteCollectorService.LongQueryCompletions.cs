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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /* The session name + DDL live in the shared definition so the ring-buffer reader and this
       lifecycle can never disagree on them (#1496). The name is this install's own (#4961), so it is a call, not a
       constant: it comes from the install id. */
    private string? LongQuerySessionName() =>
        LongQueryCompletionsCollector.TryXeSessionNameFor(LongQueryCompletionsCollector.LiteProduct, GetInstallId());

    /// <summary>
    /// The fault the reconcile kept for this server, or null: what the long-query collector's next run rethrows instead
    /// of reading a session that does not exist.
    /// </summary>
    internal Exception? LongQueryTraceFaultState(string serverId) =>
        _longQueryTraceFault.TryGetValue(serverId, out var fault) ? fault : null;

    /* Per-server last-applied state for the long-query trace's XE session, so the reconcile does not
       open a connection every cycle for a server whose state has not changed. Keyed by server id;
       unset = not yet reconciled. In-memory (cleared on app restart, which then re-reconciles once).
       Enabled true = the session is being ensured; false = confirmed dropped. StateKey is what the
       Azure SQL Database plan depended on (LongQueryTraceDatabases.StateKey): when the exclusions or
       the databases monitored as their own servers change, the last reconcile no longer says where
       the session belongs, so it runs again. Empty on every other engine. */
    private readonly ConcurrentDictionary<string, (bool Enabled, string StateKey)> _longQueryTraceApplied = new();

    /* Failed cleanup passes in a row, per server, for LongQueryTraceDatabases.DropAttemptCap, and the clock for the
       hourly attempts after the cap. */
    private readonly ConcurrentDictionary<string, LongQueryTraceDropRetry> _longQueryTraceDropRetry = new();

    /* #3754: the ENABLE failure the reconcile below caught, per server, kept until a later reconcile
       succeeds. The reconcile runs from the per-server collection loop, OUTSIDE the collector's run - so
       when the session could not be created (every monitored database refused it on Azure SQL DB; the
       one CREATE refused on-prem) the run still went ahead, its ring-buffer read (tolerant of a missing
       session then, before #4731) found no session, returned zero rows exactly as a quiet session
       would, and the cycle recorded SUCCESS.
       The blocked-process and deadlock collectors do not have this hole because their ensure runs
       INSIDE RunCollectorAsync and throws XeSessionEnsureException straight into its classification;
       this slot is how the long-query collector's out-of-run ensure reaches the same arm. The
       exception is kept whole, not its message, because that arm classifies on the type and the inner
       SqlException's number (PERMISSIONS for a denied ALTER ANY EVENT SESSION, ERROR otherwise). */
    private readonly ConcurrentDictionary<string, Exception> _longQueryTraceFault = new();

    /* #4964: the servers whose create has already logged its failure at Warning, kept until a later create succeeds. The
       create runs on every cycle, on purpose: each attempt keeps its fault (above), so the run reads SESSION_MISSING. Only
       the log level of a repeated failure changes, from Warning to Debug. Lite has no reconnect to reset it, because it
       creates on every cycle: a reconnect is the first create that succeeds. In memory, so a restart warns again. */
    private readonly ConcurrentDictionary<string, bool> _longQueryTraceCreateWarned = new();

    /* The engine edition the reconcile judges a server by. Its own instance, because the app's other one lives in the main
       window. A known live edition wins and is remembered, so a blank status that a failed connection check wrote leaves the
       server judged by the edition it had. Its dictionary is concurrent: the per-server tasks run in parallel. */
    private readonly KnownEngineEditions _engineEditions = new();

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
    ///
    /// <para>On Azure SQL Database, where each database holds its own session, the databases come from
    /// <see cref="LongQueryTraceDatabases.Plan"/>, shared with Darling. Enabled: created in each monitored
    /// database, and dropped from listed databases outside that set whenever the plan's settings change
    /// (and once after each start). Disabled: dropped from every listed database, exclusions included.
    /// Both leave alone a database monitored as its own server: its own registration owns that session.
    /// Neither drops the session from a database where another registration of the same logical server has the
    /// trace on and keeps it (<see cref="LongQueryTraceDatabases.KeptElsewhere"/>). A drop that fails is retried on the next cycles, up to <see cref="LongQueryTraceDatabases.DropAttemptCap"/>
    /// failed passes in a row; then one warning names the databases where the session may remain, and the drop is
    /// tried again once an hour, logged at Debug.</para>
    /// </summary>
    public async Task ReconcileLongQueryCompletionsXeSessionAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        var schedule = _scheduleManager.GetScheduleForServer(server.Id, "long_query_completions");
        var enabled = schedule?.Enabled ?? false;

        /* Read once. The ensure and the drop below get this answer: a status that changes during the reconcile (a blank one
           from a failed connection check, then 5 again) must not send the drop down the Azure branch with none of the
           registrations this reconcile worked from. */
        var isAzureSqlDatabase = _engineEditions.IsAzureSqlDatabase(server, _serverManager.GetConnectionStatus(server.Id).SqlEngineEdition);
        var separatelyMonitored = isAzureSqlDatabase ? SeparatelyMonitoredDatabasesFor(server) : Array.Empty<string>();

        /* Azure SQL Database: the other registrations of this logical server, so a drop leaves a database where one
           of them keeps the session (LongQueryTraceDatabases.KeptElsewhere). They are in the state key too: a drop
           skipped for one of them runs once that one turns its trace off. */
        IReadOnlyList<LongQueryTraceRegistration> registrations = isAzureSqlDatabase
            ? LongQueryTraceRegistrationsFor(server)
            : Array.Empty<LongQueryTraceRegistration>();
        var serverSeparatelyMonitored = isAzureSqlDatabase
            ? KnownEngineEditions.SeparatelyMonitoredDatabasesOnServer(server.ServerName, _serverManager.GetAllServers())
            : Array.Empty<string>();
        IReadOnlyList<string> KeptElsewhere(IEnumerable<string> candidates) =>
            LongQueryTraceDatabases.KeptElsewhere(server.Id, server.ServerName, candidates, registrations, serverSeparatelyMonitored);

        var stateKey = isAzureSqlDatabase
            ? LongQueryTraceDatabases.StateKey(
                enabled,
                databaseScope: null,
                server.ExcludedDatabases,
                separatelyMonitored,
                LongQueryTraceDatabases.CoOwners(server.Id, server.ServerName, registrations, serverSeparatelyMonitored))
            : string.Empty;

        /* After the cap, the cleanup runs once an hour, on this clock. That attempt logs its failures at Debug, so
           the one warning is not repeated. */
        var utcNow = LongQueryTraceUtcNowForTests?.Invoke() ?? DateTime.UtcNow;
        var retry = _longQueryTraceDropRetry.GetOrAdd(server.Id, _ => new LongQueryTraceDropRetry());
        var afterTheCap = retry.RetryDue(stateKey, utcNow);

        /* #4964: a create that already warned logs its repeated failure at Debug. Turning the trace off ends the run of
           create failures, so a failure after turning it on again warns again. */
        if (!enabled)
        {
            _longQueryTraceCreateWarned.TryRemove(server.Id, out _);
        }

        var createRepeats = enabled && _longQueryTraceCreateWarned.ContainsKey(server.Id);

        try
        {
            var sessionName = LongQuerySessionName();
            if (sessionName is null)
            {
                /* #4961: no install id, no session. A host built without an id store has no name to make, and never falls
                   back to the legacy one. Enabled: nothing is created, and the run records why as a fault. Disabled: there is
                   no session of this install's to drop. */
                if (enabled)
                {
                    var noId = new InvalidOperationException("The long-query trace was not created: this install has no id to name its Extended Events session.");
                    var noIdLine = $"[{server.DisplayName}] {noId.Message}";
                    if (createRepeats)
                    {
                        AppLogger.Debug("XeSession", noIdLine);
                    }
                    else
                    {
                        AppLogger.Warn("XeSession", noIdLine);
                    }

                    _longQueryTraceFault[server.Id] = noId;
                    _longQueryTraceCreateWarned[server.Id] = true;
                }
                else
                {
                    _longQueryTraceFault.TryRemove(server.Id, out _);
                }

                return;
            }

            if (enabled)
            {
                var monitored = await EnsureLongQueryCompletionsXeSessionAsync(server, sessionName, isAzureSqlDatabase, separatelyMonitored, createRepeats, cancellationToken);

                /* #3754: the session exists (everywhere it could) - a fault from an earlier cycle is over. */
                _longQueryTraceFault.TryRemove(server.Id, out _);
                _longQueryTraceCreateWarned.TryRemove(server.Id, out _);

                /* The server's own session has no cleanup pass while the trace is on, so a create that succeeded is the end of
                   a run of failed drops: the next time it is turned off, the count starts again (#4964). */
                if (monitored is null)
                {
                    retry.Reset();
                }

                /* Azure SQL Database: drop the session from listed databases outside the monitored set, when
                   the plan's settings changed since the last pass that finished (and once after each start), or
                   when the hourly attempt after the cap is due. */
                if (monitored is not null && (afterTheCap || !IsLongQueryTraceApplied(server.Id, enabled: true, stateKey)))
                {
                    await DropLongQueryTraceOutsideTheSetAsync(server, sessionName, monitored, separatelyMonitored, KeptElsewhere, afterTheCap, cancellationToken);
                    retry.Reset();
                }

                MarkLongQueryTraceApplied(server.Id, enabled: true, stateKey);
            }
            else if (afterTheCap || !IsLongQueryTraceApplied(server.Id, enabled: false, stateKey))
            {
                /* Disabled and either never reconciled, previously enabled, or reconciled under different
                   settings: drop, then remember it is gone so the next cycles skip the connection entirely. */
                await DropLongQueryCompletionsXeSessionAsync(server, sessionName, isAzureSqlDatabase, separatelyMonitored, KeptElsewhere, afterTheCap, cancellationToken);
                retry.Reset();
                MarkLongQueryTraceApplied(server.Id, enabled: false, stateKey);

                /* #3754: nothing to be honest about while disabled - the collector is not dispatched - and a
                   fault left here would classify the first run after re-enabling before its reconcile ran. */
                _longQueryTraceFault.TryRemove(server.Id, out _);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (LongQueryTraceDropException ex)
        {
            /* Azure SQL Database: a drop failed, or the databases could not be listed for it. The state stays
               unapplied so the next cycle tries again, until the cap: then it counts as done, and one warning
               names where the session may remain. After that, one attempt an hour, logged at Debug, so the
               warning is not repeated. While enabled, the create side had already finished. */
            switch (retry.RecordFailure(stateKey, utcNow))
            {
                case LongQueryTraceDropOutcome.GaveUp:
                    _longQueryTraceApplied[server.Id] = (enabled, stateKey);
                    AppLogger.Warn("XeSession", $"[{server.DisplayName}] {LongQueryTraceDatabases.GiveUpWarning(ex)}");
                    break;
                case LongQueryTraceDropOutcome.TryAgainInAnHour:
                    AppLogger.Debug("XeSession", $"[{server.DisplayName}] {ex.Message} The next attempt is in an hour.");
                    break;
                default:
                    AppLogger.Warn("XeSession", $"[{server.DisplayName}] {ex.Message} The next cycle tries again.");
                    break;
            }
        }
        catch (Exception ex)
        {
            /* Only the create side lands here (#4964): every failure of the drop half, on both arms, reaches the catch above
               as a LongQueryTraceDropException - the Azure arm's listing and each database's drop, and the server's own drop
               through ForServer - and a cancellation is rethrown before either. So a disabled trace never gets here, and this
               catch needs no clock move, unlike Darling's RetryAfterCap arm: Lite runs the create on every cycle anyway, so
               there is no pass whose repeats a due clock would multiply. A create that threw ahead of the hourly drop attempt
               leaves that attempt due, and the next cycle's create, once it succeeds, reaches it.

               Leave the applied state unchanged so the next cycle retries; a failed reconcile must
               never break the collection loop. #4964: the first failure of a create that cannot succeed logs at Warning;
               the cycles after it retry just the same, and keep the fault just the same below, but log at Debug until a
               create succeeds. */
            var reconcileFailure = $"[{server.DisplayName}] Failed to reconcile long-query completion XE session: {ex.Message}";
            if (createRepeats)
            {
                AppLogger.Debug("XeSession", reconcileFailure);
            }
            else
            {
                AppLogger.Warn("XeSession", reconcileFailure);
            }

            /* #3754: and while ENABLING, remember it, so this cycle's run of the collector is classified
               from this exception instead of reading an absent session as a quiet one. A DISABLING failure
               records nothing: no run is dispatched while disabled, and the retry the unchanged applied
               state already buys is the whole remedy. */
            if (enabled)
            {
                _longQueryTraceFault[server.Id] = ex;
                _longQueryTraceCreateWarned[server.Id] = true;
            }
        }
    }

    /// <summary>
    /// Ensures the long-query completion XE session exists and is running. Server-scoped on
    /// on-prem/MI/RDS; on Azure SQL DB a database-scoped session in each monitored database (#1535),
    /// except a database monitored as its own server, matching the per-database ring-buffer read
    /// (<see cref="LongQueryCompletionsCollector.RunsPerDatabase"/>,
    /// <see cref="LongQueryCompletionsCollector.SkipsSeparatelyMonitoredDatabases"/>). Returns the monitored
    /// databases on Azure SQL DB, for the drop outside that set; null on every other engine.
    /// <paramref name="createRepeats"/> is true once a create has warned for this server (#4964): the Azure arm's own
    /// failure lines, and the shared ensure's, then log at Debug.
    /// </summary>
    private async Task<List<string>?> EnsureLongQueryCompletionsXeSessionAsync(
        ServerConnection server, string sessionName, bool isAzureSqlDatabase, IReadOnlyList<string> separatelyMonitored, bool createRepeats, CancellationToken cancellationToken)
    {
        if (isAzureSqlDatabase)
        {
            List<string> monitored;
            try
            {
                monitored = await ListLongQueryTraceDatabasesAsync(server, allDatabases: false, cancellationToken);
            }
            catch (SqlException ex)
            {
                var listingFailure = $"[{server.DisplayName}] Failed to enumerate databases for long query completions XE sessions: {ex.Message}";
                if (createRepeats)
                {
                    AppLogger.Debug("XeSession", listingFailure);
                }
                else
                {
                    AppLogger.Error("XeSession", listingFailure);
                }

                throw new XeSessionEnsureException("long query completions", ex);
            }

            var create = LongQueryTraceDatabases.Plan(enabled: true, Array.Empty<string>(), monitored, separatelyMonitored, keptElsewhere: Array.Empty<string>()).Create;

            /* A test replaces the work in each database (LongQueryTraceDatabaseOverrideForTests), and the shared ensure
               still drives it: the same per-database isolation, log lines and all-refused throw as in production. */
            var createInDatabase = LongQueryTraceDatabaseOverrideForTests;
            await EnsureDatabaseScopedXeSessionsAsync(
                server, "long query completions", sessionName,
                (connection, token) => EnsureLongQueryCompletionsXeSessionAzureSqlDbAsync(connection, sessionName, token), create, cancellationToken,
                repeatsAtDebug: createRepeats,
                ensureInDatabaseOverrideForTests: createInDatabase is null ? null : (databaseName, token) => createInDatabase(server, databaseName, true, sessionName, token));

            return monitored;
        }

        /* A test replaces the server-scoped create, called with no database name. Null in production. */
        if (LongQueryTraceDatabaseOverrideForTests is { } createOnServer)
        {
            await createOnServer(server, string.Empty, true, sessionName, cancellationToken);
            return null;
        }

        using var connection = await CreateConnectionAsync(server, cancellationToken);
        await EnsureLongQueryCompletionsXeSessionOnPremAsync(connection, server, sessionName, cancellationToken);
        return null;
    }

    private async Task EnsureLongQueryCompletionsXeSessionOnPremAsync(SqlConnection connection, ServerConnection server, string sessionName, CancellationToken cancellationToken)
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
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
            {
                if (result is int isRunning && isRunning == 0)
                {
                    using var startCmd = new SqlCommand(LongQueryCompletionsCollector.BuildStartSessionSql(sessionName, databaseScoped: false), connection);
                    startCmd.CommandTimeout = CommandTimeoutSeconds;
                    await startCmd.ExecuteNonQueryAsync(cancellationToken);
                    AppLogger.Info("XeSession", $"[{server.DisplayName}] Started long-query completion XE session");
                }
                return;
            }
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(sessionName, databaseScoped: false, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(sessionName, databaseScoped: false), connection);
        createCmd.CommandTimeout = CommandTimeoutSeconds;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        AppLogger.Info("XeSession", $"[{server.DisplayName}] Created and started long-query completion XE session (duration >= {LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds} us)");
    }

    private async Task EnsureLongQueryCompletionsXeSessionAzureSqlDbAsync(SqlConnection connection, string sessionName, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;", connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);

            if (result != null)
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
                startCmd.CommandTimeout = CommandTimeoutSeconds;
                await startCmd.ExecuteNonQueryAsync(cancellationToken);
                AppLogger.Debug("XeSession", $"[Azure SQL DB:{connection.Database}] Long-query completion XE session verified (database-scoped)");
                return;
            }
        }

        using var createCmd = new SqlCommand(
            LongQueryCompletionsCollector.BuildCreateSessionSql(sessionName, databaseScoped: true, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds)
            + "\n\n" + LongQueryCompletionsCollector.BuildStartSessionSql(sessionName, databaseScoped: true), connection);
        createCmd.CommandTimeout = CommandTimeoutSeconds;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
        AppLogger.Info("XeSession", $"[Azure SQL DB:{connection.Database}] Created and started long-query completion XE session (database-scoped)");
    }

    /// <summary>
    /// Drops the long-query completion XE session (the opt-out path — disabling the collector removes
    /// the server-side session, the actual busy-server cost). Idempotent: the shared DROP DDL is
    /// guarded by an existence check, so a server that never had the session is a clean no-op. On Azure
    /// SQL DB the drop runs in every listed database, exclusions included, except master, a database monitored as
    /// its own server, and a database where another registration keeps the session
    /// (<see cref="LongQueryTraceDatabases.Plan"/>). There a failure throws <see cref="LongQueryTraceDropException"/>
    /// after every database was tried, so the reconcile retries it. On every other engine the drop of the server's session
    /// throws it too (<see cref="LongQueryTraceDropException.ForServer"/>), so the same cap applies.
    /// </summary>
    private async Task DropLongQueryCompletionsXeSessionAsync(
        ServerConnection server,
        string sessionName,
        bool isAzureSqlDatabase,
        IReadOnlyList<string> separatelyMonitored,
        Func<IEnumerable<string>, IReadOnlyList<string>> keptElsewhere,
        bool afterTheCap,
        CancellationToken cancellationToken)
    {
        if (isAzureSqlDatabase)
        {
            var listed = await ListEveryLongQueryTraceDatabaseAsync(server, cancellationToken);
            var plan = LongQueryTraceDatabases.Plan(enabled: false, listed, Array.Empty<string>(), separatelyMonitored, keptElsewhere(listed));
            await DropLongQueryTraceInEachAsync(server, sessionName, plan.Drop, afterTheCap, cancellationToken);
            return;
        }

        /* #4964: a failure here is a failed drop like the Azure arm's, so it reaches the reconcile's cap as one. Left as it
           was, it landed in the general catch, which warns on every cycle with no end while the trace is off. */
        try
        {
            /* A test replaces the server-scoped drop, called with no database name. Null in production. */
            if (LongQueryTraceDatabaseOverrideForTests is { } dropOnServer)
            {
                await dropOnServer(server, string.Empty, false, sessionName, cancellationToken);
            }
            else
            {
                using var conn = await CreateConnectionAsync(server, cancellationToken);
                using var cmd = new SqlCommand(LongQueryCompletionsCollector.BuildDropSessionSql(sessionName, databaseScoped: false), conn);
                cmd.CommandTimeout = CommandTimeoutSeconds;
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw LongQueryTraceDropException.ForServer(ex);
        }

        AppLogger.Info("XeSession", $"[{server.DisplayName}] Long-query completion XE session reconciled OFF (collector disabled)");
    }

    /// <summary>
    /// Replaces the database list the long-query trace reads on Azure SQL Database. Called with
    /// <c>allDatabases</c> true for every online database, false for the monitored ones. Null in production.
    /// </summary>
    internal Func<ServerConnection, bool, CancellationToken, Task<List<string>>>? LongQueryTraceListOverrideForTests { get; set; }

    /// <summary>
    /// Replaces the long-query trace's work in one Azure SQL Database database: called with <c>create</c> true to
    /// create the session there, false to drop it. On every other engine the session is the server's, and it is
    /// called with an empty database name. The fourth argument is the session name the work would have named (#4961).
    /// Null in production.
    /// </summary>
    internal Func<ServerConnection, string, bool, string, CancellationToken, Task>? LongQueryTraceDatabaseOverrideForTests { get; set; }

    /// <summary>
    /// What the last reconcile that finished applied for this server: true for on, false for off, null when no
    /// reconcile has finished since the app started.
    /// </summary>
    internal bool? LongQueryTraceAppliedState(string serverId) =>
        _longQueryTraceApplied.TryGetValue(serverId, out var applied) ? applied.Enabled : null;

    /// <summary>
    /// Replaces the clock the long-query trace's hourly attempts after the cap read (<see cref="LongQueryTraceDropRetry"/>).
    /// Null in production.
    /// </summary>
    internal Func<DateTime>? LongQueryTraceUtcNowForTests { get; set; }

    /// <summary>True when the last reconcile that finished applied this enabled state under this state key.</summary>
    private bool IsLongQueryTraceApplied(string serverId, bool enabled, string stateKey) =>
        _longQueryTraceApplied.TryGetValue(serverId, out var applied)
        && applied.Enabled == enabled
        && string.Equals(applied.StateKey, stateKey, StringComparison.Ordinal);

    /// <summary>
    /// Records a reconcile that finished. The failed-pass count starts again only where a cleanup pass ran and
    /// succeeded, so a cycle that skips the cleanup keeps the hourly attempt after the cap.
    /// </summary>
    private void MarkLongQueryTraceApplied(string serverId, bool enabled, string stateKey) =>
        _longQueryTraceApplied[serverId] = (enabled, stateKey);

    /// <summary>
    /// The databases monitored as their own servers, for this Azure SQL Database registration: the same list the
    /// blocking and deadlock alerts skip (<see cref="KnownEngineEditions.SeparatelyMonitoredDatabases(bool, ServerConnection, IEnumerable{ServerConnection})"/>).
    /// Empty unless the registration is of the logical server.
    /// </summary>
    private IReadOnlyList<string> SeparatelyMonitoredDatabasesFor(ServerConnection server) =>
        KnownEngineEditions.SeparatelyMonitoredDatabases(isAzureSqlDatabase: true, server, _serverManager.GetAllServers());

    /// <summary>
    /// The registrations of this server's logical server, for <see cref="LongQueryTraceDatabases.KeptElsewhere"/>: each
    /// configured server with the same server name, with whether its long-query trace is on and its exclusions. Lite
    /// has no database scope, so none is given.
    /// </summary>
    private List<LongQueryTraceRegistration> LongQueryTraceRegistrationsFor(ServerConnection server)
    {
        var hostKey = server.ServerName.Trim();
        return _serverManager.GetAllServers()
            .Where(other => string.Equals(other.ServerName.Trim(), hostKey, StringComparison.OrdinalIgnoreCase))
            .Select(other => new LongQueryTraceRegistration(
                other.Id,
                other.ServerName,
                other.DatabaseName,
                other.IsEnabled,
                TraceOn: _scheduleManager.GetScheduleForServer(other.Id, "long_query_completions")?.Enabled ?? false,
                other.ExcludedDatabases.ToList(),
                DatabaseScope: null))
            .ToList();
    }

    /// <summary>
    /// The per-database read's list without the databases monitored as their own servers, for a collector whose read
    /// skips them (<see cref="ICollectorDefinition{TRow}.SkipsSeparatelyMonitoredDatabases"/>). Called only on the
    /// Azure SQL Database per-database path.
    /// </summary>
    private List<string> WithoutSeparatelyMonitoredDatabases(ServerConnection server, List<string> databases) =>
        AzureSweepScope.WithoutSeparatelyMonitored(databases, SeparatelyMonitoredDatabasesFor(server));

    /// <summary>
    /// Every online database, with no exclusions, for a drop. A failure to list them throws
    /// <see cref="LongQueryTraceDropException"/> with no names, so the reconcile retries it like a failed drop.
    /// </summary>
    private async Task<List<string>> ListEveryLongQueryTraceDatabaseAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            return await ListLongQueryTraceDatabasesAsync(server, allDatabases: true, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new LongQueryTraceDropException(Array.Empty<string>(), ex);
        }
    }

    /// <summary>
    /// Trace ON on Azure SQL Database: drops the session from each listed database outside the monitored set,
    /// except a database monitored as its own server or one where another registration keeps the session.
    /// </summary>
    private async Task DropLongQueryTraceOutsideTheSetAsync(
        ServerConnection server,
        string sessionName,
        IReadOnlyList<string> monitored,
        IReadOnlyList<string> separatelyMonitored,
        Func<IEnumerable<string>, IReadOnlyList<string>> keptElsewhere,
        bool afterTheCap,
        CancellationToken cancellationToken)
    {
        var listed = await ListEveryLongQueryTraceDatabaseAsync(server, cancellationToken);
        var plan = LongQueryTraceDatabases.Plan(enabled: true, listed, monitored, separatelyMonitored, keptElsewhere(listed));
        await DropLongQueryTraceInEachAsync(server, sessionName, plan.Drop, afterTheCap, cancellationToken);
    }

    /// <summary>
    /// Drops the session in each database, tries every one, and logs a warning that names each database where the
    /// drop failed (<see cref="LongQueryTraceDatabases.DropEachAsync"/>). The hourly attempt after the cap logs it at
    /// Debug instead, so the cap's one warning is not repeated.
    /// </summary>
    private Task DropLongQueryTraceInEachAsync(ServerConnection server, string sessionName, IReadOnlyList<string> databases, bool afterTheCap, CancellationToken cancellationToken) =>
        LongQueryTraceDatabases.DropEachAsync(
            databases,
            (databaseName, token) => DropLongQueryTraceInDatabaseAsync(server, sessionName, databaseName, token),
            (databaseName, ex) =>
            {
                var line = $"[{server.DisplayName}] [{databaseName}] Could not drop the long-query completion XE session: {ex.Message}";
                if (afterTheCap)
                {
                    AppLogger.Debug("XeSession", line);
                }
                else
                {
                    AppLogger.Warn("XeSession", line);
                }
            },
            createNote: null,
            cancellationToken);

    /// <summary>
    /// The databases the long-query trace works in on Azure SQL Database. With <paramref name="allDatabases"/>, every
    /// online database with no exclusions; otherwise the monitored ones. A registration that names a database gets
    /// that database either way.
    /// </summary>
    private async Task<List<string>> ListLongQueryTraceDatabasesAsync(ServerConnection server, bool allDatabases, CancellationToken cancellationToken) =>
        LongQueryTraceListOverrideForTests is { } listOverride
            ? await listOverride(server, allDatabases, cancellationToken)
            : await GetAzureDatabaseListAsync(server, applyExclusions: !allDatabases, cancellationToken);

    /// <summary>Drops the long-query trace's database-scoped session in one Azure SQL Database database.</summary>
    private async Task DropLongQueryTraceInDatabaseAsync(ServerConnection server, string sessionName, string databaseName, CancellationToken cancellationToken)
    {
        if (LongQueryTraceDatabaseOverrideForTests is { } dropInDatabase)
        {
            await dropInDatabase(server, databaseName, false, sessionName, cancellationToken);
            return;
        }

        using var connection = await OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
        using var dropCmd = new SqlCommand(LongQueryCompletionsCollector.BuildDropSessionSql(sessionName, databaseScoped: true), connection);
        dropCmd.CommandTimeout = CommandTimeoutSeconds;
        await dropCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Collects long-query completions via the shared <see cref="LongQueryCompletionsCollector"/>
    /// definition. The session lifecycle stays in the reconcile above; a missing/inaccessible session
    /// is NOT tolerated as zero rows (#4731): the read raises <see cref="XeSessionEnsureException"/> like the
    /// ensure does (see <see cref="ReadXeSessionAsync"/>), the way the blocked-process and deadlock reads do, so
    /// the run records PERMISSIONS or ERROR with the XE session flagged unavailable, and never a SUCCESS over a
    /// source it could not read. Also (#3754) when the reconcile has already recorded that the session could not
    /// be created: then the read is not attempted and the reconcile's own exception is rethrown into
    /// <c>RunCollectorAsync</c>'s classification, the way the blocked-process and deadlock ensures throw into it
    /// from inside the run. Zero rows off a session that does not exist is not a collection; recording it as
    /// SUCCESS was the defect. The tray notice that names the failed captures (<c>MainWindow.NameXeCaptures</c>)
    /// calls this one long-query.
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

        return await ReadXeSessionAsync(
            "long query completions",
            () => RunCollectorDefinitionAsync(LongQueryCompletionsCollector.Instance, server, cancellationToken));
    }
}
