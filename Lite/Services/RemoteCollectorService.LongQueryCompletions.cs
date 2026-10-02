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

    /* #4964: the servers whose collector has already logged its own line for the kept failure above at that line's full
       level, kept until the create's own state above is cleared (a create that succeeds, or the trace turned off). The
       collector's run rethrows the kept failure on every cycle, so a create that cannot succeed made that line repeat at
       Warning or Error without end, beside create lines that log the repeats at Debug. The first run to rethrow it logs
       the line at its level; the runs after it log the same lines at Debug. Kept apart from the create's state because
       the create runs in the reconcile and the collector in a run: the run may come a cycle later, or not every cycle,
       and its line is logged once either way. The run row, its classification and the exception are the same on every
       run. In memory, so a restart logs the line again. */
    private readonly ConcurrentDictionary<string, bool> _longQueryTraceFaultLogged = new();

    /* #4964: the same rule for the always-on deadlock and blocked-process sessions' ensure, kept per server and per
       session (the key is the server id, then the session name): the ensures whose last cycle failed, until a cycle of that
       session on that server succeeds. Their ensure runs on every collector cycle: in every monitored database on Azure SQL
       Database, so a server that refuses the create refuses it on every cycle, and once on the server everywhere else. The
       first failing cycle logs its refusals at Warning and the all-refused line at Error (the server-scoped arm's one failure
       at Warning or Error); the cycles after it log the same lines at Debug, and so does the collector's own line for the
       failure. The retry on every cycle, and the exception each failing cycle throws for the collector to record, do not
       change. In memory, so a restart warns again. */
    private readonly ConcurrentDictionary<string, bool> _databaseScopedXeSessionEnsureWarned = new();

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
        /* #4961: a server whose removal has begun is not reconciled: its removal is dropping the session, and a create on
           this cycle would bring it back. */
        if (_longQueryTraceRemoved.ContainsKey(server.Id))
        {
            return;
        }

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
            _longQueryTraceFaultLogged.TryRemove(server.Id, out _);
        }

        if (!enabled)
        {
            _longQueryTraceReadOnlyRefused.TryRemove(server.Id, out _);
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

            /* #4961: the session that earlier versions shared between installs is dropped once per registration and database, and
               each cycle tries until the drop is on record. A pass runs it when it is pending and the retry has not given up, or
               the pass is due anyway (the hourly attempt, a start, a change to the settings). One listing of every database serves
               it and the drop of this install's own session. */
            var passDue = afterTheCap || !IsLongQueryTraceApplied(server.Id, enabled, stateKey);
            var pass = new LongQueryReconcilePass(
                LegacyLongQuerySessionDue(server, retry, passDue),
                () => ListEveryLongQueryTraceDatabaseAsync(server, cancellationToken));

            if (enabled)
            {
                /* #4961: a read-only database refused the last create, and stays read-only until the registration or the
                   database changes: not tried again for an hour. The kept fault stays, so the run still records it. */
                if (LongQueryTraceReadOnlyRefusalHolds(server.Id, stateKey, utcNow))
                {
                    return;
                }

                var monitored = await EnsureLongQueryCompletionsXeSessionAsync(server, sessionName, isAzureSqlDatabase, separatelyMonitored, createRepeats, pass, afterTheCap, cancellationToken);
                _longQueryTraceReadOnlyRefused.TryRemove(server.Id, out _);

                /* #3754: the session exists (everywhere it could) - a fault from an earlier cycle is over. */
                _longQueryTraceFault.TryRemove(server.Id, out _);
                _longQueryTraceCreateWarned.TryRemove(server.Id, out _);
                _longQueryTraceFaultLogged.TryRemove(server.Id, out _);

                /* The server's own session has no cleanup pass while the trace is on, so a create that succeeded is the end of
                   a run of failed drops: the next time it is turned off, the count starts again (#4964). */
                if (monitored is null && !LegacyLongQuerySessionPending(server))
                {
                    retry.Reset();
                }

                /* Azure SQL Database: drop the session from listed databases outside the monitored set, when
                   the plan's settings changed since the last pass that finished (and once after each start), or
                   when the hourly attempt after the cap is due. */
                if (monitored is not null && passDue)
                {
                    await DropLongQueryTraceOutsideTheSetAsync(server, sessionName, monitored, separatelyMonitored, KeptElsewhere, afterTheCap, pass, cancellationToken);
                    if (!LegacyLongQuerySessionPending(server))
                    {
                        retry.Reset();
                    }
                }

                /* #4961: a failed legacy drop counts as this pass's failure, after this install's own session was ensured. */
                pass.ThrowLegacyFailure();
                MarkLongQueryTraceApplied(server.Id, enabled: true, stateKey);
            }
            else if (passDue || pass.LegacyDue)
            {
                /* Disabled and either never reconciled, previously enabled, or reconciled under different
                   settings: drop, then remember it is gone so the next cycles skip the connection entirely. #4961: on the
                   server's own session, a drop would stop the trace another registration of this install keeps on the same
                   instance, so a positive match leaves it, and counts as done: that registration's own drop removes the
                   session when it turns its trace off. A name that is not known matches nothing, and drops as it always did. */
                if (!isAzureSqlDatabase && (await LongQueryTraceInstanceGuardFor(server, cancellationToken)).Kept)
                {
                    AppLogger.Info("XeSession", $"[{server.DisplayName}] Long-query completion XE session left in place: another registration of this install keeps it on the same instance");

                    /* The session earlier versions shared between installs is nobody's to keep: its drop still runs. */
                    await RunLegacyLongQuerySessionAsync(server, pass, isAzureSqlDatabase: false, afterTheCap, cancellationToken);
                    pass.ThrowLegacyFailure();
                }
                else
                {
                    await DropLongQueryCompletionsXeSessionAsync(server, sessionName, isAzureSqlDatabase, separatelyMonitored, KeptElsewhere, afterTheCap, pass, cancellationToken);
                }

                if (!LegacyLongQuerySessionPending(server))
                {
                    retry.Reset();
                }

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
            var readOnlyRefusal = enabled && IsReadOnlyDatabaseRefusal(ex);
            var reconcileFailure = $"[{server.DisplayName}] Failed to reconcile long-query completion XE session: {ex.Message}";
            if (readOnlyRefusal)
            {
                /* #4961: the database's own line already said why and what to change, at Warning. This one is for the
                   record, and the create is not tried again for an hour. */
                AppLogger.Debug("XeSession", $"{reconcileFailure} The next attempt is in an hour.");
                _longQueryTraceReadOnlyRefused[server.Id] = (stateKey, utcNow);
            }
            else if (createRepeats)
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
        ServerConnection server, string sessionName, bool isAzureSqlDatabase, IReadOnlyList<string> separatelyMonitored, bool createRepeats,
        LongQueryReconcilePass pass, bool afterTheCap, CancellationToken cancellationToken)
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

            /* #4961: in each database the legacy drop runs before this install's create, so it frees a slot there. A failure is
               kept, and the create still runs. */
            await RunLegacyLongQuerySessionAsync(server, pass, isAzureSqlDatabase: true, afterTheCap, cancellationToken);

            var create = LongQueryTraceDatabases.Plan(enabled: true, Array.Empty<string>(), monitored, separatelyMonitored, keptElsewhere: Array.Empty<string>()).Create;

            /* A test replaces the work in each database (LongQueryTraceDatabaseOverrideForTests), and the shared ensure
               still drives it: the same per-database isolation, log lines and all-refused throw as in production. Below
               it, a test replaces each step's open and work (LongQueryTraceStepOverrideForTests), and sees the connection
               string the step would have opened (#4961). */
            var createInDatabase = LongQueryTraceDatabaseOverrideForTests;
            var stepInDatabase = LongQueryTraceStepOverrideForTests;
            await EnsureDatabaseScopedXeSessionsAsync(
                server, "long query completions", sessionName,
                (connection, token) => new SqlConnectionStringBuilder(connection.ConnectionString).ApplicationIntent == ApplicationIntent.ReadOnly
                    ? EnsureLongQueryCompletionsXeSessionReadOnlyIntentAsync(connection, server, sessionName, token)
                    : EnsureLongQueryCompletionsXeSessionAzureSqlDbAsync(server, connection, sessionName, token),
                create, cancellationToken,
                repeatsAtDebug: createRepeats,
                explainRefusal: ex => IsReadOnlyDatabaseRefusal(ex) ? LongQueryTraceDatabases.ReadOnlyDatabaseMessage() : null,
                ensureInDatabaseOverrideForTests: createInDatabase is not null
                    ? async (databaseName, token) =>
                    {
                        await createInDatabase(server, databaseName, true, sessionName, token);
                        LookForLegacyLongQuerySessionForTests(server, databaseName);
                    }
                    : stepInDatabase is not null
                        ? (databaseName, token) => RunLongQueryTraceStepsAsync(server, databaseName, sessionName, stepInDatabase, token)
                        : null);

            return monitored;
        }

        /* #4961: the legacy drop first, with its failure kept, so this install's own session is still ensured. */
        await RunLegacyLongQuerySessionAsync(server, pass, isAzureSqlDatabase: false, afterTheCap, cancellationToken);

        /* A test replaces the server-scoped create, called with no database name. Null in production. */
        if (LongQueryTraceDatabaseOverrideForTests is { } createOnServer)
        {
            await createOnServer(server, string.Empty, true, sessionName, cancellationToken);
            LookForLegacyLongQuerySessionForTests(server, string.Empty);
            return null;
        }

        /* Below it, a test replaces the open and the work, and sees the registration's own connection string (#4961). */
        if (LongQueryTraceStepOverrideForTests is { } stepOnServer)
        {
            await stepOnServer(server, string.Empty, _serverManager.CredentialResolver.GetConnectionString(server), LongQueryTraceStep.CreateAndStart, sessionName, cancellationToken);
            return null;
        }

        using var connection = await CreateConnectionAsync(server, cancellationToken);
        await EnsureLongQueryCompletionsXeSessionOnPremAsync(connection, server, sessionName, cancellationToken);
        return null;
    }

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

    /* #4961: the servers whose create a read-only database refused (error 3906), with the state key the refusal was made
       under and when. A read-only database stays read-only until the registration or the database changes, so the create
       is not tried again for an hour, or sooner when the state key changes. The kept fault stays, so every run still
       records it. In memory, so a restart tries again. */
    private readonly ConcurrentDictionary<string, (string StateKey, DateTime AtUtc)> _longQueryTraceReadOnlyRefused = new();

    private bool LongQueryTraceReadOnlyRefusalHolds(string serverId, string stateKey, DateTime utcNow) =>
        _longQueryTraceReadOnlyRefused.TryGetValue(serverId, out var refused)
        && string.Equals(refused.StateKey, stateKey, StringComparison.Ordinal)
        && utcNow - refused.AtUtc < LongQueryTraceDatabases.RetryInterval;

    /// <summary>
    /// Whether a failed create is a read-only database's refusal (error 3906): the error itself, or the one an ensure
    /// exception wraps (#4961).
    /// </summary>
    internal static bool IsReadOnlyDatabaseRefusal(Exception? ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is SqlException sql && LongQueryTraceDatabases.IsReadOnlyDatabaseRefusal(sql.Errors.Cast<SqlError>().Select(e => e.Number)))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The Azure SQL Database create for a registration with read-only intent (#4961): the definition over a connection
    /// without the intent, which does not start it, then the start over the registration's own read-only connection.
    /// </summary>
    private async Task EnsureLongQueryCompletionsXeSessionReadOnlyIntentAsync(SqlConnection connection, ServerConnection server, string sessionName, CancellationToken cancellationToken)
    {
        using (var definition = await OpenAzureDatabaseConnectionAsync(server, connection.Database, cancellationToken, withoutReadOnlyIntent: true))
        {
            await CreateLongQueryCompletionsDefinitionAzureSqlDbAsync(definition, sessionName, cancellationToken);
        }

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
        startCmd.CommandTimeout = CommandTimeoutSeconds;
        await startCmd.ExecuteNonQueryAsync(cancellationToken);
        AppLogger.Debug("XeSession", $"[Azure SQL DB:{connection.Database}] Long-query completion XE session verified over the read-only connection (database-scoped)");
    }

    private async Task CreateLongQueryCompletionsDefinitionAzureSqlDbAsync(SqlConnection connection, string sessionName, CancellationToken cancellationToken)
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
            if (await cmd.ExecuteScalarAsync(cancellationToken) != null)
            {
                return;
            }
        }

        try
        {
            using var createCmd = new SqlCommand(
                LongQueryCompletionsCollector.BuildCreateSessionSql(sessionName, databaseScoped: true, LongQueryCompletionsCollector.DefaultDurationThresholdMicroseconds), connection);
            createCmd.CommandTimeout = CommandTimeoutSeconds;
            await createCmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqlException ex) when (IsBenignXeSessionAlreadyPresent(ex))
        {
            return;
        }

        AppLogger.Info("XeSession", $"[Azure SQL DB:{connection.Database}] Created the long-query completion XE session's definition over a connection without read-only intent (database-scoped)");
    }

    /// <summary>
    /// A test's stand-in for the Azure per-database ensure: each step the registration takes in the database, handed to
    /// <see cref="LongQueryTraceStepOverrideForTests"/> with the connection string it would open.
    /// </summary>
    private async Task RunLongQueryTraceStepsAsync(
        ServerConnection server,
        string databaseName,
        string sessionName,
        Func<ServerConnection, string, string, LongQueryTraceStep, string, CancellationToken, Task> step,
        CancellationToken cancellationToken)
    {
        foreach (var (kind, connectionString) in LongQueryTraceStepsFor(AzureDatabaseConnectionString(server, databaseName)))
        {
            await step(server, databaseName, connectionString, kind, sessionName, cancellationToken);
        }
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
WHERE ses.name = @session_name;" + LegacyLongQuerySessionServerProbeSql, connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            cmd.Parameters.Add(LegacyLongQuerySessionProbeParameter(server, string.Empty));
            object? result;
            using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                result = await reader.ReadAsync(cancellationToken) ? reader.GetValue(0) : null;
                if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
                {
                    ReportLegacyLongQuerySessionFound(server, string.Empty);
                }
            }

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

    private async Task EnsureLongQueryCompletionsXeSessionAzureSqlDbAsync(ServerConnection server, SqlConnection connection, string sessionName, CancellationToken cancellationToken)
    {
        using (var cmd = new SqlCommand(@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    session_state = des.name
FROM sys.database_event_sessions AS des
WHERE des.name = @session_name;" + LegacyLongQuerySessionDatabaseProbeSql, connection))
        {
            cmd.CommandTimeout = CommandTimeoutSeconds;
            cmd.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            cmd.Parameters.Add(LegacyLongQuerySessionProbeParameter(server, connection.Database));
            object? result;
            using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                result = await reader.ReadAsync(cancellationToken) ? reader.GetValue(0) : null;
                if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
                {
                    ReportLegacyLongQuerySessionFound(server, connection.Database);
                }
            }

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
        LongQueryReconcilePass pass,
        CancellationToken cancellationToken,
        bool ofARemovedServer = false)
    {
        if (isAzureSqlDatabase)
        {
            var listed = await pass.ListedAsync();
            await RunLegacyLongQuerySessionAsync(server, pass, isAzureSqlDatabase: true, afterTheCap, cancellationToken);
            var plan = LongQueryTraceDatabases.Plan(enabled: false, listed, Array.Empty<string>(), separatelyMonitored, keptElsewhere(listed));
            try
            {
                await DropLongQueryTraceInEachAsync(server, sessionName, plan.Drop, afterTheCap, cancellationToken);
            }
            catch (LongQueryTraceDropException own) when (pass.LegacyFailure is not null)
            {
                throw MergeLongQueryTraceDropFailures(pass.LegacyFailure, own);
            }

            pass.ThrowLegacyFailure();
            return;
        }

        await RunLegacyLongQuerySessionAsync(server, pass, isAzureSqlDatabase: false, afterTheCap, cancellationToken);

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

        pass.ThrowLegacyFailure();
        if (!ofARemovedServer)
        {
            AppLogger.Info("XeSession", $"[{server.DisplayName}] Long-query completion XE session reconciled OFF (collector disabled)");
        }
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
    /// Replaces one open-and-act step of the long-query trace's create, below
    /// <see cref="LongQueryTraceDatabaseOverrideForTests"/>, which wins when both are set (#4961). Called with the server,
    /// the database (empty for the server's own session), the connection string the step would open, the step, and the
    /// session name. A test sees which connection each step uses, with or without read-only intent. The step's work is
    /// not done. Null in production.
    /// </summary>
    internal Func<ServerConnection, string, string, LongQueryTraceStep, string, CancellationToken, Task>? LongQueryTraceStepOverrideForTests { get; set; }

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
        LongQueryReconcilePass pass,
        CancellationToken cancellationToken)
    {
        var listed = await pass.ListedAsync();
        var plan = LongQueryTraceDatabases.Plan(enabled: true, listed, monitored, separatelyMonitored, keptElsewhere(listed));
        try
        {
            await DropLongQueryTraceInEachAsync(server, sessionName, plan.Drop, afterTheCap, cancellationToken);
        }
        catch (LongQueryTraceDropException own) when (pass.LegacyFailure is not null)
        {
            throw MergeLongQueryTraceDropFailures(pass.LegacyFailure, own);
        }
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

    /// <summary>How long a server's removal waits for the drop of its long-query session, for the whole step (#4961).</summary>
    internal static readonly TimeSpan LongQueryTraceRemovalTimeout = TimeSpan.FromSeconds(15);

    /* #4961: the servers whose removal has begun, by connection id. The reconcile runs on the collection loop's own timer
       while the removal waits for its drop, and with the trace on it creates the session again on every cycle, so a
       server in here is left alone: a session created after the drop would outlive the server. A re-added server has a new
       connection id, so it is not held back. */
    private readonly ConcurrentDictionary<string, bool> _longQueryTraceRemoved = new();

    /// <summary>
    /// A server's removal drops this install's long-query session (#4961), so a server that is no longer monitored does not
    /// keep tracing: the trace-off drop, once, for the server's own session. On Azure SQL Database that is each listed
    /// database but master, and one that another registration keeps; elsewhere the server's own session, unless another
    /// registration of this install keeps it on the same instance. A removed server cannot be checked later, so a name that
    /// is not known leaves the session in place when another registration could keep it, and says why. Nothing is opened
    /// for a server whose last finished reconcile dropped the session. One attempt, and every failure, timeout included,
    /// is logged and returned: it never stops the removal.
    /// </summary>
    public async Task DropLongQueryTraceOfRemovedServerAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        _longQueryTraceRemoved[server.Id] = true;

        await DropLongQuerySessionOfRemovedServerAsync(server, cancellationToken);
        await DropAlwaysOnOwnSessionsOfRemovedServerAsync(server, cancellationToken);
    }

    /// <summary>
    /// The deadlock and blocked-process sessions this install chose for itself in the removed server's Azure SQL Database
    /// databases (<see cref="AlwaysOnXeChoices.OwnDatabases"/>, #4961): each is dropped, by this install's own name, unless
    /// another registration of this install reads its own session in that database. The shared session is never dropped. One
    /// attempt, every failure logged and returned. The server's choices are forgotten either way.
    /// </summary>
    private async Task DropAlwaysOnOwnSessionsOfRemovedServerAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            var others = _serverManager.GetAllServers().Where(other => other.Id != server.Id).Select(other => other.Id).ToList();
            foreach (var kind in new[] { AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeSessionKind.BlockedProcess })
            {
                var ownName = AlwaysOnOwnSessionName(kind);
                if (ownName is null)
                {
                    continue;
                }

                var keptByOthers = new HashSet<string>(others.SelectMany(id => _alwaysOnChoices.OwnDatabases(id, kind)), StringComparer.OrdinalIgnoreCase);
                foreach (var databaseName in _alwaysOnChoices.OwnDatabases(server.Id, kind).Where(database => !keptByOthers.Contains(database)))
                {
                    try
                    {
                        SqlConnection? connection = null;
                        try
                        {
                            IAlwaysOnXeDatabase database;
                            if (AlwaysOnXeDatabaseForTests is { } open)
                            {
                                database = await open(server, databaseName, cancellationToken);
                            }
                            else
                            {
                                connection = await OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
                                database = new LiteAlwaysOnXeDatabase(connection);
                            }

                            await database.ExecuteAsync(AlwaysOnXeSessions.BuildAzureDropSql(kind, ownName), cancellationToken);
                        }
                        finally
                        {
                            connection?.Dispose();
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        AppLogger.Warn("XeSession", $"[{server.DisplayName}] [{databaseName}] Could not drop this install's {kind} XE session of the removed server; it may remain in the database: {ex.Message}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            AppLogger.Warn("XeSession", $"[{server.DisplayName}] The deadlock and blocked-process XE sessions of the removed server were not all dropped within {LongQueryTraceRemovalTimeout.TotalSeconds:0} seconds; some may remain on the server");
        }
        finally
        {
            _alwaysOnChoices.Forget(server.Id);
        }
    }

    private async Task DropLongQuerySessionOfRemovedServerAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        try
        {
            /* No install id, no session of this install's to drop. */
            var sessionName = LongQuerySessionName();
            if (sessionName is null || LongQueryTraceAppliedState(server.Id) == false)
            {
                return;
            }

            var isAzureSqlDatabase = _engineEditions.IsAzureSqlDatabase(server, _serverManager.GetConnectionStatus(server.Id).SqlEngineEdition);
            if (!isAzureSqlDatabase)
            {
                var skipReason = (await LongQueryTraceInstanceGuardFor(server, cancellationToken)).RemovalSkipReason();
                if (skipReason is not null)
                {
                    AppLogger.Info("XeSession", $"[{server.DisplayName}] The long-query trace session was left in place on removal: {skipReason}.");
                    return;
                }
            }

            /* The registrations that remain: this one neither keeps a session nor leaves a database to its own registration
               once it is gone. Its own id is never another registration's. */
            var registrations = isAzureSqlDatabase ? LongQueryTraceRegistrationsFor(server) : new List<LongQueryTraceRegistration>();
            var remaining = _serverManager.GetAllServers().Where(other => other.Id != server.Id);
            var serverSeparatelyMonitored = isAzureSqlDatabase
                ? KnownEngineEditions.SeparatelyMonitoredDatabasesOnServer(server.ServerName, remaining)
                : Array.Empty<string>();
            var separatelyMonitored = isAzureSqlDatabase ? SeparatelyMonitoredDatabasesFor(server) : Array.Empty<string>();

            await DropLongQueryCompletionsXeSessionAsync(
                server,
                sessionName,
                isAzureSqlDatabase,
                separatelyMonitored,
                candidates => LongQueryTraceDatabases.KeptElsewhere(server.Id, server.ServerName, candidates, registrations, serverSeparatelyMonitored),
                afterTheCap: false,
                /* The session earlier versions shared between installs is not the removal's: its own step stays off. */
                new LongQueryReconcilePass(legacyDue: false, () => ListEveryLongQueryTraceDatabaseAsync(server, cancellationToken)),
                cancellationToken,
                ofARemovedServer: true);

            AppLogger.Info("XeSession", $"[{server.DisplayName}] Dropped the long-query trace session of the removed server");
        }
        catch (OperationCanceledException)
        {
            AppLogger.Warn("XeSession", $"[{server.DisplayName}] The long-query trace session of the removed server was not dropped within {LongQueryTraceRemovalTimeout.TotalSeconds:0} seconds; it may remain on the server");
        }
        catch (Exception ex)
        {
            AppLogger.Warn("XeSession", $"[{server.DisplayName}] Could not drop the long-query trace session of the removed server; it may remain on the server: {ex.Message}");
        }
    }

    /// <summary>
    /// #4961: where the session is the server's own (every engine but Azure SQL Database), this registration's last-known
    /// <c>@@SERVERNAME</c> beside the other registrations of this install that are monitored with their trace on
    /// (<see cref="LongQueryTraceInstanceGuard"/>). Their trace setting is the effective one, their own override or else the
    /// install's default. The names come from the identity row a wait_stats or cpu_utilization run persisted, and are read
    /// only when another registration could keep the session, so an install with no other trace on reads none.
    /// </summary>
    private async Task<LongQueryTraceInstanceGuard> LongQueryTraceInstanceGuardFor(ServerConnection server, CancellationToken cancellationToken)
    {
        var candidates = _serverManager.GetAllServers()
            .Where(other => other.Id != server.Id
                && other.IsEnabled
                && (_scheduleManager.GetScheduleForServer(other.Id, "long_query_completions")?.Enabled ?? false))
            .ToList();
        if (candidates.Count == 0)
        {
            return LongQueryTraceInstanceGuard.NoKeepers;
        }

        var ownName = await LastKnownInstanceNameAsync(server, cancellationToken);
        var keepers = new List<LongQueryTraceInstance>(candidates.Count);
        foreach (var other in candidates)
        {
            /* With no name of its own, this registration matches nothing, so the others' names are not read. */
            var name = ownName is null ? null : await LastKnownInstanceNameAsync(other, cancellationToken);
            keepers.Add(new LongQueryTraceInstance(Enabled: true, TraceOn: true, name));
        }

        return new LongQueryTraceInstanceGuard(ownName, keepers);
    }

    private Task<string?> LastKnownInstanceNameAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var serverId = GetServerId(server);
        return ServerEpoch.LastKnownNameAsync(carrier => GetCollectorStateAsync(serverId, carrier, cancellationToken));
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
            /* #4964: this run's lines for the failure follow the create's: the first run to rethrow it logs them at their
               levels, and the runs after it, until the create succeeds or the trace is turned off, log them at Debug. The
               decision is made here, once per run, by the add that only the first run wins, and goes to RunCollectorAsync
               on the run's own telemetry. The stored exception is not touched: it is the one the reconcile threw, shared
               by every reader, and the run still rethrows it as it is, so the type, the message and the classification
               are the same on every run. */
            TelemetryFor(GetServerId(server)).TraceFaultRepeatsAtDebug = !_longQueryTraceFaultLogged.TryAdd(server.Id, true);

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
