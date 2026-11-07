/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// A removed server, as it stood before the registry forgot it (#4961): the definition the loop held, the runtime that
/// connects to it, and what its last long-query reconcile left (<see cref="DarlingWorker.ServerLoopState.LongQueryTraceApplied"/>:
/// null is not yet reconciled, true is on, false is confirmed dropped).
/// </summary>
internal sealed record RemovedLongQueryServer(MonitoredServer Config, ServerRuntime Runtime, bool? LongQueryTraceApplied);

/// <summary>
/// Removing a server drops this install's Extended Events sessions on it, and nothing else (#4961): the long-query trace's
/// session, then the deadlock and blocked-process sessions this install chose for itself in the server's databases. The same
/// rule as Lite's removal. Every failure, the timeout included, is logged and returned: a session left behind never stops
/// the removal.
/// </summary>
internal static class DarlingRemovedServerSessions
{
    /// <summary>How long a server's removal waits for its sessions to drop, for the whole step.</summary>
    internal static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    private static readonly AlwaysOnXeSessionKind[] FallbackKinds = { AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeSessionKind.BlockedProcess, AlwaysOnXeSessionKind.Attention };

    /// <summary>
    /// What the removal needs from each server the reconcile is about to forget: the definition, the runtime and the
    /// long-query latch. The reconcile is one synchronous step under the servers' lock that clears the runtime and drops
    /// the state, so this is taken first, inside that lock. A server that stays is not taken, nor is one with no runtime
    /// (nothing to connect with) or one that is not SQL Server. A server whose trace was confirmed dropped has no
    /// long-query session to remove, so it is taken only when a database of it reads this install's own deadlock or
    /// blocked-process session (the fallbacks do not depend on the trace) or when its <c>schema_objects</c> collector is on
    /// (<paramref name="schemaOn"/>, by store id; null reads as off), because that session does not depend on it either (#7643).
    /// </summary>
    internal static List<RemovedLongQueryServer> Capture(
        IReadOnlyList<DarlingWorker.ServerLoopState> servers,
        IReadOnlyList<MonitoredServer> desired,
        DarlingCollectorRunner runner,
        Func<int, bool>? schemaOn = null)
    {
        var wanted = new HashSet<int>(desired.Select(server => server.ServerId));
        var removed = new List<RemovedLongQueryServer>();
        foreach (var state in servers)
        {
            if (wanted.Contains(state.Config.ServerId))
            {
                continue;
            }

            var runtime = state.Runtime;
            if (runtime is null || runtime.Target.Engine != CollectorTargetEngine.SqlServer)
            {
                continue;
            }

            var applied = state.LongQueryTraceApplied;
            if (applied == false && !HoldsOwnFallback(runner, runtime) && schemaOn?.Invoke(runtime.ServerId) != true)
            {
                continue;
            }

            removed.Add(new RemovedLongQueryServer(state.Config, runtime, applied));
        }

        return removed;
    }

    private static bool HoldsOwnFallback(DarlingCollectorRunner runner, ServerRuntime server)
    {
        var key = DarlingAlwaysOnXeSessions.ServerKey(server);
        return FallbackKinds.Any(kind => runner.AlwaysOnChoices.OwnDatabases(key, kind).Count > 0);
    }

    /// <summary>
    /// Drops this install's sessions on one removed server: the long-query session, the fallbacks, then the schema-changes session (#7643). <paramref name="remaining"/>
    /// is the registry as the reload left it, so the removed server is not among them and holds nothing back; its own id
    /// is never another registration's. <paramref name="traceOn"/> says whether another registration's long-query trace is
    /// on, and <paramref name="instanceGuard"/> is asked for an on-premises server only. One attempt for the whole step: the
    /// caller's token is the timeout. <paramref name="schemaOn"/> and <paramref name="schemaInstanceGuard"/> are the same two for the
    /// <c>schema_objects</c> collector (#7643); unset, the schema-changes session is not touched.
    /// </summary>
    internal static async Task DropAsync(
        RemovedLongQueryServer removed,
        DarlingCollectorRunner runner,
        IReadOnlyList<MonitoredServer>? remaining,
        Func<int, bool> traceOn,
        Func<CancellationToken, Task<LongQueryTraceInstanceGuard>> instanceGuard,
        ILogger logger,
        CancellationToken cancellationToken,
        Func<int, bool>? schemaOn = null,
        Func<CancellationToken, Task<LongQueryTraceInstanceGuard>>? schemaInstanceGuard = null)
    {
        var others = (remaining ?? Array.Empty<MonitoredServer>()).Where(other => other.ServerId != removed.Runtime.ServerId).ToList();
        await DropLongQuerySessionAsync(removed, runner, others, traceOn, instanceGuard, logger, cancellationToken);
        await DropOwnFallbacksAsync(removed, runner, others, logger, cancellationToken);
        await DropSchemaSessionAsync(
            removed, runner, others, schemaOn ?? (_ => false), schemaInstanceGuard ?? (_ => Task.FromResult(LongQueryTraceInstanceGuard.NoKeepers)), logger, cancellationToken);
    }

    /// <summary>
    /// The long-query trace's session: on Azure SQL Database in each listed database but master, and not in one another
    /// registration of the logical server keeps; elsewhere the server's own session, unless the instance guard says another
    /// registration of this install may keep it. A removed server cannot be checked later, so the guard leaves the session
    /// whenever another registration could keep it and says why. Nothing is opened for a server whose last finished reconcile
    /// dropped the session, or for an install with no id. The one-time legacy drop is not the removal's.
    /// </summary>
    private static async Task DropLongQuerySessionAsync(
        RemovedLongQueryServer removed,
        DarlingCollectorRunner runner,
        IReadOnlyList<MonitoredServer> others,
        Func<int, bool> traceOn,
        Func<CancellationToken, Task<LongQueryTraceInstanceGuard>> instanceGuard,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var server = removed.Runtime;
        var name = server.Config.DisplayName;
        try
        {
            /* A token that has run out ends the step before it opens anything. */
            cancellationToken.ThrowIfCancellationRequested();

            if (runner.LongQuerySessionName() is null || removed.LongQueryTraceApplied == false)
            {
                return;
            }

            var azureSqlDatabase = server.Target.IsAzureSqlDb;
            if (!azureSqlDatabase)
            {
                var skipReason = (await instanceGuard(cancellationToken)).RemovalSkipReason();
                if (skipReason is not null)
                {
                    logger.LogInformation("[{Server}] The long-query trace session was left in place on removal: {Reason}.", name, skipReason);
                    return;
                }
            }

            var registrations = azureSqlDatabase
                ? DarlingWorker.LongQueryTraceRegistrations(
                    server.Config.Host,
                    others,
                    traceOn,
                    otherId => runner.DatabaseScopeFor(LongQueryCompletionsCollector.Instance.Name, otherId))
                : Array.Empty<LongQueryTraceRegistration>();
            var separatelyMonitored = azureSqlDatabase
                ? DarlingWorker.LongQueryTraceServerSeparatelyMonitored(server.Config.Host, others)
                : Array.Empty<string>();

            await DarlingXeSessions.ReconcileLongQueryCompletionsAsync(
                server, runner, enabled: false, LongQueryTracePass.Removal, registrations, separatelyMonitored, createFailureWarned: false, logger, cancellationToken);

            logger.LogInformation("[{Server}] Dropped the long-query trace session of the removed server", name);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "[{Server}] The long-query trace session of the removed server was not dropped within {Seconds} seconds; it may remain on the server",
                name, Timeout.TotalSeconds.ToString("0", CultureInfo.InvariantCulture));
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                "[{Server}] Could not drop the long-query trace session of the removed server; it may remain on the server: {Message}", name, ex.Message);
        }
    }

    /// <summary>
    /// This install's schema-changes session (#7643), dropped with the server when its <c>schema_objects</c> collector is on for it
    /// (<paramref name="schemaOn"/>): the pass that makes the session runs only then, and a collector turned off earlier is
    /// dropped by the disable path. The session is per install, not per registration, so it stays where another registration of
    /// this install keeps it with that collector on, by the long-query drop's rules: on premises when the instance guard
    /// (<paramref name="instanceGuard"/>, built on <paramref name="schemaOn"/>) says one may; on Azure SQL Database in the databases
    /// that registration keeps. Elsewhere it is dropped from the server, or from each listed database but master. Nothing is opened
    /// for an install with no id. It never throws: the failure is logged, because a session left behind never stops the removal.
    /// </summary>
    private static async Task DropSchemaSessionAsync(
        RemovedLongQueryServer removed,
        DarlingCollectorRunner runner,
        IReadOnlyList<MonitoredServer> others,
        Func<int, bool> schemaOn,
        Func<CancellationToken, Task<LongQueryTraceInstanceGuard>> instanceGuard,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var server = removed.Runtime;
        var name = server.Config.DisplayName;
        var sessionName = SchemaChangesXeSession.TryNameFor(runner.InstallId);
        if (sessionName is null || !schemaOn(server.ServerId))
        {
            return;
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!server.Target.IsAzureSqlDb)
            {
                var skipReason = (await instanceGuard(cancellationToken)).RemovalSkipReason();
                if (skipReason is not null)
                {
                    logger.LogInformation("[{Server}] The schema-changes XE session was left in place on removal: {Reason}.", name, skipReason);
                    return;
                }

                await DropSchemaSessionInAsync(server, runner, string.Empty, sessionName, cancellationToken);
                logger.LogInformation("[{Server}] Dropped the schema-changes XE session of the removed server", name);
                return;
            }

            var registrations = DarlingWorker.LongQueryTraceRegistrations(
                server.Config.Host, others, schemaOn, otherId => runner.DatabaseScopeFor(SchemaObjectsCollector.Instance.Name, otherId));
            var separatelyMonitored = DarlingWorker.LongQueryTraceServerSeparatelyMonitored(server.Config.Host, others);
            var listed = await runner.ListLongQueryTraceDatabasesAsync(server, allDatabases: true, databaseScope: null, cancellationToken);
            var keptElsewhere = LongQueryTraceDatabases.KeptElsewhere(
                server.ServerId.ToString(CultureInfo.InvariantCulture), server.Config.Host, listed, registrations, separatelyMonitored);
            var drop = LongQueryTraceDatabases.Plan(enabled: false, listed, Array.Empty<string>(), separatelyMonitored, keptElsewhere).Drop;
            foreach (var database in drop)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await DropSchemaSessionInAsync(server, runner, database, sessionName, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(
                        "[{Server}] [{Database}] Could not drop the schema-changes XE session of the removed server; it may remain in the database: {Message}",
                        name, database, ex.Message);
                }
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "[{Server}] The schema-changes XE session of the removed server was not dropped within {Seconds} seconds; it may remain on the server",
                name, Timeout.TotalSeconds.ToString("0", CultureInfo.InvariantCulture));
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                "[{Server}] Could not drop the schema-changes XE session of the removed server; it may remain on the server: {Message}", name, ex.Message);
        }
    }

    /// <summary>One drop: in <paramref name="database"/> on Azure SQL Database, or the server's own scope for the empty name.</summary>
    private static async Task DropSchemaSessionInAsync(
        ServerRuntime server, DarlingCollectorRunner runner, string database, string sessionName, CancellationToken cancellationToken)
    {
        if (runner.SchemaSessionDropForTests is { } drop)
        {
            await drop(server, database, sessionName, cancellationToken);
            return;
        }

        if (database.Length == 0)
        {
            using var connection = new SqlConnection(DarlingXeSessions.WithoutReadOnlyIntent(server.ConnectionString));
            await connection.OpenAsync(cancellationToken);
            await DarlingSchemaXeSession.DropAsync(connection, sessionName, databaseScoped: false, cancellationToken);
            return;
        }

        using var azure = await runner.OpenAzureDatabaseConnectionAsync(server, database, cancellationToken);
        await DarlingSchemaXeSession.DropAsync(azure, sessionName, databaseScoped: true, cancellationToken);
    }

    /// The deadlock and blocked-process sessions this install chose for itself in the removed server's Azure SQL Database
    /// databases (<see cref="AlwaysOnXeChoices.OwnDatabases"/>): each is dropped by this install's own name, unless another
    /// registration of the same logical server reads its own session in that database. A database of the same name on
    /// another logical server is another database, so the registrations compare by host, ignoring case and padding. The
    /// shared session is never dropped. The server's choices are forgotten either way.
    /// </summary>
    private static async Task DropOwnFallbacksAsync(
        RemovedLongQueryServer removed,
        DarlingCollectorRunner runner,
        IReadOnlyList<MonitoredServer> others,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var server = removed.Runtime;
        var name = server.Config.DisplayName;
        var ownKey = DarlingAlwaysOnXeSessions.ServerKey(server);
        try
        {
            var hostKey = server.Config.Host.Trim();
            var sameServerKeys = others
                .Where(other => string.Equals(other.Host.Trim(), hostKey, StringComparison.OrdinalIgnoreCase))
                .Select(other => other.ServerId.ToString(CultureInfo.InvariantCulture))
                .ToList();

            foreach (var kind in FallbackKinds)
            {
                var ownName = runner.AlwaysOnOwnSessionName(kind);
                if (ownName is null)
                {
                    continue;
                }

                var keptByOthers = new HashSet<string>(
                    sameServerKeys.SelectMany(key => runner.AlwaysOnChoices.OwnDatabases(key, kind)), StringComparer.OrdinalIgnoreCase);
                foreach (var databaseName in runner.AlwaysOnChoices.OwnDatabases(ownKey, kind).Where(database => !keptByOthers.Contains(database)))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        SqlConnection? connection = null;
                        IAlwaysOnXeDatabase? database = null;
                        try
                        {
                            if (runner.AlwaysOnXeDatabaseForTests is { } open)
                            {
                                database = await open(server, databaseName, cancellationToken);
                            }
                            else if (runner.AlwaysOnXeConnectionForTests is { } openConnection)
                            {
                                database = DarlingAlwaysOnXeSessions.WithReadOnlyIntent(
                                    runner, server, databaseName,
                                    await openConnection(server, databaseName, DarlingXeSessions.LongQueryTraceConnectionString(server, databaseName), cancellationToken));
                            }
                            else
                            {
                                connection = await runner.OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
                                database = DarlingAlwaysOnXeSessions.WithReadOnlyIntent(
                                    runner, server, databaseName, new DarlingAlwaysOnXeSessions.Database(connection));
                            }

                            /* #4961: for a registration with read-only intent, stopped over its own connection when it runs
                               there, then dropped over a connection without the intent. */
                            await AlwaysOnXeAzureEnsure.DropOwnSessionAsync(database, kind, ownName, cancellationToken);
                        }
                        finally
                        {
                            (database as IDisposable)?.Dispose();
                            connection?.Dispose();
                        }

                        logger.LogInformation("[{Server}] [{Database}] Dropped this install's {Kind} XE session of the removed server", name, databaseName, kind);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        logger.LogWarning(
                            "[{Server}] [{Database}] Could not drop this install's {Kind} XE session of the removed server; it may remain in the database: {Message}",
                            name, databaseName, kind, ex.Message);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning(
                "[{Server}] The deadlock and blocked-process XE sessions of the removed server were not all dropped within {Seconds} seconds; some may remain on the server",
                name, Timeout.TotalSeconds.ToString("0", CultureInfo.InvariantCulture));
        }
        finally
        {
            runner.AlwaysOnChoices.Forget(ownKey);
        }
    }
}
