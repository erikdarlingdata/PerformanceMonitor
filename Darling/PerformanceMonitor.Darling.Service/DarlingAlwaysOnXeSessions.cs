/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One database's pass of the Azure ensure for the deadlock or blocked-process session (#4961): the shared session when it is
/// usable, this install's own when it is not, and back to the shared one once it is usable again. The decisions are
/// <see cref="AlwaysOnXeAzureEnsure"/>'s, the same for both products; this keeps the choice on the runner and logs what the
/// pass did.
/// </summary>
internal static class DarlingAlwaysOnXeSessions
{
    /// <summary>The key of one registration in the runner's choice map.</summary>
    internal static string ServerKey(ServerRuntime server) => server.ServerId.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Runs the ensure for one capture in one database, and sets the choice. A failure is logged as a warning, with the caps
    /// sentence when a create or start failed, and leaves the choice as it was: one broken database or session never blocks
    /// the rest.
    /// </summary>
    internal static async Task EnsureAsync(
        IAlwaysOnXeDatabase database,
        DarlingCollectorRunner runner,
        ServerRuntime server,
        string databaseName,
        AlwaysOnXeSessionKind kind,
        string captureName,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        try
        {
            var current = runner.AlwaysOnChoices.Get(ServerKey(server), databaseName, kind);
            var ownName = runner.AlwaysOnOwnSessionName(kind);
            var result = await AlwaysOnXeAzureEnsure.RunAsync(database, kind, ownName, current, cancellationToken);
            runner.AlwaysOnChoices.Set(ServerKey(server), databaseName, kind, result.Choice);
            runner.AlwaysOnChoices.ClearReadOnlyRefusal(ServerKey(server), databaseName, kind);

            switch (result.Change)
            {
                case AlwaysOnXeChange.Created:
                    logger?.LogInformation("[{Server}] [{Database}] Created and started {Capture} XE session (database-scoped, {Choice} session)",
                        server.Config.DisplayName, databaseName, captureName, result.Choice == AlwaysOnXeChoice.Own ? "own" : "shared");
                    break;
                case AlwaysOnXeChange.Started:
                    logger?.LogInformation("[{Server}] [{Database}] {Capture} XE session was stopped and has been started ({Choice} session)",
                        server.Config.DisplayName, databaseName, captureName, result.Choice == AlwaysOnXeChoice.Own ? "own" : "shared");
                    break;
                case AlwaysOnXeChange.FellBack:
                    logger?.LogInformation("[{Server}] [{Database}] The shared {Capture} XE session cannot be used in this database, so this install reads its own, {Session}",
                        server.Config.DisplayName, databaseName, captureName, ownName);
                    break;
                case AlwaysOnXeChange.SwitchedBack:
                    logger?.LogInformation("[{Server}] [{Database}] The shared {Capture} XE session can be used again, so this install dropped its own and reads the shared one",
                        server.Config.DisplayName, databaseName, captureName);
                    break;
                default:
                    break;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException && DarlingXeSessions.IsReadOnlyDatabaseRefusal(ex))
        {
            /* #4961: a read-only database, reached without read-only intent (an Azure geo-secondary), cannot hold a session.
               The one message says why and what to change, in place of the server's own and the caps sentence, which does not
               apply. It is a Warning the first time, and a Debug line on the hourly passes after it. */
            var first = runner.AlwaysOnChoices.MarkReadOnlyRefusal(ServerKey(server), databaseName, kind);
            logger?.Log(first ? LogLevel.Warning : LogLevel.Debug, "[{Server}] [{Database}] {Message}",
                server.Config.DisplayName, databaseName, AlwaysOnXeSessions.ReadOnlyDatabaseMessage(kind));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("[{Server}] [{Database}] Failed to ensure {Capture} XE session: {Message}",
                server.Config.DisplayName, databaseName, captureName, AlwaysOnXeSessions.DescribeFailure(ex));
        }
    }

    /// <summary>
    /// The database for one pass: a registration with read-only intent gets a database that opens a second connection, without
    /// the intent, when a create or a drop needs one (#4961); a registration without the intent gets its own connection alone,
    /// as before. The caller disposes the result when it is <see cref="IDisposable"/>.
    /// </summary>
    internal static IAlwaysOnXeDatabase WithReadOnlyIntent(
        DarlingCollectorRunner runner, ServerRuntime server, string databaseName, IAlwaysOnXeDatabase own)
    {
        var ownConnectionString = DarlingXeSessions.LongQueryTraceConnectionString(server, databaseName);
        if (new SqlConnectionStringBuilder(ownConnectionString).ApplicationIntent != ApplicationIntent.ReadOnly)
        {
            return own;
        }

        var withoutIntent = new SqlConnectionStringBuilder(ownConnectionString) { ApplicationIntent = ApplicationIntent.ReadWrite }.ConnectionString;
        return new AlwaysOnXeReadOnlyIntentDatabase(own, async token =>
        {
            if (runner.AlwaysOnXeConnectionForTests is { } open)
            {
                return await open(server, databaseName, withoutIntent, token);
            }

            var connection = new SqlConnection(withoutIntent);
            try
            {
                await connection.OpenAsync(token);
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }

            return new Database(connection, ownsConnection: true);
        });
    }

    /// <summary>One open connection to one Azure SQL Database, as <see cref="AlwaysOnXeAzureEnsure"/> needs it.</summary>
    internal sealed class Database : IAlwaysOnXeDatabase, IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly bool _ownsConnection;

        /// <param name="connection">The open connection.</param>
        /// <param name="ownsConnection">True when this object closes the connection: the one without read-only intent. The
        /// registration's own connection is the caller's to close.</param>
        internal Database(SqlConnection connection, bool ownsConnection = false)
        {
            _connection = connection;
            _ownsConnection = ownsConnection;
        }

        public void Dispose()
        {
            if (_ownsConnection)
            {
                _connection.Dispose();
            }
        }

        public async Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken)
        {
            var state = await ScalarAsync(AlwaysOnXeSessions.BuildAzureCatalogProbeSql(kind, "PerformanceMonitorDarling"), sessionName, cancellationToken);
            return state is int value && Enum.IsDefined(typeof(AlwaysOnXeCatalog), value)
                ? (AlwaysOnXeCatalog)value
                : AlwaysOnXeCatalog.Missing;
        }

        public async Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken) =>
            await ScalarAsync(AlwaysOnXeSessions.BuildAzureStartedProbeSql("PerformanceMonitorDarling"), sessionName, cancellationToken) is int started
            && started == 1;

        public async Task ExecuteAsync(string statement, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(statement, _connection);
            command.CommandTimeout = 60;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public bool IsAlreadyPresent(Exception exception) =>
            exception is SqlException sql && DarlingXeSessions.IsBenignXeSessionAlreadyPresent(sql);

        private async Task<object?> ScalarAsync(string statement, string sessionName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(statement, _connection);
            command.CommandTimeout = 60;
            command.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            return await command.ExecuteScalarAsync(cancellationToken);
        }
    }
}
