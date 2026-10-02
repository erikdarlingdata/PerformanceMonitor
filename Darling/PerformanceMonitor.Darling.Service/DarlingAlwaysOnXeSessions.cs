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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("[{Server}] [{Database}] Failed to ensure {Capture} XE session: {Message}",
                server.Config.DisplayName, databaseName, captureName, AlwaysOnXeSessions.DescribeFailure(ex));
        }
    }

    /// <summary>One open connection to one Azure SQL Database, as <see cref="AlwaysOnXeAzureEnsure"/> needs it.</summary>
    internal sealed class Database : IAlwaysOnXeDatabase
    {
        private readonly SqlConnection _connection;

        internal Database(SqlConnection connection)
        {
            _connection = connection;
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

        /* #4961: the shared marking leaves a read-only database's refusal without the caps sentence. */
        public IEnumerable<int> ErrorNumbers(Exception exception) => DarlingXeSessions.ErrorNumbersOf(exception);

        private async Task<object?> ScalarAsync(string statement, string sessionName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(statement, _connection);
            command.CommandTimeout = 60;
            command.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            return await command.ExecuteScalarAsync(cancellationToken);
        }
    }
}
