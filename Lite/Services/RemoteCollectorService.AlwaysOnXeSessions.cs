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
    /* The choice this install has made for each deadlock and blocked-process session in each Azure SQL Database it monitors
       (#4961): the shared session, or its own. The ensure sets it on every cycle and the read takes its name from it. In
       memory only: a restart starts every database at the shared session, and the first ensure sets it again. */
    private readonly AlwaysOnXeChoices _alwaysOnChoices = new();

    /// <summary>The choices the ensure has made, for the read and for a test.</summary>
    internal AlwaysOnXeChoices AlwaysOnChoices => _alwaysOnChoices;

    /// <summary>
    /// A test replaces the connection to one Azure SQL Database for the always-on sessions' ensure: the server and the
    /// database. The ensure's decisions then run against what it returns, in place of a server. Null in production.
    /// </summary>
    internal Func<ServerConnection, string, CancellationToken, Task<IAlwaysOnXeDatabase>>? AlwaysOnXeDatabaseForTests { get; set; }

    /// <summary>This install's own session of the capture, or null when the service has no install id to make it from.</summary>
    internal string? AlwaysOnOwnSessionName(AlwaysOnXeSessionKind kind) =>
        AlwaysOnXeSessions.TryOwnNameFor(LongQueryCompletionsCollector.LiteProduct, GetInstallId(), kind);

    /// <summary>
    /// The session a read of the capture names in one Azure SQL Database: the install's own when the ensure fell back to it
    /// there, else the shared name. The per-database read sets it on the collector context beside the database name.
    /// </summary>
    internal string AlwaysOnReadSessionName(ServerConnection server, string databaseName, AlwaysOnXeSessionKind kind) =>
        _alwaysOnChoices.NameFor(server.Id, databaseName, kind, AlwaysOnOwnSessionName(kind));

    /// <summary>
    /// One database's pass of the Azure ensure for the deadlock or blocked-process session (#4961): the shared session when it
    /// is usable, the install's own when it is not, and back to the shared one once it is usable again. The decisions are
    /// <see cref="AlwaysOnXeAzureEnsure"/>'s, the same for both products; this opens the connection, keeps the choice and
    /// logs what the pass did. A failure leaves the choice as it was and reaches the shared driver's per-database catch.
    /// </summary>
    private async Task EnsureAlwaysOnXeSessionInDatabaseAsync(
        ServerConnection server,
        AlwaysOnXeSessionKind kind,
        string captureName,
        string databaseName,
        CancellationToken cancellationToken)
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

            var current = _alwaysOnChoices.Get(server.Id, databaseName, kind);
            var result = await AlwaysOnXeAzureEnsure.RunAsync(
                database, kind, AlwaysOnOwnSessionName(kind), current, cancellationToken);
            _alwaysOnChoices.Set(server.Id, databaseName, kind, result.Choice);

            var label = $"[Azure SQL DB:{databaseName}] {char.ToUpperInvariant(captureName[0])}{captureName[1..]} XE session";
            switch (result.Change)
            {
                case AlwaysOnXeChange.Created:
                    AppLogger.Info("XeSession", $"{label} created and started (database-scoped, {DescribeChoice(result.Choice)})");
                    break;
                case AlwaysOnXeChange.Started:
                    AppLogger.Info("XeSession", $"{label} was stopped and has been started ({DescribeChoice(result.Choice)})");
                    break;
                case AlwaysOnXeChange.FellBack:
                    AppLogger.Info("XeSession", $"{label}: the shared session cannot be used in this database, so this install reads its own, {AlwaysOnOwnSessionName(kind)}");
                    break;
                case AlwaysOnXeChange.SwitchedBack:
                    AppLogger.Info("XeSession", $"{label}: the shared session can be used again, so this install dropped its own and reads the shared one");
                    break;
                default:
                    /* Debug, not Info: this fires once per monitored database per cycle (#1535). */
                    AppLogger.Debug("XeSession", $"{label} verified (database-scoped, {DescribeChoice(result.Choice)})");
                    break;
            }
        }
        finally
        {
            connection?.Dispose();
        }
    }

    private static string DescribeChoice(AlwaysOnXeChoice choice) => choice == AlwaysOnXeChoice.Own ? "own session" : "shared session";

    /// <summary>One open connection to one Azure SQL Database, as <see cref="AlwaysOnXeAzureEnsure"/> needs it.</summary>
    private sealed class LiteAlwaysOnXeDatabase : IAlwaysOnXeDatabase
    {
        private readonly SqlConnection _connection;

        public LiteAlwaysOnXeDatabase(SqlConnection connection)
        {
            _connection = connection;
        }

        public async Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken)
        {
            var state = await ScalarAsync(AlwaysOnXeSessions.BuildAzureCatalogProbeSql(kind, "PerformanceMonitorLite"), sessionName, cancellationToken);
            return state is int value && Enum.IsDefined(typeof(AlwaysOnXeCatalog), value)
                ? (AlwaysOnXeCatalog)value
                : AlwaysOnXeCatalog.Missing;
        }

        public async Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken) =>
            await ScalarAsync(AlwaysOnXeSessions.BuildAzureStartedProbeSql("PerformanceMonitorLite"), sessionName, cancellationToken) is int started
            && started == 1;

        public async Task ExecuteAsync(string statement, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(statement, _connection);
            command.CommandTimeout = CommandTimeoutSeconds;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public bool IsAlreadyPresent(Exception exception) =>
            exception is SqlException sql && IsBenignXeSessionAlreadyPresent(sql);

        private async Task<object?> ScalarAsync(string statement, string sessionName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(statement, _connection);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            return await command.ExecuteScalarAsync(cancellationToken);
        }
    }
}
