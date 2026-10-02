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

    /// <summary>
    /// A test replaces each connection the always-on sessions' ensure would open to one Azure SQL Database, below
    /// <see cref="AlwaysOnXeDatabaseForTests"/> (which wins when both are set): the server, the database, and the connection
    /// string the open would use, so a test sees whether each statement goes over a connection with read-only intent (#4961).
    /// Null in production.
    /// </summary>
    internal Func<ServerConnection, string, string, CancellationToken, Task<IAlwaysOnXeDatabase>>? AlwaysOnXeConnectionForTests { get; set; }

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
        IAlwaysOnXeDatabase? database = null;
        try
        {
            database = await OpenAlwaysOnXeDatabaseAsync(server, databaseName, c => connection = c, cancellationToken);

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
            (database as IDisposable)?.Dispose();
            connection?.Dispose();
        }
    }

    /// <summary>
    /// The connection to one Azure SQL Database for the always-on sessions' ensure, and for a removed server's drop of the
    /// install's own sessions. A registration with read-only intent gets a database that opens a second connection, without
    /// the intent, when a create or a drop needs one (#4961); one without the intent gets its own connection alone. A test
    /// replaces the whole database (<see cref="AlwaysOnXeDatabaseForTests"/>, no wrapper) or each connection
    /// (<see cref="AlwaysOnXeConnectionForTests"/>). <paramref name="connectionOpened"/> hands the caller the connection to
    /// dispose; the caller also disposes the returned database when it is <see cref="IDisposable"/>.
    /// </summary>
    private async Task<IAlwaysOnXeDatabase> OpenAlwaysOnXeDatabaseAsync(
        ServerConnection server, string databaseName, Action<SqlConnection> connectionOpened, CancellationToken cancellationToken)
    {
        if (AlwaysOnXeDatabaseForTests is { } open)
        {
            return await open(server, databaseName, cancellationToken);
        }

        IAlwaysOnXeDatabase own;
        if (AlwaysOnXeConnectionForTests is { } openConnection)
        {
            own = await openConnection(server, databaseName, AzureDatabaseConnectionString(server, databaseName), cancellationToken);
        }
        else
        {
            var connection = await OpenAzureDatabaseConnectionAsync(server, databaseName, cancellationToken);
            connectionOpened(connection);
            own = new LiteAlwaysOnXeDatabase(connection);
        }

        if (new SqlConnectionStringBuilder(AzureDatabaseConnectionString(server, databaseName)).ApplicationIntent != ApplicationIntent.ReadOnly)
        {
            return own;
        }

        return new AlwaysOnXeReadOnlyIntentDatabase(own, async token =>
        {
            if (AlwaysOnXeConnectionForTests is { } openWithoutIntent)
            {
                return await openWithoutIntent(server, databaseName, AzureDatabaseConnectionString(server, databaseName, withoutReadOnlyIntent: true), token);
            }

            var withoutIntent = await OpenAzureDatabaseConnectionAsync(server, databaseName, token, withoutReadOnlyIntent: true);
            return new LiteAlwaysOnXeDatabase(withoutIntent, ownsConnection: true);
        });
    }

    /// <summary>The shared driver's connection-level ensure, for the arms that ensure through <see cref="EnsureAlwaysOnXeSessionInDatabaseAsync"/> instead. Never called.</summary>
    private static Task AlwaysOnArmEnsureNotUsed(SqlConnection connection, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("The always-on sessions ensure through the per-database routine.");

    private static string DescribeChoice(AlwaysOnXeChoice choice) => choice == AlwaysOnXeChoice.Own ? "own session" : "shared session";

    /// <summary>One open connection to one Azure SQL Database, as <see cref="AlwaysOnXeAzureEnsure"/> needs it.</summary>
    private sealed class LiteAlwaysOnXeDatabase : IAlwaysOnXeDatabase, IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly bool _ownsConnection;

        /// <param name="connection">The open connection.</param>
        /// <param name="ownsConnection">True when this object closes the connection: the one without read-only intent, which the
        /// host never sees. The registration's own connection is the caller's to close.</param>
        public LiteAlwaysOnXeDatabase(SqlConnection connection, bool ownsConnection = false)
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

        /* #4961: the shared marking leaves a read-only database's refusal without the caps sentence. */
        public IEnumerable<int> ErrorNumbers(Exception exception) => ErrorNumbersOf(exception);

        private async Task<object?> ScalarAsync(string statement, string sessionName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(statement, _connection);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.Add(new SqlParameter("@session_name", SqlDbType.NVarChar, 128) { Value = sessionName });
            return await command.ExecuteScalarAsync(cancellationToken);
        }
    }
}
