/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Security.Principal;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6476: whose identity a command-line connection test used. <c>--test-connection</c> and <c>--add-server</c> probe in the command-line
/// process, which runs as the person at the console. For a server that uses Windows authentication that proves only that this person can
/// connect: the service runs as another account (<c>NT SERVICE\PerformanceMonitor Darling</c> unless it was re-homed), so a pass can still
/// be followed by a login error in the service, and a failure here can be a pass in the service.
///
/// <para>The service already has the path that tests as itself: the <c>test_connect</c> command the Viewer queues in
/// <c>config.config_command</c>. This asks it the same way (queue, poll, cancel a row it never claimed, delete the credential-bearing
/// row after a reply) when the Windows service is running. When it is not, or does not answer, the command-line test still runs as the
/// console user and <see cref="ConsoleUserNote"/> says so plainly, with the grant that gives the service account a login. A
/// SQL-authenticated server needs none of this: the login, not the process account, is what the server sees.</para>
/// </summary>
public static class DarlingServiceConnectionTest
{
    /// <summary>The account a default install runs the service as.</summary>
    internal const string DefaultServiceAccount = @"NT SERVICE\PerformanceMonitor Darling";

    /// <summary>The overall wait for one reply: the service claims commands on a ~5 second tick, and a connect has its own timeout.</summary>
    internal static readonly TimeSpan DefaultBudget = TimeSpan.FromSeconds(45);

    /// <summary>The command <c>RequestedBy</c> the CLI stamps on its rows.</summary>
    internal const string RequestedBy = "cli";

    /// <summary>True when a connection test of this server is a test of the process account: a SQL Server target on Windows authentication.</summary>
    internal static bool UsesWindowsAuthentication(MonitoredServer server) =>
        !server.IsPostgres && string.Equals(server.Auth, "integrated", StringComparison.OrdinalIgnoreCase);

    /// <summary>The account this command-line process runs as, as Windows names it; the user name where Windows cannot say.</summary>
    internal static string ConsoleIdentity()
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                return WindowsIdentity.GetCurrent().Name;
            }
        }
        catch (Exception)
        {
            /* fall through to the plain user name */
        }

        return Environment.UserName;
    }

    /// <summary>
    /// What to print under a Windows-authentication result that was NOT produced by the service: whose identity it tested, why the service
    /// did not test it, how to test as the service, and the login to grant when the service then fails. Pure.
    /// </summary>
    internal static string[] ConsoleUserNote(string consoleIdentity, string whyNotService) => new[]
    {
        $"         NOTE: this test ran as {consoleIdentity}, the person at this console, not as the service. The service runs as its own",
        $"         account ({DefaultServiceAccount} unless it was re-homed), so this result does not prove the service can connect, and a",
        "         failure here does not prove it cannot. " + whyNotService,
        "         To test as the service, start it and run --test-connection (it asks the running service). If the service then fails with a login error, grant its account a login:",
        $"         CREATE LOGIN [{DefaultServiceAccount}] FROM WINDOWS;  (the account the service runs as, with the permissions in the README).",
    };

    /// <summary>The line printed under a result the running service produced.</summary>
    internal const string TestedByServiceNote =
        "         Tested by the running service, as the account it runs under: this is the identity collection will use.";

    /// <summary>The store queue the test goes through. A seam so the wait, cancel and cleanup logic runs under test without a store.</summary>
    public interface IServiceCommandPlane
    {
        Task<long> EnqueueAsync(string commandType, string argsJson, CancellationToken cancellationToken);

        Task<(string Status, string? ResultStatus, string? ResultJson)?> ReadAsync(long commandId, CancellationToken cancellationToken);

        /// <summary>Fails the row only while it is still pending; true when this call did.</summary>
        Task<bool> CancelPendingAsync(long commandId, CancellationToken cancellationToken);

        Task DeleteAsync(long commandId, CancellationToken cancellationToken);
    }

    /// <summary>The answer of one service-side test: a probe result, or the reason the service did not answer (then the caller probes itself).</summary>
    public sealed record ServiceTestOutcome(ConnectionProbeResult? Probe, string? NotAnsweredReason)
    {
        public bool Answered => Probe is not null;
    }

    public interface IServiceConnectionTester
    {
        Task<ServiceTestOutcome> TestAsync(MonitoredServer server, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Tests through the service. <paramref name="serviceState"/> is asked once: a service that is not Running is not asked at all, so the
    /// installer's pre-flight (no service yet) does not wait on a reply that cannot come. A service that is Running but does not answer in the
    /// budget is not asked again for the next server.
    /// </summary>
    internal sealed class QueuedServiceConnectionTester : IServiceConnectionTester
    {
        private readonly IServiceCommandPlane _queue;
        private readonly Func<CancellationToken, Task<string>> _serviceState;
        private readonly TimeSpan _budget;
        private readonly TimeSpan _pollInterval;
        private string? _unavailable;
        private bool _stateChecked;

        internal QueuedServiceConnectionTester(
            IServiceCommandPlane queue, Func<CancellationToken, Task<string>> serviceState, TimeSpan budget, TimeSpan? pollInterval = null)
        {
            _queue = queue;
            _serviceState = serviceState;
            _budget = budget;
            _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(400);
        }

        public async Task<ServiceTestOutcome> TestAsync(MonitoredServer server, CancellationToken cancellationToken)
        {
            if (!_stateChecked)
            {
                _stateChecked = true;
                var state = await _serviceState(cancellationToken);
                if (!string.Equals(state, "Running", StringComparison.OrdinalIgnoreCase))
                {
                    _unavailable = $"The service is not running (state: {state}), so it could not be asked.";
                }
            }

            if (_unavailable is not null)
            {
                return new ServiceTestOutcome(null, _unavailable);
            }

            long commandId;
            try
            {
                commandId = await _queue.EnqueueAsync("test_connect", JsonSerializer.Serialize(server), cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _unavailable = $"The store did not take the request ({ex.Message}), so the service could not be asked.";
                return new ServiceTestOutcome(null, _unavailable);
            }

            var clock = Stopwatch.StartNew();
            try
            {
                while (clock.Elapsed < _budget)
                {
                    var row = await _queue.ReadAsync(commandId, cancellationToken);
                    if (row is { } read && read.Status is "succeeded" or "failed")
                    {
                        await TryDeleteAsync(commandId);
                        return new ServiceTestOutcome(ParseReply(read.ResultStatus, read.ResultJson), null);
                    }

                    await Task.Delay(_pollInterval, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                await TryCancelAsync(commandId);
                throw;
            }

            /* Out of time. A row the service never claimed is failed so it cannot run later; a row it IS running is left to it (it clears its own
               args_json when it reports). Either way the service did not answer in time. */
            await TryCancelAsync(commandId);
            _unavailable = $"The service did not answer within {(int)_budget.TotalSeconds} seconds.";
            return new ServiceTestOutcome(null, _unavailable);
        }

        private async Task TryCancelAsync(long commandId)
        {
            try
            {
                await _queue.CancelPendingAsync(commandId, CancellationToken.None);
            }
            catch (Exception)
            {
                /* best effort: the service's expiry is the backstop for a row nobody is waiting for */
            }
        }

        private async Task TryDeleteAsync(long commandId)
        {
            try
            {
                await _queue.DeleteAsync(commandId, CancellationToken.None);
            }
            catch (Exception)
            {
                /* best effort: a cleanup failure must not hide the answer */
            }
        }
    }

    /// <summary>
    /// Rebuilds the probe result from the <c>result_json</c> the service wrote (<see cref="DarlingCommandExecutor.MapProbeResult"/>). Pure.
    /// A reply with no readable body is a failure whose text names what the service said.
    /// </summary>
    internal static ConnectionProbeResult ParseReply(string? resultStatus, string? resultJson)
    {
        try
        {
            using var document = JsonDocument.Parse(resultJson ?? "{}");
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return Failed(resultStatus);
            }

            if (!root.TryGetProperty("success", out var success) || success.ValueKind != JsonValueKind.True)
            {
                var error = TryString(root, "error") ?? resultStatus ?? "the service reported a failure";
                return new ConnectionProbeResult(false, 0, 0, null, false, false, false, false, error);
            }

            var engine = Enum.TryParse<CollectorTargetEngine>(TryString(root, "engine"), ignoreCase: true, out var parsed)
                ? parsed
                : CollectorTargetEngine.SqlServer;
            return new ConnectionProbeResult(
                Success: true,
                MajorVersion: TryInt(root, "majorVersion"),
                EngineEdition: TryInt(root, "engineEdition"),
                EngineEditionDescription: TryString(root, "engineEditionDescription"),
                IsAzureSqlDb: TryBool(root, "isAzureSqlDb"),
                IsAzureManagedInstance: TryBool(root, "isAzureManagedInstance"),
                IsAwsRds: TryBool(root, "isAwsRds"),
                HasMsdbAccess: TryBool(root, "hasMsdbAccess"),
                Error: null,
                Engine: engine,
                PostgresMajorVersion: TryInt(root, "postgresMajorVersion"),
                PostgresVersionNum: TryInt(root, "postgresVersionNum"),
                IsAurora: TryBool(root, "isAurora"),
                IsInRecovery: TryBool(root, "isInRecovery"),
                ConnectedDatabase: TryString(root, "connectedDatabase"),
                MachineIdentity: TryString(root, "machineIdentity"),
                QueryStoreReplica: TryString(root, "queryStoreReplica"),
                QueryStoreReplicaError: TryString(root, "queryStoreReplicaError"),
                MissingGrantsNote: TryString(root, "missingGrantsNote"));
        }
        catch (JsonException)
        {
            return Failed(resultStatus);
        }
    }

    private static ConnectionProbeResult Failed(string? resultStatus) =>
        new(false, 0, 0, null, false, false, false, false, $"the service answered '{resultStatus}' with no readable result");

    private static string? TryString(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static int TryInt(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var number) ? number : 0;

    private static bool TryBool(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.True;

    /// <summary>The queue over the store's <c>config.config_command</c>, the same rows and statements shape the Viewer queues.</summary>
    internal sealed class PostgresServiceCommandPlane : IServiceCommandPlane
    {
        private const string EnqueueSql =
            "INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at) " +
            "VALUES ($1, $2, NULL, $3, 'pending', (now() AT TIME ZONE 'UTC')) RETURNING command_id";

        private const string ReadSql = "SELECT status, result_status, result_json FROM config.config_command WHERE command_id = $1";

        /* Only while pending, so a row the service already claimed is left alone; args_json is nulled because a test_connect row can carry a
           credential blob. The same atomic shape as the Viewer's cancel and the service's expiry. */
        private const string CancelSql =
            "UPDATE config.config_command SET status = 'failed', completed_at = now() AT TIME ZONE 'UTC', " +
            "result_status = 'cancelled', result_json = jsonb_build_object('success', false, 'error', 'cancelled: the command line stopped waiting'), " +
            "args_json = NULL WHERE command_id = $1 AND status = 'pending'";

        private const string DeleteSql = "DELETE FROM config.config_command WHERE command_id = $1";

        private readonly NpgsqlDataSource _postgres;

        internal PostgresServiceCommandPlane(NpgsqlDataSource postgres) => _postgres = postgres;

        public async Task<long> EnqueueAsync(string commandType, string argsJson, CancellationToken cancellationToken)
        {
            await using var command = _postgres.CreateCommand(EnqueueSql);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = RequestedBy });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = commandType });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = argsJson });
            return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
        }

        public async Task<(string Status, string? ResultStatus, string? ResultJson)?> ReadAsync(long commandId, CancellationToken cancellationToken)
        {
            await using var command = _postgres.CreateCommand(ReadSql);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = commandId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetFieldValue<string>(2));
        }

        public async Task<bool> CancelPendingAsync(long commandId, CancellationToken cancellationToken)
        {
            await using var command = _postgres.CreateCommand(CancelSql);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = commandId });
            return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
        }

        public async Task DeleteAsync(long commandId, CancellationToken cancellationToken)
        {
            await using var command = _postgres.CreateCommand(DeleteSql);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = commandId });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Runs one Windows-authentication target's test and returns the result plus the lines to print under it: through <paramref name="tester"/>
    /// when there is one and it answers, otherwise through <paramref name="probeAsConsole"/> with the console-user note. A target that is not
    /// Windows-authenticated is probed as before and gets no note. Pure over its seams, so every branch runs under test.
    /// </summary>
    internal static async Task<(ConnectionProbeResult Probe, string[] Notes)> TestAsync(
        MonitoredServer server,
        IServiceConnectionTester? tester,
        Func<MonitoredServer, CancellationToken, Task<ConnectionProbeResult>> probeAsConsole,
        string consoleIdentity,
        bool windowsHost,
        CancellationToken cancellationToken)
    {
        if (!UsesWindowsAuthentication(server) || !windowsHost)
        {
            return (await probeAsConsole(server, cancellationToken), Array.Empty<string>());
        }

        var reason = "Nothing was asked of the service here.";
        if (tester is not null)
        {
            var outcome = await tester.TestAsync(server, cancellationToken);
            if (outcome.Answered)
            {
                return (outcome.Probe!, new[] { TestedByServiceNote });
            }

            reason = outcome.NotAnsweredReason ?? reason;
        }

        var console = await probeAsConsole(server, cancellationToken);
        return (console, ConsoleUserNote(consoleIdentity, reason));
    }

    /// <summary>
    /// The production tester: opens the store only when a Windows-authentication server is first tested (a run of SQL logins never
    /// touches it), and asks the Windows service's state through the same Get-Service read the setup verbs use.
    /// </summary>
    internal sealed class LazyStoreServiceConnectionTester : IServiceConnectionTester, IAsyncDisposable
    {
        private readonly string _connectionString;
        private NpgsqlDataSource? _dataSource;
        private QueuedServiceConnectionTester? _inner;
        private string? _storeFault;

        internal LazyStoreServiceConnectionTester(string connectionString) => _connectionString = connectionString;

        public async Task<ServiceTestOutcome> TestAsync(MonitoredServer server, CancellationToken cancellationToken)
        {
            if (_storeFault is not null)
            {
                return new ServiceTestOutcome(null, _storeFault);
            }

            if (_inner is null)
            {
                try
                {
                    _dataSource = NpgsqlDataSource.Create(
                        DarlingStoreConnection.PinSessionTimeZoneUtc(
                            DarlingStoreConnection.WithApplicationName(_connectionString, DarlingManagedPostgres.CliApplicationName)));
                    _inner = new QueuedServiceConnectionTester(
                        new PostgresServiceCommandPlane(_dataSource),
                        token => OperatingSystem.IsWindows()
                            ? DarlingCliCommands.DescribeServiceStateAsync(token)
                            : Task.FromResult("not a Windows host"),
                        DefaultBudget);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _storeFault = $"The store could not be opened ({ex.Message}), so the service could not be asked.";
                    return new ServiceTestOutcome(null, _storeFault);
                }
            }

            return await _inner.TestAsync(server, cancellationToken);
        }

        public ValueTask DisposeAsync() => _dataSource?.DisposeAsync() ?? ValueTask.CompletedTask;
    }
}
