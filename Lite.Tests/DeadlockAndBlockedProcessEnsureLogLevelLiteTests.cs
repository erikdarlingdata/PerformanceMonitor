/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The deadlock and blocked-process XE sessions' Azure SQL Database ensure runs on every collector cycle, in every monitored
/// database. A server that refuses the create refuses it on every cycle, so the first failing cycle logs the Warning for each
/// refusing database and the Error for all of them (or the Error for a listing that failed), and the cycles after it log the
/// same lines at Debug until a cycle succeeds (#4964). The retry and the exception are the same on every cycle, so each run
/// still records the failure. The kept state is per server and per session. Each test drives the real ensure with the
/// database list and the per-database work replaced, so no server is needed.
///
/// <para>The same rule covers the server-scoped arms (on-premises, Managed Instance, RDS), whose one connection and ensure
/// are replaced the same way, and the collector's own line for a failed ensure: the first failing cycle logs at today's
/// level, and the cycles after it log at Debug until a cycle of that session on that server succeeds (#4964). The run row
/// and its classification are the same on every cycle.</para>
///
/// <para>In <c>app-logger-statics</c> because the tests read <see cref="AppLogger"/>'s process-wide buffer, which another
/// reader would drain from under them.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class DeadlockAndBlockedProcessEnsureLogLevelLiteTests : IDisposable
{
    private const string Host = "alwayson.database.windows.net";
    private const string Deadlock = "deadlock";
    private const string BlockedProcess = "blocked process";

    private readonly string _tempDir;
    private readonly string _configDir;
    private readonly string _dbPath;

    public DeadlockAndBlockedProcessEnsureLogLevelLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private sealed class Rig
    {
        public required RemoteCollectorService Service { get; init; }
        public required ServerConnection Server { get; init; }
        public required DuckDbInitializer DuckDb { get; init; }
        public required int ServerId { get; init; }

        /* What the logical server lists, before the registration's exclusions. */
        public List<string> Listed { get; set; } = new() { "master", "alpha", "beta", "gamma" };
        public List<(string Session, string Database)> Calls { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Exception? ListFailure { get; set; }

        /* The SQL error number a refusing database says. 262 is a permission refusal, which the collector's run records as
           PERMISSIONS and then skips for the rest of the session; any other number is recorded as ERROR on every cycle. */
        public int RefusalNumber { get; set; } = 262;

        /* The server-scoped arm: the error its one connection and ensure fail with, or null when they succeed. */
        public SqlException? ServerFailure { get; set; }
        public List<string> ServerCalls { get; } = new();

        /* The databases tried for the session, named as the server names it (the ensure's session name, not its log label). */
        public int TriedFor(string session) =>
            Calls.Count(c => c.Session == (session == Deadlock ? DeadlocksCollector.XeSessionName : BlockedProcessReportCollector.XeSessionName));

        public void RefuseEveryDatabase()
        {
            foreach (var database in new[] { "alpha", "beta", "gamma" })
            {
                Refuse.Add(database);
            }
        }
    }

    private static ServerConnection NewServer() =>
        new() { ServerName = Host, DisplayName = "alwayson-" + Guid.NewGuid().ToString("N")[..8] };

    private async Task<Rig> BuildRigAsync()
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var server = NewServer();
        var servers = new ServerManager(_configDir);
        servers.AddServer(server);

        /* Azure SQL Database, so a collector run takes the per-database ensure. */
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("deadlocks", enabled: true);
        schedules.UpdateSchedule("blocked_process_report", enabled: true);

        var rig = new Rig
        {
            Service = new RemoteCollectorService(duckDb, servers, schedules),
            Server = server,
            DuckDb = duckDb,
            ServerId = RemoteCollectorService.GetServerId(server),
        };

        rig.Service.XeSessionDatabaseListOverrideForTests = (_, _) =>
            rig.ListFailure is { } failure
                ? Task.FromException<List<string>>(failure)
                : Task.FromResult(rig.Listed.ToList());

        rig.Service.XeSessionDatabaseEnsureOverrideForTests = (_, session, database, _) =>
        {
            rig.Calls.Add((session, database));
            return rig.Refuse.Contains(database)
                ? Task.FromException(SqlExceptionFactory.Create(rig.RefusalNumber, errorClass: 14, message: $"The create was refused in {database}."))
                : Task.CompletedTask;
        };

        rig.Service.XeSessionServerEnsureOverrideForTests = (_, session, _) =>
        {
            rig.ServerCalls.Add(session);
            return rig.ServerFailure is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        };

        return rig;
    }

    /// <summary>One cycle of the named session's ensure on an Azure SQL Database server, for any server of the rig's service.</summary>
    private static Task EnsureAsync(Rig rig, string session, ServerConnection? server = null) =>
        session == Deadlock
            ? rig.Service.EnsureDeadlockXeSessionAsync(server ?? rig.Server, engineEdition: 5, CancellationToken.None)
            : rig.Service.EnsureBlockedProcessXeSessionAsync(server ?? rig.Server, engineEdition: 5, CancellationToken.None);

    /// <summary>A cycle that fails: the collector still gets the exception it records the failure from, on every cycle.</summary>
    private static async Task FailingCycleAsync(Rig rig, string session, ServerConnection? server = null)
    {
        var raised = await Assert.ThrowsAsync<XeSessionEnsureException>(() => EnsureAsync(rig, session, server));
        Assert.Equal(session, raised.SessionKind);
    }

    /// <summary>The lines this server logged in the buffer since the last drain.</summary>
    private static List<string> Lines(ServerConnection server) => ForServer(AppLogger.DrainBufferedLines(), server);

    private static List<string> ForServer(IEnumerable<string> drained, ServerConnection server) =>
        drained.Where(line => line.Contains(server.DisplayName, StringComparison.Ordinal)).ToList();

    private static int Count(IEnumerable<string> lines, string level, string text) =>
        lines.Count(line => line.Contains(level, StringComparison.Ordinal) && line.Contains(text, StringComparison.Ordinal));

    private static string RefusalLine(string session) => $"Failed to ensure {session} XE session";

    private static string AllRefusedLine(string session) => $"Failed to ensure the {session} XE session in all 3 database(s)";

    private static string ListingLine(string session) => $"Failed to enumerate databases for {session} XE sessions";

    /// <summary>
    /// The create is refused in every database on every cycle. The first cycle logs a Warning for each refusing database and
    /// an Error for the all-refused summary; the cycles after it log the same lines at Debug. The retry does not change:
    /// every cycle tries every database, and every cycle throws.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task ACreateRefusedInEveryDatabase_LogsOneWarningSetAndOneError_ThenDebug(string session)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync();
            rig.RefuseEveryDatabase();
            var lines = new List<string>();

            await FailingCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));
            Assert.Equal(3, Count(lines, "WARN", RefusalLine(session)));
            Assert.Equal(1, Count(lines, "ERROR", AllRefusedLine(session)));

            await FailingCycleAsync(rig, session);
            await FailingCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(3, Count(lines, "WARN", RefusalLine(session)));
            Assert.Equal(1, Count(lines, "ERROR", AllRefusedLine(session)));
            Assert.Equal(6, Count(lines, "DEBUG", RefusalLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", AllRefusedLine(session)));

            /* The retry did not change: three cycles tried all three databases. */
            Assert.Equal(9, rig.TriedFor(session));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// A cycle that succeeds ends the run of refusals, so the next refusal warns again: the Warning set and the Error come
    /// back once, and their repeats are Debug.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task ACreateRefusedAgainAfterItSucceeded_WarnsAgain(string session)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync();
            rig.RefuseEveryDatabase();
            var lines = new List<string>();

            await FailingCycleAsync(rig, session);
            await FailingCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));
            Assert.Equal(3, Count(lines, "WARN", RefusalLine(session)));
            Assert.Equal(3, Count(lines, "DEBUG", RefusalLine(session)));

            rig.Refuse.Clear();
            await EnsureAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            rig.RefuseEveryDatabase();
            await FailingCycleAsync(rig, session);
            await FailingCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(6, Count(lines, "WARN", RefusalLine(session)));
            Assert.Equal(2, Count(lines, "ERROR", AllRefusedLine(session)));
            Assert.Equal(6, Count(lines, "DEBUG", RefusalLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", AllRefusedLine(session)));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// The ensure's own listing of the databases fails with a SQL error on every cycle: the first cycle logs its Error, and
    /// the cycles after it log Debug.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task AListingThatFailsWithASqlError_LogsOneError_ThenDebug(string session)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync();
            var lines = new List<string>();

            for (var cycle = 0; cycle < 3; cycle++)
            {
                rig.ListFailure = SqlExceptionFactory.Create(4060, errorClass: 11, message: "master is not readable.");
                await FailingCycleAsync(rig, session);
                lines.AddRange(Lines(rig.Server));
            }

            Assert.Equal(1, Count(lines, "ERROR", ListingLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", ListingLine(session)));
            Assert.Empty(rig.Calls);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// The kept state is per session: the deadlock session's run of refusals does not turn the blocked-process session's
    /// first failure into a Debug line, and the reverse.
    /// </summary>
    [Fact]
    public async Task ARunOfRefusalsOfOneSession_DoesNotQuietTheOthersFirstFailure()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync();
            rig.RefuseEveryDatabase();
            var lines = new List<string>();

            await FailingCycleAsync(rig, Deadlock);
            await FailingCycleAsync(rig, Deadlock);
            await FailingCycleAsync(rig, BlockedProcess);
            await FailingCycleAsync(rig, BlockedProcess);
            lines.AddRange(Lines(rig.Server));

            foreach (var session in new[] { Deadlock, BlockedProcess })
            {
                Assert.Equal(3, Count(lines, "WARN", RefusalLine(session)));
                Assert.Equal(1, Count(lines, "ERROR", AllRefusedLine(session)));
                Assert.Equal(3, Count(lines, "DEBUG", RefusalLine(session)));
                Assert.Equal(1, Count(lines, "DEBUG", AllRefusedLine(session)));
            }
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// The kept state is per server: one server's run of refusals does not turn another server's first failure into a Debug
    /// line, and a success on one does not end the other's run.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task ARunOfRefusalsOnOneServer_DoesNotQuietAnotherServersFirstFailure(string session)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync();
            var other = NewServer();
            rig.RefuseEveryDatabase();

            await FailingCycleAsync(rig, session);
            await FailingCycleAsync(rig, session);
            await FailingCycleAsync(rig, session, other);
            var drained = AppLogger.DrainBufferedLines();
            var first = ForServer(drained, rig.Server);
            var second = ForServer(drained, other);

            Assert.Equal(3, Count(first, "WARN", RefusalLine(session)));
            Assert.Equal(1, Count(first, "ERROR", AllRefusedLine(session)));
            Assert.Equal(3, Count(second, "WARN", RefusalLine(session)));
            Assert.Equal(1, Count(second, "ERROR", AllRefusedLine(session)));

            /* The other server's success does not end the first server's run: its next refusal is still Debug. */
            rig.Refuse.Clear();
            await EnsureAsync(rig, session, other);
            rig.RefuseEveryDatabase();
            await FailingCycleAsync(rig, session);
            var again = Lines(rig.Server);

            Assert.Equal(0, Count(again, "WARN", RefusalLine(session)));
            Assert.Equal(0, Count(again, "ERROR", AllRefusedLine(session)));
            Assert.Equal(3, Count(again, "DEBUG", RefusalLine(session)));
            Assert.Equal(1, Count(again, "DEBUG", AllRefusedLine(session)));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    private static async Task WithDebugLoggingAsync(Func<Task> body)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();
            await body();
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>One cycle of the named session's ensure on a server-scoped server: on-premises, Managed Instance or RDS.</summary>
    private static Task EnsureOnPremAsync(Rig rig, string session, ServerConnection? server = null) =>
        session == Deadlock
            ? rig.Service.EnsureDeadlockXeSessionAsync(server ?? rig.Server, engineEdition: 3, CancellationToken.None)
            : rig.Service.EnsureBlockedProcessXeSessionAsync(server ?? rig.Server, engineEdition: 3, CancellationToken.None);

    /// <summary>A server-scoped cycle that fails: the collector still gets the exception it records the failure from.</summary>
    private static async Task FailingOnPremCycleAsync(Rig rig, string session, ServerConnection? server = null)
    {
        var raised = await Assert.ThrowsAsync<XeSessionEnsureException>(() => EnsureOnPremAsync(rig, session, server));
        Assert.Equal(session, raised.SessionKind);
        Assert.Same(rig.ServerFailure, raised.InnerException);
    }

    private static string CollectorName(string session) => session == Deadlock ? "deadlocks" : "blocked_process_report";

    /// <summary>The collector's own line for a failed ensure, as <c>RunCollectorAsync</c> writes it.</summary>
    private static string CollectorLine(string session) => $"{CollectorName(session)} Failed to ensure {session} XE session";

    /// <summary>One run of the session's collector on the Azure SQL Database server. The ensure fails before any read, so no server is needed.</summary>
    private static Task RunCollectorAsync(Rig rig, string session) =>
        rig.Service.RunCollectorAsync(rig.Server, CollectorName(session), CancellationToken.None);

    /// <summary>The collection_log rows the test's server has for the session's collector, oldest first.</summary>
    private static async Task<List<(string Status, string? Error)>> ReadRunsAsync(Rig rig, string session)
    {
        using var connection = rig.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT status, error_message FROM collection_log WHERE server_id = {rig.ServerId} AND collector_name = '{CollectorName(session)}' ORDER BY log_id";
        using var reader = await command.ExecuteReaderAsync();
        var runs = new List<(string, string?)>();
        while (await reader.ReadAsync())
        {
            runs.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return runs;
    }

    /// <summary>
    /// The server-scoped ensure fails with a SQL error that is not a permission refusal on every cycle: the first cycle logs
    /// its Error, and the cycles after it log Debug. The retry does not change: every cycle connects and ensures, and every
    /// cycle throws the exception the collector records its failure from.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task AServerScopedEnsureThatFailsWithASqlError_LogsOneError_ThenDebug(string session)
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync();
            rig.ServerFailure = SqlExceptionFactory.Create(1105, errorClass: 17, message: "The filegroup is full.");
            var lines = new List<string>();

            await FailingOnPremCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));
            Assert.Equal(1, Count(lines, "ERROR", RefusalLine(session)));

            await FailingOnPremCycleAsync(rig, session);
            await FailingOnPremCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(1, Count(lines, "ERROR", RefusalLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", RefusalLine(session)));
            Assert.Equal(0, Count(lines, "WARN", RefusalLine(session)));
            Assert.Equal(3, rig.ServerCalls.Count(call => call == SessionName(session)));
        });
    }

    /// <summary>
    /// A permission refusal is the same rule at its own level: the first cycle logs its Warning and the cycles after it log
    /// Debug. A collector run records PERMISSIONS and is skipped from then on, so the ensure is called directly here.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task AServerScopedEnsureRefusedForPermission_LogsOneWarning_ThenDebug(string session)
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync();
            rig.ServerFailure = SqlExceptionFactory.Create(262, errorClass: 14, message: "CREATE EVENT SESSION permission was denied.");
            var lines = new List<string>();

            await FailingOnPremCycleAsync(rig, session);
            await FailingOnPremCycleAsync(rig, session);
            await FailingOnPremCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(1, Count(lines, "WARN", RefusalLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", RefusalLine(session)));
            Assert.Equal(0, Count(lines, "ERROR", RefusalLine(session)));
        });
    }

    /// <summary>
    /// A cycle that succeeds ends the run of failures, so the next failure logs at its level again. An engine's answer that the
    /// session already exists and is running is a success too.
    /// </summary>
    [Theory]
    [InlineData(Deadlock, false)]
    [InlineData(Deadlock, true)]
    [InlineData(BlockedProcess, false)]
    [InlineData(BlockedProcess, true)]
    public async Task AServerScopedEnsureThatFailsAgainAfterItSucceeded_LogsItsErrorAgain(string session, bool alreadyPresent)
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync();
            var failure = SqlExceptionFactory.Create(1105, errorClass: 17, message: "The filegroup is full.");
            rig.ServerFailure = failure;
            var lines = new List<string>();

            await FailingOnPremCycleAsync(rig, session);
            await FailingOnPremCycleAsync(rig, session);

            /* The engine says the session exists and runs (25631), or the ensure simply succeeds. Neither throws. */
            rig.ServerFailure = alreadyPresent ? SqlExceptionFactory.Create(25631, errorClass: 16, message: "The event session already exists.") : null;
            await EnsureOnPremAsync(rig, session);

            rig.ServerFailure = failure;
            await FailingOnPremCycleAsync(rig, session);
            await FailingOnPremCycleAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(2, Count(lines, "ERROR", RefusalLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", RefusalLine(session)));
        });
    }

    /// <summary>
    /// The kept state is per session for the server-scoped arm too: the deadlock session's run of failures does not turn the
    /// blocked-process session's first failure into a Debug line, and the reverse.
    /// </summary>
    [Fact]
    public async Task ARunOfServerScopedFailuresOfOneSession_DoesNotQuietTheOthersFirstFailure()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync();
            rig.ServerFailure = SqlExceptionFactory.Create(1105, errorClass: 17, message: "The filegroup is full.");
            var lines = new List<string>();

            await FailingOnPremCycleAsync(rig, Deadlock);
            await FailingOnPremCycleAsync(rig, Deadlock);
            await FailingOnPremCycleAsync(rig, BlockedProcess);
            await FailingOnPremCycleAsync(rig, BlockedProcess);
            lines.AddRange(Lines(rig.Server));

            foreach (var session in new[] { Deadlock, BlockedProcess })
            {
                Assert.Equal(1, Count(lines, "ERROR", RefusalLine(session)));
                Assert.Equal(1, Count(lines, "DEBUG", RefusalLine(session)));
            }
        });
    }

    private static string SessionName(string session) =>
        session == Deadlock ? DeadlocksCollector.XeSessionName : BlockedProcessReportCollector.XeSessionName;

    /// <summary>
    /// The collector's own line for a failed ensure follows the ensure's: the first failing cycle logs it at Error, and the
    /// cycles after it log it at Debug. Every run still records ERROR with the same message, counts toward the collector's
    /// consecutive errors, and flags the XE session unavailable.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task ACollectorWhoseEnsureFailsOnEveryCycle_LogsItsOwnLineOnce_ThenDebug(string session)
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync();
            rig.RefuseEveryDatabase();
            rig.RefusalNumber = 4060;
            var lines = new List<string>();

            await RunCollectorAsync(rig, session);
            lines.AddRange(Lines(rig.Server));
            Assert.Equal(1, Count(lines, "ERROR", CollectorLine(session)));

            await RunCollectorAsync(rig, session);
            await RunCollectorAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(1, Count(lines, "ERROR", CollectorLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", CollectorLine(session)));

            /* The run row and its classification are the same on every cycle, and so is the retry. */
            var runs = await ReadRunsAsync(rig, session);
            Assert.Equal(3, runs.Count);
            Assert.All(runs, run =>
            {
                Assert.Equal("ERROR", run.Status);
                Assert.Contains($"Failed to ensure {session} XE session", run.Error, StringComparison.Ordinal);
            });
            Assert.Equal(9, rig.TriedFor(session));

            var failure = Assert.Single(rig.Service.GetHealthSummary(rig.ServerId).XeSessionFailures);
            Assert.Equal(3, failure.ConsecutiveErrors);
        });
    }

    /// <summary>
    /// A run whose ensure succeeded ends the run of failures, so the next failing run logs its own line at Error again.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task ACollectorWhoseEnsureFailsAgainAfterItSucceeded_LogsItsOwnLineAtErrorAgain(string session)
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync();
            rig.RefuseEveryDatabase();
            rig.RefusalNumber = 4060;
            var lines = new List<string>();

            await RunCollectorAsync(rig, session);
            await RunCollectorAsync(rig, session);

            /* The ensure succeeds in every database. (A run would go on to read the server, so the ensure is called directly.) */
            rig.Refuse.Clear();
            await EnsureAsync(rig, session);

            rig.RefuseEveryDatabase();
            await RunCollectorAsync(rig, session);
            await RunCollectorAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(2, Count(lines, "ERROR", CollectorLine(session)));
            Assert.Equal(2, Count(lines, "DEBUG", CollectorLine(session)));
            Assert.Equal(4, (await ReadRunsAsync(rig, session)).Count(run => run.Status == "ERROR"));
        });
    }

    /// <summary>
    /// A permission refusal is classified PERMISSIONS, and its collector line is a Warning the first time and Debug after it.
    /// The scheduler skips a collector that was refused for permission, so the second run happens only once the server's
    /// health is cleared, as removing the server does.
    /// </summary>
    [Theory]
    [InlineData(Deadlock)]
    [InlineData(BlockedProcess)]
    public async Task ACollectorRefusedForPermission_LogsItsOwnLineAtWarning_ThenDebug(string session)
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync();
            rig.RefuseEveryDatabase();
            var lines = new List<string>();

            await RunCollectorAsync(rig, session);
            lines.AddRange(Lines(rig.Server));
            Assert.Equal(1, Count(lines, "WARN", CollectorLine(session)));

            rig.Service.ClearHealthForServer(rig.ServerId);
            await RunCollectorAsync(rig, session);
            lines.AddRange(Lines(rig.Server));

            Assert.Equal(1, Count(lines, "WARN", CollectorLine(session)));
            Assert.Equal(1, Count(lines, "DEBUG", CollectorLine(session)));
            Assert.Equal(0, Count(lines, "ERROR", CollectorLine(session)));

            var runs = await ReadRunsAsync(rig, session);
            Assert.Equal(2, runs.Count);
            Assert.All(runs, run => Assert.Equal("PERMISSIONS", run.Status));
        });
    }
}
