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
    private readonly List<DuckDbInitializer> _initializers = [];

    private const string Host = "alwayson.database.windows.net";
    private const string Deadlock = "deadlock";
    private const string BlockedProcess = "blocked process";

    /* #5640: the client-timeout session shares the server-scoped arm, so it follows the same rule. */
    private const string Attention = "client timeout";

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
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

        /* #7639: the databases the deadlock READ can open, each with the reader it returns. Any other database fails the read,
           as a database with no session or a refused connection does. */
        public Dictionary<string, Func<System.Data.Common.DbDataReader>> Readable { get; } = new(StringComparer.OrdinalIgnoreCase);

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
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var server = NewServer();
        var servers = new ServerManager(_configDir);
        servers.AddServer(server);

        /* Azure SQL Database, so a collector run takes the per-database ensure. */
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("deadlocks", enabled: true);
        schedules.UpdateSchedule("blocked_process_report", enabled: true);
        schedules.UpdateSchedule("attention", enabled: true);

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

        /* The deadlock read follows the ensure in a collector run, so it reads the same databases through fake readers. */
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(rig.Listed.ToList());
        rig.Service.AzureDatabaseReaderOverrideForTests = (database, _) =>
            rig.Readable.TryGetValue(database, out var open)
                ? open()
                : throw SqlExceptionFactory.Create(4060, errorClass: 11, message: $"Cannot open database \"{database}\".");

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
            : session == Attention
                ? rig.Service.EnsureAttentionXeSessionAsync(server ?? rig.Server, engineEdition: 3, CancellationToken.None)
                : rig.Service.EnsureBlockedProcessXeSessionAsync(server ?? rig.Server, engineEdition: 3, CancellationToken.None);

    /// <summary>A server-scoped cycle that fails: the collector still gets the exception it records the failure from.</summary>
    private static async Task FailingOnPremCycleAsync(Rig rig, string session, ServerConnection? server = null)
    {
        var raised = await Assert.ThrowsAsync<XeSessionEnsureException>(() => EnsureOnPremAsync(rig, session, server));
        Assert.Equal(session, raised.SessionKind);
        Assert.Same(rig.ServerFailure, raised.InnerException);
    }

    private static string CollectorName(string session) =>
        session == Deadlock ? "deadlocks" : session == Attention ? "attention" : "blocked_process_report";

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
    [InlineData(Attention)]
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
    [InlineData(Attention)]
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
        session == Deadlock ? DeadlocksCollector.XeSessionName
        : session == Attention ? AlwaysOnXeSessions.AttentionXeSessionName
        : BlockedProcessReportCollector.XeSessionName;

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

    private static System.Data.Common.DbDataReader DeadlockReader(DateTime time, string? sourceDatabase, string graph) =>
        new FakeDeadlockReader(
            new[] { new object[] { time, "process1", graph, (object?)sourceDatabase ?? DBNull.Value } },
            new[] { new object[] { 100L, false } });

    private static async Task<List<string>> ReadStoredDeadlockGraphsAsync(Rig rig)
    {
        using var connection = rig.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT deadlock_graph_xml FROM deadlocks WHERE server_id = {rig.ServerId} ORDER BY deadlock_graph_xml";
        using var reader = await command.ExecuteReaderAsync();
        var graphs = new List<string>();
        while (await reader.ReadAsync())
        {
            graphs.Add(reader.GetString(0));
        }

        return graphs;
    }

    /// <summary>
    /// #7639: the deadlock session could not be created in any database of an Azure SQL Database server (a login without
    /// ALTER ANY DATABASE EVENT SESSION). The run used to end at the ensure, so the master item's telemetry arm, which needs no
    /// session, never ran and the grid stayed empty. The read now goes ahead and stores what it finds, and the run still records
    /// the ensure's PERMISSIONS and its message.
    /// </summary>
    [Fact]
    public async Task AnEnsureRefusedInEveryDatabase_StillStoresTheTelemetryDeadlocks_AndRecordsThePermissionRun()
    {
        var rig = await BuildRigAsync();
        rig.RefuseEveryDatabase();
        rig.Readable["master"] = () => DeadlockReader(DateTime.UtcNow.AddMinutes(-1), "alpha", "<deadlock id=\"telemetry\"/>");

        await RunCollectorAsync(rig, Deadlock);

        Assert.Equal(new[] { "<deadlock id=\"telemetry\"/>" }, await ReadStoredDeadlockGraphsAsync(rig));

        var run = Assert.Single(await ReadRunsAsync(rig, Deadlock));
        Assert.Equal("PERMISSIONS", run.Status);
        Assert.Contains("Failed to ensure deadlock XE session", run.Error, StringComparison.Ordinal);
    }

    /// <summary>
    /// #7639: the session was refused in one database and made in the others. The others are read and stored, and the run's
    /// note names the database that is missing, with the server's reason and the grant that fixes it, so a short grid is not
    /// read as a quiet server.
    /// </summary>
    [Fact]
    public async Task AnEnsureRefusedInOneDatabase_StoresTheOthers_AndTheNoteNamesTheRefusedOne()
    {
        var rig = await BuildRigAsync();
        rig.Refuse.Add("beta");
        rig.Readable["master"] = () => new FakeDeadlockReader(Array.Empty<object[]>(), new[] { new object[] { 100L, false } });
        rig.Readable["alpha"] = () => DeadlockReader(DateTime.UtcNow.AddMinutes(-2), null, "<deadlock id=\"alpha\"/>");
        rig.Readable["gamma"] = () => DeadlockReader(DateTime.UtcNow.AddMinutes(-1), null, "<deadlock id=\"gamma\"/>");

        await RunCollectorAsync(rig, Deadlock);

        Assert.Equal(new[] { "<deadlock id=\"alpha\"/>", "<deadlock id=\"gamma\"/>" }, await ReadStoredDeadlockGraphsAsync(rig));

        var run = Assert.Single(await ReadRunsAsync(rig, Deadlock));
        Assert.Equal("SUCCESS", run.Status);
        Assert.Contains("beta", run.Error, StringComparison.Ordinal);
        Assert.Contains("The create was refused in beta.", run.Error, StringComparison.Ordinal);
        Assert.Contains("ALTER ANY DATABASE EVENT SESSION", run.Error, StringComparison.Ordinal);
    }

    /// <summary>
    /// #7639: the same partial refusal on the blocked process and client timeout runs. Their sessions were made in the other
    /// databases and refused in one, and the run succeeded without saying so, so a short grid read as a quiet server. Their run
    /// notes now name the refused database, the server's reason and the grant, as the deadlock run's does. The refused
    /// database cannot be opened, which the Azure loop skips, and the other two read no rows, so the run succeeds.
    /// </summary>
    [Theory]
    [InlineData(BlockedProcess)]
    [InlineData(Attention)]
    public async Task AnEnsureRefusedInOneDatabase_PutsTheSameNoteOnTheBlockedProcessAndClientTimeoutRuns(string session)
    {
        var rig = await BuildRigAsync();
        rig.Refuse.Add("beta");
        rig.Readable["alpha"] = () => new FakeDeadlockReader(Array.Empty<object[]>(), Array.Empty<object[]>());
        rig.Readable["gamma"] = () => new FakeDeadlockReader(Array.Empty<object[]>(), Array.Empty<object[]>());

        await RunCollectorAsync(rig, session);

        var run = Assert.Single(await ReadRunsAsync(rig, session));
        Assert.True(run.Status == "SUCCESS", run.Status + ": " + run.Error);
        Assert.Contains("beta", run.Error, StringComparison.Ordinal);
        Assert.Contains("The create was refused in beta.", run.Error, StringComparison.Ordinal);
        Assert.Contains("ALTER ANY DATABASE EVENT SESSION", run.Error, StringComparison.Ordinal);
    }

    /// <summary>
    /// #7639: a list that is set but names no database the server has attempts nothing and refuses nothing. The run used to record
    /// SUCCESS with no rows and no reason, which reads as a quiet server. Its note now says the list matches no database. An empty
    /// list (every database) and a list that names a live database carry no such note.
    /// </summary>
    [Theory]
    [InlineData(Deadlock, "dropped_last_week", true)]
    [InlineData(BlockedProcess, "dropped_last_week", true)]
    [InlineData(Attention, "dropped_last_week", true)]
    [InlineData(BlockedProcess, "", false)]
    [InlineData(BlockedProcess, "alpha", false)]
    public async Task AnXeSessionDatabaseList_MatchingNoDatabase_PutsANoteOnTheRun_AndAnEmptyOrMatchingListDoesNot(string session, string list, bool expectNote)
    {
        var rig = await BuildRigAsync();
        rig.Server.XeSessionDatabases = list.Length == 0 ? new List<string>() : new List<string> { list };
        foreach (var database in new[] { "master", "alpha", "beta", "gamma" })
        {
            rig.Readable[database] = () => new FakeDeadlockReader(Array.Empty<object[]>(), session == Deadlock ? new[] { new object[] { 100L, false } } : Array.Empty<object[]>());
        }

        await RunCollectorAsync(rig, session);

        var run = Assert.Single(await ReadRunsAsync(rig, session));
        Assert.True(run.Status == "SUCCESS", run.Status + ": " + run.Error);
        Assert.Equal(expectNote, run.Error?.Contains("matches no database on this server", StringComparison.Ordinal) ?? false);
    }

    /// <summary>
    /// #7639: every database failed its ensure with an exception that is not a SQL error (a token or connection-open fault). The
    /// shared driver raises it raw, and the deadlock run used to end before the read. Darling records it as an ensure failure and reads
    /// first, so Lite does too: the master item's telemetry arm still stores its deadlock, and the run then records the failure.
    /// </summary>
    [Fact]
    public async Task AnEnsureFailedInEveryDatabaseWithANonSqlFault_StillStoresTheTelemetryDeadlocks_AndRecordsTheFailureAfterIt()
    {
        var rig = await BuildRigAsync();
        rig.Service.XeSessionDatabaseEnsureOverrideForTests = (_, _, _, _) =>
            Task.FromException(new InvalidOperationException("The access token could not be acquired."));
        rig.Readable["master"] = () => DeadlockReader(DateTime.UtcNow.AddMinutes(-1), "alpha", "<deadlock id=\"telemetry\"/>");

        await RunCollectorAsync(rig, Deadlock);

        Assert.Equal(new[] { "<deadlock id=\"telemetry\"/>" }, await ReadStoredDeadlockGraphsAsync(rig));

        var run = Assert.Single(await ReadRunsAsync(rig, Deadlock));
        Assert.Equal("ERROR", run.Status);
        Assert.Contains("The access token could not be acquired.", run.Error, StringComparison.Ordinal);
    }

    private sealed class FakeDeadlockReader(object[][] rows, object[][] gate) : System.Data.Common.DbDataReader
    {
        private readonly object[][][] _sets = { rows, gate };
        private int _set;
        private int _row = -1;
        private object[] Cur => _sets[_set][_row];

        public override bool Read() => ++_row < _sets[_set].Length;
        public override bool NextResult() { if (_set + 1 >= _sets.Length) { return false; } _set++; _row = -1; return true; }
        public override string GetString(int o) => (string)Cur[o];
        public override long GetInt64(int o) => (long)Cur[o];
        public override DateTime GetDateTime(int o) => (DateTime)Cur[o];
        public override bool GetBoolean(int o) => (bool)Cur[o];
        public override bool IsDBNull(int o) => Cur[o] is DBNull;
        public override object GetValue(int o) => Cur[o];
        public override int FieldCount => _sets[_set].Length == 0 ? 0 : _sets[_set][0].Length;
        public override bool HasRows => _sets[_set].Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int o] => Cur[o];
        public override object this[string n] => throw new NotSupportedException();
        public override byte GetByte(int o) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public override char GetChar(int o) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int bo, int l) => throw new NotSupportedException();
        public override string GetDataTypeName(int o) => throw new NotSupportedException();
        public override decimal GetDecimal(int o) => throw new NotSupportedException();
        public override double GetDouble(int o) => throw new NotSupportedException();
        public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int o) => throw new NotSupportedException();
        public override float GetFloat(int o) => throw new NotSupportedException();
        public override Guid GetGuid(int o) => throw new NotSupportedException();
        public override short GetInt16(int o) => throw new NotSupportedException();
        public override int GetInt32(int o) => throw new NotSupportedException();
        public override string GetName(int o) => throw new NotSupportedException();
        public override int GetOrdinal(string n) => throw new NotSupportedException();
        public override int GetValues(object[] v) => throw new NotSupportedException();
    }
}
