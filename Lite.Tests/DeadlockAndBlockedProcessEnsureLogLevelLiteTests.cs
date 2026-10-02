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
using Microsoft.Extensions.Logging;
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

        /* What the logical server lists, before the registration's exclusions. */
        public List<string> Listed { get; set; } = new() { "master", "alpha", "beta", "gamma" };
        public List<(string Session, string Database)> Calls { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Exception? ListFailure { get; set; }

        public IEnumerable<string> Tried(string session) => Calls.Where(c => c.Session == session).Select(c => c.Database);

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

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("deadlocks", enabled: true);
        schedules.UpdateSchedule("blocked_process_report", enabled: true);

        var rig = new Rig
        {
            Service = new RemoteCollectorService(duckDb, servers, schedules),
            Server = server,
        };

        rig.Service.XeSessionDatabaseListOverrideForTests = (_, _) =>
            rig.ListFailure is { } failure
                ? Task.FromException<List<string>>(failure)
                : Task.FromResult(rig.Listed.ToList());

        rig.Service.XeSessionDatabaseEnsureOverrideForTests = (_, session, database, _) =>
        {
            rig.Calls.Add((session, database));
            return rig.Refuse.Contains(database)
                ? Task.FromException(SqlExceptionFactory.Create(262, errorClass: 14, message: $"The create was refused in {database}."))
                : Task.CompletedTask;
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
            Assert.Equal(9, rig.Tried(session).Count());
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
}
