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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The long-query trace's session lifecycle on Azure SQL Database, where each database holds its own session: which
/// databases a reconcile creates the session in and drops it from, and how a failed drop is retried. Each test drives
/// the real reconcile with the database list and the per-database work replaced, so no server is needed.
///
/// <para>In <c>app-logger-statics</c> because two tests read <see cref="AppLogger"/>'s process-wide buffer, which
/// another reader would drain from under them.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class LongQueryTraceLifecycleLiteTests : IDisposable
{
    private const string Host = "lqtrace.database.windows.net";

    private readonly string _tempDir;
    private readonly string _configDir;
    private readonly string _dbPath;

    public LongQueryTraceLifecycleLiteTests()
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
        public required ServerManager Servers { get; init; }
        public required ScheduleManager Schedules { get; init; }
        public required ServerConnection Server { get; init; }

        /* What the logical server's master lists, before the registration's exclusions. */
        public List<string> Listed { get; set; } = new() { "master", "alpha", "beta", "gamma" };
        public List<(string Database, bool Create)> Calls { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Exception? ListFailure { get; set; }
        public int ListCalls { get; set; }

        /* The time each reconcile runs at. */
        public DateTime Clock { get; set; } = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

        public Task ReconcileAsync() => Service.ReconcileLongQueryCompletionsXeSessionAsync(Server, CancellationToken.None);

        public bool? Applied => Service.LongQueryTraceAppliedState(Server.Id);

        public IEnumerable<string> Dropped => Calls.Where(c => !c.Create).Select(c => c.Database);

        public IEnumerable<string> Created => Calls.Where(c => c.Create).Select(c => c.Database);
    }

    /// <summary>A logical-server registration (no database named) on an Azure SQL Database server.</summary>
    private Task<Rig> BuildRigAsync(bool traceOn, params string[] excluded) =>
        BuildRigAsync(
            new ServerConnection
            {
                ServerName = Host,
                DisplayName = "lqtrace-" + Guid.NewGuid().ToString("N")[..8],
                ExcludedDatabases = excluded.ToList(),
            },
            traceOn);

    /// <summary>A registration of one database of the same logical server, with read-only intent unless told otherwise.</summary>
    private async Task<Rig> BuildDatabaseRigAsync(string database, bool traceOn, bool readOnlyIntent = true)
    {
        var rig = await BuildRigAsync(
            new ServerConnection
            {
                ServerName = Host,
                DisplayName = "lqtrace-" + database,
                DatabaseName = database,
                ReadOnlyIntent = readOnlyIntent,
            },
            traceOn);
        rig.Listed = new List<string> { database };
        return rig;
    }

    /// <summary>A server on an engine with no per-database sessions (edition 3, an on-premises server): its session is the server's.</summary>
    private Task<Rig> BuildOnPremRigAsync(bool traceOn) =>
        BuildRigAsync(
            new ServerConnection { ServerName = "lqtrace-sql", DisplayName = "lqtrace-onprem-" + Guid.NewGuid().ToString("N")[..8] },
            traceOn,
            engineEdition: 3);

    private async Task<Rig> BuildRigAsync(ServerConnection server, bool traceOn, int engineEdition = 5)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = engineEdition;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("long_query_completions", enabled: traceOn);

        var rig = new Rig
        {
            Service = new RemoteCollectorService(duckDb, servers, schedules),
            Servers = servers,
            Schedules = schedules,
            Server = server,
        };

        rig.Service.LongQueryTraceUtcNowForTests = () => rig.Clock;
        rig.Service.LongQueryTraceListOverrideForTests = (registration, allDatabases, _) =>
        {
            rig.ListCalls++;
            if (rig.ListFailure is { } failure)
            {
                throw failure;
            }

            return Task.FromResult(allDatabases
                ? rig.Listed.ToList()
                : rig.Listed.Where(d => !registration.ExcludedDatabases.Contains(d, StringComparer.OrdinalIgnoreCase)).ToList());
        };

        rig.Service.LongQueryTraceDatabaseOverrideForTests = (_, database, create, _) =>
        {
            rig.Calls.Add((database, create));
            return rig.Refuse.Contains(database)
                ? Task.FromException(new InvalidOperationException($"The {(create ? "create" : "drop")} was refused in {database}."))
                : Task.CompletedTask;
        };

        return rig;
    }

    /// <summary>Registers one database of the same logical server as its own server.</summary>
    private static void RegisterSeparately(Rig rig, string database) =>
        rig.Servers.AddServer(new ServerConnection { ServerName = Host, DisplayName = $"{Host} {database}", DatabaseName = database });

    /// <summary>
    /// Registers another server of the same logical server with its own trace setting: one database when
    /// <paramref name="database"/> is given, otherwise a second registration of the logical server.
    /// </summary>
    private static ServerConnection RegisterOther(Rig rig, string? database, bool traceOn, bool readOnlyIntent, params string[] excluded)
    {
        var other = new ServerConnection
        {
            ServerName = Host,
            DisplayName = $"{Host} {database ?? "server"} {(readOnlyIntent ? "read-only" : "read-write")}",
            DatabaseName = database,
            ReadOnlyIntent = readOnlyIntent,
            ExcludedDatabases = excluded.ToList(),
        };
        rig.Servers.AddServer(other);
        SetTrace(rig, other, traceOn);
        return other;
    }

    private static void SetTrace(Rig rig, ServerConnection server, bool traceOn) =>
        rig.Schedules.SetScheduleForServer(
            server.Id,
            new List<CollectorSchedule> { new() { Name = "long_query_completions", Enabled = traceOn } });

    /* ── M1: a failed drop is retried, with a cap ── */

    [Fact]
    public async Task Off_AFailedListing_IsNotMarkedDone_AndTheNextCycleListsAgain()
    {
        var rig = await BuildRigAsync(traceOn: false);
        rig.ListFailure = SqlExceptionFactory.Create(40613, 20, "Database 'master' is not currently available.");

        await rig.ReconcileAsync();

        Assert.Null(rig.Applied);

        rig.ListFailure = null;
        await rig.ReconcileAsync();

        Assert.Equal(2, rig.ListCalls);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
        Assert.False(rig.Applied);
    }

    [Fact]
    public async Task Off_OneDatabaseRefusesTheDrop_TheOthersAreStillDropped_AndTheNextCycleRetries()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Warning);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: false);
            rig.Refuse.Add("beta");

            await rig.ReconcileAsync();

            Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
            Assert.Null(rig.Applied);
            Assert.Contains(AppLogger.DrainBufferedLines(), line =>
                line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)
                && line.Contains("[beta]", StringComparison.Ordinal)
                && line.Contains("WARN", StringComparison.Ordinal));

            rig.Calls.Clear();
            await rig.ReconcileAsync();

            Assert.Contains("beta", rig.Dropped);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Off_TheFifthFailedPassInARow_IsMarkedDone_WithOneWarningThatNamesTheDatabase()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Warning);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: false);
            rig.Refuse.Add("beta");

            for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
                Assert.Null(rig.Applied);
            }

            await rig.ReconcileAsync();
            Assert.False(rig.Applied);

            /* Done: the next cycle does not try again. */
            rig.Calls.Clear();
            await rig.ReconcileAsync();
            Assert.Empty(rig.Calls);

            var giveUps = AppLogger.DrainBufferedLines()
                .Where(line => line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)
                    && line.Contains("Stopped retrying", StringComparison.Ordinal))
                .ToList();
            var giveUp = Assert.Single(giveUps);
            Assert.Contains("beta", giveUp, StringComparison.Ordinal);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── After the cap: one attempt an hour ── */

    [Fact]
    public async Task Off_AfterTheCap_TriesAgainOnceAnHour_AtDebug_WithNoSecondWarning()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: false);
            rig.Refuse.Add("beta");

            for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            Assert.False(rig.Applied);
            var giveUp = Assert.Single(Lines(rig), line => line.Contains("Stopped retrying", StringComparison.Ordinal));
            Assert.Contains(
                "It tries again once an hour, and right away after a restart, a change to the trace's settings, or a change to"
                + " another registration of these databases.",
                giveUp,
                StringComparison.Ordinal);
            Assert.DoesNotContain("reconnects", giveUp, StringComparison.Ordinal);

            /* Within the hour: nothing. */
            rig.Calls.Clear();
            rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            Assert.Empty(rig.Calls);

            /* An hour after the cap: one attempt, and the cycle after it runs nothing. */
            rig.Clock += TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);

            /* It fails again an hour later. Both failures log at Debug, and the cap's warning stays the only one. */
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            Assert.Equal(new[] { "alpha", "beta", "gamma", "alpha", "beta", "gamma" }, rig.Dropped);

            var afterTheCap = Lines(rig);
            Assert.DoesNotContain(afterTheCap, line => line.Contains("WARN", StringComparison.Ordinal));
            Assert.Equal(2, afterTheCap.Count(line =>
                line.Contains("DEBUG", StringComparison.Ordinal)
                && line.Contains("The next attempt is in an hour.", StringComparison.Ordinal)));
            Assert.Equal(2, afterTheCap.Count(line =>
                line.Contains("DEBUG", StringComparison.Ordinal)
                && line.Contains("[beta]", StringComparison.Ordinal)));
            Assert.False(rig.Applied);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task AfterTheCap_AnAttemptThatSucceeds_EndsTheHourlyAttempts()
    {
        var rig = await BuildRigAsync(traceOn: false);
        rig.Refuse.Add("beta");

        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync();
        }

        rig.Refuse.Clear();
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);

        /* Done, with nothing left to retry: an hour later nothing runs. */
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();
        Assert.Empty(rig.Calls);
    }

    [Fact]
    public async Task On_AfterTheCap_TheCyclesInBetween_KeepTheHourlyAttempt()
    {
        /* gamma is excluded, so the drop outside the set tries it, and gamma refuses. */
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        rig.Refuse.Add("gamma");

        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync();
        }

        Assert.True(rig.Applied);

        /* Each cycle in the hour creates the session where it is missing and leaves the drop alone. A cycle that
           skips the drop must not end the hourly attempt. */
        rig.Calls.Clear();
        for (var cycle = 1; cycle <= 3; cycle++)
        {
            rig.Clock += TimeSpan.FromMinutes(15);
            await rig.ReconcileAsync();
        }

        Assert.Empty(rig.Dropped);
        Assert.Equal(new[] { "alpha", "beta", "alpha", "beta", "alpha", "beta" }, rig.Created);

        rig.Clock += TimeSpan.FromMinutes(15);
        await rig.ReconcileAsync();
        Assert.Equal(new[] { "gamma" }, rig.Dropped);
    }

    /// <summary>This rig's lines in the log buffer since the last drain.</summary>
    private static List<string> Lines(Rig rig) =>
        AppLogger.DrainBufferedLines()
            .Where(line => line.Contains(rig.Server.DisplayName, StringComparison.Ordinal))
            .ToList();

    /* ── #4964: a create that fails again and again logs its first failure at Warning, the repeats at Debug ── */

    private const string ReconcileLine = "Failed to reconcile long-query completion XE session";

    /// <summary>The failure the reconcile kept for the collector's run, read from the service's own slot.</summary>
    private static Exception? KeptFault(Rig rig)
    {
        var slot = typeof(RemoteCollectorService).GetField("_longQueryTraceFault", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var faults = (ConcurrentDictionary<string, Exception>)slot.GetValue(rig.Service)!;
        return faults.TryGetValue(rig.Server.Id, out var fault) ? fault : null;
    }

    private static int Count(IEnumerable<string> lines, string level, string text) =>
        lines.Count(line => line.Contains(level, StringComparison.Ordinal) && line.Contains(text, StringComparison.Ordinal));

    [Fact]
    public async Task On_ACreateThatFailsOnEveryCycle_LogsOneWarning_ThenDebug_AndRecordsTheFaultEachTime()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            var lines = new List<string>();

            rig.ListFailure = new InvalidOperationException("master is not readable, first.");
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));
            Assert.Contains("first.", KeptFault(rig)!.Message, StringComparison.Ordinal);

            /* The same failure on the next two cycles: no new Warning, one Debug line each, and the fault is kept
               again with the newest message, so the run is still classified from it. */
            foreach (var attempt in new[] { "second.", "third." })
            {
                rig.ListFailure = new InvalidOperationException("master is not readable, " + attempt);
                await rig.ReconcileAsync();
                lines.AddRange(Lines(rig));
                Assert.Contains(attempt, KeptFault(rig)!.Message, StringComparison.Ordinal);
            }

            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));
            Assert.Equal(2, Count(lines, "DEBUG", ReconcileLine));

            /* The retry did not change: every cycle listed again. */
            Assert.Equal(3, rig.ListCalls);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task On_ACreateThatSucceedsAfterFailures_WarnsAgainWhenItFailsAfterwards()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            var lines = new List<string>();

            rig.ListFailure = new InvalidOperationException("master is not readable.");
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));
            Assert.Equal(1, Count(lines, "DEBUG", ReconcileLine));

            /* A create that succeeds ends the run of failures, and the fault it kept. */
            rig.ListFailure = null;
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Null(KeptFault(rig));
            Assert.True(rig.Applied);

            /* The next failure is a new one: a Warning, and its repeat is Debug. */
            rig.ListFailure = new InvalidOperationException("master is not readable again.");
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(2, Count(lines, "WARN", ReconcileLine));
            Assert.Equal(2, Count(lines, "DEBUG", ReconcileLine));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Off_ADropFailureAfterCreateFailures_IsNotCountedAsARepeat()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            var lines = new List<string>();

            rig.ListFailure = new InvalidOperationException("master is not readable.");
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));

            /* Turned off, the drop is a different pass with its own failure: its first line is a Warning. The cap on the
               drop side is its own (#4944). */
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", "Could not list the databases to drop the long-query trace session from"));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── #4964: the per-database create lines of the shared Azure ensure ── */

    private const string EnsureLine = "Failed to ensure long query completions XE session";

    /// <summary>
    /// The create is refused in every database on every cycle. The shared ensure logs a Warning for each refusing database
    /// and an Error for the all-refused summary on the first cycle; the cycles after it log the same lines at Debug until a
    /// create succeeds. The retry does not change: every cycle tries every database, and the fault stays set.
    /// </summary>
    [Fact]
    public async Task On_ACreateRefusedInEveryDatabase_LogsOneWarningSetAndOneError_ThenDebug()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            foreach (var database in new[] { "alpha", "beta", "gamma" })
            {
                rig.Refuse.Add(database);
            }

            const string allRefused = "Failed to ensure the long query completions XE session in all 3 database(s)";
            var lines = new List<string>();

            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(3, Count(lines, "WARN", EnsureLine));
            Assert.Equal(1, Count(lines, "ERROR", allRefused));

            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));

            Assert.Equal(3, Count(lines, "WARN", EnsureLine));
            Assert.Equal(1, Count(lines, "ERROR", allRefused));
            Assert.Equal(6, Count(lines, "DEBUG", EnsureLine));
            Assert.Equal(2, Count(lines, "DEBUG", allRefused));

            /* The retry and the fault did not change: three cycles tried all three databases, and the run is still told. */
            Assert.Equal(9, rig.Created.Count());
            Assert.NotNull(KeptFault(rig));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// A create that succeeds ends the run of refusals, so the next refusal warns again: the Warning set and the Error
    /// come back once, and their repeats are Debug.
    /// </summary>
    [Fact]
    public async Task On_ACreateRefusedAgainAfterItSucceeded_WarnsAgain()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            foreach (var database in new[] { "alpha", "beta", "gamma" })
            {
                rig.Refuse.Add(database);
            }

            const string allRefused = "Failed to ensure the long query completions XE session in all 3 database(s)";
            var lines = new List<string>();

            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(3, Count(lines, "WARN", EnsureLine));
            Assert.Equal(3, Count(lines, "DEBUG", EnsureLine));

            rig.Refuse.Clear();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Null(KeptFault(rig));

            foreach (var database in new[] { "alpha", "beta", "gamma" })
            {
                rig.Refuse.Add(database);
            }

            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(6, Count(lines, "WARN", EnsureLine));
            Assert.Equal(2, Count(lines, "ERROR", allRefused));
            Assert.Equal(6, Count(lines, "DEBUG", EnsureLine));
            Assert.Equal(2, Count(lines, "DEBUG", allRefused));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// The create's own listing of the monitored databases fails with a SQL error on every cycle: its Error line is the
    /// first failure's, and the cycles after it log Debug.
    /// </summary>
    [Fact]
    public async Task On_AListingThatFailsWithASqlError_LogsOneError_ThenDebug()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            const string listing = "Failed to enumerate databases for long query completions XE sessions";
            var lines = new List<string>();

            for (var cycle = 0; cycle < 3; cycle++)
            {
                rig.ListFailure = SqlExceptionFactory.Create(4060, errorClass: 11, message: "master is not readable.");
                await rig.ReconcileAsync();
                lines.AddRange(Lines(rig));
            }

            Assert.Equal(1, Count(lines, "ERROR", listing));
            Assert.Equal(2, Count(lines, "DEBUG", listing));
            Assert.NotNull(KeptFault(rig));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── #4964: the drop of a server-scoped session follows the same cap as the Azure arm's ── */

    /* The server-scoped session reaches the test replacement with no database name. */
    private const string TheServer = "";

    private const string DropLine = "Could not drop the long-query trace session:";

    /// <summary>
    /// On an engine with no per-database sessions, a disabled trace whose drop fails on every cycle follows the cadence of
    /// the Azure arm: the first four failures are Warnings that the next cycle tries again, the fifth is the one Warning that
    /// gives up, the cycles after it run nothing, and an hour later one attempt logs at Debug. Before, the failure reached
    /// the general catch, which warned on every cycle with no end.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_ADropThatFailsOnEveryCycle_WarnsToTheCap_ThenTriesOnceAnHourAtDebug()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildOnPremRigAsync(traceOn: false);
            rig.Refuse.Add(TheServer);
            var lines = new List<string>();

            for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
                Assert.Null(rig.Applied);
            }

            await rig.ReconcileAsync();
            Assert.False(rig.Applied);
            lines.AddRange(Lines(rig));
            Assert.Equal(LongQueryTraceDatabases.DropAttemptCap - 1, Count(lines, "WARN", DropLine));
            var giveUp = Assert.Single(lines, line => line.Contains("Stopped retrying", StringComparison.Ordinal));
            Assert.Contains("may remain on the server", giveUp, StringComparison.Ordinal);
            Assert.DoesNotContain("databases could not be listed", giveUp, StringComparison.Ordinal);
            Assert.Equal(0, Count(lines, "WARN", ReconcileLine));

            /* Done: the cycles after it run nothing, until the hour is up. */
            rig.Calls.Clear();
            await rig.ReconcileAsync();
            rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            Assert.Empty(rig.Calls);

            /* An hour after the cap: one attempt, at Debug, and the cycle after it runs nothing. */
            rig.Clock += TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Single(rig.Calls);
            lines.Clear();
            lines.AddRange(Lines(rig));
            Assert.Equal(0, Count(lines, "WARN", DropLine));
            Assert.Equal(1, Count(lines, "DEBUG", "The next attempt is in an hour."));
            Assert.False(rig.Applied);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// A drop that fails, then succeeds, is done: the count and the hourly attempt end, and nothing more is tried.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_ADropThatSucceedsAfterFailures_IsDone()
    {
        var rig = await BuildOnPremRigAsync(traceOn: false);
        rig.Refuse.Add(TheServer);

        await rig.ReconcileAsync();
        await rig.ReconcileAsync();
        Assert.Null(rig.Applied);

        rig.Refuse.Clear();
        await rig.ReconcileAsync();
        Assert.False(rig.Applied);

        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();
        Assert.Empty(rig.Calls);
    }

    /// <summary>
    /// Turning the trace on is a create that succeeds, and it ends the run of drop failures: after the cap gave up, on and
    /// off again, the next failures count from one, and warn again.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_AfterTheCap_TurnedOnAndOffAgain_CountsTheFailuresAgain()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildOnPremRigAsync(traceOn: false);
            rig.Refuse.Add(TheServer);
            for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            Assert.False(rig.Applied);

            rig.Refuse.Clear();
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: true);
            await rig.ReconcileAsync();
            Assert.True(rig.Applied);

            AppLogger.DrainBufferedLines();
            rig.Refuse.Add(TheServer);
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
            var lines = new List<string>();
            for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            lines.AddRange(Lines(rig));
            Assert.Equal(LongQueryTraceDatabases.DropAttemptCap - 1, Count(lines, "WARN", DropLine));

            /* The failed passes were not marked done: the last reconcile that finished is the one that turned it on. */
            Assert.True(rig.Applied);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── M2: the trace follows the monitored set ── */

    [Fact]
    public async Task On_AnExclusionChange_DropsTheSessionFromTheNewlyExcludedDatabase()
    {
        var rig = await BuildRigAsync(traceOn: true);

        await rig.ReconcileAsync();
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Created);

        rig.Server.ExcludedDatabases.Add("gamma");
        rig.Calls.Clear();
        await rig.ReconcileAsync();

        Assert.Equal(new[] { "gamma" }, rig.Dropped);
        Assert.DoesNotContain("gamma", rig.Created);
    }

    [Fact]
    public async Task Off_DropsTheSessionEverywhere_ExcludedDatabasesIncluded()
    {
        var rig = await BuildRigAsync(traceOn: false, "gamma");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
    }

    /* ── L3: a database monitored as its own server belongs to that registration ── */

    [Fact]
    public async Task Off_LeavesTheSessionOfADatabaseMonitoredAsItsOwnServer()
    {
        var rig = await BuildRigAsync(traceOn: false);
        RegisterSeparately(rig, "beta");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task On_DoesNotCreateTheSessionInADatabaseMonitoredAsItsOwnServer()
    {
        var rig = await BuildRigAsync(traceOn: true);
        RegisterSeparately(rig, "beta");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Created);
        Assert.DoesNotContain("beta", rig.Dropped);
    }

    /// <summary>
    /// The trace ON leaves a database monitored as its own server even when this registration excludes it, which
    /// would otherwise put it in the drop outside the set. Only the plan's own-server rule keeps it: the other
    /// registration's trace is off, so no other registration keeps the session there.
    /// </summary>
    [Fact]
    public async Task On_ADatabaseMonitoredAsItsOwnServer_AndExcluded_IsNotDropped()
    {
        var rig = await BuildRigAsync(traceOn: true, "beta");
        RegisterOther(rig, "beta", traceOn: false, readOnlyIntent: false);

        await rig.ReconcileAsync();

        Assert.DoesNotContain("beta", rig.Dropped);
    }

    /* ── M1: a drop leaves a database where another registration of the server keeps the session ── */

    [Fact]
    public async Task Off_LeavesTheDatabaseOfAReadOnlyRegistration_WhileItsTraceIsOn()
    {
        var rig = await BuildRigAsync(traceOn: false);
        RegisterOther(rig, "beta", traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task ReadOnlyRegistration_Off_LeavesItsDatabase_WhileTheLogicalServersTraceIsOnAndCoversIt()
    {
        var rig = await BuildDatabaseRigAsync("beta", traceOn: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: false);

        await rig.ReconcileAsync();

        Assert.Empty(rig.Dropped);
        Assert.False(rig.Applied);
    }

    [Fact]
    public async Task ReadOnlyRegistration_Off_DropsItsDatabase_WhenTheLogicalServerExcludesIt()
    {
        var rig = await BuildDatabaseRigAsync("beta", traceOn: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: false, "beta");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "beta" }, rig.Dropped);
    }

    /// <summary>
    /// A registration of one database without read-only intent is monitored as its own server, so the logical
    /// server's registration never creates the session there and does not keep it. Turning that database's own
    /// trace off still drops it.
    /// </summary>
    [Fact]
    public async Task DatabaseRegistration_Off_DropsItsDatabase_WhileTheLogicalServerLeavesItToIt()
    {
        var rig = await BuildDatabaseRigAsync("beta", traceOn: false, readOnlyIntent: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: false);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "beta" }, rig.Dropped);
    }

    [Fact]
    public async Task Off_LeavesTheDatabasesOfASecondLogicalServerRegistration_WhileItsTraceIsOn()
    {
        var rig = await BuildRigAsync(traceOn: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Empty(rig.Dropped);
    }

    [Fact]
    public async Task On_TheOutsideDrop_LeavesAnExcludedDatabase_ThatASecondRegistrationsTraceCovers()
    {
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "beta" }, rig.Created);
        Assert.Empty(rig.Dropped);
    }

    [Fact]
    public async Task On_TheOutsideDrop_StillDropsADatabase_ThatBothLogicalServerRegistrationsExclude()
    {
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: true, "gamma");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task Off_OnceTheOtherRegistrationTurnsItsTraceOff_TheNextPassDrops()
    {
        var rig = await BuildRigAsync(traceOn: false);
        var other = RegisterOther(rig, "beta", traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();
        Assert.DoesNotContain("beta", rig.Dropped);

        /* Nothing changed: the reconcile is done and does not run again. */
        rig.Calls.Clear();
        await rig.ReconcileAsync();
        Assert.Empty(rig.Calls);

        SetTrace(rig, other, traceOn: false);
        await rig.ReconcileAsync();

        Assert.Contains("beta", rig.Dropped);
    }

    /* ── the read follows the lifecycle; the always-on sessions do not change ── */

    /// <summary>
    /// The logical server's long-query read skips the databases monitored as their own servers, the same ones its
    /// lifecycle leaves alone. Reading one would fail every cycle when that database's own trace is off (a missing
    /// session is a read failure since #4731), or store its events twice when it is on. Pinned in the source: the
    /// read loop needs a live connection per database.
    /// </summary>
    [Fact]
    public void TheLongQueryRead_SkipsTheDatabasesMonitoredAsTheirOwnServers()
    {
        var definition = ReadLf(Path.Combine("PerformanceMonitor.Collectors", "LongQueryCompletionsCollector.cs"));
        Assert.Contains("public override bool SkipsSeparatelyMonitoredDatabases => true;", definition, StringComparison.Ordinal);

        var runner = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"));
        var list = runner.IndexOf("await GetAzureDatabaseListAsync(server, cancellationToken);", StringComparison.Ordinal);
        var skip = runner.IndexOf("if (definition.SkipsSeparatelyMonitoredDatabases)", StringComparison.Ordinal);
        Assert.True(list > 0 && skip > list, "the per-database read filters its list after listing it");
        Assert.Contains("databases = WithoutSeparatelyMonitoredDatabases(server, databases);", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// Guard, not RED first: the always-on deadlock and blocked-process sessions keep their inventory lifecycle. Their
    /// ensures still list the databases themselves, with the server's exclusions and nothing else, so an exclusion or
    /// ownership change that moves the long-query trace does not move them.
    /// </summary>
    [Fact]
    public void TheAlwaysOnSessions_StillFollowTheInventory()
    {
        var shared = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.BlockedProcessReport.cs"));
        Assert.Contains("databases = await GetAzureDatabaseListAsync(server, cancellationToken);", shared, StringComparison.Ordinal);

        foreach (var (file, capture) in new[] { ("RemoteCollectorService.BlockedProcessReport.cs", "\"blocked process\""), ("RemoteCollectorService.Deadlocks.cs", "\"deadlock\"") })
        {
            var source = ReadLf(Path.Combine("Lite", "Services", file));
            var call = source.IndexOf("await EnsureDatabaseScopedXeSessionsAsync(", StringComparison.Ordinal);
            Assert.True(call > 0, $"{file} lost its database-scoped ensure");
            var args = source[call..source.IndexOf(';', call)];
            Assert.Contains(capture, args, StringComparison.Ordinal);
            Assert.EndsWith("cancellationToken)", args, StringComparison.Ordinal);
            Assert.DoesNotContain("SeparatelyMonitored", args, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The edition is read once for a reconcile. A connection check that writes a blank edition and then 5 again, between the
    /// reconcile's first read and its drop, must not send the drop down the Azure branch with no registrations: that drop
    /// listed nothing as kept and dropped the session from a database another registration still uses.
    /// </summary>
    [Fact]
    public async Task Off_AnEditionThatChangesDuringTheReconcile_StillLeavesTheDatabasesAnotherRegistrationKeeps()
    {
        var rig = await BuildRigAsync(traceOn: false);
        var other = RegisterOther(rig, database: null, traceOn: false, readOnlyIntent: true);

        /* An earlier cycle read the edition. With both traces off nothing is kept, so the drop takes every database. */
        await rig.ReconcileAsync();
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);

        /* The other registration turns its trace on, so the plan runs again. A failed check wrote a blank edition, and the
           next check wrote 5 between this reconcile's first read of it and its drop (the clock is read in between). */
        SetTrace(rig, other, traceOn: true);
        var status = rig.Servers.GetConnectionStatus(rig.Server.Id);
        status.SqlEngineEdition = 0;
        rig.Service.LongQueryTraceUtcNowForTests = () =>
        {
            status.SqlEngineEdition = 5;
            return rig.Clock;
        };
        rig.Calls.Clear();

        await rig.ReconcileAsync();

        Assert.Empty(rig.Dropped);
    }

    /// <summary>
    /// The enumeration behind the Azure database list has the server's exclusion clause and parameters only when asked for.
    /// The trace's drops ask for none, so a session created before a database was excluded is still dropped there. The
    /// lifecycle rigs replace the listing, so this reads the query the real listing runs.
    /// </summary>
    [Fact]
    public void TheAzureDatabaseList_AppliesTheServersExclusionsOnlyWhenAsked()
    {
        var server = new ServerConnection { ServerName = Host, ExcludedDatabases = new List<string> { "scratch", "tempdb_clone" } };

        var (everySql, everyParameters) = RemoteCollectorService.BuildAzureDatabaseListQuery(server, applyExclusions: false);
        Assert.DoesNotContain("NOT IN", everySql, StringComparison.Ordinal);
        Assert.Empty(everyParameters);

        var (monitoredSql, monitoredParameters) = RemoteCollectorService.BuildAzureDatabaseListQuery(server, applyExclusions: true);
        Assert.Contains("name NOT IN (@excl_db_0, @excl_db_1)", monitoredSql, StringComparison.Ordinal);
        Assert.Equal(2, monitoredParameters.Count);
    }

    private static string ReadLf(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath)).Replace("\r\n", "\n");
    }
}
