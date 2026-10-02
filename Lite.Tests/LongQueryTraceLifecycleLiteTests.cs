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

    private async Task<Rig> BuildRigAsync(ServerConnection server, bool traceOn)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

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
                ? Task.FromException(new InvalidOperationException($"The drop was refused in {database}."))
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
