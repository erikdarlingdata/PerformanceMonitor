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
        public required ServerConnection Server { get; init; }

        /* What the logical server's master lists, before the registration's exclusions. */
        public List<string> Listed { get; } = new() { "master", "alpha", "beta", "gamma" };
        public List<(string Database, bool Create)> Calls { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Exception? ListFailure { get; set; }
        public int ListCalls { get; set; }

        public Task ReconcileAsync() => Service.ReconcileLongQueryCompletionsXeSessionAsync(Server, CancellationToken.None);

        public bool? Applied => Service.LongQueryTraceAppliedState(Server.Id);

        public IEnumerable<string> Dropped => Calls.Where(c => !c.Create).Select(c => c.Database);

        public IEnumerable<string> Created => Calls.Where(c => c.Create).Select(c => c.Database);
    }

    /// <summary>A logical-server registration (no database named) on an Azure SQL Database server.</summary>
    private async Task<Rig> BuildRigAsync(bool traceOn, params string[] excluded)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var servers = new ServerManager(_configDir);
        var server = new ServerConnection
        {
            ServerName = Host,
            DisplayName = "lqtrace-" + Guid.NewGuid().ToString("N")[..8],
            ExcludedDatabases = excluded.ToList(),
        };
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("long_query_completions", enabled: traceOn);

        var rig = new Rig
        {
            Service = new RemoteCollectorService(duckDb, servers, schedules),
            Servers = servers,
            Server = server,
        };

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
