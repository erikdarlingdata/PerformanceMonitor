/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The long-query trace's session lifecycle on Azure SQL Database, where each database holds its own session: which
/// databases a reconcile creates the session in and drops it from, when the worker counts it done, and how a failed
/// drop is retried. Each test drives the worker's real reconcile with the database list and the per-database work
/// replaced on the runner, so no server or store is needed.
/// </summary>
public sealed class LongQueryTraceLifecycleTests : IAsyncDisposable
{
    private const string Host = "lqtrace.database.windows.net";

    /* Never opened: the runner reaches the store only to write collected rows, and these tests collect nothing. */
    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private sealed class Rig
    {
        public required DarlingCollectorRunner Runner { get; init; }
        public required DarlingWorker.ServerLoopState State { get; init; }
        public required MonitoredServer Config { get; init; }
        public DarlingSelfAlertTests.CapturingLogger Logger { get; } = new();

        /* What the logical server's master lists, before the registration's exclusions and scope. */
        public List<string> Listed { get; } = new() { "master", "alpha", "beta", "gamma" };
        public List<string> Scope { get; set; } = new();
        public List<string> Owned { get; set; } = new();
        public List<(string Database, bool Create)> Calls { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Exception? ListFailure { get; set; }
        public int ListCalls { get; set; }

        public Task ReconcileAsync(bool enabled) =>
            DarlingWorker.ReconcileLongQueryTraceAsync(State, Runner, enabled, Logger, CancellationToken.None);

        public IEnumerable<string> Dropped => Calls.Where(c => !c.Create).Select(c => c.Database);

        public IEnumerable<string> Created => Calls.Where(c => c.Create).Select(c => c.Database);
    }

    /// <summary>A logical-server registration (no database named) on an Azure SQL Database server.</summary>
    private Rig BuildRig(params string[] excluded)
    {
        var config = new MonitoredServer { Name = "lqtrace", Host = Host, ExcludedDatabases = excluded.ToList() };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{Host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
            StorageName = Host,
            ServerId = 4944,
        };

        Rig? rig = null;
        var runner = new DarlingCollectorRunner(
            _store,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => rig!.Scope.ToList(),
            separatelyMonitoredDatabases: _ => rig!.Owned.ToList());

        rig = new Rig
        {
            Runner = runner,
            State = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime },
            Config = config,
        };

        runner.LongQueryTraceListOverrideForTests = (_, allDatabases, scope, _) =>
        {
            rig.ListCalls++;
            if (rig.ListFailure is { } failure)
            {
                throw failure;
            }

            return Task.FromResult(allDatabases
                ? rig.Listed.ToList()
                : rig.Listed
                    .Where(d => !config.ExcludedDatabases.Contains(d, StringComparer.OrdinalIgnoreCase))
                    .Where(d => scope is null || scope.Count == 0 || scope.Contains(d, StringComparer.OrdinalIgnoreCase))
                    .ToList());
        };

        runner.LongQueryTraceDatabaseOverrideForTests = (_, database, create, _) =>
        {
            rig.Calls.Add((database, create));
            return rig.Refuse.Contains(database)
                ? Task.FromException(new InvalidOperationException($"The drop was refused in {database}."))
                : Task.CompletedTask;
        };

        return rig;
    }

    /* ── M1: a failed listing or drop is retried, with a cap ── */

    [Fact]
    public async Task Off_AFailedListing_IsNotMarkedApplied_AndTheNextSweepListsAgain()
    {
        var rig = BuildRig();
        rig.ListFailure = new InvalidOperationException("master is not reachable.");

        await rig.ReconcileAsync(enabled: false);

        Assert.Null(rig.State.LongQueryTraceApplied);

        rig.ListFailure = null;
        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(2, rig.ListCalls);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
        Assert.False(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task On_AFailedListing_IsNotMarkedApplied()
    {
        var rig = BuildRig();
        rig.ListFailure = new InvalidOperationException("master is not reachable.");

        await rig.ReconcileAsync(enabled: true);

        Assert.Null(rig.State.LongQueryTraceApplied);
        Assert.Empty(rig.Calls);
    }

    [Fact]
    public async Task Off_OneDatabaseRefusesTheDrop_TheOthersAreStillDropped_AndTheNextSweepRetries()
    {
        var rig = BuildRig();
        rig.Refuse.Add("beta");

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
        Assert.Null(rig.State.LongQueryTraceApplied);

        rig.Calls.Clear();
        await rig.ReconcileAsync(enabled: false);

        Assert.Contains("beta", rig.Dropped);
    }

    [Fact]
    public async Task Off_TheFifthFailedPassInARow_IsMarkedApplied_WithOneWarningThatNamesTheDatabase()
    {
        var rig = BuildRig();
        rig.Refuse.Add("beta");

        for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: false);
            Assert.Null(rig.State.LongQueryTraceApplied);
        }

        await rig.ReconcileAsync(enabled: false);
        Assert.False(rig.State.LongQueryTraceApplied);

        /* Done: the next sweep does not try again. */
        rig.Calls.Clear();
        await rig.ReconcileAsync(enabled: false);
        Assert.Empty(rig.Calls);

        var giveUp = Assert.Single(rig.Logger.Entries, e => e.Message.Contains("Stopped retrying", StringComparison.Ordinal));
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, giveUp.Level);
        Assert.Contains("beta", giveUp.Message, StringComparison.Ordinal);
    }

    /* ── M2: the trace follows the monitored set ── */

    [Fact]
    public async Task On_AScopeChange_DropsTheSessionFromTheDatabaseNowOutOfScope()
    {
        var rig = BuildRig();

        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Created);

        rig.Scope = new List<string> { "alpha", "beta" };
        rig.Calls.Clear();
        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(new[] { "gamma" }, rig.Dropped);
        Assert.DoesNotContain("gamma", rig.Created);
    }

    [Fact]
    public async Task Off_DropsTheSessionEverywhere_ExcludedAndOutOfScopeDatabasesIncluded()
    {
        var rig = BuildRig("gamma");
        rig.Scope = new List<string> { "alpha" };

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
    }

    /* ── L3: a database monitored as its own server belongs to that registration ── */

    [Fact]
    public async Task Off_LeavesTheSessionOfADatabaseMonitoredAsItsOwnServer()
    {
        var rig = BuildRig();
        rig.Owned = new List<string> { "beta" };

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task On_DoesNotCreateTheSessionInADatabaseMonitoredAsItsOwnServer()
    {
        var rig = BuildRig();
        rig.Owned = new List<string> { "beta" };

        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Created);
        Assert.DoesNotContain("beta", rig.Dropped);
    }

    /* ── the read follows the lifecycle; the always-on sessions do not change ── */

    /// <summary>
    /// The logical server's long-query read skips the databases monitored as their own servers, the same ones its
    /// lifecycle leaves alone, and the worker hands the runner that list from the same live store set the alert sweep
    /// uses. Pinned in the source: the read loop needs a live connection per database.
    /// </summary>
    [Fact]
    public void TheLongQueryRead_SkipsTheDatabasesMonitoredAsTheirOwnServers()
    {
        var runner = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        var list = runner.IndexOf(": await GetAzureDatabaseListAsync(server, databaseScope, cancellationToken);", StringComparison.Ordinal);
        var skip = runner.IndexOf("if (definition.SkipsSeparatelyMonitoredDatabases)", StringComparison.Ordinal);
        Assert.True(list > 0 && skip > list, "the per-database read filters its list after listing it");

        var worker = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("separatelyMonitoredDatabases: runtime => AzureMasterScope.SeparatelyMonitoredDatabases(", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// Guard, not RED first: the always-on deadlock and blocked-process sessions keep their inventory lifecycle. Their
    /// ensure still lists every inventoried database with no database scope, so a scope or ownership change that moves
    /// the long-query trace does not move them.
    /// </summary>
    [Fact]
    public void TheAlwaysOnSessions_StillFollowTheInventory()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");
        var ensure = source.IndexOf("private static async Task EnsureDatabaseScopedAsync(", StringComparison.Ordinal);
        Assert.True(ensure > 0, "the always-on database-scoped ensure is gone");
        var body = source[ensure..source.IndexOf("\n    }\n", ensure, StringComparison.Ordinal)];

        Assert.Contains("databases = await runner.GetAzureDatabaseListAsync(server, databaseScope: null, cancellationToken);", body, StringComparison.Ordinal);
        Assert.DoesNotContain("SeparatelyMonitored", body, StringComparison.Ordinal);
    }
}
