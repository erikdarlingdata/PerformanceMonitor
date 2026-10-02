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
        public List<string> Listed { get; set; } = new() { "master", "alpha", "beta", "gamma" };
        public List<string> Scope { get; set; } = new();
        public List<string> Owned { get; set; } = new();
        public List<(string Database, bool Create)> Calls { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Exception? ListFailure { get; set; }
        public int ListCalls { get; set; }

        /* The other registrations of the logical server, and the databases monitored as their own servers as the
           logical server's registration sees them. */
        public List<LongQueryTraceRegistration> Others { get; } = new();
        public List<string> ServerOwned { get; set; } = new();

        /* The time each reconcile runs at, and the databases that hold the session. */
        public DateTime Clock { get; set; } = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);
        public HashSet<string> Sessions { get; } = new(StringComparer.OrdinalIgnoreCase);

        public Task ReconcileAsync(bool enabled) =>
            DarlingWorker.ReconcileLongQueryTraceAsync(State, Runner, enabled, Others, ServerOwned, Clock, Logger, CancellationToken.None);

        public IEnumerable<string> Dropped => Calls.Where(c => !c.Create).Select(c => c.Database);

        public IEnumerable<string> Created => Calls.Where(c => c.Create).Select(c => c.Database);
    }

    /// <summary>A logical-server registration (no database named) on an Azure SQL Database server.</summary>
    private Rig BuildRig(params string[] excluded) =>
        BuildRig(new MonitoredServer { Name = "lqtrace", Host = Host, ExcludedDatabases = excluded.ToList() }, ServerId);

    /// <summary>A registration of one database of the same logical server, with read-only intent unless told otherwise.</summary>
    private Rig BuildDatabaseRig(string database, bool readOnlyIntent = true)
    {
        var rig = BuildRig(
            new MonitoredServer { Name = "lqtrace-" + database, Host = Host, Database = database, ReadOnlyIntent = readOnlyIntent },
            DatabaseServerId);
        rig.Listed = new List<string> { database };
        return rig;
    }

    private const int ServerId = 4944;
    private const int DatabaseServerId = 8001;

    private static LongQueryTraceRegistration LogicalServer(int id, bool traceOn, params string[] excluded) =>
        new(id.ToString(System.Globalization.CultureInfo.InvariantCulture), Host, null, Enabled: true, traceOn, excluded, DatabaseScope: null);

    private static LongQueryTraceRegistration OneDatabase(int id, string database, bool traceOn) =>
        new(id.ToString(System.Globalization.CultureInfo.InvariantCulture), Host, database, Enabled: true, traceOn, Array.Empty<string>(), DatabaseScope: null);

    private Rig BuildRig(MonitoredServer config, int serverId)
    {
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{Host},1433;Initial Catalog={config.Database ?? "master"};Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
            StorageName = Host,
            ServerId = serverId,
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
            if (rig.Refuse.Contains(database))
            {
                return Task.FromException(new InvalidOperationException($"The drop was refused in {database}."));
            }

            if (create)
            {
                rig.Sessions.Add(database);
            }
            else
            {
                rig.Sessions.Remove(database);
            }

            return Task.CompletedTask;
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

    /* ── After the cap: one attempt an hour ── */

    [Fact]
    public async Task Off_AfterTheCap_TriesAgainOnceAnHour_AtDebug_WithNoSecondWarning()
    {
        var rig = BuildRig();
        rig.Refuse.Add("beta");

        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: false);
        }

        Assert.False(rig.State.LongQueryTraceApplied);
        var giveUp = Assert.Single(rig.Logger.Entries, e => e.Message.Contains("Stopped retrying", StringComparison.Ordinal));
        Assert.EndsWith(
            "It tries again once an hour, and right away after a restart, a change to the trace's settings, or a change to"
            + " another registration of these databases. It also tries again after it reconnects.",
            giveUp.Message,
            StringComparison.Ordinal);
        var warnings = Warnings(rig);

        /* Within the hour: nothing. */
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
        await rig.ReconcileAsync(enabled: false);
        Assert.Empty(rig.Calls);

        /* An hour after the cap: one attempt, and the sweep after it runs nothing. */
        rig.Clock += TimeSpan.FromMinutes(1);
        await rig.ReconcileAsync(enabled: false);
        await rig.ReconcileAsync(enabled: false);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);

        /* It fails again an hour later. Both failures log at Debug, and the cap's warning stays the only one. */
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: false);
        Assert.Equal(new[] { "alpha", "beta", "gamma", "alpha", "beta", "gamma" }, rig.Dropped);

        Assert.Equal(warnings, Warnings(rig));
        Assert.Equal(2, rig.Logger.Entries.Count(e =>
            e.Level == Microsoft.Extensions.Logging.LogLevel.Debug
            && e.Message.Contains("The next attempt is in an hour.", StringComparison.Ordinal)));
        Assert.Equal(2, rig.Logger.Entries.Count(e =>
            e.Level == Microsoft.Extensions.Logging.LogLevel.Debug
            && e.Message.Contains("[beta]", StringComparison.Ordinal)));
        Assert.False(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task AfterTheCap_AnAttemptThatSucceeds_StartsTheCountAgain_AndEndsTheHourlyAttempts()
    {
        var rig = BuildRig();
        rig.Refuse.Add("beta");

        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: false);
        }

        rig.Refuse.Clear();
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
        Assert.Null(rig.State.LongQueryTraceDropRetry.NextAttemptUtc);

        /* Done, with nothing left to retry: an hour later nothing runs. */
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: false);
        Assert.Empty(rig.Calls);
    }

    /* ── While the trace is on: the create side again once an hour ── */

    [Fact]
    public async Task On_ASessionDroppedFromOutside_IsCreatedAgainAnHourLater_AndNothingIsDropped()
    {
        /* gamma is excluded, so a pass that ran the drop side would drop it again. */
        var rig = BuildRig("gamma");
        await rig.ReconcileAsync(enabled: true);

        Assert.True(rig.State.LongQueryTraceApplied);
        Assert.Equal(new[] { "gamma" }, rig.Dropped);
        Assert.Contains("beta", rig.Sessions);

        /* The session is dropped outside the app. The read returns no rows for it, the same as for a quiet trace. */
        rig.Sessions.Remove("beta");
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);

        Assert.Contains("beta", rig.Sessions);
        Assert.Equal(new[] { "alpha", "beta" }, rig.Created);
        Assert.Empty(rig.Dropped);
        Assert.True(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task On_WithinTheHour_TheLatchedReconcileRunsNothing()
    {
        var rig = BuildRig();
        await rig.ReconcileAsync(enabled: true);

        rig.Calls.Clear();
        var listCalls = rig.ListCalls;
        rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromSeconds(1);
        await rig.ReconcileAsync(enabled: true);

        Assert.Empty(rig.Calls);
        Assert.Equal(listCalls, rig.ListCalls);
    }

    [Fact]
    public async Task On_AnHourlyCreatePassThatThrows_RunsAgainAnHourLater_NotOnEverySweep()
    {
        var rig = BuildRig();
        await rig.ReconcileAsync(enabled: true);

        /* The hourly create pass is due, and the listing fails, so the pass throws. */
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        rig.ListFailure = new InvalidOperationException("master is not reachable.");
        rig.ListCalls = 0;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(1, rig.ListCalls);
        Assert.NotNull(rig.State.LongQueryTraceFault);
        var failedAt = rig.Clock;
        var warnings = Warnings(rig);

        /* The sweeps within the hour run nothing, and the fault stays set until a pass succeeds. */
        foreach (var minutes in new[] { 1, 10, 59 })
        {
            rig.Clock = failedAt + TimeSpan.FromMinutes(minutes);
            await rig.ReconcileAsync(enabled: true);
        }

        Assert.Equal(1, rig.ListCalls);
        Assert.Equal(warnings, Warnings(rig));
        Assert.NotNull(rig.State.LongQueryTraceFault);

        /* An hour after it threw, the pass runs again, and a pass that succeeds clears the fault. */
        rig.ListFailure = null;
        rig.Clock = failedAt + LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(2, rig.ListCalls);
        Assert.Null(rig.State.LongQueryTraceFault);
    }

    [Fact]
    public async Task On_TheAttemptAfterTheCap_ThatThrowsAnotherError_RunsAgainAnHourLater_NotOnEverySweep()
    {
        /* The drop outside the monitored set is refused in the excluded database until the cap gives up. */
        var rig = BuildRig("gamma");
        rig.Refuse.Add("gamma");
        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: true);
        }

        /* The attempt after the cap is due, and the listing fails: it throws something other than a failed drop. */
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        rig.ListFailure = new InvalidOperationException("master is not reachable.");
        rig.ListCalls = 0;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(1, rig.ListCalls);
        var failedAt = rig.Clock;

        foreach (var minutes in new[] { 1, 10, 59 })
        {
            rig.Clock = failedAt + TimeSpan.FromMinutes(minutes);
            await rig.ReconcileAsync(enabled: true);
        }

        Assert.Equal(1, rig.ListCalls);

        rig.Clock = failedAt + LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(2, rig.ListCalls);
    }

    private static int Warnings(Rig rig) =>
        rig.Logger.Entries.Count(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Warning);

    /* ── #4964: a create that fails again and again logs its first failure at Warning, the repeats at Debug ── */

    private const string EnumerateLine = "Failed to enumerate databases for the long-query completion XE session";

    private static int Logged(Rig rig, Microsoft.Extensions.Logging.LogLevel level, string text) =>
        rig.Logger.Entries.Count(e => e.Level == level && e.Message.Contains(text, StringComparison.Ordinal));

    /* The worker's own line has no database in it; the per-database line does. */
    private static string WorkerLine(Rig rig) => $"[{rig.Config.DisplayName}] Failed to reconcile the long-query completion XE session: ";

    [Fact]
    public async Task On_ACreateThatFailsOnEverySweep_LogsOneWarning_ThenDebug_AndRecordsTheFaultEachTime()
    {
        var rig = BuildRig();
        var warning = Microsoft.Extensions.Logging.LogLevel.Warning;
        var debug = Microsoft.Extensions.Logging.LogLevel.Debug;

        rig.ListFailure = new InvalidOperationException("master is not readable, first.");
        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(1, Logged(rig, warning, WorkerLine(rig)));
        Assert.Equal(1, Logged(rig, warning, EnumerateLine));
        Assert.Contains("first.", rig.State.LongQueryTraceFault, StringComparison.Ordinal);

        /* The same failure on the next two sweeps: no new Warning, one Debug line each, and the fault is recorded
           again with the newest message, so collection health keeps reading SESSION_MISSING. */
        foreach (var attempt in new[] { "second.", "third." })
        {
            rig.ListFailure = new InvalidOperationException("master is not readable, " + attempt);
            await rig.ReconcileAsync(enabled: true);
            Assert.Contains(attempt, rig.State.LongQueryTraceFault, StringComparison.Ordinal);
        }

        Assert.Equal(1, Logged(rig, warning, WorkerLine(rig)));
        Assert.Equal(1, Logged(rig, warning, EnumerateLine));
        Assert.Equal(2, Logged(rig, debug, WorkerLine(rig)));
        Assert.Equal(2, Logged(rig, debug, EnumerateLine));

        /* The retry did not change: every sweep listed again, and the latch is still unset. */
        Assert.Equal(3, rig.ListCalls);
        Assert.Null(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task On_ACreateThatSucceedsAfterFailures_WarnsAgainWhenItFailsAfterwards()
    {
        var rig = BuildRig();
        var warning = Microsoft.Extensions.Logging.LogLevel.Warning;

        rig.ListFailure = new InvalidOperationException("master is not readable.");
        await rig.ReconcileAsync(enabled: true);
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(1, Logged(rig, warning, WorkerLine(rig)));

        /* A create that succeeds ends the run of failures. */
        rig.ListFailure = null;
        await rig.ReconcileAsync(enabled: true);
        Assert.True(rig.State.LongQueryTraceApplied);
        Assert.Null(rig.State.LongQueryTraceFault);

        /* The hourly create pass fails: a new failure, so it warns again, and its repeat does not. */
        rig.ListFailure = new InvalidOperationException("master is not readable again.");
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(2, Logged(rig, warning, WorkerLine(rig)));

        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(2, Logged(rig, warning, WorkerLine(rig)));

        /* One repeat in each run of failures. */
        Assert.Equal(2, Logged(rig, Microsoft.Extensions.Logging.LogLevel.Debug, WorkerLine(rig)));
    }

    [Fact]
    public async Task On_ACreateRefusedInEveryDatabase_LogsEachRefusalAtWarningOnce_ThenAtDebug()
    {
        var rig = BuildRig();
        var warning = Microsoft.Extensions.Logging.LogLevel.Warning;
        var debug = Microsoft.Extensions.Logging.LogLevel.Debug;
        foreach (var database in new[] { "alpha", "beta", "gamma" })
        {
            rig.Refuse.Add(database);
        }

        await rig.ReconcileAsync(enabled: true);

        /* The first pass names each refusing database, says that every one refused, and the worker says it once. */
        Assert.Equal(1, Logged(rig, warning, "[beta] Failed to reconcile the long-query completion XE session"));
        Assert.Equal(1, Logged(rig, warning, "could not be ensured in all 3 database(s)"));
        Assert.Equal(1, Logged(rig, warning, WorkerLine(rig)));
        var firstPass = Warnings(rig);
        Assert.Equal(5, firstPass);
        Assert.NotNull(rig.State.LongQueryTraceFault);

        await rig.ReconcileAsync(enabled: true);
        await rig.ReconcileAsync(enabled: true);

        /* The repeats log the same lines at Debug, and every sweep still tried every database. */
        Assert.Equal(firstPass, Warnings(rig));
        Assert.Equal(2, Logged(rig, debug, "[beta] Failed to reconcile the long-query completion XE session"));
        Assert.Equal(2, Logged(rig, debug, "could not be ensured in all 3 database(s)"));
        Assert.Equal(2, Logged(rig, debug, WorkerLine(rig)));
        Assert.Equal(9, rig.Created.Count());
        Assert.NotNull(rig.State.LongQueryTraceFault);
    }

    [Fact]
    public async Task Off_ADropFailureAfterCreateFailures_IsNotCountedAsARepeat()
    {
        var rig = BuildRig();
        var warning = Microsoft.Extensions.Logging.LogLevel.Warning;

        rig.ListFailure = new InvalidOperationException("master is not readable.");
        await rig.ReconcileAsync(enabled: true);
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(1, Logged(rig, warning, WorkerLine(rig)));

        /* Turned off, the drop is a different pass with its own failure: its first line is a Warning. The cap on the
           drop side is its own (#4944). */
        await rig.ReconcileAsync(enabled: false);
        Assert.Equal(1, Logged(rig, warning, "Could not list the databases to drop the long-query trace session from"));
    }

    [Fact]
    public async Task AReconnect_StartsTheRunOfCreateFailuresAgain_SoTheNextOneWarns()
    {
        var rig = BuildRig();
        var warning = Microsoft.Extensions.Logging.LogLevel.Warning;

        rig.ListFailure = new InvalidOperationException("master is not readable.");
        await rig.ReconcileAsync(enabled: true);
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(1, Logged(rig, warning, WorkerLine(rig)));
        Assert.True(rig.State.LongQueryTraceCreateWarned);

        /* What the connect block does to the long-query state (pinned below). */
        rig.State.LongQueryTraceApplied = null;
        rig.State.LongQueryTraceFault = null;
        rig.State.LongQueryTracePartialNote = null;
        rig.State.LongQueryTraceAppliedKey = null;
        rig.State.LongQueryTraceAppliedAtUtc = null;
        rig.State.LongQueryTraceDropRetry.Reset();
        rig.State.LongQueryTraceCreateWarned = false;

        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(2, Logged(rig, warning, WorkerLine(rig)));
    }

    [Fact]
    public void TheConnectBlock_ClearsTheCreateFailureWarned_WithTheRestOfTheLongQueryState()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        var latchReset = source.IndexOf("server.LongQueryTraceApplied = null;", StringComparison.Ordinal);
        Assert.True(latchReset > 0);
        var slice = source.Substring(latchReset, 1600);
        Assert.Contains("server.LongQueryTraceDropRetry.Reset();", slice, StringComparison.Ordinal);
        Assert.Contains("server.LongQueryTraceCreateWarned = false;", slice, StringComparison.Ordinal);
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

    /// <summary>
    /// The trace ON leaves a database monitored as its own server even when this registration's scope leaves it
    /// out, which would otherwise put it in the drop outside the set. Only the plan's own-server rule keeps it: the
    /// other registration's trace is off, so no other registration keeps the session there.
    /// </summary>
    [Fact]
    public async Task On_ADatabaseMonitoredAsItsOwnServer_AndScopedOut_IsNotDropped()
    {
        var rig = BuildRig();
        rig.Owned = new List<string> { "beta" };
        rig.ServerOwned = new List<string> { "beta" };
        rig.Scope = new List<string> { "alpha", "gamma" };
        rig.Others.Add(OneDatabase(DatabaseServerId, "beta", traceOn: false));

        await rig.ReconcileAsync(enabled: true);

        Assert.DoesNotContain("beta", rig.Dropped);
    }

    /* ── M1: a drop leaves a database where another registration of the server keeps the session ── */

    [Fact]
    public async Task Off_LeavesTheDatabaseOfAReadOnlyRegistration_WhileItsTraceIsOn()
    {
        var rig = BuildRig();
        rig.Others.Add(OneDatabase(DatabaseServerId, "beta", traceOn: true));

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task ReadOnlyRegistration_Off_LeavesItsDatabase_WhileTheLogicalServersTraceIsOnAndCoversIt()
    {
        var rig = BuildDatabaseRig("beta");
        rig.Others.Add(LogicalServer(ServerId, traceOn: true));

        await rig.ReconcileAsync(enabled: false);

        Assert.Empty(rig.Dropped);
        Assert.False(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task ReadOnlyRegistration_Off_DropsItsDatabase_WhenTheLogicalServerExcludesIt()
    {
        var rig = BuildDatabaseRig("beta");
        rig.Others.Add(LogicalServer(ServerId, traceOn: true, "beta"));

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "beta" }, rig.Dropped);
    }

    /// <summary>
    /// A registration of one database without read-only intent is monitored as its own server, so the logical
    /// server's registration never creates the session there and does not keep it. Turning that database's own
    /// trace off still drops it. The list comes from the worker's own adapter, which counts the registration itself.
    /// </summary>
    [Fact]
    public async Task DatabaseRegistration_Off_DropsItsDatabase_WhileTheLogicalServerLeavesItToIt()
    {
        var rig = BuildDatabaseRig("beta", readOnlyIntent: false);
        rig.Others.Add(LogicalServer(ServerId, traceOn: true));
        rig.ServerOwned = DarlingWorker.LongQueryTraceServerSeparatelyMonitored(
            Host,
            new[]
            {
                new MonitoredServer { Name = "lqtrace", Host = Host },
                rig.Config,
            }).ToList();

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "beta" }, rig.ServerOwned);
        Assert.Equal(new[] { "beta" }, rig.Dropped);
    }

    [Fact]
    public async Task Off_LeavesTheDatabasesOfASecondLogicalServerRegistration_WhileItsTraceIsOn()
    {
        var rig = BuildRig();
        rig.Others.Add(LogicalServer(8002, traceOn: true));

        await rig.ReconcileAsync(enabled: false);

        Assert.Empty(rig.Dropped);
    }

    [Fact]
    public async Task On_TheOutsideDrop_LeavesAnExcludedDatabase_ThatASecondRegistrationsTraceCovers()
    {
        var rig = BuildRig("gamma");
        rig.Others.Add(LogicalServer(8002, traceOn: true));

        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(new[] { "alpha", "beta" }, rig.Created);
        Assert.Empty(rig.Dropped);
    }

    [Fact]
    public async Task On_TheOutsideDrop_StillDropsADatabase_ThatBothLogicalServerRegistrationsExclude()
    {
        var rig = BuildRig("gamma");
        rig.Others.Add(LogicalServer(8002, traceOn: true, "gamma"));

        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(new[] { "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task Off_OnceTheOtherRegistrationTurnsItsTraceOff_TheNextPassDrops()
    {
        var rig = BuildRig();
        rig.Others.Add(OneDatabase(DatabaseServerId, "beta", traceOn: true));

        await rig.ReconcileAsync(enabled: false);
        Assert.DoesNotContain("beta", rig.Dropped);

        /* Nothing changed: the reconcile is done and does not run again. */
        rig.Calls.Clear();
        await rig.ReconcileAsync(enabled: false);
        Assert.Empty(rig.Calls);

        rig.Others[0] = rig.Others[0] with { TraceOn = false };
        await rig.ReconcileAsync(enabled: false);

        Assert.Contains("beta", rig.Dropped);
    }

    /// <summary>
    /// The worker's adapter: each live registration of the same logical server, with its trace setting and its
    /// trace scope, where an empty scope means every database.
    /// </summary>
    [Fact]
    public void TheRegistrations_AreTheLiveOnesOfTheSameLogicalServer_WithTheirTraceSettingAndScope()
    {
        var master = new MonitoredServer { Name = "lqtrace", Host = Host, ExcludedDatabases = new List<string> { "gamma" } };
        var replica = new MonitoredServer { Name = "lqtrace-ro", Host = " LQTRACE.database.windows.net ", Database = "beta", ReadOnlyIntent = true };
        var elsewhere = new MonitoredServer { Name = "other", Host = "other.database.windows.net", Database = "beta" };

        var registrations = DarlingWorker.LongQueryTraceRegistrations(
            Host,
            new[] { master, replica, elsewhere },
            traceOn: serverId => serverId == replica.ServerId,
            databaseScope: serverId => serverId == master.ServerId ? new[] { "alpha" } : Array.Empty<string>());

        Assert.Equal(2, registrations.Count);

        var first = registrations[0];
        Assert.Equal(master.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture), first.Id);
        Assert.False(first.TraceOn);
        Assert.Equal(new[] { "gamma" }, first.ExcludedDatabases);
        Assert.Equal(new[] { "alpha" }, first.DatabaseScope);

        var second = registrations[1];
        Assert.Equal("beta", second.Database);
        Assert.True(second.TraceOn);
        Assert.True(second.Enabled);
        Assert.Null(second.DatabaseScope);
    }

    /// <summary>
    /// The state key reads one way: a scope of alpha and beta with gamma excluded is not the same settings as a scope
    /// of alpha with beta and gamma excluded, and the second leaves beta out of the monitored set.
    /// </summary>
    [Fact]
    public void TheStateKey_TellsWhereTheScopeEndsAndTheExclusionsBegin()
    {
        var none = Array.Empty<string>();

        Assert.NotEqual(
            LongQueryTraceDatabases.StateKey(true, new[] { "alpha", "beta" }, new[] { "gamma" }, none, none),
            LongQueryTraceDatabases.StateKey(true, new[] { "alpha" }, new[] { "beta", "gamma" }, none, none));
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
    /// The sweep hands the reconcile the other registrations and the server's own list of separately monitored
    /// databases from the live registry, through the two adapters the tests above drive. The server's list is the
    /// logical server's view for every registration, so it is not the runner's per-registration list, which is empty
    /// for a registration that names a database. Pinned in the source: the sweep needs a live registry and store.
    /// </summary>
    [Fact]
    public void TheSweep_PassesTheRegistrationsAndTheServersOwnList_FromTheLiveRegistry()
    {
        var worker = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var sweep = worker.IndexOf("private async Task ReconcileLongQueryTraceAsync(ServerLoopState server, DarlingCollectorRunner runner, CancellationToken cancellationToken)", StringComparison.Ordinal);
        var end = worker.IndexOf("await ReconcileLongQueryTraceAsync(server, runner, enabled, registrations, serverSeparatelyMonitored, DateTime.UtcNow, _logger,", sweep, StringComparison.Ordinal);
        Assert.True(sweep > 0 && end > sweep, "the sweep's reconcile passes the registrations and the server's list");

        var body = worker[sweep..end];
        Assert.Contains("var live = _registryState.Read()?.Servers;", body, StringComparison.Ordinal);
        Assert.Contains("registrations = LongQueryTraceRegistrations(", body, StringComparison.Ordinal);
        Assert.Contains("serverSeparatelyMonitored = LongQueryTraceServerSeparatelyMonitored(server.Runtime.Config.Host, live);", body, StringComparison.Ordinal);
        Assert.Contains("otherId => runner.DatabaseScopeFor(LongQueryCompletionsCollector.Instance.Name, otherId)", body, StringComparison.Ordinal);
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
