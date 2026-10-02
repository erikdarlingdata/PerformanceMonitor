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
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

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

        /* Every session name the long-query work named: the name a create or a drop would have put in its statement (#4961). */
        public List<string> Names { get; } = new();
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

        /* #4961: the guard an on-premises server's reconcile resolves, and how many times it did. A rig that sets none passes none,
           as the sweep does for an Azure SQL Database target. */
        public Func<Task<LongQueryTraceInstanceGuard>>? InstanceGuard { get; set; }
        public int GuardResolutions { get; set; }

        public Task ReconcileAsync(bool enabled) =>
            DarlingWorker.ReconcileLongQueryTraceAsync(State, Runner, enabled, Others, ServerOwned, Clock, Logger, CancellationToken.None, InstanceGuard);

        public IEnumerable<string> Dropped => Calls.Where(c => !c.Create).Select(c => c.Database);

        public IEnumerable<string> Created => Calls.Where(c => c.Create).Select(c => c.Database);

        /* #4961: the legacy session's drops, kept apart from the per-install work above. Each entry is the database the
           drop ran in; the server's own session is the empty name. */
        public List<string> LegacyCalls { get; } = new();
        public List<string> LegacyNames { get; } = new();
        public HashSet<string> RefuseLegacy { get; } = new(StringComparer.OrdinalIgnoreCase);

        /* The databases that hold a legacy session right now. An older install creates it again by adding one here, and
           two installs that monitor the same server share one set. */
        public HashSet<string> LegacySessions { get; set; } = new(StringComparer.OrdinalIgnoreCase);

        /* Every call in the order it ran: "legacy:alpha", "create:alpha", "drop:gamma". */
        public List<string> Events { get; } = new();

        /* Where this install keeps the record of the legacy drop, in memory because the store is never opened. */
        public InMemoryLegacyRecords Records { get; init; } = new();

        /* What a reconnect does to the loop state (DarlingWorker's connect block), and to the runner. */
        public void Reconnect()
        {
            Runner.OnServerReconnected(State.Runtime!.ServerId);
            State.LongQueryTraceApplied = null;
            State.LongQueryTraceAppliedKey = null;
            State.LongQueryTraceAppliedAtUtc = null;
            State.LongQueryTraceFault = null;
            State.LongQueryTracePartialNote = null;
            State.LongQueryTraceDropRetry.Reset();
            State.LongQueryTraceCreateWarned = false;
        }
    }

    /// <summary>The record the legacy drop leaves, in memory. A read or a write fails on demand, as the store's would.</summary>
    internal sealed class InMemoryLegacyRecords : ILegacyLongQueryRecords
    {
        public Dictionary<(int ServerId, string StateKey), string> Rows { get; } = new();
        public Exception? ReadFailure { get; set; }
        public Exception? WriteFailure { get; set; }
        public int Reads { get; private set; }
        public int Writes { get; private set; }

        public Task<IReadOnlyCollection<string>> ReadAsync(int serverId, CancellationToken cancellationToken)
        {
            Reads++;
            if (ReadFailure is { } failure)
            {
                return Task.FromException<IReadOnlyCollection<string>>(failure);
            }

            return Task.FromResult<IReadOnlyCollection<string>>(Rows.Keys
                .Where(k => k.ServerId == serverId && k.StateKey.StartsWith(LegacyLongQuerySession.StateKeyPrefix, StringComparison.Ordinal))
                .Select(k => k.StateKey)
                .ToList());
        }

        public Task WriteAsync(int serverId, string stateKey, DateTime droppedUtc, CancellationToken cancellationToken)
        {
            if (WriteFailure is { } failure)
            {
                return Task.FromException(failure);
            }

            Writes++;
            Rows[(serverId, stateKey)] = droppedUtc.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
            return Task.CompletedTask;
        }

        public bool Has(int serverId, string database) => Rows.ContainsKey((serverId, LegacyLongQuerySession.StateKey(database)));
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

    /// <summary>A server on an engine with no per-database sessions, an on-premises server: its session is the server's.</summary>
    private Rig BuildOnPremRig() =>
        BuildRig(new MonitoredServer { Name = "lqtrace-sql", Host = "lqtrace-sql" }, ServerId, azureSqlDatabase: false);

    /* This install's id (#4961), and the session it makes from it. */
    private const string InstallIdValue = "0a1b2c3d";
    private static readonly string OwnSession = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue);

    private Rig BuildRig(MonitoredServer config, int serverId, bool azureSqlDatabase = true, string? installId = InstallIdValue, InMemoryLegacyRecords? records = null)
    {
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{Host},1433;Initial Catalog={config.Database ?? "master"};Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDatabase },
            StorageName = Host,
            ServerId = serverId,
        };

        Rig? rig = null;
        var runner = new DarlingCollectorRunner(
            _store,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => rig!.Scope.ToList(),
            separatelyMonitoredDatabases: _ => rig!.Owned.ToList(),
            installId: () => installId);

        rig = new Rig
        {
            Runner = runner,
            State = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime },
            Config = config,
            Records = records ?? new InMemoryLegacyRecords(),
        };

        runner.LegacyLongQueryRecordsForTests = rig.Records;
        runner.LegacyLongQueryPresentForTests = (_, database, _) => Task.FromResult(rig.LegacySessions.Contains(database));

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

        runner.LongQueryTraceDatabaseOverrideForTests = (_, database, create, sessionName, _) =>
        {
            if (sessionName == LongQueryCompletionsCollector.LegacyXeSessionName)
            {
                rig.LegacyCalls.Add(database);
                rig.LegacyNames.Add(sessionName);
                rig.Events.Add("legacy:" + database);
                if (rig.RefuseLegacy.Contains(database))
                {
                    return Task.FromException(new InvalidOperationException($"The legacy drop was refused in {database}."));
                }

                rig.LegacySessions.Remove(database);
                return Task.CompletedTask;
            }

            rig.Events.Add((create ? "create:" : "drop:") + database);
            rig.Calls.Add((database, create));
            rig.Names.Add(sessionName);
            if (rig.Refuse.Contains(database))
            {
                return Task.FromException(new InvalidOperationException($"The {(create ? "create" : "drop")} was refused in {database}."));
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

    /// <summary>A service restart: a new runner and a new loop state over the same record, on the same server.</summary>
    private Rig Restart(Rig before)
    {
        var after = BuildRig(
            before.Config, before.State.Runtime!.ServerId, before.State.Runtime.Target.IsAzureSqlDb, before.Runner.InstallId, before.Records);
        after.Listed = before.Listed;
        after.LegacySessions = before.LegacySessions;
        after.Clock = before.Clock;
        return after;
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

    /* ── #4964: the drop of a server-scoped session follows the same cap as the Azure arm's ── */

    /* The server-scoped session reaches the test replacement with no database name. */
    private const string TheServer = "";

    private const string DropLine = "Could not drop the long-query trace session:";

    /// <summary>
    /// On an engine with no per-database sessions, a disabled trace whose drop fails on every sweep follows the cadence of
    /// the Azure arm: the first four failures are Warnings that the next sweep tries again, the fifth is the one Warning that
    /// gives up, the sweeps after it run nothing, and an hour later one attempt logs at Debug. Before, the failure reached
    /// the worker's general catch, which warned on every sweep with no end.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_ADropThatFailsOnEverySweep_WarnsToTheCap_ThenTriesOnceAnHourAtDebug()
    {
        var rig = BuildOnPremRig();
        rig.Refuse.Add(TheServer);

        for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: false);
            Assert.Null(rig.State.LongQueryTraceApplied);
        }

        await rig.ReconcileAsync(enabled: false);
        Assert.False(rig.State.LongQueryTraceApplied);
        Assert.Equal(LongQueryTraceDatabases.DropAttemptCap - 1, Logged(rig, Microsoft.Extensions.Logging.LogLevel.Warning, DropLine));
        var giveUp = Assert.Single(rig.Logger.Entries, e => e.Message.Contains("Stopped retrying", StringComparison.Ordinal));
        Assert.Contains("may remain on the server", giveUp.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("databases could not be listed", giveUp.Message, StringComparison.Ordinal);
        Assert.EndsWith(" It also tries again after it reconnects.", giveUp.Message, StringComparison.Ordinal);
        Assert.Equal(0, Logged(rig, Microsoft.Extensions.Logging.LogLevel.Warning, WorkerLine(rig)));
        Assert.Equal(LongQueryTraceDatabases.DropAttemptCap, Warnings(rig));

        /* Done: the sweeps within the hour run nothing. */
        rig.Calls.Clear();
        await rig.ReconcileAsync(enabled: false);
        rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
        await rig.ReconcileAsync(enabled: false);
        Assert.Empty(rig.Calls);

        /* An hour after the cap: one attempt, at Debug, and the sweep after it runs nothing. */
        rig.Clock += TimeSpan.FromMinutes(1);
        await rig.ReconcileAsync(enabled: false);
        await rig.ReconcileAsync(enabled: false);
        Assert.Single(rig.Calls);
        Assert.Equal(LongQueryTraceDatabases.DropAttemptCap, Warnings(rig));
        Assert.Equal(1, Logged(rig, Microsoft.Extensions.Logging.LogLevel.Debug, "The next attempt is in an hour."));
        Assert.False(rig.State.LongQueryTraceApplied);
    }

    /// <summary>
    /// Turning the trace on is a create that succeeds, and it ends the run of drop failures: after the cap gave up, on and
    /// off again, the next failures count from one, and warn again.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_AfterTheCap_TurnedOnAndOffAgain_CountsTheFailuresAgain()
    {
        var rig = BuildOnPremRig();
        rig.Refuse.Add(TheServer);
        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: false);
        }

        Assert.False(rig.State.LongQueryTraceApplied);

        rig.Refuse.Clear();
        await rig.ReconcileAsync(enabled: true);
        Assert.True(rig.State.LongQueryTraceApplied);

        var warnings = Warnings(rig);
        rig.Refuse.Add(TheServer);
        for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: false);
        }

        Assert.Equal(LongQueryTraceDatabases.DropAttemptCap - 1, Warnings(rig) - warnings);
        Assert.True(rig.State.LongQueryTraceApplied);
    }

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
    /// The read leaves out master too, by the trace's own rule (<see cref="LongQueryTraceDatabases.CanHoldSession"/>), and
    /// counts the databases it lists without master, so a logical server whose user databases are all monitored
    /// separately is not left reading master alone, and a list of master alone is not blamed on them (#4961). The
    /// behaviour is driven in <see cref="EmptyDatabaseListNoteLiveTests"/>; pinned here in the source as well, so the
    /// rule stays the trace's own and not a second copy in the runner.
    /// </summary>
    [Fact]
    public void TheLongQueryRead_AlsoSkipsMaster_ByTheTracesOwnRule()
    {
        var runner = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        var skip = runner.IndexOf("if (definition.SkipsSeparatelyMonitoredDatabases)", StringComparison.Ordinal);
        var note = runner.IndexOf("EmptyDatabaseListNote.For(", skip, StringComparison.Ordinal);
        Assert.True(skip > 0 && note > skip, "the skip comes before the note's inputs");

        var block = runner[skip..note];
        var master = block.IndexOf("databases = databases.FindAll(LongQueryTraceDatabases.CanHoldSession);", StringComparison.Ordinal);
        var counted = block.IndexOf("listedDatabaseCount = databases.Count;", StringComparison.Ordinal);
        var separately = block.IndexOf("databases = AzureSweepScope.WithoutSeparatelyMonitored(databases, SeparatelyMonitoredDatabasesFor(server));", StringComparison.Ordinal);
        Assert.True(master > 0 && counted > master && separately > counted,
            "master leaves the list, then the list is counted, then the separately monitored databases leave");
        Assert.DoesNotContain("\"master\"", block, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("master", false)]
    [InlineData("MASTER", false)]
    [InlineData("alpha", true)]
    [InlineData("masters", true)]
    public void TheTrace_CanHoldASessionInEveryDatabaseButMaster(string database, bool expected)
    {
        Assert.Equal(expected, LongQueryTraceDatabases.CanHoldSession(database));
    }

    [Fact]
    public void ThePlan_CreatesTheSessionWhereTheReadReads_NeverInMaster()
    {
        var listed = new[] { "master", "alpha", "zeta" };

        var plan = LongQueryTraceDatabases.Plan(
            enabled: true, listed, listed, Array.Empty<string>(), Array.Empty<string>());

        Assert.Equal(listed.Where(LongQueryTraceDatabases.CanHoldSession), plan.Create);
        Assert.DoesNotContain("master", plan.Create, StringComparer.OrdinalIgnoreCase);
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

        /* #4961: a test replaces the listing, so the read of the server is the second arm of a conditional. */
        Assert.Contains(": await runner.GetAzureDatabaseListAsync(server, databaseScope: null, cancellationToken);", body, StringComparison.Ordinal);
        Assert.DoesNotContain("SeparatelyMonitored", body, StringComparison.Ordinal);
    }

    /* ── #4961: the session is this install's own, named from its id ── */

    [Fact]
    public async Task Azure_EveryPass_NamesOnlyThisInstallsSession()
    {
        /* "gamma" is excluded and refuses the drop outside the monitored set until the cap gives up: the full pass, its
           retries, the attempt after the cap and the hourly create all run, then the trace goes off. */
        var rig = BuildRig("gamma");
        rig.Refuse.Add("gamma");
        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: true);
        }

        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        rig.Refuse.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        await rig.ReconcileAsync(enabled: false);

        Assert.Contains(rig.Calls, c => c.Create);
        Assert.Contains(rig.Calls, c => !c.Create);
        Assert.Equal(rig.Calls.Count, rig.Names.Count);
        Assert.All(rig.Names, name => Assert.Equal(OwnSession, name));
        Assert.DoesNotContain(rig.Names, name => name == LongQueryCompletionsCollector.LegacyXeSessionName);

        /* The legacy session is dropped once in each listed database but master, by its own name, and never again. */
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.LegacyCalls);
        Assert.All(rig.LegacyNames, name => Assert.Equal(LongQueryCompletionsCollector.LegacyXeSessionName, name));
    }

    [Fact]
    public async Task OnPrem_TheFullAndTheHourlyPass_AndTheDrop_NameThisInstallsSession()
    {
        var rig = BuildOnPremRig();

        await rig.ReconcileAsync(enabled: true);
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(2, rig.Calls.Count(c => c.Create));

        await rig.ReconcileAsync(enabled: false);

        Assert.Single(rig.Calls, c => !c.Create);
        Assert.Equal(3, rig.Names.Count);
        Assert.All(rig.Names, name => Assert.Equal(OwnSession, name));

        /* The legacy session is dropped once on the server, by its own name, and the hourly pass does not touch it. */
        Assert.Equal(new[] { TheServer }, rig.LegacyCalls);
        Assert.All(rig.LegacyNames, name => Assert.Equal(LongQueryCompletionsCollector.LegacyXeSessionName, name));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NoInstallId_NothingIsCreated_AndTheRunRecordsWhy(bool azureSqlDatabase)
    {
        var rig = BuildRig(
            new MonitoredServer { Name = "lqtrace-noid", Host = azureSqlDatabase ? Host : "lqtrace-sql" },
            ServerId, azureSqlDatabase, installId: null);

        await rig.ReconcileAsync(enabled: true);

        Assert.Empty(rig.Calls);
        Assert.Empty(rig.Names);
        Assert.Null(rig.State.LongQueryTraceApplied);
        Assert.NotNull(rig.State.LongQueryTraceFault);
        Assert.Contains("no id", rig.State.LongQueryTraceFault, StringComparison.Ordinal);

        /* Off: there is no session of this install's to drop, and the fault is cleared. */
        await rig.ReconcileAsync(enabled: false);

        Assert.Empty(rig.Calls);
        Assert.Null(rig.State.LongQueryTraceFault);
    }

    [Fact]
    public void TheEnsures_StartASessionTheyFindStopped_ByThisInstallsName_AndNothingNamesTheLegacySession()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");

        /* On-prem: the existence check also reports whether it runs, and a stopped one is started by name. */
        Assert.Contains("is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END", source, StringComparison.Ordinal);
        Assert.Contains("isRunning == 0", source, StringComparison.Ordinal);
        Assert.Contains("BuildStartSessionSql(sessionName, databaseScoped: false)", source, StringComparison.Ordinal);

        /* Azure SQL Database: a session that exists and does not run is started by name. */
        Assert.Contains("FROM sys.dm_xe_database_sessions AS xes", source, StringComparison.Ordinal);
        Assert.Contains("ALTER EVENT SESSION [{sessionName}] ON DATABASE STATE = START;", source, StringComparison.Ordinal);

        Assert.DoesNotContain("LegacyXeSessionName", source, StringComparison.Ordinal);
        Assert.DoesNotContain("LongQueryCompletionsCollector.XeSessionName", source, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyThePerInstallDdlStartsOff_TheDeadlockAndBlockedProcessDdlStaysOn()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");

        /* The two server-scoped always-on session statements (deadlock and blocked process) are unchanged. The two Azure ones are
           made by the shared builder since #4961, which keeps the shared sessions on and starts only an own fallback off. */
        Assert.Equal(2, source.Split("STARTUP_STATE = ON", StringSplitOptions.None).Length - 1);
        foreach (var kind in new[] { AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeSessionKind.BlockedProcess })
        {
            Assert.Contains("STARTUP_STATE = ON", AlwaysOnXeSessions.BuildAzureCreateSql(kind, AlwaysOnXeSessions.SharedNameFor(kind)), StringComparison.Ordinal);
        }
        Assert.DoesNotContain("STARTUP_STATE = OFF", source, StringComparison.Ordinal);

        var sql = LongQueryCompletionsCollector.BuildCreateSessionSql(OwnSession, databaseScoped: false, 2_000_000);
        Assert.Contains("STARTUP_STATE = OFF", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("STARTUP_STATE = ON", sql, StringComparison.Ordinal);
    }

    /* ── #4961: the legacy session is dropped once per registration and database, then recorded ── */

    private static readonly string LegacyName = LongQueryCompletionsCollector.LegacyXeSessionName;

    private static readonly string[] ThreeDatabases = { "alpha", "beta", "gamma" };

    /* The one Information line that names a legacy session an older install created again. */
    private static int Notices(Rig rig) =>
        rig.Logger.Entries.Count(e => e.Level == LogLevel.Information && e.Message.Contains(LegacyName, StringComparison.Ordinal));

    [Fact]
    public async Task Legacy_Azure_IsDroppedOncePerDatabase_RecordedAfterTheDrop_AndNeverDroppedAgain()
    {
        var rig = BuildRig();
        rig.LegacySessions.UnionWith(ThreeDatabases);

        await rig.ReconcileAsync(enabled: true);

        /* Each listed database but master, once, and the session is gone. */
        Assert.Equal(ThreeDatabases, rig.LegacyCalls);
        Assert.Empty(rig.LegacySessions);
        foreach (var database in ThreeDatabases)
        {
            Assert.True(rig.Records.Has(ServerId, database), database);
            var value = rig.Records.Rows[(ServerId, LegacyLongQuerySession.StateKey(database))];
            Assert.True(DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out _), value);
        }

        Assert.False(rig.Records.Has(ServerId, "master"));
        Assert.Equal(ThreeDatabases.Length, rig.Records.Rows.Count);

        /* A reconnect, the trace turned off and on again: each is a full pass, and none drops it again. */
        rig.Reconnect();
        await rig.ReconcileAsync(enabled: true);
        await rig.ReconcileAsync(enabled: false);
        rig.Clock += TimeSpan.FromMinutes(1);
        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(ThreeDatabases, rig.LegacyCalls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Legacy_OnPrem_IsDroppedOnceOnTheServer_WhetherTheTraceIsOnOrOff_BeforeThePerInstallWork(bool enabled)
    {
        var rig = BuildOnPremRig();
        rig.LegacySessions.Add(TheServer);

        await rig.ReconcileAsync(enabled);

        Assert.Equal(new[] { TheServer }, rig.LegacyCalls);
        Assert.Equal(new[] { "legacy:", enabled ? "create:" : "drop:" }, rig.Events);

        /* The server scope's database part is empty, and the value is the time of the drop. */
        var key = Assert.Single(rig.Records.Rows.Keys);
        Assert.Equal((ServerId, "legacy_session_dropped:"), key);

        rig.Reconnect();
        await rig.ReconcileAsync(!enabled);
        await rig.ReconcileAsync(enabled);

        Assert.Single(rig.LegacyCalls);
    }

    [Fact]
    public async Task Legacy_ACrashBetweenTheDropAndTheRecord_CostsOneMoreDrop_ThenIsRecorded()
    {
        var rig = BuildRig();
        rig.LegacySessions.UnionWith(ThreeDatabases);
        rig.Records.WriteFailure = new InvalidOperationException("The store went away.");

        await rig.ReconcileAsync(enabled: true);

        /* The drops ran and the record did not, so the pass is not done and nothing is remembered. */
        Assert.Equal(ThreeDatabases, rig.LegacyCalls);
        Assert.Empty(rig.Records.Rows);
        Assert.Null(rig.State.LongQueryTraceApplied);

        /* The next sweep drops each one more time and records it. After that nothing drops it again. */
        rig.Records.WriteFailure = null;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(ThreeDatabases.Length * 2, rig.LegacyCalls.Count);
        Assert.True(rig.State.LongQueryTraceApplied);

        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        rig.Reconnect();
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(ThreeDatabases.Length * 2, rig.LegacyCalls.Count);
    }

    [Fact]
    public async Task Legacy_TheRecordSurvivesAServiceRestart_AndAReconnect()
    {
        var rig = BuildRig();
        rig.LegacySessions.UnionWith(ThreeDatabases);
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(ThreeDatabases, rig.LegacyCalls);

        /* A service restart: a new runner that has only the record to go on. */
        var restarted = Restart(rig);
        restarted.LegacySessions.UnionWith(ThreeDatabases);
        await restarted.ReconcileAsync(enabled: true);
        Assert.Empty(restarted.LegacyCalls);

        restarted.Reconnect();
        await restarted.ReconcileAsync(enabled: true);
        Assert.Empty(restarted.LegacyCalls);
        Assert.Equal(ThreeDatabases.Length, restarted.LegacySessions.Count);
    }

    [Fact]
    public async Task Legacy_ASessionAnOlderInstallCreatesAgain_IsNotDropped_AndOneInformationLineNamesItPerConnect()
    {
        var rig = BuildRig();
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(ThreeDatabases, rig.LegacyCalls);
        Assert.Equal(0, Notices(rig));

        /* An older install creates it again. The hourly create pass finds it, says so once, and leaves it alone. */
        rig.LegacySessions.Add("alpha");
        rig.LegacyCalls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);

        Assert.Empty(rig.LegacyCalls);
        Assert.Contains("alpha", rig.LegacySessions);
        var notice = Assert.Single(rig.Logger.Entries, e => e.Level == LogLevel.Information && e.Message.Contains(LegacyName, StringComparison.Ordinal));
        Assert.Contains("older Lite or Darling created it", notice.Message, StringComparison.Ordinal);
        Assert.Contains("--drop-xe-sessions", notice.Message, StringComparison.Ordinal);

        /* A reconnect is a new connect: the full pass leaves it alone and says so again. */
        rig.Reconnect();
        await rig.ReconcileAsync(enabled: true);
        Assert.Empty(rig.LegacyCalls);
        Assert.Equal(2, Notices(rig));
        Assert.Contains("alpha", rig.LegacySessions);
    }

    [Fact]
    public async Task Legacy_TheAttemptAfterTheCap_LeavesASessionAnOlderInstallCreatedAgainAlone()
    {
        /* gamma is excluded and refuses the drop outside the monitored set, until the cap gives up on it. */
        var rig = BuildRig("gamma");
        rig.Refuse.Add("gamma");
        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: true);
        }

        Assert.Equal(ThreeDatabases, rig.LegacyCalls);
        Assert.True(rig.State.LongQueryTraceDropRetry.NextAttemptUtc is not null);

        rig.LegacySessions.Add("beta");
        rig.LegacyCalls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);

        Assert.Empty(rig.LegacyCalls);
        Assert.Contains("beta", rig.LegacySessions);
        Assert.Equal(1, Notices(rig));
    }

    [Fact]
    public async Task Legacy_TheDrop_NamesOnlyTheLegacySession_NeverAnotherInstallsNorTheDeadlockOrBlockedProcessSessions()
    {
        var azure = BuildRig("gamma");
        azure.LegacySessions.UnionWith(ThreeDatabases);
        await azure.ReconcileAsync(enabled: true);
        azure.Clock += LongQueryTraceDatabases.RetryInterval;
        await azure.ReconcileAsync(enabled: true);
        await azure.ReconcileAsync(enabled: false);

        var onPrem = BuildOnPremRig();
        onPrem.LegacySessions.Add(TheServer);
        await onPrem.ReconcileAsync(enabled: false);

        foreach (var rig in new[] { azure, onPrem })
        {
            Assert.NotEmpty(rig.LegacyNames);
            Assert.All(rig.LegacyNames, name => Assert.Equal(LegacyName, name));
            Assert.NotEmpty(rig.Names);
            Assert.All(rig.Names, name => Assert.Equal(OwnSession, name));
        }

        /* The statement builder takes the legacy name and a per-install one, and nothing else. */
        foreach (var other in new[] { DeadlocksCollector.XeSessionName, BlockedProcessReportCollector.XeSessionName })
        {
            Assert.NotEqual(LegacyName, other);
            Assert.Throws<ArgumentException>(() => LongQueryCompletionsCollector.BuildDropSessionSql(other, databaseScoped: false));
            Assert.Throws<ArgumentException>(() => LongQueryCompletionsCollector.BuildDropSessionSql(other, databaseScoped: true));
        }
    }

    [Fact]
    public async Task Legacy_AFailedDrop_IsNotRecorded_AndRetriesOnTheCap_ThenOnceAnHourAtDebug()
    {
        var rig = BuildRig();
        rig.LegacySessions.UnionWith(ThreeDatabases);
        rig.RefuseLegacy.Add("beta");

        for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: true);
            Assert.Null(rig.State.LongQueryTraceApplied);
        }

        /* The first pass tried every database. The passes after it retried beta alone, which is the one not recorded. */
        Assert.Equal(new[] { "alpha", "beta", "gamma", "beta", "beta", "beta" }, rig.LegacyCalls);
        Assert.True(rig.Records.Has(ServerId, "alpha"));
        Assert.True(rig.Records.Has(ServerId, "gamma"));
        Assert.False(rig.Records.Has(ServerId, "beta"));

        /* The failed legacy drop did not stop the per-install session from being created there. */
        Assert.Contains("beta", rig.Sessions);

        await rig.ReconcileAsync(enabled: true);
        Assert.True(rig.State.LongQueryTraceApplied);
        var giveUp = Assert.Single(rig.Logger.Entries, e => e.Message.Contains("Stopped retrying", StringComparison.Ordinal));
        Assert.Equal(LogLevel.Warning, giveUp.Level);
        Assert.Contains("beta", giveUp.Message, StringComparison.Ordinal);

        /* Done for now: the sweeps within the hour try nothing. An hour later, one attempt, logged at Debug. */
        rig.LegacyCalls.Clear();
        await rig.ReconcileAsync(enabled: true);
        Assert.Empty(rig.LegacyCalls);
        var warnings = Warnings(rig);
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(new[] { "beta" }, rig.LegacyCalls);
        Assert.Equal(warnings, Warnings(rig));

        /* The refusal ends: the next attempt records it, and the attempts end. */
        rig.RefuseLegacy.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.True(rig.Records.Has(ServerId, "beta"));
        Assert.Null(rig.State.LongQueryTraceDropRetry.NextAttemptUtc);
    }

    [Fact]
    public async Task Legacy_OnPrem_AFailedDrop_IsNotRecorded_AndRetriesOnTheCap_WhileThePerInstallSessionIsStillCreated()
    {
        var rig = BuildOnPremRig();
        rig.LegacySessions.Add(TheServer);
        rig.RefuseLegacy.Add(TheServer);

        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync(enabled: true);
        }

        Assert.Equal(LongQueryTraceDatabases.DropAttemptCap, rig.LegacyCalls.Count);
        Assert.Equal(LongQueryTraceDatabases.DropAttemptCap, rig.Created.Count());
        Assert.Empty(rig.Records.Rows);
        var giveUp = Assert.Single(rig.Logger.Entries, e => e.Message.Contains("Stopped retrying", StringComparison.Ordinal));
        Assert.Contains("may remain on the server", giveUp.Message, StringComparison.Ordinal);

        rig.RefuseLegacy.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);

        Assert.True(rig.Records.Has(ServerId, TheServer));
        Assert.Null(rig.State.LongQueryTraceDropRetry.NextAttemptUtc);
    }

    [Fact]
    public async Task Azure_InEachDatabase_TheLegacyDropRunsBeforeThePerInstallCreate()
    {
        /* gamma is excluded: its legacy drop still runs, and its per-install drop follows it. */
        var rig = BuildRig("gamma");
        rig.LegacySessions.UnionWith(ThreeDatabases);

        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(
            new[] { "legacy:alpha", "create:alpha", "legacy:beta", "create:beta", "legacy:gamma", "drop:gamma" },
            rig.Events);
        Assert.Equal(2, rig.ListCalls);
    }

    [Fact]
    public async Task Azure_AFailedLegacyDrop_DoesNotStopThePerInstallCreateInThatDatabase()
    {
        var rig = BuildRig();
        rig.RefuseLegacy.Add("beta");

        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(
            new[] { "legacy:alpha", "create:alpha", "legacy:beta", "create:beta", "legacy:gamma", "create:gamma" },
            rig.Events);
        Assert.Contains("beta", rig.Sessions);
        Assert.Null(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task Azure_TheTraceOff_DropsTheLegacySessionOnceInEachListedDatabase_WithOneListing()
    {
        var rig = BuildRig();
        rig.LegacySessions.UnionWith(ThreeDatabases);

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(
            new[] { "legacy:alpha", "legacy:beta", "legacy:gamma", "drop:alpha", "drop:beta", "drop:gamma" },
            rig.Events);
        Assert.Equal(1, rig.ListCalls);
        Assert.Equal(ThreeDatabases.Length, rig.Records.Rows.Count);
    }

    [Fact]
    public async Task Legacy_ARecordThatCannotBeRead_SkipsTheLegacyStep_NeverDropsTwice_AndTriesAgainOnTheNextPass()
    {
        /* A store that has the record, read by a restarted service while the store is not answering. */
        var rig = BuildRig();
        await rig.ReconcileAsync(enabled: true);
        var restarted = Restart(rig);
        restarted.LegacySessions.Add("alpha");
        restarted.Records.ReadFailure = new InvalidOperationException("The store is not answering.");

        await restarted.ReconcileAsync(enabled: true);

        Assert.Empty(restarted.LegacyCalls);
        Assert.Equal(ThreeDatabases, restarted.Created);
        Assert.Null(restarted.State.LongQueryTraceApplied);
        Assert.Contains(restarted.Logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("The store is not answering.", StringComparison.Ordinal));

        restarted.Records.ReadFailure = null;
        await restarted.ReconcileAsync(enabled: true);

        Assert.Empty(restarted.LegacyCalls);
        Assert.True(restarted.State.LongQueryTraceApplied);
        Assert.Contains("alpha", restarted.LegacySessions);

        /* A store with no record yet: the unreadable pass drops nothing, and the next one drops each session once. */
        var fresh = BuildRig();
        fresh.LegacySessions.UnionWith(ThreeDatabases);
        fresh.Records.ReadFailure = new InvalidOperationException("The store is not answering.");
        await fresh.ReconcileAsync(enabled: true);
        Assert.Empty(fresh.LegacyCalls);
        Assert.Equal(ThreeDatabases, fresh.Created);

        fresh.Records.ReadFailure = null;
        await fresh.ReconcileAsync(enabled: true);
        Assert.Equal(ThreeDatabases, fresh.LegacyCalls);
        Assert.Equal(ThreeDatabases.Length, fresh.Records.Rows.Count);
    }

    [Fact]
    public async Task Legacy_OnPrem_ARecordThatCannotBeRead_DropsNothing_AndTheNextPassTriesAgain()
    {
        var rig = BuildOnPremRig();
        rig.LegacySessions.Add(TheServer);
        rig.Records.ReadFailure = new InvalidOperationException("The store is not answering.");

        await rig.ReconcileAsync(enabled: false);

        Assert.Empty(rig.LegacyCalls);
        Assert.Null(rig.State.LongQueryTraceApplied);

        rig.Records.ReadFailure = null;
        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { TheServer }, rig.LegacyCalls);
        Assert.False(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task Legacy_AKeyKnownToBeRecorded_IsKeptInMemory_SoLaterPassesDoNotReadTheStoreAgain()
    {
        var rig = BuildRig();
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(1, rig.Records.Reads);

        /* Turning the trace off and on again makes full passes, and the hourly pass is the create side alone. */
        await rig.ReconcileAsync(enabled: false);
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(1, rig.Records.Reads);

        /* A connect reads it again, once. */
        rig.Reconnect();
        await rig.ReconcileAsync(enabled: true);
        Assert.Equal(2, rig.Records.Reads);
    }

    [Fact]
    public async Task Legacy_AnOlderDarlingWithTheTraceOn_LosesItsSessionOncePerUpgradedInstall_ThenNeverAgain()
    {
        var first = BuildRig(new MonitoredServer { Name = "lqtrace", Host = Host }, ServerId, installId: "0a1b2c3d");
        var second = BuildRig(new MonitoredServer { Name = "lqtrace", Host = Host }, ServerId, installId: "4e5f6a7b");

        /* One older install's session, in one server, that both upgraded installs see. */
        second.LegacySessions = first.LegacySessions;
        first.LegacySessions.UnionWith(ThreeDatabases);

        await first.ReconcileAsync(enabled: true);
        Assert.Empty(first.LegacySessions);

        /* The older install creates it again at its next connect, and the second upgraded install drops it once. */
        first.LegacySessions.UnionWith(ThreeDatabases);
        await second.ReconcileAsync(enabled: true);
        Assert.Empty(second.LegacySessions);

        /* The older install creates it once more. Neither upgraded install drops it again, at a connect or an hour later. */
        first.LegacySessions.UnionWith(ThreeDatabases);
        foreach (var rig in new[] { first, second })
        {
            rig.Reconnect();
            await rig.ReconcileAsync(enabled: true);
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync(enabled: true);
        }

        Assert.Equal(ThreeDatabases, first.LegacyCalls);
        Assert.Equal(ThreeDatabases, second.LegacyCalls);
        Assert.Equal(ThreeDatabases.Length, first.LegacySessions.Count);

        /* Each install made and kept only the session that carries its own id. */
        Assert.All(first.Names, name => Assert.Equal(LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, "0a1b2c3d"), name));
        Assert.All(second.Names, name => Assert.Equal(LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, "4e5f6a7b"), name));
    }

    [Fact]
    public void TheEnsures_AskForTheLegacySessionInTheirOwnBatch_AndTheLegacyLogicLivesInItsOwnFile()
    {
        var xe = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");

        /* The on-premises batch and the Azure SQL Database batch each carry a second SELECT for the legacy name, so finding
           a session an older install created again costs no round trip of its own. */
        Assert.Equal(2, xe.Split("legacy_present = ", StringSplitOptions.None).Length - 1);
        Assert.Contains("FROM sys.server_event_sessions AS ses", xe, StringComparison.Ordinal);
        Assert.Contains("FROM sys.database_event_sessions AS des", xe, StringComparison.Ordinal);

        /* DarlingXeSessions.cs never spells the legacy constant: the one-time drop and its record live in their own file. */
        Assert.DoesNotContain("LegacyXeSessionName", xe, StringComparison.Ordinal);
        var legacy = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingLegacyLongQuerySession.cs");
        Assert.Contains("LongQueryCompletionsCollector.LegacyXeSessionName", legacy, StringComparison.Ordinal);
        Assert.Contains("LegacyLongQuerySession.StateKey(database)", legacy, StringComparison.Ordinal);
        Assert.Contains("LegacyLongQuerySession.StateCollector", legacy, StringComparison.Ordinal);

        /* The store-backed record reads and writes through its own throwing statements, not the runner's swallowing helpers. */
        Assert.DoesNotContain("GetCollectorStateAsync", legacy, StringComparison.Ordinal);
        Assert.DoesNotContain("SaveCollectorStateAsync", legacy, StringComparison.Ordinal);
    }

    /* ── #4961: another registration of this install on the same instance keeps the session (L6, test 21) ── */

    private const string InstanceName = "SQL01";

    /// <summary>
    /// Gives the rig the guard the sweep hands an on-premises reconcile: the worker's own builder, over a registry of this
    /// install's other registrations, each one's effective trace setting, and the name each instance last reported. Every
    /// resolution is counted. A null name is an instance that has reported none.
    /// </summary>
    private static void GiveInstanceGuard(Rig rig, string? ownName, params (int Id, bool TraceOn, string? Name, string Engine)[] others)
    {
        /* The registry holds this registration too, which the guard leaves out by its id. */
        rig.Config.StoredServerId = ServerId;
        var held = others.Where(o => o.Name is not null).ToDictionary(o => o.Id, o => o.Name!);
        if (ownName is not null)
        {
            held[ServerId] = ownName;
        }

        var registry = others
            .Select(o => new MonitoredServer { Name = "alias-" + o.Id, Host = "alias-" + o.Id, Engine = o.Engine, StoredServerId = o.Id })
            .Append(rig.Config)
            .ToList();
        rig.InstanceGuard = () =>
        {
            rig.GuardResolutions++;
            return DarlingWorker.LongQueryTraceInstanceGuardFor(
                ServerId,
                registry,
                id => others.Single(o => o.Id == id).TraceOn,
                (id, carrier) => Task.FromResult(
                    carrier == ServerEpoch.IdentityCarrierCollectors[0] && held.TryGetValue(id, out var name)
                        ? new Dictionary<string, string>
                        {
                            [ServerEpoch.IdentityStateKey] = ServerEpoch.Serialize(new ServerEpoch.Stamp(new DateTime(2026, 10, 2, 8, 0, 0, DateTimeKind.Utc), name)),
                        }
                        : new Dictionary<string, string>()));
        };
    }

    private const int OtherId = 9001;

    /// <summary>
    /// Test 21: a registration whose trace is off does not drop the session another registration of the same instance
    /// keeps. The match is positive: both registrations' last-known names are known and agree, ignoring case, and the other
    /// registration has its trace on. Anything less drops, as before. Either way the reconcile is done, so the next sweep
    /// opens no connection, and the one-time drop of the legacy session still runs.
    /// </summary>
    [Theory]
    [InlineData("SQL01", "sql01", true, "sqlserver", false)]
    [InlineData("SQL01", "SQL01", true, "sqlserver", false)]
    [InlineData("SQL01", "SQL01", false, "sqlserver", true)]
    [InlineData("SQL01", "SQL01", true, "postgres", true)]
    [InlineData("SQL01", "SQL02", true, "sqlserver", true)]
    [InlineData("SQL01", null, true, "sqlserver", true)]
    [InlineData(null, "SQL01", true, "sqlserver", true)]
    public async Task Off_OnPremises_LeavesTheSession_WhileAnotherRegistrationOfTheSameInstanceKeepsIt(
        string? ownName, string? otherName, bool otherTraceOn, string otherEngine, bool expectDrop)
    {
        var rig = BuildOnPremRig();
        rig.LegacySessions.Add(TheServer);
        GiveInstanceGuard(rig, ownName, (OtherId, otherTraceOn, otherName, otherEngine));

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(expectDrop ? new[] { TheServer } : Array.Empty<string>(), rig.Dropped);
        Assert.Equal(new[] { TheServer }, rig.LegacyCalls);
        Assert.Empty(rig.LegacySessions);
        Assert.False(rig.State.LongQueryTraceApplied);
        Assert.Equal(1, rig.GuardResolutions);

        /* Done either way: the next sweep runs nothing, and does not resolve the guard again. */
        rig.Calls.Clear();
        rig.LegacyCalls.Clear();
        await rig.ReconcileAsync(enabled: false);
        Assert.Empty(rig.Calls);
        Assert.Empty(rig.LegacyCalls);
        Assert.Equal(1, rig.GuardResolutions);
    }

    [Fact]
    public async Task Off_OnPremises_ASkippedDrop_SaysSoAtInformation_AndTheLegacyDropRunsBeforeIt()
    {
        var rig = BuildOnPremRig();
        rig.LegacySessions.Add(TheServer);
        GiveInstanceGuard(rig, InstanceName, (OtherId, true, "sql01", "sqlserver"));

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "legacy:" }, rig.Events);
        Assert.Equal(1, Logged(rig, LogLevel.Information, $"[{rig.Config.DisplayName}] Long-query completion XE session left in place: another registration of this install keeps it on the same instance"));
        Assert.Equal(0, Logged(rig, LogLevel.Information, "reconciled OFF"));
        Assert.Equal(0, Warnings(rig));
    }

    /// <summary>A drop that goes ahead keeps its account, so the two cases read apart in the log.</summary>
    [Fact]
    public async Task Off_OnPremises_ADropThatGoesAhead_SaysItReconciledOff_AndNotThatItLeftTheSession()
    {
        var rig = BuildOnPremRig();
        rig.LegacySessions.Add(TheServer);
        GiveInstanceGuard(rig, InstanceName, (OtherId, true, "SQL02", "sqlserver"));

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { "legacy:", "drop:" }, rig.Events);
        Assert.Equal(1, Logged(rig, LogLevel.Information, "reconciled OFF"));
        Assert.Equal(0, Logged(rig, LogLevel.Information, "keeps it on the same instance"));
    }

    /// <summary>A failed legacy drop still fails the pass after a skipped per-install drop, and the next sweep tries again.</summary>
    [Fact]
    public async Task Off_OnPremises_ASkippedDrop_StillReportsAFailedLegacyDrop()
    {
        var rig = BuildOnPremRig();
        rig.RefuseLegacy.Add(TheServer);
        GiveInstanceGuard(rig, InstanceName, (OtherId, true, "SQL01", "sqlserver"));

        await rig.ReconcileAsync(enabled: false);

        Assert.Empty(rig.Dropped);
        Assert.Equal(new[] { TheServer }, rig.LegacyCalls);
        Assert.NotEqual(false, rig.State.LongQueryTraceApplied);

        rig.RefuseLegacy.Clear();
        rig.LegacyCalls.Clear();
        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(new[] { TheServer }, rig.LegacyCalls);
        Assert.False(rig.State.LongQueryTraceApplied);
    }

    /// <summary>The guard reads the store, so a trace that is on never resolves it: not on the create, and not on the hourly pass.</summary>
    [Fact]
    public async Task On_OnPremises_NeverResolvesTheInstanceGuard()
    {
        var rig = BuildOnPremRig();
        GiveInstanceGuard(rig, InstanceName, (OtherId, true, "SQL01", "sqlserver"));

        await rig.ReconcileAsync(enabled: true);
        rig.Clock = rig.Clock.AddHours(1);
        await rig.ReconcileAsync(enabled: true);

        Assert.Equal(0, rig.GuardResolutions);
        Assert.Equal(2, rig.Created.Count());
    }

    /// <summary>An Azure SQL Database server keeps one session per database, so its drop never asks the instance guard.</summary>
    [Fact]
    public async Task Off_Azure_NeverResolvesTheInstanceGuard()
    {
        var rig = BuildRig();
        GiveInstanceGuard(rig, InstanceName, (OtherId, true, "SQL01", "sqlserver"));

        await rig.ReconcileAsync(enabled: false);

        Assert.Equal(0, rig.GuardResolutions);
        Assert.NotEmpty(rig.Dropped);
    }

    /// <summary>
    /// The sweep builds the guard only for a SQL Server target that is not an Azure SQL Database, and builds it lazily: the
    /// instance's reconcile hands the static one a function, so a server whose trace is off and already reconciled reads
    /// no state. The function resolves the live registry and each registration's own schedule when it is called. Pinned in
    /// the source: the sweep needs a live registry and store.
    /// </summary>
    [Fact]
    public void TheSweep_HandsTheReconcileALazyInstanceGuard_OnlyForAnOnPremisesTarget()
    {
        var worker = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var sweep = worker.IndexOf("private async Task ReconcileLongQueryTraceAsync(ServerLoopState server, DarlingCollectorRunner runner, CancellationToken cancellationToken)", StringComparison.Ordinal);
        var call = worker.IndexOf("await ReconcileLongQueryTraceAsync(server, runner, enabled, registrations, serverSeparatelyMonitored, DateTime.UtcNow, _logger,", sweep, StringComparison.Ordinal);
        Assert.True(sweep > 0 && call > sweep, "the sweep's reconcile call is pinned");

        var body = worker[sweep..call];
        Assert.Contains("Func<Task<LongQueryTraceInstanceGuard>>? instanceGuard = null;", body, StringComparison.Ordinal);
        Assert.Contains("if (!server.Runtime.Target.IsAzureSqlDb)", body, StringComparison.Ordinal);
        Assert.Contains("instanceGuard = () => LongQueryTraceInstanceGuardFor(", body, StringComparison.Ordinal);
        Assert.Contains("_registryState.Read()?.Servers,", body, StringComparison.Ordinal);
        Assert.Contains("otherId => StoreConfigProvider.ResolveSchedule(\"long_query_completions\", otherId, _scheduleOverrides).Enabled,", body, StringComparison.Ordinal);
        Assert.Contains("(id, carrier) => runner.GetCollectorStateAsync(id, carrier, cancellationToken)", body, StringComparison.Ordinal);

        var end = worker.IndexOf(");", call, StringComparison.Ordinal);
        Assert.EndsWith(", cancellationToken, instanceGuard", worker[call..end], StringComparison.Ordinal);
    }
}
