/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
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
/// Removing a server drops this install's Extended Events sessions on it, and nothing else (#4961): the long-query
/// trace's session, then the deadlock and blocked-process sessions this install chose for itself in the server's
/// databases. Each test drives the removal's real step with the database list and the per-database work replaced on the
/// runner, so no server or store is needed.
/// </summary>
public sealed class LongQueryTraceRemovalTests : IAsyncDisposable
{
    private const string Host = "lqremoval.database.windows.net";
    private const string InstallIdValue = "0a1b2c3d";
    private static readonly string OwnSession = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue);

    /* Never opened: the runner reaches the store only to write collected rows, and these tests collect nothing. */
    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private sealed class RecordingDatabase : IAlwaysOnXeDatabase
    {
        private readonly List<string> _statements;

        public RecordingDatabase(List<string> statements) => _statements = statements;

        public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken) =>
            Task.FromResult(AlwaysOnXeCatalog.Present);

        public Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken) => Task.FromResult(true);

        public Task ExecuteAsync(string statement, CancellationToken cancellationToken)
        {
            _statements.Add(statement);
            return Task.CompletedTask;
        }

        public bool IsAlreadyPresent(Exception exception) => false;
    }

    private sealed class Rig
    {
        public required DarlingCollectorRunner Runner { get; init; }
        public required MonitoredServer Config { get; init; }
        public required ServerRuntime Runtime { get; init; }
        public bool? TraceApplied { get; set; } = true;
        public DarlingSelfAlertTests.CapturingLogger Logger { get; } = new();

        /* What the logical server's master lists, before the registration's exclusions. */
        public List<string> Listed { get; } = new() { "master", "alpha", "beta", "gamma" };
        public int ListCalls { get; set; }

        /* The per-install session's work: the database each call ran in (the server's own session is the empty name). */
        public List<(string Database, bool Create)> Calls { get; } = new();
        public List<string> Names { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Func<CancellationToken, Task>? Hang { get; set; }

        /* The legacy session's drops, which a removal never runs. */
        public List<string> LegacyCalls { get; } = new();
        public LongQueryTraceLifecycleTests.InMemoryLegacyRecords Records { get; } = new();

        /* The statements the always-on fallbacks' drops ran, by database. */
        public Dictionary<string, List<string>> Statements { get; } = new(StringComparer.OrdinalIgnoreCase);

        /* The registrations that remain, the ids among them whose long-query trace is on, and what the on-premises guard says. */
        public List<MonitoredServer> Remaining { get; } = new();
        public HashSet<int> TraceOn { get; } = new();
        public LongQueryTraceInstanceGuard Guard { get; set; } = LongQueryTraceInstanceGuard.NoKeepers;

        public RemovedLongQueryServer Removed => new(Config, Runtime, TraceApplied);

        public IEnumerable<string> Dropped => Calls.Where(c => !c.Create).Select(c => c.Database);

        public Task RemoveAsync(CancellationToken cancellationToken = default) =>
            DarlingRemovedServerSessions.DropAsync(
                Removed, Runner, Remaining, id => TraceOn.Contains(id), _ => Task.FromResult(Guard), Logger, cancellationToken);
    }

    private Rig BuildRig(bool azureSqlDatabase = true, string? installId = InstallIdValue)
    {
        var config = new MonitoredServer { Name = "lqremoval", Host = azureSqlDatabase ? Host : "lqremoval-sql" };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{config.Host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDatabase },
            StorageName = config.StorageName,
            ServerId = config.ServerId,
        };

        var runner = new DarlingCollectorRunner(
            _store,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => new List<string>(),
            separatelyMonitoredDatabases: _ => new List<string>(),
            installId: () => installId);

        var rig = new Rig { Runner = runner, Config = config, Runtime = runtime };
        runner.LegacyLongQueryRecordsForTests = rig.Records;
        runner.LongQueryTraceListOverrideForTests = (_, _, _, _) =>
        {
            rig.ListCalls++;
            return Task.FromResult(rig.Listed.ToList());
        };

        runner.LongQueryTraceDatabaseOverrideForTests = async (_, database, create, sessionName, token) =>
        {
            if (sessionName == LongQueryCompletionsCollector.LegacyXeSessionName)
            {
                rig.LegacyCalls.Add(database);
                return;
            }

            rig.Calls.Add((database, create));
            rig.Names.Add(sessionName);
            if (rig.Hang is { } hang)
            {
                await hang(token);
            }

            if (rig.Refuse.Contains(database))
            {
                throw new InvalidOperationException($"The drop was refused in {database}.");
            }
        };

        runner.AlwaysOnXeDatabaseForTests = (_, database, _) =>
        {
            if (!rig.Statements.TryGetValue(database, out var list))
            {
                rig.Statements[database] = list = new List<string>();
            }

            return Task.FromResult<IAlwaysOnXeDatabase>(new RecordingDatabase(list));
        };

        return rig;
    }

    /// <summary>
    /// Another registration of the logical server, with read-only intent so that it has an id of its own: the id comes from
    /// the host, the database and the intent, not the name.
    /// </summary>
    private static MonitoredServer Another(string host, params string[] excluded) =>
        new() { Name = "lqremoval-other", Host = host, ReadOnlyIntent = true, ExcludedDatabases = excluded.ToList() };

    private static string KeyOf(MonitoredServer server) => server.ServerId.ToString(CultureInfo.InvariantCulture);

    private static string Drop(Rig rig, AlwaysOnXeSessionKind kind) =>
        AlwaysOnXeSessions.BuildAzureDropSql(kind, rig.Runner.AlwaysOnOwnSessionName(kind)!);

    private static LongQueryTraceInstance Instance(bool traceOn, string? name) => new(Enabled: true, TraceOn: traceOn, name);

    /// <summary>
    /// The removal's drop, Azure: this install's session in each listed database but master, and not in a database where
    /// another registration of the logical server keeps it. The legacy session's one-time drop is not the removal's.
    /// </summary>
    [Fact]
    public async Task Removal_Azure_DropsThisInstallsSessionEverywhere_ExceptWhereAnotherRegistrationKeepsIt()
    {
        var rig = BuildRig();
        /* A second registration of the logical server, with its trace on, keeps the session in the one database it does not exclude. */
        var other = Another(Host, "alpha", "gamma");
        rig.Remaining.Add(other);
        rig.TraceOn.Add(other.ServerId);
        Assert.NotEqual(rig.Config.ServerId, other.ServerId);

        await rig.RemoveAsync();

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
        Assert.DoesNotContain(rig.Calls, c => c.Create);
        Assert.Equal(rig.Calls.Count, rig.Names.Count);
        Assert.All(rig.Names, name => Assert.Equal(OwnSession, name));
        Assert.Empty(rig.LegacyCalls);
        Assert.Equal(0, rig.Records.Reads);
        Assert.Equal(0, rig.Records.Writes);
    }

    /// <summary>With no other registration, every listed database but master loses the session.</summary>
    [Fact]
    public async Task Removal_Azure_DropsEveryListedDatabaseButMaster_WhenNoOtherRegistrationKeepsIt()
    {
        var rig = BuildRig();

        await rig.RemoveAsync();

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task Removal_OnPremises_DropsThisInstallsServerScopedSession_Once()
    {
        var rig = BuildRig(azureSqlDatabase: false);

        await rig.RemoveAsync();

        var drop = Assert.Single(rig.Calls);
        Assert.False(drop.Create);
        Assert.Equal(string.Empty, drop.Database);
        Assert.Equal(OwnSession, Assert.Single(rig.Names));
        Assert.Empty(rig.LegacyCalls);
        Assert.Equal(0, rig.Records.Reads);
    }

    [Fact]
    public async Task Removal_OnPremises_LeavesTheSession_WhileAnotherRegistrationOfTheSameInstanceKeepsIt_AndSaysSo()
    {
        var rig = BuildRig(azureSqlDatabase: false);
        rig.Guard = new LongQueryTraceInstanceGuard("sql01", new[] { Instance(traceOn: true, "SQL01") });

        await rig.RemoveAsync();

        Assert.Empty(rig.Calls);
        Assert.Contains(rig.Logger.Entries, e =>
            e.Level == LogLevel.Information
            && e.Message.Contains(rig.Config.DisplayName, StringComparison.Ordinal)
            && e.Message.Contains("keeps it on the same instance", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Removal_OnPremises_WithNoNameKnown_LeavesTheSession_WhenAnotherRegistrationCouldKeepIt_AndSaysWhy()
    {
        var rig = BuildRig(azureSqlDatabase: false);
        rig.Guard = new LongQueryTraceInstanceGuard(null, new[] { Instance(traceOn: true, "sql01") });

        await rig.RemoveAsync();

        Assert.Empty(rig.Calls);
        Assert.Contains(rig.Logger.Entries, e =>
            e.Level == LogLevel.Information
            && e.Message.Contains(rig.Config.DisplayName, StringComparison.Ordinal)
            && e.Message.Contains("instance name is not known", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Removal_OnPremises_WithNoNameKnown_StillDrops_WhenNoOtherRegistrationCouldKeepIt()
    {
        var rig = BuildRig(azureSqlDatabase: false);
        rig.Guard = new LongQueryTraceInstanceGuard(null, new[] { Instance(traceOn: false, "sql01") });

        await rig.RemoveAsync();

        Assert.Single(rig.Calls, c => !c.Create);
    }

    [Fact]
    public async Task Removal_OnPremises_DropsTheSession_WhenTheOtherRegistrationOfTheInstanceHasItsTraceOff()
    {
        var rig = BuildRig(azureSqlDatabase: false);
        rig.Guard = new LongQueryTraceInstanceGuard("sql01", new[] { Instance(traceOn: false, "sql01") });

        await rig.RemoveAsync();

        Assert.Single(rig.Calls, c => !c.Create);
    }

    /// <summary>A server whose last finished reconcile dropped the session has none to remove, so the removal opens nothing on it.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Removal_OfAServerWhoseTraceIsOff_TouchesNothing(bool azureSqlDatabase)
    {
        var rig = BuildRig(azureSqlDatabase);
        rig.TraceApplied = false;
        var guardAsked = false;

        await DarlingRemovedServerSessions.DropAsync(
            rig.Removed, rig.Runner, rig.Remaining, _ => false, _ => { guardAsked = true; return Task.FromResult(rig.Guard); }, rig.Logger, CancellationToken.None);

        Assert.Empty(rig.Calls);
        Assert.Equal(0, rig.ListCalls);
        Assert.False(guardAsked);
        Assert.Empty(rig.Statements);
    }

    /// <summary>One attempt, and nothing it hits stops the removal: a refused drop, a token that has run out, and a drop that hangs past the timeout all return.</summary>
    [Fact]
    public async Task Removal_ADropThatFails_OrTimesOut_NeverThrows_AndTriesOnce()
    {
        var rig = BuildRig();
        rig.Refuse.Add("beta");

        await rig.RemoveAsync();

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
        Assert.Contains(rig.Logger.Entries, e =>
            e.Level == LogLevel.Warning
            && e.Message.Contains(rig.Config.DisplayName, StringComparison.Ordinal)
            && e.Message.Contains("removed", StringComparison.OrdinalIgnoreCase));

        rig.Calls.Clear();
        using var spent = new CancellationTokenSource();
        spent.Cancel();
        await rig.RemoveAsync(spent.Token);
        Assert.Empty(rig.Calls);

        rig.Calls.Clear();
        rig.Refuse.Clear();
        rig.Logger.Entries.Clear();
        rig.Hang = token => Task.Delay(Timeout.Infinite, token);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await rig.RemoveAsync(timeout.Token);

        Assert.Single(rig.Calls);
        Assert.Contains(rig.Logger.Entries, e =>
            e.Level == LogLevel.Warning
            && e.Message.Contains(rig.Config.DisplayName, StringComparison.Ordinal)
            && e.Message.Contains("not dropped within", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Removal_WithNoInstallId_DropsNothing()
    {
        var rig = BuildRig(installId: null);

        await rig.RemoveAsync();

        Assert.Empty(rig.Calls);
        Assert.Equal(0, rig.ListCalls);
        Assert.Empty(rig.Statements);
    }

    /// <summary>
    /// The deadlock and blocked-process sessions this install chose for itself in the removed server's databases are dropped
    /// by their own names, except in a database where another registration of the same server reads its own session. A
    /// database where the install reads the shared session is left alone: the shared session is never dropped. The server's
    /// choices are forgotten.
    /// </summary>
    [Fact]
    public async Task Removal_Azure_DropsTheDeadlockAndBlockedProcessSessionsThisInstallChose_NeverTheSharedOnes()
    {
        var rig = BuildRig();
        rig.TraceApplied = false;
        var other = Another(Host);
        rig.Remaining.Add(other);
        var choices = rig.Runner.AlwaysOnChoices;
        var key = KeyOf(rig.Config);
        choices.Set(key, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(key, "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(key, "beta", AlwaysOnXeSessionKind.BlockedProcess, AlwaysOnXeChoice.Own);
        choices.Set(key, "gamma", AlwaysOnXeSessionKind.BlockedProcess, AlwaysOnXeChoice.Shared);
        choices.Set(KeyOf(other), "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        await rig.RemoveAsync();

        Assert.Equal(new[] { Drop(rig, AlwaysOnXeSessionKind.Deadlock) }, rig.Statements["alpha"]);
        Assert.Equal(new[] { Drop(rig, AlwaysOnXeSessionKind.BlockedProcess) }, rig.Statements["beta"]);
        Assert.False(rig.Statements.ContainsKey("gamma"));
        Assert.DoesNotContain(
            rig.Statements.Values.SelectMany(s => s),
            statement => statement.Contains("[" + AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock) + "]", StringComparison.Ordinal)
                || statement.Contains("[" + AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.BlockedProcess) + "]", StringComparison.Ordinal));
        Assert.Empty(choices.OwnDatabases(key, AlwaysOnXeSessionKind.Deadlock));
        Assert.Empty(choices.OwnDatabases(key, AlwaysOnXeSessionKind.BlockedProcess));

        /* The other registration's own choice is not the removal's to forget. */
        Assert.Single(choices.OwnDatabases(KeyOf(other), AlwaysOnXeSessionKind.Deadlock), d => string.Equals(d, "beta", StringComparison.OrdinalIgnoreCase));

        /* Trace off: the long-query session was not touched. */
        Assert.Empty(rig.Calls);
    }

    /// <summary>
    /// A database of the same name on another logical server is another database, so it holds nothing back: only another
    /// registration of the removed server's own logical server reads its own session in a database of this server. The
    /// host compares ignoring case and padding.
    /// </summary>
    [Fact]
    public async Task Removal_Azure_TheFallbackIsHeldBackOnlyByARegistrationOfTheSameServer_NotByADatabaseOfTheSameNameElsewhere()
    {
        var rig = BuildRig();
        rig.TraceApplied = false;
        var elsewhere = Another("lqremoval-other.database.windows.net");
        var sameServer = Another("  " + Host.ToUpperInvariant() + " ");
        rig.Remaining.Add(elsewhere);
        rig.Remaining.Add(sameServer);
        Assert.NotEqual(elsewhere.ServerId, sameServer.ServerId);
        var choices = rig.Runner.AlwaysOnChoices;
        var key = KeyOf(rig.Config);
        choices.Set(key, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(key, "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(KeyOf(elsewhere), "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(KeyOf(sameServer), "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        await rig.RemoveAsync();

        Assert.False(rig.Statements.ContainsKey("alpha"));
        Assert.Equal(new[] { Drop(rig, AlwaysOnXeSessionKind.Deadlock) }, rig.Statements["beta"]);
    }

    /// <summary>A failed fallback drop is logged and never stops the removal, and the next database is still tried.</summary>
    [Fact]
    public async Task Removal_Azure_AFallbackDropThatFails_IsLogged_AndTheNextDatabaseIsStillDropped()
    {
        var rig = BuildRig();
        rig.TraceApplied = false;
        var choices = rig.Runner.AlwaysOnChoices;
        var key = KeyOf(rig.Config);
        choices.Set(key, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(key, "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        rig.Runner.AlwaysOnXeDatabaseForTests = (_, database, _) =>
        {
            if (string.Equals(database, "alpha", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("The connection was refused.");
            }

            return Task.FromResult<IAlwaysOnXeDatabase>(new RecordingDatabase(rig.Statements[database] = new List<string>()));
        };

        await rig.RemoveAsync();

        Assert.Single(rig.Statements["beta"]);
        Assert.Contains(rig.Logger.Entries, e =>
            e.Level == LogLevel.Warning
            && e.Message.Contains("alpha", StringComparison.Ordinal)
            && e.Message.Contains("removed", StringComparison.OrdinalIgnoreCase));
        Assert.Empty(choices.OwnDatabases(key, AlwaysOnXeSessionKind.Deadlock));
    }

    /* ── what the registry forgets: the capture ── */

    private static DarlingWorker.ServerLoopState StateOf(Rig rig, bool? applied, bool withRuntime = true) =>
        new() { Config = rig.Config, Runtime = withRuntime ? rig.Runtime : null, LongQueryTraceApplied = applied };

    /// <summary>
    /// The reconcile forgets a server in one synchronous step, so what the drop needs is taken first: the definition, the
    /// runtime and the long-query latch of each server that is no longer wanted. A server that stays is not captured.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(true)]
    public void Capture_TakesTheServerNoLongerWanted_WithItsDefinitionRuntimeAndLatch(bool? applied)
    {
        var rig = BuildRig();
        var kept = Another(Host);
        var keptState = new DarlingWorker.ServerLoopState { Config = kept, Runtime = rig.Runtime, LongQueryTraceApplied = true };

        var captured = DarlingRemovedServerSessions.Capture(new[] { StateOf(rig, applied), keptState }, new[] { kept }, rig.Runner);

        var removed = Assert.Single(captured);
        Assert.Same(rig.Config, removed.Config);
        Assert.Same(rig.Runtime, removed.Runtime);
        Assert.Equal(applied, removed.LongQueryTraceApplied);
    }

    /// <summary>A server with no runtime has nothing to connect with, and a server whose trace was confirmed dropped has no session to drop.</summary>
    [Fact]
    public void Capture_SkipsAServerWithNoRuntime_AndAServerWhoseTraceWasConfirmedDropped()
    {
        var rig = BuildRig();

        Assert.Empty(DarlingRemovedServerSessions.Capture(new[] { StateOf(rig, applied: true, withRuntime: false) }, Array.Empty<MonitoredServer>(), rig.Runner));
        Assert.Empty(DarlingRemovedServerSessions.Capture(new[] { StateOf(rig, applied: false) }, Array.Empty<MonitoredServer>(), rig.Runner));
    }

    /// <summary>The fallbacks do not depend on the trace: a server whose trace is off but whose database reads this install's own session is still captured.</summary>
    [Fact]
    public void Capture_TakesAServerWhoseTraceIsOff_WhenItHoldsAnOwnFallbackSession()
    {
        var rig = BuildRig();
        rig.Runner.AlwaysOnChoices.Set(KeyOf(rig.Config), "alpha", AlwaysOnXeSessionKind.BlockedProcess, AlwaysOnXeChoice.Own);

        var removed = Assert.Single(DarlingRemovedServerSessions.Capture(new[] { StateOf(rig, applied: false) }, Array.Empty<MonitoredServer>(), rig.Runner));

        Assert.False(removed.LongQueryTraceApplied);
    }

    /// <summary>
    /// The reload takes the capture inside the lock and before the reconcile forgets the server, and awaits the drop after the
    /// lock, with the runner the collection loop holds. Pinned in the source: the reload needs a store.
    /// </summary>
    [Fact]
    public void TheReload_CapturesBeforeTheReconcile_AndDropsAfterTheLock()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var start = code.IndexOf("private async Task<long?> ReloadFromStoreAsync(", StringComparison.Ordinal);
        Assert.True(start > 0, "the reload could not be located");
        var signature = code[start..code.IndexOf(')', start)];
        Assert.Contains("DarlingCollectorRunner runner", signature, StringComparison.Ordinal);

        var body = CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', start)));
        var lockAt = body.IndexOf("lock (_serversLock)", StringComparison.Ordinal);
        var captureAt = body.IndexOf("DarlingRemovedServerSessions.Capture(", StringComparison.Ordinal);
        var reconcileAt = body.IndexOf("ReconcileServers(servers, view.EnabledServers);", StringComparison.Ordinal);
        var dropAt = body.IndexOf("await DropRemovedServerSessionsAsync(", StringComparison.Ordinal);
        Assert.True(lockAt >= 0 && captureAt > lockAt && reconcileAt > captureAt, "the capture is taken inside the lock, before the reconcile");
        Assert.True(dropAt > reconcileAt, "the drop comes after the reconcile");
        Assert.Contains("}", body[reconcileAt..dropAt], StringComparison.Ordinal);
        Assert.Contains("runner", body[dropAt..body.IndexOf(';', dropAt)], StringComparison.Ordinal);
        Assert.Single(Regex.Matches(body, @"return null\s*;"));

        /* The loop passes the runner it built. */
        Assert.Matches(@"await ReloadFromStoreAsync\s*\([^)]*\brunner\b[^)]*\)", code);
    }

    /* ── no session after the drop ── */

    /// <summary>
    /// A removed server's sweep that was already running can reach the reconcile after the removal retired the server and
    /// dropped its session. It creates nothing then, and records nothing.
    /// </summary>
    [Fact]
    public async Task AReconcileOfARetiredServer_CreatesNothing_EvenWithItsRuntimeStillSet()
    {
        var rig = BuildRig();
        var state = StateOf(rig, applied: null);
        state.Retired = true;

        await DarlingWorker.ReconcileLongQueryTraceAsync(
            state, rig.Runner, enabled: true, Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(), DateTime.UtcNow, rig.Logger, CancellationToken.None);

        Assert.Empty(rig.Calls);
        Assert.Equal(0, rig.ListCalls);
        Assert.Null(state.LongQueryTraceApplied);

        /* The same call for a server that is not retired runs, so the check is what stopped it. */
        var live = StateOf(rig, applied: null);
        await DarlingWorker.ReconcileLongQueryTraceAsync(
            live, rig.Runner, enabled: true, Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(), DateTime.UtcNow, rig.Logger, CancellationToken.None);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Calls.Where(c => c.Create).Select(c => c.Database));
    }

    /// <summary>The always-on ensure creates the deadlock and blocked-process sessions, so it leaves a retired server alone.</summary>
    [Fact]
    public async Task TheAlwaysOnEnsure_OfARetiredServer_CreatesNothing()
    {
        var rig = BuildRig();
        var ensured = 0;
        rig.Runner.XeEnsureOverrideForTests = (_, _) =>
        {
            ensured++;
            return Task.CompletedTask;
        };
        var state = StateOf(rig, applied: null);
        state.Retired = true;

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(state, rig.Runner, DateTime.UtcNow, rig.Logger, CancellationToken.None);

        Assert.Equal(0, ensured);

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(StateOf(rig, applied: null), rig.Runner, DateTime.UtcNow, rig.Logger, CancellationToken.None);
        Assert.Equal(1, ensured);
    }
}
