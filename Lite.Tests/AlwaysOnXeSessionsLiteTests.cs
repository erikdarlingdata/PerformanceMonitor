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
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The deadlock and blocked-process sessions on Azure SQL Database (#4961). Two installs share one session name per capture,
/// so the ensure never drops a session it did not make: a shared session that is stopped gets a start by name, one that is
/// still unusable after that (no deadlock event, or not running) sends the install to its own session, and the read takes
/// the name the ensure chose. The decisions run against a scripted database, so no server is needed.
/// </summary>
[Collection("app-logger-statics")]
public sealed class AlwaysOnXeSessionsLiteTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];

    private const string InstallIdValue = "0a1b2c3d";

    private readonly string _tempDir;
    private readonly string _configDir;
    private readonly string _dbPath;

    public AlwaysOnXeSessionsLiteTests()
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

    private static string Own(AlwaysOnXeSessionKind kind) =>
        AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, InstallIdValue, kind);

    /// <summary>The engine's "already exists" and "already started", as the scripted database raises them.</summary>
    private sealed class AlreadyThere : Exception
    {
        public AlreadyThere() : base("already there") { }
    }

    /// <summary>One Azure SQL Database as the ensure sees it: a catalog, a started-sessions DMV, and the statements sent.</summary>
    private sealed class ScriptedDatabase : IAlwaysOnXeDatabase
    {
        public Dictionary<string, bool> Started { get; } = new(StringComparer.Ordinal);
        public HashSet<string> WrongEvent { get; } = new(StringComparer.Ordinal);
        public HashSet<string> HiddenFromCatalog { get; } = new(StringComparer.Ordinal);
        public HashSet<string> HiddenFromDmv { get; } = new(StringComparer.Ordinal);
        public List<string> Statements { get; } = new();
        public Exception? CreateFailure { get; set; }
        public Exception? StartFailure { get; set; }

        public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken) =>
            Task.FromResult(
                !Started.ContainsKey(sessionName) || HiddenFromCatalog.Contains(sessionName) ? AlwaysOnXeCatalog.Missing
                : WrongEvent.Contains(sessionName) && kind == AlwaysOnXeSessionKind.Deadlock ? AlwaysOnXeCatalog.WrongEvent
                : AlwaysOnXeCatalog.Present);

        public Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken) =>
            Task.FromResult(Started.TryGetValue(sessionName, out var started) && started && !HiddenFromDmv.Contains(sessionName));

        public Task ExecuteAsync(string statement, CancellationToken cancellationToken)
        {
            Statements.Add(statement);
            var name = statement[(statement.IndexOf('[') + 1)..statement.IndexOf(']')];

            if (statement.TrimStart().StartsWith("CREATE EVENT SESSION", StringComparison.Ordinal))
            {
                if (CreateFailure is not null)
                {
                    throw CreateFailure;
                }

                if (Started.ContainsKey(name))
                {
                    throw new AlreadyThere();
                }

                Started[name] = true;
            }
            else if (statement.StartsWith("ALTER EVENT SESSION", StringComparison.Ordinal))
            {
                if (StartFailure is not null)
                {
                    throw StartFailure;
                }

                if (Started.TryGetValue(name, out var already) && already)
                {
                    throw new AlreadyThere();
                }

                Started[name] = true;
            }
            else if (statement.StartsWith("DROP EVENT SESSION", StringComparison.Ordinal))
            {
                Started.Remove(name);
            }

            return Task.CompletedTask;
        }

        public bool IsAlreadyPresent(Exception exception) => exception is AlreadyThere;

        public bool Sent(string prefix, string name) =>
            Statements.Any(s => s.TrimStart().StartsWith(prefix + " [" + name + "]", StringComparison.Ordinal));
    }

    private static Task<AlwaysOnXeAzureResult> RunAsync(ScriptedDatabase database, AlwaysOnXeSessionKind kind, AlwaysOnXeChoice current) =>
        AlwaysOnXeAzureEnsure.RunAsync(database, kind, Own(kind), current, CancellationToken.None);

    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task NoSessionAnywhere_CreatesTheSharedSession_StartedWithTheDatabase(AlwaysOnXeSessionKind kind)
    {
        var database = new ScriptedDatabase();

        var result = await RunAsync(database, kind, AlwaysOnXeChoice.Shared);

        Assert.Equal(AlwaysOnXeChoice.Shared, result.Choice);
        Assert.Equal(AlwaysOnXeChange.Created, result.Change);
        var create = Assert.Single(database.Statements);
        Assert.Contains("STARTUP_STATE = ON", create, StringComparison.Ordinal);
        Assert.Contains("CREATE EVENT SESSION [" + AlwaysOnXeSessions.SharedNameFor(kind) + "]", create, StringComparison.Ordinal);
    }

    /* Test 15: no automatic path drops a shared-name session. */
    [Fact]
    public async Task DeadlockSharedSessionWithoutTheDeadlockEvent_IsNeverDropped_AndTheInstallFallsBack()
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        var database = new ScriptedDatabase();
        database.Started[shared] = true;
        database.WrongEvent.Add(shared);

        var result = await RunAsync(database, AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Shared);

        Assert.Equal(AlwaysOnXeChoice.Own, result.Choice);
        Assert.Equal(AlwaysOnXeChange.FellBack, result.Change);
        Assert.False(database.Sent("DROP EVENT SESSION", shared));
        Assert.True(database.Started.ContainsKey(shared));
        var create = Assert.Single(database.Statements, s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal));
        Assert.Contains("[" + Own(AlwaysOnXeSessionKind.Deadlock) + "]", create, StringComparison.Ordinal);
        Assert.Contains("STARTUP_STATE = OFF", create, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task SharedSessionTheLoginCannotSee_IsStartedByName_ThenNeverDropped_AndTheInstallFallsBack(AlwaysOnXeSessionKind kind)
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(kind);
        var database = new ScriptedDatabase();
        database.Started[shared] = true;
        database.HiddenFromCatalog.Add(shared);
        database.HiddenFromDmv.Add(shared);

        var result = await RunAsync(database, kind, AlwaysOnXeChoice.Shared);

        Assert.Equal(AlwaysOnXeChoice.Own, result.Choice);
        Assert.True(database.Sent("ALTER EVENT SESSION", shared), "the ensure starts the shared session by name before it falls back");
        Assert.False(database.Sent("DROP EVENT SESSION", shared));
        Assert.True(database.Sent("CREATE EVENT SESSION", Own(kind)));
    }

    /* Test 16: a stopped shared session gets a START by name and no fallback. */
    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task StoppedSharedSession_GetsAStartByName_AndNoFallback(AlwaysOnXeSessionKind kind)
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(kind);
        var database = new ScriptedDatabase();
        database.Started[shared] = false;

        var result = await RunAsync(database, kind, AlwaysOnXeChoice.Shared);

        Assert.Equal(AlwaysOnXeChoice.Shared, result.Choice);
        Assert.Equal(AlwaysOnXeChange.Started, result.Change);
        Assert.Equal("ALTER EVENT SESSION [" + shared + "] ON DATABASE STATE = START;", Assert.Single(database.Statements));
        Assert.True(database.Started[shared]);
    }

    /* Test 18: a shared session that reads back usable makes the install drop its fallback and switch back. */
    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task InstallOnItsOwnSession_DropsItAndSwitchesBack_WhenTheSharedSessionReadsBackUsable(AlwaysOnXeSessionKind kind)
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(kind);
        var database = new ScriptedDatabase();
        database.Started[shared] = true;
        database.Started[Own(kind)] = true;

        var result = await RunAsync(database, kind, AlwaysOnXeChoice.Own);

        Assert.Equal(AlwaysOnXeChoice.Shared, result.Choice);
        Assert.Equal(AlwaysOnXeChange.SwitchedBack, result.Change);
        Assert.Equal("DROP EVENT SESSION [" + Own(kind) + "] ON DATABASE;", Assert.Single(database.Statements));
        Assert.True(database.Started.ContainsKey(shared));
    }

    [Fact]
    public async Task InstallOnItsOwnSession_CreatesTheSharedSessionWhenItIsGone_ThenSwitchesBack()
    {
        var kind = AlwaysOnXeSessionKind.BlockedProcess;
        var database = new ScriptedDatabase();
        database.Started[Own(kind)] = true;

        var result = await RunAsync(database, kind, AlwaysOnXeChoice.Own);

        Assert.Equal(AlwaysOnXeChoice.Shared, result.Choice);
        Assert.True(database.Started[AlwaysOnXeSessions.SharedNameFor(kind)]);
        Assert.False(database.Started.ContainsKey(Own(kind)));
    }

    [Fact]
    public async Task InstallOnItsOwnSession_KeepsItWhileTheSharedSessionStaysUnusable_AndDropsNothing()
    {
        var kind = AlwaysOnXeSessionKind.Deadlock;
        var shared = AlwaysOnXeSessions.SharedNameFor(kind);
        var database = new ScriptedDatabase();
        database.Started[shared] = true;
        database.WrongEvent.Add(shared);
        database.Started[Own(kind)] = true;

        var result = await RunAsync(database, kind, AlwaysOnXeChoice.Own);

        Assert.Equal(AlwaysOnXeChoice.Own, result.Choice);
        Assert.DoesNotContain(database.Statements, s => s.StartsWith("DROP", StringComparison.Ordinal));
    }

    [Fact]
    public void TheOnlyDropBuilderRefusesTheSharedName_AndAnotherInstallsOwnName()
    {
        var kind = AlwaysOnXeSessionKind.Deadlock;

        Assert.Throws<ArgumentException>(() => AlwaysOnXeSessions.BuildAzureDropSql(kind, AlwaysOnXeSessions.SharedNameFor(kind)));
        Assert.Throws<ArgumentException>(() => AlwaysOnXeSessions.BuildAzureDropSql(kind, "PerformanceMonitor_Lite_zzzzzzzz_Deadlock"));
        Assert.Equal("DROP EVENT SESSION [" + Own(kind) + "] ON DATABASE;", AlwaysOnXeSessions.BuildAzureDropSql(kind, Own(kind)));
    }

    [Fact]
    public void OwnName_IsMadeFromTheProductTheIdAndTheSharedSuffix()
    {
        Assert.Equal("PerformanceMonitor_Lite_0a1b2c3d_Deadlock", Own(AlwaysOnXeSessionKind.Deadlock));
        Assert.Equal("PerformanceMonitor_Darling_0a1b2c3d_BlockedProcess",
            AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, AlwaysOnXeSessionKind.BlockedProcess));
        Assert.Null(AlwaysOnXeSessions.TryOwnNameFor(LongQueryCompletionsCollector.LiteProduct, null, AlwaysOnXeSessionKind.Deadlock));
        Assert.Throws<ArgumentException>(() => AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, "NOTHEX!!", AlwaysOnXeSessionKind.Deadlock));
    }

    /* Test 27: an Azure CREATE or START that fails carries the caps sentence. */
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task FailedCreateOrStart_CarriesTheCapsSentence(bool create)
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        var database = new ScriptedDatabase();
        if (create)
        {
            database.CreateFailure = new InvalidOperationException("The create was refused.");
        }
        else
        {
            database.Started[shared] = false;
            database.StartFailure = new InvalidOperationException("The start was refused.");
        }

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            () => RunAsync(database, AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Shared));

        Assert.Contains(AlwaysOnXeSessions.AzureCapsSentence, AlwaysOnXeSessions.DescribeFailure(failure), StringComparison.Ordinal);
        Assert.Contains("100 started event sessions", AlwaysOnXeSessions.AzureCapsSentence, StringComparison.Ordinal);
        Assert.Contains("512 MB per pool", AlwaysOnXeSessions.AzureCapsSentence, StringComparison.Ordinal);
    }

    /* Test 17: the read uses the name the ensure chose. */
    [Fact]
    public void TheDeadlockReadNamesTheSessionTheEnsureChose_AndTheSharedNameWithNoChoice()
    {
        var own = Own(AlwaysOnXeSessionKind.Deadlock);
        var ownQuery = DeadlocksCollector.Instance.BuildQuery(AzureContext(own));
        var sharedQuery = DeadlocksCollector.Instance.BuildQuery(AzureContext(null));

        Assert.Contains($"N'{own}'", ownQuery.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("N'PerformanceMonitor_Deadlock'", ownQuery.Text, StringComparison.Ordinal);
        Assert.Contains("N'PerformanceMonitor_Deadlock'", sharedQuery.Text, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => DeadlocksCollector.Instance.BuildQuery(AzureContext("PerformanceMonitor_Other")));
    }

    [Fact]
    public void TheBlockedProcessReadNamesTheSessionTheEnsureChose_AndTheSharedNameWithNoChoice()
    {
        var own = Own(AlwaysOnXeSessionKind.BlockedProcess);
        var ownQuery = BlockedProcessReportCollector.Instance.BuildQuery(AzureContext(own));
        var sharedQuery = BlockedProcessReportCollector.Instance.BuildQuery(AzureContext(null));

        Assert.Contains($"N'{own}'", ownQuery.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("N'PerformanceMonitor_BlockedProcess'", ownQuery.Text, StringComparison.Ordinal);
        Assert.Contains("N'PerformanceMonitor_BlockedProcess'", sharedQuery.Text, StringComparison.Ordinal);
    }

    private static CollectorContext AzureContext(string? sessionName) => new()
    {
        ServerId = 1,
        ServerName = "alwayson",
        CollectionTime = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new DeltaCalculator(),
        Target = new CollectorTargetInfo { IsAzureSqlDb = true },
        CurrentDatabaseName = "alpha",
        AlwaysOnSessionName = sessionName,
    };

    private sealed class Rig
    {
        public required RemoteCollectorService Service { get; init; }
        public required ServerConnection Server { get; init; }
        public required ScriptedDatabase Alpha { get; init; }
        public required ScriptedDatabase Beta { get; init; }
    }

    private async Task<Rig> BuildRigAsync()
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        _initializers.Add(duckDb);
        await duckDb.InitializeAsync();

        var server = new ServerConnection { ServerName = "alwayson.database.windows.net", DisplayName = "alwayson-" + Guid.NewGuid().ToString("N")[..8] };
        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("deadlocks", enabled: true);
        schedules.UpdateSchedule("blocked_process_report", enabled: true);

        var service = new RemoteCollectorService(
            duckDb, servers, schedules, installIdStore: new InstallIdStore(_configDir, "test-machine", null));
        var alpha = new ScriptedDatabase();
        var beta = new ScriptedDatabase();

        service.XeSessionDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", "alpha", "beta" });
        service.AlwaysOnXeDatabaseForTests = (_, database, _) =>
            Task.FromResult<IAlwaysOnXeDatabase>(string.Equals(database, "alpha", StringComparison.OrdinalIgnoreCase) ? alpha : beta);

        return new Rig { Service = service, Server = server, Alpha = alpha, Beta = beta };
    }

    [Fact]
    public async Task TheEnsureOfADeadlockSession_ChoosesPerDatabase_AndTheReadTakesItsNameFromTheChoice()
    {
        var rig = await BuildRigAsync();
        var ownName = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, rig.Service.GetInstallId(), AlwaysOnXeSessionKind.Deadlock);
        var shared = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Started[shared] = true;
        rig.Alpha.WrongEvent.Add(shared);

        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);

        Assert.Equal(ownName,
            rig.Service.AlwaysOnReadSessionName(rig.Server, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.Equal(shared, rig.Service.AlwaysOnReadSessionName(rig.Server, "beta", AlwaysOnXeSessionKind.Deadlock));
        Assert.False(rig.Alpha.Sent("DROP EVENT SESSION", shared));
        Assert.True(rig.Beta.Started[shared]);

        /* The shared session is repaired from outside: the next cycle switches back and the read follows. */
        rig.Alpha.WrongEvent.Clear();
        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);

        Assert.Equal(shared, rig.Service.AlwaysOnReadSessionName(rig.Server, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.False(rig.Alpha.Started.ContainsKey(ownName));
    }

    [Fact]
    public async Task TheEnsureOfABlockedProcessSession_AStoppedSharedSessionIsStartedByName_NotReplaced()
    {
        var rig = await BuildRigAsync();
        var shared = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.BlockedProcess);
        rig.Alpha.Started[shared] = false;
        rig.Beta.Started[shared] = false;

        await rig.Service.EnsureBlockedProcessXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);

        Assert.True(rig.Alpha.Started[shared]);
        Assert.True(rig.Beta.Started[shared]);
        Assert.Equal(shared, rig.Service.AlwaysOnReadSessionName(rig.Server, "alpha", AlwaysOnXeSessionKind.BlockedProcess));
        Assert.DoesNotContain(rig.Alpha.Statements, s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheEnsureThatFailsToCreateInEveryDatabase_RaisesAnErrorThatCarriesTheCapsSentence()
    {
        var rig = await BuildRigAsync();
        rig.Alpha.CreateFailure = SqlExceptionFactory.Create(1105, errorClass: 17, message: "The create was refused.");
        rig.Beta.CreateFailure = SqlExceptionFactory.Create(1105, errorClass: 17, message: "The create was refused.");

        var raised = await Assert.ThrowsAsync<XeSessionEnsureException>(
            () => rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None));

        Assert.Contains("The create was refused.", raised.Message, StringComparison.Ordinal);
        Assert.Contains(AlwaysOnXeSessions.AzureCapsSentence, raised.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A pass that cannot use the shared session and has no id to name its own throws one message. A host whose id store
    /// says why it has no id gets the cause and the retry in it, in the long-query fault's words; a host with no store gets
    /// the plain sentence.
    /// </summary>
    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task ASharedSessionThatCannotBeUsed_WithNoId_SaysWhyThereIsNone_AndThatTheNextCycleTriesAgain(AlwaysOnXeSessionKind kind)
    {
        var shared = AlwaysOnXeSessions.SharedNameFor(kind);
        var database = new ScriptedDatabase();
        database.Started[shared] = false;
        database.HiddenFromCatalog.Add(shared);
        database.HiddenFromDmv.Add(shared);
        database.CreateFailure = new AlreadyThere();
        const string cause = "the id file C:\\data\\install-id.json could not be read (IOException: in use)";

        var withCause = await Assert.ThrowsAsync<InvalidOperationException>(
            () => AlwaysOnXeAzureEnsure.RunAsync(database, kind, null, AlwaysOnXeChoice.Shared, CancellationToken.None, cause));
        var plain = await Assert.ThrowsAsync<InvalidOperationException>(
            () => AlwaysOnXeAzureEnsure.RunAsync(database, kind, null, AlwaysOnXeChoice.Shared, CancellationToken.None));

        Assert.Equal(
            $"The shared {shared} session cannot be used in this database, and this install has no id for now to name a session of its own, because {cause}. The next cycle tries again.",
            withCause.Message);
        Assert.Equal(
            $"The shared {shared} session cannot be used in this database, and this install has no id to name a session of its own.",
            plain.Message);
        Assert.Equal(plain.Message, AlwaysOnXeAzureEnsure.NoOwnSessionMessage(kind, null));
    }

    /// <summary>
    /// The service hands the pass its id store's reason: with the id file unreadable, the fault each database raises names
    /// the cause and says the next cycle tries again.
    /// </summary>
    [Fact]
    public async Task TheEnsureOfAnInstallWhoseIdFileCannotBeRead_RaisesAnErrorThatNamesTheCause()
    {
        var rig = await BuildRigAsync();
        Directory.CreateDirectory(Path.Combine(_configDir, InstallIdStore.FileName));
        var shared = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        foreach (var database in new[] { rig.Alpha, rig.Beta })
        {
            database.Started[shared] = true;
            database.WrongEvent.Add(shared);
        }

        var raised = await Assert.ThrowsAsync<InvalidOperationException>(
            () => rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None));

        Assert.Null(rig.Service.GetInstallId());
        Assert.Contains("this install has no id for now", raised.Message, StringComparison.Ordinal);
        Assert.Contains("could not be read", raised.Message, StringComparison.Ordinal);
        Assert.Contains("The next cycle tries again.", raised.Message, StringComparison.Ordinal);
    }
}
