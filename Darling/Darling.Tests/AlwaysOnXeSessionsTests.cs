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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The deadlock and blocked-process sessions on Azure SQL Database (#4961), as the service ensures them: a stopped shared
/// session gets a start by name, one that stays unusable sends the install to its own session, the install switches back
/// once the shared one reads back usable, no path drops a shared-name session, and the read takes the name the ensure chose.
/// The service ensures at connect and then once an hour, so a session dropped from outside comes back within the hour. Each
/// test drives the real ensure against a scripted database, so no server or store is needed.
/// </summary>
public sealed class AlwaysOnXeSessionsTests : IAsyncDisposable
{
    private const string Host = "alwayson.database.windows.net";
    private const string InstallIdValue = "0a1b2c3d";

    /* Never opened: the runner reaches the store only to write collected rows, and these tests collect nothing. */
    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private static string Own(AlwaysOnXeSessionKind kind) =>
        AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, kind);

    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    [InlineData(AlwaysOnXeSessionKind.Attention)]
    public void TheSharedSessionStartsWithTheDatabase_AndThisInstallsOwnFallbackSessionDoesNot(AlwaysOnXeSessionKind kind)
    {
        var shared = AlwaysOnXeSessions.BuildAzureCreateSql(kind, AlwaysOnXeSessions.SharedNameFor(kind));
        var own = AlwaysOnXeSessions.BuildAzureCreateSql(kind, Own(kind));

        Assert.Contains("STARTUP_STATE = ON", shared, StringComparison.Ordinal);
        Assert.DoesNotContain("STARTUP_STATE = OFF", shared, StringComparison.Ordinal);
        Assert.Contains("STARTUP_STATE = OFF", own, StringComparison.Ordinal);
        Assert.DoesNotContain("STARTUP_STATE = ON", own, StringComparison.Ordinal);
    }

    private sealed class AlreadyThere : Exception
    {
        public AlreadyThere() : base("already there") { }
    }

    /// <summary>One Azure SQL Database as the ensure sees it: a catalog, a started-sessions DMV, and the statements sent.</summary>
    private sealed class ScriptedDatabase : IAlwaysOnXeDatabase
    {
        public Dictionary<string, bool> Started { get; } = new(StringComparer.Ordinal);
        public HashSet<string> WrongEvent { get; } = new(StringComparer.Ordinal);
        public List<string> Statements { get; } = new();
        public Exception? CreateFailure { get; set; }

        /* #8146: a create whose text names max_events_limit is refused, as an engine that does not know the option would. */
        public bool RefuseLimit { get; set; }

        /* The engine error a refused limit raises: 25629 (the attribute does not exist) or 25713 (the value is invalid). */
        public int RefuseLimitNumber { get; set; } = 25629;

        /* Raised by a create that carries no max_events_limit, so the retry itself can fail. */
        public Exception? CreateWithoutLimitFailure { get; set; }

        /* #5640: a create whose text holds this is refused with error 25744, as Azure SQL Database refuses an action it lacks. */
        public string? RefuseActionText { get; set; }

        public IEnumerable<int> ErrorNumbers(Exception exception) => DarlingXeSessions.ErrorNumbersOf(exception);

        public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken) =>
            Task.FromResult(
                !Started.ContainsKey(sessionName) ? AlwaysOnXeCatalog.Missing
                : WrongEvent.Contains(sessionName) && kind is AlwaysOnXeSessionKind.Deadlock or AlwaysOnXeSessionKind.Attention ? AlwaysOnXeCatalog.WrongEvent
                : AlwaysOnXeCatalog.Present);

        public Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken) =>
            Task.FromResult(Started.TryGetValue(sessionName, out var started) && started);

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

                if (CreateWithoutLimitFailure is not null && !statement.Contains("max_events_limit", StringComparison.Ordinal))
                {
                    throw CreateWithoutLimitFailure;
                }

                if (RefuseLimit && statement.Contains("max_events_limit", StringComparison.Ordinal))
                {
                    throw LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(RefuseLimitNumber, 16, "The option max_events_limit is not supported.");
                }

                if (RefuseActionText is not null && statement.Contains(RefuseActionText, StringComparison.Ordinal))
                {
                    throw LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(25744, 16, "The action 'sqlserver.sql_text' is not available for Azure SQL Database.");
                }

                if (Started.ContainsKey(name))
                {
                    throw new AlreadyThere();
                }

                Started[name] = true;
            }
            else if (statement.StartsWith("ALTER EVENT SESSION", StringComparison.Ordinal))
            {
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

        public int Created(string name) =>
            Statements.Count(s => s.TrimStart().StartsWith("CREATE EVENT SESSION [" + name + "]", StringComparison.Ordinal));

        public bool Sent(string prefix, string name) =>
            Statements.Any(s => s.TrimStart().StartsWith(prefix + " [" + name + "]", StringComparison.Ordinal));
    }

    private sealed class Rig
    {
        public required DarlingCollectorRunner Runner { get; init; }
        public required ServerRuntime Runtime { get; init; }
        public required DarlingWorker.ServerLoopState State { get; init; }
        public ScriptedDatabase Alpha { get; } = new();
        public ScriptedDatabase Beta { get; } = new();
        public int ServerScopedEnsures { get; set; }
    }

    private Rig BuildRig(bool azureSqlDatabase = true)
    {
        var config = new MonitoredServer { Name = "alwayson", Host = Host };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{Host},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDatabase },
            StorageName = Host,
            ServerId = 4242,
        };
        var runner = new DarlingCollectorRunner(
            _store,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => new List<string>(),
            separatelyMonitoredDatabases: _ => new List<string>(),
            installId: () => InstallIdValue);

        var rig = new Rig
        {
            Runner = runner,
            Runtime = runtime,
            State = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime },
        };

        runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", "alpha", "beta" });
        runner.AlwaysOnXeDatabaseForTests = (_, database, _) =>
            Task.FromResult<IAlwaysOnXeDatabase>(string.Equals(database, "alpha", StringComparison.OrdinalIgnoreCase) ? rig.Alpha : rig.Beta);
        runner.XeEnsureOverrideForTests = (_, _) =>
        {
            rig.ServerScopedEnsures++;
            return Task.CompletedTask;
        };

        return rig;
    }

    private static Task EnsureAsync(Rig rig) =>
        DarlingXeSessions.EnsureAllAsync(rig.Runtime, rig.Runner, NullLogger.Instance, CancellationToken.None);

    /* #7639: with "Databases that get Extended Events sessions" set to one of the two databases, the ensure creates the deadlock and
       blocked process sessions there and sends nothing to the other. */
    [Fact]
    public async Task TheXeSessionDatabaseList_NarrowsTheEnsure_ToTheListedDatabases()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Runtime.Config.XeSessionDatabases = new List<string> { "ALPHA" };

        await EnsureAsync(rig);

        Assert.True(rig.Alpha.Started[AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock)]);
        Assert.True(rig.Alpha.Started[AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.BlockedProcess)]);
        Assert.Empty(rig.Beta.Statements);
    }

    /* #7639: a list that names only a database that is gone or renamed leaves nothing to create: no database is attempted, nothing is sent,
       and the ensure does not fail. */
    [Fact]
    public async Task AnXeSessionDatabaseList_NamingNoLiveDatabase_SendsNothingAndDoesNotFail()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Runtime.Config.XeSessionDatabases = new List<string> { "dropped_last_week" };

        await EnsureAsync(rig);

        Assert.Empty(rig.Alpha.Statements);
        Assert.Empty(rig.Beta.Statements);
    }

    /* #7639: a list that names no database the server has attempts nothing and refuses nothing. The deadlock and blocked process runs used
       to record SUCCESS with no rows and no reason, so each now carries a note that the list matches no database. An empty list (every
       database) and a list that names a live database carry no such note. The client timeout run (Lite's carries it too) is one of the three. */
    [Theory]
    [InlineData("dropped_last_week", true)]
    [InlineData("", false)]
    [InlineData("alpha", false)]
    public async Task AnXeSessionDatabaseList_MatchingNoDatabase_PutsANoteOnTheDeadlockBlockedProcessAndClientTimeoutRuns(string list, bool expectNote)
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Runtime.Config.XeSessionDatabases = list.Length == 0 ? new List<string>() : new List<string> { list };
        var outcome = new DarlingXeSessions.CaptureEnsureOutcome();

        await DarlingXeSessions.EnsureAllAsync(rig.Runtime, rig.Runner, NullLogger.Instance, CancellationToken.None, outcome: outcome);

        foreach (var note in new[] { outcome.DeadlockPartialNote, outcome.BlockedProcessPartialNote, outcome.AttentionPartialNote })
        {
            Assert.Equal(expectNote, note?.Contains("matches no database on this server", StringComparison.Ordinal) ?? false);
        }
    }

    /* #7639: the client timeout collector is switched off, so its session is not ensured and its run has nothing to say about the list. */
    [Fact]
    public async Task AnXeSessionDatabaseList_MatchingNoDatabase_PutsNoNoteOnTheClientTimeoutRun_WhenItsCollectorIsOff()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Runtime.Config.XeSessionDatabases = new List<string> { "dropped_last_week" };
        var outcome = new DarlingXeSessions.CaptureEnsureOutcome();

        await DarlingXeSessions.EnsureAllAsync(rig.Runtime, rig.Runner, NullLogger.Instance, CancellationToken.None, attentionEnabled: false, outcome: outcome);

        Assert.NotNull(outcome.DeadlockPartialNote);
        Assert.Null(outcome.AttentionPartialNote);
    }

    /* #7639: the worker merges the note onto a client timeout run through the same accessor the deadlock and blocked process runs use. */
    [Fact]
    public async Task TheClientTimeoutRun_CarriesTheListNote_AndAPartialRefusalNote_ThroughTheWorkersAccessor()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Runtime.Config.XeSessionDatabases = new List<string> { "dropped_last_week" };
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow, NullLogger.Instance, CancellationToken.None);
        Assert.Contains("matches no database on this server", DarlingWorker.CaptureEnsurePartialNoteFor("attention", rig.State), StringComparison.Ordinal);

        var partial = BuildRig();
        partial.Runner.XeEnsureOverrideForTests = null;
        partial.Alpha.CreateFailure = new InvalidOperationException("The create was refused.");
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(partial.State, partial.Runner, DateTime.UtcNow, NullLogger.Instance, CancellationToken.None);
        var note = DarlingWorker.CaptureEnsurePartialNoteFor("attention", partial.State);
        Assert.NotNull(note);
        Assert.Contains("alpha", note, StringComparison.Ordinal);
        Assert.DoesNotContain("beta", note, StringComparison.Ordinal);
        Assert.Null(DarlingWorker.CaptureEnsurePartialNoteFor("wait_stats", partial.State));
    }

    /* #7639: an empty list is how it worked before the setting: every database gets its sessions. */
    [Fact]
    public async Task AnEmptyXeSessionDatabaseList_EnsuresEveryDatabase()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;

        await EnsureAsync(rig);

        var deadlock = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        Assert.True(rig.Alpha.Started[deadlock]);
        Assert.True(rig.Beta.Started[deadlock]);
    }

    /* #7639: a database on both the excluded list and the XE list gets none. The test's listing stands in for the real one, which leaves the
       excluded databases out before the XE list narrows what is left. */
    [Fact]
    public async Task ADatabaseOnBothTheExcludedAndTheXeSessionList_GetsNoSession()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Runtime.Config.ExcludedDatabases = new List<string> { "beta" };
        rig.Runtime.Config.XeSessionDatabases = new List<string> { "alpha", "beta" };
        rig.Runner.AzureDatabaseListOverrideForTests = (server, _) => Task.FromResult(
            new List<string> { "master", "alpha", "beta" }
                .Where(name => !server.Config.ExcludedDatabases.Contains(name, StringComparer.OrdinalIgnoreCase)).ToList());

        await EnsureAsync(rig);

        Assert.True(rig.Alpha.Started[AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock)]);
        Assert.Empty(rig.Beta.Statements);
    }

    private static DarlingWorker CreateReloadWorker()
    {
        var worker = (DarlingWorker)System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);
        return worker;
    }

    private static void Reload(DarlingWorker worker, DarlingWorker.ServerLoopState state, MonitoredServer desired) =>
        typeof(DarlingWorker).GetMethod("ReconcileServers", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .Invoke(worker, new object[] { new List<DarlingWorker.ServerLoopState> { state }, new List<MonitoredServer> { desired } });

    /* #7639: the ensure reads the list off the runtime's connect-time Config, which is init-only, so an edit made only to the list has to drop
       the runtime on the reload. The reconnect builds the new runtime from the reloaded definition, and its next ensure narrows to the new list. */
    [Fact]
    public async Task AnEditedXeSessionDatabaseList_DropsTheRuntimeOnReload_AndTheNextEnsureNarrows()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        await EnsureAsync(rig);
        var deadlock = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        Assert.True(rig.Alpha.Started[deadlock]);
        Assert.True(rig.Beta.Started[deadlock]);

        var edited = new MonitoredServer { Name = "alwayson", Host = Host, XeSessionDatabases = new List<string> { "Alpha" } };
        Reload(CreateReloadWorker(), rig.State, edited);

        Assert.Null(rig.State.Runtime);
        Assert.Same(edited, rig.State.Config);

        var reconnected = new ServerRuntime
        {
            Config = rig.State.Config,
            ConnectionString = rig.Runtime.ConnectionString,
            Target = rig.Runtime.Target,
            StorageName = rig.Runtime.StorageName,
            ServerId = rig.Runtime.ServerId,
        };
        var fresh = BuildRig();
        fresh.Runner.XeEnsureOverrideForTests = null;
        await DarlingXeSessions.EnsureAllAsync(reconnected, fresh.Runner, NullLogger.Instance, CancellationToken.None);

        Assert.True(fresh.Alpha.Started[deadlock]);
        Assert.Empty(fresh.Beta.Statements);
    }

    /* #7639: a reload that leaves the list as it was (any casing) keeps the runtime, so no server reconnects without cause. */
    [Fact]
    public void AnUnchangedXeSessionDatabaseList_KeepsTheRuntimeOnReload()
    {
        var rig = BuildRig();
        rig.State.Config.XeSessionDatabases = new List<string> { "alpha", "beta" };

        var same = new MonitoredServer { Name = "alwayson", Host = Host, XeSessionDatabases = new List<string> { "ALPHA", "Beta" } };
        Reload(CreateReloadWorker(), rig.State, same);

        Assert.Same(rig.Runtime, rig.State.Runtime);
    }

    /* Test 16: a stopped shared session gets a START by name and no fallback. */
    [Fact]
    public async Task AStoppedSharedSession_GetsAStartByName_AndNoFallback()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        var deadlock = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        var blocked = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.BlockedProcess);
        rig.Alpha.Started[deadlock] = false;
        rig.Alpha.Started[blocked] = false;

        await EnsureAsync(rig);

        Assert.True(rig.Alpha.Started[deadlock]);
        Assert.True(rig.Alpha.Started[blocked]);
        Assert.Equal(0, rig.Alpha.Created(Own(AlwaysOnXeSessionKind.Deadlock)));
        Assert.Equal(deadlock, rig.Runner.AlwaysOnReadSessionName(rig.Runtime, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.DoesNotContain(rig.Alpha.Statements, s => s.StartsWith("DROP", StringComparison.Ordinal));
    }

    /* Tests 15 and 17: a shared deadlock session without the deadlock event is never dropped, the install reads its own. */
    [Fact]
    public async Task ASharedDeadlockSessionWithoutTheDeadlockEvent_IsNeverDropped_AndTheReadTakesTheOwnName()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        var shared = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Started[shared] = true;
        rig.Alpha.WrongEvent.Add(shared);

        await EnsureAsync(rig);

        Assert.False(rig.Alpha.Sent("DROP EVENT SESSION", shared));
        Assert.Equal(Own(AlwaysOnXeSessionKind.Deadlock), rig.Runner.AlwaysOnReadSessionName(rig.Runtime, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.Equal(shared, rig.Runner.AlwaysOnReadSessionName(rig.Runtime, "beta", AlwaysOnXeSessionKind.Deadlock));
        Assert.Contains("STARTUP_STATE = OFF", rig.Alpha.Statements.Single(s => s.Contains("CREATE EVENT SESSION [" + Own(AlwaysOnXeSessionKind.Deadlock) + "]", StringComparison.Ordinal)), StringComparison.Ordinal);
    }

    /* Test 18: a shared session that reads back usable makes the install drop its fallback and switch back. */
    [Fact]
    public async Task TheInstallOnItsOwnSession_DropsItAndSwitchesBack_WhenTheSharedSessionReadsBackUsable()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        var shared = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Started[shared] = true;
        rig.Alpha.WrongEvent.Add(shared);
        await EnsureAsync(rig);
        Assert.True(rig.Alpha.Started.ContainsKey(Own(AlwaysOnXeSessionKind.Deadlock)));

        rig.Alpha.WrongEvent.Clear();
        await EnsureAsync(rig);

        Assert.False(rig.Alpha.Started.ContainsKey(Own(AlwaysOnXeSessionKind.Deadlock)));
        Assert.True(rig.Alpha.Sent("DROP EVENT SESSION", Own(AlwaysOnXeSessionKind.Deadlock)));
        Assert.False(rig.Alpha.Sent("DROP EVENT SESSION", shared));
        Assert.Equal(shared, rig.Runner.AlwaysOnReadSessionName(rig.Runtime, "alpha", AlwaysOnXeSessionKind.Deadlock));
    }

    /* Test 19, Azure: sessions dropped from outside come back within the hour, and not before it. */
    [Fact]
    public async Task AzureSessionsDroppedFromOutside_ComeBackWithinTheHour()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        var start = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var deadlock = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock);
        var blocked = AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.BlockedProcess);

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start, NullLogger.Instance, CancellationToken.None);
        Assert.True(rig.Alpha.Started[deadlock]);
        Assert.True(rig.Beta.Started[blocked]);

        rig.Alpha.Started.Clear();
        rig.Beta.Started.Clear();
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start.AddMinutes(30), NullLogger.Instance, CancellationToken.None);
        Assert.Empty(rig.Alpha.Started);

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start.AddMinutes(61), NullLogger.Instance, CancellationToken.None);
        Assert.True(rig.Alpha.Started[deadlock]);
        Assert.True(rig.Alpha.Started[blocked]);
        Assert.True(rig.Beta.Started[deadlock]);
        Assert.True(rig.Beta.Started[blocked]);
    }

    /* Test 19, server scope: the ensure runs once an hour as well as at connect. */
    [Fact]
    public async Task TheServerScopedEnsure_RunsAgainOnceTheHourIsUp()
    {
        var rig = BuildRig(azureSqlDatabase: false);
        var start = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start, NullLogger.Instance, CancellationToken.None);
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start.AddMinutes(59), NullLogger.Instance, CancellationToken.None);
        Assert.Equal(1, rig.ServerScopedEnsures);

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start.AddMinutes(60), NullLogger.Instance, CancellationToken.None);
        Assert.Equal(2, rig.ServerScopedEnsures);
        Assert.Equal(start.AddMinutes(60), rig.State.XeSessionsEnsuredAtUtc);
    }

    /* Test 27: an Azure create that fails carries the caps sentence in the line the service logs. */
    [Fact]
    public async Task AFailedAzureCreate_LogsTheCapsSentence()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Alpha.CreateFailure = new InvalidOperationException("The create was refused.");
        var logger = new CapturingLogger();

        await DarlingXeSessions.EnsureAllAsync(rig.Runtime, rig.Runner, logger, CancellationToken.None);

        var line = Assert.Single(logger.Lines, l => l.Contains("alpha", StringComparison.Ordinal) && l.Contains("deadlock", StringComparison.Ordinal));
        Assert.Contains("The create was refused.", line, StringComparison.Ordinal);
        Assert.Contains(AlwaysOnXeSessions.AzureCapsSentence, line, StringComparison.Ordinal);
    }

    /* Test 15, pinned at the source: the Azure ensure in this service holds no drop of a shared name. */
    [Fact]
    public void TheAzureEnsure_HoldsNoDropOfASharedName()
    {
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingXeSessions.cs");

        Assert.DoesNotContain("DROP EVENT SESSION [{DeadlocksCollector.XeSessionName}]", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DROP EVENT SESSION [{sessionName}] ON DATABASE;\", connection))\n                    {\n                        dropCmd.CommandTimeout = 60;", source, StringComparison.Ordinal);
    }

    /* Test 17: the read builders name the session the ensure chose. */
    [Fact]
    public void TheReadBuildersNameTheSessionTheEnsureChose()
    {
        var own = Own(AlwaysOnXeSessionKind.Deadlock);
        var ownBlocked = Own(AlwaysOnXeSessionKind.BlockedProcess);

        Assert.Contains($"N'{own}'", DeadlocksCollector.Instance.BuildQuery(AzureContext(own)).Text, StringComparison.Ordinal);
        Assert.DoesNotContain("N'PerformanceMonitor_Deadlock'", DeadlocksCollector.Instance.BuildQuery(AzureContext(own)).Text, StringComparison.Ordinal);
        Assert.Contains("N'PerformanceMonitor_Deadlock'", DeadlocksCollector.Instance.BuildQuery(AzureContext(null)).Text, StringComparison.Ordinal);
        Assert.Contains($"N'{ownBlocked}'", BlockedProcessReportCollector.Instance.BuildQuery(AzureContext(ownBlocked)).Text, StringComparison.Ordinal);
        Assert.DoesNotContain("N'PerformanceMonitor_BlockedProcess'", BlockedProcessReportCollector.Instance.BuildQuery(AzureContext(ownBlocked)).Text, StringComparison.Ordinal);
    }

    /* ── #5640 review round 1: H2 (a denied create is PERMISSIONS), L4 (definition probe), L5 (collector gate), L6 (Azure retry) ── */

    private static bool IsAttentionStatement(string s) => s.Contains(AlwaysOnXeSessions.AttentionXeSessionName, StringComparison.Ordinal);

    [Fact]
    public async Task H2_ACreateDeniedInEveryDatabase_LeavesAPermissionFault_ThatTheWorkerKeepsForTheRun()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Alpha.CreateFailure = LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 14, "User does not have permission to perform this action.");
        rig.Beta.CreateFailure = rig.Alpha.CreateFailure;

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow, NullLogger.Instance, CancellationToken.None);

        Assert.NotNull(rig.State.AttentionXePermissionFault);
        Assert.Contains("ALTER ANY EVENT SESSION", rig.State.AttentionXePermissionFault, StringComparison.Ordinal);
        /* The run raises the denied exception (PERMISSIONS) for the attention collector alone; Capture Down reads SESSION_MISSING only. */
        Assert.Equal(rig.State.AttentionXePermissionFault, DarlingWorker.AttentionPermissionFaultFor("attention", rig.State));
        Assert.Null(DarlingWorker.AttentionPermissionFaultFor("deadlocks", rig.State));
    }

    [Fact]
    public async Task H2_ADenialInSomeDatabases_AnOtherFailure_AndAGrant_LeaveNoFault_SoTheReadStaysSessionMissing()
    {
        var denial = LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 14, "denied");

        var some = BuildRig();
        some.Runner.XeEnsureOverrideForTests = null;
        some.Alpha.CreateFailure = denial;
        Assert.Null(await DarlingXeSessions.EnsureAllAsync(some.Runtime, some.Runner, NullLogger.Instance, CancellationToken.None));

        var other = BuildRig();
        other.Runner.XeEnsureOverrideForTests = null;
        other.Alpha.CreateFailure = new InvalidOperationException("The create was refused.");
        other.Beta.CreateFailure = other.Alpha.CreateFailure;
        Assert.Null(await DarlingXeSessions.EnsureAllAsync(other.Runtime, other.Runner, NullLogger.Instance, CancellationToken.None));

        /* A session dropped by hand on a login that may create one: recreated, no fault; a stale fault is replaced by the ensure. */
        var granted = BuildRig();
        granted.Runner.XeEnsureOverrideForTests = null;
        granted.State.AttentionXePermissionFault = "stale";
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(granted.State, granted.Runner, DateTime.UtcNow, NullLogger.Instance, CancellationToken.None);
        Assert.Null(granted.State.AttentionXePermissionFault);
        Assert.Null(DarlingWorker.AttentionPermissionFaultFor("attention", granted.State));
        Assert.Contains(granted.Alpha.Statements, IsAttentionStatement);
    }

    [Fact]
    public void H2_OnPremFaultClassification_ReadsThePermissionNumbers()
    {
        Assert.NotNull(DarlingXeSessions.AttentionFaultFor(LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 14, "denied")));
        Assert.NotNull(DarlingXeSessions.AttentionFaultFor(LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(262, 14, "denied")));
        Assert.Null(DarlingXeSessions.AttentionFaultFor(LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(1105, 17, "The filegroup is full.")));
        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("throw new DarlingXeSessionDeniedException(attentionFault);", source, StringComparison.Ordinal);
    }

    /* ── #6893: a refused deadlock or blocked-process create is carried to the run, never read as a quiet server ── */

    private static readonly Exception Denial = LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(15247, 14, "User does not have permission to perform this action.");

    [Fact]
    public async Task Deadlock_AndBlockedProcess_CreateDeniedInEveryDatabase_RecordPermissions_AndNoReadHappens()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Alpha.CreateFailure = Denial;
        rig.Beta.CreateFailure = Denial;

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow, NullLogger.Instance, CancellationToken.None);

        var deadlock = DarlingWorker.CaptureEnsureFaultFor("deadlocks", rig.State);
        var blocked = DarlingWorker.CaptureEnsureFaultFor("blocked_process_report", rig.State);
        Assert.NotNull(deadlock);
        Assert.NotNull(blocked);
        Assert.True(deadlock!.IsPermission);
        Assert.True(blocked!.IsPermission);
        Assert.Contains("ALTER ANY EVENT SESSION", deadlock.Message, StringComparison.Ordinal);
        Assert.Null(DarlingWorker.CaptureEnsureFaultFor("wait_stats", rig.State));
        Assert.Null(DarlingWorker.CaptureEnsurePartialNoteFor("deadlocks", rig.State));
    }

    [Fact]
    public async Task Deadlock_AndBlockedProcess_AnyOtherCreateFailureInEveryDatabase_RecordsSessionMissing()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Alpha.CreateFailure = new InvalidOperationException("The create was refused.");
        rig.Beta.CreateFailure = rig.Alpha.CreateFailure;
        var logger = new CapturingLogger();

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow, logger, CancellationToken.None);

        var deadlock = DarlingWorker.CaptureEnsureFaultFor("deadlocks", rig.State);
        Assert.NotNull(deadlock);
        Assert.False(deadlock!.IsPermission);
        Assert.Contains("The create was refused.", deadlock.Message, StringComparison.Ordinal);
        Assert.False(DarlingWorker.CaptureEnsureFaultFor("blocked_process_report", rig.State)!.IsPermission);
        Assert.DoesNotContain(logger.Lines, l => l.Contains("zero rows", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Deadlock_CreateRefusedInSomeDatabases_IsAPartialNote_NotAFault_AndALaterSuccessClearsBoth()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Alpha.CreateFailure = new InvalidOperationException("The create was refused.");

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow, NullLogger.Instance, CancellationToken.None);

        Assert.Null(DarlingWorker.CaptureEnsureFaultFor("deadlocks", rig.State));
        var note = DarlingWorker.CaptureEnsurePartialNoteFor("deadlocks", rig.State);
        Assert.NotNull(note);
        Assert.Contains("alpha", note, StringComparison.Ordinal);
        Assert.DoesNotContain("beta", note, StringComparison.Ordinal);

        /* The login is granted: the next ensure replaces what the last one left. */
        rig.Alpha.CreateFailure = null;
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow.AddHours(2), NullLogger.Instance, CancellationToken.None);
        Assert.Null(DarlingWorker.CaptureEnsureFaultFor("deadlocks", rig.State));
        Assert.Null(DarlingWorker.CaptureEnsurePartialNoteFor("deadlocks", rig.State));
    }

    [Fact]
    public async Task Deadlock_CreateDenied_ThenGranted_ClearsTheFault()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Alpha.CreateFailure = Denial;
        rig.Beta.CreateFailure = Denial;
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow, NullLogger.Instance, CancellationToken.None);
        Assert.NotNull(DarlingWorker.CaptureEnsureFaultFor("deadlocks", rig.State));

        rig.Alpha.CreateFailure = null;
        rig.Beta.CreateFailure = null;
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, DateTime.UtcNow.AddHours(2), NullLogger.Instance, CancellationToken.None);
        Assert.Null(DarlingWorker.CaptureEnsureFaultFor("deadlocks", rig.State));
        Assert.Null(DarlingWorker.CaptureEnsureFaultFor("blocked_process_report", rig.State));
    }

    [Fact]
    public void CaptureFaultFor_ClassifiesByThePermissionNumbers()
    {
        Assert.True(DarlingXeSessions.CaptureFaultFor(Denial, "deadlock").IsPermission);
        Assert.True(DarlingXeSessions.CaptureFaultFor(LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(262, 14, "denied"), "deadlock").IsPermission);
        Assert.False(DarlingXeSessions.CaptureFaultFor(LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(1105, 17, "The filegroup is full."), "deadlock").IsPermission);
    }

    [Fact]
    public void ATransientEnsureFailure_StoresNoFault_AndMarksTheOutcomeTransient()
    {
        foreach (var deadlock in new[] { true, false })
        {
            var outcome = new DarlingXeSessions.CaptureEnsureOutcome();
            var fault = DarlingXeSessions.RecordOnPremCaptureFault(
                outcome, deadlock, LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(-2, 11, "Execution Timeout Expired."));

            Assert.Null(fault);
            Assert.Null(outcome.Deadlock);
            Assert.Null(outcome.BlockedProcess);
            Assert.True(outcome.TransientFailure);
            Assert.Equal(deadlock, outcome.DeadlockTransient);
            Assert.Equal(!deadlock, outcome.BlockedProcessTransient);
        }
    }

    /* #7164: a transient ensure failure must not throw away the fault an earlier ensure stored. Per capture: the one that failed
       transiently keeps its old fault, the one that ensured cleanly keeps its own (new) result. */
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ATransientFailureOnOneCapture_KeepsItsOldFault_AndLeavesTheOtherCapturesNewResultInPlace(bool deadlockFailsTransiently)
    {
        var oldDeadlock = DarlingXeSessions.CaptureFaultFor(Denial, "deadlock");
        var oldBlocked = DarlingXeSessions.CaptureFaultFor(Denial, "blocked process");
        var previous = new DarlingXeSessions.CaptureEnsureOutcome { Deadlock = oldDeadlock, BlockedProcess = oldBlocked };

        /* The other capture's ensure succeeded this time: it has no fault, and the carry must not bring the old one back. */
        var next = new DarlingXeSessions.CaptureEnsureOutcome();
        DarlingXeSessions.RecordOnPremCaptureFault(
            next, deadlockFailsTransiently, LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(-2, 11, "Execution Timeout Expired."));
        next.CarryForwardTransient(previous);

        Assert.Same(deadlockFailsTransiently ? oldDeadlock : null, next.Deadlock);
        Assert.Same(deadlockFailsTransiently ? null : oldBlocked, next.BlockedProcess);
        Assert.True(next.TransientFailure);

        /* A clean ensure of both clears both: nothing is carried. */
        var clean = new DarlingXeSessions.CaptureEnsureOutcome();
        clean.CarryForwardTransient(previous);
        Assert.Null(clean.Deadlock);
        Assert.Null(clean.BlockedProcess);
    }

    /* #7164: the connection to the server would not open. That says nothing about the sessions, so the stored faults survive, the
       runs keep recording them, and the ensure is due again in five minutes and not an hour. */
    [Fact]
    public async Task AConnectionThatWillNotOpen_KeepsTheStoredFaults_AndIsDueAgainInFiveMinutes()
    {
        var rig = BuildRig(azureSqlDatabase: false);
        rig.Runner.XeEnsureOverrideForTests = null;
        var runtime = new ServerRuntime
        {
            Config = rig.Runtime.Config,
            ConnectionString = "Server=tcp:127.0.0.1,1;Initial Catalog=master;Encrypt=False;Connect Timeout=3;ConnectRetryCount=0",
            Target = new CollectorTargetInfo(),
            StorageName = rig.Runtime.StorageName,
            ServerId = rig.Runtime.ServerId,
        };
        rig.State.Runtime = runtime;
        rig.State.CaptureEnsure = new DarlingXeSessions.CaptureEnsureOutcome
        {
            Deadlock = DarlingXeSessions.CaptureFaultFor(Denial, "deadlock"),
            BlockedProcess = DarlingXeSessions.CaptureFaultFor(Denial, "blocked process"),
        };
        var now = DateTime.UtcNow;

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, now, NullLogger.Instance, CancellationToken.None);

        Assert.NotNull(DarlingWorker.CaptureEnsureFaultFor("deadlocks", rig.State));
        Assert.NotNull(DarlingWorker.CaptureEnsureFaultFor("blocked_process_report", rig.State));
        Assert.False(AlwaysOnXeSessions.EnsureIsDue(rig.State.XeSessionsEnsuredAtUtc, now.AddMinutes(4)));
        Assert.True(AlwaysOnXeSessions.EnsureIsDue(rig.State.XeSessionsEnsuredAtUtc, now.AddMinutes(5)));
    }

    [Theory]
    [InlineData(15247, 14, true)]
    [InlineData(1105, 17, false)]
    public void ANonTransientEnsureFailure_StillStoresTheFault(int number, byte severity, bool permission)
    {
        var ex = LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(number, severity, "failed");

        var deadlockOutcome = new DarlingXeSessions.CaptureEnsureOutcome();
        var deadlockFault = DarlingXeSessions.RecordOnPremCaptureFault(deadlockOutcome, deadlock: true, ex);
        Assert.NotNull(deadlockFault);
        Assert.Same(deadlockFault, deadlockOutcome.Deadlock);
        Assert.Null(deadlockOutcome.BlockedProcess);
        Assert.Equal(permission, deadlockFault.IsPermission);
        Assert.False(deadlockOutcome.TransientFailure);

        var bpOutcome = new DarlingXeSessions.CaptureEnsureOutcome();
        var bpFault = DarlingXeSessions.RecordOnPremCaptureFault(bpOutcome, deadlock: false, ex);
        Assert.Same(bpFault, bpOutcome.BlockedProcess);
        Assert.Null(bpOutcome.Deadlock);
    }

    [Fact]
    public void ATransientEnsureFailure_MakesTheNextEnsureDueInFiveMinutes_NotAnHour()
    {
        var rig = BuildRig(azureSqlDatabase: false);
        var now = DateTime.UtcNow;
        rig.State.XeSessionsEnsuredAtUtc = now;

        DarlingWorker.ApplyTransientEnsureRetry(rig.State, new DarlingXeSessions.CaptureEnsureOutcome(), now);
        Assert.False(AlwaysOnXeSessions.EnsureIsDue(rig.State.XeSessionsEnsuredAtUtc, now.AddMinutes(59)));

        DarlingWorker.ApplyTransientEnsureRetry(rig.State, new DarlingXeSessions.CaptureEnsureOutcome { DeadlockTransient = true }, now);
        Assert.False(AlwaysOnXeSessions.EnsureIsDue(rig.State.XeSessionsEnsuredAtUtc, now.AddMinutes(4)));
        Assert.True(AlwaysOnXeSessions.EnsureIsDue(rig.State.XeSessionsEnsuredAtUtc, now.AddMinutes(5)));
    }

    [Theory]
    [InlineData(true, true, false)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(false, false, true)]
    public async Task AnRdsWebOrExpressInstance_GetsNoEnsure_AndNoSessionIsTried(bool isAwsRds, bool webOrExpress, bool ensureRuns)
    {
        var rig = BuildRig(azureSqlDatabase: false);
        var runtime = new ServerRuntime
        {
            Config = rig.Runtime.Config,
            ConnectionString = rig.Runtime.ConnectionString,
            Target = new CollectorTargetInfo { IsAwsRds = isAwsRds, IsAwsRdsWebOrExpress = webOrExpress },
            StorageName = rig.Runtime.StorageName,
            ServerId = rig.Runtime.ServerId,
        };

        var attention = await DarlingXeSessions.EnsureAllAsync(runtime, rig.Runner, NullLogger.Instance, CancellationToken.None);

        Assert.Null(attention);
        Assert.Equal(ensureRuns ? 1 : 0, rig.ServerScopedEnsures);
        Assert.Empty(rig.Alpha.Statements);
    }

    [Fact]
    public async Task L5_ADisabledCollector_GetsNoSession_AndNothingIsDropped_AndEnablingItEnsuresAtOnce()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        var start = DateTime.UtcNow;

        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start, NullLogger.Instance, CancellationToken.None, attentionEnabled: false);
        Assert.DoesNotContain(rig.Alpha.Statements, IsAttentionStatement);
        Assert.Contains(rig.Alpha.Statements, s => s.Contains("PerformanceMonitor_Deadlock", StringComparison.Ordinal));
        Assert.DoesNotContain(rig.Alpha.Statements, s => s.StartsWith("DROP", StringComparison.Ordinal));

        /* Enabled within the hour: ensured at the next sweep, not after the hour. */
        await DarlingWorker.EnsureAlwaysOnXeSessionsAsync(rig.State, rig.Runner, start.AddMinutes(1), NullLogger.Instance, CancellationToken.None, attentionEnabled: true);
        Assert.Contains(rig.Alpha.Statements, IsAttentionStatement);

        var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        Assert.Contains("ResolveSchedule(\"attention\", server.Config.ServerId, _scheduleOverrides).Enabled", source, StringComparison.Ordinal);
    }

    [Fact]
    public async Task L6_ARefusedStatementTextAction_RetriesOnceWithoutIt_AndNotesIt()
    {
        var rig = BuildRig();
        rig.Runner.XeEnsureOverrideForTests = null;
        rig.Alpha.RefuseActionText = "sqlserver.sql_text";
        var logger = new CapturingLogger();

        await DarlingXeSessions.EnsureAllAsync(rig.Runtime, rig.Runner, logger, CancellationToken.None);

        var creates = rig.Alpha.Statements.Where(s => IsAttentionStatement(s) && s.TrimStart().StartsWith("CREATE", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, creates.Count);
        Assert.Contains("sqlserver.sql_text", creates[0], StringComparison.Ordinal);
        Assert.DoesNotContain("sqlserver.sql_text", creates[1], StringComparison.Ordinal);
        Assert.Contains("sqlserver.client_hostname", creates[1], StringComparison.Ordinal);
        Assert.Contains(logger.Lines, l => l.Contains("without statement text", StringComparison.Ordinal));
        Assert.True(rig.Alpha.Started[AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Attention)]);
    }

    /// <summary>
    /// #7639: the deadlock create was refused in every database of an Azure SQL Database server. The deadlock run still reads
    /// (the master item's durable telemetry arm needs no session), and only then records the failure the ensure carried, as
    /// PERMISSIONS or SESSION_MISSING. It used to throw before the read, so the telemetry deadlocks never reached the store.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ACarriedEnsureFailure_OnAzureSqlDatabase_DoesNotStopTheDeadlockRead_AndTheRunRecordsTheFailureAfterIt(bool permission)
    {
        var order = new List<string>();
        var fault = new DarlingXeSessions.XeEnsureFailure("The create was refused.", permission);

        var raised = await Assert.ThrowsAnyAsync<Exception>(() => DarlingWorker.RaiseCaptureEnsureFaultAsync(
            "deadlocks", isAzureSqlDb: true, fault, () => { order.Add("read"); return Task.CompletedTask; }, NullLogger.Instance, "azure"));

        Assert.Equal(new[] { "read" }, order);
        Assert.Equal(permission ? "DarlingXeSessionDeniedException" : "DarlingXeSessionMissingException", raised.GetType().Name);
        Assert.Contains("The create was refused.", raised.Message, StringComparison.Ordinal);
    }

    /// <summary>A read that fails after a failed ensure is dropped: the ensure's failure is the cause, and the one the run records.</summary>
    [Fact]
    public async Task ACarriedEnsureFailure_IsRecordedEvenWhenTheReadAfterItFails()
    {
        var fault = new DarlingXeSessions.XeEnsureFailure("The create was refused.", IsPermission: false);

        var raised = await Assert.ThrowsAnyAsync<Exception>(() => DarlingWorker.RaiseCaptureEnsureFaultAsync(
            "deadlocks", isAzureSqlDb: true, fault, () => throw new InvalidOperationException("The read failed."), NullLogger.Instance, "azure"));

        Assert.Equal("DarlingXeSessionMissingException", raised.GetType().Name);
        Assert.Contains("The create was refused.", raised.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("The read failed.", raised.Message, StringComparison.Ordinal);
    }

    /// <summary>Everywhere else the read of a session that does not exist would return zero rows, so it does not run: the blocked-process collector, and any other engine.</summary>
    [Theory]
    [InlineData("blocked_process_report", true)]
    [InlineData("deadlocks", false)]
    public async Task ACarriedEnsureFailure_ElsewhereThanTheAzureDeadlockRun_DoesNotRead(string collector, bool isAzureSqlDb)
    {
        var read = false;
        var fault = new DarlingXeSessions.XeEnsureFailure("The create was refused.", IsPermission: false);

        var raised = await Assert.ThrowsAnyAsync<Exception>(() => DarlingWorker.RaiseCaptureEnsureFaultAsync(
            collector, isAzureSqlDb, fault, () => { read = true; return Task.CompletedTask; }, NullLogger.Instance, "azure"));

        Assert.Equal("DarlingXeSessionMissingException", raised.GetType().Name);
        Assert.False(read);
    }

    [Fact]
    public void L4_TheServerProbe_ComparesEventsActionsAndTargets_AndAnswersTwoForAnotherDefinition()
    {
        var sql = AlwaysOnXeSessions.BuildAttentionServerProbeSql("PerformanceMonitorDarling");

        Assert.Contains("sys.server_event_session_events", sql, StringComparison.Ordinal);
        Assert.Contains("sys.server_event_session_actions", sql, StringComparison.Ordinal);
        Assert.Contains("sys.server_event_session_targets", sql, StringComparison.Ordinal);
        foreach (var action in new[] { "client_app_name", "client_hostname", "database_name", "session_id", "sql_text" })
        {
            Assert.Contains($"N'{action}'", sql, StringComparison.Ordinal);
        }

        Assert.Contains("N'event_counter'", sql, StringComparison.Ordinal);
        Assert.Contains("N'ring_buffer'", sql, StringComparison.Ordinal);
        Assert.Contains(") = 5", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE 2", sql, StringComparison.Ordinal);
        Assert.Equal(2, AlwaysOnXeSessions.AttentionDefinitionMismatch);
        Assert.Equal("DROP EVENT SESSION [PerformanceMonitor_Attention] ON SERVER;", AlwaysOnXeSessions.BuildAttentionServerDropSql());

        /* Azure: the database-scoped catalog, sql_text allowed and not required, so a session created without it still matches. */
        var azure = AlwaysOnXeSessions.BuildAzureCatalogProbeSql(AlwaysOnXeSessionKind.Attention, "PerformanceMonitorDarling");
        Assert.Contains("sys.database_event_session_actions", azure, StringComparison.Ordinal);
        Assert.Contains(") = 4", azure, StringComparison.Ordinal);
        Assert.Contains("ELSE 1", azure, StringComparison.Ordinal);
    }

    /* #8146: the limit the host asks for reaches the create the ensure sends, for both ring-buffer captures. */
    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task AzureCreate_CarriesConfiguredLimit(AlwaysOnXeSessionKind kind)
    {
        var database = new ScriptedDatabase();

        await AlwaysOnXeAzureEnsure.RunAsync(database, kind, Own(kind), AlwaysOnXeChoice.Shared, CancellationToken.None, ringBufferMaxEvents: 3000);

        var create = Assert.Single(database.Statements, s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal));
        Assert.Contains("max_events_limit = 3000", create, StringComparison.Ordinal);
    }

    /* #8146: max_events_limit is unverified on Azure SQL Database. A refusal costs one retry without it, and the session exists. */
    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock, 25629)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess, 25629)]
    [InlineData(AlwaysOnXeSessionKind.Deadlock, 25713)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess, 25713)]
    public async Task AzureCreate_LimitRefused_RetriesWithoutLimit(AlwaysOnXeSessionKind kind, int refusal)
    {
        var database = new ScriptedDatabase { RefuseLimit = true, RefuseLimitNumber = refusal };

        var result = await AlwaysOnXeAzureEnsure.RunAsync(database, kind, Own(kind), AlwaysOnXeChoice.Shared, CancellationToken.None, ringBufferMaxEvents: 3000);

        Assert.Equal(AlwaysOnXeChoice.Shared, result.Choice);
        Assert.True(database.Started[AlwaysOnXeSessions.SharedNameFor(kind)]);
        var creates = database.Statements.Where(s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, creates.Count);
        Assert.Contains("max_events_limit = 3000", creates[0], StringComparison.Ordinal);
        Assert.DoesNotContain("max_events_limit", creates[1], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task AzureCreate_RetryAlsoFails_Throws(AlwaysOnXeSessionKind kind)
    {
        var database = new ScriptedDatabase { RefuseLimit = true, CreateWithoutLimitFailure = new InvalidOperationException("The create was refused.") };

        var raised = await Assert.ThrowsAsync<InvalidOperationException>(
            () => AlwaysOnXeAzureEnsure.RunAsync(database, kind, Own(kind), AlwaysOnXeChoice.Shared, CancellationToken.None, ringBufferMaxEvents: 3000));

        Assert.Equal("The create was refused.", raised.Message);
        Assert.Equal(2, database.Statements.Count(s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal)));
    }

    /* #8146: when the create with the limit was refused and the retry without it reports the session as already there, the first
       attempt may have left a session that never started. "Already there" is swallowed as benign further up, so the first
       exception surfaces instead and the failure is not hidden. */
    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess)]
    public async Task AzureCreate_LimitRefused_RetryReportsAlreadyPresent_SurfacesTheFirstException(AlwaysOnXeSessionKind kind)
    {
        var database = new ScriptedDatabase { RefuseLimit = true, RefuseLimitNumber = 25629, CreateWithoutLimitFailure = new AlreadyThere() };

        var raised = await Assert.ThrowsAnyAsync<Exception>(
            () => AlwaysOnXeAzureEnsure.RunAsync(database, kind, Own(kind), AlwaysOnXeChoice.Shared, CancellationToken.None, ringBufferMaxEvents: 3000));

        Assert.Equal(25629, Assert.IsType<Microsoft.Data.SqlClient.SqlException>(raised).Number);
        Assert.Equal(2, database.Statements.Count(s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal)));
    }

    /* #8146: a create that fails for a reason other than the limit (a read-only database, a permission error) is sent once and its
       exception surfaces as before; retrying it without the option would only repeat the failure. */
    [Theory]
    [InlineData(AlwaysOnXeSessionKind.Deadlock, 3906)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess, 3906)]
    [InlineData(AlwaysOnXeSessionKind.Deadlock, 15247)]
    [InlineData(AlwaysOnXeSessionKind.BlockedProcess, 15247)]
    public async Task AzureCreate_FailureUnrelatedToTheLimit_IsNotRetried(AlwaysOnXeSessionKind kind, int number)
    {
        var database = new ScriptedDatabase { CreateFailure = LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(number, 16, "not about the limit") };

        var raised = await Assert.ThrowsAnyAsync<Exception>(
            () => AlwaysOnXeAzureEnsure.RunAsync(database, kind, Own(kind), AlwaysOnXeChoice.Shared, CancellationToken.None, ringBufferMaxEvents: 3000));

        Assert.Equal(number, Assert.IsType<Microsoft.Data.SqlClient.SqlException>(raised).Number);
        Assert.Single(database.Statements, s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal));
    }

    /* #8146: the attention session has no ring-buffer limit to refuse, so its create is sent once and carries none. */
    [Fact]
    public async Task AzureCreate_Attention_CarriesNoLimit_AndIsNotRetriedWithoutOne()
    {
        var database = new ScriptedDatabase { CreateFailure = new InvalidOperationException("The create was refused.") };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => AlwaysOnXeAzureEnsure.RunAsync(database, AlwaysOnXeSessionKind.Attention, Own(AlwaysOnXeSessionKind.Attention), AlwaysOnXeChoice.Shared, CancellationToken.None, ringBufferMaxEvents: 3000));

        var create = Assert.Single(database.Statements, s => s.Contains("CREATE EVENT SESSION", StringComparison.Ordinal));
        Assert.DoesNotContain("max_events_limit", create, StringComparison.Ordinal);
    }

    private static CollectorContext AzureContext(string? sessionName) => new()
    {
        ServerId = 1,
        ServerName = "alwayson",
        CollectionTime = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { IsAzureSqlDb = true },
        CurrentDatabaseName = "alpha",
        AlwaysOnSessionName = sessionName,
    };

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }
}
