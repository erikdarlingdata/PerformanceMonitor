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

        public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken) =>
            Task.FromResult(
                !Started.ContainsKey(sessionName) ? AlwaysOnXeCatalog.Missing
                : WrongEvent.Contains(sessionName) && kind == AlwaysOnXeSessionKind.Deadlock ? AlwaysOnXeCatalog.WrongEvent
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
