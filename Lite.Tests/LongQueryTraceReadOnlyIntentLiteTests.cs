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
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Which connection each step of the long-query trace's create opens, for a registration with read-only intent and for one
/// without (#4961). A session cannot be created on a read-only replica, and the definition replicates from the primary, so
/// on Azure SQL Database a read-only-intent registration creates the definition over a connection without the intent and
/// starts the session over its own. Each test drives the real reconcile with the step's open and work replaced, which hands
/// over the connection string the step would have opened.
///
/// <para>In <c>app-logger-statics</c> because the tests read <see cref="AppLogger"/>'s process-wide buffer, which another
/// reader would drain from under them.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class LongQueryTraceReadOnlyIntentLiteTests : IDisposable
{
    private const string Host = "example.database.windows.net";

    private readonly string _tempDir;
    private readonly string _configDir;
    private readonly string _dbPath;

    public LongQueryTraceReadOnlyIntentLiteTests()
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
        public required ServerConnection Server { get; init; }

        /* Every step the create took: which one, the database ("" on a server-scoped engine) and the string it opened. */
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString)> Steps { get; } = new();

        /* Every step a drop took, the one-time drop of the legacy session included: the session's name rides along. */
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString, string SessionName)> DropSteps { get; } = new();

        /* What the step says when it runs: null to succeed. */
        public Func<LongQueryTraceStep, Exception?> Refusal { get; set; } = _ => null;

        /* What the check finds on the replica: by default a database that has never had the session. */
        public LongQueryTraceReplicaState Replica { get; set; }

        public DateTime Clock { get; set; } = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

        public Task ReconcileAsync() => Service.ReconcileLongQueryCompletionsXeSessionAsync(Server, CancellationToken.None);

        /// <summary>The drop steps of this install's own session: what a drop of the long-query trace does, apart from the legacy session's.</summary>
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString)> OwnDrops() =>
            DropSteps.Where(s => s.SessionName != LongQueryCompletionsCollector.LegacyXeSessionName)
                .Select(s => (s.Step, s.Database, s.ConnectionString)).ToList();

        /// <summary>The drop steps of the legacy session older versions shared between installs.</summary>
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString)> LegacyDrops() =>
            DropSteps.Where(s => s.SessionName == LongQueryCompletionsCollector.LegacyXeSessionName)
                .Select(s => (s.Step, s.Database, s.ConnectionString)).ToList();

        /// <summary>This rig's lines at Warning or above since the last drain.</summary>
        public List<string> LoudLines() =>
            Lines().Where(line => line.Contains("WARN", StringComparison.Ordinal) || line.Contains("ERROR", StringComparison.Ordinal)).ToList();

        /// <summary>This rig's lines in the log buffer since the last drain.</summary>
        public List<string> Lines() =>
            AppLogger.DrainBufferedLines()
                .Where(line => line.Contains(Server.DisplayName, StringComparison.Ordinal))
                .ToList();
    }

    private async Task<Rig> BuildRigAsync(string database, bool readOnlyIntent, int engineEdition = 5, bool traceOn = true)
    {
        var server = new ServerConnection
        {
            ServerName = Host,
            DisplayName = "intent-" + Guid.NewGuid().ToString("N")[..8],
            DatabaseName = database,
            ReadOnlyIntent = readOnlyIntent,
        };

        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = engineEdition;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("long_query_completions", enabled: traceOn);

        var rig = new Rig
        {
            Service = new RemoteCollectorService(
                duckDb, servers, schedules,
                installIdStore: new InstallIdStore(_configDir, "test-machine", null)),
            Server = server,
        };

        rig.Service.LongQueryTraceUtcNowForTests = () => rig.Clock;
        rig.Service.LongQueryTraceListOverrideForTests = (_, _, _) => Task.FromResult(new List<string> { database });
        /* The one-time drop of the legacy session has nothing to find. */
        rig.Service.LegacyLongQuerySessionExistsForTests = (_, _) => false;
        rig.Service.LongQueryTraceReplicaStateForTests = (_, _) => rig.Replica;
        rig.Service.LongQueryTraceStepOverrideForTests = (_, databaseName, connectionString, step, sessionName, _) =>
        {
            if (step is LongQueryTraceStep.Stop or LongQueryTraceStep.Drop)
            {
                rig.DropSteps.Add((step, databaseName, connectionString, sessionName));
            }
            else
            {
                rig.Steps.Add((step, databaseName, connectionString));
            }

            return rig.Refusal(step) is { } refusal ? Task.FromException(refusal) : Task.CompletedTask;
        };

        return rig;
    }

    private static ApplicationIntent IntentOf(string connectionString) =>
        new SqlConnectionStringBuilder(connectionString).ApplicationIntent;

    private static string DatabaseOf(string connectionString) =>
        new SqlConnectionStringBuilder(connectionString).InitialCatalog;

    /* A line at Warning or above that speaks of a read-only database: what an operator reads about it. The one-time drop of
       the legacy session logs lines of its own, which these tests do not read. */
    private static bool IsLoud(string line) =>
        (line.Contains("WARN", StringComparison.Ordinal) || line.Contains("ERROR", StringComparison.Ordinal))
        && line.Contains("read-only", StringComparison.OrdinalIgnoreCase);

    /* ── Azure SQL Database ── */

    [Fact]
    public async Task Azure_AReadOnlyIntentRegistration_CreatesTheDefinitionOverAReadWriteConnection_ThenStartsOverItsOwnReadOnlyOne()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Equal(
            new[] { LongQueryTraceStep.Check, LongQueryTraceStep.CreateDefinition, LongQueryTraceStep.Start },
            rig.Steps.Select(s => s.Step));
        Assert.Equal(
            new[] { ApplicationIntent.ReadOnly, ApplicationIntent.ReadWrite, ApplicationIntent.ReadOnly },
            rig.Steps.Select(s => IntentOf(s.ConnectionString)));
        Assert.All(rig.Steps, s => Assert.Equal("beta", DatabaseOf(s.ConnectionString)));
        Assert.All(rig.Steps, s => Assert.Equal("beta", s.Database));
    }

    [Fact]
    public async Task Azure_ARegistrationWithoutReadOnlyIntent_CreatesAndStartsOverItsOwnConnection()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: false);

        await rig.ReconcileAsync();

        var step = Assert.Single(rig.Steps);
        Assert.Equal(LongQueryTraceStep.CreateAndStart, step.Step);
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(step.ConnectionString));
        Assert.Equal("beta", DatabaseOf(step.ConnectionString));
    }

    /* ── Azure SQL Database: a cycle that needs nothing opens nothing on the primary ── */

    [Fact]
    public async Task Azure_AReadOnlyIntentCycleWhereTheDefinitionExistsAndTheSessionRuns_OpensOnlyTheRegistrationsOwnConnection()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true);
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: true);

        await rig.ReconcileAsync();

        var step = Assert.Single(rig.Steps);
        Assert.Equal(LongQueryTraceStep.Check, step.Step);
        Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(step.ConnectionString));
        Assert.Equal("beta", DatabaseOf(step.ConnectionString));
    }

    [Fact]
    public async Task Azure_AReadOnlyIntentCycleWhereTheSessionIsStopped_StartsItOverItsOwnConnectionAndOpensNothingElse()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true);
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { LongQueryTraceStep.Check, LongQueryTraceStep.Start }, rig.Steps.Select(s => s.Step));
        Assert.All(rig.Steps, s => Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(s.ConnectionString)));
    }

    [Fact]
    public async Task Azure_AStartThatFailsRightAfterTheDefinitionWasCreated_IsRetriedOnTheNextCycleWithoutAWarning()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true);
        rig.Refusal = step => step == LongQueryTraceStep.Start
            ? SqlExceptionFactory.Create(15151, errorClass: 16, message: "Cannot alter the event session, because it does not exist or you do not have permission.")
            : null;
        AppLogger.DrainBufferedLines();

        await rig.ReconcileAsync();

        /* The replica had not caught up yet: no fault, and nothing at Warning or above. */
        Assert.Null(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
        Assert.Empty(rig.LoudLines());
        Assert.Equal(
            new[] { LongQueryTraceStep.Check, LongQueryTraceStep.CreateDefinition, LongQueryTraceStep.Start },
            rig.Steps.Select(s => s.Step));

        /* The replica shows the definition by the next cycle: that cycle starts the session. */
        rig.Refusal = _ => null;
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);
        rig.Steps.Clear();
        await rig.ReconcileAsync();

        Assert.Equal(new[] { LongQueryTraceStep.Check, LongQueryTraceStep.Start }, rig.Steps.Select(s => s.Step));
        Assert.Null(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
        Assert.Empty(rig.LoudLines());
    }

    [Fact]
    public async Task Azure_AStartThatFailsWhenTheDefinitionWasAlreadyVisible_IsAFailureLikeAnyOther()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true);
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);
        rig.Refusal = step => step == LongQueryTraceStep.Start
            ? SqlExceptionFactory.Create(262, errorClass: 14, message: "ALTER EVENT SESSION permission denied.")
            : null;
        AppLogger.DrainBufferedLines();

        await rig.ReconcileAsync();

        Assert.NotNull(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
        Assert.NotEmpty(rig.LoudLines());
    }

    /* ── Azure SQL Database: a drop stops the session over the replica, then drops the definition over the primary ── */

    [Fact]
    public async Task Azure_TurningTheTraceOff_ForAReadOnlyIntentRegistration_StopsOverItsOwnConnection_ThenDropsOverAReadWriteOne()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true, traceOn: false);

        await rig.ReconcileAsync();

        var drops = rig.OwnDrops();
        Assert.Equal(new[] { LongQueryTraceStep.Stop, LongQueryTraceStep.Drop }, drops.Select(d => d.Step));
        Assert.Equal(new[] { ApplicationIntent.ReadOnly, ApplicationIntent.ReadWrite }, drops.Select(d => IntentOf(d.ConnectionString)));
        Assert.All(drops, d => Assert.Equal("beta", DatabaseOf(d.ConnectionString)));
        Assert.All(drops, d => Assert.Equal("beta", d.Database));
        Assert.Empty(rig.Steps);
    }

    [Fact]
    public async Task Azure_TurningTheTraceOff_ForARegistrationWithoutReadOnlyIntent_DropsOnceOverItsOwnConnection()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: false, traceOn: false);

        await rig.ReconcileAsync();

        var drop = Assert.Single(rig.OwnDrops());
        Assert.Equal(LongQueryTraceStep.Drop, drop.Step);
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(drop.ConnectionString));
        Assert.Equal("beta", DatabaseOf(drop.ConnectionString));
    }

    [Fact]
    public async Task Azure_ARemovedServer_ForAReadOnlyIntentRegistration_StopsOverItsOwnConnection_ThenDropsOverAReadWriteOne()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true);

        await rig.Service.DropLongQueryTraceOfRemovedServerAsync(rig.Server, CancellationToken.None);

        var drops = rig.OwnDrops();
        Assert.Equal(new[] { LongQueryTraceStep.Stop, LongQueryTraceStep.Drop }, drops.Select(d => d.Step));
        Assert.Equal(new[] { ApplicationIntent.ReadOnly, ApplicationIntent.ReadWrite }, drops.Select(d => IntentOf(d.ConnectionString)));
        Assert.All(drops, d => Assert.Equal("beta", DatabaseOf(d.ConnectionString)));
    }

    [Fact]
    public async Task Azure_TheOneTimeDropOfTheLegacySession_ForAReadOnlyIntentRegistration_FollowsTheSameOrder()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: true);

        await rig.ReconcileAsync();

        var drops = rig.LegacyDrops();
        Assert.Equal(new[] { LongQueryTraceStep.Stop, LongQueryTraceStep.Drop }, drops.Select(d => d.Step));
        Assert.Equal(new[] { ApplicationIntent.ReadOnly, ApplicationIntent.ReadWrite }, drops.Select(d => IntentOf(d.ConnectionString)));
        Assert.All(drops, d => Assert.Equal("beta", DatabaseOf(d.ConnectionString)));
    }

    [Fact]
    public async Task Azure_TheOneTimeDropOfTheLegacySession_ForARegistrationWithoutReadOnlyIntent_IsOneDropOverItsOwnConnection()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: false);

        await rig.ReconcileAsync();

        var drop = Assert.Single(rig.LegacyDrops());
        Assert.Equal(LongQueryTraceStep.Drop, drop.Step);
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(drop.ConnectionString));
    }

    /* ── Every other engine ── */

    [Fact]
    public async Task OnPrem_AReadOnlyIntentRegistration_CreatesAndStartsItsSessionOverItsOwnConnection()
    {
        var rig = await BuildRigAsync("app", readOnlyIntent: true, engineEdition: 3);

        await rig.ReconcileAsync();

        var step = Assert.Single(rig.Steps);
        Assert.Equal(LongQueryTraceStep.CreateAndStart, step.Step);
        Assert.Equal(string.Empty, step.Database);
        Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(step.ConnectionString));
        Assert.Equal("app", DatabaseOf(step.ConnectionString));
    }

    [Fact]
    public async Task OnPrem_TurningTheTraceOff_DropsOnceOverTheRegistrationsOwnConnection()
    {
        var rig = await BuildRigAsync("app", readOnlyIntent: true, engineEdition: 3, traceOn: false);

        await rig.ReconcileAsync();

        var own = Assert.Single(rig.OwnDrops());
        Assert.Equal(LongQueryTraceStep.Drop, own.Step);
        Assert.Equal(string.Empty, own.Database);
        Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(own.ConnectionString));
        Assert.Equal("app", DatabaseOf(own.ConnectionString));

        var legacy = Assert.Single(rig.LegacyDrops());
        Assert.Equal(LongQueryTraceStep.Drop, legacy.Step);
        Assert.Equal(own.ConnectionString, legacy.ConnectionString);
    }

    /* ── A registration that lands on a read-only database without the intent ── */

    [Fact]
    public async Task Azure_AReadOnlyDatabaseWithoutTheIntent_LogsOneClearMessage_KeepsTheFault_AndDoesNotRetryInALoop()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync("beta", readOnlyIntent: false);
            rig.Refusal = _ => ReadOnlyDatabaseRefusal();

            await rig.ReconcileAsync();

            /* One line at Warning or above, and it says why and what to change. */
            var loud = Assert.Single(rig.Lines(), IsLoud);
            Assert.Contains("read-only", loud, StringComparison.Ordinal);
            Assert.Contains("3906", loud, StringComparison.Ordinal);
            Assert.Contains("Register the primary database instead", loud, StringComparison.Ordinal);
            Assert.Contains("turn the long-query trace off", loud, StringComparison.Ordinal);

            /* The run still records the fault. */
            Assert.NotNull(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
            Assert.Single(rig.Steps);

            /* Within the hour: no attempt, no line, and the fault stays. */
            rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Single(rig.Steps);
            Assert.DoesNotContain(rig.Lines(), IsLoud);
            Assert.NotNull(rig.Service.LongQueryTraceFaultState(rig.Server.Id));

            /* An hour after the first attempt: one more, and it logs at Debug, so the one message stays the only one. */
            rig.Clock += TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            Assert.Equal(2, rig.Steps.Count);
            Assert.DoesNotContain(rig.Lines(), IsLoud);
            Assert.NotNull(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Azure_ARefusalThatIsNotReadOnly_StillRetriesOnEveryCycle()
    {
        var rig = await BuildRigAsync("beta", readOnlyIntent: false);
        rig.Refusal = _ => SqlExceptionFactory.Create(262, errorClass: 14, message: "CREATE EVENT SESSION permission denied.");

        await rig.ReconcileAsync();
        await rig.ReconcileAsync();
        await rig.ReconcileAsync();

        Assert.Equal(3, rig.Steps.Count);
    }

    private static SqlException ReadOnlyDatabaseRefusal() =>
        SqlExceptionFactory.Create(
            LongQueryTraceDatabases.ReadOnlyDatabaseErrorNumber, errorClass: 16, message: "Failed to update database because the database is read-only.");
}
