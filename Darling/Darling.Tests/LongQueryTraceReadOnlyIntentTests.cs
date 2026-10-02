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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Which connection each step of the long-query trace's create opens, for a registration with read-only intent and for one
/// without (#4961). A session cannot be created on a read-only replica, and the definition replicates from the primary, so
/// on Azure SQL Database a read-only-intent registration creates the definition over a connection without the intent and
/// starts the session over its own. Each test drives the worker's real reconcile with the step's open and work replaced on
/// the runner, which hands over the connection string the step would have opened.
/// </summary>
public sealed class LongQueryTraceReadOnlyIntentTests : IAsyncDisposable
{
    private const string Host = "example.database.windows.net";
    private const string InstallIdValue = "0a1b2c3d";

    /* Never opened: the runner reaches the store only to write collected rows, and these tests collect nothing. */
    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private sealed class Rig
    {
        public required DarlingCollectorRunner Runner { get; init; }
        public required DarlingWorker.ServerLoopState State { get; init; }
        public DarlingSelfAlertTests.CapturingLogger Logger { get; } = new();
        public List<string> Listed { get; set; } = new();

        /* Every step the create took: which one, the database ("" on a server-scoped engine) and the string it opened. */
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString)> Steps { get; } = new();

        /* Every step a drop took, the one-time drop of the legacy session included: the session's name rides along. */
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString, string SessionName)> DropSteps { get; } = new();

        /* What the step says when it runs: null to succeed. */
        public Func<LongQueryTraceStep, Exception?> Refusal { get; set; } = _ => null;

        /* What the check finds on the replica: by default a database that has never had the session. */
        public LongQueryTraceReplicaState Replica { get; set; }

        public DateTime Clock { get; set; } = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

        public Task ReconcileAsync(bool enabled = true, IReadOnlyList<LongQueryTraceRegistration>? registrations = null) =>
            DarlingWorker.ReconcileLongQueryTraceAsync(
                State, Runner, enabled, registrations ?? Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(), Clock, Logger, CancellationToken.None);

        /// <summary>The drop steps of this install's own session: what a drop of the long-query trace does, apart from the legacy session's.</summary>
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString)> OwnDrops() =>
            DropSteps.Where(s => s.SessionName != LongQueryCompletionsCollector.LegacyXeSessionName)
                .Select(s => (s.Step, s.Database, s.ConnectionString)).ToList();

        /// <summary>The drop steps of the legacy session older versions shared between installs.</summary>
        public List<(LongQueryTraceStep Step, string Database, string ConnectionString)> LegacyDrops() =>
            DropSteps.Where(s => s.SessionName == LongQueryCompletionsCollector.LegacyXeSessionName)
                .Select(s => (s.Step, s.Database, s.ConnectionString)).ToList();

        /// <summary>
        /// The lines logged at Warning or above that speak of a read-only database: what an operator reads about it. The
        /// one-time drop of the legacy session logs lines of its own, which these tests do not read.
        /// </summary>
        public List<string> Loud() => Logger.Entries
            .Where(e => e.Level >= LogLevel.Warning && e.Message.Contains("read-only", StringComparison.OrdinalIgnoreCase))
            .Select(e => e.Message)
            .ToList();
    }

    private Rig BuildRig(string database, bool readOnlyIntent, bool azureSqlDatabase = true)
    {
        var config = new MonitoredServer { Name = "intent-test", Host = Host, Database = database, ReadOnlyIntent = readOnlyIntent };

        /* The string the runtime holds is what carries the intent: the worker builds it from the registration. */
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{Host},1433;Initial Catalog={database};Encrypt=True;ApplicationIntent={(readOnlyIntent ? "ReadOnly" : "ReadWrite")}",
            Target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDatabase },
            StorageName = Host,
            ServerId = 4961,
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
            State = new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime },
        };
        rig.Listed = new List<string> { database };

        runner.LongQueryTraceListOverrideForTests = (_, _, _, _) => Task.FromResult(rig.Listed.ToList());
        /* The one-time drop of the legacy session has nothing to find, and keeps its record in memory. */
        runner.LegacyLongQueryRecordsForTests = new LongQueryTraceLifecycleTests.InMemoryLegacyRecords();
        runner.LegacyLongQueryPresentForTests = (_, _, _) => Task.FromResult(false);
        runner.LongQueryTraceReplicaStateForTests = (_, _) => rig.Replica;
        runner.LongQueryTraceStepOverrideForTests = (_, databaseName, connectionString, step, sessionName, _) =>
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

    /* ── Azure SQL Database ── */

    [Fact]
    public async Task Azure_AReadOnlyIntentRegistration_CreatesTheDefinitionOverAReadWriteConnection_ThenStartsOverItsOwnReadOnlyOne()
    {
        var rig = BuildRig("beta", readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Equal(
            new[] { LongQueryTraceStep.Check, LongQueryTraceStep.CreateDefinition, LongQueryTraceStep.Start },
            rig.Steps.Select(s => s.Step));
        Assert.Equal(
            new[] { ApplicationIntent.ReadOnly, ApplicationIntent.ReadWrite, ApplicationIntent.ReadOnly },
            rig.Steps.Select(s => IntentOf(s.ConnectionString)));
        Assert.All(rig.Steps, s => Assert.Equal("beta", DatabaseOf(s.ConnectionString)));
        Assert.All(rig.Steps, s => Assert.Equal("beta", s.Database));
        Assert.Empty(rig.Loud());
    }

    [Fact]
    public async Task Azure_ARegistrationWithoutReadOnlyIntent_CreatesAndStartsOverItsOwnConnection()
    {
        var rig = BuildRig("beta", readOnlyIntent: false);

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
        var rig = BuildRig("beta", readOnlyIntent: true);
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
        var rig = BuildRig("beta", readOnlyIntent: true);
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { LongQueryTraceStep.Check, LongQueryTraceStep.Start }, rig.Steps.Select(s => s.Step));
        Assert.All(rig.Steps, s => Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(s.ConnectionString)));
    }

    [Fact]
    public async Task Azure_AStartThatFailsRightAfterTheDefinitionWasCreated_IsRetriedOnALaterPassWithoutAWarning()
    {
        var rig = BuildRig("beta", readOnlyIntent: true);
        rig.Refusal = step => step == LongQueryTraceStep.Start
            ? SqlExceptionFactory.Create(15151, 16, "Cannot alter the event session, because it does not exist or you do not have permission.")
            : null;

        await rig.ReconcileAsync();

        /* The replica had not caught up yet: no fault, and nothing at Warning or above. */
        Assert.Null(rig.State.LongQueryTraceFault);
        Assert.DoesNotContain(rig.Logger.Entries, e => e.Level >= LogLevel.Warning);
        Assert.Equal(
            new[] { LongQueryTraceStep.Check, LongQueryTraceStep.CreateDefinition, LongQueryTraceStep.Start },
            rig.Steps.Select(s => s.Step));

        /* The replica shows the definition by the next pass of the create side: that pass starts the session. */
        rig.Refusal = _ => null;
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);
        rig.Steps.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();

        Assert.Equal(new[] { LongQueryTraceStep.Check, LongQueryTraceStep.Start }, rig.Steps.Select(s => s.Step));
        Assert.Null(rig.State.LongQueryTraceFault);
        Assert.DoesNotContain(rig.Logger.Entries, e => e.Level >= LogLevel.Warning);
    }

    [Fact]
    public async Task Azure_AStartThatFailsRightAfterTheDefinitionWasCreated_LeavesTheLatchUnsetSoTheNextSweepRetries()
    {
        var rig = BuildRig("beta", readOnlyIntent: true);
        rig.Refusal = step => step == LongQueryTraceStep.Start
            ? SqlExceptionFactory.Create(15151, 16, "Cannot alter the event session, because it does not exist or you do not have permission.")
            : null;

        await rig.ReconcileAsync();

        /* Not applied: a latch would hold the retry back until the hourly create pass. */
        Assert.Null(rig.State.LongQueryTraceApplied);
        Assert.Null(rig.State.LongQueryTraceFault);

        /* The next sweep, with the clock where it was, checks again and starts the session, and only then is it applied. */
        rig.Refusal = _ => null;
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);
        rig.Steps.Clear();
        await rig.ReconcileAsync();

        Assert.Equal(new[] { LongQueryTraceStep.Check, LongQueryTraceStep.Start }, rig.Steps.Select(s => s.Step));
        Assert.True(rig.State.LongQueryTraceApplied);
    }

    [Fact]
    public async Task Azure_AStartThatFailsWhenTheDefinitionWasAlreadyVisible_IsAFailureLikeAnyOther()
    {
        var rig = BuildRig("beta", readOnlyIntent: true);
        rig.Replica = new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);
        rig.Refusal = step => step == LongQueryTraceStep.Start
            ? SqlExceptionFactory.Create(262, 14, "ALTER EVENT SESSION permission denied.")
            : null;

        await rig.ReconcileAsync();

        Assert.NotNull(rig.State.LongQueryTraceFault);
        Assert.Contains(rig.Logger.Entries, e => e.Level >= LogLevel.Warning);
    }

    /* ── Azure SQL Database: a drop stops the session over the replica, then drops the definition over the primary ── */

    [Fact]
    public async Task Azure_TurningTheTraceOff_ForAReadOnlyIntentRegistration_StopsOverItsOwnConnection_ThenDropsOverAReadWriteOne()
    {
        var rig = BuildRig("beta", readOnlyIntent: true);

        await rig.ReconcileAsync(enabled: false);

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
        var rig = BuildRig("beta", readOnlyIntent: false);

        await rig.ReconcileAsync(enabled: false);

        var drop = Assert.Single(rig.OwnDrops());
        Assert.Equal(LongQueryTraceStep.Drop, drop.Step);
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(drop.ConnectionString));
        Assert.Equal("beta", DatabaseOf(drop.ConnectionString));
    }

    [Fact]
    public async Task Azure_TheOneTimeDropOfTheLegacySession_ForAReadOnlyIntentRegistration_FollowsTheSameOrder()
    {
        var rig = BuildRig("beta", readOnlyIntent: true);

        await rig.ReconcileAsync();

        var drops = rig.LegacyDrops();
        Assert.Equal(new[] { LongQueryTraceStep.Stop, LongQueryTraceStep.Drop }, drops.Select(d => d.Step));
        Assert.Equal(new[] { ApplicationIntent.ReadOnly, ApplicationIntent.ReadWrite }, drops.Select(d => IntentOf(d.ConnectionString)));
        Assert.All(drops, d => Assert.Equal("beta", DatabaseOf(d.ConnectionString)));
    }

    [Fact]
    public async Task Azure_TheOneTimeDropOfTheLegacySession_ForARegistrationWithoutReadOnlyIntent_IsOneDropOverItsOwnConnection()
    {
        var rig = BuildRig("beta", readOnlyIntent: false);

        await rig.ReconcileAsync();

        var drop = Assert.Single(rig.LegacyDrops());
        Assert.Equal(LongQueryTraceStep.Drop, drop.Step);
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(drop.ConnectionString));
    }

    [Fact]
    public async Task Azure_ADatabaseAnotherRegistrationKeeps_GetsNeitherStepOfTheDrop()
    {
        var rig = BuildRig("beta", readOnlyIntent: true);
        var other = new LongQueryTraceRegistration("77", Host, Database: null, Enabled: true, TraceOn: true, Array.Empty<string>(), DatabaseScope: null);

        await rig.ReconcileAsync(enabled: false, new[] { other });

        Assert.Empty(rig.OwnDrops());
    }

    /* ── Every other engine ── */

    [Fact]
    public async Task OnPrem_AReadOnlyIntentRegistration_CreatesAndStartsItsSessionOverItsOwnConnection()
    {
        var rig = BuildRig("app", readOnlyIntent: true, azureSqlDatabase: false);

        await rig.ReconcileAsync();

        var step = Assert.Single(rig.Steps);
        Assert.Equal(LongQueryTraceStep.CreateAndStart, step.Step);
        Assert.Equal(string.Empty, step.Database);
        Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(step.ConnectionString));
        Assert.Equal(rig.State.Runtime!.ConnectionString, step.ConnectionString);
    }

    [Fact]
    public async Task OnPrem_TurningTheTraceOff_DropsOnceOverTheRegistrationsOwnConnection()
    {
        var rig = BuildRig("app", readOnlyIntent: true, azureSqlDatabase: false);

        await rig.ReconcileAsync(enabled: false);

        var own = Assert.Single(rig.OwnDrops());
        Assert.Equal(LongQueryTraceStep.Drop, own.Step);
        Assert.Equal(string.Empty, own.Database);
        Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(own.ConnectionString));
        Assert.Equal(rig.State.Runtime!.ConnectionString, own.ConnectionString);

        var legacy = Assert.Single(rig.LegacyDrops());
        Assert.Equal(LongQueryTraceStep.Drop, legacy.Step);
        Assert.Equal(rig.State.Runtime!.ConnectionString, legacy.ConnectionString);
    }

    /* ── A registration that lands on a read-only database without the intent ── */

    [Fact]
    public async Task Azure_AReadOnlyDatabaseWithoutTheIntent_LogsOneClearMessage_KeepsTheFault_AndDoesNotRetryInALoop()
    {
        var rig = BuildRig("beta", readOnlyIntent: false);
        rig.Refusal = step => step is LongQueryTraceStep.Stop or LongQueryTraceStep.Drop ? null : ReadOnlyDatabaseRefusal();

        await rig.ReconcileAsync();

        /* One line at Warning or above, and it says why and what to change. */
        var loud = Assert.Single(rig.Loud());
        Assert.Contains("read-only", loud, StringComparison.Ordinal);
        Assert.Contains("3906", loud, StringComparison.Ordinal);
        Assert.Contains("Register the primary database instead", loud, StringComparison.Ordinal);
        Assert.Contains("turn the long-query trace off", loud, StringComparison.Ordinal);

        /* The run still records the fault. */
        Assert.NotNull(rig.State.LongQueryTraceFault);
        Assert.Single(rig.Steps);

        /* Within the hour: no attempt, no line, and the fault stays. */
        rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
        await rig.ReconcileAsync();
        await rig.ReconcileAsync();
        Assert.Single(rig.Steps);
        Assert.Single(rig.Loud());
        Assert.NotNull(rig.State.LongQueryTraceFault);

        /* An hour after the first attempt: one more, and it logs at Debug, so the one message stays the only one. */
        rig.Clock += TimeSpan.FromMinutes(1);
        await rig.ReconcileAsync();
        await rig.ReconcileAsync();
        Assert.Equal(2, rig.Steps.Count);
        Assert.Single(rig.Loud());
        Assert.NotNull(rig.State.LongQueryTraceFault);
    }

    [Fact]
    public async Task Azure_AReadOnlyDatabaseWithoutTheIntent_TheFaultNamesTheSessionNotTheDatabaseError()
    {
        var rig = BuildRig("beta", readOnlyIntent: false);
        rig.Refusal = step => step is LongQueryTraceStep.Stop or LongQueryTraceStep.Drop ? null : ReadOnlyDatabaseRefusal();

        await rig.ReconcileAsync();

        Assert.Contains(
            LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue),
            rig.State.LongQueryTraceFault,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Azure_ARefusalThatIsNotReadOnly_StillRetriesOnEverySweep()
    {
        var rig = BuildRig("beta", readOnlyIntent: false);
        rig.Refusal = _ => SqlExceptionFactory.Create(262, 14, "CREATE EVENT SESSION permission denied.");

        await rig.ReconcileAsync();
        await rig.ReconcileAsync();
        await rig.ReconcileAsync();

        Assert.Equal(3, rig.Steps.Count);
    }

    /* ── A failed create or start on Azure SQL Database carries the caps sentence (#4961, plan test 27) ── */

    private static bool Speaks(string line, string text) => line.Contains(text, StringComparison.Ordinal);

    private static List<string> LoggedLines(Rig rig) => rig.Logger.Entries.Select(e => e.Message).ToList();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Azure_AFailedCreateOrStart_CarriesTheCapsSentence_InTheLoggedLineAndTheRecordedFault(bool start)
    {
        /* A registration without the intent creates and starts in one step. One with it creates the definition, then starts. */
        var rig = BuildRig("beta", readOnlyIntent: start);
        var refused = start ? LongQueryTraceStep.Start : LongQueryTraceStep.CreateAndStart;
        rig.Refusal = step => step == refused ? SqlExceptionFactory.Create(1105, 17, "The statement was refused.") : null;

        await rig.ReconcileAsync();

        Assert.Contains(LoggedLines(rig), line => Speaks(line, "The statement was refused.") && Speaks(line, AlwaysOnXeSessions.AzureCapsSentence));
        Assert.NotNull(rig.State.LongQueryTraceFault);
        Assert.Contains(AlwaysOnXeSessions.AzureCapsSentence, rig.State.LongQueryTraceFault, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnPrem_AFailedCreate_CarriesNoCapsSentence()
    {
        var rig = BuildRig("app", readOnlyIntent: false, azureSqlDatabase: false);
        rig.Refusal = _ => SqlExceptionFactory.Create(1105, 17, "The statement was refused.");

        await rig.ReconcileAsync();

        var lines = LoggedLines(rig);
        Assert.Contains(lines, line => Speaks(line, "The statement was refused."));
        Assert.DoesNotContain(lines, line => Speaks(line, AlwaysOnXeSessions.AzureCapsSentence));
        Assert.NotNull(rig.State.LongQueryTraceFault);
        Assert.DoesNotContain(AlwaysOnXeSessions.AzureCapsSentence, rig.State.LongQueryTraceFault, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(25631)]
    [InlineData(25705)]
    public async Task Azure_AnAlreadyThereAnswer_CarriesNoCapsSentence(int number)
    {
        var rig = BuildRig("beta", readOnlyIntent: false);
        rig.Refusal = _ => SqlExceptionFactory.Create(number, 16, "The session is already there.");

        await rig.ReconcileAsync();

        Assert.DoesNotContain(LoggedLines(rig), line => Speaks(line, AlwaysOnXeSessions.AzureCapsSentence));
        Assert.NotNull(rig.State.LongQueryTraceFault);
        Assert.DoesNotContain(AlwaysOnXeSessions.AzureCapsSentence, rig.State.LongQueryTraceFault, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Azure_AReadOnlyDatabasesRefusalOfACreateOrStart_CarriesNoCapsSentence(bool start)
    {
        var rig = BuildRig("beta", readOnlyIntent: start);
        var refused = start ? LongQueryTraceStep.Start : LongQueryTraceStep.CreateAndStart;
        rig.Refusal = step => step == refused ? ReadOnlyDatabaseRefusal() : null;

        await rig.ReconcileAsync();

        Assert.DoesNotContain(LoggedLines(rig), line => Speaks(line, AlwaysOnXeSessions.AzureCapsSentence));
        Assert.NotNull(rig.State.LongQueryTraceFault);
        Assert.DoesNotContain(AlwaysOnXeSessions.AzureCapsSentence, rig.State.LongQueryTraceFault, StringComparison.Ordinal);
    }

    private static SqlException ReadOnlyDatabaseRefusal() =>
        SqlExceptionFactory.Create(LongQueryTraceDatabases.ReadOnlyDatabaseErrorNumber, 16, "Failed to update database because the database is read-only.");

    /// <summary>SqlException has no public constructor; this builds one through the driver's internals.</summary>
    internal static class SqlExceptionFactory
    {
        public static SqlException Create(int number, byte errorClass, string message)
        {
            const BindingFlags all = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static;
            var errorCtor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .OrderByDescending(c => c.GetParameters().Length).First();
            var ints = new Queue<object>(new object[] { number, 0, 0, 0 });
            var bytes = new Queue<object>(new object[] { (byte)0, errorClass, (byte)0, (byte)0 });
            var strings = new Queue<object>(new object[] { "server", message, "procedure", "extra", "extra" });
            var args = errorCtor.GetParameters().Select(p =>
                p.ParameterType == typeof(int) ? ints.Dequeue()
                : p.ParameterType == typeof(byte) ? bytes.Dequeue()
                : p.ParameterType == typeof(string) ? strings.Dequeue()
                : p.ParameterType == typeof(uint) ? (object)0u
                : p.ParameterType == typeof(Exception) ? null!
                : p.HasDefaultValue ? p.DefaultValue! : throw new InvalidOperationException($"unmapped SqlError ctor parameter {p.ParameterType}")).ToArray();
            var error = errorCtor.Invoke(args);

            var collection = Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
            typeof(SqlErrorCollection).GetMethod("Add", all, null, new[] { typeof(SqlError) }, null)!.Invoke(collection, new[] { error });

            var create = typeof(SqlException).GetMethods(all)
                .Where(m => m.Name == "CreateException" && m.GetParameters().Length >= 2
                    && m.GetParameters()[0].ParameterType == typeof(SqlErrorCollection)
                    && m.GetParameters()[1].ParameterType == typeof(string))
                .OrderBy(m => m.GetParameters().Length).First();
            var createArgs = create.GetParameters().Select((p, i) =>
                i == 0 ? collection
                : i == 1 ? "11.0"
                : p.ParameterType == typeof(Guid) ? Guid.Empty
                : p.HasDefaultValue ? p.DefaultValue! : null!).ToArray();
            return (SqlException)create.Invoke(null, createArgs)!;
        }
    }
}
