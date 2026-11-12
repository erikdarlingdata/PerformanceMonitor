/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8146: when the long-query read finds this install's own session below <c>xeRingBufferMaxEvents</c>, the runner recreates
/// that session after the item's rows are written, at most once an hour. Drives the real <see cref="DarlingCollectorRunner.RunAsync"/>
/// on a fake target and records each lifecycle step the recreate sends through <c>LongQueryTraceStepOverrideForTests</c>, the
/// seam the long-query lifecycle tests use. The store is real, so a failed write is the runner's own path.
/// </summary>
[Collection("live-postgres")]
public sealed class LongQuerySessionRecreateRunnerLiveTests
{
    private const int LiveServerId = -498146;
    private const string InstallId = "0a1b2c3d";
    private static readonly string OwnSession = LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallId);
    private static readonly DateTime Created = new(2026, 11, 7, 6, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime ReadAt = new(2026, 11, 7, 9, 0, 0, DateTimeKind.Utc);

    private sealed record Rig(DarlingCollectorRunner Runner, List<(string Database, LongQueryTraceStep Step, string Session)> Steps, CapturingTestLogger Logger)
    {
        public DateTime Clock { get; set; } = ReadAt;
    }

    [Fact]
    public async Task OnPrem_ALimitBelowTheSetting_RecreatesTheOwnSession_AndASecondRequestWithinTheHourSendsNothing()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            var server = OnPremServer();

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(
                new[] { (string.Empty, LongQueryTraceStep.Drop, OwnSession), (string.Empty, LongQueryTraceStep.CreateAndStart, OwnSession) },
                rig.Steps);
            Assert.Contains($"Recreated long-query session {OwnSession} to apply max_events_limit 5000 (was 1000)", rig.Logger.Joined, StringComparison.Ordinal);

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(2, rig.Steps.Count);
        });
    }

    [Fact]
    public async Task OnPrem_AFailedRecreate_IsLoggedAtWarning_AndDoesNotFailTheRun()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            rig.Runner.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
            {
                rig.Steps.Add((database, step, session));
                return step == LongQueryTraceStep.CreateAndStart
                    ? Task.FromException(new InvalidOperationException("The create was refused."))
                    : Task.CompletedTask;
            };

            var result = await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, OnPremServer(), ct);

            Assert.Equal(1, result.Rows);
            Assert.Equal(1, rig.Logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            Assert.Contains($"Failed to recreate long-query session {OwnSession}", rig.Logger.Joined, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Azure_ADatabaseWhoseWriteFailed_IsNotRecreated_WhileItsSiblingIs()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha", "beta" });
            rig.Runner.PerDatabaseWriteFaultForTests = database =>
            {
                if (database == "beta")
                {
                    throw new InvalidOperationException("injected write failure for beta");
                }
            };
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "lqrecreate", Host = "lqrecreate.database.windows.net" },
                ConnectionString = "Server=azure;Database=master",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "lqrecreate",
                ServerId = LiveServerId,
            };

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(
                new[] { ("alpha", LongQueryTraceStep.Drop, OwnSession), ("alpha", LongQueryTraceStep.CreateAndStart, OwnSession) },
                rig.Steps);
        });
    }

    /// <summary>
    /// A registration with read-only intent reads a session that runs on its replica: the recreate stops it there, drops it
    /// on the primary, creates the definition there and starts it on the replica, the reconcile's own steps.
    /// </summary>
    [Fact]
    public async Task Azure_ReadOnlyIntent_StopsDropsCreatesAndStartsTheOwnSession()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "lqrecreate", Host = "lqrecreate.database.windows.net", Database = "alpha", ReadOnlyIntent = true },
                ConnectionString = "Server=azure;Database=alpha;ApplicationIntent=ReadOnly",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "lqrecreate",
                ServerId = LiveServerId,
            };

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(
                new[]
                {
                    ("alpha", LongQueryTraceStep.Stop, OwnSession),
                    ("alpha", LongQueryTraceStep.Drop, OwnSession),
                    ("alpha", LongQueryTraceStep.Check, OwnSession),
                    ("alpha", LongQueryTraceStep.CreateDefinition, OwnSession),
                    ("alpha", LongQueryTraceStep.Start, OwnSession),
                },
                rig.Steps);
        });
    }

    /// <summary>
    /// An engine that refuses or ignores the option leaves the session below the setting after a recreate that completed.
    /// That recreate is final for the process: the same read two hours later sends nothing.
    /// </summary>
    [Fact]
    public async Task ACompletedRecreate_IsNotRepeated_HoursLater()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            var server = OnPremServer();

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);
            Assert.Equal(2, rig.Steps.Count);

            rig.Clock = ReadAt.AddHours(2);
            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);
            Assert.Equal(2, rig.Steps.Count);
        });
    }

    [Fact]
    public async Task AFailedRecreate_IsTriedAgainAfterTheHour()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            var refuse = true;
            rig.Runner.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
            {
                rig.Steps.Add((database, step, session));
                return refuse && step == LongQueryTraceStep.CreateAndStart
                    ? Task.FromException(new InvalidOperationException("The create was refused."))
                    : Task.CompletedTask;
            };
            var server = OnPremServer();

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);
            Assert.Equal(2, rig.Steps.Count);

            rig.Clock = ReadAt.AddMinutes(30);
            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);
            Assert.Equal(2, rig.Steps.Count);

            refuse = false;
            rig.Clock = ReadAt.AddHours(2);
            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);
            Assert.Equal(4, rig.Steps.Count);
        });
    }

    /// <summary>
    /// The server's XE session database list keeps beta off the long-query create list. The read still reaches beta, but its
    /// older own session is left alone: no step names beta.
    /// </summary>
    [Fact]
    public async Task Azure_ADatabaseOffTheXeSessionDatabaseList_IsNotRecreated()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha", "beta" });
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "lqrecreate", Host = "lqrecreate.database.windows.net", XeSessionDatabases = new List<string> { "alpha" } },
                ConnectionString = "Server=azure;Database=master",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "lqrecreate",
                ServerId = LiveServerId,
            };

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(
                new[] { ("alpha", LongQueryTraceStep.Drop, OwnSession), ("alpha", LongQueryTraceStep.CreateAndStart, OwnSession) },
                rig.Steps);
        });
    }

    /// <summary>
    /// A registration with read-only intent whose replica refuses the start (it does not show the new definition yet): the
    /// log says the start is pending and that the next collection sweep starts it, and that sweep's long-query reconcile runs
    /// the create pass, which starts the session, within the hour the latch would otherwise wait. The sweep after it runs
    /// nothing more.
    /// </summary>
    [Fact]
    public async Task Azure_ReadOnlyIntent_AStartTheReplicaRefuses_IsLoggedAsPending()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            var server = ReadOnlyIntentServer();
            var state = await LatchedAsync(rig, server, ct);

            var refuseStart = true;
            rig.Runner.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
            {
                rig.Steps.Add((database, step, session));
                return refuseStart && step == LongQueryTraceStep.Start ? Task.FromException(StartNotVisible()) : Task.CompletedTask;
            };

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Contains("the replica refused its start for now, and the next collection sweep starts it", rig.Logger.Joined, StringComparison.Ordinal);
            Assert.Equal(0, rig.Logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

            /* The replica now shows the definition, stopped. */
            refuseStart = false;
            rig.Runner.LongQueryTraceReplicaStateForTests = (_, _) => new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);
            rig.Clock = ReadAt.AddMinutes(1);
            Assert.Equal(
                new[] { ("alpha", LongQueryTraceStep.Check, OwnSession), ("alpha", LongQueryTraceStep.Start, OwnSession) },
                await SweepAsync(rig, state, ct));

            rig.Clock = ReadAt.AddMinutes(2);
            Assert.Empty(await SweepAsync(rig, state, ct));
        });
    }

    /// <summary>
    /// The create pass that follows a pending start can meet the same refusal: the replica still does not show the definition
    /// the recreate put on the primary. That pass finds the definition already there and creates nothing, and its refused start
    /// is still pending, not a failure: no warning, no fault, and the sweep after it asks again. Each refusal asks once more,
    /// and the sweep where the replica shows the definition starts the session; the one after it sends nothing.
    /// </summary>
    [Fact]
    public async Task Azure_ReadOnlyIntent_AFollowUpPassWhoseStartIsRefusedAgain_RunsAgainOnTheNextSweep()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            var server = ReadOnlyIntentServer();
            var state = await LatchedAsync(rig, server, ct);

            /* The primary's definition, as the drop and the create leave it. */
            var onPrimary = true;
            var refuseStart = true;
            rig.Runner.LongQueryTraceDefinitionOnPrimaryForTests = (_, _) => onPrimary;
            rig.Runner.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
            {
                rig.Steps.Add((database, step, session));
                if (step == LongQueryTraceStep.Drop)
                {
                    onPrimary = false;
                }
                else if (step == LongQueryTraceStep.CreateDefinition)
                {
                    onPrimary = true;
                }

                return refuseStart && step == LongQueryTraceStep.Start ? Task.FromException(StartNotVisible()) : Task.CompletedTask;
            };

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);
            Assert.Contains(("alpha", LongQueryTraceStep.CreateDefinition, OwnSession), rig.Steps);

            var refusedStart = new[] { ("alpha", LongQueryTraceStep.Check, OwnSession), ("alpha", LongQueryTraceStep.Start, OwnSession) };
            foreach (var minutes in new[] { 1, 2 })
            {
                rig.Clock = ReadAt.AddMinutes(minutes);
                Assert.Equal(refusedStart, await SweepAsync(rig, state, ct));
                Assert.Null(state.LongQueryTraceFault);
                Assert.Equal(0, rig.Logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
            }

            /* The replica now shows the definition, stopped, and takes the start. */
            refuseStart = false;
            rig.Runner.LongQueryTraceReplicaStateForTests = (_, _) => new LongQueryTraceReplicaState(DefinitionExists: true, Running: false);
            rig.Clock = ReadAt.AddMinutes(3);
            Assert.Equal(refusedStart, await SweepAsync(rig, state, ct));

            rig.Clock = ReadAt.AddMinutes(4);
            Assert.Empty(await SweepAsync(rig, state, ct));
        });
    }

    /// <summary>
    /// On any engine the drop and the create run on separate connections, so the drop can succeed and the create fail. The
    /// session is then missing, and the next collection sweep's create pass creates it, not the hourly one.
    /// </summary>
    [Fact]
    public async Task OnPrem_TheDropSucceedsAndTheCreateFails_TheNextSweepCreatesTheSession()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            var server = OnPremServer();
            var state = await LatchedAsync(rig, server, ct);

            var refuseCreate = true;
            rig.Runner.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
            {
                rig.Steps.Add((database, step, session));
                return refuseCreate && step == LongQueryTraceStep.CreateAndStart
                    ? Task.FromException(new InvalidOperationException("A transient error."))
                    : Task.CompletedTask;
            };

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Equal(
                new[] { (string.Empty, LongQueryTraceStep.Drop, OwnSession), (string.Empty, LongQueryTraceStep.CreateAndStart, OwnSession) },
                rig.Steps);
            Assert.Contains("the next collection sweep creates it if it is missing", rig.Logger.Joined, StringComparison.Ordinal);

            refuseCreate = false;
            rig.Clock = ReadAt.AddMinutes(1);
            Assert.Equal(new[] { (string.Empty, LongQueryTraceStep.CreateAndStart, OwnSession) }, await SweepAsync(rig, state, ct));

            rig.Clock = ReadAt.AddMinutes(2);
            Assert.Empty(await SweepAsync(rig, state, ct));
        });
    }

    /// <summary>A recreate that completed leaves nothing for the next sweep: the latch holds and no create pass runs.</summary>
    [Fact]
    public async Task OnPrem_ACleanRecreate_AddsNoPassToTheNextSweep()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            var server = OnPremServer();
            var state = await LatchedAsync(rig, server, ct);

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);
            Assert.Equal(2, rig.Steps.Count);

            rig.Clock = ReadAt.AddMinutes(1);
            Assert.Empty(await SweepAsync(rig, state, ct));
        });
    }

    private static ServerRuntime ReadOnlyIntentServer() => new()
    {
        Config = new MonitoredServer { Name = "lqrecreate", Host = "lqrecreate.database.windows.net", Database = "alpha", ReadOnlyIntent = true },
        ConnectionString = "Server=azure;Database=alpha;ApplicationIntent=ReadOnly",
        Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
        StorageName = "lqrecreate",
        ServerId = LiveServerId,
    };

    private static Exception StartNotVisible() => LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(
        LongQueryTraceDatabases.EventSessionNotVisibleErrorNumber, 16, "Cannot alter the event session, because it does not exist or you do not have permission.");

    /// <summary>
    /// The worker's state for this server after the long-query reconcile ran and latched, as it is between the hourly passes:
    /// the session exists, and the next sweep's reconcile runs nothing unless something asked for it. The steps it sent are
    /// cleared.
    /// </summary>
    private static async Task<DarlingWorker.ServerLoopState> LatchedAsync(Rig rig, ServerRuntime server, CancellationToken ct)
    {
        rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });
        rig.Runner.LongQueryTraceListOverrideForTests = (_, _, _, _) => Task.FromResult(new List<string> { "alpha" });
        rig.Runner.LegacyLongQueryRecordsForTests = new LongQueryTraceLifecycleTests.InMemoryLegacyRecords();
        var state = new DarlingWorker.ServerLoopState { Config = server.Config, Runtime = server };

        rig.Clock = ReadAt.AddMinutes(-1);
        await SweepAsync(rig, state, ct);
        Assert.True(state.LongQueryTraceApplied);
        rig.Steps.Clear();
        rig.Clock = ReadAt;
        return state;
    }

    /// <summary>The long-query reconcile of one collection sweep, at the rig's clock; returns the steps it sent.</summary>
    private static async Task<List<(string Database, LongQueryTraceStep Step, string Session)>> SweepAsync(
        Rig rig, DarlingWorker.ServerLoopState state, CancellationToken ct)
    {
        var before = rig.Steps.Count;
        await DarlingWorker.ReconcileLongQueryTraceAsync(
            state, rig.Runner, enabled: true, Array.Empty<LongQueryTraceRegistration>(), Array.Empty<string>(), rig.Clock, rig.Logger, ct);
        return rig.Steps.Skip(before).ToList();
    }

    private static ServerRuntime OnPremServer() => new()
    {
        Config = new MonitoredServer { Name = "lqrecreate", Host = "lqrecreate-sql" },
        ConnectionString = "Server=onprem;Database=master",
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 16 },
        StorageName = "lqrecreate",
        ServerId = LiveServerId,
    };

    private static async Task WithRunnerAsync(Func<Rig, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live long-query recreate test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: rows are keyed by a distinctive fake server id and removed in the cleanup. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var logger = new CapturingTestLogger();
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator(), logger: logger, installId: () => InstallId, xeRingBufferMaxEvents: 5000);
        var rig = new Rig(runner, new List<(string, LongQueryTraceStep, string)>(), logger);
        runner.TargetProviderOverrideForTests = _ => new FakeTargetProvider();
        runner.LongQueryUtcNowForTests = () => rig.Clock;
        runner.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
        {
            rig.Steps.Add((database, step, session));
            return Task.CompletedTask;
        };

        var bodySucceeded = false;
        try
        {
            await body(rig, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var id = LiveServerId.ToString(CultureInfo.InvariantCulture);
        foreach (var sql in new[]
        {
            $"DELETE FROM collect.collector_state WHERE server_id = {id}",
            $"DELETE FROM long_query_completions WHERE server_id = {id}",
        })
        {
            using var command = new NpgsqlCommand(sql, connection);
            command.CommandTimeout = 30;
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /* One completed row (so the write runs), then the trailing row: the three truncation columns, then the position with the
       session's limit (1000). */
    private static DbDataReader LongQueryRead(string database)
    {
        var rows = Table(new object[]
        {
            ReadAt, "sql_batch_completed", 5_000_000L, 4_000_000L, 1L, 10L, 0L, 1L, "OK", "SELECT 1;", DBNull.Value,
            "SqlClient", 1234, 7, database, 1L, DBNull.Value, "0xABCD", "sa", 55,
        });
        var position = Table(new object[] { false, 0L, DBNull.Value, 40L, 10L, Created, 0L, 1000L, ReadAt });
        return new DataTableReader(new[] { rows, position });
    }

    private static DataTable Table(object[] row)
    {
        var table = new DataTable();
        for (var i = 0; i < row.Length; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), row[i] is DBNull ? typeof(string) : row[i].GetType());
        }

        table.Rows.Add(row);
        return table;
    }

    private sealed class FakeTargetProvider : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand((FakeConnection)connection);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;

        public string DatabaseRead => _cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "SO";

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _cs; set => _cs = value ?? string.Empty; }
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        public override Task OpenAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class FakeCommand(FakeConnection owner) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get => owner; set { } }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => null;
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => LongQueryRead(owner.DatabaseRead);
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult(LongQueryRead(owner.DatabaseRead));
    }
}
