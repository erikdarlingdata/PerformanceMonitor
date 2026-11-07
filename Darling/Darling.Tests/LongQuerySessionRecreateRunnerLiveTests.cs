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
    /// log says the start is pending, not that the session was recreated and runs.
    /// </summary>
    [Fact]
    public async Task Azure_ReadOnlyIntent_AStartTheReplicaRefuses_IsLoggedAsPending()
    {
        await WithRunnerAsync(async (rig, ct) =>
        {
            rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });
            rig.Runner.LongQueryTraceStepOverrideForTests = (_, database, _, step, session, _) =>
            {
                rig.Steps.Add((database, step, session));
                return step == LongQueryTraceStep.Start
                    ? Task.FromException(LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(
                        LongQueryTraceDatabases.EventSessionNotVisibleErrorNumber, 16, "Cannot alter the event session, because it does not exist or you do not have permission."))
                    : Task.CompletedTask;
            };
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "lqrecreate", Host = "lqrecreate.database.windows.net", Database = "alpha", ReadOnlyIntent = true },
                ConnectionString = "Server=azure;Database=alpha;ApplicationIntent=ReadOnly",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "lqrecreate",
                ServerId = LiveServerId,
            };

            await rig.Runner.RunAsync(LongQueryCompletionsCollector.Instance, server, ct);

            Assert.Contains("the replica refused its start for now", rig.Logger.Joined, StringComparison.Ordinal);
            Assert.Equal(0, rig.Logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
        });
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
        runner.LongQuerySessionRecreateUtcNowForTests = () => rig.Clock;
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
