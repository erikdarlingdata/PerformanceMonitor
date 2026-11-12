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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7490: runs the host's REAL <see cref="DarlingCollectorRunner.RunAsync"/> for query_stats on a fake SQL Server target
/// that answers the main read, the text fetch and the plan fetch, against a real scratch store whose write is made to
/// fail. query_stats' read advances every delta baseline (#7424), so a write that fails must hand them back (#7419), and
/// the digests the run held as pending must be discarded, not confirmed.
///
/// <para>A trigger on the target table that raises fails the COPY (COPY fires row triggers), and a trigger on the hour
/// ledger table fails the write after the COPY, in the same transaction. The first test asserts the phase
/// <see cref="CollectorFaultCopyPhase.For"/> reports for the COPY fault before it relies on it.</para>
///
/// <para><b>#1776 own-store:</b> every store test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so none is in the <c>live-postgres</c> collection. The scratch database is a clone of the
/// migrated template, so these tests never call <c>PgMigrations.MigrateAsync</c> (#7489 is a deadlock on that call).</para>
/// </summary>
public sealed class QueryStatsWriteFaultRunnerLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the query_stats write-fault pins (each mints its own scratch database).";

    private const string DeltaKey = "0x03AB:0:-1:0x06AB";

    /// <summary>What the last stored run left, per counter family: every one at or below the fake statement's counter, so no family reads as a reset.</summary>
    private static readonly (string Group, long Value)[] BaselineGroups =
    {
        ("query_stats_exec", 100), ("query_stats_worker", 1000), ("query_stats_elapsed", 2000), ("query_stats_reads", 0),
        ("query_stats_writes", 0), ("query_stats_phys_reads", 0), ("query_stats_rows", 0), ("query_stats_spills", 0),
    };

    /// <summary>The counters the last stored run left, five minutes before the run.</summary>
    private static CollectorDeltaCalculator SeededDeltas(int serverId)
    {
        var deltas = new CollectorDeltaCalculator();
        foreach (var (group, value) in BaselineGroups)
        {
            deltas.CalculateDeltaWithSeriesAge(
                serverId, group, DeltaKey, value, null, out _, collectionTime: DateTime.UtcNow.AddMinutes(-5), maxGapSeconds: 3600);
        }

        return deltas;
    }

    private static ServerRuntime MakeServer(int serverId) => new()
    {
        Config = new MonitoredServer { Name = "wf-qs", Host = "wf-qs" },
        ConnectionString = "Server=onprem;Database=master",
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
        StorageName = "wf-qs",
        ServerId = serverId,
    };

    private static string RowsOf(int serverId) =>
        "SELECT count(*) FROM collect.query_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture);

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }

        public required NpgsqlDataSource DataSource { get; init; }

        public required CollectorDeltaCalculator Deltas { get; init; }

        public required FakeTarget Target { get; init; }

        public required DarlingCollectorRunner Runner { get; init; }

        public required ServerRuntime Server { get; init; }

        public static async Task<Rig> OpenAsync(int serverId, CancellationToken ct)
        {
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            var deltas = SeededDeltas(serverId);
            var target = new FakeTarget();
            /* The service signs in with the store's search path, which the unqualified COPY target needs. */
            var dataSource = NpgsqlDataSource.Create(DarlingStoreConnection.EnsureStoreSearchPath(scratch.ConnectionString));
            return new Rig
            {
                Scratch = scratch,
                DataSource = dataSource,
                Deltas = deltas,
                Target = target,
                Server = MakeServer(serverId),
                Runner = new DarlingCollectorRunner(dataSource, deltas) { TargetProviderOverrideForTests = _ => new FakeTargetProvider(target) },
            };
        }

        public Task<CollectorRunResult> RunAsync(CancellationToken ct) => Runner.RunAsync(QueryStatsCollector.Instance, Server, ct);

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>A trigger that raises on every row written to <paramref name="table"/>.</summary>
    private static Task FailWritesAsync(string connectionString, string table, string events, CancellationToken ct) =>
        ExecAsync(
            connectionString,
            "CREATE FUNCTION collect.wf_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'write fault'; END $$; "
            + "CREATE TRIGGER wf_fail BEFORE " + events + " ON collect." + table + " FOR EACH ROW EXECUTE FUNCTION collect.wf_fail();",
            ct);

    private static Task StopFailingWritesAsync(string connectionString, string table, CancellationToken ct) =>
        ExecAsync(connectionString, "DROP TRIGGER wf_fail ON collect." + table + "; DROP FUNCTION collect.wf_fail();", ct);

    private static async Task<Exception> FaultOfAsync(Task<CollectorRunResult> run)
    {
        try
        {
            await run;
        }
        catch (Exception fault)
        {
            return fault;
        }

        Assert.Fail("the run was expected to fault and returned");
        return null!;
    }

    [Fact]
    public async Task ACopyThatFails_PutsTheBaselinesBack_AndDiscardsThePendingDigests()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(-490101, ct);
        var bodySucceeded = false;
        try
        {
            var connectionString = rig.Scratch.ConnectionString;
            await FailWritesAsync(connectionString, "query_stats", "INSERT", ct);

            var fault = await FaultOfAsync(rig.RunAsync(ct));

            /* The phase is asserted, not assumed: a COPY row trigger surfaces from the data phase, and any phase but Unknown rolls back. */
            Assert.Equal(StoreCopyPhase.Data, CollectorFaultCopyPhase.For(fault));
            Assert.Equal(1, rig.Target.TextFetches);
            Assert.Equal(1, rig.Target.PlanFetches);
            Assert.Equal(100, rig.Deltas.PeekBaselines(rig.Server.ServerId, "query_stats_exec")[DeltaKey]);

            /* The pending text and plan digests were discarded, so the next run reads both again and stores the row. */
            await StopFailingWritesAsync(connectionString, "query_stats", ct);
            await rig.RunAsync(ct);

            Assert.Equal(2, rig.Target.TextFetches);
            Assert.Equal(2, rig.Target.PlanFetches);
            Assert.Equal(1, await CountAsync(connectionString, RowsOf(rig.Server.ServerId), ct));
            Assert.Equal(160, rig.Deltas.PeekBaselines(rig.Server.ServerId, "query_stats_exec")[DeltaKey]);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AFaultAfterTheCopy_KeepsTheAdvancedBaselines()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(-490102, ct);
        var bodySucceeded = false;
        try
        {
            var connectionString = rig.Scratch.ConnectionString;

            /* The hour ledger's upsert runs after the COPY, in the same transaction, in the region no phase stamp covers. */
            await FailWritesAsync(connectionString, "query_stats_hour_ledger", "INSERT OR UPDATE", ct);

            var fault = await FaultOfAsync(rig.RunAsync(ct));

            Assert.Equal(StoreCopyPhase.Unknown, CollectorFaultCopyPhase.For(fault));
            Assert.Equal(1, rig.Target.TextFetches);
            Assert.Equal(160, rig.Deltas.PeekBaselines(rig.Server.ServerId, "query_stats_exec")[DeltaKey]);

            /* The transaction rolled back, so the COPY's row is not stored. */
            Assert.Equal(0, await CountAsync(connectionString, RowsOf(rig.Server.ServerId), ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ACommittedWrite_ConfirmsTheDigests_SoTheNextRunReadsNoText()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(-490103, ct);
        var bodySucceeded = false;
        try
        {
            await rig.RunAsync(ct);
            Assert.Equal(1, rig.Target.TextFetches);
            Assert.Equal(1, rig.Target.PlanFetches);

            /* The statement ran again, so the second run keeps its row, and the committed digests answer its text and plan. */
            rig.Target.Executions = 220;
            await rig.RunAsync(ct);

            Assert.Equal(1, rig.Target.TextFetches);
            Assert.Equal(1, rig.Target.PlanFetches);
            Assert.Equal(2, await CountAsync(rig.Scratch.ConnectionString, RowsOf(rig.Server.ServerId), ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreConnectionThatFailsToOpenAtTheWrite_PutsTheBaselinesBack()
    {
        const int serverId = -490104;
        var ct = TestContext.Current.CancellationToken;
        var deltas = SeededDeltas(serverId);
        var target = new FakeTarget();

        /* Nothing listens on port 1: the open at the write is refused, after the read advanced every baseline and before a row was sent. */
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Timeout=1;Username=x;Database=x");
        var runner = new DarlingCollectorRunner(postgres, deltas) { TargetProviderOverrideForTests = _ => new FakeTargetProvider(target) };

        var fault = await FaultOfAsync(runner.RunAsync(QueryStatsCollector.Instance, MakeServer(serverId), ct));

        Assert.IsNotType<PostgresException>(fault);
        Assert.Equal(StoreCopyPhase.Unknown, CollectorFaultCopyPhase.For(fault));
        Assert.Equal(1, target.MainReads);
        Assert.Equal(1, target.TextFetches);
        Assert.Equal(1, target.PlanFetches);
        Assert.Equal(100, deltas.PeekBaselines(serverId, "query_stats_exec")[DeltaKey]);
    }

    /// <summary>What the fake target has been asked, and the execution count its one statement reports.</summary>
    private sealed class FakeTarget
    {
        public long Executions { get; set; } = 160;

        public int MainReads;

        public int TextFetches;

        public int PlanFetches;
    }

    private sealed class FakeTargetProvider(FakeTarget state) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;
        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);
        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand(query.Text, (FakeConnection)connection, state);
        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;
        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;
        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;

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

    private sealed class FakeCommand(string text, FakeConnection owner, FakeTarget state) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = text;
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
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            if (CommandText.Contains("sys.dm_exec_sql_text(k.plan_handle)", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref state.TextFetches);
                return Task.FromResult<DbDataReader>(TextFetch());
            }

            if (CommandText.Contains("sys.dm_exec_text_query_plan", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref state.PlanFetches);
                return Task.FromResult<DbDataReader>(PlanFetch());
            }

            Interlocked.Increment(ref state.MainReads);
            return Task.FromResult<DbDataReader>(MainQuery(DateTime.UtcNow, state.Executions));
        }

        /// <summary>The text fetch's result: (ord, text) for the one key.</summary>
        private static DbDataReader TextFetch()
        {
            var table = new DataTable();
            table.Columns.Add("ord", typeof(int));
            table.Columns.Add("text", typeof(string));
            table.Rows.Add(0, "SELECT 1 FROM dbo.WriteFault;");
            return table.CreateDataReader();
        }

        /// <summary>The plan fetch's result: (ord, plan, bytes) for the one key.</summary>
        private static DbDataReader PlanFetch()
        {
            const string plan = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence /></ShowPlanXML>";
            var table = new DataTable();
            table.Columns.Add("ord", typeof(int));
            table.Columns.Add("plan", typeof(string));
            table.Columns.Add("bytes", typeof(long));
            table.Rows.Add(0, plan, (long)plan.Length);
            return table.CreateDataReader();
        }

        /// <summary>One statement at <paramref name="executions"/> executions, in the 44-column shape of the candidate query.</summary>
        private static DbDataReader MainQuery(DateTime at, long executions)
        {
            var table = new DataTable();
            var stringColumns = new HashSet<int> { 0, 1, 2, 36, 37, 38, 42 };
            for (var i = 0; i < 44; i++)
            {
                var type = stringColumns.Contains(i) ? typeof(string)
                    : i is 3 or 4 ? typeof(DateTime)
                    : i is 40 or 41 or 43 ? typeof(int)
                    : typeof(long);
                table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), type);
            }

            var values = new object?[44];
            for (var i = 0; i < 44; i++)
            {
                values[i] = table.Columns[i].DataType == typeof(long) ? 0L : DBNull.Value;
            }

            values[0] = "app";
            values[1] = "0x01";
            /* A fixed creation time: it is part of the plan's identity, which the digest caches key on. */
            values[3] = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            values[4] = at;
            values[5] = executions;
            values[6] = 1600L;
            values[7] = 3200L;
            values[36] = "0x03AB";
            values[37] = "0x06AB";
            values[40] = 0;
            values[41] = -1;
            values[43] = 3600;
            table.Rows.Add(values);
            return table.CreateDataReader();
        }
    }
}
