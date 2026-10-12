/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
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
/// Drives the host's REAL Azure per-database loop through a failed store write. A database's deadlock cursors
/// must be saved only after that database's rows are written: a database whose write throws keeps its old
/// cursor, a sibling's success advances only its own, and the telemetry cursor (read from <c>master</c>) stays
/// put when master's own write fails. Calling <c>LandStagedItemState</c> by hand, or reading the source order,
/// cannot show an exception path landing the cursor early; running the loop can.
/// </summary>
[Collection("live-postgres")]
public sealed class DeadlocksAzureLoopStateLiveTests
{
    private const int LiveServerId = -490002;
    private static readonly DateTime T0 = new(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T1 = new(2026, 8, 26, 12, 3, 0, DateTimeKind.Utc);
    private static string Iso(DateTime t) => t.ToString("o", CultureInfo.InvariantCulture);

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task AFailedDatabaseWrite_KeepsThatDatabasesCursors_WhileASiblingsSuccessAdvancesItsOwn(
        bool masterFaults, bool telemetryAdvanced)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Azure loop state test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the rows are keyed by a distinctive fake server id and removed in the cleanup. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var name = DeadlocksCollector.Instance.Name;

        var bodySucceeded = false;
        try
        {
            await runner.SaveCollectorStateAsync(LiveServerId, name, new Dictionary<string, string>
            {
                [DeadlocksCollector.TelemetryCursorStateKey] = Iso(T0),
                ["dl_ring_cursor:alpha"] = Iso(T0),
                ["dl_ring_cursor:zeta"] = Iso(T0),
            }, ct);

            /* The source-database column sits after the optional plan column; follow the host's own gate. */
            var withPlan = runner.ShouldCapturePlanXmlFor(name, LiveServerId);
            object[] Row(string graph, object source) => withPlan
                ? new object[] { T1, "process1", graph, DBNull.Value, source }
                : new object[] { T1, "process1", graph, source };
            var master = new[] { Row("<deadlock id='m'/>", "alpha") };
            var alpha = new[] { Row("<deadlock id='a'/>", DBNull.Value) };
            var zeta = new[] { Row("<deadlock id='z'/>", DBNull.Value) };
            var provider = new FakeTargetProvider(new Dictionary<string, object[][]>
            {
                ["master"] = master, ["alpha"] = alpha, ["zeta"] = zeta,
            });

            runner.TargetProviderOverrideForTests = _ => provider;
            runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", "alpha", "zeta" });
            runner.PerDatabaseWriteFaultForTests = db =>
            {
                if (db == "zeta" || (masterFaults && db == "master"))
                {
                    throw new InvalidOperationException("injected write failure for " + db);
                }
            };

            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "t", Host = "h" },
                ConnectionString = "Server=azure;Database=master",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
                StorageName = "h",
                ServerId = LiveServerId,
            };

            /* Not every database failed, so the run itself must not throw. */
            await runner.RunAsync(DeadlocksCollector.Instance, server, ct);

            var state = await runner.GetCollectorStateAsync(LiveServerId, name, ct);
            Assert.Equal(Iso(T1), state["dl_ring_cursor:alpha"]);
            Assert.Equal(Iso(T0), state["dl_ring_cursor:zeta"]);
            Assert.Equal(telemetryAdvanced ? Iso(T1) : Iso(T0), state[DeadlocksCollector.TelemetryCursorStateKey]);

            var graphs = await GraphsAsync(connection, ct);
            Assert.Contains("<deadlock id='a'/>", graphs);
            Assert.DoesNotContain("<deadlock id='z'/>", graphs);
            Assert.Equal(masterFaults ? 1 : 2, graphs.Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<List<string>> GraphsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var graphs = new List<string>();
        using var command = new NpgsqlCommand(
            $"SELECT deadlock_graph_xml FROM deadlocks WHERE server_id = {LiveServerId.ToString(CultureInfo.InvariantCulture)}", connection);
        command.CommandTimeout = 30;
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            graphs.Add(reader.GetString(0));
        }

        return graphs;
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var id = LiveServerId.ToString(CultureInfo.InvariantCulture);
        foreach (var sql in new[]
        {
            $"DELETE FROM collect.collector_state WHERE server_id = {id}",
            $"DELETE FROM deadlocks WHERE server_id = {id}",
        })
        {
            using var command = new NpgsqlCommand(sql, connection);
            command.CommandTimeout = 30;
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private sealed class FakeTargetProvider(Dictionary<string, object[][]> rowsByDatabase) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString, rowsByDatabase);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand((FakeConnection)connection);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString, Dictionary<string, object[][]> rowsByDatabase) : DbConnection
    {
        private string _cs = connectionString;
        public object[][] Rows => rowsByDatabase[_cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "master"];

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
        protected override DbConnection? DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => Open();
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult(Open());

        private DbDataReader Open() => new Reader(owner.Rows, new[] { new object[] { 100L, false } });
    }

    private sealed class Reader(object[][] rows, object[][] gate) : DbDataReader
    {
        private readonly object[][][] _sets = { rows, gate };
        private int _set;
        private int _row = -1;
        private object[] Cur => _sets[_set][_row];

        public override bool Read() => ++_row < _sets[_set].Length;
        public override bool NextResult() { if (_set + 1 >= _sets.Length) { return false; } _set++; _row = -1; return true; }
        public override string GetString(int o) => (string)Cur[o];
        public override long GetInt64(int o) => (long)Cur[o];
        public override DateTime GetDateTime(int o) => (DateTime)Cur[o];
        public override bool GetBoolean(int o) => (bool)Cur[o];
        public override bool IsDBNull(int o) => Cur[o] is DBNull;
        public override object GetValue(int o) => Cur[o];
        public override int FieldCount => _sets[_set].Length == 0 ? 0 : _sets[_set][0].Length;
        public override bool HasRows => _sets[_set].Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int o] => Cur[o];
        public override object this[string n] => throw new NotSupportedException();
        public override byte GetByte(int o) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public override char GetChar(int o) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int bo, int l) => throw new NotSupportedException();
        public override string GetDataTypeName(int o) => throw new NotSupportedException();
        public override decimal GetDecimal(int o) => throw new NotSupportedException();
        public override double GetDouble(int o) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int o) => throw new NotSupportedException();
        public override float GetFloat(int o) => throw new NotSupportedException();
        public override Guid GetGuid(int o) => throw new NotSupportedException();
        public override short GetInt16(int o) => throw new NotSupportedException();
        public override int GetInt32(int o) => throw new NotSupportedException();
        public override string GetName(int o) => throw new NotSupportedException();
        public override int GetOrdinal(string n) => throw new NotSupportedException();
        public override int GetValues(object[] v) => throw new NotSupportedException();
    }
}
