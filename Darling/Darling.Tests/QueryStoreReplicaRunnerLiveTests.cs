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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254 / #6279: drives the host's REAL enumerated Query Store loop with a fake target that applies the cutoff the way the SQL
/// does (<c>last_execution_time &gt; @cutoff_time</c>) and tells the primary connection from the replica's. A late row that lands
/// between W - L and W must be stored, a row already shipped must not be written again, the replica's rows land under the
/// primary's server id, a setting that is empty opens no replica connection, and a replica that fails costs nothing but its reason.
///
/// <para><b>#1776 own-store</b> - every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so none is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreReplicaRunnerLiveTests
{
    private const int LiveServerId = -625400;
    private const string Db = "AgDb";
    private const string PrimaryConnection = "Server=primary";
    private const string ReplicaConnection = "Server=replica";
    private static readonly Guid GroupDatabaseId = Guid.Parse("7a88cd1f-6fcf-444c-a320-feaed6e9a6a1");

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static ServerRuntime MakeServer(string? replica) => new()
    {
        Config = new MonitoredServer { Name = "t", Host = "h", QueryStoreReplica = replica },
        ConnectionString = PrimaryConnection,
        Target = new CollectorTargetInfo { IsAzureSqlDb = false, Engine = CollectorTargetEngine.SqlServer },
        StorageName = "h",
        ServerId = LiveServerId,
    };

    /// <summary>What the fake servers hold and what happened to them.</summary>
    private sealed class Target
    {
        public List<object[]> Rows { get; } = new();
        public int FlushSeconds { get; set; } = 60;
        public bool ReplicaOpenFails { get; set; }
        public bool ReplicaReadFails { get; set; }
        public List<string> ConnectionsOpened { get; } = new();
        public int ReplicaReads;
        public List<DateTime> Cutoffs { get; } = new();
        public int PrimaryReads;
        public DateTime CommitTime { get; set; } = DateTime.UtcNow;
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
            {
                lock (Lines)
                {
                    Lines.Add(formatter(state, exception));
                }
            }
        }

        public string Text { get { lock (Lines) { return string.Join(" | ", Lines); } } }
    }

    private sealed class Rig : IAsyncDisposable
    {
        public ScratchPostgres Scratch = null!;
        public NpgsqlDataSource Source = null!;
        public DarlingCollectorRunner Runner = null!;
        public CapturingLogger Log = new();

        public static async Task<Rig> OpenAsync(Target target, CancellationToken ct)
        {
            var rig = new Rig();
            rig.Scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            await using (var migrate = new NpgsqlConnection(rig.Scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            rig.Source = new NpgsqlDataSourceBuilder(rig.Scratch.ConnectionString).Build();
            rig.Runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log);
            var provider = new FakeTargetProvider(target);
            rig.Runner.TargetProviderOverrideForTests = _ => provider;
            rig.Runner.QueryStoreReplicaConnectionStringOverrideForTests = _ => ReplicaConnection;
            return rig;
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One runtime-stats row for interval <paramref name="id"/>, last executed at <paramref name="lastUtc"/>.</summary>
    private static object[] Row(long id, DateTime lastUtc, string? replicaRole = null, long executions = 1)
    {
        var stamp = new DateTimeOffset(lastUtc, TimeSpan.Zero);
        var row = new object[56];
        Array.Fill(row, DBNull.Value);
        row[0] = id;
        row[1] = id;
        row[2] = "Regular";
        row[3] = stamp;
        row[4] = stamp;
        row[7] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        row[8] = executions;
        row[51] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        row[52] = (object?)replicaRole ?? DBNull.Value;
        row[53] = id;
        return row;
    }

    private static DateTime WholeSeconds(DateTime value) => new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);

    private static async Task<Dictionary<long, long>> StoredAsync(Rig rig, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT runtime_stats_interval_id, count(*) FROM collect.query_store_stats WHERE server_id = @s AND database_name = @d GROUP BY 1",
            connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        command.Parameters.AddWithValue("d", Db);
        var result = new Dictionary<long, long>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            result[reader.GetInt64(0)] = reader.GetInt64(1);
        }

        return result;
    }

    /// <summary>
    /// T5: with a replica set, run 1 shows row A at W; run 2 shows A again and a late row B older than W. B reaches the store
    /// (the cutoff is W - L, not W), A is not written twice (the ship guard), the rows are the primary's server id, and the
    /// run says where it read.
    /// </summary>
    [Fact]
    public async Task AReplicaRead_StoresALateRowBelowTheWatermark_NeverWritesAShippedRowAgain_UnderThePrimarysServerId()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        var w = WholeSeconds(DateTime.UtcNow.AddMinutes(-20));
        target.Rows.Add(Row(1, w));

        var first = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.Equal(1, first.Rows);
        Assert.Equal("replica 1, primary 0", first.ReadSource);

        /* B first executed three minutes before A's time: below the watermark, inside the 6-minute look-back (60 s flush + 5). */
        target.Rows.Add(Row(2, w.AddMinutes(-3)));
        var second = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        var stored = await StoredAsync(rig, token);
        Assert.Equal(new Dictionary<long, long> { [1] = 1, [2] = 1 }, stored);
        Assert.Equal(1, second.Rows);
        Assert.Equal("replica 1, primary 0", second.ReadSource);
        Assert.Equal(0, target.PrimaryReads);
        Assert.Equal(2, target.ReplicaReads);

        /* Nothing new: nothing ships, and the guard does not let the look-back re-read write A or B again. */
        var third = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);
        Assert.Equal(0, third.Rows);
        Assert.Equal(new Dictionary<long, long> { [1] = 1, [2] = 1 }, await StoredAsync(rig, token));
    }

    /// <summary>
    /// T6: the setting is empty and no secondary captures: no replica connection opens, nothing reads the replica, the
    /// run reports no read source, and a row older than the watermark is NOT re-read (today's cutoff exactly).
    /// </summary>
    [Fact]
    public async Task WithTheSettingEmptyAndNoCapturingSecondary_NoReplicaConnectionOpens_AndTheCutoffIsTodays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        var w = WholeSeconds(DateTime.UtcNow.AddMinutes(-20));
        target.Rows.Add(Row(1, w));
        var first = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);
        target.Rows.Add(Row(2, w.AddMinutes(-3)));
        var second = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);

        Assert.Equal(1, first.Rows);
        Assert.Null(first.ReadSource);
        Assert.Null(second.ReadSource);
        Assert.DoesNotContain(ReplicaConnection, target.ConnectionsOpened);
        Assert.Equal(0, target.ReplicaReads);
        Assert.Equal(0, second.Rows);
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, token));
    }

    /// <summary>
    /// #6279: the setting is empty, but rows arrive with a non-primary replica role (a capturing secondary's). The next read of
    /// that database uses the look-back: a late row below the watermark is stored, a shipped one is not written twice, and still
    /// no replica connection opens.
    /// </summary>
    [Fact]
    public async Task ACapturingSecondaryOnTheSettingEmptyPath_GetsTheLookBack_WithoutAReplicaConnection()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target();
        await using var rig = await Rig.OpenAsync(target, token);

        var w = WholeSeconds(DateTime.UtcNow.AddMinutes(-40));
        target.Rows.Add(Row(1, w, replicaRole: "Secondary"));
        await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);

        /* Late by 15 minutes: inside the capturing look-back (900 s flush + 10 minutes = 25 minutes), far below W. */
        target.Rows.Add(Row(2, w.AddMinutes(-15), replicaRole: "Secondary"));
        var second = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);

        /* The guard was empty on the first look-back read (run 1 had none), so A is written once more and B, the late row, lands: the
           one rewrite at activation the design allows. From here the guard knows both. */
        Assert.Equal(new Dictionary<long, long> { [1] = 2, [2] = 1 }, await StoredAsync(rig, token));
        Assert.Equal(2, second.Rows);
        Assert.Null(second.ReadSource);

        var third = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(null), token);
        Assert.Equal(0, third.Rows);
        Assert.Equal(new Dictionary<long, long> { [1] = 2, [2] = 1 }, await StoredAsync(rig, token));
        Assert.DoesNotContain(ReplicaConnection, target.ConnectionsOpened);
    }

    /// <summary>A replica read that fails reads the primary in the same run: the rows are stored once, and the run names the reason.</summary>
    [Fact]
    public async Task AReplicaReadThatFails_IsRetriedOnThePrimaryInTheSameRun_AndSaysWhy()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target { ReplicaReadFails = true };
        await using var rig = await Rig.OpenAsync(target, token);

        target.Rows.Add(Row(1, WholeSeconds(DateTime.UtcNow.AddMinutes(-20))));
        var result = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        Assert.Equal(1, result.Rows);
        Assert.Equal(1, target.PrimaryReads);
        Assert.Equal("replica 0, primary 1 (replica_unavailable: " + Db + ")", result.ReadSource);
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, token));
    }

    /// <summary>A replica that cannot be opened is every database on the primary, with the same reason, and the run still succeeds.</summary>
    [Fact]
    public async Task AReplicaThatCannotBeOpened_SendsEveryDatabaseToThePrimary()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        var target = new Target { ReplicaOpenFails = true };
        await using var rig = await Rig.OpenAsync(target, token);

        target.Rows.Add(Row(1, WholeSeconds(DateTime.UtcNow.AddMinutes(-20))));
        var result = await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer("replica-host"), token);

        Assert.Equal(1, result.Rows);
        Assert.Equal(0, target.ReplicaReads);
        Assert.Equal("replica 0, primary 1 (replica_unavailable: " + Db + ")", result.ReadSource);
    }

    /// <summary>The run's read source reaches <c>collection_log.read_source</c> through the one writer; NULL when none is given.</summary>
    [Fact]
    public async Task TheReadSource_IsWrittenToTheCollectionLog_AndIsNullWhenAbsent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #6254 runner pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(new Target(), token);
        var server = MakeServer("replica-host");

        await DarlingObservability.LogCollectionAsync(
            rig.Source, server, "query_store", "SUCCESS", 1, 1, 1, null, null, null, null, null, null, null, token,
            readSource: "replica 3, primary 1 (redo_behind: Sales)");
        await DarlingObservability.LogCollectionAsync(
            rig.Source, server, "wait_stats", "SUCCESS", 1, 1, 1, null, null, null, null, null, null, null, token);

        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT collector_name, read_source FROM collection_log WHERE server_id = @s ORDER BY collector_name", connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        var seen = new Dictionary<string, string?>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            seen[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        Assert.Equal("replica 3, primary 1 (redo_behind: Sales)", seen["query_store"]);
        Assert.Null(seen["wait_stats"]);
    }

    // ---- the fake target ----------------------------------------------------------------------------------------------------

    private sealed class FakeTargetProvider(Target target) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString, target);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand((FakeConnection)connection, target, query);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => connectionString;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString, Target target) : DbConnection
    {
        private string _cs = connectionString;
        public bool IsReplica => _cs == ReplicaConnection;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _cs; set => _cs = value ?? string.Empty; }
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            lock (target.ConnectionsOpened)
            {
                target.ConnectionsOpened.Add(_cs);
            }

            return IsReplica && target.ReplicaOpenFails
                ? Task.FromException(new InvalidOperationException("the replica is down"))
                : Task.CompletedTask;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class FakeCommand(FakeConnection owner, Target target, CollectorQuery query) : DbCommand
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
        public override object? ExecuteScalar() =>
            query.Text == QueryStoreCollector.ProductVersionProbeText ? 16 : throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var text = query.Text;
            if (text == QueryStoreCollector.ProductVersionProbeText)
            {
                return Task.FromResult<DbDataReader>(new Reader([[16]]));
            }

            if (text.Contains("sys.dm_hadr_database_replica_states", StringComparison.Ordinal))
            {
                var redo = new DataTable();
                redo.Columns.Add("database_name", typeof(string));
                redo.Columns.Add("group_database_id", typeof(Guid));
                redo.Columns.Add("last_commit_time", typeof(DateTime));
                redo.Columns.Add("is_primary_replica", typeof(bool));
                redo.Rows.Add(Db, GroupDatabaseId, target.CommitTime, !owner.IsReplica);
                return Task.FromResult<DbDataReader>(redo.CreateDataReader());
            }

            var cutoff = query.Parameters?.FirstOrDefault(static p => p.Name == "@cutoff_time")?.Value as DateTime?;
            if (cutoff is null && text.Contains("sys.databases", StringComparison.Ordinal))
            {
                return Task.FromResult<DbDataReader>(Enumeration(owner.IsReplica));
            }

            if (cutoff is null)
            {
                /* The plan-XML and statement-text fetches: nothing to fetch for these rows. */
                return Task.FromResult<DbDataReader>(new Reader(Array.Empty<object[]>()));
            }

            if (owner.IsReplica)
            {
                Interlocked.Increment(ref target.ReplicaReads);
                if (target.ReplicaReadFails)
                {
                    return Task.FromException<DbDataReader>(new InvalidOperationException("the replica read failed"));
                }
            }
            else
            {
                Interlocked.Increment(ref target.PrimaryReads);
            }

            lock (target.Cutoffs)
            {
                target.Cutoffs.Add(cutoff.Value);
            }

            /* The real predicate: last_execution_time > @cutoff_time. */
            var floor = DateTime.SpecifyKind(cutoff.Value, DateTimeKind.Utc);
            return Task.FromResult<DbDataReader>(
                new Reader(target.Rows.Where(r => ((DateTimeOffset)r[4]).UtcDateTime > floor).ToArray()));
        }

        /// <summary>The database list: the primary's three result sets (names, probe failures, flush facts), or the replica's two.</summary>
        private DbDataReader Enumeration(bool replica)
        {
            var failures = new DataTable();
            failures.Columns.Add("item_name", typeof(string));
            failures.Columns.Add("error_text", typeof(string));
            if (replica)
            {
                var list = new DataTable();
                list.Columns.Add("name", typeof(string));
                list.Columns.Add("actual_state", typeof(int));
                list.Columns.Add("readonly_reason", typeof(int));
                list.Columns.Add("flush_interval_seconds", typeof(int));
                list.Rows.Add(Db, 0, 8, target.FlushSeconds);
                return new DataTableReader([list, failures]);
            }

            var names = new DataTable();
            names.Columns.Add("name", typeof(string));
            names.Rows.Add(Db);
            var facts = new DataTable();
            facts.Columns.Add("name", typeof(string));
            facts.Columns.Add("flush_interval_seconds", typeof(int));
            facts.Rows.Add(Db, target.FlushSeconds);
            return new DataTableReader([names, failures, facts]);
        }
    }

    private sealed class Reader(object[][] rows) : DbDataReader
    {
        private int _row = -1;
        private object[] Cur => rows[_row];

        public override bool Read() => ++_row < rows.Length;
        public override bool NextResult() => false;
        public override string GetString(int o) => (string)Cur[o];
        public override long GetInt64(int o) => Convert.ToInt64(Cur[o], System.Globalization.CultureInfo.InvariantCulture);
        public override DateTime GetDateTime(int o) => (DateTime)Cur[o];
        public override bool GetBoolean(int o) => (bool)Cur[o];
        public override bool IsDBNull(int o) => Cur[o] is DBNull;
        public override object GetValue(int o) => Cur[o];
        public override int FieldCount => rows.Length == 0 ? 0 : rows[0].Length;
        public override bool HasRows => rows.Length > 0;
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
