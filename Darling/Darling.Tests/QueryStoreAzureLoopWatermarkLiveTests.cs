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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5514: drives the host's REAL Azure per-database loop for query_store and checks what each fault leaves in the
/// per-database watermark cache. A fault AFTER a database's batch was staged (its write threw, and may or may not
/// have committed) drops that database's key, so the next resolve reads the store; a fault BEFORE the stage (the
/// read itself threw, nothing was written) keeps the key, so a database that fails every cycle is not a store read
/// every cycle. A sibling database that succeeded keeps its advanced key and equals the bounded store read. The
/// enumerated arm has the same pins in <see cref="DatabaseWatermarkCacheRunnerLiveTests"/>; this is the Azure arm's.
///
/// <para><b>#1776 own-store</b> - every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so none is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreAzureLoopWatermarkLiveTests
{
    private const int LiveServerId = -551400;
    private const string Alpha = "alpha";
    private const string Zeta = "zeta";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static long Reads(CommandCountingLoggerFactory f) => f.Provider.CountContaining("MAX(last_execution_time)");

    private static ServerRuntime MakeServer() => new()
    {
        Config = new MonitoredServer { Name = "t", Host = "h" },
        ConnectionString = "Server=azure;Database=master",
        Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
        StorageName = "h",
        ServerId = LiveServerId,
    };

    private static Task<DateTime?> ResolveAsync(DarlingCollectorRunner runner, ServerRuntime server, string database, CancellationToken token)
    {
        var ct = DateTime.UtcNow;
        return runner.ResolveQueryStoreDatabaseWatermarkAsync(
            server, "query_store_stats", "last_execution_time", "database_name", database, WatermarkPolicy.ReadFloor(ct)!.Value, ct, token);
    }

    private static Task<DateTime?> StoreAsync(DarlingCollectorRunner runner, string database, CancellationToken token) =>
        runner.GetLastCollectedTimeForDatabaseAsync(
            LiveServerId, "query_store_stats", "last_execution_time", "database_name", database, token,
            WatermarkPolicy.ReadFloor(DateTime.UtcNow));

    private sealed class Rig : IAsyncDisposable
    {
        public ScratchPostgres Scratch = null!;
        public NpgsqlDataSource Source = null!;
        public CommandCountingLoggerFactory Logger = new();
        public DarlingCollectorRunner Runner = null!;

        public static async Task<Rig> OpenAsync(CancellationToken ct)
        {
            var rig = new Rig();
            rig.Scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            await using (var migrate = new NpgsqlConnection(rig.Scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, ct);
            }

            rig.Source = new NpgsqlDataSourceBuilder(rig.Scratch.ConnectionString).UseLoggerFactory(rig.Logger).Build();
            rig.Runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator());
            return rig;
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One run of the Azure loop over alpha and zeta. Both return one runtime-stats row stamped five
    /// minutes ago. <paramref name="writeFaultsOn"/> throws from the per-database write hook (after the stage);
    /// <paramref name="readFaultsOn"/> throws from the database's read (before the stage).</summary>
    private static async Task RunAsync(Rig rig, string? writeFaultsOn, string? readFaultsOn, CancellationToken token)
    {
        var stamp = new DateTimeOffset(DateTime.UtcNow.AddMinutes(-5), TimeSpan.Zero);
        object[] Row(long id)
        {
            var row = new object[56];
            Array.Fill(row, DBNull.Value);
            row[0] = id;
            row[1] = id;
            row[2] = "Regular";
            row[3] = stamp;
            row[4] = stamp;
            row[7] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
            row[8] = 1L;
            row[51] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
            row[53] = id;
            return row;
        }

        var provider = new FakeTargetProvider(
            new Dictionary<string, object[][]> { [Alpha] = new[] { Row(1) }, [Zeta] = new[] { Row(2) } }, readFaultsOn);
        rig.Runner.TargetProviderOverrideForTests = _ => provider;
        rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Alpha, Zeta });
        rig.Runner.PerDatabaseWriteFaultForTests = db =>
        {
            if (db == writeFaultsOn)
            {
                throw new InvalidOperationException("injected write failure for " + db);
            }
        };

        /* Not every database fails, so the run itself must not throw. */
        await rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(), token);
    }

    [Fact]
    public async Task AWriteFaultAfterTheStage_DropsThatDatabasesKey_AndTheNextResolveReadsTheStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5514 Azure loop pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        await RunAsync(rig, writeFaultsOn: Zeta, readFaultsOn: null, token);

        /* The sibling that succeeded: its key advanced from its own batch, so the next resolve is a cache hit that
           equals the bounded store read. */
        rig.Logger.Provider.Reset();
        var alpha = await ResolveAsync(rig.Runner, server, Alpha, token);
        Assert.Equal(0, Reads(rig.Logger));
        Assert.NotNull(alpha);
        Assert.Equal(await StoreAsync(rig.Runner, Alpha, token), alpha);

        /* The database whose write threw: its staged batch is not in the store, and the key was dropped, so this
           resolve is a store read (one) and equals the store (no row). */
        rig.Logger.Provider.Reset();
        var zeta = await ResolveAsync(rig.Runner, server, Zeta, token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.Null(zeta);
        Assert.Equal(await StoreAsync(rig.Runner, Zeta, token), zeta);
    }

    [Fact]
    public async Task AReadFaultBeforeTheStage_KeepsThatDatabasesKey_SoTheNextResolveIsACacheHit()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5514 Azure loop pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        await RunAsync(rig, writeFaultsOn: null, readFaultsOn: Zeta, token);

        /* Nothing was staged or written for zeta; the key the loop seeded before the read stays, and it equals the
           store (no row), so this resolve is a cache hit. */
        rig.Logger.Provider.Reset();
        var zeta = await ResolveAsync(rig.Runner, server, Zeta, token);
        Assert.Equal(0, Reads(rig.Logger));
        Assert.Null(zeta);
        Assert.Equal(await StoreAsync(rig.Runner, Zeta, token), zeta);

        var alpha = await ResolveAsync(rig.Runner, server, Alpha, token);
        Assert.NotNull(alpha);
        Assert.Equal(await StoreAsync(rig.Runner, Alpha, token), alpha);
    }

    private sealed class FakeTargetProvider(Dictionary<string, object[][]> rowsByDatabase, string? readFaultOn) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString, rowsByDatabase);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand((FakeConnection)connection, readFaultOn);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString, Dictionary<string, object[][]> rowsByDatabase) : DbConnection
    {
        private string _cs = connectionString;
        public string DatabaseName => _cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "master";
        public object[][] Rows => rowsByDatabase.TryGetValue(DatabaseName, out var rows) ? rows : Array.Empty<object[]>();

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

    private sealed class FakeCommand(FakeConnection owner, string? readFaultOn) : DbCommand
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

        private DbDataReader Open()
        {
            if (readFaultOn is not null && owner.DatabaseName == readFaultOn)
            {
                throw new InvalidOperationException("injected read failure for " + readFaultOn);
            }

            return new Reader(owner.Rows);
        }
    }

    private sealed class Reader(object[][] rows) : DbDataReader
    {
        private int _row = -1;
        private object[] Cur => rows[_row];

        public override bool Read() => ++_row < rows.Length;
        public override bool NextResult() => false;
        public override string GetString(int o) => (string)Cur[o];
        public override long GetInt64(int o) => (long)Cur[o];
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
