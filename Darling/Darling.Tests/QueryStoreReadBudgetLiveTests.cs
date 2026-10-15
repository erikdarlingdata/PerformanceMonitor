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
/// #5813: drives the host's REAL per-database Query Store loop (the Azure SQL Database arm, the one a fake target can
/// reach end to end) with one database whose runtime-stats read never answers, the stand-in for a WAITFOR. The pass is
/// cut at the read budget, that database's watermark does not move, the sibling database is written in the same pass,
/// the timed-out database is the run's slowest item under its marker, and the next pass that can read it resumes from
/// the same watermark with every interval stored exactly once. The timed-out warning is logged once per database per
/// UTC day. The enumerated arm wraps the same statement the same way; its wiring is pinned by
/// <see cref="QueryStoreReadBudgetWiringPinTests"/>.
///
/// <para><b>#1776 own-store</b> - every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so none is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreReadBudgetLiveTests
{
    private const int LiveServerId = -581300;
    private const string Alpha = "alpha";
    private const string Zeta = "zeta";
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(1);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static ServerRuntime MakeServer() => new()
    {
        Config = new MonitoredServer { Name = "t", Host = "h" },
        ConnectionString = "Server=azure;Database=master",
        Target = new CollectorTargetInfo { IsAzureSqlDb = true, Engine = CollectorTargetEngine.SqlServer },
        StorageName = "h",
        ServerId = LiveServerId,
    };

    private static Task<DateTime?> StoreAsync(DarlingCollectorRunner runner, string database, CancellationToken token) =>
        runner.GetLastCollectedTimeForDatabaseAsync(
            LiveServerId, "query_store_stats", "last_execution_time", "database_name", database, token,
            WatermarkPolicy.ReadFloor(DateTime.UtcNow));

    /// <summary>The target the fakes read: rows per database, and which database's read never answers.</summary>
    private sealed class Target
    {
        public Dictionary<string, List<object[]>> Rows { get; } = new(StringComparer.Ordinal);
        public string? SlowOn { get; set; }
        public int SlowReads;
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<(LogLevel Level, string Text)> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add((logLevel, formatter(state, exception)));
            }
        }

        public int TimedOutWarnings(string database)
        {
            lock (Lines)
            {
                return Lines.Count(l => l.Level == LogLevel.Warning && l.Text.Contains("timed out:", StringComparison.Ordinal)
                    && l.Text.Contains("[" + database + "]", StringComparison.Ordinal));
            }
        }
    }

    private sealed class Rig : IAsyncDisposable
    {
        public ScratchPostgres Scratch = null!;
        public NpgsqlDataSource Source = null!;
        public CapturingLogger Log = new();
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

            rig.Source = new NpgsqlDataSourceBuilder(rig.Scratch.ConnectionString).Build();
            rig.Runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator(), rig.Log)
            {
                QueryStoreReadBudgetSpan = Budget,
            };
            return rig;
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One runtime-stats row for interval <paramref name="id"/>, last executed at <paramref name="lastUtc"/>.</summary>
    private static object[] Row(long id, DateTime lastUtc)
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
        row[8] = 1L;
        row[51] = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);
        row[53] = id;
        return row;
    }

    private static Task<CollectorRunResult> PassAsync(Rig rig, Target target, CancellationToken token)
    {
        var provider = new FakeTargetProvider(target);
        rig.Runner.TargetProviderOverrideForTests = _ => provider;
        rig.Runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { Alpha, Zeta });
        return rig.Runner.RunAsync(QueryStoreCollector.Instance, MakeServer(), token);
    }

    /// <summary>Each stored interval of <paramref name="database"/> and how many raw rows hold it.</summary>
    private static async Task<Dictionary<long, long>> StoredAsync(Rig rig, string database, CancellationToken token)
    {
        await using var connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
        await connection.OpenAsync(token);
        await using var command = new NpgsqlCommand(
            "SELECT runtime_stats_interval_id, count(*) FROM collect.query_store_stats WHERE server_id = @s AND database_name = @d GROUP BY 1",
            connection);
        command.Parameters.AddWithValue("s", LiveServerId);
        command.Parameters.AddWithValue("d", database);
        var result = new Dictionary<long, long>();
        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            result[reader.GetInt64(0)] = reader.GetInt64(1);
        }

        return result;
    }

    [Fact]
    public async Task ASlowRead_IsCutAtTheBudget_KeepsItsWatermark_AndTheNextPassStoresEveryIntervalOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #5813 read-budget pin.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);

        /* Whole seconds, so the store's microseconds compare exactly. Both inside the 15-minute floor the #2111 shrink
           narrows to after two failures, so the resumed cutoff is the stored watermark itself, not a shrunk floor. */
        var now = DateTime.UtcNow;
        now = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        var first = now.AddMinutes(-8);
        var second = now.AddMinutes(-4);

        var target = new Target();
        target.Rows[Alpha] = [Row(1, first)];
        target.Rows[Zeta] = [Row(11, first)];

        /* Pass 1: both databases answer. */
        await PassAsync(rig, target, token);
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));

        /* A new interval on each database; alpha's read now never answers. */
        target.Rows[Alpha].Add(Row(2, second));
        target.Rows[Zeta].Add(Row(12, second));
        target.SlowOn = Alpha;

        /* Pass 2: the stand-in never answers, so only a budget can end the read. The marker and the budget-sized
           slowest_item_ms below say it was THIS budget, not the 500 s command timeout or the 10-minute item budget
           (either of those would name no timed-out item), so no clock is needed. */
        var cut = await PassAsync(rig, target, token);
        Assert.Equal(1, target.SlowReads);

        /* The sibling is written in the same pass; alpha ships nothing and its watermark stays. */
        Assert.Equal(new Dictionary<long, long> { [11] = 1, [12] = 1 }, await StoredAsync(rig, Zeta, token));
        Assert.Equal(new Dictionary<long, long> { [1] = 1 }, await StoredAsync(rig, Alpha, token));
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));

        /* slowest_item names the timed-out database with its marker, and slowest_item_ms is the budget. */
        Assert.NotNull(cut.Fanout);
        Assert.Equal(QueryStoreReadBudget.SlowestItemName(Alpha), cut.Fanout!.Value.SlowestItem);
        Assert.Equal((int)Budget.TotalMilliseconds, cut.Fanout.Value.SlowestItemMs);
        Assert.Equal(2, cut.Fanout.Value.ItemCount);
        Assert.Contains("timed out", cut.HostNote ?? string.Empty, StringComparison.Ordinal);
        Assert.Equal(1, rig.Log.TimedOutWarnings(Alpha));

        /* Pass 3, the same UTC day: cut again, and not named again. */
        await PassAsync(rig, target, token);
        Assert.Equal(2, target.SlowReads);
        Assert.Equal(1, rig.Log.TimedOutWarnings(Alpha));
        Assert.Equal(first, await StoreAsync(rig.Runner, Alpha, token));

        /* Pass 4: alpha answers, and resumes from the watermark it kept. Every interval is stored exactly once. */
        target.SlowOn = null;
        var resumed = await PassAsync(rig, target, token);
        Assert.Equal(new Dictionary<long, long> { [1] = 1, [2] = 1 }, await StoredAsync(rig, Alpha, token));
        Assert.Equal(new Dictionary<long, long> { [11] = 1, [12] = 1 }, await StoredAsync(rig, Zeta, token));
        Assert.Equal(second, await StoreAsync(rig.Runner, Alpha, token));
        Assert.DoesNotContain(QueryStoreReadBudget.TimedOutMarker, resumed.Fanout?.SlowestItem ?? string.Empty, StringComparison.Ordinal);

        /* Pass 5, the next UTC day: a database that is still timing out is named once more at the day boundary. */
        rig.Runner.QueryStoreReadTimeoutClockForTests = () => DateTime.UtcNow.AddDays(1);
        target.SlowOn = Alpha;
        await PassAsync(rig, target, token);
        Assert.Equal(2, rig.Log.TimedOutWarnings(Alpha));
        Assert.Equal(0, rig.Log.TimedOutWarnings(Zeta));
    }

    private sealed class FakeTargetProvider(Target target) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand((FakeConnection)connection, target, query);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;
        public string DatabaseName => _cs.StartsWith("db=", StringComparison.Ordinal) ? _cs[3..] : "master";

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
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();

        /* The runtime-stats read is the one command that carries @cutoff_time. For the slow database it is the
           stand-in for a WAITFOR: it waits until the token is cancelled, so only the budget can end it. Every read
           ships the database's rows past the cutoff, as the real `> @cutoff_time` predicate does, so a row the store
           already holds is offered again only if the watermark went backwards. */
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            var cutoff = query.Parameters?.FirstOrDefault(static p => p.Name == "@cutoff_time")?.Value as DateTime?;
            if (cutoff is null)
            {
                return new Reader(Array.Empty<object[]>());
            }

            if (owner.DatabaseName == target.SlowOn)
            {
                Interlocked.Increment(ref target.SlowReads);
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            var floor = DateTime.SpecifyKind(cutoff.Value, DateTimeKind.Utc);
            var rows = target.Rows.TryGetValue(owner.DatabaseName, out var all) ? all : new List<object[]>();
            return new Reader(rows.Where(r => ((DateTimeOffset)r[4]).UtcDateTime > floor).ToArray());
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
