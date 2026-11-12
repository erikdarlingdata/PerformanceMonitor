/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7419: query_stats' read advances every candidate's delta baseline, so a run that ends after the read and before the rows
/// are stored must put them back, or the next run's delta loses the interval for every statement. Runs the real
/// <see cref="RemoteCollectorService.RunCollectorDefinitionAsync"/> on its server-wide path through
/// <c>ServerReaderOverrideForTests</c>, against a real DuckDB file, and reads the baseline back from the real calculator.
/// </summary>
public abstract class QueryStatsBaselineRollbackRunnerBase : IDisposable
{
    private const string Key = "0x03AB:0:-1:0x06AB";
    private static readonly DateTime T0 = DateTime.UtcNow.AddMinutes(-5);

    protected readonly SharedDuckDbFixture _fixture;
    private readonly string _configDir;
    protected readonly ServerConnection _server;
    private readonly int _serverId;
    protected readonly RemoteCollectorService _collector;

    protected QueryStatsBaselineRollbackRunnerBase(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-qsrollback-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        var serverManager = new ServerManager(_configDir);
        _server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "QsRollbackSrv", DisplayName = "QsRollbackSrv", IsEnabled = true };
        serverManager.AddServer(_server);
        _serverId = RemoteCollectorService.GetServerId(_server);
        _collector = new RemoteCollectorService(fixture.DuckDb, serverManager, new ScheduleManager(_configDir));

        /* The baselines the last stored run left: 100 executions at T0. */
        foreach (var group in new[]
        {
            "query_stats_exec", "query_stats_worker", "query_stats_elapsed", "query_stats_reads",
            "query_stats_writes", "query_stats_phys_reads", "query_stats_rows", "query_stats_spills",
        })
        {
            _collector.DeltaCalculator.CalculateDeltaWithSeriesAge(_serverId, group, Key, 100, null, out _, collectionTime: T0, maxGapSeconds: 3600);
        }
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    protected long ExecBaseline() => _collector.DeltaCalculator.PeekBaselines(_serverId, "query_stats_exec")[Key];

    protected void Exec(string sql)
    {
        using var read = _fixture.DuckDb.AcquireReadLock();
        using var connection = _fixture.DuckDb.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    /// <summary>One statement at 160 executions, in the 44-column shape of the candidate query.</summary>
    protected static DataTableReader MainQuery()
    {
        var table = new DataTable();
        var stringColumns = new[] { 0, 1, 2, 36, 37, 38, 42 };
        for (var i = 0; i < 44; i++)
        {
            var type = Array.IndexOf(stringColumns, i) >= 0 ? typeof(string)
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
        values[3] = DateTime.UtcNow.AddHours(-1);
        values[4] = DateTime.UtcNow;
        values[5] = 160L;
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

    /// <summary>Forwards to a <see cref="DataTableReader"/> and runs a hook when the runner disposes it.</summary>
    protected sealed class HookedReader(DataTableReader inner, Action? onDispose) : DbDataReader
    {
        public override int Depth => inner.Depth;
        public override int FieldCount => inner.FieldCount;
        public override bool HasRows => inner.HasRows;
        public override bool IsClosed => inner.IsClosed;
        public override int RecordsAffected => inner.RecordsAffected;
        public override object this[int ordinal] => inner[ordinal];
        public override object this[string name] => inner[name];
        public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => inner.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) =>
            inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => inner.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) =>
            inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
        public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
        public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
        public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
        public override IEnumerator GetEnumerator() => ((IEnumerable)inner).GetEnumerator();
        public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
        public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
        public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
        public override string GetName(int ordinal) => inner.GetName(ordinal);
        public override int GetOrdinal(string name) => inner.GetOrdinal(name);
        public override string GetString(int ordinal) => inner.GetString(ordinal);
        public override object GetValue(int ordinal) => inner.GetValue(ordinal);
        public override int GetValues(object[] values) => inner.GetValues(values);
        public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);
        public override bool NextResult() => inner.NextResult();
        public override bool Read() => inner.Read();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
                onDispose?.Invoke();
            }

            base.Dispose(disposing);
        }
    }
}

/// <summary>The stop, fault and control cases, on a database of their own.</summary>
public sealed class QueryStatsBaselineRollbackRunnerTests(SharedDuckDbFixture fixture)
    : QueryStatsBaselineRollbackRunnerBase(fixture), IClassFixture<SharedDuckDbFixture>
{
    [Fact]
    public async Task ARunThatStores_AdvancesTheBaseline()
    {
        _collector.ServerReaderOverrideForTests = _ => new HookedReader(MainQuery(), onDispose: null);

        await _collector.RunCollectorDefinitionAsync(QueryStatsCollector.Instance, _server, CancellationToken.None);

        Assert.Equal(160, ExecBaseline());
    }

    [Fact]
    public async Task AStopAfterTheRead_BeforeTheStoreOpens_LeavesTheBaselineWhereTheLastStoredRunPutIt()
    {
        using var stop = new CancellationTokenSource();

        /* The reader is disposed after the read returns and before the DuckDB connection is created and opened, so the stop
           lands exactly in the gap between the read and the write. */
        _collector.ServerReaderOverrideForTests = _ => new HookedReader(MainQuery(), onDispose: stop.Cancel);

        var fault = await Record.ExceptionAsync(() =>
            _collector.RunCollectorDefinitionAsync(QueryStatsCollector.Instance, _server, stop.Token));

        Assert.NotNull(fault);
        Assert.Equal(100, ExecBaseline());
    }

    /// <summary>
    /// The stop arm's own rollback. The stop lands as the provider's exception (SqlClient reports a cancelled command as a
    /// SqlException) at the end of the read, after the rows exist and before the store: no delta collector has a provider call
    /// between its read and the store but the reader's disposal. The run surfaces a cancellation carrying that exception, and
    /// the baseline the stopped run advanced to 160 is put back to the 100 of the last stored run.
    /// </summary>
    [Fact]
    public async Task AStopThatArrivesAsTheProvidersException_RollsTheBaselineBack_AndSurfacesACancellation()
    {
        using var stop = new CancellationTokenSource();
        _collector.ServerReaderOverrideForTests = _ => new HookedReader(MainQuery(), onDispose: () =>
        {
            stop.Cancel();
            throw SqlExceptionFactory.Create(0, errorClass: 11, message: "Operation cancelled by user.");
        });

        var thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            _collector.RunCollectorDefinitionAsync(QueryStatsCollector.Instance, _server, stop.Token));

        Assert.IsType<Microsoft.Data.SqlClient.SqlException>(thrown.InnerException);
        Assert.Equal(100, ExecBaseline());
    }

    [Fact]
    public async Task AFaultAfterTheRead_BeforeTheStore_LeavesTheBaselineWhereTheLastStoredRunPutIt()
    {
        _collector.ServerReaderOverrideForTests = _ =>
            new HookedReader(MainQuery(), onDispose: () => throw new InvalidOperationException("the end of the read failed"));

        var fault = await Record.ExceptionAsync(() =>
            _collector.RunCollectorDefinitionAsync(QueryStatsCollector.Instance, _server, CancellationToken.None));

        Assert.NotNull(fault);
        Assert.Equal(100, ExecBaseline());
    }
}

/// <summary>
/// The write failure case, on a database of its own because the test adds a column to query_stats and the views over it
/// keep it from being dropped again.
/// </summary>
public sealed class QueryStatsBaselineRollbackWriteFailureTests(SharedDuckDbFixture fixture)
    : QueryStatsBaselineRollbackRunnerBase(fixture), IClassFixture<SharedDuckDbFixture>
{
    [Fact]
    public async Task AWriteThatFailsOnTheFirstRow_LeavesTheBaselineWhereTheLastStoredRunPutIt()
    {
        _collector.ServerReaderOverrideForTests = _ => new HookedReader(MainQuery(), onDispose: null);

        /* A column the writer does not fill: the first row cannot be completed, so nothing is stored and nothing was flushed. */
        Exec("ALTER TABLE query_stats ADD COLUMN rollback_probe INTEGER");
        try
        {
            var fault = await Record.ExceptionAsync(() =>
                _collector.RunCollectorDefinitionAsync(QueryStatsCollector.Instance, _server, CancellationToken.None));

            Assert.NotNull(fault);
        }
        finally
        {
            /* Left in place: this class owns its database, and no other test shares it. */
        }

        Assert.Equal(100, ExecBaseline());
    }
}
