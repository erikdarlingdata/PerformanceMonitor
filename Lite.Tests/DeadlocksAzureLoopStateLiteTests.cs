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
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Drives the real Azure per-database loop of Lite's definition runner through a failed store write. A database's
/// deadlock cursor must be saved only after THAT database's rows are written: a failed write keeps its cursor
/// where it was while a sibling database's success advances its own. The reads come from fake readers and the
/// store is a real DuckDB file; the only test hooks are the database list, the per-database reader and a fault
/// thrown directly before the write.
///
/// <para>The second pin composes the restart path: the startup cleanup, the persisted ring cursor and the
/// exact-duplicate drop together store nothing twice, on every start.</para>
/// </summary>
public class DeadlocksAzureLoopStateLiteTests : IDisposable
{
    private const string TelemetryKey = "dl_telemetry_cursor";
    private const string AlphaKey = "dl_ring_cursor:alpha";
    private const string ZetaKey = "dl_ring_cursor:zeta";

    private static readonly DateTime T0 = new(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T1 = T0.AddMinutes(5);

    private static string Iso(DateTime t) => t.ToString("o", CultureInfo.InvariantCulture);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _configDir;

    public DeadlocksAzureLoopStateLiteTests()
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

    private sealed record Rig(DuckDbInitializer DuckDb, RemoteCollectorService Service, ServerConnection Server, int ServerId);

    private async Task<Rig> BuildRigAsync()
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { ServerName = "azure-test", DisplayName = "azure-test" };
        serverManager.AddServer(server);

        /* Azure SQL Database engine edition, so the definition takes the per-database loop. */
        serverManager.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var service = new RemoteCollectorService(duckDb, serverManager, new ScheduleManager(_configDir));
        return new Rig(duckDb, service, server, RemoteCollectorService.GetServerId(server));
    }

    private static object[] Row(DateTime t, object source, string graph) => new object[] { t, "process1", graph, source };

    private static DbDataReader FakeReader(params object[][] rows) =>
        new Reader(rows, new[] { new object[] { 100L, false } });

    private static async Task SeedStateAsync(Rig rig, params (string Key, DateTime Time)[] entries)
    {
        using var conn = rig.DuckDb.CreateConnection();
        await conn.OpenAsync();
        foreach (var (key, time) in entries)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES ($1,$2,$3,$4,$5)";
            cmd.Parameters.Add(new DuckDBParameter { Value = rig.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "deadlocks" });
            cmd.Parameters.Add(new DuckDBParameter { Value = key });
            cmd.Parameters.Add(new DuckDBParameter { Value = Iso(time) });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task<Dictionary<string, string>> ReadStateAsync(Rig rig)
    {
        using var conn = rig.DuckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT state_key, state_value FROM collector_state WHERE server_id = " + rig.ServerId + " AND collector_name = 'deadlocks'";
        using var reader = await cmd.ExecuteReaderAsync();
        var state = new Dictionary<string, string>(StringComparer.Ordinal);
        while (await reader.ReadAsync())
        {
            state[reader.GetString(0)] = reader.GetString(1);
        }

        return state;
    }

    private static async Task<List<string>> ReadStoredGraphsAsync(Rig rig)
    {
        using var conn = rig.DuckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT deadlock_graph_xml FROM deadlocks WHERE server_id = " + rig.ServerId + " ORDER BY deadlock_graph_xml";
        using var reader = await cmd.ExecuteReaderAsync();
        var graphs = new List<string>();
        while (await reader.ReadAsync())
        {
            graphs.Add(reader.GetString(0));
        }

        return graphs;
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task AFailedDatabaseWrite_KeepsThatDatabasesCursors_WhileASiblingsSuccessAdvancesItsOwn(bool masterFaults, bool telemetryAdvances)
    {
        var rig = await BuildRigAsync();
        await SeedStateAsync(rig, (TelemetryKey, T0), (AlphaKey, T0), (ZetaKey, T0));

        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", "alpha", "zeta" });
        rig.Service.AzureDatabaseReaderOverrideForTests = (database, _) => database switch
        {
            "master" => FakeReader(Row(T1, "alpha", "<deadlock id=\"master\"/>")),
            "alpha" => FakeReader(Row(T1, DBNull.Value, "<deadlock id=\"alpha\"/>")),
            _ => FakeReader(Row(T1, DBNull.Value, "<deadlock id=\"zeta\"/>")),
        };
        rig.Service.PerDatabaseWriteFaultForTests = database =>
        {
            if (database == "zeta" || (masterFaults && database == "master"))
            {
                throw new InvalidOperationException("injected write failure for " + database);
            }
        };

        /* Not every database failed, so the run completes. */
        await rig.Service.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, rig.Server, CancellationToken.None);

        var state = await ReadStateAsync(rig);
        Assert.Equal(Iso(T1), state[AlphaKey]);
        Assert.Equal(Iso(T0), state[ZetaKey]);
        Assert.Equal(Iso(telemetryAdvances ? T1 : T0), state[TelemetryKey]);

        var graphs = await ReadStoredGraphsAsync(rig);
        var expected = new List<string> { "<deadlock id=\"alpha\"/>" };
        if (!masterFaults)
        {
            expected.Insert(0, "<deadlock id=\"master\"/>");
        }

        Assert.Equal(expected.OrderBy(g => g, StringComparer.Ordinal).ToList(), graphs.OrderBy(g => g, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task EachStart_ReReadsBehindThePersistedCursor_AndStoresNothingTwice()
    {
        var e1 = T0.AddMinutes(10);
        var e2 = T0.AddMinutes(15);
        const string g1 = "<deadlock id=\"g1\"/>";
        const string g2 = "<deadlock id=\"g2\"/>";

        /* Start 0 only builds the file; the rows are seeded afterwards because the cleanup runs on every start. */
        var first = await BuildRigAsync();
        using (var seed = first.DuckDb.CreateConnection())
        {
            await seed.OpenAsync();
            /* Two pre-upgrade copies of E1 (101, 102) and one E2. */
            await InsertAsync(seed, 101, first.ServerId, e1, g1);
            await InsertAsync(seed, 102, first.ServerId, e1, g1);
            await InsertAsync(seed, 201, first.ServerId, e2, g2);
        }

        await SeedStateAsync(first, (ZetaKey, e2));

        for (var start = 1; start <= 2; start++)
        {
            /* A fresh initializer and service each time: no in-memory state, only what was persisted. */
            var rig = await BuildRigAsync();

            using (var verify = rig.DuckDb.CreateConnection())
            {
                await verify.OpenAsync();
                using var cmd = verify.CreateCommand();
                cmd.CommandText = "SELECT deadlock_id FROM deadlocks ORDER BY deadlock_id";
                using var ids = await cmd.ExecuteReaderAsync();
                var kept = new List<long>();
                while (await ids.ReadAsync())
                {
                    kept.Add(ids.GetInt64(0));
                }

                /* The cleanup removed the later copy of E1 on the first start and has nothing left on the second. */
                Assert.Equal(new List<long> { 101, 201 }, kept);
            }

            var state = await ReadStateAsync(rig);
            var query = DeadlocksCollector.Instance.BuildQuery(new CollectorContext
            {
                ServerId = rig.ServerId,
                ServerName = "azure-test",
                CollectionTime = e2.AddMinutes(1),
                Deltas = null!,
                Target = new CollectorTargetInfo { IsAzureSqlDb = true },
                CurrentDatabaseName = "zeta",
                State = state,
            });
            var cutoff = (DateTime)query.Parameters.First(p => p.Name == "@cutoff_time").Value!;
            Assert.Equal(e2.AddMinutes(-10), cutoff);
            Assert.True(cutoff <= e1);

            rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "zeta" });
            rig.Service.AzureDatabaseReaderOverrideForTests = (_, _) => FakeReader(
                Row(e1, DBNull.Value, g1), Row(e2, DBNull.Value, g2));

            var written = await rig.Service.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, rig.Server, CancellationToken.None);

            Assert.Equal(0, written);
            Assert.Equal(new List<string> { g1, g2 }, await ReadStoredGraphsAsync(rig));
        }
    }

    [Fact]
    public void WithNoRingCursor_TheCutoffFallsBackToTheWatermarkMinusTenMinutes()
    {
        var watermark = T0.AddMinutes(30);
        var query = DeadlocksCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 1,
            ServerName = "azure-test",
            CollectionTime = watermark.AddMinutes(5),
            Deltas = null!,
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
            CurrentDatabaseName = "zeta",
            Watermark = watermark,
            State = CollectorContext.NoState,
        });

        Assert.Equal(watermark.AddMinutes(-10), query.Parameters.First(p => p.Name == "@cutoff_time").Value);
    }

    private static async Task InsertAsync(DuckDBConnection connection, long id, int serverId, DateTime deadlockTime, string graph)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml) VALUES ($1,$2,$3,$4,$5,$6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = deadlockTime.AddMinutes(1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "azure-test" });
        cmd.Parameters.Add(new DuckDBParameter { Value = deadlockTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = graph });
        await cmd.ExecuteNonQueryAsync();
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
