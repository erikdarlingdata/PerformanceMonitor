using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: scratch timing, removed before the PR is ready. */
public sealed class StoreTableActivityTimingScratch
{
    [Fact]
    public async Task Timing()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "no pg");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "no timescale");
        var ok = false;
        try
        {
            await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
            async Task Exec(string sql) { await using var c = store.CreateCommand(sql + ";SELECT pg_stat_force_next_flush();"); c.CommandTimeout = 600; await c.ExecuteNonQueryAsync(ct); }
            await Exec("SELECT _timescaledb_functions.stop_background_workers(); CREATE SCHEMA zz9413t; CREATE TABLE zz9413t.ht (ts timestamptz NOT NULL, dev integer NOT NULL, v double precision); SELECT create_hypertable('zz9413t.ht','ts', chunk_time_interval => interval '1 day'); ALTER TABLE zz9413t.ht SET (timescaledb.compress, timescaledb.compress_segmentby='dev'); INSERT INTO zz9413t.ht SELECT now() - (d || ' days')::interval, d, 1 FROM generate_series(0, 299) AS d; SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('zz9413t.ht', older_than => now() - interval '1 day') AS c; ANALYZE");
            await using var conn = new NpgsqlConnection(scratch.ConnectionString);
            await conn.OpenAsync(ct);
            var sw = Stopwatch.StartNew();
            await StoreTableActivity.CaptureAsync(conn, true, new DateTime(2026, 11, 20, 10, 0, 0, DateTimeKind.Utc), null, 300, ct);
            var first = sw.ElapsedMilliseconds;
            await Exec("SELECT count(*) FROM zz9413t.ht");
            sw.Restart();
            var rows = await StoreTableActivity.CaptureAsync(conn, true, new DateTime(2026, 11, 20, 11, 0, 0, DateTimeKind.Utc), null, 300, ct);
            var second = sw.ElapsedMilliseconds;
            await using var cnt = store.CreateCommand("SELECT (SELECT count(*) FROM pg_stat_user_tables), (SELECT count(*) FROM collect.store_table_activity_baseline), (SELECT max(relations_scanned) FROM collect.store_table_activity WHERE table_name='ht')");
            await using var rd = await cnt.ExecuteReaderAsync(ct);
            await rd.ReadAsync(ct);
            var msg = $"TIMING first={first}ms second={second}ms rows={rows} userTables={rd.GetValue(0)} baseline={rd.GetValue(1)} htScanned={rd.GetValue(2)}";
            ok = true;
            Assert.Fail(msg);
        }
        finally { await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask); }
    }
}
