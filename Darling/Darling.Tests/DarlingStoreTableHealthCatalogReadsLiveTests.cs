/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The relation list the per-index read sizes from (#9224). On a store with thousands of chunks the planner
/// estimated millions of rows for them, and every join and sort above them was planned for that many rows. The estimate is a
/// count the plan gives, so the test compares it with the rows the statement returns.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class DarlingStoreTableHealthCatalogReadsLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(long Planned, long Actual)> PlannedAndActualRowsAsync(
        NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection) { CommandTimeout = 300 };
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var planned = (long)doc.RootElement[0].GetProperty("Plan").GetProperty("Plan Rows").GetDouble();

        long actual = 0;
        await using var run = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await using var reader = await run.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            actual++;
        }

        return (planned, actual);
    }

    private const string SeedSql = @"
CREATE SCHEMA zz9224;
DO $$
BEGIN
    FOR i IN 1..150 LOOP
        EXECUTE format('CREATE TABLE zz9224.t%s (id integer PRIMARY KEY, v text)', i);
        EXECUTE format('CREATE INDEX t%s_v ON zz9224.t%s (v)', i, i);
    END LOOP;
END $$;
CREATE TABLE zz9224.ht (ts timestamptz NOT NULL, dev integer NOT NULL, v double precision);
SELECT create_hypertable('zz9224.ht', 'ts', chunk_time_interval => interval '1 day');
CREATE INDEX ht_dev ON zz9224.ht (dev, ts DESC);
INSERT INTO zz9224.ht SELECT now() - (d || ' days')::interval, d, 1 FROM generate_series(0, 11) AS d;
CREATE TABLE zz9224.pt (d date NOT NULL, v text) PARTITION BY RANGE (d);
CREATE TABLE zz9224.pt_1 PARTITION OF zz9224.pt FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE zz9224.pt_2 PARTITION OF zz9224.pt FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE INDEX pt_v ON zz9224.pt (v);
ANALYZE;";

    /// <summary>The rows the <c>est</c> CTE (the per-index estimate) produced, found by its subplan name in an EXPLAIN ANALYZE plan.</summary>
    private static long? EstimateRowsProduced(JsonElement node)
    {
        if (node.TryGetProperty("Subplan Name", out var name) && name.GetString() == "CTE est")
        {
            return (long)node.GetProperty("Actual Rows").GetDouble();
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                var found = EstimateRowsProduced(child);
                if (found is not null)
                {
                    return found;
                }
            }
        }

        return null;
    }

    [Fact]
    public async Task TheIndexList_IsNotPlannedForMillionsOfRows_WhenTheStoreHasManyTables()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9224 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await ExecAsync(connection, SeedSql, ct);

        var indexes = await PlannedAndActualRowsAsync(connection, DarlingStoreTableHealthReader.IndexRelationsSql(timescale: true), ct);

        // The seeded store has well over a hundred indexes, so the list is not trivial.
        Assert.True(indexes.Actual >= 300, $"indexes listed {indexes.Actual}");

        // One set-returning function per partitioned root is estimated at 1000 rows, so the bound is generous. The old plan
        // was a hundred times the real count and more.
        Assert.True(indexes.Planned <= indexes.Actual * 20 + 2000, $"indexes: planned {indexes.Planned} rows, listed {indexes.Actual}");
    }

    [Fact]
    public async Task ThePerIndexEstimate_IsKeptToTheIndexesTheReadWasHandedIn()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9224 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, SeedSql, ct);

        // Indexes the read does not list: a temporary table is not a store relation, so the index list leaves it out, but its btree
        // indexes are in the database for the estimate to read.
        await ExecAsync(connection, @"
CREATE TEMPORARY TABLE zz9224_tmp (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int);
DO $$
BEGIN
    FOR i IN 1..600 LOOP
        EXECUTE format('CREATE INDEX zz9224_tmp_%s ON zz9224_tmp (c%s)', i, 1 + i % 10);
    END LOOP;
END $$;
ANALYZE;", ct);

        // What the reader hands the statement: the relations the list names, and the size of each.
        long[] oids;
        long[] bytes;
        long indexesHandedIn;
        await using (var sizes = new NpgsqlCommand(
            "SELECT array_agg(l.oid ORDER BY l.oid), array_agg(pg_catalog.pg_relation_size(l.oid::oid) ORDER BY l.oid), "
            + "count(*) FILTER (WHERE c.relkind IN ('i', 'I')) FROM ("
            + DarlingStoreTableHealthReader.IndexRelationsSql(timescale: true) + ") AS l(oid) JOIN pg_catalog.pg_class AS c ON c.oid = l.oid::oid", connection))
        await using (var sizeReader = await sizes.ExecuteReaderAsync(ct))
        {
            Assert.True(await sizeReader.ReadAsync(ct));
            oids = (long[])sizeReader.GetValue(0);
            bytes = (long[])sizeReader.GetValue(1);
            indexesHandedIn = sizeReader.GetInt64(2);
        }

        await using var explain = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + DarlingStoreTableHealthReader.BuildIndexSql(timescale: true, presized: true), connection) { CommandTimeout = 300 };
        explain.Parameters.Add(new NpgsqlParameter { Value = oids, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
        explain.Parameters.Add(new NpgsqlParameter { Value = bytes, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
        using var doc = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
        var estimated = EstimateRowsProduced(doc.RootElement[0].GetProperty("Plan"))
            ?? throw new InvalidOperationException("The per-index plan has no estimate node.");

        // The per-index estimate used to run over every btree index in the database, the temporary table's and TimescaleDB's own
        // among them, and size each one a second time. Kept to the indexes the read was handed, it produces no more rows than
        // the read has indexes.
        Assert.True(estimated <= indexesHandedIn, $"per-index estimate: {estimated} rows for {indexesHandedIn} indexes handed in");
    }

    [Fact]
    public async Task TheTablesRead_AndTheIndexesRead_StatEachRelationOnce_AndSizeTheSame()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9224 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, SeedSql, ct);

        // The relations the two reads use, counted outside the reader: every member heap and every index on one.
        await using var count = new NpgsqlCommand("SELECT count(*) FROM (" + DarlingStoreTableHealthReader.IndexRelationsSql(timescale: true) + ") AS l(oid)", connection);
        var listed = (long)(await count.ExecuteScalarAsync(ct))!;
        Assert.True(listed >= 300, $"relations listed {listed}");

        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tables = await DarlingStoreTableHealthReader.GetAsync(store, ct);
        var indexes = await DarlingStoreTableHealthReader.GetIndexesAsync(store, ct);

        // The first read sized every relation, and the second took that pass: one stat per relation, not two. Before, the tables
        // read sized the heaps and the indexes' forks and the indexes read sized every heap and index again.
        Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(store));
        Assert.Equal(listed, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(store));
        Assert.NotEmpty(tables);
        Assert.NotEmpty(indexes);

        // The values are the ones the size functions give on their own: a plain table's heap, index and TOAST bytes.
        foreach (var n in new[] { 1, 40, 150 })
        {
            await using var direct = new NpgsqlCommand(
                $"SELECT pg_catalog.pg_relation_size('zz9224.t{n}'), pg_catalog.pg_indexes_size('zz9224.t{n}'), "
                + $"COALESCE(pg_catalog.pg_total_relation_size(NULLIF((SELECT reltoastrelid FROM pg_catalog.pg_class WHERE oid = 'zz9224.t{n}'::regclass), 0)), 0)", connection);
            await using var reader = await direct.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            var row = tables.Single(t => t.SchemaName == "zz9224" && t.TableName == $"t{n}");
            Assert.Equal(reader.GetInt64(0), row.HeapBytes);
            Assert.Equal(reader.GetInt64(1), row.IndexBytes);
            Assert.Equal(reader.GetInt64(2), row.ToastBytes);
        }
    }

    /// <summary>What a store with TOAST values, an unlogged table (an index with an init fork), a vacuumed index (a free space map
    /// fork) and a hypertable with chunks adds to the #9224 seed (#9364).</summary>
    private const string ForksSeedSql = @"
CREATE TABLE zz9224.big (id integer PRIMARY KEY, body text);
INSERT INTO zz9224.big SELECT g, (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 400) AS i) FROM generate_series(1, 200) AS g;
CREATE INDEX big_body_len ON zz9224.big (length(body));
DELETE FROM zz9224.big WHERE id % 2 = 0;
CREATE UNLOGGED TABLE zz9224.ul (id integer PRIMARY KEY, v text);
CREATE INDEX ul_v ON zz9224.ul (v);
INSERT INTO zz9224.ul SELECT g, md5(g::text) FROM generate_series(1, 500) AS g;";

    private static async Task<long> ListedRelationsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var count = new NpgsqlCommand("SELECT count(*) FROM (" + DarlingStoreTableHealthReader.IndexRelationsSql(timescale: true) + ") AS l(oid)", connection);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task TheIndexesRead_RunsTheMainPassOnly_AndTheTablesReadAddsTheExtras()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9364 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, SeedSql, ct);
        var listed = await ListedRelationsAsync(connection, ct);
        Assert.True(listed >= 300, $"relations listed {listed}");

        // The indexes read first: it waits for the main pass only, and nothing stats the other forks or the TOAST tables.
        await using (var indexesFirst = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            Assert.NotEmpty(await DarlingStoreTableHealthReader.GetIndexesAsync(indexesFirst, ct));
            Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(indexesFirst));
            Assert.Equal(0, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(indexesFirst));

            // The tables read right after takes that main pass and runs the extras.
            Assert.NotEmpty(await DarlingStoreTableHealthReader.GetAsync(indexesFirst, ct));
            Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(indexesFirst));
            Assert.Equal(listed, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(indexesFirst));
        }

        // The other order: the tables read runs both, and the indexes read after it runs neither.
        await using (var tablesFirst = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            Assert.NotEmpty(await DarlingStoreTableHealthReader.GetAsync(tablesFirst, ct));
            Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(tablesFirst));
            Assert.Equal(listed, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(tablesFirst));
            Assert.NotEmpty(await DarlingStoreTableHealthReader.GetIndexesAsync(tablesFirst, ct));
            Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(tablesFirst));
            Assert.Equal(listed, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(tablesFirst));
        }
    }

    [Fact]
    public async Task TheRowsOfBothReads_MatchTheUnsizedForm_OnAStoreWithTOASTForksAndAHypertable()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9364 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, SeedSql, ct);
        await ExecAsync(connection, ForksSeedSql, ct);
        foreach (var vacuum in new[] { "VACUUM zz9224.big", "VACUUM zz9224.ul", "VACUUM zz9224.ht", "ANALYZE" })
        {
            await ExecAsync(connection, vacuum, ct);
        }

        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        var tables = await DarlingStoreTableHealthReader.GetAsync(store, ct);
        var indexes = await DarlingStoreTableHealthReader.GetIndexesAsync(store, ct);

        await using var version = new NpgsqlCommand("SELECT current_setting('server_version_num')::int", connection);
        var serverVersion = (int)(await version.ExecuteScalarAsync(ct))!;

        // The unsized form calls the size functions inline: the original statement, and the oracle for the passes.
        var oracleTables = new System.Collections.Generic.Dictionary<(string, string), (long Total, long Heap, long Index, long Toast)>();
        await using (var command = new NpgsqlCommand(DarlingStoreTableHealthReader.BuildSql(timescale: true, serverVersion, presized: false), connection) { CommandTimeout = 300 })
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                oracleTables[(reader.GetString(0), reader.GetString(1))] = (reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetInt64(6));
            }
        }

        var oracleIndexes = new System.Collections.Generic.Dictionary<(string, string, string), (long Bytes, long Heap, long? Reclaimable, long? Basis)>();
        await using (var command = new NpgsqlCommand(DarlingStoreTableHealthReader.BuildIndexSql(timescale: true, presized: false), connection) { CommandTimeout = 300 })
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                oracleIndexes[(reader.GetString(0), reader.GetString(1), reader.GetString(3))] =
                    (reader.GetInt64(7), reader.GetInt64(8), reader.IsDBNull(18) ? null : reader.GetInt64(18), reader.IsDBNull(22) ? null : reader.GetInt64(22));
            }
        }

        Assert.Equal(oracleTables.Count, tables.Count);
        foreach (var t in tables)
        {
            var o = oracleTables[(t.SchemaName, t.TableName)];
            Assert.Equal(o, (t.TotalBytes, t.HeapBytes, t.IndexBytes, t.ToastBytes));
        }

        Assert.Equal(oracleIndexes.Count, indexes.Count);
        foreach (var i in indexes)
        {
            var o = oracleIndexes[(i.SchemaName, i.TableName, i.IndexName)];
            Assert.Equal(o, (i.Bytes, i.HeapBytes, i.EstimatedReclaimableBytes, i.EstimatedBasisBytes));
        }

        // The seed has what the passes treat differently: TOAST on a heap, indexes with forks beyond the main one, a hypertable with chunks.
        Assert.True(tables.Single(t => t.TableName == "big").ToastBytes > 0, "the TOAST table has bytes");
        await using (var forks = new NpgsqlCommand(
            "SELECT pg_catalog.pg_indexes_size('zz9224.ul') - pg_catalog.pg_relation_size('zz9224.ul_v') - pg_catalog.pg_relation_size('zz9224.ul_pkey')", connection))
        {
            Assert.True((long)(await forks.ExecuteScalarAsync(ct))! > 0, "the unlogged table's indexes have forks beyond the main one");
        }

        Assert.Contains(tables, t => t.TableName == "ht" && t.ChunkCount > 0);
    }

    [Fact]
    public async Task AFailedExtrasPass_DoesNotStopTheIndexesRead_AndTheNextTablesReadRunsTheExtrasAgain()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9364 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, SeedSql, ct);
        var listed = await ListedRelationsAsync(connection, ct);

        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);
        DarlingStoreTableHealthReader.FailExtrasPassesOn(store, new InvalidOperationException("injected extras failure"));

        // The tables read needs the extras and fails with them. It does not take the main pass down: the indexes read returns its rows
        // and the main pass is held.
        await Assert.ThrowsAsync<InvalidOperationException>(() => DarlingStoreTableHealthReader.GetAsync(store, ct));
        Assert.NotEmpty(await DarlingStoreTableHealthReader.GetIndexesAsync(store, ct));
        Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(store));
        Assert.Equal(0, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(store));

        // A failed pass is not held: once it can run, the next tables read runs it and does not size the main pass again.
        DarlingStoreTableHealthReader.FailExtrasPassesOn(store, null);
        Assert.NotEmpty(await DarlingStoreTableHealthReader.GetAsync(store, ct));
        Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(store));
        Assert.Equal(listed, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(store));
    }

    /// <summary>Enough relations that each pass slices onto the most connections it can (800 or more), so two passes that did not share
    /// a limit would hold twice as many.</summary>
    private const string ManyRelationsSeedSql = @"
CREATE SCHEMA zz9364;
DO $$
BEGIN
    FOR i IN 1..500 LOOP
        EXECUTE format('CREATE TABLE zz9364.t%s (id integer PRIMARY KEY, v text)', i);
        EXECUTE format('CREATE INDEX t%s_v ON zz9364.t%s (v)', i, i);
    END LOOP;
END $$;
ANALYZE;";

    private static async Task<(ScratchPostgres Scratch, long Listed)> SeedManyRelationsAsync(string baseCs, System.Threading.CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseCs, ct);
        if (!scratch.TimescaleEnabled)
        {
            await scratch.DisposeAsync();
            Assert.Skip("TimescaleDB is not enabled on this cluster.");
        }

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, ManyRelationsSeedSql, ct);
        var listed = await ListedRelationsAsync(connection, ct);
        Assert.True(listed >= 800, $"relations listed {listed}");
        return (scratch, listed);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, System.Threading.CancellationToken ct)
    {
        var until = DateTime.UtcNow.AddSeconds(60);
        while (!condition() && DateTime.UtcNow < until)
        {
            await Task.Delay(50, ct);
        }

        Assert.True(condition(), "the condition was not met within 60 seconds");
    }

    [Fact]
    public async Task ACold_TablesRead_AndIndexesRead_StartedTogether_NeverHoldMoreThanTheSizeReadLimit_AndMainGoesFirst()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9364 live tests.");
        var ct = TestContext.Current.CancellationToken;

        var (scratch, listed) = await SeedManyRelationsAsync(baseCs!, ct);
        await using var scratchOwner = scratch;
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);

        // Every read holds its grant long enough for the others to overlap it, and the grants are recorded in the order they were handed out.
        var granted = new System.Collections.Concurrent.ConcurrentQueue<DarlingStoreTableHealthReader.SizePassPart>();
        DarlingStoreTableHealthReader.OnSizePassGrantOn(store, async part =>
        {
            granted.Enqueue(part);
            await Task.Delay(150, ct);
        });

        var tables = DarlingStoreTableHealthReader.GetAsync(store, ct);
        var indexes = DarlingStoreTableHealthReader.GetIndexesAsync(store, ct);
        Assert.NotEmpty(await tables);
        Assert.NotEmpty(await indexes);

        Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(store));
        Assert.Equal(listed, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(store));
        var peak = DarlingStoreTableHealthReader.PeakSizeConnectionsHeldOn(store);
        Assert.InRange(peak, 1, 4);
        Assert.Equal(0, DarlingStoreTableHealthReader.SizeConnectionsHeldOn(store));

        // The main pass's slices all got the limit before the first slice of the extras pass did.
        var order = granted.ToArray();
        var lastMain = Array.FindLastIndex(order, p => p == DarlingStoreTableHealthReader.SizePassPart.Main);
        var firstExtras = Array.FindIndex(order, p => p == DarlingStoreTableHealthReader.SizePassPart.Extras);
        Assert.True(lastMain >= 0 && firstExtras >= 0, "both passes took grants");
        Assert.True(lastMain < firstExtras, "a main slice was granted after an extras slice: " + string.Join(",", order));
    }

    [Fact]
    public async Task AFailedExtrasSlice_AndAFailedExtrasPass_LeaveTheSizeReadLimitFree()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9364 live tests.");
        var ct = TestContext.Current.CancellationToken;

        var (scratch, listed) = await SeedManyRelationsAsync(baseCs!, ct);
        await using var scratchOwner = scratch;
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);

        // One extras slice fails after it holds the limit while the others hold it too: every grant comes back.
        DarlingStoreTableHealthReader.OnSizePassGrantOn(store, async part =>
        {
            await Task.Delay(50, ct);
            if (part == DarlingStoreTableHealthReader.SizePassPart.Extras)
            {
                throw new InvalidOperationException("injected extras slice failure");
            }
        });
        await Assert.ThrowsAsync<InvalidOperationException>(() => DarlingStoreTableHealthReader.GetAsync(store, ct));
        Assert.Equal(0, DarlingStoreTableHealthReader.SizeConnectionsHeldOn(store));

        // The whole extras pass failing before it reads anything leaves the limit free as well.
        DarlingStoreTableHealthReader.OnSizePassGrantOn(store, null);
        DarlingStoreTableHealthReader.FailExtrasPassesOn(store, new InvalidOperationException("injected extras failure"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => DarlingStoreTableHealthReader.GetAsync(store, ct));
        Assert.Equal(0, DarlingStoreTableHealthReader.SizeConnectionsHeldOn(store));

        // A later read still runs and holds no more than the limit.
        DarlingStoreTableHealthReader.FailExtrasPassesOn(store, null);
        Assert.NotEmpty(await DarlingStoreTableHealthReader.GetAsync(store, ct));
        Assert.Equal(listed, DarlingStoreTableHealthReader.ExtraRelationsSizedOn(store));
        Assert.Equal(0, DarlingStoreTableHealthReader.SizeConnectionsHeldOn(store));
        Assert.InRange(DarlingStoreTableHealthReader.PeakSizeConnectionsHeldOn(store), 1, 4);
    }

    [Fact]
    public async Task ACancelledCaller_LeavesTheSharedPassesRunning_AndTheSizeReadLimitFreeWhenTheyEnd()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9364 live tests.");
        var ct = TestContext.Current.CancellationToken;

        var (scratch, listed) = await SeedManyRelationsAsync(baseCs!, ct);
        await using var scratchOwner = scratch;
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var store = NpgsqlDataSource.Create(scratch.ConnectionString);

        var firstMain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        DarlingStoreTableHealthReader.OnSizePassGrantOn(store, async part =>
        {
            if (part == DarlingStoreTableHealthReader.SizePassPart.Main)
            {
                firstMain.TrySetResult();
            }

            await Task.Delay(150, ct);
        });

        using var cancel = System.Threading.CancellationTokenSource.CreateLinkedTokenSource(ct);
        var read = DarlingStoreTableHealthReader.GetAsync(store, cancel.Token);
        await firstMain.Task.WaitAsync(TimeSpan.FromSeconds(60), ct);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);

        // The passes are shared work: the cancelled caller's wait ends, the passes run on, and when they end every grant is back.
        await WaitUntilAsync(() => DarlingStoreTableHealthReader.ExtraRelationsSizedOn(store) == listed, ct);
        await WaitUntilAsync(() => DarlingStoreTableHealthReader.SizeConnectionsHeldOn(store) == 0, ct);
        Assert.InRange(DarlingStoreTableHealthReader.PeakSizeConnectionsHeldOn(store), 1, 4);

        // A later read takes the held passes.
        Assert.NotEmpty(await DarlingStoreTableHealthReader.GetAsync(store, ct));
        Assert.Equal(listed, DarlingStoreTableHealthReader.MainRelationsSizedOn(store));
        Assert.Equal(0, DarlingStoreTableHealthReader.SizeConnectionsHeldOn(store));
    }
}
