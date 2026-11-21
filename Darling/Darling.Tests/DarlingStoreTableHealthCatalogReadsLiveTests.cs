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
        Assert.Equal(listed, DarlingStoreTableHealthReader.RelationsSizedOn(store));
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
}
