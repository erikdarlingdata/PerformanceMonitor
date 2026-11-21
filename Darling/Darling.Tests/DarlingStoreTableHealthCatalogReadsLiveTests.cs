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

        await ExecAsync(connection, @"
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
ANALYZE;", ct);

        var indexes = await PlannedAndActualRowsAsync(connection, DarlingStoreTableHealthReader.IndexRelationsSql(timescale: true), ct);

        // The seeded store has well over a hundred indexes, so the list is not trivial.
        Assert.True(indexes.Actual >= 300, $"indexes listed {indexes.Actual}");

        // One set-returning function per partitioned root is estimated at 1000 rows, so the bound is generous. The old plan
        // was a hundred times the real count and more.
        Assert.True(indexes.Planned <= indexes.Actual * 20 + 2000, $"indexes: planned {indexes.Planned} rows, listed {indexes.Actual}");
    }
}
