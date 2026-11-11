/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #8570: <c>get_store_metrics</c> reads the newest exactly-sized sweep (<c>StoreMetricsExactSweepSql</c>). The statement
/// walked every retained sweep, up to 400 days of hourly ones, probing each for an estimate row, to find the newest sweep with none.
/// The service sizes the store exactly once a day, so the answer is nearly always within a day or two of the newest sweep. This test
/// seeds 100 days of hourly sweeps in which hour 00:00 is the only exact one of each day, and checks two things: the rows the read
/// returns are those of the newest exact sweep (also when the last three days hold none, which sends the read to the whole history),
/// and the statement touches a small fraction of the buffers the whole-history walk needs.
/// </summary>
public sealed class StoreMetricsExactSweepBuffersLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live exact-sweep buffer test (it mints its own scratch database).";
    private const int Days = 100;
    private const int Objects = 60;

    /* The newest sweep: 17:00 on 9 November. The exact sweep of that day ran at 00:00, 17 hours before it. */
    private static readonly DateTime Newest = new(2026, 11, 9, 17, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task ExactSweepRead_ReturnsTheNewestExactSweep_AndDoesNotWalkTheRetainedHistory()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);
        if (scratch.TimescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        await PgTableTuning.ApplyAsync(connection, null, ct);

        /* Hourly sweeps for 100 days. One store row per sweep, and Objects table rows; every fifth table is a hypertable, which an hourly
           sweep sizes from the catalog (size_estimated) and the 00:00 sweep sizes exactly (NULL). */
        await using (var seed = new NpgsqlCommand(
            @"INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes, row_count, chunk_count, size_estimated)
              SELECT t.at, CASE WHEN o = 0 THEN 'store' ELSE 'obj_' || o END,
                     CASE WHEN o = 0 THEN 'store' WHEN o % 5 = 0 THEN 'hypertable' ELSE 'table' END,
                     1000000 + o * 1000 + t.h, 1000 + t.h, 5,
                     CASE WHEN o % 5 = 0 AND extract(hour FROM t.at) <> 0 THEN TRUE END
              FROM (SELECT $1::timestamp - make_interval(hours => h) AS at, h FROM generate_series(0, $2 * 24 - 1) h) AS t
              CROSS JOIN generate_series(0, $3) AS o", connection))
        {
            seed.Parameters.AddWithValue(Newest);
            seed.Parameters.AddWithValue(Days);
            seed.Parameters.AddWithValue(Objects);
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using (var analyze = new NpgsqlCommand("VACUUM (ANALYZE) collect.store_metrics", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString);

        /* The newest exact sweep is 00:00 on 9 November. */
        var exactTime = new DateTime(2026, 11, 9, 0, 0, 0, DateTimeKind.Unspecified);
        var rows = await DarlingStoreMetricsReader.GetExactSweepAsync(postgres, ct);
        Assert.Equal(Objects + 1, rows.Count);
        Assert.All(rows, r => Assert.Equal(exactTime, r.MetricTime));
        Assert.Contains(rows, r => r.ObjectKind == "store" && r.ObjectName == "store");

        /* Whole statement, as the tool runs it. The walk of every sweep touched 10,000 buffers or more on this seed. */
        var buffers = await StatementBuffersAsync(connection, ct);
        Assert.True(buffers < 3000, $"the exact-sweep statement touched {buffers} buffers");

        /* The last three days hold no exact sweep (every hypertable row is an estimate from 7 November on): the newest one is
           00:00 on 6 November, found by the whole-history read the recent window falls through to. */
        await using (var estimate = new NpgsqlCommand(
            "UPDATE collect.store_metrics SET size_estimated = TRUE WHERE object_kind = 'hypertable' AND metric_time >= $1", connection))
        {
            estimate.Parameters.AddWithValue(new DateTime(2026, 11, 6, 1, 0, 0, DateTimeKind.Unspecified));
            await estimate.ExecuteNonQueryAsync(ct);
        }

        var older = await DarlingStoreMetricsReader.GetExactSweepAsync(postgres, ct);
        Assert.Equal(Objects + 1, older.Count);
        Assert.All(older, r => Assert.Equal(new DateTime(2026, 11, 6, 0, 0, 0, DateTimeKind.Unspecified), r.MetricTime));
    }

    private static async Task<long> StatementBuffersAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingStoreMetricsReader.StoreMetricsExactSweepSql, connection);
        using var doc = JsonDocument.Parse(Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);
        var plan = doc.RootElement[0].GetProperty("Plan");
        return plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();
    }
}
