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
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;

using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* #1776 own-store: each test mints a scratch database, because it materializes a continuous aggregate and drops it,
   which no other class sharing the store may see. */
/// <summary>
/// #9147, live: a raw-tier top-procedures window whose whole hours the interval-honest hourly rollup has settled ranks those
/// hours from the rollup and reads raw only for the edges. The oracle is the same reader with the rollup dropped (raw alone):
/// every row, every total and every detail field must match, for each ranking the rollup carries. The seed has rows exactly on
/// both window bounds, a first-collection row (interval 0) the raw read leaves out, a NULL schema and a CPU tie.
/// </summary>
public sealed class TopProceduresHourlyMidLiveTests
{
    private const int ServerId = -914701;
    private const string ServerName = "a9147-top-procedures-mid";
    private const int Top = 8;
    private const int Objects = 60;

    [Fact]
    public Task MidRoute_ReturnsTheRawPage_ForEveryRankingTheRollupCarries() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration, TopRanking.Executions })
            {
                await using (var source = NpgsqlDataSource.Create(scratch.ConnectionString))
                {
                    var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
                    Assert.NotNull(DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, ranking, start, end));
                }
            }

            var mid = new System.Collections.Generic.Dictionary<TopRanking, string>();
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration, TopRanking.Executions })
            {
                mid[ranking] = await ReadAsync(scratch, ranking, start, end, ct);
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalDailyView}");
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}");
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration, TopRanking.Executions })
            {
                var raw = await ReadAsync(scratch, ranking, start, end, ct);
                Assert.Equal(raw, mid[ranking]);
                Assert.NotEqual("[]", raw);
            }
        });

    [Fact]
    public Task MidRoute_ReadsFewerPagesThanTheRawStatement() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
            var plan = DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, end);
            Assert.NotNull(plan);

            var rawSql = TopRankings.Apply(DarlingDataReader.TopProceduresSql, TopRanking.Cpu, hourly: false);
            var midSql = DarlingDataReader.BuildProceduresMidSql(TopRanking.Cpu, plan!);
            var rawPages = ExplainBuffers.DataPages(await ExplainAsync(connection, rawSql, start, end, null, ct));
            var midPages = ExplainBuffers.DataPages(await ExplainAsync(connection, midSql, start, end, plan, ct));
            Assert.True(midPages < rawPages, $"mid route read {midPages} pages, the raw statement {rawPages}");
        });

    [Fact]
    public Task MidRoute_ReadsRaw_WhenAWinnerHoldsTwoObjectTypes() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await AssertMidPlannedButRawPageAsync(scratch, connection, start, end, ct);
        }, SeedShape.TwoObjectTypes);

    [Fact]
    public Task MidRoute_ReadsRaw_WhenTheRollupHasAHoleInTheMid() =>
        WithStoreAsync(async (scratch, connection, start, end, ct) =>
        {
            await AssertMidPlannedButRawPageAsync(scratch, connection, start, end, ct);
        }, SeedShape.MaterializationGap);

    /// <summary>The plan takes the mid, yet the routed page equals the page with the rollup dropped (raw alone). The seed shape is one
    /// where the mid's own answer would differ from raw's, so a read that skipped its fallback fails here.</summary>
    private static async Task AssertMidPlannedButRawPageAsync(ScratchPostgres scratch, NpgsqlConnection connection, DateTime start, DateTime end, CancellationToken ct)
    {
        await using (var source = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(source, ct);
            Assert.NotNull(DarlingDataReader.PlanProceduresHourlyMid(rollups, coverage, TopRanking.Cpu, start, end));
        }

        var routed = await ReadAsync(scratch, TopRanking.Cpu, start, end, ct);

        await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalDailyView}");
        await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}");
        var raw = await ReadAsync(scratch, TopRanking.Cpu, start, end, ct);

        Assert.Equal(raw, routed);
        Assert.NotEqual("[]", raw);
    }

    private static async Task<object?> ExplainAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, DarlingDataReader.HourlyMid? mid, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        if (mid is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidStart });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mid.MidEnd });
        }

        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>The routed read through a fresh data source (coverage is cached per source), as JSON so record fields compare whole.</summary>
    private static async Task<string> ReadAsync(ScratchPostgres scratch, TopRanking ranking, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var result = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(source, ServerId, start, end, Top, databaseName: null, ranking: ranking, cancellationToken: ct);
        Assert.Equal(RetentionTier.Raw, result.Tier);
        return JsonSerializer.Serialize(result.Rows);
    }

    /// <summary>Extra seed on top of the plain store: <see cref="TwoObjectTypes"/> gives one procedure a second object type, and
    /// <see cref="MaterializationGap"/> leaves hours of the window out of the rollup's refresh.</summary>
    private enum SeedShape
    {
        Plain,
        TwoObjectTypes,
        MaterializationGap,
    }

    /// <summary>
    /// Plants <see cref="Objects"/> procedures, one row each every five minutes from two hours before the window to one hour after it,
    /// plus the edge rows, refreshes the interval-honest hourly rollup over all of it, and runs the body. The window ends 20 minutes
    /// past an hour of the previous UTC day, so it is inside raw retention and its last part is an edge.
    /// </summary>
    private static async Task WithStoreAsync(Func<ScratchPostgres, NpgsqlConnection, DateTime, DateTime, CancellationToken, Task> body, SeedShape shape = SeedShape.Plain)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9147 top procedures mid tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #9147 top procedures mid tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Anchored through LiveClock (#5608): a 24 hour window straddles a raw chunk boundary, and with the end placed "two hours
           ago" that boundary moves with the hour the test runs. The end sits 20 minutes past an hour of the previous UTC day, so the
           window is inside raw retention, its last part is an edge, and the layout is the same at any hour. */
        var end = LiveClock.AnchoredEndNotAfterNow().AddMinutes(20);
        var start = end.AddHours(-24);
        var hour = new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(2);

        /* Object n: CPU 1000 + n per row (P058 and P059 tie on CPU, P059 has more executions), elapsed 3000 - n,
           executions 1 + n % 7. P007 has a NULL schema. One extra row each sits exactly on the window's start (left out) and
           end (kept), and P003 gets a first-collection row (interval 0) inside the window that raw leaves out. */
        await DarlingMcpTestData.ExecAsync(connection, ct, $"""
            INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                         object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                         delta_logical_reads, delta_spills, min_worker_time, max_worker_time, sample_interval_seconds)
            SELECT 914700000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', 'db' || (n % 3),
                   CASE WHEN n = 7 THEN NULL ELSE 'dbo' END, 'P' || lpad(n::text, 3, '0'), 'P', 'sh' || n, 'ph' || n,
                   1 + n % 7 + CASE WHEN n = 59 THEN 1 ELSE 0 END, CASE WHEN n = 59 THEN 1058 ELSE 1000 + n END, 3000 - n,
                   10 * n, n % 2, n, 2 * n, 300
            FROM generate_series(0, {Objects - 1}) AS n
            CROSS JOIN generate_series($1::timestamp, $2::timestamp, interval '5 minutes') AS t
            """, start.AddHours(-2).AddMinutes(1), end.AddHours(1));
        await DarlingMcpTestData.ExecAsync(connection, ct, $"""
            INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                         object_type, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            VALUES (914709000001, $1, {ServerId}, '{ServerName}', 'db0', 'dbo', 'P000', 'P', 5, 900000, 900000, 300),
                   (914709000002, $2, {ServerId}, '{ServerName}', 'db0', 'dbo', 'P003', 'P', 5, 700000, 700000, 300),
                   (914709000003, $3, {ServerId}, '{ServerName}', 'db0', 'dbo', 'P003', 'P', 5, 990000, 990000, 0)
            """, start, end, start.AddHours(5).AddMinutes(2));

        if (shape == SeedShape.TwoObjectTypes)
        {
            /* P010 also exists as a function ('FN') with the same load, so the rollup, which has no object_type, ranks the two together
               (about 582,000 CPU, a winner) while raw ranks each type alone (about 290,000 each, below the eighth place). */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                             object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                             delta_logical_reads, delta_spills, min_worker_time, max_worker_time, sample_interval_seconds)
                SELECT 914710000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', 'db' || (n % 3), 'dbo', 'P' || lpad(n::text, 3, '0'),
                       'FN', 'shf' || n, 'phf' || n, 1 + n % 7, 1000 + n, 3000 - n, 10 * n, n % 2, n, 2 * n, 300
                FROM generate_series(10, 10) AS n
                CROSS JOIN generate_series($1::timestamp, $2::timestamp, interval '5 minutes') AS t
                """, start.AddHours(-2).AddMinutes(1), end.AddHours(1));
        }

        if (shape == SeedShape.MaterializationGap)
        {
            /* The load is uniform across the window, so a gap alone would not move the ranking. PGAP does all its work inside the
               skipped hours (about 3.6 million CPU, the top of the raw page), so a mid that ignored the hole would not rank it. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"""
                INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
                                             object_type, sql_handle, plan_handle, delta_execution_count, delta_worker_time, delta_elapsed_time,
                                             sample_interval_seconds)
                SELECT 914720000000 + row_number() OVER (), t, {ServerId}, '{ServerName}', 'db0', 'dbo', 'PGAP', 'P', 'shg', 'phg', 3, 100000, 100000, 300
                FROM generate_series($1::timestamp, $2::timestamp, interval '5 minutes') AS t
                """, start.AddHours(8).AddMinutes(1), start.AddHours(11));
        }

        /* A refresh over a sub-range leaves the rest unmaterialized. The gap case skips hours 8 to 12 of the window: raw holds
           rows there and the rollup holds no bucket, which is the hole the mid route's scan must see. */
        var refreshRanges = shape == SeedShape.MaterializationGap
            ? new[] { (start.AddHours(-3), start.AddHours(8)), (start.AddHours(12), hour) }
            : new[] { (start.AddHours(-3), hour) };
        foreach (var (lo, hi) in refreshRanges)
        {
            await using var refresh = new NpgsqlCommand(
                $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection);
            refresh.Parameters.AddWithValue(lo);
            refresh.Parameters.AddWithValue(hi);
            await refresh.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, start, end, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }
}
