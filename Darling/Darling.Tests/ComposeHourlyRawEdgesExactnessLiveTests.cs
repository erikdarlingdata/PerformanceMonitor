/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: the live exactness proof for the hourly-plus-raw-edges route on <c>query_stats</c> and
/// <c>procedure_stats</c>. Seeds ~30 hours of one-minute raw rows (restart rows at
/// <c>sample_interval_seconds = 0</c>, pre-column rows at NULL), refreshes the product's successor hourly
/// rollups through the real scheduler, reads the coverage through the product, and compiles the SAME panel
/// twice through <see cref="ComposeCompiler"/>: once with the real coverage (must take
/// <see cref="ComposeSourceTier.HourlyRawEdges"/>) and once with no coverage (must take
/// <see cref="ComposeSourceTier.Raw"/>). Both are executed and the rows compared EXACTLY, with no tolerance.
///
/// <para><b>#1776 own-store</b>: mints its own scratch database through <see cref="ScratchPostgres"/>.</para>
/// </summary>
public sealed class ComposeHourlyRawEdgesExactnessLiveTests
{
    private static readonly string[] s_servers = { "hybrid-exact-1", "hybrid-exact-2" };
    private static readonly string[] s_databases = { "dbA", "dbB" };
    private const int HashCount = 6;

    [Fact]
    public async Task HybridAndRaw_ReturnIdenticalRows_AcrossEveryPanelCase()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #4605 hourly-plus-raw-edges exactness live test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the hourly-plus-raw-edges exactness live test");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await SeedAsync(connection, now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        foreach (var view in new[] { TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView })
        {
            await RunJobViaSchedulerAsync(connection, await ReadJobIdAsync(connection, view, ct), ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, ct);

        var start = now.AddHours(-20).AddMinutes(17);
        var end = now.AddMinutes(-5);

        await AssertIdenticalAsync(connection, rollups, coverage, start, end, now, ct,
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":5,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}",
            "Ranked top-5 SUM query_worker_us by query_hash");
        await AssertIdenticalAsync(connection, rollups, coverage, start, end, now, ct,
            "{\"source\":\"query_stats\",\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\",\"viz\":\"stat\"}",
            "Scalar SUM query_elapsed_us");
        await AssertIdenticalAsync(connection, rollups, coverage, start, end, now, ct,
            "{\"source\":\"query_stats\",\"measure\":\"query_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}",
            "Hour TimeSeries SUM query_executions");
        await AssertIdenticalAsync(connection, rollups, coverage, start, end, now, ct,
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"max\",\"topN\":5,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}",
            "Ranked MAX query_worker_us by query_hash");
        await AssertIdenticalAsync(connection, rollups, coverage, start, end, now, ct,
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":5,\"groupBy\":[\"object_name\"],\"viz\":\"table\"}",
            "query_stats Ranked SUM by object_name (module join)");
        await AssertIdenticalAsync(connection, rollups, coverage, start, end, now, ct,
            "{\"source\":\"procedure_stats\",\"measure\":\"proc_worker_us\",\"aggregate\":\"sum\",\"topN\":5,\"groupBy\":[\"object_name\"],\"viz\":\"table\"}",
            "procedure_stats Ranked SUM by object_name");
        await AssertIdenticalAsync(connection, rollups, coverage, start, end, now, ct,
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":5,\"groupBy\":[\"query_hash\"],\"viz\":\"table\","
            + "\"filters\":[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"dbA\"}]}",
            "Ranked SUM with a database_name filter");
    }

    /* ---- seed: 30 h of 1-minute raw rows, restart rows (interval 0) and pre-column rows (NULL interval) ---- */

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        var from = now.AddHours(-30);
        var to = now;
        var servers = string.Join(",", s_servers.Select(s => "'" + s + "'"));
        var databases = string.Join(",", s_databases.Select(d => "'" + d + "'"));

        /* One row per minute for every (server, database, hash|object). The deltas vary with the minute and the
           identity so a dropped or doubled row moves a sum; the max varies per group and hour. */
        await using (var queries = new NpgsqlCommand(
            $@"INSERT INTO collect.query_stats
                (collection_id, server_id, server_name, database_name, query_hash, sql_handle, collection_time,
                 delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
              SELECT $3 + row_number() OVER (), -460501 - s.n, s.name, d.name, 'qh' || h.n, 'sh' || (h.n % 3), gs,
                     1 + ((extract(epoch FROM gs)::bigint / 60) * (7 + h.n) + d.n * 31 + s.n * 17) % 1000,
                     1 + ((extract(epoch FROM gs)::bigint / 60) * (11 + h.n) + d.n * 13) % 5000,
                     1 + ((extract(epoch FROM gs)::bigint / 60) + h.n) % 9,
                     CASE WHEN (extract(epoch FROM gs)::bigint / 60) % 97 = 0 THEN 0
                          WHEN (extract(epoch FROM gs)::bigint / 60) % 89 = 0 THEN NULL
                          ELSE 60 END
              FROM generate_series(date_trunc('minute', $1::timestamp), $2::timestamp, INTERVAL '1 minute') AS gs
              CROSS JOIN (SELECT row_number() OVER () - 1 AS n, name FROM unnest(ARRAY[{servers}]) AS name) AS s
              CROSS JOIN (SELECT row_number() OVER () - 1 AS n, name FROM unnest(ARRAY[{databases}]) AS name) AS d
              CROSS JOIN generate_series(0, {HashCount - 1}) AS h(n)",
            connection))
        {
            queries.CommandTimeout = 300;
            queries.Parameters.AddWithValue(from);
            queries.Parameters.AddWithValue(to);
            queries.Parameters.AddWithValue(CollectionIdGenerator.Next());
            await queries.ExecuteNonQueryAsync(ct);
        }

        await using (var procs = new NpgsqlCommand(
            $@"INSERT INTO collect.procedure_stats
                (collection_id, server_id, server_name, database_name, schema_name, object_name, sql_handle, collection_time,
                 delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
              SELECT $3 + row_number() OVER (), -460501 - s.n, s.name, d.name, 'dbo', 'usp_' || h.n, 'sh' || h.n, gs,
                     1 + ((extract(epoch FROM gs)::bigint / 60) * (5 + h.n) + d.n * 29 + s.n * 19) % 900,
                     1 + ((extract(epoch FROM gs)::bigint / 60) * (13 + h.n) + d.n * 7) % 4000,
                     1 + ((extract(epoch FROM gs)::bigint / 60) + h.n) % 7,
                     CASE WHEN (extract(epoch FROM gs)::bigint / 60) % 101 = 0 THEN 0
                          WHEN (extract(epoch FROM gs)::bigint / 60) % 83 = 0 THEN NULL
                          ELSE 60 END
              FROM generate_series(date_trunc('minute', $1::timestamp), $2::timestamp, INTERVAL '1 minute') AS gs
              CROSS JOIN (SELECT row_number() OVER () - 1 AS n, name FROM unnest(ARRAY[{servers}]) AS name) AS s
              CROSS JOIN (SELECT row_number() OVER () - 1 AS n, name FROM unnest(ARRAY[{databases}]) AS name) AS d
              CROSS JOIN generate_series(0, 2) AS h(n)",
            connection))
        {
            procs.CommandTimeout = 300;
            procs.Parameters.AddWithValue(from);
            procs.Parameters.AddWithValue(to);
            procs.Parameters.AddWithValue(CollectionIdGenerator.Next() + 10_000_000);
            await procs.ExecuteNonQueryAsync(ct);
        }
    }

    /* ---- compile the same panel twice (real coverage, no coverage) and compare rows exactly ---- */

    private static async Task AssertIdenticalAsync(
        NpgsqlConnection connection, RollupAvailability rollups, RollupCoverage coverage,
        DateTime start, DateTime end, DateTime now, CancellationToken ct, string planJson, string caseName)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(planJson)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        var hybridContext = new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, rollups, now, coverage);
        var rawContext = new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, rollups, now, RollupCoverage.Unknown);

        var (hybridTier, hybridRows) = await RunAsync(connection, plan!, hybridContext, ct);
        var (rawTier, rawRows) = await RunAsync(connection, plan!, rawContext, ct);

        Assert.True(hybridTier == ComposeSourceTier.HourlyRawEdges, $"{caseName}: expected the hybrid route, got {hybridTier}");
        Assert.True(rawTier == ComposeSourceTier.Raw, $"{caseName}: expected the raw route, got {rawTier}");
        Assert.True(rawRows.Count > 0, $"{caseName}: the seed produced no rows; the comparison would be vacuous");

        var rawSet = rawRows.Select(RowKey).OrderBy(k => k, StringComparer.Ordinal).ToList();
        var hybridSet = hybridRows.Select(RowKey).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.True(rawSet.SequenceEqual(hybridSet),
            $"{caseName}: raw and hybrid rows differ.\nraw:    {string.Join(" ; ", rawSet.Take(6))}\nhybrid: {string.Join(" ; ", hybridSet.Take(6))}");
    }

    private static string RowKey(object?[] row) =>
        string.Join("|", row.Select(v => v is null ? "<null>" : Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)));

    private static async Task<(ComposeSourceTier Tier, List<object?[]> Rows)> RunAsync(
        NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, compileError) = ComposeCompiler.Compile(plan, context);
        Assert.True(compileError is null, compileError);
        Assert.NotNull(compiled);

        var rows = new List<object?[]>();
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        command.CommandTimeout = 300;
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new object?[reader.FieldCount];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = await reader.IsDBNullAsync(i, ct) ? null : reader.GetValue(i);
            }

            rows.Add(values);
        }

        return (compiled.Route.Tier, rows);
    }

    /* ---- the successor rollups' own refresh policy, run through the real scheduler ---- */

    private static async Task<int> ReadJobIdAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            @"SELECT j.job_id FROM timescaledb_information.jobs AS j
              JOIN timescaledb_information.continuous_aggregates AS ca
                ON ca.view_schema = j.hypertable_schema AND ca.view_name = j.hypertable_name
              WHERE j.proc_name = 'policy_refresh_continuous_aggregate' AND ca.view_name = $1",
            connection);
        read.Parameters.AddWithValue(view);
        return Convert.ToInt32(await read.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task RunJobViaSchedulerAsync(NpgsqlConnection connection, int jobId, CancellationToken ct)
    {
        var before = await ReadLastSuccessfulFinishAsync(connection, jobId, ct);
        await using (var arm = new NpgsqlCommand("SELECT alter_job($1::integer, scheduled => true, next_start => now())", connection))
        {
            arm.Parameters.AddWithValue(jobId);
            await arm.ExecuteNonQueryAsync(ct);
        }

        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (await ReadLastSuccessfulFinishAsync(connection, jobId, ct) <= before)
        {
            Assert.True(DateTime.UtcNow < deadline, $"the scheduler did not complete a run of job {jobId} within 120s of next_start => now()");
            await Task.Delay(500, ct);
        }
    }

    private static async Task<DateTime> ReadLastSuccessfulFinishAsync(NpgsqlConnection connection, int jobId, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand("SELECT last_successful_finish FROM timescaledb_information.job_stats WHERE job_id = $1", connection);
        read.Parameters.AddWithValue(jobId);
        var value = await read.ExecuteScalarAsync(ct);
        return value is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : DateTime.MinValue;
    }
}
