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
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")] (it also stops the scratch database's TimescaleDB
   background workers, which the shared store must never inherit). */

/// <summary>
/// #5562 (lane L4): a query trend asked for through the web dispatch past seven days is served from the hourly rollup
/// (the Custom Views way: age picks the tier through <c>DurationTrendRouting</c>), and the read's ceiling is the one the
/// reach table declares. A window of 60 days back holds the rolled-up hour and not the raw rows (a TimescaleDB scratch
/// store; the raw rows are purged after the rollup refreshes, as retention does at four days).
/// </summary>
public sealed class WebReadReachLiveTests
{
    private const int ServerId = -556201;
    private const string ServerName = "a5562-reach-hourly";
    private const string Db = "ReachDb";

    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string read, int hours)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(new List<KeyValuePair<string, string?>>
        {
            new("server_name", ServerName),
            new("hours_back", hours.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        });
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    [Fact]
    public async Task TheDurationTrends_PastSevenDays_ReadTheHourlyRollup_AndRefuseBeyondNinetyDays()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live reach test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live reach test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* One busy hour, 60 days back by the wall clock: far past the raw tier's four days, well inside the hourly
               rollup's ninety. */
            var now = DateTime.UtcNow;
            var hour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddDays(-60);
            var at = hour.AddMinutes(10);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                  VALUES ($1, $2, $3, $4, $5, '0xQREACH', '0xSQLHREACH', 10, 3600000, 3600000, 100, 1000, 3600)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                  VALUES ($1, $2, $3, $4, $5, 'dbo', 'usp_REACH', '0xSQLHREACH', 10, 3600000, 3600000, 100, 1000, 3600)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db);

            foreach (var view in new[] { TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(hour.AddHours(-1));
                refresh.Parameters.AddWithValue(hour.AddHours(2));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            foreach (var table in new[] { "collect.query_stats", "collect.procedure_stats" })
            {
                await using var purge = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
                purge.Parameters.AddWithValue(ServerId);
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            foreach (var read in new[] { "get_query_duration_trend", "get_procedure_duration_trend" })
            {
                /* 2160 hours is the read's declared reach and reaches the planted hour: the rollup answers, labelled hourly. */
                var json = await WebReadAsync(postgres, read, WebReadReach.RollupTrendHours);
                using var doc = JsonDocument.Parse(json);
                Assert.False(doc.RootElement.TryGetProperty("error", out _), $"{read} refused its own declared reach: {json}");
                Assert.Equal("hourly", doc.RootElement.GetProperty("source").GetString());
                var points = doc.RootElement.GetProperty("trend").EnumerateArray().ToArray();
                Assert.NotEmpty(points);
                Assert.Contains(points, p => p.GetProperty("elapsed_ms_per_second").GetDouble() > 0);

                /* One hour past the reach is refused, never clamped. */
                var refused = McpHelpers.ErrorMessageOf(await WebReadAsync(postgres, read, WebReadReach.RollupTrendHours + 1));
                Assert.Contains("exceeds maximum of 2160 hours (90 days)", refused);
            }

            /* Query Store's trend takes the same ceiling: 2160 is answered (an empty status is an answer), 2161 is refused. */
            var qs = await WebReadAsync(postgres, "get_query_store_duration_trend", WebReadReach.RollupTrendHours);
            Assert.DoesNotContain("exceeds maximum", qs);
            Assert.Contains("exceeds maximum of 2160 hours (90 days)",
                McpHelpers.ErrorMessageOf(await WebReadAsync(postgres, "get_query_store_duration_trend", WebReadReach.RollupTrendHours + 1)));

            /* A read that did not opt in keeps 168: a ranking that routes through the same rollup. */
            foreach (var read in new[] { "get_top_queries_by_cpu" })
            {
                var refused = McpHelpers.ErrorMessageOf(await WebReadAsync(postgres, read, McpHelpers.MaxHoursBack + 1));
                Assert.Contains("exceeds maximum of 168 hours (7 days)", refused);
            }

            /* The raw-table trends reach their table's 30 days (#5562 L4b): 720 is answered, 721 is refused, never clamped. */
            foreach (var read in new[] { "get_tempdb_trend", "get_memory_trend", "get_cpu_utilization" })
            {
                /* No "error" key at all, not merely a message without the ceiling text: a read that failed for another reason must not pass (review L6). */
                var answered = await WebReadAsync(postgres, read, WebReadReach.RawTrendHours);
                using (var answeredDoc = JsonDocument.Parse(answered))
                {
                    Assert.False(answeredDoc.RootElement.TryGetProperty("error", out _), $"{read} refused its own declared reach: {answered}");
                }

                var refused = McpHelpers.ErrorMessageOf(await WebReadAsync(postgres, read, WebReadReach.RawTrendHours + 1));
                Assert.Contains("exceeds maximum of 720 hours (30 days)", refused);
            }

            /* Raised-then-lowered (#5562 review r1): the perfmon trend (11.2 s cold at 30 days, #5574) and the heatmap (raw query_stats, four days on a TimescaleDB store) stay at 168. */
            foreach (var read in new[] { "get_query_heatmap" })
            {
                Assert.Contains("exceeds maximum of 168 hours (7 days)",
                    McpHelpers.ErrorMessageOf(await WebReadAsync(postgres, read, McpHelpers.MaxHoursBack + 1)));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt)));
            });
        }
    }
}
