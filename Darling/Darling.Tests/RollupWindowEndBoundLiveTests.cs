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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A rollup bucket is stamped at its START, so a read that bounds the window end with <c>bucket &lt;= end</c>
/// took the whole bucket that begins AT the end whenever the end fell exactly on a bucket start: one extra hour
/// in an hourly sum. These live pins seed one query and one procedure in three hours (H0 = 1 execution, H1 = 10,
/// H2 = 100, so a total names its hours), refresh the hourly rollups, and read each rollup path with three
/// ends: (i) exactly H2, which must NOT add the hour that begins there (11); (ii) H2 plus 25 minutes, with the
/// newest bucket still open as "now" is, which counts H2 (111); (iii) H1 plus 30 minutes, inside H1 (11, the
/// same as before the change). Every raw row is stamped 10 minutes past its hour, never exactly on it: the raw
/// arm still reads a row stamped exactly at the end, so an exact-on-the-hour stamp would blur the two arms.
///
/// <para>Covered here against real rollups: the MCP top-queries and top-procedures reads, the viewer's Queries
/// and Procedures tab reads (with the raw arm's total for the same window as the agreement check), the hourly
/// duration trend (plain and bucketed), the one-query hourly history, and a Custom View panel on the hourly
/// route. The Query Store rollup trend and the daily route of a Custom View are pinned at the text level in
/// <c>RollupWindowEndBoundTests</c>.</para>
///
/// <para><b>#1776 own-store</b> — each test mints a scratch database (it materializes continuous aggregates the
/// shared fixture must never inherit), so the class is deliberately NOT in the <c>live-postgres</c>
/// collection.</para>
/// </summary>
public sealed class RollupWindowEndBoundLiveTests
{
    private const int ServerId = -947101;
    private const string ServerName = "rollup-window-end-bound";
    private const string ComposeServerName = "rollup-window-end-compose";
    private const int ComposeServerId = -947102;
    private const string Db = "RollupEndDb";
    private const string QueryHash = "0xRWEQ1";
    private const string ProcName = "usp_RollupEnd";

    /// <summary>A fixed anchor, never wall-clock relative: the window is far older than raw retention, and it is
    /// the purge of raw's rows (and the refresh of the rollup) that drives the router, not calendar time.</summary>
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime H0 = WindowStart.AddHours(1);
    private static readonly DateTime H1 = WindowStart.AddHours(2);
    private static readonly DateTime H2 = WindowStart.AddHours(3);

    /// <summary>The three window ends and what each one sums, in executions.</summary>
    private static readonly (string Label, DateTime End, long Executions, DateTime[] Buckets)[] Ends =
    {
        ("end exactly at H2", H2, 11L, new[] { H0, H1 }),
        ("end 25 minutes into H2", H2.AddMinutes(25), 111L, new[] { H0, H1, H2 }),
        ("end 30 minutes into H1", H1.AddMinutes(30), 11L, new[] { H0, H1 }),
    };

    [Fact]
    public async Task QueryAndProcedureReads_StopAtTheWindowEnd_OnRawAndOnTheHourlyRollup()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live rollup window-end test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live rollup window-end test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var windowEnd = WindowStart.AddDays(1);
        var bodySucceeded = false;
        try
        {
            /* One query and one procedure in three hours: 1, 10 and 100 executions, each stamped 10 minutes past its hour. */
            var hours = new[] { H0, H1, H2 };
            var executions = new[] { 1L, 10L, 100L };
            for (var i = 0; i < hours.Length; i++)
            {
                await PlantQueryAsync(connection, ct, ServerId, ServerName, hours[i].AddMinutes(10), executions[i]);
                await PlantProcedureAsync(connection, ct, hours[i].AddMinutes(10), executions[i]);
            }

            /* ── raw is still present: this is the tier the hourly reads must agree with. A viewer instance caches
               its rollup probe for five minutes, so the raw-stage instance is never reused after the refresh. ── */
            await using var rawSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var rawViewer = new ViewerDataService(scratch.ConnectionString);
            foreach (var (label, end, expected, _) in Ends)
            {
                var mcpQueries = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                    rawSource, ServerId, WindowStart, end, top: 10, databaseName: null, cancellationToken: ct);
                Assert.Equal(RetentionTier.Raw, mcpQueries.Tier);
                Assert.Equal(expected, mcpQueries.Rows.Sum(r => r.TotalExecutions));

                var mcpProcedures = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                    rawSource, ServerId, WindowStart, end, top: 10, databaseName: null, cancellationToken: ct);
                Assert.Equal(RetentionTier.Raw, mcpProcedures.Tier);
                Assert.Equal(expected, mcpProcedures.Rows.Sum(r => r.TotalExecutions));

                var (viewerQueries, queriesTier) = await rawViewer.GetTopQueriesByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("raw", queriesTier);
                Assert.True(expected == viewerQueries.Sum(r => r.TotalExecutions), $"viewer raw queries, {label}");

                var (viewerProcedures, proceduresTier) = await rawViewer.GetTopProceduresByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("raw", proceduresTier);
                Assert.True(expected == viewerProcedures.Sum(r => r.TotalExecutions), $"viewer raw procedures, {label}");
            }

            /* Refresh the hourly successors over the window BEFORE deleting raw: the product's own refresh path,
               not a hand-built rollup row. */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);

            /* ── the trend and history builders, run over the seeded hourly relations ── */
            var queryRollup = "collect." + TimescaleSupport.QueryStatsIntervalHourlyView;
            var procedureRollup = "collect." + TimescaleSupport.ProcedureStatsIntervalHourlyView;
            foreach (var (label, end, _, buckets) in Ends)
            {
                foreach (var rollup in new[] { queryRollup, procedureRollup })
                {
                    var plain = await ReadFirstColumnTimesAsync(
                        connection, DurationTrendRouting.BuildHourlyTrendSql(rollup, withDatabaseFilter: false), ct, ServerId, WindowStart, end);
                    Assert.True(buckets.SequenceEqual(plain), $"hourly duration trend over {rollup}, {label}: [{string.Join(", ", plain)}]");

                    var bucketed = await ReadFirstColumnTimesAsync(
                        connection, DurationTrendRouting.BuildBucketedHourlyTrendSql(rollup), ct, ServerId, WindowStart, end, 60);
                    Assert.True(buckets.SequenceEqual(bucketed), $"bucketed hourly duration trend over {rollup}, {label}: [{string.Join(", ", bucketed)}]");
                }

                var history = await ReadFirstColumnTimesAsync(
                    connection, DarlingTrendReader.QueryHistoryHourlySqlFor(queryRollup), ct, ServerId, Db, QueryHash, WindowStart, end);
                Assert.True(buckets.SequenceEqual(history), $"one-query hourly history, {label}: [{string.Join(", ", history)}]");
            }

            /* ── move raw's floor past the window: delete W's raw rows, as retention would. ── */
            foreach (var table in new[] { "query_stats", "procedure_stats" })
            {
                await using var purge = new NpgsqlCommand(
                    $"DELETE FROM collect.{table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* ── the hourly tier answers. FRESH data source and viewer: coverage is cached per instance, and the
               instances above cached the null hourly floor measured before the refresh. ── */
            await using var hourlySource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var hourlyViewer = new ViewerDataService(scratch.ConnectionString);
            foreach (var (label, end, expected, _) in Ends)
            {
                var mcpQueries = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                    hourlySource, ServerId, WindowStart, end, top: 10, databaseName: null, cancellationToken: ct);
                Assert.Equal(RetentionTier.Hourly, mcpQueries.Tier);
                Assert.True(expected == mcpQueries.Rows.Sum(r => r.TotalExecutions), $"MCP hourly top queries, {label}");

                var mcpProcedures = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                    hourlySource, ServerId, WindowStart, end, top: 10, databaseName: null, cancellationToken: ct);
                Assert.Equal(RetentionTier.Hourly, mcpProcedures.Tier);
                Assert.True(expected == mcpProcedures.Rows.Sum(r => r.TotalExecutions), $"MCP hourly top procedures, {label}");

                var (viewerQueries, queriesTier) = await hourlyViewer.GetTopQueriesByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("hourly", queriesTier);
                Assert.True(expected == viewerQueries.Sum(r => r.TotalExecutions), $"viewer hourly queries, {label}");

                var (viewerProcedures, proceduresTier) = await hourlyViewer.GetTopProceduresByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("hourly", proceduresTier);
                Assert.True(expected == viewerProcedures.Sum(r => r.TotalExecutions), $"viewer hourly procedures, {label}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch.ConnectionString, bodySucceeded);
        }
    }

    /// <summary>
    /// A Custom View panel on the hourly route sums the hours up to the window end and stops before the hour that
    /// begins at it. The same three ends, the same rows: the panel's worker time is 1,000 us per execution and it
    /// reports the measure's default unit, ms, so the sums name their hours (11 / 111 / 11).
    /// </summary>
    [Fact]
    public async Task CustomViewPanel_OnTheHourlyRoute_StopsAtTheWindowEnd()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live rollup window-end test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live rollup window-end test needs TimescaleDB: the rollup route reads a continuous aggregate.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ComposeServerId, ComposeServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var hours = new[] { H0, H1, H2 };
            var executions = new[] { 1L, 10L, 100L };
            for (var i = 0; i < hours.Length; i++)
            {
                await PlantQueryAsync(connection, ct, ComposeServerId, ComposeServerName, hours[i].AddMinutes(10), executions[i]);
            }

            /* Both hourly rollups are refreshed, as the rollup-average pin does: the compiler picks between the
               legacy hourly and its successor by coverage, and either must read the same hours. */
            foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView })
            {
                await RefreshAsync(connection, view, WindowStart, WindowStart.AddDays(1), ct);
            }

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

            var json = (JsonObject)JsonNode.Parse(
                "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"viz\":\"table\"}")!;
            var (plan, parseError) = ComposeSpec.TryParsePanel(json, Array.Empty<string>());
            Assert.True(parseError is null, parseError);

            foreach (var (label, end, expected, _) in Ends)
            {
                /* "Now" is days past the window, so the panel is old enough for the rollup route. */
                var context = new ComposeRunContext(
                    new[] { ComposeServerName }, H0, end, ComposeRunContext.NoVariables, rollups, H2.AddDays(5), coverage);
                var (compiled, error) = ComposeCompiler.Compile(plan!, context);
                Assert.True(error is null, error);
                Assert.Equal(ComposeSourceTier.Hourly, compiled!.Route.Tier);

                await using var command = new NpgsqlCommand(compiled.Sql, connection);
                foreach (var p in compiled.Parameters)
                {
                    command.Parameters.Add(p);
                }

                await using var reader = await command.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                var sum = Convert.ToDouble(reader.GetValue(reader.FieldCount - 1));
                /* The panel reports the measure's default unit, ms, so 1,000 us per execution reads as 1. */
                Assert.True(expected == sum, $"Custom View sum of worker time, {label}: {sum:R}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch.ConnectionString, bodySucceeded);
        }
    }

    private static async Task CleanupAsync(string connectionString, bool bodySucceeded)
    {
        await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
        {
            await using var probe = new NpgsqlCommand(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
            var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
            Assert.Equal(0L, schedulers);
        });
    }

    /// <summary>Runs <paramref name="sql"/> with positional parameters and returns the first column of every row
    /// as a time.</summary>
    private static async Task<List<DateTime>> ReadFirstColumnTimesAsync(
        NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in parameters)
        {
            command.Parameters.AddWithValue(value);
        }

        var times = new List<DateTime>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            times.Add(reader.GetDateTime(0));
        }

        return times;
    }

    /// <summary>One query_stats row: worker time is 1,000 us per execution, a real (nonzero) sample interval so the
    /// hourly rollup admits it.</summary>
    private static async Task PlantQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, long executions)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     host_object_name, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(QueryHash);
        insert.Parameters.AddWithValue("0xRWEQ1H");
        insert.Parameters.AddWithValue("usp_RollupEndHost");
        insert.Parameters.AddWithValue(executions * 1_000L);
        insert.Parameters.AddWithValue(executions * 900L);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(3600);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantProcedureAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, long executions)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10, $11)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(ProcName);
        insert.Parameters.AddWithValue("0x" + ProcName);
        insert.Parameters.AddWithValue(executions * 1_000L);
        insert.Parameters.AddWithValue(executions * 900L);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(3600);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
