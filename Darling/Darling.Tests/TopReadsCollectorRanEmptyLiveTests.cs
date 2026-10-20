/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6226 against live PostgreSQL: a ranking over a window in which the collector ran successfully and stored no row (nothing
/// executed since its watermark) is <c>empty</c> ("we looked, and nothing ran"), with the run count. A window with no successful
/// run stays <c>unavailable</c> ("could have it, does not right now"). #6127's snapshots-without-a-delta answer is unchanged.
/// </summary>
[Collection("live-postgres")]
public sealed class TopReadsCollectorRanEmptyLiveTests
{
    private const string ServerName = "top-reads-collector-ran-empty-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task QueryStats_RanSuccessfullyAndStoredNothing_IsEmptyWithTheRunCount()
    {
        await RunAsync(async (connection, postgres, now, ct) =>
        {
            for (var minutes = 5; minutes <= 55; minutes += 10)
            {
                await PlantRunAsync(connection, ct, "query_stats", now.AddMinutes(-minutes), "SUCCESS", rows: 0);
            }

            var answer = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, as_of: now.ToString("o"))).RootElement;

            Assert.Equal("empty", answer.GetProperty("status").GetString());
            Assert.Equal("query_stats ran 6 time(s) in the last 1 hour(s) and found no executed statements to store. This is not a collection gap: collection health showing successful runs that stored no rows is consistent with it.",
                answer.GetProperty("message").GetString());
            Assert.Equal(6, answer.GetProperty("hints").GetProperty("successful_runs_in_window").GetInt64());
        });
    }

    [Fact]
    public async Task QueryStats_WithNoSuccessfulRunInTheWindow_StaysUnavailable()
    {
        await RunAsync(async (connection, postgres, now, ct) =>
        {
            /* A failed run in the window and a successful one before it: neither looked at this window. */
            await PlantRunAsync(connection, ct, "query_stats", now.AddMinutes(-10), "ERROR", rows: 0);
            await PlantRunAsync(connection, ct, "query_stats", now.AddHours(-5), "SUCCESS", rows: 0);

            var answer = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, as_of: now.ToString("o"))).RootElement;

            Assert.Equal("unavailable", answer.GetProperty("status").GetString());
            Assert.Equal("No query stats available for the specified time range.", answer.GetProperty("message").GetString());
        });
    }

    [Fact]
    public async Task QueryStats_SnapshotsWithoutADelta_KeepTheSnapshotAnswerEvenWhenTheCollectorRan()
    {
        await RunAsync(async (connection, postgres, now, ct) =>
        {
            await PlantRunAsync(connection, ct, "query_stats", now.AddMinutes(-20), "SUCCESS", rows: 1);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop, sample_interval_seconds)
                  VALUES ($1,$2,$3,$4,'StackOverflow','0xFIRST','0xPLANHASH','0xSQLH','0xPLANH','SELECT 1',0,0,0,0,1,1,0)",
                CollectionIdGenerator.Next(), now.AddMinutes(-20), ServerId, ServerName);

            var answer = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, as_of: now.ToString("o"))).RootElement;

            Assert.Equal("empty", answer.GetProperty("status").GetString());
            Assert.Contains("none carries a measured delta", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal(1, answer.GetProperty("hints").GetProperty("snapshots_in_window").GetInt64());
        });
    }

    [Fact]
    public async Task QueryStats_ADatabaseFilter_ClaimsOnlyThatDatabase()
    {
        await RunAsync(async (connection, postgres, now, ct) =>
        {
            await PlantRunAsync(connection, ct, "query_stats", now.AddMinutes(-10), "SUCCESS", rows: 0);

            var answer = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, database_name: "StackOverflow", as_of: now.ToString("o"))).RootElement;

            Assert.Equal("empty", answer.GetProperty("status").GetString());
            Assert.Contains("stored no executed statements for database 'StackOverflow'", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task PgTopQueries_RanAfterABaselineAndStoredNothing_IsEmpty_ButOneRunIsNotYetAMeasurement()
    {
        await RunAsync(async (connection, postgres, now, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, MonitoredEngineKind.Postgres);

            /* The first run after a restart stores nothing because it has nothing to difference against. */
            await PlantRunAsync(connection, ct, "pg_statement_stats", now.AddMinutes(-10), "SUCCESS", rows: 0);
            var first = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(postgres, ServerName, 1, as_of: now.ToString("o"))).RootElement;
            Assert.Equal("unavailable", first.GetProperty("status").GetString());

            await PlantRunAsync(connection, ct, "pg_statement_stats", now.AddMinutes(-5), "SUCCESS", rows: 0);
            var second = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(postgres, ServerName, 1, as_of: now.ToString("o"))).RootElement;
            Assert.Equal("empty", second.GetProperty("status").GetString());
            Assert.Equal(2, second.GetProperty("hints").GetProperty("successful_runs_in_window").GetInt64());
            Assert.StartsWith("pg_statement_stats ran 2 time(s) in the last 1 hour(s)", second.GetProperty("message").GetString(), StringComparison.Ordinal);
        });
    }

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-ran-empty test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            await body(connection, postgres, now, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task PlantRunAsync(NpgsqlConnection connection, CancellationToken ct, string collector, DateTime at, string status, int rows) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
              VALUES ($1, $2, $3, $4, $5, 120, $6, NULL, $7, 90, 30)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector, at, status, rows);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
