/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the Darling performance monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 (review follow-up to item 2): <c>QUERY_SPILLS.spilling_query_count</c> is the number of DISTINCT queries that spilled in the
/// window. It used to be a row count over <c>v_query_stats</c> (one row per query per snapshot), so one query spilling in thirty
/// snapshots read as "across 30 distinct queries" in the headline's investigation text.
/// </summary>
[Collection("live-postgres")]
public sealed class QuerySpillsDistinctQueryCountLiveTests
{
    /* #1776 own-store: this fact seeds and removes rows under its own server id in the shared dev store. */
    private const int TestServerId = -563301;
    private const string TestServerName = "QuerySpillsDistinctSrv";
    private const string Db = "SpillsDb";

    [Fact]
    public async Task SpillingQueryCount_CountsDistinctQueries_NotSnapshotRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the QUERY_SPILLS distinct-count live test.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DeleteRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(ct);
                /* Query A spills in three snapshots, query B in one, query C never: three rows with spills, two distinct queries. */
                await SeedAsync(connection, "0xSPILLA", periodEnd.AddMinutes(-90), 100, ct);
                await SeedAsync(connection, "0xSPILLA", periodEnd.AddMinutes(-60), 100, ct);
                await SeedAsync(connection, "0xSPILLA", periodEnd.AddMinutes(-30), 100, ct);
                await SeedAsync(connection, "0xSPILLB", periodEnd.AddMinutes(-45), 50, ct);
                await SeedAsync(connection, "0xSPILLC", periodEnd.AddMinutes(-40), 0, ct);
            }

            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodEnd.AddHours(-4),
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

            var spills = Assert.Single(facts, f => f.Key == "QUERY_SPILLS");
            Assert.Equal(350.0, spills.Metadata["total_spills"]);
            Assert.Equal(2.0, spills.Metadata["spilling_query_count"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task SeedAsync(NpgsqlConnection c, string queryHash, DateTime at, long spills, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
     min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,10,10000,20000,500,0,5,100,$11,1,1,800,1500,1600,3000)", c);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryHash);
        command.Parameters.AddWithValue(queryHash + "PLAN");
        command.Parameters.AddWithValue(queryHash + "SQLH");
        command.Parameters.AddWithValue(queryHash + "PLANH");
        command.Parameters.AddWithValue("SELECT * FROM SpillsTable");
        command.Parameters.AddWithValue(spills);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }
}
