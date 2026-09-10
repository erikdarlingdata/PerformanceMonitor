/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// Live-Postgres pins for #2705: QUERY_HIGH_DOP read <c>v_query_stats.max_dop</c> — a lifetime-max for
/// the plan's time in cache, the same semantics <c>get_top_queries_by_cpu</c>'s own tool description has
/// always warned about — with no guard, and fed it into a finding at full confidence. A plan compiled
/// before <c>max degree of parallelism</c> was lowered keeps reporting its old, higher DOP until it is
/// evicted or recompiled, so on a server now configured to MAXDOP 1 a cached max_dop of 16 is not a
/// current problem; it is a stale high-water mark that predates the configuration change.
///
/// <para>Live rather than a string pin because the fix is a JOIN against <c>server_config</c> evaluated
/// by the real Postgres engine — whether the CTE actually excludes the provably-impossible row, and
/// whether the LEFT JOIN's NULL-safe fallback still counts the row when no <c>server_config</c> data
/// exists at all, are both engine behavior a text assertion cannot see.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class QueryHighDopStaleMaxDopLiveTests
{
    private const int TestServerId = -270200;
    private const string TestServerName = "StaleMaxDopSrv";
    private const string Db = "StaleMaxDopDb";

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static async Task SeedHighDopQueryStatsAsync(NpgsqlConnection c, DateTime t, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
     min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)", c);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(t, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue("0xHIGHDOPHASH");
        command.Parameters.AddWithValue("0xHIGHDOPPLANHASH");
        command.Parameters.AddWithValue("0xHIGHDOPSQLH");
        command.Parameters.AddWithValue("0xHIGHDOPPLANH");
        command.Parameters.AddWithValue("SELECT * FROM StaleMaxDopTable");
        command.Parameters.AddWithValue(10L);      // delta_execution_count
        command.Parameters.AddWithValue(10_000L);  // delta_worker_time
        command.Parameters.AddWithValue(20_000L);  // delta_elapsed_time
        command.Parameters.AddWithValue(500L);     // delta_logical_reads
        command.Parameters.AddWithValue(0L);       // delta_logical_writes
        command.Parameters.AddWithValue(5L);       // delta_physical_reads
        command.Parameters.AddWithValue(100L);     // delta_rows
        command.Parameters.AddWithValue(0L);       // delta_spills
        command.Parameters.AddWithValue(1);        // min_dop
        command.Parameters.AddWithValue(16);       // max_dop — the stale high-water mark
        command.Parameters.AddWithValue(800L);     // min_worker_time
        command.Parameters.AddWithValue(1500L);    // max_worker_time
        command.Parameters.AddWithValue(1600L);    // min_elapsed_time
        command.Parameters.AddWithValue(3000L);    // max_elapsed_time
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedServerConfigMaxDopAsync(
        NpgsqlConnection c, DateTime captureTime, long valueInUse, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use,
     is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", c);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue("max degree of parallelism");
        command.Parameters.AddWithValue(valueInUse);
        command.Parameters.AddWithValue(valueInUse);
        command.Parameters.AddWithValue(true);
        command.Parameters.AddWithValue(false);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var command = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection))
        {
            command.Parameters.AddWithValue(TestServerId);
            await command.ExecuteNonQueryAsync(ct);
        }

        await using (var command = new NpgsqlCommand("DELETE FROM server_config WHERE server_id = $1", connection))
        {
            command.Parameters.AddWithValue(TestServerId);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    [Fact]
    public async Task QueryHighDop_DoesNotFire_WhenMaxDopExceedsCurrentServerConfig()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #2705 stale-max_dop test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.UtcNow);
            var periodStart = periodEnd.AddHours(-4);
            var seedTime = periodEnd.AddMinutes(-30);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodStart,
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            /* ── #2705, RED under the old query: the plan's lifetime max_dop (16) predates the server
                  being reconfigured to MAXDOP 1, which makes DOP > 1 impossible right now. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedHighDopQueryStatsAsync(connection, seedTime, ct);
                await SeedServerConfigMaxDopAsync(connection, seedTime.AddMinutes(5), valueInUse: 1, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
                Assert.DoesNotContain(facts, f => f.Key == "QUERY_HIGH_DOP");
            }

            /* ── The unlimited case: current MAXDOP 0 makes no reading impossible, so the finding still
                  fires exactly as before the fix. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedHighDopQueryStatsAsync(connection, seedTime, ct);
                await SeedServerConfigMaxDopAsync(connection, seedTime.AddMinutes(5), valueInUse: 0, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
                var fact = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
                Assert.Equal(1.0, fact.Metadata["high_dop_query_count"]);
            }

            /* ── No server_config row at all (never collected, or a target this collector doesn't apply
                  to): unknown current MAXDOP corroborates nothing, so the count is unchanged rather than
                  manufacturing confidence either way — same "omit rather than invent" rule the CPU-
                  attribution ratio already follows. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedHighDopQueryStatsAsync(connection, seedTime, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
                var fact = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
                Assert.Equal(1.0, fact.Metadata["high_dop_query_count"]);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }
}
