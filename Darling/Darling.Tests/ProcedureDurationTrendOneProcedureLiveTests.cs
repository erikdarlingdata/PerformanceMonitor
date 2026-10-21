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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #6303: get_procedure_duration_trend limited to ONE procedure, by the schema-qualified name the Top Procedures grid shows
/// (<c>full_name</c>) inside one database, so the web can draw a procedure's own history. The weights make a wrong match show in the
/// value: the same name in another database, a sibling in the same database and a procedure with no schema each carry a different
/// weight, and a call must read exactly the one it named. Raw tier on the shared live store (the hourly rollup shares the builder's
/// predicate, built by <see cref="DurationTrendRouting.BuildBucketedHourlyTrendSql"/> with the same name parameter).
/// </summary>
[Collection("live-postgres")]
public sealed class ProcedureDurationTrendOneProcedureLiveTests
{
    private const string A = "OneProcA";
    private const string B = "OneProcB";
    private const double PerSecond = 1.0 / 1000 / 3600;

    private static Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, string schema, string name, long weight) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats
                  (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                   delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 10, $9, $9, 100, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db, schema, name, "0xH" + db + schema + name, weight);

    [Fact]
    public Task AProcedureName_ReadsOnlyThatProcedure_InThatDatabase_AndIsRefusedWithoutOne() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a6303-one-procedure", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var at = now.AddHours(-2);
            await PlantAsync(connection, ct, serverId, serverName, at, A, "dbo", "usp_One", 3_000);
            await PlantAsync(connection, ct, serverId, serverName, at, A, "dbo", "usp_Two", 5_000);
            await PlantAsync(connection, ct, serverId, serverName, at, A, "", "usp_Bare", 1_000);
            await PlantAsync(connection, ct, serverId, serverName, at, B, "dbo", "usp_One", 7_000);

            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            async Task<string> Read(DatabaseFilter databases, string? procedure) =>
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 24, null, null, databases, budget, procedure, ct);

            Assert.Equal(3_000 * PerSecond, DurationTrendDatabaseFilterLiveTests.LastRate(await Read(DatabaseFilter.One(A), "dbo.usp_One"))!.Value, 9);
            Assert.Equal(5_000 * PerSecond, DurationTrendDatabaseFilterLiveTests.LastRate(await Read(DatabaseFilter.One(A), "dbo.usp_Two"))!.Value, 9);
            /* The same name in the other database is that database's weight, never the sum of both. */
            Assert.Equal(7_000 * PerSecond, DurationTrendDatabaseFilterLiveTests.LastRate(await Read(DatabaseFilter.One(B), "dbo.usp_One"))!.Value, 9);
            /* A procedure with no schema is shown, and so matched, by its bare name. */
            Assert.Equal(1_000 * PerSecond, DurationTrendDatabaseFilterLiveTests.LastRate(await Read(DatabaseFilter.One(A), "usp_Bare"))!.Value, 9);

            /* No procedure name: the database's whole sum, and the payload carries no procedure_name key (the old output). */
            var wholeDatabase = JsonDocument.Parse(await Read(DatabaseFilter.One(A), null)).RootElement;
            Assert.Equal(9_000 * PerSecond, wholeDatabase.GetProperty("trend").EnumerateArray().Last().GetProperty("elapsed_ms_per_second").GetDouble(), 9);
            Assert.False(wholeDatabase.TryGetProperty("procedure_name", out _));

            var one = JsonDocument.Parse(await Read(DatabaseFilter.One(A), "dbo.usp_One")).RootElement;
            Assert.Equal("dbo.usp_One", one.GetProperty("procedure_name").GetString());
            Assert.Equal(A, one.GetProperty("database_name").GetString());

            /* A name nothing ran under in the window is an empty answer that still says which procedure it was asked about, never a 0 line. */
            var missing = JsonDocument.Parse(await Read(DatabaseFilter.One(A), "dbo.usp_NoSuch")).RootElement;
            Assert.Equal("empty", missing.GetProperty("status").GetString());
            Assert.Equal("dbo.usp_NoSuch", missing.GetProperty("procedure_name").GetString());

            /* Case-sensitive, as the grid's name is. */
            Assert.Equal("empty", JsonDocument.Parse(await Read(DatabaseFilter.One(A), "DBO.USP_ONE")).RootElement.GetProperty("status").GetString());

            /* A name needs exactly one database: with none or two it is refused, not summed across databases. */
            foreach (var databases in new[] { DatabaseFilter.All, DatabaseFilter.Of([A, B]) })
            {
                var refused = JsonDocument.Parse(await Read(databases, "dbo.usp_One")).RootElement;
                Assert.Equal("invalid", refused.GetProperty("status").GetString());
                Assert.Equal("procedure_name", refused.GetProperty("hints").GetProperty("parameter").GetString());
            }
        }, DurationTrendDatabaseFilterLiveTests.Cleanup);

    /// <summary>
    /// The hourly rollup tier (a TimescaleDB scratch store; the window's raw rows are purged after the interval rollup refreshes, so
    /// the route reads <c>procedure_stats_interval_hourly</c>, which groups by schema and object). Same weights as the raw test.
    /// </summary>
    [Fact]
    public async Task TheHourlyTier_AProcedureName_ReadsOnlyThatProcedure()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly one-procedure trend test.");
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: the scratch database is this test's own, so the rollup refresh and purge below touch nothing shared.
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly one-procedure trend test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        const int serverId = -6303;
        const string serverName = "a6303-one-procedure-hourly";
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var windowStart = new DateTime(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
            var windowEnd = windowStart.AddDays(1);
            var hourOne = windowStart.AddHours(1);
            foreach (var (db, schema, name, weight, minutes) in new[]
            {
                (A, "dbo", "usp_One", 3_000L, 10), (A, "dbo", "usp_Two", 5_000L, 20), (B, "dbo", "usp_One", 7_000L, 30),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 10, $9, $9, 100, 1000, 3600)",
                    CollectionIdGenerator.Next(), hourOne.AddMinutes(minutes), serverId, serverName, db, schema, name, "0xH" + db + name, weight);
            }

            /* The survivor keeps raw's oldest row later than the rollup's floor, so the route reads the rollup (as the database filter's hourly test does). */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                  VALUES ($1, $2, $3, $4, $5, 'dbo', 'usp_Surv', '0xHSURV', 1, 1000, 1000, 7, 1000, 3600)",
                CollectionIdGenerator.Next(), windowStart.AddHours(5), serverId, serverName, A);

            await using (var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection))
            {
                refresh.Parameters.AddWithValue(windowStart);
                refresh.Parameters.AddWithValue(windowEnd.AddHours(1));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            await using (var purge = new NpgsqlCommand("DELETE FROM collect.procedure_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection))
            {
                purge.Parameters.AddWithValue(serverId);
                purge.Parameters.AddWithValue(windowStart);
                purge.Parameters.AddWithValue(windowStart.AddHours(3));
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var hourly = NpgsqlDataSource.Create(scratch.ConnectionString);
            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            var asOf = DateTime.SpecifyKind(windowEnd, DateTimeKind.Utc).ToString("o");

            async Task<JsonElement> Read(string db, string procedure) =>
                JsonDocument.Parse(await DarlingMcpTrendTools.GetProcedureDurationTrend(hourly, serverName, 24, asOf, 60, DatabaseFilter.One(db), budget, procedure, ct)).RootElement;

            double RateAt(JsonElement payload, DateTime hour) => payload.GetProperty("trend").EnumerateArray()
                .First(p => DateTime.Parse(p.GetProperty("time").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind) == hour)
                .GetProperty("elapsed_ms_per_second").GetDouble();

            var one = await Read(A, "dbo.usp_One");
            Assert.Equal("hourly", one.GetProperty("source").GetString());
            Assert.Equal(3_000 * PerSecond, RateAt(one, hourOne), 9);
            Assert.Equal(5_000 * PerSecond, RateAt(await Read(A, "dbo.usp_Two"), hourOne), 9);
            Assert.Equal(7_000 * PerSecond, RateAt(await Read(B, "dbo.usp_One"), hourOne), 9);
            Assert.Equal("dbo.usp_One", one.GetProperty("procedure_name").GetString());
            Assert.Equal("empty", (await Read(A, "dbo.usp_NoSuch")).GetProperty("status").GetString());

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
