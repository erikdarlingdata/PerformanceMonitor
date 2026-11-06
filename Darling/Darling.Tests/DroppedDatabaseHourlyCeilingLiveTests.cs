/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database (it materializes continuous aggregates the shared fixture must never inherit), so it shares no rows with another test. */
/// <summary>
/// #7725: the hourly arm of Top Queries and Top Procedures, read with a dropped database AND a known materialization ceiling. The dropped
/// database's exclusion sits at one parameter slot and the ceiling at the next, so a wrong slot or a wrong bind order fails only here:
/// the other hourly live tests run with the ceiling clause empty. One hour (H0) is materialized for a heavier dropped database and a lighter
/// listed one; a second hour (H2) holds one more raw row for the listed database that the rollups never materialized, so a ceiling that
/// is bound wrongly (or not at all) adds that row to the listed database's total. The listed database ranks first, the dropped one fills
/// after it marked, and a top-1 page leaves the dropped one out and names it.
/// </summary>
public sealed class DroppedDatabaseHourlyCeilingLiveTests
{
    private const string Gone = "GoneDb";
    private const string Live = "LiveDb";

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime H0 = WindowStart.AddHours(1);
    private static readonly DateTime H2 = WindowStart.AddHours(3);
    private static readonly DateTime End = WindowStart.AddHours(8);

    private static Task PlantQueryAsync(NpgsqlConnection connection, int serverId, string serverName, string database, DateTime at, long weight, string tag, CancellationToken ct)
    {
        var text = "SELECT " + database;
        return DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 10, $12, $12, 100, 0, 0, 1, $12, 1, $12, 1, 1, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, database,
            "0xQ" + database, "0xP" + database, "0xS" + database, "0xL" + database + tag, text, SHA256.HashData(Encoding.UTF8.GetBytes(text)), weight);
    }

    private static Task PlantProcedureAsync(NpgsqlConnection connection, int serverId, string serverName, string database, DateTime at, long executions, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                                                   delta_worker_time, delta_elapsed_time, delta_execution_count, delta_spills, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,'dbo','usp_Work',$6,$7,$8,$9,0,3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, database, "0xH" + database,
            executions * 1_000L, executions * 2_000L, executions);

    private static async Task SnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct, params string[] databases)
    {
        var id = 1;
        foreach (var database in databases)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,$6,'ONLINE',FALSE)",
                CollectionIdGenerator.Next(), DateTime.SpecifyKind(at, DateTimeKind.Unspecified), serverId, serverName, database, id++);
        }
    }

    [Fact]
    public async Task HourlyTopQueriesAndTopProcedures_WithADroppedDatabaseAndAKnownCeiling_ExcludeThenFillMarkedAndBindEverySlot()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly dropped-database ceiling test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly dropped-database ceiling test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        const string serverName = "dropped-hourly-ceiling";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* H0: the dropped database is the HEAVIER one, so a read that ranked it in with the rest would put it first. */
            await PlantQueryAsync(connection, serverId, serverName, Gone, H0.AddMinutes(10), 900_000, "", ct);
            await PlantQueryAsync(connection, serverId, serverName, Live, H0.AddMinutes(10), 100_000, "", ct);
            await PlantProcedureAsync(connection, serverId, serverName, Gone, H0.AddMinutes(10), 900, ct);
            await PlantProcedureAsync(connection, serverId, serverName, Live, H0.AddMinutes(10), 100, ct);
            /* H2: one more raw row for the listed database, which no rollup materializes (they are refreshed to H2's start only). */
            await PlantQueryAsync(connection, serverId, serverName, Live, H2.AddMinutes(10), 7_000, "H2", ct);
            await PlantProcedureAsync(connection, serverId, serverName, Live, H2.AddMinutes(10), 7, ct);
            await SnapshotAsync(connection, serverId, serverName, End.AddHours(-4), ct, Gone, Live);
            await SnapshotAsync(connection, serverId, serverName, End.AddHours(-1), ct, Live);

            foreach (var view in new[]
            {
                TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIoHourlyView,
                TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView,
            })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(WindowStart);
                refresh.Parameters.AddWithValue(H2);
                await refresh.ExecuteNonQueryAsync(ct);
            }

            /* Purge raw's H0 so the router answers from the rollup; H2's raw row stays, so only the ceiling bound keeps it out. */
            foreach (var table in new[] { "query_stats", "procedure_stats" })
            {
                await using var purge = new NpgsqlCommand(
                    $"DELETE FROM collect.{table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
                purge.Parameters.AddWithValue(serverId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(H2);
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
            Assert.Equal(H0.AddHours(1), coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsIoHourlyView, H0));
            Assert.Equal(H0.AddHours(1), coverage.HourlyEndCeiling(TimescaleSupport.ProcedureStatsIoHourlyView, H0));

            var asOf = DateTime.SpecifyKind(End, DateTimeKind.Utc).ToString("o");

            /* ── The service tools. ── */
            using (var queries = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(dataSource, serverName, hours_back: 24, top: 20, as_of: asOf, cancellationToken: ct)))
            {
                Assert.Equal("hourly", queries.RootElement.GetProperty("tier_used").GetString());
                var rows = queries.RootElement.GetProperty("queries").EnumerateArray().ToArray();
                Assert.Equal(new[] { Live, Gone }, rows.Select(q => q.GetProperty("database_name").GetString()!).ToArray());
                Assert.False(rows[0].TryGetProperty("database_dropped", out _));
                Assert.True(rows[1].GetProperty("database_dropped").GetBoolean());
                Assert.Equal(new long[] { 10, 10 }, rows.Select(q => q.GetProperty("execution_count").GetInt64()).ToArray());   /* the H2 row is past the ceiling */
            }

            using (var queriesTop1 = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(dataSource, serverName, hours_back: 24, top: 1, as_of: asOf, cancellationToken: ct)))
            {
                Assert.Equal(new[] { Live }, queriesTop1.RootElement.GetProperty("queries").EnumerateArray().Select(q => q.GetProperty("database_name").GetString()!).ToArray());
                Assert.Equal(new[] { Gone }, queriesTop1.RootElement.GetProperty("databases_left_out").EnumerateArray().Select(n => n.GetString()!).ToArray());
            }

            using (var procedures = JsonDocument.Parse(await DarlingMcpDataTools.GetTopProceduresByCpu(dataSource, serverName, hours_back: 24, top: 20, as_of: asOf, cancellationToken: ct)))
            {
                var rows = procedures.RootElement.GetProperty("procedures").EnumerateArray().ToArray();
                Assert.Equal(new[] { Live, Gone }, rows.Select(p => p.GetProperty("database_name").GetString()!).ToArray());
                Assert.True(rows[1].GetProperty("database_dropped").GetBoolean());
                Assert.Equal(new long[] { 100, 900 }, rows.Select(p => p.GetProperty("execution_count").GetInt64()).ToArray());
            }

            /* ── The Viewer grids. ── */
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var viewerQueries = await viewer.GetTopQueriesByCpuRoutedAsync(serverId, H0, End, 20, cancellationToken: ct);
            Assert.Equal("hourly", viewerQueries.Tier);
            Assert.Equal(new[] { Live, Gone }, viewerQueries.Rows.Select(r => r.DatabaseName).ToArray());
            Assert.Equal(new[] { false, true }, viewerQueries.Rows.Select(r => r.IsDropped).ToArray());
            Assert.Equal(new[] { Live, Gone + " (dropped)" }, viewerQueries.Rows.Select(r => r.DatabaseDisplay).ToArray());
            Assert.Equal(new long[] { 10, 10 }, viewerQueries.Rows.Select(r => r.TotalExecutions).ToArray());

            var viewerQueriesTop1 = await viewer.GetTopQueriesByCpuRoutedAsync(serverId, H0, End, 1, cancellationToken: ct);
            Assert.Equal(new[] { Live }, viewerQueriesTop1.Rows.Select(r => r.DatabaseName).ToArray());

            var viewerProcedures = await viewer.GetTopProceduresByCpuRoutedAsync(serverId, H0, End, 20, cancellationToken: ct);
            Assert.Equal("hourly", viewerProcedures.Tier);
            Assert.Equal(new[] { Live, Gone }, viewerProcedures.Rows.Select(r => r.DatabaseName).ToArray());
            Assert.Equal(new[] { false, true }, viewerProcedures.Rows.Select(r => r.IsDropped).ToArray());
            Assert.Equal(new long[] { 100, 900 }, viewerProcedures.Rows.Select(r => r.TotalExecutions).ToArray());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
