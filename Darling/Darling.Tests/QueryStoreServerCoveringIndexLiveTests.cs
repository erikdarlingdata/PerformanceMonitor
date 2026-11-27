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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */
/// <summary>
/// #9215 (V291): the corrected hourly Query Store rollup keeps ONE server index, the covering
/// <c>(server_id, bucket DESC) INCLUDE (database_name, execution_count_sum, duration_us_weighted_sum)</c>. A fresh store and an upgraded store both end
/// with it and without the plain <c>(server_id, bucket)</c> group index, a second pass changes nothing, and the 7-day duration trend's rollup arm reads
/// from it.
/// </summary>
public sealed class QueryStoreServerCoveringIndexLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9215 covering index tests.";
    private const int ServerCount = 8;
    private const int Groups = 100;
    private const int Databases = 20;
    private const long FirstServerId = -921500;

    private sealed record Store(ScratchPostgres Scratch, NpgsqlConnection Connection);

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<Store> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The live #9215 tests need TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            return new Store(scratch, connection);
        }
        catch
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task<string> MaterializationAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name)
              FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect' AND view_name = $1", c);
        command.Parameters.AddWithValue(TimescaleSupport.QueryStoreStatsCorrectedHourlyView);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The hypertable-level indexes on the materialization, by name, with their key and included column counts.</summary>
    private static async Task<List<(string Name, int Keys, int Total, string FirstKey)>> HypertableIndexesAsync(NpgsqlConnection c, string materialization, CancellationToken ct)
    {
        var rows = new List<(string, int, int, string)>();
        await using var command = new NpgsqlCommand(
            @"SELECT ic.relname, i.indnkeyatts::int, i.indnatts::int, a.attname
              FROM pg_index i
              JOIN pg_class ic ON ic.oid = i.indexrelid
              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
              WHERE i.indrelid = $1::regclass
              ORDER BY ic.relname", c);
        command.Parameters.AddWithValue(materialization);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetString(3)));
        }

        return rows;
    }

    /// <summary>The covering server index is there, once, and no plain (server_id, bucket) index is.</summary>
    private static async Task AssertEndStateAsync(NpgsqlConnection c, CancellationToken ct, string when)
    {
        var materialization = await MaterializationAsync(c, ct);
        var indexes = await HypertableIndexesAsync(c, materialization, ct);
        var covering = indexes.Where(i => i.Name == TimescaleSupport.QueryStoreCorrectedHourlyServerCoveringIndexName).ToList();
        Assert.True(covering.Count == 1, $"{when}: the covering server index must exist once, indexes are {string.Join(", ", indexes.Select(i => i.Name))}");
        Assert.True(covering[0].Total > covering[0].Keys, $"{when}: the server index must carry included columns");
        Assert.Equal("server_id", covering[0].FirstKey);
        Assert.True(
            indexes.All(i => !(i.FirstKey == "server_id" && i.Total == 2)),
            $"{when}: the plain (server_id, bucket) index must be gone, indexes are {string.Join(", ", indexes.Select(i => i.Name))}");
        Assert.Contains(indexes, i => i.FirstKey == "server_name" && i.Total == 2);
    }

    /// <summary>Every index on the materialization and its chunks with the file it lives in: a rebuilt, dropped or created index changes this.</summary>
    private static async Task<List<string>> IndexFilesAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(
            @"SELECT ic.relname || ':' || ic.oid || ':' || ic.relfilenode
              FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class t ON t.oid = i.indrelid
              WHERE t.relname LIKE '_materialized_hypertable_%' OR t.relname LIKE '_hyper_%'
              ORDER BY 1", c);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    /// <summary>The state of a store before V291: the covering server index gone and V152's plain (server_id, bucket DESC) index in its place.</summary>
    private static async Task DowngradeToPlainAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var materialization = await MaterializationAsync(c, ct);
        await Exec(c, $"DROP INDEX IF EXISTS {materialization.Split('.')[0]}.{TimescaleSupport.QueryStoreCorrectedHourlyServerCoveringIndexName}", ct);
        await Exec(c, $"CREATE INDEX ON {materialization} (server_id, bucket DESC)", ct);
        var indexes = await HypertableIndexesAsync(c, materialization, ct);
        Assert.DoesNotContain(indexes, i => i.Name == TimescaleSupport.QueryStoreCorrectedHourlyServerCoveringIndexName);
        Assert.Contains(indexes, i => i.FirstKey == "server_id" && i.Total == 2);
    }

    private static async Task RunInTransactionAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var tx = await c.BeginTransactionAsync(ct);
        await Exec(c, sql, ct);
        await tx.CommitAsync(ct);
    }

    /// <summary>A fresh store ends with the covering server index and without the plain one, and a second convergence pass changes no index.</summary>
    [Fact]
    public async Task AFreshStore_HasTheCoveringServerIndexAndNoPlainOne_AndASecondPassChangesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            await AssertEndStateAsync(c, ct, "fresh store");
            var before = await IndexFilesAsync(c, ct);

            var convergence = TimescaleSupport.QueryStoreRollupKeptIndexesSql[TimescaleSupport.QueryStoreStatsCorrectedHourlyView];
            await RunInTransactionAsync(c, convergence, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(c, null, ct);

            await AssertEndStateAsync(c, ct, "second pass");
            Assert.Equal(before, await IndexFilesAsync(c, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>V291 on a store with the plain index: the covering index is built, the plain one dropped, and a second run changes no index. The
    /// convergence pass reaches the same state from a store that has both (an older build put the plain one back).</summary>
    [Fact]
    public async Task TheMigration_BuildsTheCoveringIndex_DropsThePlainOne_AndRunsTwice()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            var v291 = PgMigrations.Scripts.Single(m => m.Version == 291);
            Assert.Equal(291, v291.Version);

            await DowngradeToPlainAsync(c, ct);
            await RunInTransactionAsync(c, v291.Sql, ct);
            await AssertEndStateAsync(c, ct, "after V291");
            var afterFirst = await IndexFilesAsync(c, ct);

            await RunInTransactionAsync(c, v291.Sql, ct);
            await AssertEndStateAsync(c, ct, "after V291 twice");
            Assert.Equal(afterFirst, await IndexFilesAsync(c, ct));

            /* An older build's convergence pass built the plain index again beside the covering one: the current pass drops it and leaves the covering index alone. */
            var materialization = await MaterializationAsync(c, ct);
            await Exec(c, $"CREATE INDEX ON {materialization} (server_id, bucket DESC)", ct);
            var coveringBefore = (await IndexFilesAsync(c, ct)).Where(i => i.Contains(TimescaleSupport.QueryStoreCorrectedHourlyServerCoveringIndexName)).ToList();
            var convergence = TimescaleSupport.QueryStoreRollupKeptIndexesSql[TimescaleSupport.QueryStoreStatsCorrectedHourlyView];
            await RunInTransactionAsync(c, convergence, ct);
            await AssertEndStateAsync(c, ct, "convergence over both indexes");
            Assert.Equal(coveringBefore, (await IndexFilesAsync(c, ct)).Where(i => i.Contains(TimescaleSupport.QueryStoreCorrectedHourlyServerCoveringIndexName)).ToList());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static string Utc(DateTime t) => t.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

    private static async Task SeedAsync(NpgsqlConnection c, DateTime from, DateTime to, CancellationToken ct)
    {
        for (var i = 1; i <= ServerCount; i++)
        {
            await DarlingMcpTestData.RegisterServerAsync(c, (int)FirstServerId - i, "c9215-" + i, ct);
        }

        await Exec(c, $@"INSERT INTO collect.query_store_stats
              (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
               runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
            SELECT 900000000::bigint + row_number() OVER (), h.ts + interval '10 minutes', {FirstServerId} - s, 'c9215-' || s, 'db' || (g % {Databases}), 'dbo.P',
              '0x' || g, g, g, 'Regular', 'PRIMARY', g, h.ts, h.ts, 1 + (extract(hour FROM h.ts))::int % 5, 2000 + g, 1000 + g, 900, 400, h.ts + interval '1 hour'
            FROM generate_series(TIMESTAMP '{Utc(from)}', TIMESTAMP '{Utc(to)}', interval '1 hour') AS h(ts)
            CROSS JOIN generate_series(1, {ServerCount}) s CROSS JOIN generate_series(1, {Groups}) g", ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection c, DateTime from, DateTime to, CancellationToken ct)
    {
        foreach (var view in new[] { TimescaleSupport.QueryStoreStatsIntervalHourlyView, TimescaleSupport.QueryStoreStatsCorrectedHourlyView })
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", c);
            refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
            refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
            await refresh.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>Every index name an Index Scan, Index Only Scan or Bitmap Index Scan node of the plan reads, with its node type.</summary>
    private static void CollectIndexReads(JsonElement plan, List<(string NodeType, string IndexName)> reads)
    {
        if (plan.TryGetProperty("Index Name", out var name) && plan.TryGetProperty("Node Type", out var type))
        {
            reads.Add((type.GetString()!, name.GetString()!));
        }

        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                CollectIndexReads(child, reads);
            }
        }
    }

    /// <summary>The 7-day duration trend's rollup arm, on a seeded store with the covering index, reads one server's rows from that index and answers from it
    /// alone (an Index Only Scan). On the plain index it went to the heap once per row, or read the whole hash index and filtered by server.</summary>
    [Fact]
    public async Task TheSevenDayTrendRollupArm_ReadsFromTheCoveringServerIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            var now = LiveClock.Now();
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
            var start = end.AddDays(-7);
            await SeedAsync(c, start, end.AddHours(-1), ct);
            await RefreshAsync(c, start.Date.AddDays(-1), end, ct);
            var materialization = await MaterializationAsync(c, ct);
            await Exec(c, $"ALTER TABLE {materialization} SET (autovacuum_enabled = false)", ct);
            await Exec(c, $"VACUUM (ANALYZE) {materialization}", ct);

            await using var command = new NpgsqlCommand(
                "EXPLAIN (FORMAT JSON) " + QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true), c);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, (int)FirstServerId - 1);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, start);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end.AddHours(-1));
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
            var json = (string)(await command.ExecuteScalarAsync(ct))!;

            using var document = JsonDocument.Parse(json);
            var reads = new List<(string NodeType, string IndexName)>();
            CollectIndexReads(document.RootElement[0].GetProperty("Plan"), reads);
            TestContext.Current.SendDiagnosticMessage("index reads: " + string.Join("; ", reads.Select(r => r.NodeType + " " + r.IndexName).Distinct()));
            Assert.Contains(reads, r => r.NodeType == "Index Only Scan" && r.IndexName.EndsWith(TimescaleSupport.QueryStoreCorrectedHourlyServerCoveringIndexName, StringComparison.Ordinal));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
