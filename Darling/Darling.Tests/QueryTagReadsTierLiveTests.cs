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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5984 round 1: the query tag filter on the non-raw statements. <see cref="QueryTagReadsLiveTests"/> seeds ten minutes back, so
/// everything lands on the raw tier; here each read is driven onto its other tier through the product's own routed read, and a tag
/// call must apply the filter there too, before the top-N cut: the tag is put on the LOWEST-ranked rows, so a filter applied after
/// the cut (or not at all) would come back empty or with the wrong rows. One test per tier: the hourly rollup of query stats, the
/// hourly rollup of procedure stats, Query Store's interval table, and Query Store's daily summary.
///
/// <para><b>#1776 own-store</b> - each test mints a scratch database (the hourly pair materializes continuous aggregates the shared
/// fixture must never inherit), so the class is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryTagReadsTierLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed for the hourly pair) to run the live query-tag tier tests.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.AddWithValue(value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task TagAsync(NpgsqlConnection connection, int serverId, string kind, string tagName, string[] keys, CancellationToken ct)
    {
        await ExecAsync(connection, "INSERT INTO config.query_tags (name) VALUES ($1)", ct, tagName);
        foreach (var key in keys)
        {
            await ExecAsync(connection,
                "INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) SELECT $1, $2, $3, id, 'rule' FROM config.query_tags WHERE name = $4",
                ct, serverId, kind, key, tagName);
        }
    }

    private static string[] Values(string json, string member, string field) =>
        JsonDocument.Parse(json).RootElement.GetProperty(member).EnumerateArray().Select(r => r.GetProperty(field).GetString()!).ToArray();

    /* ───────────────────────── hourly rollups (queries, procedures) ───────────────────────── */

    private static async Task WithHourlyStoreAsync(int serverId, string serverName, Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "The hourly tier needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await body(connection, source, ct);
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

    private static async Task RefreshAndPurgeAsync(NpgsqlConnection connection, string view, string rawTable, int serverId, DateTime windowEnd, CancellationToken ct)
    {
        await ExecAsync(connection, $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", ct, WindowStart, windowEnd.AddHours(1));
        await ExecAsync(connection, $"DELETE FROM collect.{rawTable} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", ct, serverId, WindowStart, windowEnd);
    }

    [Fact]
    public async Task HourlyQueryStats_TheTagFilterAppliesBeforeTheCut()
    {
        const int serverId = -984101;
        const string serverName = "a5984-tier-hourly-queries";
        await WithHourlyStoreAsync(serverId, serverName, async (connection, source, ct) =>
        {
            foreach (var (hash, cpu) in new[] { ("0xTIERQ1", 900_000L), ("0xTIERQ2", 500_000L), ("0xTIERQ3", 100_000L) })
            {
                await ExecAsync(connection, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     host_object_name, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds, min_dop, max_dop)
VALUES ($1, $2, $3, $4, 'TierDb', $5, $5, 'usp_Host', $6, $6, 10, 3600, 1, 1)",
                    ct, CollectionIdGenerator.Next(), WindowStart.AddHours(1), serverId, serverName, hash, cpu);
            }

            var windowEnd = WindowStart.AddDays(1);
            await RefreshAndPurgeAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, "query_stats", serverId, windowEnd, ct);
            await TagAsync(connection, serverId, "hash", "TierTagLow", new[] { "0xTIERQ3" }, ct);
            var hours = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
            var asOf = windowEnd.ToString("o");

            var plain = await DarlingMcpDataTools.GetTopQueriesByCpu(source, serverName, hours, 1, as_of: asOf, cancellationToken: ct);
            Assert.Equal("hourly", JsonDocument.Parse(plain).RootElement.GetProperty("tier_used").GetString());
            Assert.Equal(["0xTIERQ1"], Values(plain, "queries", "query_hash"));

            var tagged = await DarlingMcpDataTools.GetTopQueriesByCpu(source, serverName, hours, 1, as_of: asOf, tag: "TierTagLow", cancellationToken: ct);
            Assert.Equal("hourly", JsonDocument.Parse(tagged).RootElement.GetProperty("tier_used").GetString());
            Assert.Equal(["0xTIERQ3"], Values(tagged, "queries", "query_hash"));
            Assert.Equal("TierTagLow", JsonDocument.Parse(tagged).RootElement.GetProperty("queries")[0].GetProperty("tag").GetString());

            var untagged = await DarlingMcpDataTools.GetTopQueriesByCpu(source, serverName, hours, 10, as_of: asOf, tag: "untagged", cancellationToken: ct);
            Assert.Equal(["0xTIERQ1", "0xTIERQ2"], Values(untagged, "queries", "query_hash"));
        });
    }

    [Fact]
    public async Task HourlyProcedureStats_TheTagFilterAppliesBeforeTheCut()
    {
        const int serverId = -984102;
        const string serverName = "a5984-tier-hourly-procedures";
        await WithHourlyStoreAsync(serverId, serverName, async (connection, source, ct) =>
        {
            foreach (var (name, cpu) in new[] { ("usp_TierHigh", 900_000L), ("usp_TierMid", 500_000L), ("usp_TierLow", 100_000L) })
            {
                await ExecAsync(connection, @"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'TierDb', 'dbo', $5, $6, $7, $7, 10, 3600)",
                    ct, CollectionIdGenerator.Next(), WindowStart.AddHours(1), serverId, serverName, name, "0x" + name, cpu);
            }

            var windowEnd = WindowStart.AddDays(1);
            await RefreshAndPurgeAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, "procedure_stats", serverId, windowEnd, ct);
            await TagAsync(connection, serverId, "object", "TierTagLow", new[] { "TierDb.dbo.usp_TierLow" }, ct);
            var hours = (int)Math.Ceiling((windowEnd - WindowStart).TotalHours);
            var asOf = windowEnd.ToString("o");

            var plain = await DarlingMcpDataTools.GetTopProceduresByCpu(source, serverName, hours, 1, as_of: asOf, cancellationToken: ct);
            Assert.Equal("hourly", JsonDocument.Parse(plain).RootElement.GetProperty("tier_used").GetString());
            Assert.Equal(["dbo.usp_TierHigh"], Values(plain, "procedures", "full_name"));

            var tagged = await DarlingMcpDataTools.GetTopProceduresByCpu(source, serverName, hours, 1, as_of: asOf, tag: "TierTagLow", cancellationToken: ct);
            Assert.Equal("hourly", JsonDocument.Parse(tagged).RootElement.GetProperty("tier_used").GetString());
            Assert.Equal(["dbo.usp_TierLow"], Values(tagged, "procedures", "full_name"));

            var untagged = await DarlingMcpDataTools.GetTopProceduresByCpu(source, serverName, hours, 10, as_of: asOf, tag: "untagged", cancellationToken: ct);
            Assert.Equal(["dbo.usp_TierHigh", "dbo.usp_TierMid"], Values(untagged, "procedures", "full_name"));
        });
    }

    /* ───────────────────────── Query Store: interval table, daily summary ───────────────────────── */

    private static readonly DateTime BuildNow = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);
    private const string AsOf = "2026-01-14T06:00:00Z";

    /// <summary>Eight queries (hash h1..h8), hourly rows from 2026-01-05 to 2026-01-14 12:00, each query a different cost.</summary>
    private const string SeedSql = @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
 avg_physical_io_reads, avg_rowcount, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT
    ct, 1, 'db0', q, q, 'Regular', ct - interval '10 minutes', ct, NULL, 'h' || q, 10,
    1000 + q * 100000, 10 + q, 100 + q, 1, 1, 10, 'ph' || q, NULL, q * 100000 + h, ct - interval '10 minutes'
FROM
(
    SELECT q, h, TIMESTAMP '2026-01-05 00:00:00' + make_interval(hours => h) AS ct
    FROM generate_series(1, 8) AS q
    CROSS JOIN generate_series(0, 24 * 9 + 12) AS h
) AS s;";

    private static async Task WithQueryStoreAsync(bool daily, Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, SeedSql, ct);
            await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (1, 'srv1', TRUE)", ct);
            await ExecAsync(connection,
                "INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES (1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')", ct);
            if (daily)
            {
                var tick = await QueryStoreTopDaily.RunTickAsync(source, BuildNow, 9, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
                Assert.Equal(0, tick.Failed);
                Assert.True(tick.Built > 0);
            }

            await body(connection, source, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>Reads the unfiltered ranking, tags its three LOWEST rows, and checks a top-3 tag call returns exactly those.</summary>
    private static async Task AssertTagAppliesBeforeTheCutAsync(NpgsqlConnection connection, NpgsqlDataSource source, int hours, CancellationToken ct, Action<JsonElement> checkTier)
    {
        var unfiltered = await DarlingMcpDataTools.GetQueryStoreTop(source, "srv1", hours, 8, null, AsOf, null, null, false, 400, ct);
        checkTier(JsonDocument.Parse(unfiltered).RootElement.Clone());
        var ranked = Values(unfiltered, "queries", "query_hash");
        Assert.Equal(8, ranked.Distinct().Count());
        var lowest = ranked.TakeLast(3).ToArray();
        await TagAsync(connection, 1, "hash", "TierTagLow", lowest, ct);

        var plainTop = Values(await DarlingMcpDataTools.GetQueryStoreTop(source, "srv1", hours, 3, null, AsOf, null, null, false, 400, ct), "queries", "query_hash");
        Assert.Empty(plainTop.Intersect(lowest));

        var tagged = await DarlingMcpDataTools.GetQueryStoreTop(source, "srv1", hours, 3, null, AsOf, null, null, false, 400, ct, tag: "TierTagLow");
        checkTier(JsonDocument.Parse(tagged).RootElement.Clone());
        Assert.Equal(lowest.OrderBy(h => h, StringComparer.Ordinal), Values(tagged, "queries", "query_hash").OrderBy(h => h, StringComparer.Ordinal));
        Assert.All(JsonDocument.Parse(tagged).RootElement.GetProperty("queries").EnumerateArray(), r => Assert.Equal("TierTagLow", r.GetProperty("tag").GetString()));

        var untagged = Values(await DarlingMcpDataTools.GetQueryStoreTop(source, "srv1", hours, 8, null, AsOf, null, null, false, 400, ct, tag: "untagged"), "queries", "query_hash");
        Assert.Equal(5, untagged.Length);
        Assert.Empty(untagged.Intersect(lowest));
    }

    [Fact]
    public Task QueryStoreIntervalTable_TheTagFilterAppliesBeforeTheCut() => WithQueryStoreAsync(daily: false, (connection, source, ct) =>
        AssertTagAppliesBeforeTheCutAsync(connection, source, 168, ct, answer =>
        {
            Assert.Equal("interval_table", answer.GetProperty("history_source").GetString());
            Assert.False(answer.GetProperty("approximate").GetBoolean());
        }));

    [Fact]
    public Task QueryStoreDailySummary_TheTagFilterAppliesBeforeTheCut() => WithQueryStoreAsync(daily: true, (connection, source, ct) =>
        AssertTagAppliesBeforeTheCutAsync(connection, source, 168, ct, answer =>
        {
            Assert.Equal("interval_table", answer.GetProperty("history_source").GetString());
            Assert.True(answer.GetProperty("approximate").GetBoolean());
        }));
}
