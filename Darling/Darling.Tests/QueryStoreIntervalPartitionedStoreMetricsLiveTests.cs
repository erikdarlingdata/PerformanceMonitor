/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store's own size inventory over the day-partitioned Query Store interval tables (#5571). A partitioned parent has
/// no storage of its own, so its <c>collect.store_metrics</c> row must SUM its partitions, and the partitions must not
/// show up again as separate tables in the un-enumerated census (the <c>other</c> row and the MCP top-N), or the
/// reconciliation would count their bytes twice.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreIntervalPartitionedStoreMetricsLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> LongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static string Lit(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    /* Rows into the legacy table (first_execution_time before S) and into a day partition (an hour after S). */
    private static Task SeedWideAsync(NpgsqlConnection connection, DateTime first, int rows, CancellationToken ct) => ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT '{Lit(first)}'::timestamp, 1, 'db', g, g, 'Regular', '{Lit(first)}'::timestamp, '{Lit(first)}'::timestamp,
       'select 1', 1, 1000, 1, '{Lit(first)}'::timestamp
FROM generate_series(1, {rows}) AS g", ct);

    private static Task SeedLatestAsync(NpgsqlConnection connection, DateTime first, int rows, CancellationToken ct) => ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_latest
    (collection_time, server_id, database_name, query_id, plan_id, first_execution_time, last_execution_time,
     query_text, execution_count, avg_duration_us, runtime_stats_interval_id)
SELECT '{Lit(first)}'::timestamp, 1, 'db', g, g, '{Lit(first)}'::timestamp, '{Lit(first)}'::timestamp,
       'select 1', 1, 1000, 1
FROM generate_series(1, {rows}) AS g", ct);

    [Fact]
    public async Task TheParentRows_SumTheirPartitions_AndThePartitionsLeaveTheCensus()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = DateTime.UtcNow;
        var today = DateTime.SpecifyKind(now.Date, DateTimeKind.Unspecified);
        var s = QueryStoreIntervalPartitions.ArmBound(now);

        /* The legacy table holds 600 rows over three past days. */
        for (var back = 1; back <= 3; back++)
        {
            await SeedWideAsync(connection, today.AddDays(-back).AddHours(12), 200, ct);
            await SeedLatestAsync(connection, today.AddDays(-back).AddHours(12), 200, ct);
        }

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            var promote = await QueryStoreIntervalPartitions.RunPromotionAsync(connection, table, now, NullLogger.Instance, ct);
            Assert.True(promote.Outcome is QueryStoreIntervalPartitions.StepOutcome.Done or QueryStoreIntervalPartitions.StepOutcome.NothingToDo, $"{table.Name}: {promote.Outcome} {promote.Detail}");
        }

        /* And 300 rows in the first day partition (S + 1 h), so the data lives in more than one partition. */
        await SeedWideAsync(connection, s.AddHours(1), 300, ct);
        await SeedLatestAsync(connection, s.AddHours(1), 300, ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_wide; ANALYZE collect.query_store_interval_latest;", ct);

        var written = await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: false, now, null, null, null, ct);
        Assert.True(written > 0);

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            var leaves = $"SELECT t.relid FROM pg_partition_tree('{table.Parent}'::regclass) AS t WHERE t.isleaf";
            Assert.True(await LongAsync(connection, $"SELECT count(*) FROM ({leaves}) AS x", ct) >= 4, "legacy, the promoted days and DEFAULT are leaves");

            var expectedBytes = await LongAsync(connection, $"SELECT sum(pg_total_relation_size(relid)) FROM ({leaves}) AS x(relid)", ct);
            var expectedRows = await LongAsync(connection, $"SELECT sum(reltuples)::bigint FROM pg_class WHERE oid IN ({leaves}) AND reltuples >= 0", ct);
            Assert.Equal(0, await LongAsync(connection, $"SELECT pg_total_relation_size('{table.Parent}'::regclass)", ct)); /* why the sum is needed */
            Assert.Equal(900, expectedRows);

            await using var command = new NpgsqlCommand(
                "SELECT total_bytes, row_count FROM collect.store_metrics WHERE object_name = $1 AND object_kind = $2 ORDER BY metric_time DESC LIMIT 1", connection);
            command.Parameters.AddWithValue(table.Parent);
            command.Parameters.AddWithValue(StoreSelfMetrics.TableObjectKind);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), $"no {table.Parent} row");
            Assert.True(expectedBytes > 0);
            Assert.Equal(expectedBytes, reader.GetInt64(0));
            Assert.Equal(expectedRows, reader.GetInt64(1));
        }

        /* The census: no partition of either parent, and a partition of an unrelated partitioned table stays. */
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_census_probe (a integer NOT NULL) PARTITION BY RANGE (a);
CREATE TABLE collect.zz_census_probe_p1 PARTITION OF collect.zz_census_probe FOR VALUES FROM (0) TO (10);", ct);

        var census = new List<string>();
        await using (var census_ = new NpgsqlCommand($@"
SELECT n.nspname || '.' || c.relname
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE {StoreSelfMetrics.CensusRelationPredicateSql}
AND   n.nspname = 'collect'", connection))
        {
            await using var reader = await census_.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                census.Add(reader.GetString(0));
            }
        }

        foreach (var table in QueryStoreIntervalPartitions.All)
        {
            Assert.Contains(table.Parent, census);
            Assert.DoesNotContain(table.Legacy, census);
            Assert.DoesNotContain(table.Default, census);
            Assert.DoesNotContain(census, name => name.StartsWith(table.Parent + "_p2", StringComparison.Ordinal));
        }

        Assert.Contains("collect.zz_census_probe", census);
        Assert.Contains("collect.zz_census_probe_p1", census);
        /* The two non-partition neighbours that share the name prefix are still counted. */
        Assert.Contains("collect.query_store_interval_wide_pending", census);
        Assert.Contains("collect.query_store_interval_latest_coverage", census);

        /* The catch-all rows never counted the partitions: the "other" row is the census, so it equals the census
           relations' sizes summed. */
        var otherBytes = await LongAsync(connection, $"SELECT total_bytes FROM collect.store_metrics WHERE object_kind = '{StoreSelfMetrics.OtherObjectKind}' ORDER BY metric_time DESC LIMIT 1", ct);
        var legacyBytes = await LongAsync(connection, "SELECT pg_total_relation_size('collect.query_store_interval_wide_legacy'::regclass)", ct);
        Assert.True(legacyBytes > 0);
        var otherCensusBytes = await LongAsync(connection, $@"
SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE {StoreSelfMetrics.CensusRelationPredicateSql}
AND   NOT {StoreSelfMetrics.NamedRelationPredicateSql}
AND   NOT {StoreSelfMetrics.SystemSchemaPredicateSql}", ct);
        Assert.InRange(otherBytes, otherCensusBytes - 1_000_000, otherCensusBytes + 1_000_000);
    }
}
