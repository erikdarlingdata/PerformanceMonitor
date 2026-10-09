/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5520: the dimension rows of <c>collect.store_metrics</c> take <c>row_count</c> from <c>pg_class.reltuples</c>,
/// not from a <c>count(*)</c> over the dimension table (149,562 blocks and 25.9 s a call on a large store). The
/// estimate is NULL where PostgreSQL reports <c>-1</c> (never vacuumed or analysed), like the table kind's, and the
/// scan is gone from the statement.
/// </summary>
public sealed class StoreDimensionRowCountLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the dimension row_count pins (each mints its own scratch database).";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheDimensionSweep_TakesRowCountFromReltuples_AndNeverScansTheDimension()
    {
        var sql = StoreSelfMetrics.DimensionInsertSql;

        Assert.DoesNotContain("count(*)", sql, StringComparison.OrdinalIgnoreCase);
        foreach (var dim in new[] { PayloadDimensions.QueryTextDimTable, PayloadDimensions.QueryPlanDimTable })
        {
            /* -1 (never vacuumed or analysed, PostgreSQL 14+) is NULL, never a count of minus one. */
            Assert.Contains(
                $"(SELECT CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint END FROM pg_class AS c WHERE c.oid = 'collect.{dim}'::regclass),",
                sql, StringComparison.Ordinal);
            Assert.DoesNotContain($"FROM collect.{dim})", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task TheDimensionRowCount_IsTheAnalyzedEstimate_AndNullWhereTheTableWasNeverAnalyzed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, $"TRUNCATE collect.{PayloadDimensions.QueryTextDimTable}, collect.{PayloadDimensions.QueryPlanDimTable}", ct);
            for (var i = 0; i < 5; i++)
            {
                await ExecAsync(connection,
                    $"INSERT INTO collect.{PayloadDimensions.QueryTextDimTable} ({PayloadDimensions.DigestColumn}, {PayloadDimensions.PayloadColumnOf(PayloadDimensions.QueryTextDimTable)}, {PayloadDimensions.LastSeenColumn}) " +
                    $"VALUES (decode('{i:x2}', 'hex'), 'text {i}', now() AT TIME ZONE 'UTC')", ct);
            }

            for (var i = 0; i < 3; i++)
            {
                await ExecAsync(connection,
                    $"INSERT INTO collect.{PayloadDimensions.QueryPlanDimTable} ({PayloadDimensions.DigestColumn}, {PayloadDimensions.PayloadColumnOf(PayloadDimensions.QueryPlanDimTable)}, {PayloadDimensions.LastSeenColumn}) " +
                    $"VALUES (decode('{i:x2}', 'hex'), '<plan {i}/>', now() AT TIME ZONE 'UTC')", ct);
            }

            /* Never analysed: reltuples is -1 on PostgreSQL 14+, so the figure is NULL. An autovacuum that got there
               first would have set it, so the expectation follows the catalog rather than assuming it. */
            var beforeAnalyze = await SweepAsync(connection, DateTime.UtcNow, ct);
            foreach (var dim in new[] { PayloadDimensions.QueryTextDimTable, PayloadDimensions.QueryPlanDimTable })
            {
                var reltuples = await ReltuplesAsync(connection, dim, ct);
                Assert.Equal(reltuples >= 0 ? (long?)reltuples : null, beforeAnalyze[dim]);
            }

            /* Analysed: the estimate of a table this small is its row count. */
            await ExecAsync(connection, $"ANALYZE collect.{PayloadDimensions.QueryTextDimTable}", ct);
            await ExecAsync(connection, $"ANALYZE collect.{PayloadDimensions.QueryPlanDimTable}", ct);
            var afterAnalyze = await SweepAsync(connection, DateTime.UtcNow.AddMinutes(1), ct);
            Assert.Equal(5L, afterAnalyze[PayloadDimensions.QueryTextDimTable]);
            Assert.Equal(3L, afterAnalyze[PayloadDimensions.QueryPlanDimTable]);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task<Dictionary<string, long?>> SweepAsync(NpgsqlConnection connection, DateTime metricTimeUtc, CancellationToken ct)
    {
        var metricTime = DateTime.SpecifyKind(new DateTime(metricTimeUtc.Ticks - (metricTimeUtc.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
        await using (var sweep = new NpgsqlCommand(StoreSelfMetrics.DimensionInsertSql, connection))
        {
            sweep.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = metricTime });
            Assert.Equal(2, await sweep.ExecuteNonQueryAsync(ct));
        }

        var rows = new Dictionary<string, long?>();
        await using var read = new NpgsqlCommand(
            "SELECT object_name, row_count FROM collect.store_metrics WHERE object_kind = 'dimension' AND metric_time = $1", connection);
        read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = metricTime });
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetInt64(1);
        }

        return rows;
    }

    private static async Task<double> ReltuplesAsync(NpgsqlConnection connection, string dim, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand($"SELECT reltuples::float8 FROM pg_class WHERE oid = 'collect.{dim}'::regclass", connection);
        return (double)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
