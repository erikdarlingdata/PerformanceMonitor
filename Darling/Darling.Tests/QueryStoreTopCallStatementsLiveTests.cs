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
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5815: what one default <c>get_query_store_top</c> call (24 hours, top 20, no database filter) sends to the store
/// when the interval table answers. A field sample counted two floor reads per call; the call path issues one. This
/// runs the tool once inside a capturing <see cref="ReadScope"/>, the same per-statement capture the slow-read log
/// uses, and counts the statements it executed by the hash of their text: one read-gate floor read
/// (<see cref="QueryStoreIntervalWide.PlainTableFloorSql"/>), no legacy-row probe on the raw table
/// (<see cref="QueryStoreIntervalWide.HasLegacyRowSql"/>, which the top read does not ask for), one wide-table read
/// (<see cref="DarlingDataReader.QueryStoreTopTableSql"/>), and no raw read. A change that runs the gate twice,
/// re-runs the read per fill round, or falls through to raw after the table answered fails here.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then
   works entirely inside it, the same shape as QueryStoreTopServerWideReadLiveTests. */
public sealed class QueryStoreTopCallStatementsLiveTests
{
    private const int ServerId = 1;
    private const string ServerName = "srv5815";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OneDefaultCall_ReadsTheFloorOnce_NeverProbesLegacyRows_AndReadsTheWideTableOnce()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5815 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, ct);
        }

        ReadStatementCapture.Register();
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        string json;
        System.Collections.Generic.IReadOnlyList<StatementTiming> seen;
        using (var opened = ReadScope.Open(null))
        {
            opened.Scope.CaptureStatements = true;
            json = await DarlingMcpDataTools.GetQueryStoreTop(
                source, ServerName, hours_back: 24, top: 20, database_name: (string?)null, as_of: null,
                execution_type: null, module_name: null, full_text: false, previewLength: 200, ct);
            seen = opened.Scope.Snapshot();
            Assert.Equal(0, opened.Scope.StatementsDropped);
        }

        /* The table answered with a full page, so every count below is the table path's. */
        using (var doc = JsonDocument.Parse(json))
        {
            Assert.Equal("interval_table", doc.RootElement.GetProperty("history_source").GetString());
            Assert.Equal(20, doc.RootElement.GetProperty("queries").GetArrayLength());
        }

        int Count(string sql) => seen.Count(t => t.Hash == StatementLabel.Hash(sql));
        var listing = string.Join(" | ", seen.Select(t => t.Label));
        Assert.True(Count(QueryStoreIntervalWide.PlainTableFloorSql) == 1, $"floor reads: {Count(QueryStoreIntervalWide.PlainTableFloorSql)}; statements: {listing}");
        Assert.True(Count(QueryStoreIntervalWide.HasLegacyRowSql) == 0, $"legacy probes: {Count(QueryStoreIntervalWide.HasLegacyRowSql)}; statements: {listing}");
        Assert.True(Count(QueryStoreIntervalWide.ReadSourceInputsSql) == 1, $"gate input reads: {Count(QueryStoreIntervalWide.ReadSourceInputsSql)}; statements: {listing}");
        Assert.True(Count(DarlingDataReader.QueryStoreTopTableSql) == 1, $"wide-table reads: {Count(DarlingDataReader.QueryStoreTopTableSql)}; statements: {listing}");
        Assert.True(Count(DarlingDataReader.QueryStoreTopSql) == 0, $"raw reads: {Count(DarlingDataReader.QueryStoreTopSql)}; statements: {listing}");
        Assert.True(Count(DarlingDataReader.QueryStoreTopDailyTableSql) == 0, $"daily reads: {Count(DarlingDataReader.QueryStoreTopDailyTableSql)}; statements: {listing}");
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* One server, three databases, 40 plans, one row per closed hourly interval for the last 30 hours (collected two
           minutes after the hour closes), plus one interval four days old so the table's floor sits below the 24-hour
           window's purge edge and the gate may answer from the table. Coverage reaches back 50 hours and nothing is
           pending. */
        var now = DateTime.UtcNow;
        var hour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
        var first = hour.AddHours(-31);
        var last = hour.AddHours(-2);
        var old = hour.AddDays(-4);
        string At(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        await using var seed = new NpgsqlCommand($"""
            INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({ServerId}, '{ServerName}', TRUE);
            INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through)
            VALUES ({ServerId}, timestamp '{At(hour.AddHours(-50))}', now() AT TIME ZONE 'UTC');
            INSERT INTO collect.query_store_interval_wide
                (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
                 last_execution_time, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                 avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, runtime_stats_interval_id,
                 interval_start_time_utc)
            SELECT
                t.h + interval '62 minutes', {ServerId}, 'db' || d.d, p.p, p.p, 'Regular', t.h + (p.p % 50) * interval '1 minute',
                t.h + interval '55 minutes', 'qh' || p.p, 'ph' || p.p, 10 + p.p % 7, 1000 * p.p + d.d, 100 * p.p, p.p, 0, 0, 1,
                CAST(extract(epoch FROM t.h) AS bigint) / 3600, t.h
            FROM (
                SELECT g AS h FROM generate_series(timestamp '{At(first)}', timestamp '{At(last)}', interval '1 hour') AS g
                UNION ALL
                SELECT timestamp '{At(old)}'
            ) AS t
            CROSS JOIN generate_series(1, 3) AS d(d)
            CROSS JOIN generate_series(1, 40) AS p(p);
            INSERT INTO collect.query_store_text (server_id, database_name, query_id, last_seen, query_sql_text)
            SELECT {ServerId}, 'db' || d.d, p.p, timestamp '{At(last)}', 'SELECT ' || p.p
            FROM generate_series(1, 3) AS d(d)
            CROSS JOIN generate_series(1, 40) AS p(p);
            """, connection);
        await seed.ExecuteNonQueryAsync(ct);
    }
}
