/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582 part 3, lane 4: the seed and the helpers the three <c>ComposeQueryStoreStamp*LiveTests</c> classes share. The seed is lane 1's
/// (<see cref="QueryStoreComposeStampLiveTests"/>): 30 hours of Query Store wide rows ending at the current hour, three servers
/// (ids 1 to 3, registered as srv1 to srv3), 12 stamps an hour, six query hashes with three plans each, with NULL modules, hashes,
/// durations and counts, Aborted and Exception rows, and products that pass a bigint. The builder then runs hour by hour, so the
/// store has 28 built hours: hourNow - 30 h up to hourNow - 3 h.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. This class reaches DARLING_TEST_PG only to CREATE and DROP each
   test's own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
internal static class ComposeStampLiveSupport
{
    internal const int RetentionDays = 9;

    internal const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #5582 compose rollup's live facts.";

    internal static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    internal static string At(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    internal static DateTime ThisHour() => new(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, 0, 0, DateTimeKind.Unspecified);

    internal static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    internal static async Task<string> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) ?? string.Empty;
    }

    internal static async Task EnrollAsync(NpgsqlConnection connection, int serverId, CancellationToken ct) =>
        await ExecAsync(connection,
            $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({serverId}, 'srv{serverId}', TRUE) ON CONFLICT (server_id) DO NOTHING;"
            + $"INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES ({serverId}, date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '30 hours', now() AT TIME ZONE 'UTC')", ct);

    internal static Task SeedAsync(NpgsqlConnection connection, DateTime hourNow, CancellationToken ct) => ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, replica_role,
 runtime_stats_interval_id, interval_start_time_utc)
SELECT t.ct, s.server_id, 'db' || (q % 2), q, q * 10 + p,
       CASE WHEN (h + m + q + p) % 11 = 0 THEN 'Aborted' WHEN (h + m + q + p) % 13 = 0 THEN 'Exception' ELSE 'Regular' END,
       t.ct - interval '3 minutes', t.ct,
       CASE WHEN q IN (3, 6) THEN NULL ELSE 'mod' || (q % 3) END,
       CASE WHEN q = 5 THEN NULL ELSE 'hash' || q END,
       CASE WHEN (h + m + p) % 17 = 0 AND q <> 1 THEN NULL WHEN q = 1 THEN 2000000 ELSE ((h + m + q + p) * 100 + 1)::bigint END,
       CASE WHEN (h + m + q) % 7 = 0 AND q <> 1 THEN NULL WHEN q = 1 THEN 3000000000000::bigint ELSE ((h * 1000 + m * 37 + q * 13 + p) * 1000)::bigint END,
       CASE WHEN (h + q) % 5 = 0 AND q <> 1 THEN NULL ELSE ((h + m + q) * 523 + p)::bigint END,
       ((h + 1) * 997 + m + q)::bigint, CASE WHEN (m + q) % 4 = 0 THEN NULL ELSE ((h + 2) * 311 + m * q)::bigint END,
       NULL,
       ((((h * 12 + m) * 3 + s.k) * 6 + q) * 3 + p), t.ct - interval '10 minutes'
FROM generate_series(0, 29) AS h
CROSS JOIN generate_series(0, 11) AS m
CROSS JOIN (VALUES (1, 0), (2, 1), (3, 2)) AS s (server_id, k)
CROSS JOIN generate_series(1, 6) AS q
CROSS JOIN generate_series(1, 3) AS p
CROSS JOIN LATERAL (SELECT TIMESTAMP '{At(hourNow)}' - interval '30 hours' + h * interval '1 hour' + m * interval '5 minutes' AS ct) AS t", ct);

    /// <summary>Builds every hour that is due, tick by tick, and returns how many hours were built.</summary>
    internal static async Task<int> BuildAllAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        var total = 0;
        for (var guard = 0; guard < 20; guard++)
        {
            var result = await QueryStoreComposeStamp.RunTickAsync(source, DateTime.UtcNow, RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, result.Failed);
            if (result.Built == 0)
            {
                return total;
            }

            total += result.Built;
        }

        throw new InvalidOperationException("the builder never ran out of hours");
    }

    /// <summary>One late row (a replay) into an hour, at the given stamp.</summary>
    internal static Task InsertRowAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, long interval, CancellationToken ct) => ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id)
VALUES (TIMESTAMP '{At(collectionTime)}', {serverId}, 'dbX', 99, 99, 'Regular', TIMESTAMP '{At(collectionTime.AddMinutes(-3))}', TIMESTAMP '{At(collectionTime)}',
        'modX', 'hashX', 7, 1000, 500, 2000, 900, {interval})", ct);

    /// <summary>A migrated scratch store with the three servers enrolled, the 30-hour seed, and every due hour built (28).</summary>
    internal static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection, NpgsqlDataSource Source, DateTime HourNow)> ArrangeAsync(
        CancellationToken ct, int servers = 3, bool build = true)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        for (var s = 1; s <= servers; s++)
        {
            await EnrollAsync(connection, s, ct);
        }

        var hourNow = ThisHour();
        await SeedAsync(connection, hourNow, ct);
        var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        if (build)
        {
            Assert.Equal(28, await BuildAllAsync(source, ct));
        }

        return (scratch, connection, source, hourNow);
    }

    /// <summary>The runner's StampThrough lookup on a connection, for the servers 1 to <paramref name="servers"/>.</summary>
    internal static Task<DateTime?> StampThroughAsync(NpgsqlConnection connection, DateTime wideStart, DateTime end, CancellationToken ct, int servers = 3) =>
        PerformanceMonitor.Darling.Service.DarlingWebEndpoints.ResolveQueryStoreStampThroughAsync(
            connection, System.Linq.Enumerable.ToArray(System.Linq.Enumerable.Range(1, servers)), wideStart, end, QueryStoreComposeStamp.RungVersion, ct);

    internal static async Task CleanupAsync(ScratchPostgres scratch, NpgsqlConnection connection, NpgsqlDataSource source, bool bodySucceeded)
    {
        await source.DisposeAsync();
        await connection.DisposeAsync();
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        await scratch.DisposeAsync();
    }
}
