/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch database through ScratchPostgres and never touch another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that records what a capacity question needs (V182, #5506): its place in the ladder, the shape of its script,
/// the probe's sentinel, and on a live store: a store at V181 holding read-latency rows migrates with every row kept (band
/// none, zeros), the primary key names the band, a second run changes nothing, a slow read stores its in-flight count and
/// connection wait, and a read that waits on an exhausted pool records the wait.
/// </summary>
public sealed class ReadLatencyCapacityRungTests
{
    private const int RungVersion = 182;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegistered_OnceBelowTheTopOfTheLadder()
    {
        Assert.Equal("read-latency-capacity-fields", Rung.Name);
        Assert.Single(PgMigrations.Scripts, m => m.Version == RungVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        /* V183 (#5793) and V184 (#5632) sit above it; this rung stays registered below them. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void TheScript_AddsTheColumnsWithDefaults_AndRecreatesTheKeyOnlyWhileItLacksTheBand()
    {
        var sql = Rung.Sql;
        Assert.Contains("ADD COLUMN IF NOT EXISTS window_band text NOT NULL DEFAULT 'none',", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS max_in_flight integer NOT NULL DEFAULT 0,", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS wait_total_ms bigint NOT NULL DEFAULT 0;", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS in_flight integer,", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS wait_ms bigint;", sql, StringComparison.Ordinal);
        Assert.Contains("PRIMARY KEY (metric_time, surface, route, outcome, window_band)", sql, StringComparison.Ordinal);
        Assert.Contains("a.attname = 'window_band'", sql, StringComparison.Ordinal);
        foreach (var verb in new[] { "DELETE", "INSERT", "UPDATE", "GRANT", "TRUNCATE", "DROP TABLE", "DROP COLUMN" })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ASlowReadRecord_CarriesTheScopesInFlightAndWait()
    {
        using var scope = ReadScope.Open(null);
        scope.Scope.AddWait(System.Diagnostics.Stopwatch.Frequency / 4);

        var record = SlowReadLog.Build(scope.Scope, ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 6_000, null, null, DateTime.UtcNow);

        Assert.Equal(scope.Scope.InFlightAtStart, record.InFlight);
        Assert.InRange(record.WaitMs!.Value, 249, 251);
    }

    /// <summary>A store at V181 that already holds read-latency and slow-read rows takes V182: every row is kept with band none and
    /// zeros, the key names the band, slow reads gain two NULL columns, the probe maps to 182, and a second run changes nothing.</summary>
    [Fact]
    public async Task AStoreAtV181WithRows_TakesV182_AndKeepsEveryRow()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V182 migration pin.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Put the two tables back to their V181 shape and the ladder back to 181. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        await ExecAsync(connection, @"
DELETE FROM darling_schema_version WHERE version >= 182;
INSERT INTO collect.read_latency (metric_time, surface, route, outcome, run_count, total_ms, max_ms, bucket_counts)
VALUES ('2026-10-01 05:00', 'mcp', 'get_wait_stats', 'ok', 7, 700, 300, ARRAY[7]::bigint[]),
       ('2026-10-01 05:00', 'mcp', 'get_wait_stats', 'timeout', 1, 60000, 60000, ARRAY[1]::bigint[]),
       ('2026-10-01 06:00', 'web', 'get_blocking', 'ok', 2, 40, 30, ARRAY[2]::bigint[]);
INSERT INTO collect.slow_reads (read_time, surface, route, outcome, total_ms, arguments, arguments_truncated, statements, statement_count, statements_truncated)
VALUES ('2026-10-01 05:10', 'mcp', 'get_wait_stats', 'timeout', 60000, '{}', false, '[]', 0, false);", ct);
        Assert.Equal(181, await ProbedVersionAsync(connection, ct));

        await PgMigrations.MigrateAsync(connection, ct);
        await AssertMigratedAsync(connection, ct);

        await ExecAsync(connection, Rung.Sql, ct);
        await AssertMigratedAsync(connection, ct);
    }

    private static async Task AssertMigratedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
        Assert.Equal("3|3|0|0|10", await ScalarAsync(connection, @"
SELECT count(*) || '|' || count(*) FILTER (WHERE window_band = 'none') || '|' || max(max_in_flight) || '|' || sum(wait_total_ms) || '|' || sum(run_count)
FROM collect.read_latency", ct));
        Assert.Equal("metric_time,surface,route,outcome,window_band", await ScalarAsync(connection, @"
SELECT string_agg(a.attname, ',' ORDER BY k.ord)
FROM pg_constraint AS c
CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_attribute AS a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
WHERE c.conrelid = 'collect.read_latency'::regclass AND c.contype = 'p'", ct));
        Assert.Equal("1|1", await ScalarAsync(connection, @"
SELECT count(*) || '|' || count(*) FILTER (WHERE in_flight IS NULL AND wait_ms IS NULL) FROM collect.slow_reads", ct));
    }

    /// <summary>The wait is measured where Npgsql's pool reports it: with a pool of one connection held by another caller, a
    /// read inside a scope waits until it is released, and that wait lands in the scope.</summary>
    [Fact]
    public async Task AReadThatWaitsOnAnExhaustedPool_RecordsTheWait()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V182 connection-wait pin.");

        var ct = TestContext.Current.CancellationToken;
        ReadConnectionWait.Register();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Pooling = true, MaxPoolSize = 1, Timeout = 30 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);

        var held = await dataSource.OpenConnectionAsync(ct);
        long waitMs;
        using (var scope = ReadScope.Open(null))
        {
            var release = Task.Run(async () =>
            {
                await Task.Delay(400, ct);
                await held.DisposeAsync();
            }, ct);

            await using (var command = dataSource.CreateCommand("SELECT 1"))
            {
                Assert.Equal(1, Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            await release;
            waitMs = scope.Scope.WaitMs;
        }

        Assert.InRange(waitMs, 300, 20_000);
    }

    /// <summary>The unit of Npgsql's <c>db.client.connection.npgsql.create_time</c>, pinned on a real pooled connection. The
    /// scope opens first and the pool is empty, so the read's one physical open happens inside the scope with no pending
    /// wait, and only the create callback can record a wait. The recorded wait must be over 0 ms (the instrument name and
    /// the callback are wired), under 10 s, and no longer than the open measured around it: a create time in milliseconds
    /// or ticks read as seconds would be 1000 times or more too long and fail the last bound.</summary>
    [Fact]
    public async Task APhysicalOpenInsideAScope_RecordsItsCreateTime_InTheRightUnit()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V182 create-time unit pin.");

        var ct = TestContext.Current.CancellationToken;
        ReadConnectionWait.Register();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Pooling = true, MinPoolSize = 0, MaxPoolSize = 4, Timeout = 30 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);

        long waitMs;
        long openMs;
        using (var scope = ReadScope.Open(null))
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            await using (var connection = await dataSource.OpenConnectionAsync(ct))
            {
                stopwatch.Stop();
                await using var command = new NpgsqlCommand("SELECT 1", connection);
                Assert.Equal(1, Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            openMs = stopwatch.ElapsedMilliseconds;
            waitMs = scope.Scope.WaitMs;
        }

        Assert.True(waitMs > 0, $"the physical open recorded no wait (open took {openMs} ms)");
        Assert.True(waitMs < 10_000, $"the physical open recorded {waitMs} ms of wait");
        Assert.True(waitMs <= openMs + 1, $"the recorded create time ({waitMs} ms) is longer than the open itself ({openMs} ms): a unit is wrong");
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var count = method.GetParameters().Length;
        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var args = Enumerable.Range(0, count).Select(i => (object)reader.GetBoolean(i)).ToArray();
        return (int)method.Invoke(null, args)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
