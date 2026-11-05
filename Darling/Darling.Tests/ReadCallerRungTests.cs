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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch database through ScratchPostgres and never touch another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that records who made each read (V183, #5793): its place at the top of the ladder, the shape of its script,
/// the probe's sentinel, and on a live store: a store at V182 holding rows migrates with every row kept (caller NULL, count
/// 0), a second run changes nothing, a slow read stores its caller and get_slow_reads returns it, get_read_latency returns
/// distinct_callers, and no column of either table is named like personal data.
/// </summary>
public sealed class ReadCallerRungTests
{
    private const int RungVersion = 183;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegistered_OnceBelowTheTopOfTheLadder()
    {
        Assert.Equal("read-caller", Rung.Name);
        Assert.Single(PgMigrations.Scripts, m => m.Version == RungVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        /* V184 (#5632) and V185 (#5812) took the top of the ladder; this rung stays registered below them. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void TheScript_AddsTheTwoColumns_IfMissing_AndNothingElse()
    {
        var sql = Rung.Sql;
        Assert.Contains("ADD COLUMN IF NOT EXISTS caller text;", sql, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS distinct_callers integer NOT NULL DEFAULT 0;", sql, StringComparison.Ordinal);
        foreach (var verb in new[] { "DELETE", "INSERT", "UPDATE", "GRANT", "TRUNCATE", "DROP", "CONSTRAINT", "INDEX" })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.Ordinal);
        }
    }

    /// <summary>A store at V182 that already holds read-latency and slow-read rows takes V183: every row is kept with a NULL
    /// caller and a count of 0, the probe maps to 183, and a second run changes nothing.</summary>
    [Fact]
    public async Task AStoreAtV182WithRows_TakesV183_AndKeepsEveryRow()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V183 migration pin.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Put the two tables back to their V182 shape and the ladder back to 182. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        await ExecAsync(connection, @"
DELETE FROM darling_schema_version WHERE version >= 183;
INSERT INTO collect.read_latency (metric_time, surface, route, outcome, window_band, run_count, total_ms, max_ms, bucket_counts)
VALUES ('2026-10-01 05:00', 'mcp', 'get_wait_stats', 'ok', '24h', 7, 700, 300, ARRAY[7]::bigint[]),
       ('2026-10-01 06:00', 'web', 'get_blocking', 'ok', 'none', 2, 40, 30, ARRAY[2]::bigint[]);
INSERT INTO collect.slow_reads (read_time, surface, route, outcome, total_ms, arguments, arguments_truncated, statements, statement_count, statements_truncated)
VALUES ('2026-10-01 05:10', 'mcp', 'get_wait_stats', 'timeout', 60000, '{}', false, '[]', 0, false);", ct);
        Assert.Equal(182, await ProbedVersionAsync(connection, ct));

        /* V183 and the rungs above it (V184, #5632; V185, #5812) all apply. */
        Assert.Equal(StorageVersion.SchemaVersion - 182, await PgMigrations.MigrateAsync(connection, ct));
        await AssertMigratedAsync(connection, ct);

        await ExecAsync(connection, Rung.Sql, ct);
        await AssertMigratedAsync(connection, ct);
    }

    private static async Task AssertMigratedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
        Assert.Equal("2|2|9", await ScalarAsync(connection, @"
SELECT count(*) || '|' || count(*) FILTER (WHERE distinct_callers = 0) || '|' || sum(run_count) FROM collect.read_latency", ct));
        Assert.Equal("1|1", await ScalarAsync(connection, @"
SELECT count(*) || '|' || count(*) FILTER (WHERE caller IS NULL) FROM collect.slow_reads", ct));
    }

    /// <summary>A slow read stored with its caller comes back from get_slow_reads with it; a flushed hour comes back from
    /// get_read_latency with its distinct_callers; and no column of either table is named like personal data.</summary>
    [Fact]
    public async Task TheCallerRoundTrips_ThroughBothReaders_AndNoStoredColumnIsNamedLikePersonalData()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V183 caller round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);

            var accumulator = new ReadLatencyAccumulator();
            accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 50, ReadWindowBand.OneDay, 0, 0, "claude-desktop 1.2.3");
            accumulator.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 50, ReadWindowBand.OneDay, 0, 0, "ua:node/22.3.0");
            await accumulator.FlushAsync(migrate, DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-1), DateTimeKind.Utc), logger: null, ct);

            var columns = await ScalarAsync(migrate, @"
SELECT string_agg(table_name || '.' || column_name, ',' ORDER BY table_name, ordinal_position)
FROM information_schema.columns
WHERE table_schema = 'collect' AND table_name IN ('slow_reads', 'read_latency')", ct);
            var names = columns!.Split(',');
            Assert.Contains("slow_reads.caller", names);
            Assert.Contains("read_latency.distinct_callers", names);
            Assert.Empty(ReadCallerTests.PersonalDataNames(names));
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        using (var scope = ReadScope.Open(null))
        {
            var log = new SlowReadLog(0);
            var record = SlowReadLog.Build(scope.Scope, ReadSurface.Web, "get_blocking", ReadOutcome.Timeout, 6_000, new JsonObject(), "57014", DateTime.UtcNow, "web:0123456789ab");
            await log.StoreAsync(postgres, record, null);
        }

        var slow = JsonNode.Parse(await DarlingMcpSlowReadTools.GetSlowReads(postgres, 24, null, null, null, 20, false, ct))!;
        Assert.Equal("web:0123456789ab", (string?)slow["reads"]![0]!["caller"]);

        var latency = JsonNode.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, 24, null, null, 50, cancellationToken: ct))!;
        var row = latency.AsObject().Select(p => p.Value).OfType<JsonArray>().SelectMany(a => a).Single(r => (string?)r!["route"] == "get_wait_stats")!;
        Assert.Equal(2, (int)row["distinct_callers"]!);
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
