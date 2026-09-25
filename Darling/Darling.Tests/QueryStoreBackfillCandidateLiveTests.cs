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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4197 end to end against a REAL TimescaleDB: <see cref="QueryStoreBackfill.GetCandidateDatabasesAsync"/> and
/// <see cref="QueryStoreBackfill.GetStoredFloorAsync"/> now bind their reads at the backfill's own horizon
/// instead of scanning all of raw retention, and the union with recorded hole keys is what keeps a database
/// that has gone fully quiet from aging out of the scan. Only a live TimescaleDB proves the bound actually
/// excludes the old chunk rather than merely reading fewer rows out of the same chunk.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database rather than sharing the live fixture; this
/// class compresses a chunk directly (bypassing the compression policy), which a shared fixture must not
/// inherit.</para>
///
/// <para><b>The compression trap.</b> The three seeded databases span two day-chunks (chunk interval is
/// <see cref="TimescaleSupport.ChunkIntervalDays"/> = 1): <c>quiet_hole_db</c> and <c>finished_db</c> sit in an
/// old chunk, <c>new_db</c> sits in the current ("hot") chunk. Compressing with a bare
/// <c>show_chunks('collect.query_store_stats')</c> — the slice-repair live test's own pattern — would compress
/// the hot chunk too and make <see cref="CandidateRead_BoundAtItsOwnHorizon_ScansNoCompressedChunk"/>
/// impossible to pass, since a bounded read of a compressed hot chunk still name-checks a Decompress node even
/// when chunk exclusion works. Compression here is bounded with <c>older_than</c> so only the old chunk is
/// touched, and both states are proven against <c>timescaledb_information.chunks.is_compressed</c> before
/// anything else is asserted.</para>
/// </summary>
public sealed class QueryStoreBackfillCandidateLiveTests
{
    private const int TestServerId = -419719;
    private const string QuietHoleDb = "quiet_hole_db";
    private const string FinishedDb = "finished_db";
    private const string NewDb = "new_db";

    /// <summary>A fixed anchor, not <c>DateTime.UtcNow</c>, so the test is deterministic regardless of wall
    /// clock. Unspecified kind throughout the seeded data: <c>AddWithValue</c> on a Utc-kind <c>DateTime</c>
    /// against a <c>timestamp</c> (without time zone) column throws in Npgsql.</summary>
    private static readonly DateTime FloorLimit = new(2026, 6, 15, 9, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The old day-chunk both <see cref="QuietHoleDb"/> and <see cref="FinishedDb"/> seed into —
    /// several days before <see cref="FloorLimit"/> and a different day-chunk from it.</summary>
    private static readonly DateTime OldChunkDay = new(2026, 6, 10, 8, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The backfilled-row fact: old <c>last_execution_time</c>, recent <c>collection_time</c> (inside
    /// the window, landing in the hot chunk) — the shape a row shipped late by a repair produces. Distinct
    /// from both <see cref="FloorLimit"/> and <see cref="NewDb"/>'s own <c>collection_time</c> so the floor
    /// read's answer is unambiguous.</summary>
    private static readonly DateTime BackfilledLastExecutionTime = new(2026, 5, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Mirrors <see cref="QueryStoreBackfill.GetStoredFloorAsync"/>'s EXISTS text exactly. Not a
    /// shared constant like <see cref="QueryStoreBackfill.CandidateSql"/> — the production method builds it
    /// inline — so keep this in sync by hand if that literal ever changes.</summary>
    private const string ExistsSql =
        "SELECT 1 FROM query_store_stats WHERE server_id = $1 AND database_name = $2 AND collection_time <= $3 LIMIT 1";

    [Fact]
    public async Task CandidateDatabases_ServicesAQuietHole_DropsAFinishedDatabase_FindsANewDatabase()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4197 backfill-candidate test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var setup = await CreateMigratedScratchAsync(baseConnectionString!, ct);
        await using var scratch = setup.Scratch;
        await using var connection = setup.Connection;

        var state = await SeedScenarioAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var backfill = new QueryStoreBackfill(postgres, runner, new CollectorDeltaCalculator(), logger: null);

        var candidates = await backfill.GetCandidateDatabasesAsync(TestServerId, FloorLimit, state, ct);

        /* #4197 trap: GetCandidateDatabasesAsync swallows read exceptions internally and still returns the
           hole-merged list, so asserting only the hole-derived member (quiet_hole_db) could pass even with a
           dead store read. new_db's presence is what proves the bounded SELECT itself executed; finished_db's
           absence is what proves the bound actually excludes a database with nothing newer than the floor. */
        Assert.Equal(new[] { NewDb, QuietHoleDb }, candidates);
        Assert.DoesNotContain(FinishedDb, candidates);
    }

    [Fact]
    public async Task StoredFloor_ARowAtOrBeforeTheFloor_ReturnsTheFloor_ABackfilledRow_ReturnsItsTrueMin()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4197 stored-floor test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var setup = await CreateMigratedScratchAsync(baseConnectionString!, ct);
        await using var scratch = setup.Scratch;
        await using var connection = setup.Connection;

        await SeedScenarioAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var backfill = new QueryStoreBackfill(postgres, runner, new CollectorDeltaCalculator(), logger: null);

        /* quiet_hole_db has a row with collection_time <= floorLimit, so the EXISTS hits and the method
           returns floorLimit itself (the caller only ever compares the result against floorLimit on that
           branch, so returning floorLimit keeps the decision byte-identical without reading the real MIN). */
        var quietFloor = await backfill.GetStoredFloorAsync(TestServerId, QuietHoleDb, FloorLimit, ct);
        Assert.Equal(FloorLimit, quietFloor);

        /* new_db has no row that old - its only row is after floorLimit - so the EXISTS misses and the
           method falls through to the real bounded MIN(last_execution_time), which is the seeded backfilled
           value, not floorLimit and not new_db's own collection_time. */
        var newFloor = await backfill.GetStoredFloorAsync(TestServerId, NewDb, FloorLimit, ct);
        Assert.Equal(BackfilledLastExecutionTime, newFloor);
        Assert.NotEqual(FloorLimit, newFloor);
        Assert.NotEqual(FloorLimit.AddHours(1), newFloor);
    }

    /// <summary>
    /// The plan-shape pin: the candidate read bound at its own horizon must exclude the compressed old chunk
    /// entirely, not merely read fewer rows out of it. Uses LITERAL values substituted into
    /// <see cref="QueryStoreBackfill.CandidateSql"/> rather than parameters — a parameterized EXPLAIN can defer
    /// chunk exclusion to executor startup and leave never-executed Decompress nodes in the plan text, which
    /// would hide the exact regression #4197 fixed.
    /// </summary>
    [Fact]
    public async Task CandidateRead_BoundAtItsOwnHorizon_ScansNoCompressedChunk()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4197 candidate-read plan-shape test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var setup = await CreateMigratedScratchAsync(baseConnectionString!, ct);
        await using var scratch = setup.Scratch;
        await using var connection = setup.Connection;

        await SeedScenarioAsync(connection, ct);
        var compressedChunkName = await CompressedChunkNameAsync(connection, ct);

        var literalSql = QueryStoreBackfill.CandidateSql
            .Replace("$1", TestServerId.ToString(CultureInfo.InvariantCulture))
            .Replace("$2", Literal(FloorLimit));

        var plan = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + literalSql, ct);

        Assert.DoesNotContain("DecompressChunk", plan);
        Assert.DoesNotContain("ColumnarScan", plan);
        Assert.DoesNotContain(compressedChunkName, plan);
        Assert.True(PlanChunkScans.Count(plan) >= 1,
            "expected the hot (2026-06-15) chunk to still be scanned:\n" + plan);
    }

    /// <summary>
    /// #4197's worst case: a database with NO rows older than floorLimit forces the EXISTS to walk the
    /// compressed old chunk(s) before it can conclude no match, so it falls through to the MIN query too.
    /// Decompress nodes are EXPECTED here - this is the cost of "database is new," not a defect - so this
    /// pins the shape (the search does reach the compressed chunk) and reports buffers/timing as diagnostics
    /// rather than asserting on them. See the PR body for the measured numbers; the ruling is not to change
    /// the design based on them.
    /// </summary>
    [Fact]
    public async Task StoredFloorExistsMiss_ForANewDatabase_WalksTheCompressedChunk_MeasuredCost()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4197 EXISTS-miss cost test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var setup = await CreateMigratedScratchAsync(baseConnectionString!, ct);
        await using var scratch = setup.Scratch;
        await using var connection = setup.Connection;

        await SeedScenarioAsync(connection, ct);
        var compressedChunkName = await CompressedChunkNameAsync(connection, ct);

        var literalSql = ExistsSql
            .Replace("$1", TestServerId.ToString(CultureInfo.InvariantCulture))
            .Replace("$2", LiteralText(NewDb))
            .Replace("$3", Literal(FloorLimit));

        var plan = await ExplainAsync(connection, "EXPLAIN (ANALYZE, BUFFERS) " + literalSql, ct);

        Console.Error.WriteLine("DIAG queryStoreBackfillExistsMiss (new_db, no rows <= floorLimit):");
        Console.Error.WriteLine(plan);

        Assert.Contains(compressedChunkName, plan);
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> CreateMigratedScratchAsync(
        string baseConnectionString, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        return (scratch, connection);
    }

    /// <summary>Seeds the three databases and compresses ONLY the old chunk, then proves both compression
    /// states before returning. Returns the hand-built state dict carrying quiet_hole_db's hole key - no
    /// runner state round-trip needed for this dict.</summary>
    private static async Task<Dictionary<string, string>> SeedScenarioAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await SeedRowAsync(connection, 4197001, OldChunkDay, QuietHoleDb, OldChunkDay, ct);
        await SeedRowAsync(connection, 4197002, OldChunkDay.AddHours(1), FinishedDb, OldChunkDay.AddHours(1), ct);
        await SeedRowAsync(connection, 4197003, FloorLimit.AddHours(1), NewDb, BackfilledLastExecutionTime, ct);

        /* Compression enablement lives in a separate service-start step this test deliberately skips (a
           background policy racing the assertions is pure interference); enable it directly, exactly like the
           slice-repair test's compressed-chunk pin. Bounded with older_than so ONLY the 2026-06-10 chunk is
           compressed - the trap this class's own doc comment explains. */
        await ExecAsync(connection,
            "ALTER TABLE collect.query_store_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);
        await ExecAsync(connection,
            "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('collect.query_store_stats', older_than => TIMESTAMP '2026-06-12 00:00:00') c", ct);

        Assert.Equal(1L, await ScalarAsync(connection, @"
SELECT count(*) FROM timescaledb_information.chunks
WHERE hypertable_name = 'query_store_stats' AND is_compressed", ct));
        Assert.True(await ScalarAsync(connection, @"
SELECT count(*) FROM timescaledb_information.chunks
WHERE hypertable_name = 'query_store_stats' AND NOT is_compressed", ct) >= 1,
            "expected the hot (2026-06-15) chunk to remain uncompressed");

        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            /* Spans across floorLimit (from a day before it to a day after) so the hole has not expired. */
            [QueryStoreBackfillState.HoleKeyPrefix + QuietHoleDb] =
                QueryStoreBackfillState.EncodeHole(FloorLimit.AddDays(-1), FloorLimit.AddDays(1)),
        };
    }

    private static async Task SeedRowAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, string databaseName,
        DateTime lastExecutionTime, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us)
VALUES
    ($1, $2, $3, 'SQL01', $4, 'dbo.GetOrders', '0xABCD', 91, 111, 'Regular', 'Primary',
     1, $2, $5, $5, 1, 100, 100, 100, 100)";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(lastExecutionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task<string> CompressedChunkNameAsync(NpgsqlConnection connection, CancellationToken ct)
        => ScalarStringAsync(connection, @"
SELECT chunk_name FROM timescaledb_information.chunks
WHERE hypertable_name = 'query_store_stats' AND is_compressed
ORDER BY chunk_name LIMIT 1", ct);

    private static string Literal(DateTime value) =>
        "TIMESTAMP '" + value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";

    private static string LiteralText(string value) => "'" + value.Replace("'", "''") + "'";

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var plan = new StringBuilder();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? 0L : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static async Task<string> ScalarStringAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
