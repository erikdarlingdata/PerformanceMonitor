/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling's restart continuity (the Postgres twin of Lite's DuckDB delta seeding): the four
/// seed queries' latest-row form and column lists, and the shared-core inheritance. The
/// end-to-end test runs only when DARLING_TEST_PG points at a dev Postgres: it inserts two
/// wait_stats rows for a fake server at two collection times, runs SeedFromStoreAsync, and proves
/// the LATEST stored row became the delta baseline — so the first collection after a service
/// restart produces a real delta instead of 0.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingDeltaSeederTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -535353;

    private const string TestWaitType = "DARLING_SEED_E2E_WAIT";

    [Fact]
    public void DarlingDeltaCalculator_IsTheSharedCore()
    {
        /* The seeding host must ride the shared baseline / counter-reset / gap-policy semantics —
           same relationship Lite's DeltaCalculator has to the core. */
        Assert.IsAssignableFrom<CollectorDeltaCalculator>(new DarlingDeltaCalculator());
    }

    [Fact]
    public void SeedSql_WaitStats_LatestRowFormAndColumns()
    {
        var sql = DarlingDeltaCalculator.WaitStatsSeedSql;
        Assert.Contains("SELECT server_id, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, collection_time",
            sql, StringComparison.Ordinal);
        Assert.Contains("FROM wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("(server_id, collection_time) IN (", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT server_id, MAX(collection_time) FROM wait_stats WHERE collection_time >= $1 GROUP BY server_id",
            sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SeedSql_FileIoStats_LatestRowFormAndColumns()
    {
        var sql = DarlingDeltaCalculator.FileIoStatsSeedSql;
        Assert.Contains("SELECT server_id, database_name, file_name,", sql, StringComparison.Ordinal);
        Assert.Contains("num_of_reads, num_of_writes, read_bytes, write_bytes,", sql, StringComparison.Ordinal);
        Assert.Contains("io_stall_read_ms, io_stall_write_ms,", sql, StringComparison.Ordinal);
        Assert.Contains("io_stall_queued_read_ms, io_stall_queued_write_ms,", sql, StringComparison.Ordinal);
        Assert.Contains("FROM file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("(server_id, collection_time) IN (", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT server_id, MAX(collection_time) FROM file_io_stats WHERE collection_time >= $1 GROUP BY server_id",
            sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SeedSql_PerfmonStats_LatestRowFormAndColumns()
    {
        var sql = DarlingDeltaCalculator.PerfmonStatsSeedSql;
        Assert.Contains("SELECT server_id, object_name, counter_name, instance_name, cntr_value, collection_time",
            sql, StringComparison.Ordinal);
        Assert.Contains("FROM perfmon_stats", sql, StringComparison.Ordinal);
        Assert.Contains("(server_id, collection_time) IN (", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT server_id, MAX(collection_time) FROM perfmon_stats WHERE collection_time >= $1 GROUP BY server_id",
            sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SeedSql_MemoryGrantStats_LatestRowFormAndColumns()
    {
        var sql = DarlingDeltaCalculator.MemoryGrantStatsSeedSql;

        /* collection_time joined this SELECT list in #1772; without it the memory-grant baselines
           seeded with a null timestamp and the gap policy could not reject a stale one. */
        Assert.Contains("SELECT server_id, pool_id, resource_semaphore_id, timeout_error_count, forced_grant_count, collection_time",
            sql, StringComparison.Ordinal);
        Assert.Contains("FROM memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("(server_id, collection_time) IN (", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT server_id, MAX(collection_time) FROM memory_grant_stats WHERE collection_time >= $1 GROUP BY server_id",
            sql, StringComparison.Ordinal);
    }

    public static TheoryData<string, string> SeedQueries() => new()
    {
        { DarlingDeltaCalculator.WaitStatsSeedSql, "wait_stats" },
        { DarlingDeltaCalculator.FileIoStatsSeedSql, "file_io_stats" },
        { DarlingDeltaCalculator.PerfmonStatsSeedSql, "perfmon_stats" },
        { DarlingDeltaCalculator.MemoryGrantStatsSeedSql, "memory_grant_stats" },
    };

    /// <summary>
    /// The #1772 pin, mirrored by Lite's LiteDeltaSeederTests: BOTH halves carry the cutoff. Bounding
    /// only the outer read still lets the inner MAX() aggregate every chunk of the hypertable;
    /// bounding only the inner one still lets the outer row-value probe scan them. Deliberately blunt
    /// — the cutoff appears exactly twice — so removing EITHER goes red.
    /// </summary>
    [Theory]
    [MemberData(nameof(SeedQueries))]
    public void SeedSql_BoundsTheOuterReadAndTheInnerMax(string sql, string table)
    {
        Assert.Equal(2, CountOccurrences(sql, "collection_time >= $1"));

        /* The inner aggregate, verbatim on one line so the assertion cannot be satisfied by a bound
           that sits anywhere else in the statement. */
        Assert.Contains(
            $"SELECT server_id, MAX(collection_time) FROM {table} WHERE collection_time >= $1 GROUP BY server_id",
            sql, StringComparison.Ordinal);

        /* ...and the other occurrence is the OUTER one: it precedes the row-value probe. */
        Assert.True(
            sql.IndexOf("collection_time >= $1", StringComparison.Ordinal)
            < sql.IndexOf("(server_id, collection_time) IN (", StringComparison.Ordinal),
            "the first cutoff must bound the outer read, before the row-value probe");
    }

    /// <summary>
    /// One placeholder, reused — which is what keeps these queries byte-identical with Lite's, since
    /// both engines resolve a repeated <c>$1</c> to a single bound parameter. A second placeholder
    /// would leave every caller's single AddWithValue short and the read would fail outright.
    /// </summary>
    [Theory]
    [MemberData(nameof(SeedQueries))]
    public void SeedSql_ReferencesExactlyOneParameter(string sql, string table)
    {
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }
        return count;
    }

    [Fact]
    public async Task EndToEnd_SeedFromStore_LatestRowBecomesBaseline_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live delta-seeding test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        /* Migrations are idempotent — a fresh store comes up, a current store no-ops. */
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
            await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);

            /* Two rows for the same wait type: an older baseline and the latest one, both recent
               enough to stay inside the 300 s gap this call passes explicitly. */
            var olderTime = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-2), DateTimeKind.Unspecified);
            var latestTime = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified);
            await InsertWaitStatsRowAsync(connection, olderTime, waitingTasks: 10, waitTimeMs: 2000, signalWaitTimeMs: 500);
            await InsertWaitStatsRowAsync(connection, latestTime, waitingTasks: 40, waitTimeMs: 5000, signalWaitTimeMs: 800);

            var deltas = new DarlingDeltaCalculator();
            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                await deltas.SeedFromStoreAsync(postgres, null, TestContext.Current.CancellationToken);
            }

            /* The LATEST row (wait_time_ms 5000) is the baseline: current 5100 => delta exactly
               100. Unseeded, this first sighting would return 0; had the OLDER row (2000) seeded
               instead, the delta would be 3100. */
            var now = DateTime.UtcNow;
            Assert.Equal(100, deltas.CalculateDelta(TestServerId, "wait_stats_time", TestWaitType, 5100, now, 300));

            /* The same latest row seeded the other two wait-stats delta groups. */
            Assert.Equal(5, deltas.CalculateDelta(TestServerId, "wait_stats_tasks", TestWaitType, 45, now, 300));
            Assert.Equal(200, deltas.CalculateDelta(TestServerId, "wait_stats_signal", TestWaitType, 1000, now, 300));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The evidence no local green run can give: that the cutoff actually keeps TimescaleDB OFF the
    /// rest of the hypertable. Which relations a query reads is invisible to a test that only checks
    /// the values it returns — the unbounded form returned the same baselines, it just read every
    /// chunk to find them, which is precisely why #1772 reached a 276 GB field store undetected.
    ///
    /// <para>So this asks the planner directly. Two sentinel rows are planted, one inside the window
    /// and one two days back, and the row's own <c>tableoid</c> names the chunk each landed in — exact,
    /// and free of the timezone arithmetic that reading chunk ranges out of the catalog would need.
    /// The seed query is then EXPLAINed with the real cutoff bound: the old row's chunk must be absent
    /// from the plan, and the window's chunk must be present (an assertion that the query still reads
    /// SOMETHING, so "excluded everything" cannot pass as success).</para>
    ///
    /// <para>Self-skipping rather than self-deceiving: if both rows land in one relation the store has
    /// no per-day chunks to exclude — an unpartitioned table, or a chunk interval wider than the two
    /// days — and the pin would assert nothing, so it says so instead of passing.</para>
    /// </summary>
    [Fact]
    public async Task SeedRead_ExcludesChunksOlderThanTheWindow_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live chunk-exclusion test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        /* Migrations alone leave the collector tables PLAIN — the hypertable conversion is a separate
           step the service runs on every start. Run the product's own, idempotent, so the pin measures
           the store shape a field install actually has (its real chunk interval included) rather than
           one hand-built here. */
        /* #1922: on its OWN connection. This site discarded the result entirely and then kept using the same
           connection for the whole test, so an unpreloaded TimescaleDB left every statement below failing
           with "Connection is not open" instead of the real cause.

           The result is CAPTURED and skipped on rather than discarded, which is the same point one layer up:
           without TimescaleDB the conversion below fails per table into a swallowed warning, the table stays
           plain, and the test lands on its own "no per-day chunks to exclude" skip several asserts later —
           true, but arrived at by inference. Saying it here names the actual condition. */
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, TestContext.Current.CancellationToken);
        Assert.SkipWhen(!timescaleEnabled, "TimescaleDB is not enabled on DARLING_TEST_PG — there are no chunks to exclude.");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);

            var insideWindow = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified);
            var twoDaysBack = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
            await InsertWaitStatsRowAsync(connection, insideWindow, waitingTasks: 40, waitTimeMs: 5000, signalWaitTimeMs: 800);
            await InsertWaitStatsRowAsync(connection, twoDaysBack, waitingTasks: 7, waitTimeMs: 1000, signalWaitTimeMs: 100);

            var recentChunk = await ChunkHoldingRowAsync(connection, insideWindow);
            var oldChunk = await ChunkHoldingRowAsync(connection, twoDaysBack);

            Assert.SkipWhen(recentChunk is null || oldChunk is null, "the sentinel rows did not land — nothing to prove.");
            Assert.SkipWhen(string.Equals(recentChunk, oldChunk, StringComparison.Ordinal),
                $"wait_stats put both timestamps in {recentChunk} — this store has no separate chunk to exclude.");

            var plan = await ExplainAsync(connection, DarlingDeltaCalculator.WaitStatsSeedSql, CollectorDeltaCalculator.SeedCutoff());

            Assert.DoesNotContain(oldChunk!, plan, StringComparison.Ordinal);
            Assert.Contains(recentChunk!, plan, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Every seed query actually RUNS against the real store schema — all four, not just the one the
    /// end-to-end test drives.
    ///
    /// <para>This closes a hole that would reproduce the exact field symptom. The memory-grant seed swallows
    /// its own exceptions entirely (a deliberate tolerance for a table that may not exist yet after a schema
    /// migration), so a column that is not there fails in total silence; the other three propagate to
    /// <c>SeedFromStoreAsync</c>'s catch, which logs the one warning the field already reported. Either way a
    /// broken query is invisible to any test that only inspects the SQL as a string — and #1772 added a
    /// column to one SELECT list and a bound parameter to all four.</para>
    /// </summary>
    [Fact]
    public async Task EverySeedQuery_RunsAgainstTheRealSchema_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live seed-query test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        var queries = new (string Sql, int Columns)[]
        {
            (DarlingDeltaCalculator.WaitStatsSeedSql, 6),
            (DarlingDeltaCalculator.FileIoStatsSeedSql, 12),
            (DarlingDeltaCalculator.PerfmonStatsSeedSql, 6),
            (DarlingDeltaCalculator.MemoryGrantStatsSeedSql, 6),
        };

        foreach (var (sql, columns) in queries)
        {
            using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue(CollectorDeltaCalculator.SeedCutoff());

            /* Executing is the assertion: a missing column, a mis-bound parameter, or a renamed table throws
               here instead of vanishing into a caller's catch. The column count then pins that the SELECT
               list the reader indexes by ordinal is the one the store actually returns. */
            using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.Equal(columns, reader.FieldCount);
        }
    }

    /// <summary>
    /// The chunk a planted row actually lives in. <c>tableoid</c> on a hypertable read resolves to the
    /// CHUNK, because the chunk is the table the row is stored in; on a plain table it resolves to the
    /// table itself, which is what lets the caller detect an unpartitioned store and skip. Returned
    /// unqualified, matching how EXPLAIN names relations.
    /// </summary>
    private static async Task<string?> ChunkHoldingRowAsync(NpgsqlConnection connection, DateTime collectionTime)
    {
        using var cmd = new NpgsqlCommand(
            "SELECT tableoid::regclass::text FROM wait_stats WHERE server_id = $1 AND collection_time = $2", connection);
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(collectionTime);
        var name = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken) as string;
        var dot = name?.LastIndexOf('.') ?? -1;
        return dot >= 0 ? name![(dot + 1)..] : name;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, DateTime cutoff)
    {
        using var cmd = new NpgsqlCommand("EXPLAIN (COSTS OFF)" + sql, connection);
        cmd.Parameters.AddWithValue(cutoff);
        var plan = new StringBuilder();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            plan.AppendLine(reader.GetString(0));
        }
        return plan.ToString();
    }

    private static async Task InsertWaitStatsRowAsync(
        NpgsqlConnection connection, DateTime collectionTime, long waitingTasks, long waitTimeMs, long signalWaitTimeMs)
    {
        using var insert = new NpgsqlCommand(
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms) " +
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        insert.Parameters.AddWithValue(1L);
        /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
        insert.Parameters.AddWithValue(collectionTime);
        insert.Parameters.AddWithValue(TestServerId);
        insert.Parameters.AddWithValue("delta-seed-e2e");
        insert.Parameters.AddWithValue(TestWaitType);
        insert.Parameters.AddWithValue(waitingTasks);
        insert.Parameters.AddWithValue(waitTimeMs);
        insert.Parameters.AddWithValue(signalWaitTimeMs);
        await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {TestServerId}", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
