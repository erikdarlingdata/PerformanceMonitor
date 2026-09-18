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
/// Pins Darling's restart continuity (the Postgres twin of Lite's DuckDB delta seeding): every seed
/// query's form and column list, and the shared-core inheritance. The end-to-end tests run only when
/// DARLING_TEST_PG points at a dev Postgres: they insert two collections per family for a fake server,
/// run SeedFromStoreAsync, and prove the LATEST stored row became the delta baseline — so the first
/// collection after a service restart produces a real delta instead of 0.
///
/// <para>#3540 A4 widened the seed from four families to every delta family the service monitors, plus
/// the per-group pass window the #2235 series-age rescue reads. Two query shapes are pinned here: the
/// original latest-collection-per-server row-value probe (now also latch_stats and spinlock_stats), and
/// the latest-row-per-key <c>DISTINCT ON</c> form for the families whose collectors do not write every
/// key every pass (procedure_stats, pg_wait_stats, pg_statement_stats — and query_stats since V128, #3540,
/// once the store persisted the two statement offsets its key is made of; a pre-V128 row seeds the pass
/// window and no key). The census in Lite.Tests holds the set.</para>
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

    /// <summary>
    /// #3540 A4: the two families that mirror wait_stats exactly (every key written every pass, keyed by
    /// name) take wait_stats' exact shape. Column lists are the collector's counters, in its order.
    /// </summary>
    [Fact]
    public void SeedSql_LatchAndSpinlock_LatestRowFormAndColumns()
    {
        var latch = DarlingDeltaCalculator.LatchStatsSeedSql;
        Assert.Contains("SELECT server_id, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, collection_time",
            latch, StringComparison.Ordinal);
        Assert.Contains("FROM latch_stats", latch, StringComparison.Ordinal);
        Assert.Contains("(server_id, collection_time) IN (", latch, StringComparison.Ordinal);

        var spin = DarlingDeltaCalculator.SpinlockStatsSeedSql;
        Assert.Contains("SELECT server_id, spinlock_name, collisions, spins, sleep_time, backoffs, collection_time",
            spin, StringComparison.Ordinal);
        Assert.Contains("FROM spinlock_stats", spin, StringComparison.Ordinal);
        Assert.Contains("(server_id, collection_time) IN (", spin, StringComparison.Ordinal);
        /* spins_per_collision is a DMV-computed ratio, never delta'd, so it is not a baseline. */
        Assert.DoesNotContain("spins_per_collision", spin, StringComparison.Ordinal);
    }

    /// <summary>
    /// The procedure_stats key is built IN the SQL, exactly as <c>ProcedureStatsCollector.WritePayload</c>
    /// builds it (<c>plan_handle ?? $"{db}.{schema}.{object}"</c>, null parts formatting as empty), so
    /// DISTINCT ON partitions by the key the collector will present and the reader uses it verbatim. A key
    /// assembled differently seeds a baseline nothing ever reads — silently.
    /// </summary>
    [Fact]
    public void SeedSql_ProcedureStats_BuildsTheCollectorsKeyAndTakesTheLatestRowPerKey()
    {
        var sql = DarlingDeltaCalculator.ProcedureStatsSeedSql;
        Assert.Contains("SELECT DISTINCT ON (server_id, delta_key)", sql, StringComparison.Ordinal);
        Assert.Contains(
            "COALESCE(plan_handle, COALESCE(database_name, '') || '.' || COALESCE(schema_name, '') || '.' || COALESCE(object_name, '')) AS delta_key",
            sql, StringComparison.Ordinal);
        Assert.Contains("execution_count, total_worker_time, total_elapsed_time,", sql, StringComparison.Ordinal);
        Assert.Contains("total_logical_reads, total_logical_writes, total_physical_reads, total_spills,", sql, StringComparison.Ordinal);
        Assert.Contains("FROM procedure_stats", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, delta_key, collection_time DESC", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The PostgreSQL pair's keys are the collectors' own: the numeric event id for waits (the name changes
    /// case across Aurora majors), and the full (queryid, database_id, user_id, toplevel) identity for
    /// statements. The datid in that key is the collector's choice as written (#3540 A11c); the seed
    /// reproduces it rather than redesigning it.
    /// </summary>
    [Fact]
    public void SeedSql_PostgresPair_PartitionByTheCollectorsKeyAndTakeTheLatestRowPerKey()
    {
        var waits = DarlingDeltaCalculator.PgWaitStatsSeedSql;
        Assert.Contains("SELECT DISTINCT ON (server_id, wait_event_id)", waits, StringComparison.Ordinal);
        Assert.Contains("server_id, wait_event_id, waits, wait_time_us, collection_time", waits, StringComparison.Ordinal);
        Assert.Contains("FROM pg_wait_stats", waits, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, wait_event_id, collection_time DESC", waits, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_event,", waits, StringComparison.Ordinal);

        var statements = DarlingDeltaCalculator.PgStatementStatsSeedSql;
        Assert.Contains("SELECT DISTINCT ON (server_id, queryid, database_id, user_id, toplevel)", statements, StringComparison.Ordinal);
        Assert.Contains("calls, total_exec_time_ms, rows_returned, collection_time", statements, StringComparison.Ordinal);
        Assert.Contains("FROM pg_statement_stats", statements, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, queryid, database_id, user_id, toplevel, collection_time DESC", statements, StringComparison.Ordinal);
    }

    /// <summary>
    /// query_stats is key-seeded since V128 (#3540): the store persists both statement offsets now, so
    /// the read partitions by the collector's FULL key — sql_handle, both offsets, plan_handle — and
    /// returns each key's latest row inside the window, with the eight counters the collector's eight
    /// series difference. The offsets are selected RAW (no COALESCE, no arithmetic) because the seeder
    /// rebuilds the key from them with the collector's own interpolation, and there is no offset filter
    /// in the SQL: a pre-V128 row (NULL offsets) is read for the pass window and skipped for keys in C#,
    /// so one read serves both halves.
    /// </summary>
    [Fact]
    public void SeedSql_QueryStats_PartitionsByTheFullDeltaKeyAndSelectsTheOffsetsRaw()
    {
        var sql = DarlingDeltaCalculator.QueryStatsSeedSql;
        Assert.Contains("SELECT DISTINCT ON (server_id, sql_handle, statement_start_offset, statement_end_offset, plan_handle)", sql, StringComparison.Ordinal);
        Assert.Contains("server_id, sql_handle, statement_start_offset, statement_end_offset, plan_handle,", sql, StringComparison.Ordinal);
        Assert.Contains("execution_count, total_worker_time, total_elapsed_time,", sql, StringComparison.Ordinal);
        Assert.Contains("total_logical_reads, total_logical_writes, total_physical_reads, total_rows, total_spills,", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, sql_handle, statement_start_offset, statement_end_offset, plan_handle, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("COALESCE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GROUP BY", sql, StringComparison.Ordinal);
    }

    public static TheoryData<string, string> SeedQueries() => new()
    {
        { DarlingDeltaCalculator.WaitStatsSeedSql, "wait_stats" },
        { DarlingDeltaCalculator.FileIoStatsSeedSql, "file_io_stats" },
        { DarlingDeltaCalculator.PerfmonStatsSeedSql, "perfmon_stats" },
        { DarlingDeltaCalculator.MemoryGrantStatsSeedSql, "memory_grant_stats" },
        { DarlingDeltaCalculator.LatchStatsSeedSql, "latch_stats" },
        { DarlingDeltaCalculator.SpinlockStatsSeedSql, "spinlock_stats" },
    };

    /// <summary>The latest-row-per-key shape (#3540 A4, query_stats since V128): one table read, one bound, DISTINCT ON.</summary>
    public static TheoryData<string, string> PerKeySeedQueries() => new()
    {
        { DarlingDeltaCalculator.ProcedureStatsSeedSql, "procedure_stats" },
        { DarlingDeltaCalculator.QueryStatsSeedSql, "query_stats" },
        { DarlingDeltaCalculator.PgWaitStatsSeedSql, "pg_wait_stats" },
        { DarlingDeltaCalculator.PgStatementStatsSeedSql, "pg_statement_stats" },
    };

    /// <summary>
    /// The per-key shape's bound: the cutoff appears exactly ONCE, on its only table read, before the
    /// DISTINCT ON's ORDER BY — there is no inner aggregate to bind a second time, and a second table read
    /// would be the unbounded scan #1772 removed. The live chunk-exclusion pin below proves the one bound
    /// keeps TimescaleDB on the window's chunk.
    /// </summary>
    [Theory]
    [MemberData(nameof(PerKeySeedQueries))]
    public void PerKeySeedSql_BoundsItsOnlyTableRead_OnceBeforeTheOrdering(string sql, string table)
    {
        Assert.Equal(1, CountOccurrences(sql, "collection_time >= $1"));
        Assert.Equal(1, CountOccurrences(sql, "FROM " + table));
        Assert.Contains("SELECT DISTINCT ON (server_id,", sql, StringComparison.Ordinal);
        Assert.EndsWith("collection_time DESC", sql.TrimEnd(), StringComparison.Ordinal);
        Assert.True(
            sql.IndexOf("collection_time >= $1", StringComparison.Ordinal)
            < sql.IndexOf("ORDER BY server_id,", StringComparison.Ordinal),
            "the bound must sit on the table read, ahead of the ordering that picks the latest row per key");
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        /* No MAX(collection_time) probe: that shape returns the latest COLLECTION, which for these families
           is missing every key whose counter was idle or fell out of the TOP (n) on the last pass. */
        Assert.DoesNotContain("MAX(collection_time)", sql, StringComparison.Ordinal);
    }

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
    /// Every seed query actually RUNS against the real store schema — all ten, not just the ones the
    /// end-to-end tests drive.
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
            /* #3540 A4 */
            (DarlingDeltaCalculator.LatchStatsSeedSql, 6),
            (DarlingDeltaCalculator.SpinlockStatsSeedSql, 7),
            (DarlingDeltaCalculator.ProcedureStatsSeedSql, 10),
            /* #3540 V128: 5 key parts + 8 counters + collection_time */
            (DarlingDeltaCalculator.QueryStatsSeedSql, 14),
            (DarlingDeltaCalculator.PgWaitStatsSeedSql, 5),
            (DarlingDeltaCalculator.PgStatementStatsSeedSql, 9),
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
    /// #3540 A4, end to end on the real schema: every family the seed gained produces a REAL delta on the
    /// first post-restart call, the per-key shape restores a key the latest pass did not write, the
    /// query_stats pass window arms the #2235 series-age rescue on that first pass, and an original
    /// family's pass window is seeded too. Every expected value below was worked by hand from the rows
    /// and reproduced against PG18 + TimescaleDB 2.28.1 before this was written.
    ///
    /// <para>Two servers: the seeded one, and a second whose only rows predate the window so the
    /// same pass proves the bound is doing the discriminating (a first sighting, and no rescue).</para>
    /// </summary>
    [Fact]
    public async Task EndToEnd_EveryFamilyAndThePassWindow_SeedFromStore_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live delta-seeding test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            await DeleteFamilyRowsAsync(connection, TestContext.Current.CancellationToken);

            var now = DateTime.UtcNow;
            var stale = Naive(now - CollectorDeltaCalculator.SeedLookback - TimeSpan.FromMinutes(5));
            var older = Naive(now.AddMinutes(-4));
            var latest = Naive(now.AddMinutes(-2));

            /* latch_stats / spinlock_stats: every key every pass; the stale server's only pass is outside. */
            await LatchAsync(connection, TestServerId, stale, 1, 1, 1);
            await LatchAsync(connection, TestServerId, older, 100, 1000, 50);
            await LatchAsync(connection, TestServerId, latest, 140, 1500, 60);
            await LatchAsync(connection, StaleServerId, stale, 5, 50, 5);
            await SpinlockAsync(connection, older, 10, 100, 5, 2);
            await SpinlockAsync(connection, latest, 20, 200, 8, 3);

            /* procedure_stats: 0x01 in both passes; 0x02 only in the OLDER pass (fell out of the TOP (150));
               a null-handle row keyed by db.schema.object; and a stale 0x02 row outside the window. */
            await ProcAsync(connection, older, "0x01", "db", "dbo", "p1", 10);
            await ProcAsync(connection, latest, "0x01", "db", "dbo", "p1", 15);
            await ProcAsync(connection, older, "0x02", "db", "dbo", "p2", 7);
            await ProcAsync(connection, latest, null, "db", "dbo", "proc3", 3);
            await ProcAsync(connection, stale, "0x02", "db", "dbo", "p2", 1);

            /* query_stats: three PRE-V128 passes (no offsets), one stale — the pass window must come from the
               two inside; plus V128 rows carrying the offsets (#3540): the whole-batch statement (0, -1) in
               both passes, a second statement of the same batch/plan (100, 240) only in the older pass, and a
               null-handle row. */
            foreach (var t in new[] { stale, older, latest })
            {
                await QueryStatsAsync(connection, t);
            }
            await KeyedQueryStatsAsync(connection, older, "0xSH1", 0, -1, "0xPH1", 10);
            await KeyedQueryStatsAsync(connection, latest, "0xSH1", 0, -1, "0xPH1", 15);
            await KeyedQueryStatsAsync(connection, older, "0xSH1", 100, 240, "0xPH1", 7);
            await KeyedQueryStatsAsync(connection, latest, null, 0, -1, null, 3);

            /* pg_wait_stats: 1001 both passes; 1002 idle at the latest pass, so its newest row is the older. */
            await PgWaitAsync(connection, older, 1001, 5, 500);
            await PgWaitAsync(connection, latest, 1001, 10, 900);
            await PgWaitAsync(connection, older, 1002, 3, 300);

            /* pg_statement_stats: statement 11 both passes; statement 12 (toplevel false) idle at the latest. */
            await PgStatementAsync(connection, older, 11, 16384, 10, true, 100, 1234.9, 50);
            await PgStatementAsync(connection, latest, 11, 16384, 10, true, 120, 1500.7, 60);
            await PgStatementAsync(connection, older, 12, 16384, 10, false, 7, 70.2, 7);

            /* wait_stats: an ORIGINAL family, to prove its pass window is seeded too. */
            await InsertWaitStatsRowAsync(connection, older, waitingTasks: 10, waitTimeMs: 2000, signalWaitTimeMs: 500);
            await InsertWaitStatsRowAsync(connection, latest, waitingTasks: 40, waitTimeMs: 5000, signalWaitTimeMs: 800);

            var deltas = new DarlingDeltaCalculator();
            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                await deltas.SeedFromStoreAsync(postgres, null, TestContext.Current.CancellationToken);
            }

            const int Gap = CollectorDeltaCalculator.DefaultMaxGapSeconds;
            var pass = now;

            /* latch: baseline is the LATEST pass (1500), not the older (1000) or the stale (1). */
            Assert.Equal(100, deltas.CalculateDeltaWithInterval(TestServerId, "latch_stats_wait_time", "BUFFER", 1600, out var latchInterval, pass, Gap));
            Assert.InRange(latchInterval, 118, 122);
            Assert.Equal(10, deltas.CalculateDelta(TestServerId, "latch_stats_waiting_requests", "BUFFER", 150, pass, Gap));
            Assert.Equal(0, deltas.CalculateDelta(TestServerId, "latch_stats_max_wait", "BUFFER", 60, pass, Gap));

            /* spinlock: all four counters. */
            Assert.Equal(5, deltas.CalculateDelta(TestServerId, "spinlock_stats_collisions", "LOCK_HASH", 25, pass, Gap));
            Assert.Equal(60, deltas.CalculateDelta(TestServerId, "spinlock_stats_spins", "LOCK_HASH", 260, pass, Gap));
            Assert.Equal(1, deltas.CalculateDelta(TestServerId, "spinlock_stats_sleep_time", "LOCK_HASH", 9, pass, Gap));
            Assert.Equal(4, deltas.CalculateDelta(TestServerId, "spinlock_stats_backoffs", "LOCK_HASH", 7, pass, Gap));

            /* procedure_stats: 0x01 from the latest pass; 0x02 from the OLDER pass (the latest-collection
               shape would have missed it entirely and this would be 0); the null-handle fallback key. */
            Assert.Equal(3, deltas.CalculateDelta(TestServerId, "proc_stats_exec", "0x01", 18, pass, Gap));
            Assert.Equal(30, deltas.CalculateDelta(TestServerId, "proc_stats_worker", "0x01", 180, pass, Gap));
            Assert.Equal(2, deltas.CalculateDeltaWithInterval(TestServerId, "proc_stats_exec", "0x02", 9, out var procInterval, pass, Gap));
            Assert.InRange(procInterval, 238, 242);
            Assert.Equal(2, deltas.CalculateDelta(TestServerId, "proc_stats_exec", "db.dbo.proc3", 5, pass, Gap));

            /* pg_wait_stats: the idle-at-latest event is seeded from its older row. */
            Assert.Equal(2, deltas.CalculateDelta(TestServerId, "pg_wait_stats_waits", "1001", 12, pass, Gap));
            Assert.Equal(100, deltas.CalculateDelta(TestServerId, "pg_wait_stats_time", "1001", 1000, pass, Gap));
            Assert.Equal(1, deltas.CalculateDelta(TestServerId, "pg_wait_stats_waits", "1002", 4, pass, Gap));

            /* pg_statement_stats: the collector's key, and its long-truncation of the stored double. */
            Assert.Equal(10, deltas.CalculateDelta(TestServerId, "pg_statement_stats_calls", "11|16384|10|1", 130, pass, Gap));
            Assert.Equal(100, deltas.CalculateDelta(TestServerId, "pg_statement_stats_time", "11|16384|10|1", 1600, pass, Gap));
            Assert.Equal(1, deltas.CalculateDelta(TestServerId, "pg_statement_stats_rows", "11|16384|10|1", 61, pass, Gap));
            Assert.Equal(1, deltas.CalculateDelta(TestServerId, "pg_statement_stats_calls", "12|16384|10|0", 8, pass, Gap));

            /* query_stats KEYS (#3540, V128): the latest pass is the baseline for the key both passes wrote,
               spelled with the raw -1 as the collector spells it; the statement that fell out of the TOP (n)
               is restored from its OLDER row over ~240 s; a null handle formats as empty on both sides; and
               the pre-V128 rows seeded NOTHING — not even under a normalizing guess (gap policy off, so only a
               first sighting reads 0; a seeded baseline of 1 would return 4). */
            Assert.Equal(3, deltas.CalculateDeltaWithInterval(TestServerId, "query_stats_exec", "0xSH1:0:-1:0xPH1", 18, out var keyedInterval, pass, Gap));
            Assert.InRange(keyedInterval, 118, 122);
            Assert.Equal(210, deltas.CalculateDelta(TestServerId, "query_stats_spills", "0xSH1:0:-1:0xPH1", 1260, pass, Gap));
            Assert.Equal(2, deltas.CalculateDeltaWithInterval(TestServerId, "query_stats_exec", "0xSH1:100:240:0xPH1", 9, out var fellOutInterval, pass, Gap));
            Assert.InRange(fellOutInterval, 238, 242);
            Assert.Equal(2, deltas.CalculateDelta(TestServerId, "query_stats_exec", ":0:-1:", 5, pass, Gap));
            Assert.Equal(0, deltas.CalculateDelta(TestServerId, "query_stats_exec", "sh:0:0:ph", 5, pass, 0));
            Assert.Equal(0, deltas.CalculateDelta(TestServerId, "query_stats_exec", "sh:0:-1:ph", 5, pass, 0));

            /* The series-age rescue on the FIRST post-restart pass: the pass window is seeded from EVERY
               query_stats row, the pre-V128 ones included, so a plan compiled 30 s ago (inside the ~120 s
               since the last pre-restart pass) is credited in full with a real interval, while a plan older
               than that gap baselines honestly. Unseeded, both are (0, 0) — the defect #3614 closed and V128
               must not reopen on the first restart after the upgrade, when the window holds only such rows. */
            Assert.Equal(900, deltas.CalculateDeltaWithSeriesAge(TestServerId, "query_stats_worker", "sh:0:99:newplan", 900, 30, out var rescueInterval, pass, Gap));
            Assert.InRange(rescueInterval, 118, 122);
            Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(TestServerId, "query_stats_exec", "sh:0:99:oldplan", 900, 3_000, out var oldInterval, pass, Gap));
            Assert.Equal(0, oldInterval);

            /* An original family's pass window is seeded too, proven through the same rescue path. */
            Assert.Equal(77, deltas.CalculateDeltaWithSeriesAge(TestServerId, "wait_stats_tasks", "NEW_WAIT", 77, 10, out _, pass, Gap));

            /* The stale server: no key seed (first sighting even with the gap policy off) and no pass window. */
            Assert.Equal(0, deltas.CalculateDelta(StaleServerId, "latch_stats_wait_time", "BUFFER", 999, pass, 0));
            Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(StaleServerId, "query_stats_worker", "k", 900, 30, out _, pass, Gap));

            /* ClearServer drops both seeded halves — the re-add path both hosts now wire (#3540 A4). */
            deltas.ClearServer(TestServerId);
            Assert.Equal(0, deltas.CalculateDelta(TestServerId, "latch_stats_wait_time", "BUFFER", 1700, pass.AddSeconds(30), 0));
            Assert.Equal(0, deltas.CalculateDeltaWithSeriesAge(TestServerId, "query_stats_worker", "sh:0:99:another", 500, 5, out _, pass.AddSeconds(30), Gap));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteFamilyRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The per-key shape's bound, asked of the planner the way the #1772 pin asks it of the row-value
    /// shape: with one bound on one table read, DISTINCT ON over the window must still exclude the
    /// two-day-old chunk and read the window's. pg_statement_stats is the family with the most rows per
    /// pass on the dogfood fleet, so it is the one whose shape matters most; the other two per-key
    /// queries are the same shape over the same kind of index.
    /// </summary>
    [Fact]
    public async Task PerKeySeedRead_ExcludesChunksOlderThanTheWindow_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live chunk-exclusion test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, TestContext.Current.CancellationToken);
        Assert.SkipWhen(!timescaleEnabled, "TimescaleDB is not enabled on DARLING_TEST_PG — there are no chunks to exclude.");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            await DeleteFamilyRowsAsync(connection, TestContext.Current.CancellationToken);

            var insideWindow = Naive(DateTime.UtcNow.AddMinutes(-1));
            var twoDaysBack = Naive(DateTime.UtcNow.AddDays(-2));
            await PgStatementAsync(connection, insideWindow, 11, 16384, 10, true, 120, 1500.7, 60);
            await PgStatementAsync(connection, twoDaysBack, 11, 16384, 10, true, 1, 1, 1);

            var recentChunk = await ChunkHoldingRowAsync(connection, "pg_statement_stats", insideWindow);
            var oldChunk = await ChunkHoldingRowAsync(connection, "pg_statement_stats", twoDaysBack);

            Assert.SkipWhen(recentChunk is null || oldChunk is null, "the sentinel rows did not land — nothing to prove.");
            Assert.SkipWhen(string.Equals(recentChunk, oldChunk, StringComparison.Ordinal),
                $"pg_statement_stats put both timestamps in {recentChunk} — this store has no separate chunk to exclude.");

            var plan = await ExplainAsync(connection, DarlingDeltaCalculator.PgStatementStatsSeedSql, CollectorDeltaCalculator.SeedCutoff());

            Assert.DoesNotContain(oldChunk!, plan, StringComparison.Ordinal);
            Assert.Contains(recentChunk!, plan, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteFamilyRowsAsync(cleanup, cleanupCt));
        }
    }

    /* Naive-UTC storage by convention across the product; Npgsql 6+ rejects Kind=Utc against `timestamp`. */
    private static DateTime Naive(DateTime t) => DateTime.SpecifyKind(t, DateTimeKind.Unspecified);

    /// <summary>A second server whose only rows predate the lookback window.</summary>
    private const int StaleServerId = -545454;

    private static async Task LatchAsync(NpgsqlConnection connection, int serverId, DateTime t, long requests, long waitMs, long maxWaitMs)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms) " +
            "VALUES (1, $1, $2, 'delta-seed-e2e', 'BUFFER', $3, $4, $5)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(requests);
        cmd.Parameters.AddWithValue(waitMs);
        cmd.Parameters.AddWithValue(maxWaitMs);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task SpinlockAsync(NpgsqlConnection connection, DateTime t, long collisions, long spins, long sleepTime, long backoffs)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs) " +
            "VALUES (1, $1, $2, 'delta-seed-e2e', 'LOCK_HASH', $3, $4, 10.0, $5, $6)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(collisions);
        cmd.Parameters.AddWithValue(spins);
        cmd.Parameters.AddWithValue(sleepTime);
        cmd.Parameters.AddWithValue(backoffs);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Counters are multiples of the execution count so every group's expected delta is derivable.</summary>
    private static async Task ProcAsync(NpgsqlConnection connection, DateTime t, string? planHandle, string db, string schema, string obj, long executions)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, " +
            "execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_logical_writes, total_physical_reads, total_spills, plan_handle) " +
            "VALUES (1, $1, $2, 'delta-seed-e2e', $3, $4, $5, 'P', $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(db);
        cmd.Parameters.AddWithValue(schema);
        cmd.Parameters.AddWithValue(obj);
        cmd.Parameters.AddWithValue(executions);
        cmd.Parameters.AddWithValue(executions * 10);
        cmd.Parameters.AddWithValue(executions * 20);
        cmd.Parameters.AddWithValue(executions * 30);
        cmd.Parameters.AddWithValue(executions * 40);
        cmd.Parameters.AddWithValue(executions * 50);
        cmd.Parameters.AddWithValue(executions * 60);
        cmd.Parameters.AddWithValue((object?)planHandle ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task QueryStatsAsync(NpgsqlConnection connection, DateTime t)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, query_hash, sql_handle, plan_handle, execution_count) " +
            "VALUES (1, $1, $2, 'delta-seed-e2e', 'qh', 'sh', 'ph', 1)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(TestServerId);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>A V128 query_stats row (#3540): both offsets stored raw; the eight counters are multiples of the
    /// execution count so every group's expected delta is derivable.</summary>
    private static async Task KeyedQueryStatsAsync(NpgsqlConnection connection, DateTime t, string? sqlHandle, int start, int end, string? planHandle, long executions)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, query_hash, sql_handle, plan_handle, " +
            "statement_start_offset, statement_end_offset, " +
            "execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_logical_writes, total_physical_reads, total_rows, total_spills) " +
            "VALUES (1, $1, $2, 'delta-seed-e2e', 'qh', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue((object?)sqlHandle ?? DBNull.Value);
        cmd.Parameters.AddWithValue((object?)planHandle ?? DBNull.Value);
        cmd.Parameters.AddWithValue(start);
        cmd.Parameters.AddWithValue(end);
        cmd.Parameters.AddWithValue(executions);
        cmd.Parameters.AddWithValue(executions * 10);
        cmd.Parameters.AddWithValue(executions * 20);
        cmd.Parameters.AddWithValue(executions * 30);
        cmd.Parameters.AddWithValue(executions * 40);
        cmd.Parameters.AddWithValue(executions * 50);
        cmd.Parameters.AddWithValue(executions * 60);
        cmd.Parameters.AddWithValue(executions * 70);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task PgWaitAsync(NpgsqlConnection connection, DateTime t, long eventId, long waits, long waitTimeUs)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us) " +
            "VALUES (1, $1, $2, 'delta-seed-e2e', 1, $3, 'IPC', 'X', $4, $5)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(eventId);
        cmd.Parameters.AddWithValue(waits);
        cmd.Parameters.AddWithValue(waitTimeUs);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task PgStatementAsync(NpgsqlConnection connection, DateTime t, long queryId, long databaseId, long userId, bool topLevel, long calls, double totalMs, long rows)
    {
        using var cmd = new NpgsqlCommand(
            "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, calls, total_exec_time_ms, rows_returned) " +
            "VALUES (1, $1, $2, 'delta-seed-e2e', $3, $4, $5, $6, $7, $8, $9)", connection);
        cmd.Parameters.AddWithValue(t);
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(queryId);
        cmd.Parameters.AddWithValue(databaseId);
        cmd.Parameters.AddWithValue(userId);
        cmd.Parameters.AddWithValue(topLevel);
        cmd.Parameters.AddWithValue(calls);
        cmd.Parameters.AddWithValue(totalMs);
        cmd.Parameters.AddWithValue(rows);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteFamilyRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "wait_stats", "latch_stats", "spinlock_stats", "procedure_stats", "query_stats", "pg_wait_stats", "pg_statement_stats" })
        {
            using var cleanup = new NpgsqlCommand(
                $"DELETE FROM {table} WHERE server_id IN ({TestServerId}, {StaleServerId})", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// The chunk a planted row actually lives in. <c>tableoid</c> on a hypertable read resolves to the
    /// CHUNK, because the chunk is the table the row is stored in; on a plain table it resolves to the
    /// table itself, which is what lets the caller detect an unpartitioned store and skip. Returned
    /// unqualified, matching how EXPLAIN names relations.
    /// </summary>
    private static Task<string?> ChunkHoldingRowAsync(NpgsqlConnection connection, DateTime collectionTime)
        => ChunkHoldingRowAsync(connection, "wait_stats", collectionTime);

    private static async Task<string?> ChunkHoldingRowAsync(NpgsqlConnection connection, string table, DateTime collectionTime)
    {
        using var cmd = new NpgsqlCommand(
            $"SELECT tableoid::regclass::text FROM {table} WHERE server_id = $1 AND collection_time = $2", connection);
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
