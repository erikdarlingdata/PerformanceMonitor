/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */

/// <summary>
/// #9274: a rollup chunk whose one autovacuum ran beside an older open snapshot keeps zero all-visible pages, and nothing brings autovacuum back,
/// so every index-only scan of it fetches each row from the heap. The hourly store-maintenance step vacuums such a chunk and leaves a settled
/// one alone.
/// </summary>
public sealed class RollupChunkVisibilityLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9274 rollup visibility tests.";
    private const string Hash = "0xQS9274";
    private const int ServerCount = 12;
    private const int Groups = 300;

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task Refresh(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> Scalar<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The heap fetches an index-only read of the daily rollup for the hash makes (the sum over every <c>Index Only Scan</c> node), with the other scan
    /// types off so the read cannot go around the covering index.</summary>
    private static async Task<long> HeapFetchesAsync(NpgsqlConnection c, DateTime from, DateTime to, CancellationToken ct)
    {
        await Exec(c, "SET enable_seqscan = off; SET enable_bitmapscan = off", ct);
        try
        {
            await using var command = new NpgsqlCommand(
                $"EXPLAIN (ANALYZE, FORMAT JSON) SELECT sum(execution_count_sum) FROM collect.{TimescaleSupport.QueryStoreStatsCorrectedDailyView} WHERE query_hash = $1 AND bucket >= $2 AND bucket < $3", c);
            command.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
            using var doc = JsonDocument.Parse((string)(await command.ExecuteScalarAsync(ct))!);
            var scans = 0;
            var fetches = Sum(doc.RootElement[0].GetProperty("Plan"), ref scans);
            Assert.True(scans > 0, "the read made no index-only scan of the daily rollup, so it measures nothing");
            return fetches;
        }
        finally
        {
            await Exec(c, "RESET enable_seqscan; RESET enable_bitmapscan", ct);
        }
    }

    private static long Sum(JsonElement node, ref int scans)
    {
        var total = 0L;
        if (node.GetProperty("Node Type").GetString() == "Index Only Scan")
        {
            scans++;
            total += node.GetProperty("Heap Fetches").GetInt64();
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                total += Sum(child, ref scans);
            }
        }

        return total;
    }

    private sealed record ChunkState(string Name, int Pages, int AllVisible, DateTime? LastVacuum);

    private static async Task<ChunkState> DailyChunkAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await Exec(c, "SELECT pg_stat_force_next_flush()", ct);
        await using var command = new NpgsqlCommand(
            @"SELECT format('%I.%I', ch.chunk_schema, ch.chunk_name), c.relpages, c.relallvisible, greatest(st.last_vacuum, st.last_autovacuum)
              FROM timescaledb_information.continuous_aggregates ca
              JOIN timescaledb_information.chunks ch ON ch.hypertable_schema = ca.materialization_hypertable_schema AND ch.hypertable_name = ca.materialization_hypertable_name
              JOIN pg_class c ON c.relname = ch.chunk_name AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = ch.chunk_schema)
              JOIN pg_stat_all_tables st ON st.relid = c.oid
              WHERE ca.view_name = $1", c);
        command.Parameters.AddWithValue(TimescaleSupport.QueryStoreStatsCorrectedDailyView);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the daily rollup has no chunk");
        var state = new ChunkState(reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetDateTime(3));
        Assert.False(await reader.ReadAsync(ct), "the seeded daily rollup is expected to be one chunk");
        return state;
    }

    /// <summary>How many chunks the step's catalog read lists right now, with no minimum age.</summary>
    private static async Task<int> LaggingAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await Exec(c, "SELECT pg_stat_force_next_flush()", ct);
        await using var command = new NpgsqlCommand(TimescaleSupport.RollupChunkVisibilityLagSql, c);
        command.Parameters.AddWithValue(NpgsqlDbType.Interval, TimeSpan.Zero);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = 0;
        while (await reader.ReadAsync(ct))
        {
            rows++;
        }

        return rows;
    }

    private sealed record Seeded(DateTime From, DateTime To, string Daily, string Hourly);

    /// <summary>
    /// Migrates the scratch store, builds the aggregates and seeds a week of Query Store rows. The daily rollup's one chunk is then in the stuck state: written
    /// while an older snapshot is open and vacuumed once, so no page is all-visible and no insert is counted since that vacuum. With <paramref name="stuckHourly"/>
    /// the hourly rollup's chunks are put in the same state, so several chunks lag.
    /// </summary>
    private static async Task<Seeded> SeedStuckAsync(NpgsqlConnection c, ScratchPostgres scratch, bool stuckHourly, CancellationToken ct)
    {
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(c, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #9274 tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(c, null, ct));
        await Exec(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(c, null, ct);
        await Exec(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        var end = DateTime.SpecifyKind(LiveClock.Now().Date.AddDays(-8).AddHours(12), DateTimeKind.Unspecified);
        var from = end.AddHours(-167).Date;
        var to = end.Date;
        for (var i = 1; i <= ServerCount; i++)
        {
            await DarlingMcpTestData.RegisterServerAsync(c, -927400 - i, "p9274q-" + i, ct);
        }

        await Exec(c, $@"INSERT INTO collect.query_store_stats
                  (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
                   runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
                SELECT 900000000::bigint + row_number() OVER (), h.ts + interval '10 minutes', -927400 - s, 'p9274q-' || s, 'db' || (g % 10), 'dbo.P',
                  CASE WHEN g = 1 THEN '{Hash}' ELSE '0x' || g END, g, g, 'Regular', 'PRIMARY', g, h.ts, h.ts, 1 + (extract(hour FROM h.ts))::int % 5, 2000 + g, 1000 + g, 900, 400, h.ts + interval '1 hour'
                FROM generate_series(TIMESTAMP '{from:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{end.AddHours(-1):yyyy-MM-dd HH:mm:ss}', interval '1 hour') AS h(ts)
                CROSS JOIN generate_series(1, {ServerCount}) s CROSS JOIN generate_series(1, {Groups}) g
                WHERE extract(hour FROM h.ts)::int % 6 = 0 OR g = 1", ct);
        if (!stuckHourly)
        {
            await Refresh(c, TimescaleSupport.QueryStoreStatsIntervalHourlyView, from.AddDays(-1), end, ct);
        }

        /* The rollups' chunks stay out of autovacuum so only the vacuums this test issues touch them. */
        var daily = await Scalar<string>(c,
            $"SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates WHERE view_name = '{TimescaleSupport.QueryStoreStatsCorrectedDailyView}'", ct);
        var hourly = await Scalar<string>(c,
            $"SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates WHERE view_name = '{TimescaleSupport.QueryStoreStatsIntervalHourlyView}'", ct);
        await Exec(c, $"ALTER TABLE {daily} SET (autovacuum_enabled = false)", ct);
        if (stuckHourly)
        {
            await Exec(c, $"ALTER TABLE {hourly} SET (autovacuum_enabled = false)", ct);
        }

        /* The stuck state: a snapshot that began before the refresh commits is still open when the one vacuum runs, so that vacuum marks no page. */
        await using (var snapshot = new NpgsqlConnection(scratch.ConnectionString))
        {
            await snapshot.OpenAsync(ct);
            await using var tx = await snapshot.BeginTransactionAsync(IsolationLevel.RepeatableRead, ct);
            _ = await Scalar<long>(snapshot, "SELECT count(*) FROM servers", ct);
            if (stuckHourly)
            {
                await Refresh(c, TimescaleSupport.QueryStoreStatsIntervalHourlyView, from.AddDays(-1), end, ct);
            }

            await Refresh(c, TimescaleSupport.QueryStoreStatsCorrectedDailyView, from, to, ct);
            await Exec(c, $"VACUUM {daily}", ct);
            if (stuckHourly)
            {
                await Exec(c, $"VACUUM {hourly}", ct);
            }

            await tx.CommitAsync(ct);
        }

        return new Seeded(from, to, daily, hourly);
    }

    private static async Task<string> ShowAsync(NpgsqlConnection c, string setting, CancellationToken ct) =>
        await Scalar<string>(c, $"SHOW {setting}", ct);

    /// <summary>
    /// A daily chunk written while an older snapshot is open and vacuumed once (the stuck state: no page all-visible, no insert since that vacuum) is
    /// vacuumed by the step once the snapshot is closed, ends all-visible, and the index-only read then makes no heap fetch. A step run again on the settled
    /// chunk issues no VACUUM (the chunk's last vacuum time does not move). A chunk whose last vacuum is younger than the minimum age is left alone.
    /// Red on a store with no step: the chunk stays at no all-visible page and the read keeps fetching from the heap.
    /// </summary>
    [Fact]
    public async Task StuckDailyChunk_IsVacuumedByTheStep_AndASettledOneIsLeftAlone()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var scratchHandle = scratch;
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var seeded = await SeedStuckAsync(c, scratch, stuckHourly: false, ct);
            var from = seeded.From;
            var to = seeded.To;

            var stuck = await DailyChunkAsync(c, ct);
            Assert.True(stuck.Pages >= TimescaleSupport.RollupVacuumMinPages, $"the seeded chunk has only {stuck.Pages} pages, under the step's floor");
            Assert.Equal(0, stuck.AllVisible);
            Assert.True(await HeapFetchesAsync(c, from, to, ct) > 0, "the stuck chunk's index-only read made no heap fetch, so the seed is not the stuck state");

            /* Just vacuumed, so the default minimum age leaves it for a later pass. */
            Assert.Equal(0, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, cancellationToken: ct));
            Assert.Equal(0, (await DailyChunkAsync(c, ct)).AllVisible);

            Assert.Equal(1, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct));
            var settled = await DailyChunkAsync(c, ct);
            Assert.Equal(settled.Pages, settled.AllVisible);
            Assert.Equal(0, await HeapFetchesAsync(c, from, to, ct));

            /* Settled: no chunk lags, so no VACUUM is issued (the last vacuum time is the same). */
            Assert.Equal(0, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct));
            Assert.Equal(settled.LastVacuum, (await DailyChunkAsync(c, ct)).LastVacuum);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A chunk another session holds locked is skipped by VACUUM (SKIP_LOCKED warns and succeeds): the step does not count it, it keeps lagging, and the
    /// next pass after the lock is released vacuums it. While the loop runs the session carries autovacuum's cost delay; after the step, whether it
    /// returned or was cancelled, the session's vacuum_cost_delay and vacuum_cost_limit are back to what they were. Red on a step that counts every command it
    /// issues and never sets or resets a cost limit.
    /// </summary>
    [Fact]
    public async Task ALockedChunk_IsNotCounted_AndTheSessionIsPacedLikeAutovacuumOnlyWhileTheStepRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var scratchHandle = scratch;
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await SeedStuckAsync(c, scratch, stuckHourly: false, ct);
            var stuck = await DailyChunkAsync(c, ct);
            Assert.Equal(0, stuck.AllVisible);

            var delayBefore = await ShowAsync(c, "vacuum_cost_delay", ct);
            var limitBefore = await ShowAsync(c, "vacuum_cost_limit", ct);
            var autovacuumDelay = await ShowAsync(c, "autovacuum_vacuum_cost_delay", ct);
            Assert.NotEqual(delayBefore, autovacuumDelay);

            /* The clock the step asks before each vacuum is read on its own connection, which is idle at that point. */
            var during = new List<(string Delay, string Limit)>();
            TimeSpan Observe()
            {
                using var delay = new NpgsqlCommand("SHOW vacuum_cost_delay", c);
                using var limit = new NpgsqlCommand("SHOW vacuum_cost_limit", c);
                during.Add(((string)delay.ExecuteScalar()!, (string)limit.ExecuteScalar()!));
                return TimeSpan.Zero;
            }

            await using (var locker = new NpgsqlConnection(scratch.ConnectionString))
            {
                await locker.OpenAsync(ct);
                await using var tx = await locker.BeginTransactionAsync(ct);
                await Exec(locker, $"LOCK TABLE {stuck.Name} IN SHARE UPDATE EXCLUSIVE MODE", ct);

                Assert.Equal(0, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct, Observe));
                var locked = await DailyChunkAsync(c, ct);
                Assert.Equal(0, locked.AllVisible);
                Assert.Equal(stuck.LastVacuum, locked.LastVacuum);
                Assert.Equal(1, await LaggingAsync(c, ct));
                Assert.Equal(new[] { (autovacuumDelay, limitBefore) }, during);
                Assert.Equal(delayBefore, await ShowAsync(c, "vacuum_cost_delay", ct));
                Assert.Equal(limitBefore, await ShowAsync(c, "vacuum_cost_limit", ct));

                /* A step cancelled by its budget while it runs: the exception reaches the caller and the session is still reset. */
                using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => TimescaleSupport.EnsureRollupChunkVisibilityAsync(
                    c, null, TimeSpan.Zero, cancel.Token, () => { cancel.Cancel(); return TimeSpan.Zero; }));
                Assert.Equal(delayBefore, await ShowAsync(c, "vacuum_cost_delay", ct));
                Assert.Equal(limitBefore, await ShowAsync(c, "vacuum_cost_limit", ct));

                await tx.CommitAsync(ct);
            }

            Assert.Equal(1, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct));
            var settled = await DailyChunkAsync(c, ct);
            Assert.Equal(settled.Pages, settled.AllVisible);
            Assert.Equal(delayBefore, await ShowAsync(c, "vacuum_cost_delay", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Two or more lagging chunks and a step budget that runs out after the first vacuum: exactly one is vacuumed, the rest still lag, and the next pass
    /// takes them. Red on a step that has no budget.
    /// </summary>
    [Fact]
    public async Task AStepBudgetThatRunsOut_VacuumsOneChunk_AndTheNextPassTakesTheRest()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var scratchHandle = scratch;
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await SeedStuckAsync(c, scratch, stuckHourly: true, ct);
            var lagging = await LaggingAsync(c, ct);
            Assert.True(lagging >= 2, $"the seed left {lagging} lagging chunk(s), and the budget needs at least two");

            /* The budget has time for the first vacuum and none after it. */
            var asked = 0;
            TimeSpan Clock() => asked++ == 0 ? TimeSpan.Zero : TimescaleSupport.RollupVacuumStepBudget;
            Assert.Equal(1, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct, Clock));
            var left = await LaggingAsync(c, ct);
            Assert.True(left >= lagging - 1 && left >= 1, $"{lagging} chunk(s) lagged and {left} still do after a one-vacuum pass");

            /* The next pass, with its whole budget, takes the chunks the first one left (the read lists at most RollupVacuumMaxChunksPerPass). */
            Assert.Equal(left, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct));
            for (var pass = 0; pass < 3 && await LaggingAsync(c, ct) > 0; pass++)
            {
                Assert.True(await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct) > 0);
            }

            Assert.Equal(0, await LaggingAsync(c, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A lagging chunk with inserts counted since its vacuum is listed once its range ends before its aggregate's refresh window starts (no refresh can
    /// write it again, so the inserts are the refresh's own and below autovacuum's insert trigger), and is not listed while it is inside the window.
    /// Red on a catalog read that lists only chunks with no insert since the vacuum.
    /// </summary>
    [Fact]
    public async Task AChunkWithInsertsIsListedOnlyPastItsAggregatesRefreshWindow()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var scratchHandle = scratch;
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var seeded = await SeedStuckAsync(c, scratch, stuckHourly: false, ct);
            Assert.Equal(1, await LaggingAsync(c, ct));

            /* An insert since the last vacuum: the pages still lag (the page counts move only with a vacuum). */
            await Exec(c, $"INSERT INTO {seeded.Daily} SELECT * FROM {seeded.Daily} LIMIT 1", ct);
            var inserts = await Scalar<long>(c, $"SELECT n_ins_since_vacuum FROM pg_stat_all_tables WHERE relid = '{(await DailyChunkAsync(c, ct)).Name}'::regclass", ct);
            Assert.True(inserts > 0, "the chunk shows no insert since its vacuum, so the seed does not test the refresh window");

            var jobId = await Scalar<int>(c,
                $"SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_name = (SELECT materialization_hypertable_name FROM timescaledb_information.continuous_aggregates WHERE view_name = '{TimescaleSupport.QueryStoreStatsCorrectedDailyView}')", ct);

            /* Inside the window: the refresh could still write the chunk. */
            await Exec(c, $"SELECT alter_job({jobId}, config => jsonb_set((SELECT config FROM timescaledb_information.jobs WHERE job_id = {jobId}), '{{start_offset}}', to_jsonb('3650 days'::text)))", ct);
            Assert.Equal(0, await LaggingAsync(c, ct));
            Assert.Equal(0, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct));

            /* Past the window: its range ended before the window starts. */
            await Exec(c, $"SELECT alter_job({jobId}, config => jsonb_set((SELECT config FROM timescaledb_information.jobs WHERE job_id = {jobId}), '{{start_offset}}', to_jsonb('3 days'::text)))", ct);
            Assert.True(
                await Scalar<bool>(c, $"SELECT range_end < now() - interval '3 days' FROM timescaledb_information.chunks WHERE format('%I.%I', chunk_schema, chunk_name) = '{(await DailyChunkAsync(c, ct)).Name}'", ct),
                "the seeded chunk's range ends inside a 3-day window, so it cannot be past it");
            Assert.Equal(1, await LaggingAsync(c, ct));
            Assert.Equal(1, await TimescaleSupport.EnsureRollupChunkVisibilityAsync(c, null, TimeSpan.Zero, ct));
            var settled = await DailyChunkAsync(c, ct);
            Assert.Equal(settled.Pages, settled.AllVisible);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
