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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6800 part 2, step 1: V221's <c>collect.query_store_hash_hourly</c> against a real PostgreSQL store (<c>DARLING_TEST_PG</c>). The
/// hourly builder fills it in the stamp's transaction, and its ledger column <c>hash_built_seq</c> says whether a pair's rows are
/// valid. The seed is the shared 30-hour one (three servers, NULL hashes, Aborted rows) with the three measures the stamp does not
/// carry (logical reads, logical writes, memory) added. Facts: a built hour's rows equal a direct GROUP BY of the wide rows, also
/// after an interval moved between hours; a late write makes the pair stale and the next tick rebuilds it; an hour built before V221
/// plans as stale (newest first) and the ordinary ticks backfill it; a build that wrote <c>built_seq</c> only leaves the pair stale
/// for this table; the cleanup removes the table's hours below the floor; the rung is idempotent and rewrites no row.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreHashHourlyLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the V221 hash-hourly facts.";

    private static string At(DateTime value) => ComposeStampLiveSupport.At(value);

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        ComposeStampLiveSupport.ExecAsync(connection, sql, ct);

    private static Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        ComposeStampLiveSupport.CountAsync(connection, sql, ct);

    /// <summary>
    /// The shared 30-hour seed, built hour by hour (28 hours), with logical reads, logical writes and memory added to every wide row
    /// first. The values are small so no product passes a bigint, and some are NULL, so a SUM over a NULL measure is exercised. An
    /// interval that crosses the top of an hour is in the seed (its first_execution_time is 3 minutes before the stamp).
    /// </summary>
    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection, NpgsqlDataSource Source, DateTime HourNow)> ArrangeAsync(CancellationToken ct)
    {
        var arranged = await ComposeStampLiveSupport.ArrangeAsync(ct, build: false);
        await ExecAsync(arranged.Connection, @"
UPDATE collect.query_store_interval_wide
SET avg_logical_io_reads = CASE WHEN query_id % 4 = 0 THEN NULL ELSE ((query_id * 7 + plan_id) % 1000) + 1 END,
    avg_logical_io_writes = CASE WHEN query_id % 3 = 0 THEN NULL ELSE query_id * 5 + (plan_id % 7) END,
    max_query_max_used_memory = CASE WHEN query_id % 5 = 0 THEN NULL ELSE (runtime_stats_interval_id % 5000) + 1 END", ct);
        Assert.Equal(28, await ComposeStampLiveSupport.BuildAllAsync(arranged.Source, ct, ComposeStampLiveSupport.ClockWithin(arranged.HourNow)));
        return arranged;
    }

    /// <summary>
    /// Rows where the table and a direct GROUP BY of the wide rows differ, over the hours the builder has done. The direct side is
    /// written out here, not read from the builder's text. EXCEPT both ways, so a missing row, an extra row and a different sum all count,
    /// and a NULL query_hash matches a NULL query_hash.
    /// </summary>
    private static Task<long> MismatchesAsync(NpgsqlConnection connection, CancellationToken ct) => CountAsync(connection, @"
WITH direct AS
(
    SELECT date_trunc('hour', w.collection_time) AS hour, count(*) AS wide_rows, max(w.max_query_max_used_memory) AS mem_max, w.server_id, w.query_hash,
           sum(w.execution_count) AS ec_sum, sum(w.avg_cpu_time_us * w.execution_count) AS cpu_wsum, sum(w.avg_duration_us * w.execution_count) AS dur_wsum,
           sum(w.avg_logical_io_reads * w.execution_count) AS reads_wsum, sum(w.avg_logical_io_writes * w.execution_count) AS writes_wsum
    FROM collect.query_store_interval_wide AS w
    WHERE date_trunc('hour', w.collection_time) IN (SELECT h.hour FROM collect.query_store_compose_stamp_hours AS h)
    GROUP BY 1, w.server_id, w.query_hash
),
stored AS
(
    SELECT hour, wide_rows, mem_max, server_id, query_hash, ec_sum, cpu_wsum, dur_wsum, reads_wsum, writes_wsum FROM collect.query_store_hash_hourly
)
SELECT (SELECT count(*) FROM (SELECT * FROM direct EXCEPT SELECT * FROM stored) AS a)
     + (SELECT count(*) FROM (SELECT * FROM stored EXCEPT SELECT * FROM direct) AS b)", ct);

    /// <summary>The pairs of the hours the builder has done whose ledger value differs from <c>late_seq</c>, the rows the plan counts as stale (a pair of an hour never built is not stale).</summary>
    private static Task<long> HashStalePairsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hash_built_seq IS DISTINCT FROM late_seq AND hour IN (SELECT h.hour FROM collect.query_store_compose_stamp_hours AS h)", ct);

    private static Task<long> CustomViewsStalePairsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE built_seq IS DISTINCT FROM late_seq AND hour IN (SELECT h.hour FROM collect.query_store_compose_stamp_hours AS h)", ct);

    private static Task<QueryStoreComposeStamp.Plan> PlanAsync(NpgsqlConnection connection, DateTime hourNow, CancellationToken ct) =>
        QueryStoreComposeStamp.PlanAsync(
            connection, ComposeStampLiveSupport.ClockWithin(hourNow), ComposeStampLiveSupport.RetentionDays, ct, ComposeStampLiveSupport.ClockWithin(hourNow));

    [Fact]
    public async Task ABuiltHoursRows_EqualADirectGroupByOfTheWideRows_AlsoAfterAnIntervalMovedBetweenHours()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Teeth: the compared side is not empty, covers all three servers and holds the NULL-hash groups, and the sums are not all one value. */
            /* 28 hours x 3 servers x 6 hashes (one of them NULL): the plans of a hash and the stamps of an hour are merged into one row. */
            Assert.Equal(28 * 3 * 6, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(3, await CountAsync(connection, "SELECT count(DISTINCT server_id) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(28 * 3, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly WHERE query_hash IS NULL", ct));
            Assert.True(await CountAsync(connection, "SELECT count(DISTINCT reads_wsum) FROM collect.query_store_hash_hourly", ct) > 10);
            Assert.True(await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly WHERE mem_max IS NULL", ct) > 0);
            Assert.Equal(28, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(0, await MismatchesAsync(connection, ct));
            Assert.Equal(0, await HashStalePairsAsync(connection, ct));
            Assert.Equal(0, await CustomViewsStalePairsAsync(connection, ct));

            /* Intervals that move between hours: server 2's stamps of one built hour are re-collected an hour later (the open interval's row is
               refreshed with a newer collection_time), and one row of server 1 moves back an hour. Both hours of each move go stale and a
               rebuild makes the table equal again. */
            var from = hourNow.AddHours(-12);
            await ExecAsync(connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time + interval '1 hour', avg_logical_io_reads = 4000
WHERE server_id = 2 AND query_id IN (1, 2) AND collection_time >= TIMESTAMP '{At(from)}' AND collection_time < TIMESTAMP '{At(from.AddHours(1))}'", ct);
            await ExecAsync(connection, $@"
UPDATE collect.query_store_interval_wide
SET collection_time = collection_time - interval '1 hour'
WHERE server_id = 1 AND query_id = 3 AND plan_id = 31 AND collection_time >= TIMESTAMP '{At(hourNow.AddHours(-20))}' AND collection_time < TIMESTAMP '{At(hourNow.AddHours(-20).AddMinutes(30))}'", ct);
            Assert.True(await MismatchesAsync(connection, ct) > 0, "the moves must change the direct side, or the rebuild proves nothing");
            Assert.Equal(4, await HashStalePairsAsync(connection, ct));

            Assert.Equal(4, await ComposeStampLiveSupport.BuildAllAsync(source, ct, ComposeStampLiveSupport.ClockWithin(hourNow)));
            Assert.Equal(0, await MismatchesAsync(connection, ct));
            Assert.Equal(0, await HashStalePairsAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ALateWrite_MarksThePairStaleForTheTable_AndTheNextTickRebuildsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-10);
            await ComposeStampLiveSupport.InsertRowAsync(connection, 2, hour.AddMinutes(41), 987654, ct);
            Assert.Equal(1, await HashStalePairsAsync(connection, ct));
            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly WHERE query_hash = 'hashX'", ct));
            Assert.True(await MismatchesAsync(connection, ct) > 0);

            var plan = await PlanAsync(connection, hourNow, ct);
            var planned = Assert.Single(plan.Builds);
            Assert.Equal(hour, planned.Hour);
            Assert.True(planned.Stale);

            var tick = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(1, tick.Built);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(1, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_hourly WHERE query_hash = 'hashX' AND server_id = 2 AND hour = TIMESTAMP '{At(hour)}' AND ec_sum = 7", ct));
            Assert.Equal(0, await MismatchesAsync(connection, ct));
            Assert.Equal(0, await HashStalePairsAsync(connection, ct));
            Assert.Empty((await PlanAsync(connection, hourNow, ct)).Builds);
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task HoursBuiltBeforeV221_PlanAsStale_NewestFirst_AndTheOrdinaryTicksBackfillThem()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* The state a store has right after V221 lands: every ledger row built, hash_built_seq NULL, the new table empty. */
            await ExecAsync(connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_hash_hourly", ct);
            Assert.Equal(0, await CustomViewsStalePairsAsync(connection, ct));
            Assert.Equal(28 * 3, await HashStalePairsAsync(connection, ct));

            var plan = await PlanAsync(connection, hourNow, ct);
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick, plan.Builds.Count);
            Assert.All(plan.Builds, build => Assert.True(build.Stale));
            Assert.Equal(
                Enumerable.Range(0, QueryStoreComposeStamp.MaxBuildsPerTick).Select(i => hourNow.AddHours(-3 - i)).ToArray(),
                plan.Builds.Select(build => build.Hour).OrderByDescending(hour => hour).ToArray());

            var tick = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            /* #6865: the six normal builds, then the extra backlog builds (up to 24 more): the 28 hours are done in one tick. */
            Assert.Equal(28, tick.Built);
            Assert.Equal(28, tick.BuiltBacklog);
            Assert.Equal(28, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(hourNow.AddHours(-3), (DateTime)(await ScalarAsync(connection, "SELECT max(hour) FROM collect.query_store_hash_hourly", ct))!);

            Assert.Equal(0, await ComposeStampLiveSupport.BuildAllAsync(source, ct, ComposeStampLiveSupport.ClockWithin(hourNow)));
            Assert.Equal(0, await HashStalePairsAsync(connection, ct));
            Assert.Equal(0, await MismatchesAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// #6865: with never-built hours waiting (a new hour is due every tick), the backlog of hours that are stale only for the hash table
    /// used to get half of the tick's six builds. Now the never-built hours build first, the backlog fills the rest and then keeps going
    /// (up to 24 extra hours) inside the tick's extra budget.
    /// </summary>
    [Fact]
    public async Task ATickWithNeverBuiltHoursWaiting_StillBuildsTheWholeBacklogItCanReach_NotHalfOfTheTick()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_hash_hourly", ct);
            /* The newest two hours were never built (the hour due this tick and the one before it). */
            var never = new[] { hourNow.AddHours(-3), hourNow.AddHours(-4) };
            foreach (var hour in never)
            {
                await ExecAsync(connection, $"DELETE FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{At(hour)}'", ct);
                await ExecAsync(connection, $"DELETE FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(hour)}'", ct);
            }

            var plan = await PlanAsync(connection, hourNow, ct);
            Assert.Equal(2, plan.Builds.Count(b => !b.Stale));
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick - 2, plan.Builds.Count(b => b.Backlog));
            Assert.Equal(22, plan.ExtraBacklog!.Count);

            var tick = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(2, tick.BuiltMissing);
            Assert.Equal(0, tick.BuiltStale);
            Assert.Equal(0, tick.Failed);
            /* 26 backlog hours: 4 in the normal builds and 22 extra, far above the 3 the old half-and-half split gave. */
            Assert.Equal(26, tick.BuiltBacklog);
            Assert.Equal(28, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(0, await HashStalePairsAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// #6865: a writer of the wide table that opens after the six normal builds holds the extra backlog builds back (its start time is not readable, so it
    /// holds every hour). Those extras are opportunistic: they are the next tick's backlog, so they are not "deferred" and not "deferred for writers". The
    /// plan held 6 hours and all 6 built, so <c>Deferred</c> is 0, not the 22 extras the writer held. The clock opens the writer the first time it sees six
    /// hours built, which is before the first extra starts.
    /// </summary>
    [Fact]
    public async Task AWriterThatOpensAfterTheNormalBuilds_HoldsOnlyExtras_SoDeferredStaysAtThePlansOwnZero()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        NpgsqlConnection? writer = null;
        NpgsqlTransaction? open = null;
        try
        {
            await ExecAsync(connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_hash_hourly", ct);
            var plan = await PlanAsync(connection, hourNow, ct);
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick, plan.Builds.Count);
            Assert.Equal(22, plan.ExtraBacklog!.Count);

            using var clockConnection = source.OpenConnection();
            TimeSpan Elapsed()
            {
                using var command = new NpgsqlCommand("SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", clockConnection);
                var built = Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
                if (built >= QueryStoreComposeStamp.MaxBuildsPerTick && writer is null)
                {
                    writer = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { ApplicationName = "writer_6865", Options = "-c track_activities=off" }.ConnectionString);
                    writer.Open();
                    open = writer.BeginTransaction();
                    var at = ComposeStampLiveSupport.At(hourNow.AddMinutes(-10));
                    using var insert = new NpgsqlCommand($@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, runtime_stats_interval_id)
VALUES (TIMESTAMP '{at}', 1, 'dbX', 99, 99, 'Regular', TIMESTAMP '{at}', TIMESTAMP '{at}', 'modX', 'hashX', 7, 1000, 500, 2000, 900, 9300001)", writer, open);
                    insert.ExecuteNonQuery();
                }

                return TimeSpan.Zero;
            }

            var now = ComposeStampLiveSupport.ClockWithin(hourNow);
            var tick = await QueryStoreComposeStamp.RunTickAsync(
                source, now, ComposeStampLiveSupport.RetentionDays, NullLogger.Instance, Elapsed, ct, new QueryStoreComposeStamp.TickOptions(StoreNow: now));

            Assert.NotNull(writer);
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick, tick.Built);
            Assert.Equal(tick.Built, tick.BuiltBacklog);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(0, tick.Deferred);
            Assert.Equal(0, tick.DeferredForWriters);
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            bodySucceeded = true;
        }
        finally
        {
            if (open is not null)
            {
                await open.RollbackAsync(ct);
            }

            if (writer is not null)
            {
                await writer.DisposeAsync();
            }

            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// A tick whose clock reads <paramref name="perBuild"/> for every hour already built into the hash table, so the elapsed time the tick sees is what
    /// its builds cost: the budget and the tick limit are reached by building, not by a call counter.
    /// </summary>
    private static async Task<QueryStoreComposeStamp.TickResult> TickWithBuildCostAsync(NpgsqlDataSource source, DateTime hourNow, TimeSpan perBuild, CancellationToken ct)
    {
        using var clockConnection = source.OpenConnection();
        TimeSpan Elapsed()
        {
            using var command = new NpgsqlCommand("SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", clockConnection);
            return perBuild * Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        }

        var now = ComposeStampLiveSupport.ClockWithin(hourNow);
        return await QueryStoreComposeStamp.RunTickAsync(
            source, now, ComposeStampLiveSupport.RetentionDays, NullLogger.Instance, Elapsed, ct, new QueryStoreComposeStamp.TickOptions(StoreNow: now));
    }

    /// <summary>
    /// #6865: the extra backlog builds stop at <see cref="QueryStoreComposeStamp.ExtraBacklogBudget"/> of their own time, not at the 24 hours the plan
    /// offers, and the tick stays inside <see cref="QueryStoreComposeStamp.MaxTickDuration"/>. Each build costs 40 s on the clock: the six normal builds
    /// take 240 s, the extras are measured from there, and the fifth extra is the first to find the 180 s used (the plan offers 22). Every normal hour
    /// started, so nothing is deferred: the extras the budget did not reach are the next tick's backlog, not deferred hours.
    /// </summary>
    [Fact]
    public async Task TheExtraBacklogBuilds_StopAtTheirBudget_InsideTheTickLimit_AndLeaveNothingDeferred()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_hash_hourly", ct);
            var plan = await PlanAsync(connection, hourNow, ct);
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick, plan.Builds.Count);
            Assert.Equal(22, plan.ExtraBacklog!.Count);

            var perBuild = TimeSpan.FromSeconds(40);
            var tick = await TickWithBuildCostAsync(source, hourNow, perBuild, ct);
            var extras = (int)Math.Ceiling(QueryStoreComposeStamp.ExtraBacklogBudget / perBuild);
            Assert.Equal(5, extras);
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick + extras, tick.Built);
            Assert.Equal(tick.Built, tick.BuiltBacklog);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(0, tick.Deferred);
            Assert.Equal(0, tick.DeferredForWriters);
            Assert.Equal(tick.Built, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            Assert.True(perBuild * tick.Built < QueryStoreComposeStamp.MaxTickDuration);
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// #6865: when the tick limit is reached inside the normal builds, no extra build starts, and <c>Deferred</c> counts the normal hours not started
    /// (2 of the 6), not the 22 extras that were never reached. Each build costs 150 s, so the fifth normal build finds 600 s used.
    /// </summary>
    [Fact]
    public async Task WhenTheTickLimitEndsTheNormalBuilds_NoExtraStarts_AndDeferredCountsOnlyThePlannedHoursNotStarted()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, "UPDATE collect.query_store_compose_stamp_built SET hash_built_seq = NULL", ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_hash_hourly", ct);
            var plan = await PlanAsync(connection, hourNow, ct);
            Assert.Equal(22, plan.ExtraBacklog!.Count);

            var perBuild = TimeSpan.FromSeconds(150);
            var tick = await TickWithBuildCostAsync(source, hourNow, perBuild, ct);
            var started = (int)Math.Ceiling(QueryStoreComposeStamp.MaxTickDuration / perBuild);
            Assert.Equal(4, started);
            Assert.Equal(started, tick.Built);
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick - started, tick.Deferred);
            Assert.Equal(0, tick.DeferredForWriters);
            Assert.Equal(started, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ABuildThatSetsBuiltSeqOnly_LeavesThePairStaleForTheTable_AndTheNextTickRebuildsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-7);
            await ComposeStampLiveSupport.InsertRowAsync(connection, 3, hour.AddMinutes(12), 987655, ct);
            Assert.Equal(1, await CustomViewsStalePairsAsync(connection, ct));

            /* A service host that predates V221 rebuilds the stamp hour and writes built_seq (the older upsert), never hash_built_seq. */
            await ExecAsync(connection, $"UPDATE collect.query_store_compose_stamp_built SET built_seq = late_seq WHERE server_id = 3 AND hour = TIMESTAMP '{At(hour)}'", ct);
            Assert.Equal(0, await CustomViewsStalePairsAsync(connection, ct));
            Assert.Equal(1, await HashStalePairsAsync(connection, ct));
            Assert.Equal(0, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly WHERE query_hash = 'hashX'", ct));

            var planned = Assert.Single((await PlanAsync(connection, hourNow, ct)).Builds);
            Assert.Equal(hour, planned.Hour);
            Assert.True(planned.Stale);

            Assert.Equal(1, (await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct)).Built);
            Assert.Equal(0, await HashStalePairsAsync(connection, ct));
            Assert.Equal(0, await MismatchesAsync(connection, ct));
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly WHERE query_hash = 'hashX' AND server_id = 3", ct));
            bodySucceeded = true;
        }
        finally
        {
            await source.DisposeAsync();
            await connection.DisposeAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TheCleanup_RemovesTheTablesHoursBelowTheFloor_AndKeepsTheRest()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var floor = QueryStoreComposeStamp.FloorHour(now, ComposeStampLiveSupport.RetentionDays);
            foreach (var hour in new[] { floor.AddHours(-2), floor.AddHours(-1), floor, floor.AddHours(1) })
            {
                await ExecAsync(connection, $@"
INSERT INTO collect.query_store_hash_hourly (hour, wide_rows, server_id, query_hash) VALUES (TIMESTAMP '{At(hour)}', 1, 1, 'a'), (TIMESTAMP '{At(hour)}', 1, 2, NULL);
INSERT INTO collect.query_store_compose_stamp_built (server_id, hour, late_seq, built_seq, hash_built_seq) VALUES (1, TIMESTAMP '{At(hour)}', 0, 0, 0), (2, TIMESTAMP '{At(hour)}', 0, 0, 0);
INSERT INTO collect.query_store_compose_stamp_hours (hour, built_at) VALUES (TIMESTAMP '{At(hour)}', now() AT TIME ZONE 'UTC')", ct);
            }

            Assert.Equal(2, await QueryStoreComposeStamp.GcAsync(connection, now, ComposeStampLiveSupport.RetentionDays, ct));
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_hourly WHERE hour < TIMESTAMP '{At(floor)}'", ct));
            Assert.Equal(4, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(2, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(0, await QueryStoreComposeStamp.GcAsync(connection, now, ComposeStampLiveSupport.RetentionDays, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// A build before V221 retires an hour from the hours table without touching the hash-hourly table (the rollback the admin guide
    /// documents), so after a roll forward the hours table no longer names the hour and a delete driven by the retired hours would keep its
    /// rows for good. The cleanup deletes by the floor: rows below it go whether or not the hours table still holds the hour.
    /// </summary>
    [Fact]
    public async Task TheCleanup_RemovesHashHourlyRowsBelowTheFloor_ForAnHourTheHoursTableNoLongerHolds()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var floor = QueryStoreComposeStamp.FloorHour(now, ComposeStampLiveSupport.RetentionDays);
            /* Hash-hourly rows for two hours below the floor and two at or above it, and hours-table rows only for the ones at or above:
               an older build retired the two below without deleting their hash rows. */
            foreach (var hour in new[] { floor.AddHours(-5), floor.AddHours(-3), floor, floor.AddHours(1) })
            {
                await ExecAsync(connection, $@"
INSERT INTO collect.query_store_hash_hourly (hour, wide_rows, server_id, query_hash) VALUES (TIMESTAMP '{At(hour)}', 1, 1, 'a'), (TIMESTAMP '{At(hour)}', 1, 2, NULL)", ct);
            }
            foreach (var hour in new[] { floor, floor.AddHours(1) })
            {
                await ExecAsync(connection, $"INSERT INTO collect.query_store_compose_stamp_hours (hour, built_at) VALUES (TIMESTAMP '{At(hour)}', now() AT TIME ZONE 'UTC')", ct);
            }

            Assert.Equal(0, await QueryStoreComposeStamp.GcAsync(connection, now, ComposeStampLiveSupport.RetentionDays, ct));
            Assert.Equal(0, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_hash_hourly WHERE hour < TIMESTAMP '{At(floor)}'", ct));
            Assert.Equal(4, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(2, await CountAsync(connection, "SELECT count(DISTINCT hour) FROM collect.query_store_hash_hourly", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheRung_RunsTwiceCleanly_ScansAndRewritesNothing_AndAddsANullableColumnWithNoDefault()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ComposeStampLiveSupport.BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, @"
INSERT INTO collect.query_store_compose_stamp_built (server_id, hour, late_seq, built_seq) VALUES (1, TIMESTAMP '2026-01-01 00:00:00', 0, 0);
INSERT INTO collect.query_store_hash_hourly (hour, wide_rows, server_id, query_hash, ec_sum) VALUES (TIMESTAMP '2026-01-01 00:00:00', 1, 1, 'a', 5)", ct);

            /* A store at V220 has the ledger without the column and no table: take the V221 objects away, then run the rung's text. */
            await ExecAsync(connection, "ALTER TABLE collect.query_store_compose_stamp_built DROP COLUMN hash_built_seq", ct);
            var relfilenodeBefore = await ScalarAsync(connection, "SELECT relfilenode FROM pg_class WHERE oid = 'collect.query_store_compose_stamp_built'::regclass", ct);
            await ExecAsync(connection, QueryStoreComposeStamp.HashHourlyCreateSql, ct);
            await ExecAsync(connection, QueryStoreComposeStamp.HashHourlyCreateSql, ct);
            Assert.Equal(relfilenodeBefore, await ScalarAsync(connection, "SELECT relfilenode FROM pg_class WHERE oid = 'collect.query_store_compose_stamp_built'::regclass", ct));

            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly", ct));
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'ix_query_store_hash_hourly_hour_server' AND indexdef LIKE '%(hour, server_id)'", ct));
            Assert.Equal(1, await CountAsync(connection, @"
SELECT count(*) FROM information_schema.columns
WHERE table_schema = 'collect' AND table_name = 'query_store_compose_stamp_built' AND column_name = 'hash_built_seq'
  AND data_type = 'bigint' AND is_nullable = 'YES' AND column_default IS NULL", ct));
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hash_built_seq IS NULL", ct));

            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(1, await CountAsync(connection, "SELECT count(*) FROM collect.query_store_hash_hourly", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }
}
