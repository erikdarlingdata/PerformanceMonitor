/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9280: the hourly Query Store summary, <c>collect.query_store_top_hourly</c> (V296), built by the compose stamp's hourly pass. Behaviour on a
/// scratch store seeded by <see cref="ComposeStampLiveSupport"/>.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreTopHourlyLiveTests
{
    private static string At(DateTime h) => ComposeStampLiveSupport.At(h);

    private static Task InsertKeyedRowAsync(NpgsqlConnection connection, DateTime collectionTime, string firstExecution, long queryId, string executionCount, long interval, CancellationToken ct) =>
        ComposeStampLiveSupport.ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id)
VALUES (TIMESTAMP '{At(collectionTime)}', 1, 'dbN', {queryId}, {queryId}, 'Regular', {firstExecution}, TIMESTAMP '{At(collectionTime)}',
        NULL, NULL, {executionCount}, 1000, 500, 2000, 900, NULL, {interval})", ct);

    /* Rows of the hour, grouped as the read groups them, minus the summary's rows: both directions must be empty. */
    private static async Task<long> DifferenceAsync(NpgsqlConnection connection, CancellationToken ct) => await ComposeStampLiveSupport.CountAsync(connection, @"
WITH expected AS
(
    SELECT date_trunc('hour', w.collection_time) AS hour, w.server_id, w.database_name, w.query_id, w.plan_id, w.query_hash, w.execution_type_desc, w.replica_role, w.module_name,
           count(*) AS wide_rows, sum(w.execution_count) AS ec,
           sum(w.avg_duration_us::numeric * w.execution_count) AS d, sum(w.avg_cpu_time_us::numeric * w.execution_count) AS c,
           sum(w.avg_logical_io_reads::numeric * w.execution_count) AS r, sum(w.avg_logical_io_writes::numeric * w.execution_count) AS wr,
           sum(w.avg_physical_io_reads::numeric * w.execution_count) AS pr, sum(w.avg_rowcount::numeric * w.execution_count) AS rc,
           max(w.last_execution_time) AS le, max(w.query_plan_hash) AS ph
    FROM collect.query_store_interval_wide AS w
    WHERE date_trunc('hour', w.collection_time) IN (SELECT hour FROM collect.query_store_compose_stamp_hours)
    GROUP BY 1, w.server_id, w.database_name, w.query_id, w.plan_id, w.query_hash, w.execution_type_desc, w.replica_role, w.module_name
),
actual AS
(
    SELECT hour, server_id, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name, wide_rows, execution_count_sum,
           avg_duration_us_wsum, avg_cpu_time_us_wsum, avg_logical_io_reads_wsum, avg_logical_io_writes_wsum, avg_physical_io_reads_wsum, avg_rowcount_wsum,
           last_execution_time_max, query_plan_hash_max
    FROM collect.query_store_top_hourly
)
SELECT (SELECT count(*) FROM (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) AS a)
     + (SELECT count(*) FROM (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected) AS b)", ct);

    [Fact]
    public async Task ABuiltHourHoldsTheGroupByOfItsWideRows_AndEveryPairIsLevelWithTheLedger_AndALateWriteLeavesThePairStaleUntilTheNextTick()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-20);
            /* Two rows with an all-NULL key tail group together; a NULL execution_count; a first execution just inside the margin before the hour; a row whose
               server clock ran 15 hours fast (first execution after its collection time), which the build keeps. */
            await InsertKeyedRowAsync(connection, hour.AddMinutes(10), $"TIMESTAMP '{At(hour.AddMinutes(7))}'", 9001, "5", 9_100_001, ct);
            await InsertKeyedRowAsync(connection, hour.AddMinutes(20), $"TIMESTAMP '{At(hour.AddMinutes(17))}'", 9001, "6", 9_100_002, ct);
            await InsertKeyedRowAsync(connection, hour.AddMinutes(30), $"TIMESTAMP '{At(hour.AddMinutes(27))}'", 9002, "NULL", 9_100_003, ct);
            await InsertKeyedRowAsync(connection, hour.AddMinutes(40), $"TIMESTAMP '{At(hour)}' - {QueryStoreIntervalWide.PurgeEdgeMarginSql} + interval '1 minute'", 9003, "3", 9_100_004, ct);
            await InsertKeyedRowAsync(connection, hour.AddMinutes(50), $"TIMESTAMP '{At(hour.AddMinutes(50))}' + interval '15 hours'", 9004, "4", 9_100_005, ct);
            Assert.Equal(ComposeStampLiveSupport.BuiltHours, await ComposeStampLiveSupport.BuildAllAsync(source, ct, ComposeStampLiveSupport.ClockWithin(hourNow)));

            Assert.Equal(0, await DifferenceAsync(connection, ct));
            Assert.Equal(2, await ComposeStampLiveSupport.CountAsync(connection, "SELECT wide_rows FROM collect.query_store_top_hourly WHERE query_id = 9001", ct));
            foreach (var queryId in new[] { 9002, 9003, 9004 })
            {
                Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_top_hourly WHERE query_id = {queryId}", ct));
            }

            /* Every pair of a built hour is level for every table. */
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE top_built_seq IS DISTINCT FROM built_seq OR built_seq IS DISTINCT FROM late_seq", ct));

            /* A late write into a built hour: the pair is stale for the top table, its rows are short of the wide table, and the next tick rebuilds the hour. */
            await ComposeStampLiveSupport.InsertRowAsync(connection, 1, hour.AddMinutes(55), 9_200_001, ct);
            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE top_built_seq IS DISTINCT FROM late_seq", ct));
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_top_hourly WHERE query_hash = 'hashX'", ct));
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE top_built_seq IS DISTINCT FROM late_seq", ct));
            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_top_hourly WHERE query_hash = 'hashX'", ct));
            Assert.Equal(0, await DifferenceAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task HoursBuiltBeforeTheRung_PlanAsTopOnlyNewestFirstInsideTheBacklogCaps_AndTheTopOnlyBuildRewritesNoStampHashOrFleetRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ComposeStampLiveSupport.ExecAsync(connection, "UPDATE collect.query_store_compose_stamp_built SET top_built_seq = NULL; DELETE FROM collect.query_store_top_hourly", ct);
            var clock = ComposeStampLiveSupport.ClockWithin(hourNow);
            var plan = await QueryStoreComposeStamp.PlanAsync(connection, clock, ComposeStampLiveSupport.RetentionDays, ct, clock);
            var all = plan.Builds.Concat(plan.ExtraBacklog ?? Array.Empty<QueryStoreComposeStamp.Build>()).ToList();
            Assert.All(all, b => Assert.True(b.TopOnly && b.Stale && b.Backlog && !b.FleetOnly));
            Assert.Equal(QueryStoreComposeStamp.MaxBuildsPerTick, plan.Builds.Count);
            Assert.Equal(ComposeStampLiveSupport.ExtraBacklogHours, plan.ExtraBacklog!.Count);
            Assert.Equal(all.OrderByDescending(b => b.Hour).Select(b => b.Hour), all.Select(b => b.Hour));

            var hour = hourNow.AddHours(-10);
            var where = $"WHERE collection_time >= TIMESTAMP '{At(hour)}' AND collection_time < TIMESTAMP '{At(hour.AddHours(1))}'";
            var before = await ComposeStampLiveSupport.TextAsync(connection,
                $"SELECT (SELECT string_agg(xmin::text, ',' ORDER BY ctid) FROM collect.query_store_compose_stamp {where}) || '|' || (SELECT string_agg(xmin::text, ',' ORDER BY ctid) FROM collect.query_store_compose_fleet {where}) || '|' || (SELECT string_agg(xmin::text, ',' ORDER BY ctid) FROM collect.query_store_hash_hourly WHERE hour = TIMESTAMP '{At(hour)}')", ct);
            Assert.NotNull(await QueryStoreComposeStamp.BuildTopHourAsync(connection, hour, ct));
            var after = await ComposeStampLiveSupport.TextAsync(connection,
                $"SELECT (SELECT string_agg(xmin::text, ',' ORDER BY ctid) FROM collect.query_store_compose_stamp {where}) || '|' || (SELECT string_agg(xmin::text, ',' ORDER BY ctid) FROM collect.query_store_compose_fleet {where}) || '|' || (SELECT string_agg(xmin::text, ',' ORDER BY ctid) FROM collect.query_store_hash_hourly WHERE hour = TIMESTAMP '{At(hour)}')", ct);
            Assert.Equal(before, after);
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_top_hourly WHERE hour = TIMESTAMP '{At(hour)}'", ct) > 0);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{At(hour)}' AND top_built_seq IS DISTINCT FROM late_seq", ct));

            /* The ticks backfill every other hour, and the summary then equals the wide table's grouping. */
            Assert.True(await ComposeStampLiveSupport.BuildAllAsync(source, ct, clock) > 0);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE top_built_seq IS DISTINCT FROM late_seq", ct));
            Assert.Equal(0, await DifferenceAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task AFailingTopInsert_DoesNotFailTheHour_TheStampHourBuilds_TopBuiltSeqStaysUnset_AndTheTopOnlyPassFillsItLater()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-12);
            var stamp = $"SELECT count(*) FROM collect.query_store_compose_stamp WHERE collection_time >= TIMESTAMP '{At(hour)}' AND collection_time < TIMESTAMP '{At(hour.AddHours(1))}'";
            var rows = await ComposeStampLiveSupport.CountAsync(connection, stamp, ct);
            await ComposeStampLiveSupport.ExecAsync(connection, @"
CREATE OR REPLACE FUNCTION collect.fail_9280() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    RAISE EXCEPTION 'planted top insert failure 9280';
END
$f$;
CREATE TRIGGER trg_fail_9280 BEFORE INSERT ON collect.query_store_top_hourly FOR EACH ROW EXECUTE FUNCTION collect.fail_9280();", ct);

            /* A late write makes the hour stale, so the tick runs the full build, whose top insert fails inside its savepoint. */
            await ComposeStampLiveSupport.InsertRowAsync(connection, 1, hour.AddMinutes(55), 9_300_001, ct);
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, ComposeStampLiveSupport.ClockWithin(hourNow), ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_failures WHERE hour = TIMESTAMP '{At(hour)}'", ct));
            Assert.Equal(rows + 1, await ComposeStampLiveSupport.CountAsync(connection, stamp, ct));
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{At(hour)}' AND built_seq IS DISTINCT FROM late_seq", ct));
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE hour = TIMESTAMP '{At(hour)}' AND top_built_seq IS NOT DISTINCT FROM late_seq", ct) == 0);
            /* The savepoint took the failed delete and insert back: the hour still holds its old top rows, without the late row, and the NULL ledger value says not to trust them. */
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_top_hourly WHERE hour = TIMESTAMP '{At(hour)}' AND query_hash = 'hashX'", ct));

            /* Once the failure is gone the top-only pass fills the hour. */
            await ComposeStampLiveSupport.ExecAsync(connection, "DROP TRIGGER trg_fail_9280 ON collect.query_store_top_hourly", ct);
            Assert.True(await ComposeStampLiveSupport.BuildAllAsync(source, ct, ComposeStampLiveSupport.ClockWithin(hourNow)) > 0);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_built WHERE top_built_seq IS DISTINCT FROM late_seq", ct));
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_top_hourly WHERE hour = TIMESTAMP '{At(hour)}'", ct) > 0);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheCleanupRemovesTopRowsBelowTheFloor_IncludingTheRowsOfAnHourWithNoHoursRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var clock = ComposeStampLiveSupport.ClockWithin(hourNow);
            var floor = QueryStoreComposeStamp.FloorHour(clock, ComposeStampLiveSupport.RetentionDays);
            await ComposeStampLiveSupport.ExecAsync(connection, $@"
INSERT INTO collect.query_store_top_hourly (hour, server_id, wide_rows) VALUES
(TIMESTAMP '{At(floor.AddHours(-5))}', 1, 1), (TIMESTAMP '{At(floor.AddHours(-1))}', 1, 1), (TIMESTAMP '{At(floor)}', 1, 1)", ct);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour < TIMESTAMP '{At(floor)}'", ct));
            var tick = await ComposeStampLiveSupport.TickAtAsync(source, clock, ct);
            Assert.Equal(0, tick.Failed);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_top_hourly WHERE hour < TIMESTAMP '{At(floor)}'", ct));
            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, $"SELECT count(*) FROM collect.query_store_top_hourly WHERE hour = TIMESTAMP '{At(floor)}' AND wide_rows = 1 AND query_id IS NULL", ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheRungReRunsOnTheStore_AndTheRewindRestoresThePreRungShape()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, _) = await ComposeStampLiveSupport.ArrangeAsync(ct, build: false);
        var bodySucceeded = false;
        try
        {
            var rung = PgMigrations.Scripts.Single(m => m.Version == QueryStoreComposeStamp.TopHourlyRungVersion);
            await ComposeStampLiveSupport.ExecAsync(connection, rung.Sql, ct);   /* idempotent on a store that has it */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 294, ct);
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM pg_class WHERE relname = 'ix_query_store_top_hourly_hour_server'", ct));
            Assert.Equal(0, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'query_store_compose_stamp_built' AND column_name = 'top_built_seq'", ct));
            await ComposeStampLiveSupport.ExecAsync(connection, rung.Sql, ct);
            Assert.Equal(1, await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM pg_class WHERE relname = 'ix_query_store_top_hourly_hour_server'", ct));
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
