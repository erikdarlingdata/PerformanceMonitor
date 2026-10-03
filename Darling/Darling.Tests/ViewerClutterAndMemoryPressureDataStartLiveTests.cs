/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's Query Store Clutter panel and Memory Pressure Events chart against a real store (#4966). Each case runs the
/// calls the server tab makes (the surface's read, the data-start probe) and then the tab's own banner step on a real banner
/// control, and reads what the banner says.
///
/// <para><b>Query Store Clutter</b> reads <c>query_store_stats</c>, a raw relation the gated purge owns, so the probe's coverage is the
/// oldest row the server holds at or before the window's end (or the server's first collection when it holds none): a range that
/// reaches before it names it, and a range the store covers names nothing, whenever the first row of the range came.</para>
///
/// <para><b>Memory Pressure Events</b> is SPARSE (a server goes days with no pressure event), so its edge is the schedule's purge
/// cutoff, never its oldest row: a quiet week with one event at its end is covered and names nothing, where a walk to the oldest row
/// would name that event as if the store began there. The chart filters on each event's own time, which can come before the
/// server's first collection (the ring buffer's history), and the note names that event.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. The banner's time text reads the process-wide
   display mode, which QueryGridSeed.ReadBanner sets and restores; the viewer-time-statics collection serializes that with every
   other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerClutterAndMemoryPressureDataStartLiveTests
{
    private const int ClutterNewServerId = -496641;
    private const int ClutterQuietServerId = -496642;
    private const int ClutterNoRowsServerId = -496643;
    private const int MemoryQuietWeekServerId = -496644;
    private const int MemoryNewServerId = -496645;
    private const int MemoryHistoryServerId = -496646;

    // ── Query Store Clutter ──

    /* Added 2 days ago, its first Query Store row came a day later: the range reaches before the oldest row the store holds for it,
       and the panel still loads its rows. */
    [Fact]
    public async Task Clutter_ARangePastTheCoverage_NamesWhereTheCoverageStarts_AndTheRowsStillLoad_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var coverage = await store.Viewer.GetQueryStoreClutterDataStartAsync(ClutterNewServerId, store.Start, store.End, ct);
        var result = await store.Viewer.GetQueryStoreClutterAsync(ClutterNewServerId, store.Start, store.End, cancellationToken: ct);

        Assert.Equal(store.End.AddDays(-1), coverage);
        Assert.NotEmpty(result.Databases);
        Assert.Equal(QueryGridSeed.Since(store.End.AddDays(-1)), ClutterBanner(store, Task.FromResult(coverage)));

        /* A probe that throws shows no note, and the rows the same read loads are unchanged. */
        Assert.Null(ClutterBanner(store, Task.FromException<DateTime?>(new InvalidOperationException("the store went away"))));
        Assert.NotEmpty((await store.Viewer.GetQueryStoreClutterAsync(ClutterNewServerId, store.Start, store.End, cancellationToken: ct)).Databases);
    }

    /* A quiet start: monitored for 120 days, a row 100 days back proves the store covered the range, and the first row of the range
       came 5 hours in. And a server that has logged its collector's runs but holds no row at all is covered from its first collection.
       Neither names a note. */
    [Fact]
    public async Task Clutter_AQuietStart_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var quiet = await store.Viewer.GetQueryStoreClutterDataStartAsync(ClutterQuietServerId, store.Start, store.End, ct);
        var noRows = await store.Viewer.GetQueryStoreClutterDataStartAsync(ClutterNoRowsServerId, store.Start, store.End, ct);

        Assert.NotNull(quiet);
        Assert.True(quiet <= store.Start, $"coverage {quiet:O} should reach the range start {store.Start:O}");
        Assert.Null(ClutterBanner(store, Task.FromResult(quiet)));
        Assert.Equal(store.End.AddDays(-120), noRows);
        Assert.Null(ClutterBanner(store, Task.FromResult(noRows)));
        Assert.Empty((await store.Viewer.GetQueryStoreClutterAsync(ClutterNoRowsServerId, store.Start, store.End, cancellationToken: ct)).Databases);
    }

    // ── Memory Pressure Events ──

    /* A quiet week: monitored for 20 days, and the only pressure event of the 7-day range came 10 minutes before its end. The store
       covered the whole range (the purge edge is 30 days back, the first collection 20 days back), so no note. A walk to the table's
       oldest row would name that event, as if the store began a week into its own coverage. */
    [Fact]
    public async Task Memory_AQuietWeekWithOneEventAtItsEnd_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, rows) = await store.ReadMemoryAsync(MemoryQuietWeekServerId, ct);

        Assert.Single(rows);
        Assert.Equal(store.End.AddMinutes(-10), rows[0].SampleTime);
        Assert.NotNull(coverage);
        Assert.Equal(store.End.AddDays(-20), coverage);
        Assert.Null(MemoryBanner(store, Task.FromResult(coverage), rows));
    }

    /* Added 2 days ago, its first pressure event came a day later: the range reaches before the coverage, and the note names where it
       starts. */
    [Fact]
    public async Task Memory_ARangePastTheCoverage_NamesWhereTheCoverageStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, rows) = await store.ReadMemoryAsync(MemoryNewServerId, ct);

        Assert.Single(rows);
        Assert.Equal(store.End.AddDays(-2), coverage);
        Assert.Equal(QueryGridSeed.Since(store.End.AddDays(-2)), MemoryBanner(store, Task.FromResult(coverage), rows));
    }

    /* The ring buffer's history comes in with a server's first collection: an event stamped 3 days ago, collected 2 days ago when the
       server was added, is on the chart. The note names that event's own time. A probe that throws shows no note, and the chart still
       has its events. */
    [Fact]
    public async Task Memory_AnEventStampedBeforeTheFirstCollection_IsTheTimeNamed_AndAFailedProbeKeepsTheChart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, rows) = await store.ReadMemoryAsync(MemoryHistoryServerId, ct);

        var historic = store.End.AddDays(-3).AddSeconds(7);
        Assert.Equal(2, rows.Count);
        Assert.Equal(historic, rows.Min(r => r.SampleTime));
        Assert.Equal(QueryGridSeed.Since(historic), MemoryBanner(store, Task.FromResult(coverage), rows));
        /* Even a probe that found no coverage leaves the chart's own earliest event as the time named. */
        Assert.Equal(QueryGridSeed.Since(historic), MemoryBanner(store, Task.FromResult<DateTime?>(null), rows));

        Assert.Null(MemoryBanner(store, Task.FromException<DateTime?>(new InvalidOperationException("the store went away")), rows));
        Assert.Equal(2, ViewerServerTab.PressureRowsDrawn(rows).Count);
    }

    private static string? ClutterBanner(Store store, Task<DateTime?> probe) =>
        QueryGridSeed.ReadBanner(banner => ViewerServerTab.ShowQueryStoreClutterDataStartAsync(banner, probe, store.Start));

    private static string? MemoryBanner(Store store, Task<DateTime?> probe, IReadOnlyCollection<MemoryPressureEventRow> rows) =>
        QueryGridSeed.ReadBanner(banner => ViewerServerTab.ShowMemoryPressureEventsDataStartAsync(banner, probe, store.Start, rows));

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, ViewerDataService viewer, DateTime end)
        {
            _scratch = scratch;
            Viewer = viewer;
            End = end;
        }

        public ViewerDataService Viewer { get; }

        public DateTime End { get; }

        /// <summary>The range the viewer's default window draws: the 7 days before <see cref="End"/>.</summary>
        public DateTime Start => End.AddDays(-7);

        /// <summary>The probe's answer and the events the chart's read returned, for the default range.</summary>
        public async Task<(DateTime? Coverage, List<MemoryPressureEventRow> Rows)> ReadMemoryAsync(int serverId, CancellationToken ct)
        {
            var coverage = await Viewer.GetMemoryPressureEventsDataStartAsync(serverId, Start, End, ct);
            var rows = await Viewer.GetMemoryPressureEventsAsync(serverId, Start, End, ct);
            return (coverage, rows);
        }

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var scratch = await QueryGridSeed.OpenScratchAsync(ct);
            try
            {
                var end = QueryGridSeed.NowToTheMinute();
                var queryStoreCollector = DataWindowFloor.Source.ForCollectorTable("query_store_stats").CollectorName!;
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    /* Added 2 days ago: its first Query Store row came a day later. */
                    await EventGridSeed.AddServerAsync(connection, ClutterNewServerId, "clutter-new", end.AddDays(-2), queryStoreCollector, end, clockOffsetMinutes: null, ct);
                    await InsertQueryStoreStatsAsync(connection, ClutterNewServerId, "clutter-new", end.AddDays(-1), end, 1, ct);

                    /* Monitored for 120 days: a row 100 days back, then nothing until 5 hours into the range. */
                    await EventGridSeed.AddServerAsync(connection, ClutterQuietServerId, "clutter-quiet", end.AddDays(-120), queryStoreCollector, end, clockOffsetMinutes: null, ct);
                    await InsertQueryStoreStatsAsync(connection, ClutterQuietServerId, "clutter-quiet", end.AddDays(-100), end.AddDays(-100), 1, ct);
                    await InsertQueryStoreStatsAsync(connection, ClutterQuietServerId, "clutter-quiet", end.AddDays(-7).AddHours(5), end, 1, ct);

                    /* Monitored for 120 days, the collector has run, and nothing has been written to the table. */
                    await EventGridSeed.AddServerAsync(connection, ClutterNoRowsServerId, "clutter-norows", end.AddDays(-120), queryStoreCollector, end, clockOffsetMinutes: null, ct);

                    /* Monitored for 20 days, one pressure event in the range, 10 minutes before its end. */
                    await EventGridSeed.AddServerAsync(connection, MemoryQuietWeekServerId, "memory-quiet", end.AddDays(-20), "memory_pressure_events", end, clockOffsetMinutes: null, ct);
                    await InsertEventAsync(connection, MemoryQuietWeekServerId, "memory-quiet", end.AddMinutes(-10), end.AddMinutes(-10), ct);

                    /* Added 2 days ago: its first event came a day later. */
                    await EventGridSeed.AddServerAsync(connection, MemoryNewServerId, "memory-new", end.AddDays(-2), "memory_pressure_events", end, clockOffsetMinutes: null, ct);
                    await InsertEventAsync(connection, MemoryNewServerId, "memory-new", end.AddDays(-1), end.AddDays(-1), ct);

                    /* Added 2 days ago: its first collection stored an event from 3 days ago, then one from a day ago. */
                    await EventGridSeed.AddServerAsync(connection, MemoryHistoryServerId, "memory-history", end.AddDays(-2), "memory_pressure_events", end, clockOffsetMinutes: null, ct);
                    await InsertEventAsync(connection, MemoryHistoryServerId, "memory-history", end.AddDays(-3).AddSeconds(7), end.AddDays(-2).AddHours(1), ct);
                    await InsertEventAsync(connection, MemoryHistoryServerId, "memory-history", end.AddDays(-1), end.AddDays(-1), ct);
                }

                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /* One row per step from firstUtc to lastUtc, in one database, with a few distinct queries and plans so the plan-churn arm has
           something to count. */
        private static async Task InsertQueryStoreStatsAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, int stepHours, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.query_store_stats
                    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
                     query_id, plan_id, execution_type_desc, replica_role,
                     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
                     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
                SELECT
                    row_number() OVER (ORDER BY t) + $6 + (extract(epoch FROM $3::timestamp)::bigint % 1000000),
                    t, $1, $2, 'ClutterDb', 'dbo.proc', md5(t::text),
                    100 + (extract(epoch FROM t)::bigint % 5), 1000 + (extract(epoch FROM t)::bigint % 7), 'Regular', 'PRIMARY',
                    77, t, t,
                    10, 500, 300, 900, 700
                FROM generate_series($3::timestamp, $4::timestamp, make_interval(hours => $5)) AS t
                """, connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(firstUtc));
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(lastUtc));
            insert.Parameters.AddWithValue(stepHours);
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        private static async Task InsertEventAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime sampleTimeUtc, DateTime collectionTimeUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.memory_pressure_events
                    (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
                VALUES ($1, $2, $3, $4, $5, 'RESOURCE_MEMPHYSICAL_LOW', 2, 0)
                """, connection);
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L + (long)(sampleTimeUtc - new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds);
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(collectionTimeUtc));
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(sampleTimeUtc));
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await Viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
