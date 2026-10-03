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
/// The desktop viewer's Plan Corrections grid against a real store (#4966). The read windows on <c>collection_time</c> and
/// keeps the newest 200 rows, and <c>plan_correction</c> has a schedule edge, so the probe's coverage is the later of the server's
/// first collection (the registry's created date) and the table's retention edge, moved earlier by a row in the range. The
/// notice names the earlier of that coverage and the earliest row the grid shows, and a read that returned a full page names its
/// oldest row, since the grid reaches back no further. Each case runs the same two calls the server tab makes (the grid's rows,
/// the data-start probe) and the same rule it applies.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerPlanCorrectionsDataStartLiveTests
{
    private const int NewServerId = -496631;
    private const int QuietServerId = -496632;
    private const int FullPageServerId = -496633;
    private const int PastTheCapServerId = -496634;
    private const int UnderTheCapServerId = -496635;

    private const int Cap = ViewerDataService.PlanCorrectionsRowCap;

    /* Rows start inside the range: the server was added 2 days ago, and its first recommendation was collected a day later. The
       notice reads back the coverage start, the day the server was added. */
    [Fact]
    public async Task RowsThatStartInsideTheRange_GiveANotice_AtTheCoverageStart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, shown) = await store.ReadAsync(NewServerId, ct);
        var notice = Notice(coverage, shown);

        Assert.NotEmpty(shown);
        Assert.Equal(store.End.AddDays(-2), coverage);
        Assert.Equal(store.End.AddDays(-2), notice);
        Assert.True(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* A quiet start: monitored for 120 days, so the store covers the whole range (its retention edge, 30 days back, is the
       coverage), but the first recommendation of the range was collected 5 hours in. No notice. */
    [Fact]
    public async Task AQuietStart_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, shown) = await store.ReadAsync(QuietServerId, ct);
        var notice = Notice(coverage, shown);

        Assert.NotEmpty(shown);
        Assert.Equal(store.Start.AddHours(5), shown.Min(r => r.CollectionTime));
        Assert.NotNull(notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* A read that fills the cap (200 rows exactly, and 220 seeded, which returns 200): the grid shows the newest 200 recommendations
       and reaches back no further than the oldest of them, which came 199 minutes before the range's end, days after its start. The
       store covers the whole range (120 days monitored), yet the notice names that row. */
    [Theory]
    [InlineData(Cap)]
    [InlineData(Cap + 20)]
    public async Task AFullPage_GivesANotice_AtItsOldestRow_ThoughTheStoreCoversTheRange_AgainstDevPostgres(int seeded)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, shown) = await store.ReadAsync(seeded == Cap ? FullPageServerId : PastTheCapServerId, ct);
        var notice = Notice(coverage, shown);

        Assert.Equal(Cap, shown.Count);
        Assert.Equal(store.End.AddMinutes(-(Cap - 1)), notice);
        Assert.True(RawWindowFloor.IsTruncated(notice, store.Start));

        /* The cap is what raises it: without the cap rule the store's coverage reaches the range start, and nothing shows. */
        Assert.False(RawWindowFloor.IsTruncated(Notice(coverage, shown, applyCap: false), store.Start));
    }

    /* One row under the cap: the read returned everything the range holds, so the coverage rule stands and the covered range
       shows no notice. */
    [Fact]
    public async Task AReadOneRowUnderItsCap_KeepsItsResult_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, shown) = await store.ReadAsync(UnderTheCapServerId, ct);
        var notice = Notice(coverage, shown);

        Assert.Equal(Cap - 1, shown.Count);
        Assert.NotNull(notice);
        Assert.Equal(Notice(coverage, shown, applyCap: false), notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* The rule the tab applies (ViewerServerTab.ShowEventDataStartAsync): the probe's answer, the collection time of each row shown,
       and the grid's cap. */
    private static DateTime? Notice(DateTime? coverage, IReadOnlyCollection<PlanCorrectionRow> shown, bool applyCap = true) =>
        ViewerEventDataStart.Of(
            coverage,
            ViewerEventDataStart.EarliestOf(shown.Select(r => (DateTime?)r.CollectionTime)),
            applyCap && ViewerEventDataStart.ReadHitCap(shown.Count, Cap));

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;
        private readonly ViewerDataService _viewer;

        private Store(ScratchPostgres scratch, ViewerDataService viewer, DateTime end)
        {
            _scratch = scratch;
            _viewer = viewer;
            End = end;
        }

        public DateTime End { get; }

        public DateTime Start => End.AddDays(-7);

        /// <summary>The probe's answer and the rows the grid's read returned, for the range the viewer's default window draws.</summary>
        public async Task<(DateTime? Coverage, List<PlanCorrectionRow> Shown)> ReadAsync(int serverId, CancellationToken ct)
        {
            var coverage = await _viewer.GetPlanCorrectionsDataStartAsync(serverId, Start, End, ct);
            var rows = await _viewer.GetPlanCorrectionsAsync(serverId, Start, End, cancellationToken: ct);
            return (coverage, rows);
        }

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var scratch = await QueryGridSeed.OpenScratchAsync(ct);
            try
            {
                var end = QueryGridSeed.NowToTheMinute();
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    /* Added 2 days ago: its first recommendation came a day later. */
                    await EventGridSeed.AddServerAsync(connection, NewServerId, "plan-corrections-new", end.AddDays(-2), "plan_correction", end, clockOffsetMinutes: null, ct);
                    await InsertAsync(connection, NewServerId, "plan-corrections-new", end.AddDays(-1), end.AddDays(-1).AddHours(4), 60, ct);

                    /* Monitored for 120 days: the first recommendation of the range came 5 hours in. */
                    await EventGridSeed.AddServerAsync(connection, QuietServerId, "plan-corrections-quiet", end.AddDays(-120), "plan_correction", end, clockOffsetMinutes: null, ct);
                    await InsertAsync(connection, QuietServerId, "plan-corrections-quiet", end.AddDays(-7).AddHours(5), end.AddDays(-7).AddHours(9), 60, ct);

                    /* Monitored for 120 days, recommendations one minute apart ending at the range's end: the cap, one past it, one under it. */
                    foreach (var (serverId, name, seeded) in new[]
                    {
                        (FullPageServerId, "plan-corrections-full", Cap),
                        (PastTheCapServerId, "plan-corrections-past", Cap + 20),
                        (UnderTheCapServerId, "plan-corrections-under", Cap - 1),
                    })
                    {
                        await EventGridSeed.AddServerAsync(connection, serverId, name, end.AddDays(-120), "plan_correction", end, clockOffsetMinutes: null, ct);
                        await InsertAsync(connection, serverId, name, end.AddMinutes(-(seeded - 1)), end, 1, ct);
                    }
                }

                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /* recommendation_name is NOT NULL on the grid's read: the collector's enablement-only rows carry none, and the read drops them. */
        private static async Task InsertAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, int stepMinutes, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.plan_correction
                    (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score, query_id, query_text)
                SELECT row_number() OVER () + $6, t, $1, $2, 'PlanDb', 'rec-' || t::text, 'Active', 50, 42, 'SELECT 1'
                FROM generate_series($3::timestamp, $4::timestamp, make_interval(mins => $5)) AS t
                """, connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(firstUtc));
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(lastUtc));
            insert.Parameters.AddWithValue(stepMinutes);
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await _viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>
/// The desktop viewer's Query Heatmap against a real store (#4966). The read draws the raw <c>query_stats</c> table's rows, one
/// column per 5-minute bin that holds a row. <c>query_stats</c> carries no schedule edge, so the probe's coverage is the oldest
/// row the server holds at or before the range's end, and the notice names the earlier of that and the first column drawn.
/// </summary>
/* A quiet start does not apply here, unlike the grids whose collector has a schedule edge. With no edge, the server's first
   collection (its registry row) says nothing about where the table's coverage starts: the oldest row is the coverage. A server
   whose first row of the range came late and which holds nothing older reads as a server that did not exist before that row,
   and the notice names the row's bin. A range the store covered from before it started has an older row before the range, which
   is the case that gives no notice. */
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerQueryHeatmapDataStartLiveTests
{
    private const int NewServerId = -496641;
    private const int OlderRowServerId = -496642;

    /* Rows start inside the range: the server was added 2 days ago, and its first query_stats row came a day later, 3 minutes past
       a 5-minute boundary. Nothing older vouches for the stretch before it, so coverage is that row; the first column drawn starts
       on the boundary before it, and the notice names the boundary, never a time later than a column on screen. */
    [Fact]
    public async Task RowsThatStartInsideTheRange_GiveANotice_AtTheFirstColumnDrawn_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, columns) = await store.ReadAsync(NewServerId, ct);
        var notice = ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(columns.Select(t => (DateTime?)t)));

        Assert.Equal(store.FirstRowInTheRange, coverage);
        Assert.Equal(store.FirstRowInTheRange.AddMinutes(-3), columns.Min());
        Assert.Equal(store.FirstRowInTheRange.AddMinutes(-3), notice);
        Assert.True(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* An older row before the range: the server holds a row from 10 days ago, then a gap, then rows from 5 hours into the range. That row
       is where the table's coverage starts, before the range, so the first column drawn (5 hours in) is not a cut. No notice. */
    [Fact]
    public async Task AnOlderRowBeforeTheRange_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var (coverage, columns) = await store.ReadAsync(OlderRowServerId, ct);
        var notice = ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(columns.Select(t => (DateTime?)t)));

        Assert.NotEmpty(columns);
        Assert.Equal(store.End.AddDays(-10), coverage);
        Assert.True(columns.Min() > store.Start);
        Assert.NotNull(notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;
        private readonly ViewerDataService _viewer;

        private Store(ScratchPostgres scratch, ViewerDataService viewer, DateTime end, DateTime firstRowInTheRange)
        {
            _scratch = scratch;
            _viewer = viewer;
            End = end;
            FirstRowInTheRange = firstRowInTheRange;
        }

        public DateTime End { get; }

        public DateTime Start => End.AddDays(-7);

        /// <summary>The first row of the new server: 3 minutes past a 5-minute boundary, a day before the range's end.</summary>
        public DateTime FirstRowInTheRange { get; }

        /// <summary>The probe's answer and the time of each column the heatmap draws.</summary>
        public async Task<(DateTime? Coverage, DateTime[] Columns)> ReadAsync(int serverId, CancellationToken ct)
        {
            var coverage = await _viewer.GetQueryHeatmapDataStartAsync(serverId, Start, End, ct);
            var heatmap = await _viewer.GetQueryHeatmapAsync(serverId, HeatmapMetric.Duration, Start, End, cancellationToken: ct);
            return (coverage, heatmap.TimeBuckets);
        }

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var scratch = await QueryGridSeed.OpenScratchAsync(ct);
            try
            {
                var end = QueryGridSeed.NowToTheMinute();
                var dayBefore = end.AddDays(-1);
                var first = new DateTime(dayBefore.Year, dayBefore.Month, dayBefore.Day, dayBefore.Hour, 3, 0, DateTimeKind.Utc);
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    await EventGridSeed.AddServerAsync(connection, NewServerId, "heatmap-new", end.AddDays(-2), "query_stats", end, clockOffsetMinutes: null, ct);
                    await InsertAsync(connection, NewServerId, "heatmap-new", first, first.AddMinutes(55), ct);

                    await EventGridSeed.AddServerAsync(connection, OlderRowServerId, "heatmap-older", end.AddDays(-30), "query_stats", end, clockOffsetMinutes: null, ct);
                    await InsertAsync(connection, OlderRowServerId, "heatmap-older", end.AddDays(-10), end.AddDays(-10), ct);
                    await InsertAsync(connection, OlderRowServerId, "heatmap-older", end.AddDays(-7).AddHours(5), end.AddDays(-7).AddHours(6), ct);
                }

                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), end, first);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /* One row every 5 minutes: the read keeps a row with an execution count and a metric, so both are set. */
        private static async Task InsertAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.query_stats
                    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds, query_text)
                SELECT row_number() OVER () + $5, t, $1, $2, 'HeatDb', 'HASHHEAT', '0xHEAT', 1000, 1000, 10, 300, 'SELECT 1'
                FROM generate_series($3::timestamp, $4::timestamp, interval '5 minutes') AS t
                """, connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(firstUtc));
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(lastUtc));
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L + (long)(firstUtc - DateTime.UnixEpoch).TotalMinutes);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await _viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>
/// The desktop viewer's Query Store Regressions grid against a real store (#4966). The read compares the range with the
/// 7 days before it (<see cref="ViewerDataService.QueryStoreRegressionsBaselineStart"/>), so the notice keys on that EARLIER window:
/// a server added two days before a one-day range covers the range whole but holds two days of baseline, not seven, and a notice
/// keyed on the range alone stays silent. <c>query_store_stats</c> carries no schedule edge, so coverage is the oldest row.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerQueryStoreRegressionsDataStartLiveTests
{
    private const int NewServerId = -496651;
    private const int DeepServerId = -496652;

    /* Rows start inside the baseline window: the server was added 2 days before the one-day range, so it holds 2 days of the 7-day
       baseline and the range itself. The probe asks about the baseline window and names where that window's data starts, the day the
       server was added; the range-keyed comparison (the range's own start) would say nothing, since the coverage precedes it. */
    [Fact]
    public async Task RowsThatStartInsideTheBaselineWindow_GiveANotice_AtTheBaselinesDataStart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var coverage = await store.ProbeAsync(NewServerId, ct);
        var baselineStart = ViewerDataService.QueryStoreRegressionsBaselineStart(store.Start);

        Assert.Equal(store.Start.AddDays(-2), coverage);
        Assert.True(RawWindowFloor.IsTruncated(coverage, baselineStart));
        Assert.Equal(store.Start.AddDays(-2), RawWindowFloor.EffectiveStart(coverage, baselineStart));
        Assert.False(RawWindowFloor.IsTruncated(coverage, store.Start));
    }

    /* Rows reach back before the baseline: the server holds a row from 10 days before the range, a day before the baseline window
       starts. The baseline is whole, so no notice. */
    [Fact]
    public async Task RowsThatReachBackBeforeTheBaseline_GiveNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        var coverage = await store.ProbeAsync(DeepServerId, ct);
        var baselineStart = ViewerDataService.QueryStoreRegressionsBaselineStart(store.Start);

        Assert.Equal(store.Start.AddDays(-10), coverage);
        Assert.False(RawWindowFloor.IsTruncated(coverage, baselineStart));
    }

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;
        private readonly ViewerDataService _viewer;

        private Store(ScratchPostgres scratch, ViewerDataService viewer, DateTime end)
        {
            _scratch = scratch;
            _viewer = viewer;
            End = end;
        }

        public DateTime End { get; }

        /// <summary>The start of the one-day range the grid compares with its baseline.</summary>
        public DateTime Start => End.AddDays(-1);

        /// <summary>The probe's answer for the one-day range, as the server tab asks it.</summary>
        public Task<DateTime?> ProbeAsync(int serverId, CancellationToken ct) =>
            _viewer.GetQueryStoreRegressionsDataStartAsync(serverId, Start, End, ct);

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var scratch = await QueryGridSeed.OpenScratchAsync(ct);
            try
            {
                var end = QueryGridSeed.NowToTheMinute();
                var start = end.AddDays(-1);
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    /* Added 2 days before the range: a row an hour from then on. */
                    await EventGridSeed.AddServerAsync(connection, NewServerId, "regressions-new", start.AddDays(-2), "query_store_stats", end, clockOffsetMinutes: null, ct);
                    await InsertAsync(connection, NewServerId, "regressions-new", start.AddDays(-2), end, ct);

                    /* Monitored for a month, with rows from 10 days before the range: a day before the baseline window starts. */
                    await EventGridSeed.AddServerAsync(connection, DeepServerId, "regressions-deep", end.AddDays(-30), "query_store_stats", end, clockOffsetMinutes: null, ct);
                    await InsertAsync(connection, DeepServerId, "regressions-deep", start.AddDays(-10), end, ct);
                }

                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        private static async Task InsertAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand("""
                INSERT INTO collect.query_store_stats
                    (collection_id, collection_time, server_id, server_name, database_name,
                     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
                     query_hash, execution_count, avg_duration_us, avg_cpu_time_us)
                SELECT row_number() OVER () + $5, t, $1, $2, 'RegressionDb',
                       42, 4200, 'Regular', t, t,
                       'HASHREGR', 10, 500, 300
                FROM generate_series($3::timestamp, $4::timestamp, interval '1 hour') AS t
                """, connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(firstUtc));
            insert.Parameters.AddWithValue(QueryGridSeed.Naive(lastUtc));
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await _viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>What the three query-grid live classes share: the scratch database and the instants a range is built from.</summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. This helper reaches DARLING_TEST_PG only to CREATE and DROP
   each test's own database through ScratchPostgres; nothing here touches the shared database. */
internal static class QueryGridSeed
{
    public static async Task<ScratchPostgres> OpenScratchAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query grid data-start tests.");

        return await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
    }

    /// <summary>The current UTC time with the seconds dropped, as the range's end.</summary>
    public static DateTime NowToTheMinute()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
    }

    /// <summary>The instant as the store's naive UTC timestamp.</summary>
    public static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);
}
