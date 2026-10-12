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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5449: the procedure_stats window floor and the per-procedure history chart, read against what the Darling collector writes.
/// Run k stamps its rows at its start, T_k, and writes its collection_log row after the run, at T_k + 3 s with duration_ms 3000 and
/// rows_collected = the rows it stored. A run that found every procedure idle logs the same way with rows_collected 0. A failed
/// run logs ERROR and stores nothing. An outage logs nothing. Seeding that order is the point: a log time sits AFTER its rows,
/// so a reader that treats the log time as the run's start owns each row with the wrong run.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ProcedureStatsFloorAndHistoryLiveTests
{
    private const int Server = -544911;

    // ── the history chart ──────────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ProcedureHistoryChart_AQuietRunPlotsZero_OnlyBetweenTheBusyRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        /* Busy 0-2, quiet 3-4, busy 5, one procedure. */
        await store.SeedMinutesAsync("BBBIIB", ct);
        await using var viewer = new ViewerDataService(store.ConnectionString);

        var (grid, chart) = await ReadHistoryAsync(viewer, store, ct);

        /* The grid lists only the minutes with work. */
        Assert.Equal(4, grid.Count);
        /* The chart draws every run from the first row to the last: the quiet minutes 3 and 4 are zero points, and only those.
           A zero sits at the run's log time minus its duration, which is where the run really started. */
        Assert.Equal(Enumerable.Range(0, 6).Select(m => store.Start.AddMinutes(m)), chart.Select(r => r.CollectionTime));
        Assert.Equal(new long[] { 10, 10, 10, 0, 0, 10 }, chart.Select(r => (long)r.DeltaExecutions).ToArray());
    }

    [Fact]
    public async Task ProcedureHistoryChart_AFailedRunIsNotAQuietPoint()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        /* Minute 3 failed (an ERROR log row, nothing stored); minute 4 ran and found the procedure idle. */
        await store.SeedMinutesAsync("BBBEIB", ct);
        await using var viewer = new ViewerDataService(store.ConnectionString);

        var (_, chart) = await ReadHistoryAsync(viewer, store, ct);

        Assert.Equal(new[] { 0, 1, 2, 4, 5 }, chart.Select(r => (int)(r.CollectionTime - store.Start).TotalMinutes).ToArray());
        Assert.DoesNotContain(chart, r => r.CollectionTime == store.Start.AddMinutes(3));
    }

    [Fact]
    public async Task ProcedureHistoryChart_AnOutageIsNotAQuietPoint()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        /* Minutes 2 and 3: the service was down, nothing was logged at all. */
        await store.SeedMinutesAsync("BB--BB", ct);
        await using var viewer = new ViewerDataService(store.ConnectionString);

        var (_, chart) = await ReadHistoryAsync(viewer, store, ct);

        Assert.Equal(new[] { 0, 1, 4, 5 }, chart.Select(r => (int)(r.CollectionTime - store.Start).TotalMinutes).ToArray());
    }

    [Fact]
    public void IdleRunTimes_ARunOwnsTheRowsUpToItsLogTime_AndThePointSitsAtItsStart()
    {
        var t = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);
        /* Each run's log row is written 3 s after its start (duration 3000 ms), its rows stamped at the start. Runs 0-7. */
        var runs = Enumerable.Range(0, 8).Select(m => new ViewerProcedureHistoryIdleRuns.Run(t.AddMinutes(m).AddSeconds(3), 3000)).ToList();
        /* Rows at minutes 1, 4 and 6 (each a run's start), and one stamped exactly on run 6's log time. */
        var rows = new List<DateTime> { t.AddMinutes(1), t.AddMinutes(4), t.AddMinutes(6).AddSeconds(3) };

        var idle = ViewerProcedureHistoryIdleRuns.IdleRunTimes(rows, runs);

        /* Minutes 2, 3 and 5 own no row, and each 0 is at the run's START (log time minus duration), not its log time.
           Run 1 owns the first row, runs 4 and 6 their rows (6's sits on its own log time, still in (previous, own]); 0 and 7 lie
           outside the first and last row. The previous rule, ownership in [log time, next log time), gave minute 4's row to run 3. */
        Assert.Equal(new[] { 2, 3, 5 }, idle.Select(i => (int)(i - t).TotalMinutes).ToArray());
        Assert.All(idle, i => Assert.Equal(0, i.Second));
        Assert.Empty(ViewerProcedureHistoryIdleRuns.IdleRunTimes(new List<DateTime>(), runs));
    }

    // ── the window floor ───────────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ProcedureWindowFloor_AWindowOfOnlyIdleRuns_IsCoveredByTheCollectorsRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        /* Three busy minutes, then every procedure idle for the next six hours. */
        await store.SeedMinutesAsync("BBB" + new string('I', 360), ct);
        var end = store.Start.AddMinutes(363);

        /* The window holds only idle runs: no row in it, still covered. */
        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, Server, end.AddHours(-2), end, cancellationToken: ct);

        Assert.NotNull(floor);
        /* The oldest row, a few seconds before the first run's log row: nothing was dropped, so coverage starts there. */
        Assert.Equal(store.Start, floor);
    }

    [Fact]
    public async Task ProcedureWindowFloor_ARunsOnlyStoreHasAFloor_AsItsFirstRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        /* A server the collector has run against but that has no procedure_stats row at all yet. */
        await store.SeedMinutesAsync(new string('I', 120), ct);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, Server, store.Start.AddHours(1), store.Start.AddHours(2), cancellationToken: ct);

        /* The first run's log time: T_0 + 3 s. */
        Assert.Equal(store.Start.AddSeconds(3), floor);
    }

    [Fact]
    public async Task ProcedureWindowFloor_RetentionDroppedBusyRows_TheFloorIsWhereTheRowsStart()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct, daysBack: 10);
        /* Busy runs logged hourly from 10 days ago, but the rows reach back only 4 days: retention dropped the older ones. */
        var cut = store.Start.AddDays(6);
        await store.SeedStepsAsync(TimeSpan.FromHours(1), 240, k => store.Start.AddHours(k) >= cut ? 'B' : 'D', ct);

        /* A 7-day window. The oldest ROW is the floor; the older runs say rows existed there, and they are gone. */
        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, Server, store.Start.AddDays(3), store.Start.AddDays(10), cancellationToken: ct);

        Assert.Equal(cut, floor);
        /* So the window (7 days) is truncated by about 3 days: the viewer shows its coverage note. */
        Assert.True(floor > store.Start.AddDays(3).AddHours(6));
    }

    [Fact]
    public async Task ProcedureWindowFloor_IdleRunsBeforeTheFirstRow_TheFloorIsTheOldestRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct, daysBack: 10);
        /* Idle runs hourly from 10 days ago (nothing stored, so nothing was lost), busy from 1 day ago. */
        var busyFrom = store.Start.AddDays(9);
        await store.SeedStepsAsync(TimeSpan.FromHours(1), 240, k => store.Start.AddHours(k) >= busyFrom ? 'B' : 'I', ct);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, Server, store.Start.AddDays(3), store.Start.AddDays(10), cancellationToken: ct);

        /* No retention cut: coverage reaches the oldest run, older than the window. */
        Assert.Equal(store.Start.AddSeconds(3), floor);
    }

    [Fact]
    public async Task ProcedureWindowFloor_IdleRunsAfterTheDroppedRows_TheFloorIsTheFirstRunAfterThem()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct, daysBack: 5);
        /* Hours 0-9: busy runs whose rows were dropped. Hours 10-19: idle runs. From hour 20: busy runs with their rows. */
        await store.SeedStepsAsync(TimeSpan.FromHours(1), 120, k => k < 10 ? 'D' : k < 20 ? 'I' : 'B', ct);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, Server, store.Start.AddDays(-2), store.Start.AddDays(5), cancellationToken: ct);

        /* The newest run that stored rows and sits before the oldest row (hour 9) is the last of the dropped ones: the idle run at
           hour 10 is the first thing left standing, and it covers the quiet stretch before the oldest row. */
        Assert.Equal(store.Start.AddHours(10).AddSeconds(3), floor);
    }

    [Fact]
    public async Task ProcedureWindowFloor_BusyRowsDroppedAndNoRowSinceThen_TheFloorIsTheCutNotTheOldestRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct, daysBack: 10);
        /* Hours 0-71: busy runs (outside the window). Hours 72-119, 7 to 5 days ago: busy runs whose rows retention dropped.
           From hour 120 to now: every procedure idle, so no row is left at or before the window end (#5449 round 2). */
        await store.SeedStepsAsync(TimeSpan.FromHours(1), 240, k => k < 120 ? 'D' : 'I', ct);

        var windowStart = store.Start.AddDays(3);
        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, Server, windowStart, store.Start.AddDays(10), cancellationToken: ct);

        /* The first run after the newest dropped one (hour 119): the idle stretch since is covered, the dropped hours are not.
           Falling back to the oldest run (hour 0) would read the whole window as covered with no truncation note. */
        Assert.Equal(store.Start.AddHours(120).AddSeconds(3), floor);
        Assert.True(RawWindowFloor.IsTruncated(floor, windowStart));
    }

    [Fact]
    public void QueryViewFloors_AreByteIdenticalToTheirPreviousText()
    {
        /* #5449 changed procedure_stats' floor only. SHA-256 of the previous text (line endings normalised). */
        Assert.Equal("391564b3c3941545b1bcd0bd052f60720f4356915743c8317e556a14c9646da2", Sha(RawWindowFloor.FloorSql(RawWindowFloor.Table.QueryStats)));
        Assert.Equal("aee6e54cd891717a2b8bafb6c0214cb9846d130b954917ba903a1674cb7f1073", Sha(RawWindowFloor.FloorSql(RawWindowFloor.Table.QueryStoreStats)));
    }

    private static string Sha(string text) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text.Replace("\r\n", "\n")))).ToLowerInvariant();

    private static async Task<(List<ViewerProcedureStatsHistoryRow> Grid, List<ViewerProcedureStatsHistoryRow> Chart)> ReadHistoryAsync(
        ViewerDataService viewer, SeededStore store, CancellationToken ct)
    {
        var grid = await viewer.GetProcedureStatsHistoryAsync(Server, "AppDb", "dbo", "usp_Work", store.Start.AddMinutes(-5), store.Start.AddHours(1), ct);
        var chart = await viewer.GetProcedureStatsHistoryChartRowsAsync(Server, grid, ct);
        return (grid, chart);
    }

    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime start)
        {
            _scratch = scratch;
            DataSource = dataSource;
            Start = start;
        }

        public NpgsqlDataSource DataSource { get; }

        public string ConnectionString => _scratch.ConnectionString;

        /// <summary>A whole hour, <c>daysBack</c> days ago (6 hours by default): run 0 starts here.</summary>
        public DateTime Start { get; }

        public static async Task<SeededStore> CreateAsync(CancellationToken ct, int daysBack = 0)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live procedure_stats floor and history tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, Server, "floor-history", ct);

                var back = daysBack > 0 ? DateTime.UtcNow.AddDays(-daysBack) : DateTime.UtcNow.AddHours(-6);
                var start = new DateTime(back.Year, back.Month, back.Day, back.Hour, 0, 0, DateTimeKind.Utc);
                return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), start);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>One run per character, a minute apart from <see cref="Start"/>: <c>B</c> busy, <c>I</c> idle, <c>E</c> failed,
        /// <c>-</c> an outage (nothing at all).</summary>
        public Task SeedMinutesAsync(string pattern, CancellationToken ct) =>
            SeedStepsAsync(TimeSpan.FromMinutes(1), pattern.Length, k => pattern[k], ct);

        /// <summary>
        /// Run k at <see cref="Start"/> + k * step, in the order the collector writes them. <c>B</c>: a row at the run's start, then a
        /// SUCCESS log row 3 s later, duration 3000 ms, one row collected. <c>I</c>: the SUCCESS log row only, rows_collected 0.
        /// <c>D</c>: the busy log row without its row (retention dropped it). <c>E</c>: an ERROR log row only. <c>-</c>: nothing.
        /// </summary>
        public async Task SeedStepsAsync(TimeSpan step, int count, Func<int, char> kind, CancellationToken ct)
        {
            await using var connection = await DataSource.OpenConnectionAsync(ct);
            for (var k = 0; k < count; k++)
            {
                var at = Start + (step * k);
                var what = kind(k);
                if (what == 'B')
                {
                    await InsertRowAsync(connection, at, ct);
                }

                if (what == '-')
                {
                    continue;
                }

                await InsertLogAsync(connection, at.AddSeconds(3), what == 'E' ? "ERROR" : "SUCCESS", (what is 'B' or 'D') ? 1 : 0, ct);
            }
        }

        private static async Task InsertRowAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ((SELECT COALESCE(MAX(collection_id), 0) + 1 FROM collect.procedure_stats), $1, $2, 'floor-history', 'AppDb', 'dbo', 'usp_Work', '0x01', 600000, 600000, 10, 60)", connection);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(Server);
            await insert.ExecuteNonQueryAsync(ct);
        }

        private static async Task InsertLogAsync(NpgsqlConnection connection, DateTime at, string status, int rowsCollected, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ((SELECT COALESCE(MAX(log_id), 0) + 1 FROM collect.collection_log), $1, 'floor-history', 'procedure_stats', $2, 3000, $3, $4)", connection);
            insert.Parameters.AddWithValue(Server);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(status);
            insert.Parameters.AddWithValue(rowsCollected);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
