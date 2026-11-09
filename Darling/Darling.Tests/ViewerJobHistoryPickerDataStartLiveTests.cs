/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8342: the Job History tab's time range picker names where the stored job history starts, for the scope on screen: the oldest
/// run the store holds, fleet-wide at All Servers and the picked server's own at one server. A typed range wider than the stored
/// history previews that run, and a range inside the stored history shows no note. The note cannot come from the grid's
/// window-bounded coverage probe: a server collected for a month whose job history reaches back three days covers a week's
/// window, so that probe answers the window's own start and the note stays silent. The tab is driven through its own load over a
/// real store.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerJobHistoryPickerDataStartLiveTests
{
    private const int ServerA = -834211;
    private const int ServerB = -834212;

    private static string ServerName(int serverId) => $"SERVER-{Math.Abs(serverId)}";

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>The seeded instants, relative to the clock the picker's relative ranges resolve against.</summary>
    private sealed record Seeded(DateTime OldestA, DateTime LaterA, DateTime FirstB);

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, ViewerDataService viewer, Seeded seeded)
        {
            _scratch = scratch;
            Viewer = viewer;
            Seeded = seeded;
        }

        public ViewerDataService Viewer { get; }

        public Seeded Seeded { get; }

        /// <summary>
        /// A store where both servers have been collected daily for a month (so a week's window is covered), holding server A's run
        /// from three days ago and a later one, and server B's first run, both of the later ones within the last day.
        /// </summary>
        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live Job History picker data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                var now = DateTime.UtcNow;
                var nowUtc = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                var seeded = new Seeded(nowUtc.AddDays(-3), nowUtc.AddHours(-20), nowUtc.AddHours(-10));

                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    foreach (var serverId in new[] { ServerA, ServerB })
                    {
                        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName(serverId), ct);
                        await using var added = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection);
                        added.Parameters.AddWithValue(serverId);
                        added.Parameters.AddWithValue(Naive(nowUtc.AddDays(-30)));
                        await added.ExecuteNonQueryAsync(ct);

                        await using var log = new NpgsqlCommand(@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT row_number() OVER () + $4, $1, $2, 'job_history', t, 12, 'SUCCESS', 0
FROM generate_series($3::timestamp - interval '30 days', $3::timestamp, interval '1 day') AS t", connection);
                        log.Parameters.AddWithValue(serverId);
                        log.Parameters.AddWithValue(ServerName(serverId));
                        log.Parameters.AddWithValue(Naive(nowUtc));
                        log.Parameters.AddWithValue(-(long)serverId * 1000L);
                        await log.ExecuteNonQueryAsync(ct);
                    }

                    await InsertRunAsync(connection, 1, ServerA, seeded.OldestA, ct);
                    await InsertRunAsync(connection, 2, ServerA, seeded.LaterA, ct);
                    await InsertRunAsync(connection, 3, ServerB, seeded.FirstB, ct);
                }

                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), seeded);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /* One job outcome, stored at its own run time (the server has no clock row, so its stored run time reads as UTC). */
        private static Task InsertRunAsync(NpgsqlConnection connection, long id, int serverId, DateTime utc, CancellationToken ct) =>
            InsertRunAsync(connection, id, serverId, utc, utc, ct);

        /* One job outcome stored at collectedUtc that ran at runUtc: a first collection copies the history msdb already holds. */
        private static async Task InsertRunAsync(NpgsqlConnection connection, long id, int serverId, DateTime collectedUtc, DateTime runUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name,
     run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
VALUES ($1, $3, $2, $4, $1, 'job-1', 'Nightly', true, 0, '(Job outcome)', 1, 'The job succeeded.', $5, 5, 0, NULL)", connection);
            insert.Parameters.AddWithValue(id);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(Naive(collectedUtc));
            insert.Parameters.AddWithValue(ServerName(serverId));
            insert.Parameters.AddWithValue(Naive(runUtc));
            await insert.ExecuteNonQueryAsync(ct);
        }

        /// <summary>Adds a run of <paramref name="serverId"/> stored at <paramref name="collectedUtc"/> that ran at <paramref name="runWall"/>, the
        /// server's wall clock.</summary>
        public async Task AddRunAsync(long id, int serverId, DateTime collectedUtc, DateTime runWall, CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(_scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await InsertRunAsync(connection, id, serverId, collectedUtc, runWall, ct);
        }

        /// <summary>Gives <paramref name="serverId"/> a clock: its newest server_properties row carries <paramref name="timeZoneId"/> and the
        /// offset that zone has at <paramref name="collectedUtc"/>, the row the grid reads the server's run times on.</summary>
        public async Task AddClockAsync(int serverId, string timeZoneId, int offsetMinutes, DateTime collectedUtc, CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(_scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.server_properties (collection_id, server_id, server_name, collection_time, utc_offset_minutes, time_zone_id)
VALUES (1, $1, $2, $3, $4, $5)", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(ServerName(serverId));
            insert.Parameters.AddWithValue(Naive(collectedUtc));
            insert.Parameters.AddWithValue(offsetMinutes);
            insert.Parameters.AddWithValue(timeZoneId);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public ValueTask DisposeAsync() => _scratch.DisposeAsync();
    }

    private static JobHistoryTab Tab(ViewerDataService viewer)
    {
        var tab = new JobHistoryTab();
        tab.Initialize(viewer);

        var host = new Border { Width = 1000, Height = 680, Child = tab };
        host.Measure(new Size(1000, 680));
        host.Arrange(new Rect(0, 0, 1000, 680));
        return tab;
    }

    /// <summary>The compact picker's notes: it collapses its note line and carries the notes in its button's tooltip, under the range's label.</summary>
    private static string Notes(TimeRangePicker picker) => Assert.IsType<string>(Assert.IsType<Button>(picker.FindName("PickerButton")).ToolTip);

    [Fact]
    public async Task AWeekAtAllServers_NamesTheOldestStoredRun_NotTheWindowStart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        StaTestThread.Run(async () =>
        {
            var tab = Tab(store.Viewer);
            tab.TimeRangePickerControl.Value = TimeRangePresets.Find("1w")!;

            await tab.RefreshJobsAsync();

            Assert.Equal(store.Seeded.OldestA, tab.TimeRangePickerControl.DataStartUtc);
            Assert.Contains("\nData starts ", Notes(tab.TimeRangePickerControl), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task FourHoursInsideTheStoredHistory_ShowsNoNote_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        StaTestThread.Run(async () =>
        {
            var tab = Tab(store.Viewer);
            tab.TimeRangePickerControl.Value = TimeRangePresets.Find("4h")!;

            await tab.RefreshJobsAsync();

            Assert.Equal(store.Seeded.OldestA, tab.TimeRangePickerControl.DataStartUtc);
            Assert.DoesNotContain("Data starts", Notes(tab.TimeRangePickerControl), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task OneServer_NamesThatServersOwnFirstRun_NotAnotherServersOlderOne_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        StaTestThread.Run(async () =>
        {
            var tab = Tab(store.Viewer);
            tab.TimeRangePickerControl.Value = TimeRangePresets.Find("1w")!;
            await tab.RefreshJobsAsync();

            /* The combo offers both servers after the first load; picking B scopes the next load, and the note, to it. */
            var pickedB = false;
            for (var i = 1; i < tab.ServerFilterComboBox.Items.Count; i++)
            {
                if ((tab.ServerFilterComboBox.Items[i] as ComboBoxItem)?.Tag as string == ServerB.ToString(System.Globalization.CultureInfo.InvariantCulture))
                {
                    tab.ServerFilterComboBox.SelectedIndex = i;
                    pickedB = true;
                }
            }

            Assert.True(pickedB, "the Server combo does not offer server B");
            await tab.RefreshJobsAsync();

            Assert.Equal(store.Seeded.FirstB, tab.TimeRangePickerControl.DataStartUtc);
            Assert.Contains("\nData starts ", Notes(tab.TimeRangePickerControl), StringComparison.Ordinal);
        });
    }

    /* Server A's first collection, three days ago, copied a run msdb already held from a week before that. The grid can show
       that run, so the note names it, not the collection that stored it. */
    [Fact]
    public async Task ARunCopiedFromMsdbBeforeTheFirstCollection_IsWhereTheDataStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var copiedRun = store.Seeded.OldestA.AddDays(-7);
        await store.AddRunAsync(4, ServerA, store.Seeded.OldestA, copiedRun, ct);
        /* The instant the grid shows for that run: the server has no clock row, so its runs are read on the machine's clock, the
           rule every grid row follows. */
        var expected = (await store.Viewer.GetServerClockOrMachineAsync(ServerA, ct)).ToUtc(copiedRun);

        StaTestThread.Run(async () =>
        {
            var tab = Tab(store.Viewer);
            /* A month: a range that starts before the copied run, so the note has something to say. */
            tab.TimeRangePickerControl.Value = TimeRangePresets.Find("1mo")!;

            await tab.RefreshJobsAsync();

            Assert.Equal(expected, tab.TimeRangePickerControl.DataStartUtc);
            Assert.Contains("\nData starts ", Notes(tab.TimeRangePickerControl), StringComparison.Ordinal);
        });
    }

    /* Server A keeps Pacific time: its newest clock row carries the zone and November's offset (-480), and its oldest run is a July
       noon on its wall clock, when the zone was an hour ahead of that offset. The note names the instant the grid shows for that
       run, read on the server's zone, not the wall clock shifted by the one newest offset. */
    [Fact]
    public async Task AnOldestRunAcrossADaylightSavingChange_IsReadOnTheServersZone_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);
        var pacific = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");
        var oldest = store.Seeded.OldestA;
        var julyNoon = new DateTime(oldest.Month <= 7 ? oldest.Year - 1 : oldest.Year, 7, 1, 12, 0, 0, DateTimeKind.Unspecified);
        await store.AddClockAsync(ServerA, pacific.Id, -480, oldest, ct);
        await store.AddRunAsync(4, ServerA, oldest, julyNoon, ct);

        StaTestThread.Run(async () =>
        {
            var tab = Tab(store.Viewer);
            tab.TimeRangePickerControl.Value = TimeRangePresets.Find("1mo")!;

            await tab.RefreshJobsAsync();

            Assert.Equal(TimeZoneInfo.ConvertTimeToUtc(julyNoon, pacific), tab.TimeRangePickerControl.DataStartUtc);
        });
    }

    /* A read that fails on another scope hides the note. Coming back to a scope whose start was read within the cache's life
       must show that start again, not stay hidden until the cache ages out. */
    [Fact]
    public async Task AHiddenNote_ComesBackOnACachedScope_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        StaTestThread.Run(async () =>
        {
            var tab = Tab(store.Viewer);
            tab.TimeRangePickerControl.Value = TimeRangePresets.Find("1w")!;
            await tab.RefreshJobsAsync();
            Assert.Equal(store.Seeded.OldestA, tab.TimeRangePickerControl.DataStartUtc);

            /* What a failed read on another scope leaves behind, with this scope's read still cached. */
            tab.TimeRangePickerControl.DataStartUtc = null;

            await tab.RefreshJobsAsync();

            Assert.Equal(store.Seeded.OldestA, tab.TimeRangePickerControl.DataStartUtc);
            Assert.Contains("\nData starts ", Notes(tab.TimeRangePickerControl), StringComparison.Ordinal);
        });
    }
}
