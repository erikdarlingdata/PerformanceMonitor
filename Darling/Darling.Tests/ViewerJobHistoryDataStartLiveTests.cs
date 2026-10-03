/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Job History tab's "Showing since" note against a real store (#4966). Every test reads the grid through the calls the tab
/// makes (the read and the coverage probe, both from one start) and raises the note through the tab's own step
/// (<c>JobHistoryTab.ShowJobHistoryDataStartAsync</c>), so a change to the rule fails here. The store holds the history a first
/// collection copied (runs older than the collection that stored them), a quiet start, a full page of the newest 2,000 runs, and
/// the tab's All Servers view.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. The note's time text reads the
   process-wide display mode, which the tests set (restored in Dispose); the shared collection serializes it with every
   other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerJobHistoryDataStartLiveTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private const int ServerA = -499911;
    private const int ServerB = -499912;
    private const string Collector = "job_history";

    private static string Since(DateTime utc) =>
        "Showing since " + utc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /* The server was added 2 days before the range's end; its first collection copied runs from 6 days back. The probe names the
       first collection, but the grid's earliest run is older: the note names that run. */
    [Fact]
    public async Task ARunStampedBeforeTheFirstCollection_IsTheTimeNamed_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        await store.AddServerAsync(ServerA, store.End.AddDays(-2), ct);
        await store.LogRunsAsync(ServerA, store.End.AddDays(-2), store.End, ct);
        await store.InsertRunsAsync(ServerA, store.Start.AddDays(1), store.End, TimeSpan.FromHours(6), collectedAtUtc: store.End.AddDays(-2), ct);

        Assert.Equal(store.End.AddDays(-2), await store.Viewer.GetJobHistoryDataStartAsync(ServerA, store.Start, store.End, ct));
        Assert.Equal(Since(store.Start.AddDays(1)), await store.NoteAsync(ServerA, ct));
    }

    /* The first collection came 2 days before the range's end and stored runs from the last 2 days only: the note names the first
       collection (the later of it and the retention edge), a time before the earliest run. */
    [Fact]
    public async Task ARangePastTheCoverage_NamesTheCoverageStart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        await store.AddServerAsync(ServerA, store.End.AddDays(-2), ct);
        await store.LogRunsAsync(ServerA, store.End.AddDays(-2), store.End, ct);
        await store.InsertRunsAsync(ServerA, store.End.AddDays(-2).AddHours(3), store.End, TimeSpan.FromHours(6), collectedAtUtc: store.End.AddDays(-2), ct);

        Assert.Equal(Since(store.End.AddDays(-2)), await store.NoteAsync(ServerA, ct));
    }

    /* Collected for 40 days, the whole range; the first run came 5 hours into it. A quiet start, not a cut: no note. */
    [Fact]
    public async Task AQuietStart_ShowsNoNote_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        await store.AddServerAsync(ServerA, store.End.AddDays(-40), ct);
        await store.LogRunsAsync(ServerA, store.End.AddDays(-40), store.End, ct);
        await store.InsertRunsAsync(ServerA, store.Start.AddHours(5), store.End, TimeSpan.FromHours(6), collectedAtUtc: null, ct);

        Assert.Null(await store.NoteAsync(ServerA, ct));
    }

    /* A full page of the newest 2,000 runs names its oldest run though the store covers the range (collected for 40 days); a page
       one run short is the whole range and shows none. Runs a minute apart, the newest a minute before the range's end. */
    [Fact]
    public async Task AFullPage_NamesItsOldestRun_AndAPageOneRunShortDoesNot_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await Store.CreateAsync(ct);

        foreach (var (serverId, runs) in new[] { (ServerA, JobHistoryTab.RowCap), (ServerB, JobHistoryTab.RowCap - 1) })
        {
            await store.AddServerAsync(serverId, store.End.AddDays(-40), ct);
            await store.LogRunsAsync(serverId, store.End.AddDays(-40), store.End, ct);
            await store.InsertRunsAsync(
                serverId, store.End.AddMinutes(-runs), store.End.AddMinutes(-1), TimeSpan.FromMinutes(1), collectedAtUtc: null, ct);
        }

        Assert.Equal(Since(store.End.AddMinutes(-JobHistoryTab.RowCap)), await store.NoteAsync(ServerA, ct));
        Assert.Null(await store.NoteAsync(ServerB, ct));
    }

    /* The All Servers view: coverage is the earliest among the servers that count. One server collected for 40 days and one added
       2 days ago, both with runs in the range: the first covers the range, so there is no note; the second's own view names its first
       collection. With only the newer server in the store, the All Servers view names that first collection. */
    [Fact]
    public async Task TheAllServersView_TakesTheEarliestCoverageAmongTheServersThatCount_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using (var both = await Store.CreateAsync(ct))
        {
            await both.AddServerAsync(ServerA, both.End.AddDays(-40), ct);
            await both.LogRunsAsync(ServerA, both.End.AddDays(-40), both.End, ct);
            await both.InsertRunsAsync(ServerA, both.Start.AddHours(5), both.End, TimeSpan.FromHours(6), collectedAtUtc: null, ct);
            await both.AddServerAsync(ServerB, both.End.AddDays(-2), ct);
            await both.LogRunsAsync(ServerB, both.End.AddDays(-2), both.End, ct);
            await both.InsertRunsAsync(ServerB, both.End.AddDays(-2).AddHours(3), both.End, TimeSpan.FromHours(6), collectedAtUtc: both.End.AddDays(-2), ct);

            var fleet = await both.Viewer.GetJobHistoryDataStartAsync(null, both.Start, both.End, ct);
            Assert.NotNull(fleet);
            Assert.True(fleet <= both.Start, $"the fleet answer {fleet:o} should reach the range's start, the older server's coverage");
            Assert.Null(await both.NoteAsync(null, ct));
            Assert.Equal(Since(both.End.AddDays(-2)), await both.NoteAsync(ServerB, ct));
        }

        await using var newer = await Store.CreateAsync(ct);
        await newer.AddServerAsync(ServerB, newer.End.AddDays(-2), ct);
        await newer.LogRunsAsync(ServerB, newer.End.AddDays(-2), newer.End, ct);
        await newer.InsertRunsAsync(ServerB, newer.End.AddDays(-2).AddHours(3), newer.End, TimeSpan.FromHours(6), collectedAtUtc: newer.End.AddDays(-2), ct);

        Assert.Equal(newer.End.AddDays(-2), await newer.Viewer.GetJobHistoryDataStartAsync(null, newer.Start, newer.End, ct));
        Assert.Equal(Since(newer.End.AddDays(-2)), await newer.NoteAsync(null, ct));
    }

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

        /// <summary>The end of the 7-day range every test reads, to the minute.</summary>
        public DateTime End { get; }

        public DateTime Start => End.AddDays(-7);

        public static async Task<Store> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live Job History data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);
                }

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                return new Store(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>
        /// The note the tab raises over the range: the probe and the read it starts from one start, handed to the tab's own step.
        /// Null when it is hidden.
        /// </summary>
        public async Task<string?> NoteAsync(int? serverId, CancellationToken ct)
        {
            var probe = Viewer.GetJobHistoryDataStartAsync(serverId, Start, End, ct);
            var read = await Viewer.GetJobHistoryAsync(Start, serverId, JobHistoryTab.RowCap, ct);
            await probe.WaitAsync(ct);

            string? text = null;
            OnStaThread(() =>
            {
                ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
                /* Seeded visible, so a no-op cannot pass as a hidden note. */
                var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

                JobHistoryTab.ShowJobHistoryDataStartAsync(banner, probe, Start, read).GetAwaiter().GetResult();

                text = banner.Visibility == Visibility.Visible ? banner.Text : null;
            });
            return text;
        }

        /* The registry row (created_date: the server's first successful connect). */
        public async Task AddServerAsync(int serverId, DateTime addedUtc, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName(serverId), ct);

            await using var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection);
            update.Parameters.AddWithValue(serverId);
            update.Parameters.AddWithValue(Naive(addedUtc));
            await update.ExecuteNonQueryAsync(ct);
        }

        /* The collector's runs from fromUtc to toUtc, every 30 minutes, whether or not a job ran. */
        public async Task LogRunsAsync(int serverId, DateTime fromUtc, DateTime toUtc, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT row_number() OVER () + $6, $1, $2, $5, t, 12, 'SUCCESS', 0
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(ServerName(serverId));
            insert.Parameters.AddWithValue(Naive(fromUtc));
            insert.Parameters.AddWithValue(Naive(toUtc));
            insert.Parameters.AddWithValue(Collector);
            insert.Parameters.AddWithValue(-(long)serverId * 1000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* One job-outcome row at each step from firstUtc to lastUtc. All are stored at collectedAtUtc (a first collection copying
           history), or, with none, each at its own run time (a collector keeping up with the server). The server has no clock row,
           so its stored run time reads as UTC. */
        public async Task InsertRunsAsync(int serverId, DateTime firstUtc, DateTime lastUtc, TimeSpan step, DateTime? collectedAtUtc, CancellationToken ct)
        {
            await using var connection = await OpenAsync(ct);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name,
     run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT row_number() OVER (ORDER BY t) + $7, COALESCE($6, t), $1, $2, row_number() OVER (ORDER BY t), 'job-1', 'Nightly', true, 0, '(Job outcome)',
       1, 'The job succeeded.', t, 5, 0, NULL
FROM generate_series($3::timestamp, $4::timestamp, $5::interval) AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(ServerName(serverId));
            insert.Parameters.AddWithValue(Naive(firstUtc));
            insert.Parameters.AddWithValue(Naive(lastUtc));
            insert.Parameters.AddWithValue(step);
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectedAtUtc is DateTime c ? Naive(c) : DBNull.Value });
            insert.Parameters.AddWithValue(-(long)serverId * 1000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        private async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
        {
            var connection = new NpgsqlConnection(_scratch.ConnectionString);
            await connection.OpenAsync(ct);
            return connection;
        }

        private static string ServerName(int serverId) => $"job-history-start-{-serverId}";

        /* SpecifyKind(Unspecified), not the bare value: Npgsql infers timestamptz from Kind=Utc and PostgreSQL then zone-shifts the bounds against the store's NAIVE timestamp columns. */
        private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

        public async ValueTask DisposeAsync()
        {
            await Viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }

    /* WPF objects require STA; same shape as ViewerJobHistoryDataStartTests. */
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
