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
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's blocked process reports and deadlocks grids against a real store (#4966). The grids window on
/// the event's own time, and a server's first collection stores the server's event history, so the notice names the
/// earlier of the collector's coverage start and the earliest event the grid shows. Four stores, each read through the
/// same two calls the server tab makes (the grid's rows, the data-start probe) and the same rule it applies.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerEventDataStartLiveTests
{
    public enum Grid
    {
        BlockedProcessReports,
        Deadlocks,
    }

    private const int NewServerId = -496601;
    private const string NewServerName = "event-start-new";
    private const int QuietServerId = -496602;
    private const string QuietServerName = "event-start-quiet";
    private const int HistoryServerId = -496603;
    private const string HistoryServerName = "event-start-history";
    private const int DeepHistoryServerId = -496604;
    private const string DeepHistoryServerName = "event-start-deep-history";

    /* Rows start inside the range: the server was added 2 days ago, and its first event came a day later. The notice reads
       back the coverage start, the day the server was added. */
    [Theory]
    [InlineData(Grid.BlockedProcessReports)]
    [InlineData(Grid.Deadlocks)]
    public async Task RowsThatStartInsideTheRange_GiveANotice_AtTheCoverageStart_AgainstDevPostgres(Grid grid)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(grid, ct);

        var notice = await store.NoticeAsync(grid, NewServerId, ct);

        Assert.Equal(store.End.AddDays(-2), notice);
        Assert.True(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* A quiet start: monitored for months, and the store covered the whole range, but the first event of the range came 5
       hours in. No notice. */
    [Theory]
    [InlineData(Grid.BlockedProcessReports)]
    [InlineData(Grid.Deadlocks)]
    public async Task AQuietStart_GivesNoNotice_AgainstDevPostgres(Grid grid)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(grid, ct);

        var notice = await store.NoticeAsync(grid, QuietServerId, ct);

        Assert.NotNull(notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* History that reaches before the coverage start: the server was added 2 days ago, and its first collection stored
       events from 5 days ago. The notice names the history's start, not the later day the server was added. */
    [Theory]
    [InlineData(Grid.BlockedProcessReports)]
    [InlineData(Grid.Deadlocks)]
    public async Task HistoryThatReachesBeforeTheCoverage_GivesANotice_AtTheHistorysStart_AgainstDevPostgres(Grid grid)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(grid, ct);

        var coverage = await store.CoverageAsync(grid, HistoryServerId, ct);
        var notice = await store.NoticeAsync(grid, HistoryServerId, ct);

        Assert.Equal(store.End.AddDays(-2), coverage);
        Assert.Equal(store.End.AddDays(-5), notice);
        Assert.True(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* History that reaches the range start: the same recent server, its first collection stored events from 9 days ago,
       so the grid's first row is the range's own start. No notice, though the coverage starts 5 days later. */
    [Theory]
    [InlineData(Grid.BlockedProcessReports)]
    [InlineData(Grid.Deadlocks)]
    public async Task HistoryThatReachesTheRangeStart_GivesNoNotice_AgainstDevPostgres(Grid grid)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(grid, ct);

        var coverage = await store.CoverageAsync(grid, DeepHistoryServerId, ct);
        var notice = await store.NoticeAsync(grid, DeepHistoryServerId, ct);

        Assert.Equal(store.End.AddDays(-2), coverage);
        Assert.NotNull(notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;
        private readonly ViewerDataService _viewer;

        private SeededStore(ScratchPostgres scratch, ViewerDataService viewer, DateTime end)
        {
            _scratch = scratch;
            _viewer = viewer;
            End = end;
        }

        public DateTime End { get; }

        /// <summary>The start of the 7-day range every test reads.</summary>
        public DateTime Start => End.AddDays(-7);

        /// <summary>Where the collector's coverage starts for the range: the probe's own answer.</summary>
        public Task<DateTime?> CoverageAsync(Grid grid, int serverId, CancellationToken ct) => grid switch
        {
            Grid.BlockedProcessReports => _viewer.GetBlockedProcessReportsDataStartAsync(serverId, Start, End, ct),
            _ => _viewer.GetDeadlocksDataStartAsync(serverId, Start, End, ct),
        };

        /// <summary>The instant the server tab's notice names: the probe's answer, and the grid's rows, through the tab's rule.</summary>
        public async Task<DateTime?> NoticeAsync(Grid grid, int serverId, CancellationToken ct)
        {
            var coverage = await CoverageAsync(grid, serverId, ct);
            if (grid == Grid.BlockedProcessReports)
            {
                var rows = await _viewer.GetRecentBlockedProcessReportsAsync(serverId, Start, End, cancellationToken: ct);
                Assert.NotEmpty(rows);
                return ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(rows.Select(r => r.EventTime)));
            }

            var deadlocks = await _viewer.GetRecentDeadlocksAsync(serverId, Start, End, ct);
            Assert.NotEmpty(deadlocks);
            return ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(deadlocks.Select(r => r.DeadlockTime)));
        }

        public static async Task<SeededStore> CreateAsync(Grid grid, CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live event data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    var now = DateTime.UtcNow;
                    var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                    var collector = grid == Grid.BlockedProcessReports ? "blocked_process_report" : "deadlocks";

                    /* Added 2 days ago; its first event came a day later, collected when it happened. */
                    await AddServerAsync(connection, NewServerId, NewServerName, end.AddDays(-2), collector, end, ct);
                    await InsertEventsAsync(connection, grid, NewServerId, NewServerName, end.AddDays(-1), end, collectedFirst: null, ct);

                    /* Monitored for 120 days: the store ran the collector the whole range, and the range's first event
                       came 5 hours in. */
                    await AddServerAsync(connection, QuietServerId, QuietServerName, end.AddDays(-120), collector, end, ct);
                    await InsertEventsAsync(connection, grid, QuietServerId, QuietServerName, end.AddDays(-7).AddHours(5), end, collectedFirst: null, ct);

                    /* Added 2 days ago; its first collection stored the events of the 3 days before that, then the
                       collector kept up. */
                    await AddServerAsync(connection, HistoryServerId, HistoryServerName, end.AddDays(-2), collector, end, ct);
                    await InsertEventsAsync(connection, grid, HistoryServerId, HistoryServerName, end.AddDays(-5), end.AddDays(-2), collectedFirst: end.AddDays(-2), ct);
                    await InsertEventsAsync(connection, grid, HistoryServerId, HistoryServerName, end.AddDays(-2).AddHours(6), end, collectedFirst: null, ct);

                    /* The same, with 7 days of history before it, so the first collection stored the range's own start. */
                    await AddServerAsync(connection, DeepHistoryServerId, DeepHistoryServerName, end.AddDays(-2), collector, end, ct);
                    await InsertEventsAsync(connection, grid, DeepHistoryServerId, DeepHistoryServerName, end.AddDays(-9), end.AddDays(-2), collectedFirst: end.AddDays(-2), ct);
                    await InsertEventsAsync(connection, grid, DeepHistoryServerId, DeepHistoryServerName, end.AddDays(-2).AddHours(6), end, collectedFirst: null, ct);
                }

                var viewer = new ViewerDataService(scratch.ConnectionString);
                var end2 = await EndAsync(scratch.ConnectionString, ct);
                return new SeededStore(scratch, viewer, end2);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /* The seed's own "end", recovered from the store (the registry row the quiet server's runs end on), so the reads and
           the seed agree on it to the minute. */
        private static async Task<DateTime> EndAsync(string connectionString, CancellationToken ct)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await using var select = new NpgsqlCommand("SELECT MAX(collection_time) FROM collect.collection_log WHERE server_id = $1", connection);
            select.Parameters.AddWithValue(QuietServerId);
            var value = await select.ExecuteScalarAsync(ct);
            return DateTime.SpecifyKind((DateTime)value!, DateTimeKind.Utc);
        }

        /* The registry row (created_date: the server's first successful connect) and the collector's runs from then on,
           every 30 minutes, whether or not anything happened. */
        private static async Task AddServerAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime addedUtc, string collector, DateTime endUtc, CancellationToken ct)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

            await using (var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection))
            {
                update.Parameters.AddWithValue(serverId);
                update.Parameters.AddWithValue(DateTime.SpecifyKind(addedUtc, DateTimeKind.Unspecified));
                await update.ExecuteNonQueryAsync(ct);
            }

            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT
    row_number() OVER () + $6,
    $1,
    $2,
    $5,
    t,
    12,
    'SUCCESS',
    0
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(addedUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(collector);
            insert.Parameters.AddWithValue(-(long)serverId * 1000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* One event every 6 hours from firstUtc to lastUtc, few enough to stay under the grids' caps (200 reports, 50
           deadlocks), so the earliest row shown is the earliest event. Collected when it happened, or all in one first
           collection at collectedFirst (the history a server's first collection stores). */
        private static async Task InsertEventsAsync(
            NpgsqlConnection connection, Grid grid, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, DateTime? collectedFirst, CancellationToken ct)
        {
            var sql = grid == Grid.BlockedProcessReports
                ? @"
INSERT INTO collect.blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms)
SELECT
    row_number() OVER () + $6,
    COALESCE($5::timestamp, t),
    $1,
    $2,
    t,
    'EventDb',
    55,
    56,
    1000
FROM generate_series($3::timestamp, $4::timestamp, interval '6 hours') AS t"
                : @"
INSERT INTO collect.deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
SELECT
    row_number() OVER () + $6,
    COALESCE($5::timestamp, t),
    $1,
    $2,
    t,
    'process1',
    'SELECT 1',
    '<deadlock />'
FROM generate_series($3::timestamp, $4::timestamp, interval '6 hours') AS t";

            await using var insert = new NpgsqlCommand(sql, connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(lastUtc, DateTimeKind.Unspecified));
            insert.Parameters.Add(new NpgsqlParameter { Value = collectedFirst is DateTime c ? DateTime.SpecifyKind(c, DateTimeKind.Unspecified) : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
            /* Distinct ids per call: a store keys a row on its id, and two calls for one server share an id range otherwise. */
            insert.Parameters.AddWithValue(-(long)serverId * 1000L + (collectedFirst is null ? 500L : 0L));
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await _viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
