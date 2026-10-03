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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's System Events and Default Trace grids against a real store (#4966). Both window on the event's own
/// time and a server's first collection stores the server's event history, so the notice names the earlier of the
/// collector's coverage start and the earliest event the grid shows. Four stores per grid, each read through the same two
/// calls the server tab makes (the grid's rows, the data-start probe) and the same rule it applies. Default Trace stores
/// the server's own clock: every server here runs 5 hours behind UTC, so a notice that skipped the per-row conversion
/// would name the history's start 5 hours early.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerSystemEventsDataStartLiveTests
{
    public enum Grid
    {
        SystemEvents,
        DefaultTrace,
    }

    /// <summary>The server's clock: 5 hours behind UTC, as a server in the US Central daylight zone reports.</summary>
    private const int ClockOffsetMinutes = -300;

    private const int NewServerId = -496611;
    private const int QuietServerId = -496612;
    private const int HistoryServerId = -496613;
    private const int DeepHistoryServerId = -496614;

    /* Rows start inside the range: the server was added 2 days ago, and its first event came a day later. The notice reads
       back the coverage start, the day the server was added. */
    [Theory]
    [InlineData(Grid.SystemEvents)]
    [InlineData(Grid.DefaultTrace)]
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
    [InlineData(Grid.SystemEvents)]
    [InlineData(Grid.DefaultTrace)]
    public async Task AQuietStart_GivesNoNotice_AgainstDevPostgres(Grid grid)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(grid, ct);

        var notice = await store.NoticeAsync(grid, QuietServerId, ct);

        Assert.NotNull(notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    /* History that reaches before the coverage start: the server was added 2 days ago, and its first collection stored
       events from 5 days ago. The notice names the history's start, not the later day the server was added. For Default
       Trace the events are stored on the server's clock, 5 hours behind: the notice is the same instant in UTC. */
    [Theory]
    [InlineData(Grid.SystemEvents)]
    [InlineData(Grid.DefaultTrace)]
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
    [InlineData(Grid.SystemEvents)]
    [InlineData(Grid.DefaultTrace)]
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
            Grid.SystemEvents => _viewer.GetSystemHealthEventsDataStartAsync(serverId, Start, End, ct),
            _ => _viewer.GetDefaultTraceDataStartAsync(serverId, Start, End, ct),
        };

        /// <summary>The instant the server tab's notice names: the probe's answer, and the grid's rows, through the tab's rule.</summary>
        public async Task<DateTime?> NoticeAsync(Grid grid, int serverId, CancellationToken ct)
        {
            var coverage = await CoverageAsync(grid, serverId, ct);
            if (grid == Grid.SystemEvents)
            {
                /* One of the eight grids that read system_health_events: Severe Errors. The row's event time is the XE
                   timestamp the shred reads off the event, which the seed keeps equal to the table's event_time. */
                var errors = await _viewer.GetSevereErrorsAsync(serverId, Start, End, cancellationToken: ct);
                Assert.NotEmpty(errors);
                return ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(errors.Select(r => r.EventTime)));
            }

            var trace = await _viewer.GetDefaultTraceEventsAsync(serverId, Start, End, cancellationToken: ct);
            Assert.NotEmpty(trace);
            return ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(trace.Select(r => r.EventTimeUtc)));
        }

        public static async Task<SeededStore> CreateAsync(Grid grid, CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live event data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                DateTime end;
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    var now = DateTime.UtcNow;
                    end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                    var table = grid == Grid.SystemEvents ? "system_health_events" : "default_trace_events";
                    var collector = DataWindowFloor.Source.ForCollectorTable(table).CollectorName!;

                    /* Added 2 days ago; its first event came a day later, collected when it happened. */
                    await EventGridSeed.AddServerAsync(connection, NewServerId, "event-grids-new", end.AddDays(-2), collector, end, ClockOffsetMinutes, ct);
                    await InsertEventsAsync(connection, grid, NewServerId, end.AddDays(-1), end, collectedFirst: null, ct);

                    /* Monitored for 120 days: the store ran the collector the whole range, and the range's first event
                       came 5 hours in. */
                    await EventGridSeed.AddServerAsync(connection, QuietServerId, "event-grids-quiet", end.AddDays(-120), collector, end, ClockOffsetMinutes, ct);
                    await InsertEventsAsync(connection, grid, QuietServerId, end.AddDays(-7).AddHours(5), end, collectedFirst: null, ct);

                    /* Added 2 days ago; its first collection stored the events of the 3 days before that, then the
                       collector kept up. */
                    await EventGridSeed.AddServerAsync(connection, HistoryServerId, "event-grids-history", end.AddDays(-2), collector, end, ClockOffsetMinutes, ct);
                    await InsertEventsAsync(connection, grid, HistoryServerId, end.AddDays(-5), end.AddDays(-2), collectedFirst: end.AddDays(-2), ct);
                    await InsertEventsAsync(connection, grid, HistoryServerId, end.AddDays(-2).AddHours(6), end, collectedFirst: null, ct);

                    /* The same, with 7 days of history before it, so the first collection stored the range's own start. */
                    await EventGridSeed.AddServerAsync(connection, DeepHistoryServerId, "event-grids-deep-history", end.AddDays(-2), collector, end, ClockOffsetMinutes, ct);
                    await InsertEventsAsync(connection, grid, DeepHistoryServerId, end.AddDays(-9), end.AddDays(-2), collectedFirst: end.AddDays(-2), ct);
                    await InsertEventsAsync(connection, grid, DeepHistoryServerId, end.AddDays(-2).AddHours(6), end, collectedFirst: null, ct);
                }

                return new SeededStore(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /* One event every 6 hours from firstUtc to lastUtc (UTC instants), collected when it happened, or all in one first
           collection at collectedFirst (the history a server's first collection stores). A system_health_events row carries
           its time twice, in the column the read windows on and in the XE timestamp the grid shows; both are the same UTC
           instant. A default_trace_events row carries the server's own clock, 5 hours behind. */
        private static async Task InsertEventsAsync(
            NpgsqlConnection connection, Grid grid, int serverId, DateTime firstUtc, DateTime lastUtc, DateTime? collectedFirst, CancellationToken ct)
        {
            var sql = grid == Grid.SystemEvents
                ? """
                    INSERT INTO collect.system_health_events
                        (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
                    SELECT
                        row_number() OVER () + $4,
                        COALESCE($5::timestamp, t),
                        $1,
                        'event-grids',
                        t,
                        'error_reported',
                        '<event name="error_reported" package="sqlserver" timestamp="' || to_char(t, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') || '">'
                        || '<data name="error_number"><value>823</value></data><data name="severity"><value>24</value></data>'
                        || '<data name="state"><value>2</value></data><data name="message"><value>read error</value></data></event>'
                    FROM generate_series($2::timestamp, $3::timestamp, interval '6 hours') AS t
                    """
                : """
                    INSERT INTO collect.default_trace_events
                        (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, event_class)
                    SELECT
                        row_number() OVER () + $4,
                        COALESCE($5::timestamp, t),
                        $1,
                        'event-grids',
                        t + make_interval(mins => $6),
                        'Data File Auto Grow',
                        92
                    FROM generate_series($2::timestamp, $3::timestamp, interval '6 hours') AS t
                    """;

            await using var insert = new NpgsqlCommand(sql, connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(lastUtc, DateTimeKind.Unspecified));
            /* Distinct ids per call: a store keys a row on its id, and two calls for one server share an id range otherwise. */
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L + (collectedFirst is null ? 50_000L : 0L));
            insert.Parameters.Add(new NpgsqlParameter { Value = collectedFirst is DateTime c ? DateTime.SpecifyKind(c, DateTimeKind.Unspecified) : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
            if (grid == Grid.DefaultTrace)
            {
                insert.Parameters.AddWithValue(ClockOffsetMinutes);
            }

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
/// The caps of the desktop viewer's two Blocking grids against a real store (#4966): a read that returns a full page of its
/// newest rows (200 blocked process reports, 50 deadlocks) names its oldest row in the notice, even where the store covers
/// the whole range, because the grid reaches back no further than that row. A read under its cap keeps the result it had.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ViewerEventCapDataStartLiveTests
{
    public enum Grid
    {
        BlockedProcessReports,
        Deadlocks,
    }

    private const int ServerId = -496621;

    private static int CapOf(Grid grid) => grid == Grid.BlockedProcessReports
        ? ViewerDataService.BlockedProcessReportsRowCap
        : ViewerDataService.DeadlocksRowCap;

    /* A server monitored for 120 days, so the store covers the whole 7-day range, and `seeded` events one minute apart ending
       at the range's end. The read keeps the newest `cap` of them, so its oldest row came `cap - 1` minutes before the end:
       inside the range, days after the range's start. A full page (the cap exactly, or more rows than it) names that row. */
    [Theory]
    [InlineData(Grid.BlockedProcessReports, 200)]
    [InlineData(Grid.BlockedProcessReports, 260)]
    [InlineData(Grid.Deadlocks, 50)]
    [InlineData(Grid.Deadlocks, 70)]
    public async Task AFullPage_GivesANotice_AtItsOldestRow_ThoughTheStoreCoversTheRange_AgainstDevPostgres(Grid grid, int seeded)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await CapStore.CreateAsync(grid, seeded, ct);

        var (coverage, shown) = await store.ReadAsync(grid, ct);
        var notice = ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(shown), ViewerEventDataStart.ReadHitCap(shown.Count, CapOf(grid)));

        Assert.Equal(CapOf(grid), shown.Count);
        Assert.Equal(store.End.AddMinutes(-(CapOf(grid) - 1)), notice);
        Assert.True(RawWindowFloor.IsTruncated(notice, store.Start));

        /* The cap is what raises it: without the cap rule the store's coverage reaches the range start, and nothing shows. */
        Assert.False(RawWindowFloor.IsTruncated(ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(shown)), store.Start));
    }

    /* One row under the cap: the read returned everything the range holds, so the coverage rule stands and the covered range
       shows no notice. */
    [Theory]
    [InlineData(Grid.BlockedProcessReports)]
    [InlineData(Grid.Deadlocks)]
    public async Task AReadUnderItsCap_KeepsItsResult_AgainstDevPostgres(Grid grid)
    {
        var ct = TestContext.Current.CancellationToken;
        var seeded = CapOf(grid) - 1;
        await using var store = await CapStore.CreateAsync(grid, seeded, ct);

        var (coverage, shown) = await store.ReadAsync(grid, ct);
        var notice = ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(shown), ViewerEventDataStart.ReadHitCap(shown.Count, CapOf(grid)));

        Assert.Equal(seeded, shown.Count);
        Assert.NotNull(notice);
        Assert.Equal(ViewerEventDataStart.Of(coverage, ViewerEventDataStart.EarliestOf(shown)), notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, store.Start));
    }

    private sealed class CapStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;
        private readonly ViewerDataService _viewer;

        private CapStore(ScratchPostgres scratch, ViewerDataService viewer, DateTime end)
        {
            _scratch = scratch;
            _viewer = viewer;
            End = end;
        }

        public DateTime End { get; }

        public DateTime Start => End.AddDays(-7);

        /// <summary>The probe's answer and the event time of every row the grid's read returned.</summary>
        public async Task<(DateTime? Coverage, List<DateTime?> Shown)> ReadAsync(Grid grid, CancellationToken ct)
        {
            if (grid == Grid.BlockedProcessReports)
            {
                var coverage = await _viewer.GetBlockedProcessReportsDataStartAsync(ServerId, Start, End, ct);
                var rows = await _viewer.GetRecentBlockedProcessReportsAsync(ServerId, Start, End, cancellationToken: ct);
                return (coverage, rows.Select(r => r.EventTime).ToList());
            }

            var deadlockCoverage = await _viewer.GetDeadlocksDataStartAsync(ServerId, Start, End, ct);
            var deadlocks = await _viewer.GetRecentDeadlocksAsync(ServerId, Start, End, ct);
            return (deadlockCoverage, deadlocks.Select(r => (DateTime?)r.DeadlockTime).ToList());
        }

        public static async Task<CapStore> CreateAsync(Grid grid, int seeded, CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live event data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                DateTime end;
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);

                    var now = DateTime.UtcNow;
                    end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                    var collector = grid == Grid.BlockedProcessReports ? "blocked_process_report" : "deadlocks";

                    await EventGridSeed.AddServerAsync(connection, ServerId, "event-cap", end.AddDays(-120), collector, end, clockOffsetMinutes: null, ct);

                    var sql = grid == Grid.BlockedProcessReports
                        ? """
                            INSERT INTO collect.blocked_process_reports
                                (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms)
                            SELECT row_number() OVER () + $4, t, $1, 'event-cap', t, 'EventDb', 55, 56, 1000
                            FROM generate_series($2::timestamp, $3::timestamp, interval '1 minute') AS t
                            """
                        : """
                            INSERT INTO collect.deadlocks
                                (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
                            SELECT row_number() OVER () + $4, t, $1, 'event-cap', t, 'process1', 'SELECT 1', '<deadlock />'
                            FROM generate_series($2::timestamp, $3::timestamp, interval '1 minute') AS t
                            """;

                    await using var insert = new NpgsqlCommand(sql, connection);
                    insert.Parameters.AddWithValue(ServerId);
                    insert.Parameters.AddWithValue(DateTime.SpecifyKind(end.AddMinutes(-(seeded - 1)), DateTimeKind.Unspecified));
                    insert.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
                    insert.Parameters.AddWithValue(Math.Abs((long)ServerId) * 100_000L);
                    await insert.ExecuteNonQueryAsync(ct);
                }

                return new CapStore(scratch, new ViewerDataService(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _viewer.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>The registry row and the collector's runs a store needs before a data-start probe can answer about a server.</summary>
internal static class EventGridSeed
{
    /* The registry row (created_date: the server's first successful connect), the collector's runs from then on, every 30
       minutes whether or not anything happened, and, when the server has a clock, the offset the viewer converts with. */
    public static async Task AddServerAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime addedUtc, string collector, DateTime endUtc,
        int? clockOffsetMinutes, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

        await using (var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection))
        {
            update.Parameters.AddWithValue(serverId);
            update.Parameters.AddWithValue(DateTime.SpecifyKind(addedUtc, DateTimeKind.Unspecified));
            await update.ExecuteNonQueryAsync(ct);
        }

        await using (var insert = new NpgsqlCommand("""
            INSERT INTO collect.collection_log
                (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
            SELECT row_number() OVER () + $6, $1, $2, $5, t, 12, 'SUCCESS', 0
            FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t
            """, connection))
        {
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(addedUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(collector);
            insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 100_000L);
            await insert.ExecuteNonQueryAsync(ct);
        }

        if (clockOffsetMinutes is int offset)
        {
            await using var properties = new NpgsqlCommand(
                "INSERT INTO collect.server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES ($1, $2, $3, $4, $5)", connection);
            properties.Parameters.AddWithValue(Math.Abs((long)serverId));
            properties.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
            properties.Parameters.AddWithValue(serverId);
            properties.Parameters.AddWithValue(serverName);
            properties.Parameters.AddWithValue(offset);
            await properties.ExecuteNonQueryAsync(ct);
        }
    }
}
