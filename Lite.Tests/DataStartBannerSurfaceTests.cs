/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: the "Showing since" notice on the Collection Log, the nine System Events grids that read stored events
/// (Scheduler Issues, Severe Errors, Memory Conditions, Memory Broker, Memory Node OOM, Significant Waits, CPU Tasks,
/// I/O Issues and Default Trace), the three Config Changes grids and Long Queries. Each one reads its rows over the
/// toolbar's window, so a window that starts before the store's coverage drew a shorter range with no notice. The
/// notice comes from the ONE shared probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the ONE banner
/// step (<see cref="ServerTab.ApplyWindowFloorToBanner"/>) the Queries tab, Active Queries and Current Waits use, and
/// each surface has the two tests the issue asks for: rows that start inside the range read the notice back, and a
/// quiet start (the collector covered the whole range, the first row comes late) shows none.
///
/// <para>The surfaces with no notice are pinned too (<see cref="SurfacesWithoutANotice_KeepTheShapeThatNeedsNone"/>):
/// the Health Summary is a fixed seven-day aggregate that does not read the toolbar's range, and the Duration Trends,
/// Corruption Events and Contention Events charts pin their time axis to the asked range, so an empty span is already
/// drawn as one.</para>
/// </summary>
[Collection("server-time-helper")]
public sealed class DataStartBannerSurfaceTests : IDisposable
{
    private const int ServerId = 4343;
    private const string ServerName = "SurfaceBannerServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DataStartBannerSurfaceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "SurfaceBanner_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>One relation per surface that gets a notice. The eight system_health grids share a relation, as they
    /// share a collector and a table; the wiring test below pins each grid's own banner and call.</summary>
    public static TheoryData<QueryWindowRelation> NoticeRelations => new()
    {
        QueryWindowRelation.CollectionLog,
        QueryWindowRelation.SystemHealthEvents,
        QueryWindowRelation.DefaultTraceEvents,
        QueryWindowRelation.ServerConfig,
        QueryWindowRelation.DatabaseConfig,
        QueryWindowRelation.TraceFlags,
        QueryWindowRelation.LongQueryCompletions
    };

    /// <summary>The two relations whose grids filter on <c>event_time</c>, the event's own time, and not on
    /// <c>collection_time</c>, the time a run stored it (#4989): the System Events grids that read system_health
    /// events, and the Default Trace grid.</summary>
    public static TheoryData<QueryWindowRelation> EventRelations => new()
    {
        QueryWindowRelation.SystemHealthEvents,
        QueryWindowRelation.DefaultTraceEvents
    };

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static string Literal(DateTime instant) =>
        "TIMESTAMP '" + Naive(instant).ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";

    /// <summary>
    /// One row in the relation's table with its time column at <paramref name="at"/>. Every other column the table
    /// requires (NOT NULL without a default) gets a placeholder of its type, found from the table itself, so the test
    /// does not repeat any collector's column list. The two event tables carry two clocks (#4989): <c>event_time</c>, the
    /// event's own time and nullable, and <c>collection_time</c>, the time a run stored the row. A row there sets BOTH,
    /// whatever column the probe measures, so a test of one clock cannot lean on the other: <paramref name="at"/> is the
    /// event's time, and <paramref name="collectedAt"/> (<paramref name="at"/> when not given) the time the run stamped,
    /// which is later for the history a server's first run stores. Any other relation has the one time column.
    /// </summary>
    private async Task SeedRowAsync(QueryWindowRelation relation, DateTime at, DateTime? collectedAt = null)
    {
        var table = LocalDataService.QueryWindowRelationView(relation)[2..];
        var times = relation is QueryWindowRelation.SystemHealthEvents or QueryWindowRelation.DefaultTraceEvents
            ? new Dictionary<string, DateTime> { ["event_time"] = at, ["collection_time"] = collectedAt ?? at }
            : new Dictionary<string, DateTime> { [LocalDataService.QueryWindowRelationTimeColumn(relation)] = at };

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();

        var names = new List<string>();
        var values = new List<string>();
        using (var info = connection.CreateCommand())
        {
            info.CommandText = $"SELECT name, type, \"notnull\", pk, dflt_value IS NOT NULL FROM pragma_table_info('{table}') ORDER BY cid";
            using var reader = await info.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var name = reader.GetString(0);
                var type = reader.GetString(1);
                var pk = reader.GetBoolean(3);
                if (!times.ContainsKey(name) && (!(reader.GetBoolean(2) || pk) || reader.GetBoolean(4)))
                {
                    continue;
                }

                names.Add(name);
                values.Add(PlaceholderFor(name, type, pk, times, at));
            }
        }

        using var insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {table} ({string.Join(", ", names)}) VALUES ({string.Join(", ", values)})";
        await insert.ExecuteNonQueryAsync();
    }

    private string PlaceholderFor(string name, string type, bool pk, Dictionary<string, DateTime> times, DateTime at)
    {
        if (times.TryGetValue(name, out var time)) return Literal(time);
        if (name == "server_id") return ServerId.ToString();
        if (name == "server_name") return $"'{ServerName}'";
        if (pk) return (_nextId++).ToString();

        var upper = type.ToUpperInvariant();
        if (upper.StartsWith("VARCHAR", StringComparison.Ordinal)) return "'x'";
        if (upper.StartsWith("TIMESTAMP", StringComparison.Ordinal)) return Literal(at);
        if (upper == "BOOLEAN") return "false";
        if (upper.Contains("INT", StringComparison.Ordinal) || upper.StartsWith("DECIMAL", StringComparison.Ordinal)
            || upper is "DOUBLE" or "FLOAT" or "REAL")
        {
            return $"CAST(1 AS {type})";
        }

        return $"CAST('x' AS {type})";
    }

    /// <summary>
    /// The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes from
    /// <paramref name="firstUtc"/> to <paramref name="lastUtc"/>, whether or not they stored a row.
    /// </summary>
    private async Task SeedLogRunsAsync(string collector, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT {_nextId} + row_number() OVER (), {ServerId}, '{ServerName}', '{collector}', g.t, 12, 'SUCCESS', 0
FROM generate_series({Literal(firstUtc)}, {Literal(lastUtc)}, INTERVAL {everyMinutes} MINUTE) AS g(t)";
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    /// <summary>The probe, then the banner step the surface runs on the result: (visible, text). The probe reads the Default
    /// Trace through <paramref name="clock"/>, the monitored server's clock (#4989). A test that names none is a UTC server's,
    /// whatever clock the process holds as the active one.</summary>
    private async Task<(bool Visible, string Text)> BannerForAsync(QueryWindowRelation relation, DateTime startUtc, DateTime endUtc, ServerClock? clock = null)
    {
        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(relation, ServerId, startUtc, endUtc, clock ?? ServerClock.Utc);
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.ApplyWindowFloorToBanner(banner, floor, startUtc, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });
    }

    /// <summary>
    /// A range that starts before the first stored row, and before the collector's first run, gets the notice, worded
    /// at that first row or run. Covers the config snapshots too, whose time column is <c>capture_time</c>.
    /// </summary>
    [Theory]
    [MemberData(nameof(NoticeRelations))]
    public async Task RowsStartInsideTheRange_ShowTheNotice(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var first = end.AddDays(-2);
        await SeedRowAsync(relation, first);
        await SeedRowAsync(relation, end.AddHours(-1));
        if (LocalDataService.QueryWindowRelationCollector(relation) is { } collector)
        {
            await SeedLogRunsAsync(collector, first, end, 60);
        }

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(first).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// A quiet start is no reason for a notice. The store covered the whole range, but the first row inside it comes
    /// three days in: for an event or snapshot table the collector ran for a month and stored nothing near the start,
    /// and for the run log itself the oldest row is a month old.
    /// </summary>
    [Theory]
    [MemberData(nameof(NoticeRelations))]
    public async Task QuietStart_TheStoreCoveredTheRange_ShowsNoNotice(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        if (LocalDataService.QueryWindowRelationCollector(relation) is { } collector)
        {
            await SeedLogRunsAsync(collector, end.AddDays(-30), end, 180);
        }
        else
        {
            await SeedRowAsync(relation, end.AddDays(-30));
        }

        await SeedRowAsync(relation, end.AddDays(-3));
        await SeedRowAsync(relation, end.AddHours(-1));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// A server's first run (T0, <paramref name="firstRun"/>): it stores the history the server already holds, each row
    /// stamped with the run's own <c>collection_time</c> while its <c>event_time</c> goes back to the event's real time
    /// (<paramref name="historyEventTimes"/>). The collector's runs are then logged hourly to <paramref name="end"/>, and
    /// the newest one stores an event as it happens, so its two times agree.
    /// </summary>
    private async Task SeedFirstRunAsync(QueryWindowRelation relation, DateTime firstRun, DateTime end, params DateTime[] historyEventTimes)
    {
        foreach (var eventTime in historyEventTimes)
        {
            await SeedRowAsync(relation, eventTime, collectedAt: firstRun);
        }

        await SeedRowAsync(relation, end.AddHours(-1));
        await SeedLogRunsAsync(LocalDataService.QueryWindowRelationCollector(relation)!, firstRun, end, 60);
    }

    /// <summary>
    /// #4989: a server's first run stores the server's event history, every row stamped with that run's
    /// <c>collection_time</c> (T0) while its <c>event_time</c> goes back days. The grids filter on <c>event_time</c>, so a
    /// range that starts before the oldest event (S &lt; H &lt; T0) shows rows from H, and the banner names H, never T0: a
    /// banner never names a time later than the earliest row its grid shows. RED before the fix: the probe measured
    /// <c>collection_time</c>, found only T0, and said "Showing since T0" above rows from before T0.
    /// </summary>
    [Theory]
    [MemberData(nameof(EventRelations))]
    public async Task EventRelations_HistoryStoredAtTheFirstRun_NamesTheOldestEvent_NotTheRun(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var firstRun = end.AddDays(-2);
        var oldestEvent = end.AddDays(-4);
        await SeedFirstRunAsync(relation, firstRun, end, oldestEvent, end.AddDays(-3));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Contains(Naive(oldestEvent).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
        Assert.DoesNotContain(Naive(firstRun).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same first-run history, with the range starting after its oldest event (H &lt;= S): the grid's rows run back to
    /// the start of the range, so the window was served whole and there is no banner, though the run that stored the
    /// history (T0) is later than the start.
    /// </summary>
    [Theory]
    [MemberData(nameof(EventRelations))]
    public async Task EventRelations_HistoryReachesBackToTheRangeStart_ShowsNoNotice(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedFirstRunAsync(relation, end.AddDays(-2), end, end.AddDays(-10), end.AddDays(-5));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// A first run that found no history stores nothing older than itself: the store starts at that run (T0), and the
    /// banner names it.
    /// </summary>
    [Theory]
    [MemberData(nameof(EventRelations))]
    public async Task EventRelations_NoStoredHistory_NamesTheFirstRun(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var firstRun = end.AddDays(-2);
        await SeedFirstRunAsync(relation, firstRun, end);

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Contains(Naive(firstRun).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
    }

    private static DateTime NowUtcSeconds()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);
    }

    /// <summary>
    /// One Default Trace row as the collector stores it: <c>event_time</c> is the monitored SERVER's wall clock
    /// (<paramref name="serverLocal"/>) and <c>collection_time</c> the UTC time of the run that stored it (#4989). The event
    /// is a 'Server Memory Change', one the grid's significance gate keeps, so a test can hold the probe's answer against the
    /// rows the grid shows.
    /// </summary>
    private async Task SeedDefaultTraceRowAsync(DateTime serverLocal, DateTime collectedAtUtc)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name,
     database_name, duration_us, integer_data, severity, error_number, text_data)
VALUES ({_nextId++}, {Literal(collectedAtUtc)}, {ServerId}, '{ServerName}', {Literal(serverLocal)}, 'Server Memory Change',
        NULL, NULL, NULL, NULL, NULL, 'x')";
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// A server's first run (T0, <paramref name="firstRunUtc"/>) on a server whose clock is <paramref name="clock"/>: it
    /// stores the history the server already holds, each row stamped with the run's UTC <c>collection_time</c> and an
    /// <c>event_time</c> that is the server's WALL CLOCK at the event (<paramref name="historyEventsUtc"/>, run through the
    /// clock). The collector's runs are then logged hourly to <paramref name="end"/>, and the newest run stores an event as
    /// it happens.
    /// </summary>
    private async Task SeedDefaultTraceFirstRunAsync(ServerClock clock, DateTime firstRunUtc, DateTime end, params DateTime[] historyEventsUtc)
    {
        foreach (var eventUtc in historyEventsUtc)
        {
            await SeedDefaultTraceRowAsync(clock.ToServerLocal(eventUtc), firstRunUtc);
        }

        await SeedDefaultTraceRowAsync(clock.ToServerLocal(end.AddHours(-1)), end.AddHours(-1));
        await SeedLogRunsAsync(LocalDataService.QueryWindowRelationCollector(QueryWindowRelation.DefaultTraceEvents)!, firstRunUtc, end, 60);
    }

    /// <summary>
    /// #4989: the Default Trace's <c>event_time</c> is the server's wall clock, so the probe converts it through the server's
    /// clock as the grid does. On a server 5 hours behind UTC the first run (T0) stores history whose oldest event (H) is
    /// inside the range (S &lt; H &lt; T0), and the banner names H in UTC: not H less 5 hours, the stored wall clock read as
    /// if it were UTC, and not T0. RED before the fix: the probe compared the wall clock with the UTC window as it was and
    /// worded the banner at H shifted by 5 hours.
    /// </summary>
    [Fact]
    public async Task DefaultTrace_ServerBehindUtc_HistoryBeforeTheFirstRun_NamesTheOldestEventInUtc()
    {
        await _duckDb.InitializeAsync();
        var clock = ServerClock.FixedOffset(-300);
        var end = DateTime.UtcNow;
        var firstRun = end.AddDays(-2);
        var oldest = end.AddDays(-4);
        await SeedDefaultTraceFirstRunAsync(clock, firstRun, end, oldest, end.AddDays(-3));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.DefaultTraceEvents, end.AddDays(-7), end, clock);

        Assert.True(visible);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
        Assert.DoesNotContain(Naive(oldest.AddHours(-5)).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
        Assert.DoesNotContain(Naive(firstRun).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The oldest event (H) is before the range's start (S) once both are in UTC, so the grid's range holds the history from
    /// S on and there is no banner. On a server 5 hours AHEAD of UTC its stored wall clock (H plus 5 hours) is after S, so a
    /// probe that read the wall clock as UTC found no older row and said the data starts late. (A server behind UTC cannot
    /// show this: its wall clock reads earlier than the instant, never later.) H sits half an hour before S, inside the
    /// pre-filter's margin the probe converts row by row, and 72 hours before it, below the margin, where one query answers.
    /// </summary>
    [Theory]
    [InlineData(0.5)]
    [InlineData(72.0)]
    public async Task DefaultTrace_ServerAheadOfUtc_OldestEventBeforeTheStartInUtc_ShowsNoNotice(double hoursBeforeStart)
    {
        await _duckDb.InitializeAsync();
        var clock = ServerClock.FixedOffset(300);
        var end = DateTime.UtcNow;
        var start = end.AddDays(-7);
        await SeedDefaultTraceFirstRunAsync(clock, end.AddDays(-2), end, start.AddHours(-hoursBeforeStart), end.AddDays(-3));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.DefaultTraceEvents, start, end, clock);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// The other direction on a server 5 hours behind UTC: the oldest event (H) is 3 hours after S in UTC, so the data does
    /// start late and the banner names H, though its stored wall clock reads as 2 hours BEFORE S. A probe that read the wall
    /// clock as UTC took that for an older row, and hid the banner.
    /// </summary>
    [Fact]
    public async Task DefaultTrace_ServerBehindUtc_OldestEventJustAfterTheStartInUtc_ShowsTheNotice()
    {
        await _duckDb.InitializeAsync();
        var clock = ServerClock.FixedOffset(-300);
        var end = DateTime.UtcNow;
        var start = end.AddDays(-7);
        var oldest = start.AddHours(3);
        await SeedDefaultTraceFirstRunAsync(clock, end.AddDays(-2), end, oldest);

        var (visible, text) = await BannerForAsync(QueryWindowRelation.DefaultTraceEvents, start, end, clock);

        Assert.True(visible);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The collector's runs in <c>v_collection_log</c> are UTC and take no part in the clock: on a server 5 hours behind UTC
    /// whose first run found no history, the floor is that run as logged, not the run shifted by the clock.
    /// </summary>
    [Fact]
    public async Task DefaultTrace_ServerBehindUtc_NoStoredHistory_NamesTheFirstRunInUtc()
    {
        await _duckDb.InitializeAsync();
        var clock = ServerClock.FixedOffset(-300);
        var end = DateTime.UtcNow;
        var firstRun = end.AddDays(-2);
        await SeedDefaultTraceFirstRunAsync(clock, firstRun, end);

        var (visible, text) = await BannerForAsync(QueryWindowRelation.DefaultTraceEvents, end.AddDays(-7), end, clock);

        Assert.True(visible);
        Assert.Contains(Naive(firstRun).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// A probe that names no clock reads the one the grid falls back to, <see cref="ServerTimeHelper.ActiveServerClock"/>,
    /// the Default Trace grid's own default (it passes none), so the banner and the rows it sits over agree.
    /// </summary>
    [Fact]
    public async Task DefaultTrace_WithoutAClock_UsesTheActiveServerClockLikeTheGrid()
    {
        await _duckDb.InitializeAsync();
        var clock = ServerClock.FixedOffset(-300);
        var end = NowUtcSeconds();
        var start = end.AddDays(-7);
        var oldest = end.AddDays(-4);
        await SeedDefaultTraceFirstRunAsync(clock, end.AddDays(-2), end, oldest);

        var previous = ServerTimeHelper.ActiveServerClock;
        try
        {
            ServerTimeHelper.ActiveServerClock = clock;
            var service = new LocalDataService(_duckDb);
            var floor = await service.GetQueryWindowFloorAsync(QueryWindowRelation.DefaultTraceEvents, ServerId, start, end);
            var gridRows = await service.GetDefaultTraceEventsAsync(ServerId, fromDate: start, toDate: end);

            Assert.Equal(oldest, floor);
            Assert.Equal(floor, gridRows.Min(r => r.EventTimeUtc));
        }
        finally
        {
            ServerTimeHelper.ActiveServerClock = previous;
        }
    }

    /// <summary>
    /// Across a daylight-saving change the probe takes each row's offset from the row's OWN date, as the grid does (#4766),
    /// and the floor is exactly the oldest row the grid shows. US Eastern, 2026: the spring-forward is 8 March (02:00 EST to
    /// 03:00 EDT, 07:00 UTC) and the fall-back is 1 November (02:00 EDT to 01:00 EST, 06:00 UTC). The range straddles the
    /// change, and the oldest row is on each side of it in turn, so a probe that applied one offset to the whole range,
    /// the one at its start or the one at its end, is an hour wrong on one of the four.
    /// </summary>
    [Theory]
    [InlineData("2026-03-07 00:00:00", "2026-03-10 00:00:00", "2026-03-08 01:30:00", "2026-03-08 06:30:00")]
    [InlineData("2026-03-07 00:00:00", "2026-03-10 00:00:00", "2026-03-08 03:30:00", "2026-03-08 07:30:00")]
    [InlineData("2026-10-30 00:00:00", "2026-11-03 00:00:00", "2026-11-01 00:30:00", "2026-11-01 04:30:00")]
    [InlineData("2026-10-30 00:00:00", "2026-11-03 00:00:00", "2026-11-01 02:30:00", "2026-11-01 07:30:00")]
    public async Task DefaultTrace_RangeAcrossADaylightSavingChange_UsesTheOffsetOfTheRowsOwnDate(
        string startUtc, string endUtc, string oldestServerLocal, string expectedFloorUtc)
    {
        await _duckDb.InitializeAsync();
        var clock = ServerClock.Resolve("Eastern Standard Time", -300);
        var start = DateTime.Parse(startUtc, CultureInfo.InvariantCulture);
        var end = DateTime.Parse(endUtc, CultureInfo.InvariantCulture);
        var oldest = DateTime.Parse(oldestServerLocal, CultureInfo.InvariantCulture);
        await SeedDefaultTraceRowAsync(oldest, end.AddHours(-1));
        await SeedDefaultTraceRowAsync(oldest.AddHours(30), end.AddHours(-1));

        var service = new LocalDataService(_duckDb);
        var floor = await service.GetQueryWindowFloorAsync(QueryWindowRelation.DefaultTraceEvents, ServerId, start, end, clock);
        var gridRows = await service.GetDefaultTraceEventsAsync(ServerId, fromDate: start, toDate: end, serverClock: clock);

        Assert.Equal(DateTime.Parse(expectedFloorUtc, CultureInfo.InvariantCulture), floor);
        Assert.Equal(floor, gridRows.Min(r => r.EventTimeUtc));
    }

    /// <summary>
    /// The system_health <c>event_time</c> is the XE <c>@timestamp</c>, UTC, so a server clock never shifts it: the same
    /// history as the Default Trace case, on a server 5 hours behind UTC, names the oldest event as stored.
    /// </summary>
    [Fact]
    public async Task SystemHealth_UtcEventTime_IsNotShiftedByTheServerClock()
    {
        await _duckDb.InitializeAsync();
        var relation = QueryWindowRelation.SystemHealthEvents;
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-4);
        await SeedFirstRunAsync(relation, end.AddDays(-2), end, oldest, end.AddDays(-3));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end, ServerClock.FixedOffset(-300));

        Assert.True(visible);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:", CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
    }

    /// <summary>Only the Default Trace holds the server's wall clock (#4989); every other relation's time column is UTC.</summary>
    [Fact]
    public void OnlyTheDefaultTrace_HoldsServerLocalTime()
    {
        foreach (var relation in Enum.GetValues<QueryWindowRelation>())
        {
            Assert.Equal(relation == QueryWindowRelation.DefaultTraceEvents, LocalDataService.QueryWindowRelationTimeIsServerLocal(relation));
        }
    }

    /// <summary>
    /// Each relation reads the view its grid reads, by the column that grid filters on (#4989: <c>event_time</c> for the
    /// two event relations, <c>collection_time</c> or <c>capture_time</c> for the rest), and takes its coverage from
    /// the collector whose runs the store logs under that name. The Collection Log is the run log, so it keeps the
    /// row-only probe.
    /// </summary>
    [Theory]
    [InlineData(QueryWindowRelation.CollectionLog, "v_collection_log", null, "collection_time")]
    [InlineData(QueryWindowRelation.SystemHealthEvents, "v_system_health_events", "system_health_events", "event_time")]
    [InlineData(QueryWindowRelation.DefaultTraceEvents, "v_default_trace_events", "default_trace_events", "event_time")]
    [InlineData(QueryWindowRelation.ServerConfig, "v_server_config", "server_config", "capture_time")]
    [InlineData(QueryWindowRelation.DatabaseConfig, "v_database_config", "database_config", "capture_time")]
    [InlineData(QueryWindowRelation.TraceFlags, "v_trace_flags", "trace_flags", "capture_time")]
    [InlineData(QueryWindowRelation.LongQueryCompletions, "v_long_query_completions", "long_query_completions", "collection_time")]
    public void Relations_NameTheViewCollectorAndTimeColumnTheirGridReads(
        QueryWindowRelation relation, string view, string? collector, string timeColumn)
    {
        Assert.Equal(view, LocalDataService.QueryWindowRelationView(relation));
        Assert.Equal(collector, LocalDataService.QueryWindowRelationCollector(relation));
        Assert.Equal(timeColumn, LocalDataService.QueryWindowRelationTimeColumn(relation));
    }

    /// <summary>
    /// The wiring: each surface has its banner TextBlock, and the one method that reads it refreshes the banner after
    /// the rows are bound, through the shared helper and over the UTC window the read took. Every read path of these
    /// surfaces goes through that method: the sub-tab switch and the toolbar's range change reach the System Events,
    /// Config Changes and Long Queries loaders through <c>RefreshVisibleTabAsync</c>, and a loader's own Refresh button
    /// calls the same refresh. Comments are stripped first, so a sentence that names the call cannot satisfy the pin.
    /// </summary>
    [Theory]
    [InlineData("private async System.Threading.Tasks.Task RefreshCollectionHealthAsync(", "ServerTab.Refresh.cs", "CollectionLog", "CollectionLogWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadSchedulerIssuesAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "SchedulerIssuesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadSevereErrorsAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "SevereErrorsWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadMemoryConditionsAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "MemoryConditionsWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadMemoryBrokerAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "MemoryBrokerWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadMemoryNodeOomAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "MemoryNodeOomWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadSignificantWaitsAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "SignificantWaitsWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadCpuTasksAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "CpuTasksWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadIoIssuesAsync(", "ServerTab.SystemEvents.cs", "SystemHealthEvents", "IoIssuesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadDefaultTraceEventsAsync(", "ServerTab.SystemEvents.cs", "DefaultTraceEvents", "DefaultTraceWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadServerConfigChangesAsync(", "ServerTab.ConfigChanges.cs", "ServerConfig", "ServerConfigChangesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadDatabaseConfigChangesAsync(", "ServerTab.ConfigChanges.cs", "DatabaseConfig", "DatabaseConfigChangesWindowTruncatedBanner")]
    [InlineData("private async System.Threading.Tasks.Task LoadTraceFlagChangesAsync(", "ServerTab.ConfigChanges.cs", "TraceFlags", "TraceFlagChangesWindowTruncatedBanner")]
    [InlineData("private async Task RefreshLongQueriesAsync(", "ServerTab.LongQueries.cs", "LongQueryCompletions", "LongQueriesWindowTruncatedBanner")]
    public void EverySurface_HasABanner_RefreshedAfterTheRowsAreBound(string signature, string file, string relation, string banner)
    {
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        Assert.Contains($"x:Name=\"{banner}\"", xaml, StringComparison.Ordinal);

        var code = StripComments(File.ReadAllText(ControlsFile(file)).Replace("\r\n", "\n"));
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} is not in {file}");
        var next = Regex.Match(code[(start + signature.Length)..], @"\n    (private|internal|public) ");
        var body = next.Success ? code.Substring(start, signature.Length + next.Index) : code[start..];

        var call = $"RefreshStoredWindowBannerAsync(QueryWindowRelation.{relation}, {banner}, hoursBack, fromDate, toDate)";
        Assert.Contains(call, body, StringComparison.Ordinal);
        var bind = body.IndexOf("UpdateData(", StringComparison.Ordinal);
        Assert.True(bind >= 0 && bind < body.IndexOf(call, StringComparison.Ordinal),
            $"{banner} must be refreshed after the grid's rows are bound");
    }

    /// <summary>
    /// The one helper every group surface calls: it takes the window the grids' reads take
    /// (<see cref="LocalDataService.GetQueriesTabWindowUtc"/>, the same <c>GetTimeRange</c> call) and hands it to the
    /// shared banner step, which hides the banner when the probe throws instead of unwinding the refresh.
    /// </summary>
    [Fact]
    public void TheBannerHelper_ProbesTheWindowTheGridsReadThroughTheSharedStep()
    {
        var code = StripComments(File.ReadAllText(ControlsFile("ServerTab.SystemEvents.cs")).Replace("\r\n", "\n"));
        var start = code.IndexOf("RefreshStoredWindowBannerAsync(QueryWindowRelation relation,", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = code[start..Math.Min(code.Length, start + 600)];
        Assert.Contains("LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate)", body, StringComparison.Ordinal);
        Assert.Contains("RefreshWindowTruncatedBannerAsync(relation, banner, startUtc, endUtc)", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The surfaces that need no notice, and the shape that makes it true. The Health Summary reads a fixed seven
    /// days per collector, with no range parameter, so a picked range never starts before its data. The Duration
    /// Trends, Corruption Events and Contention Events charts set their X axis to the asked range, so a span with no
    /// data is drawn as the empty span it is. If one of them starts reading the toolbar's range as a grid, or stops
    /// pinning its axis, it needs a notice and this test says so.
    /// </summary>
    [Fact]
    public void SurfacesWithoutANotice_KeepTheShapeThatNeedsNone()
    {
        var health = File.ReadAllText(RepoFile("Lite", "Services", "LocalDataService.CollectionHealth.cs"));
        Assert.Contains("public async Task<List<CollectorHealthRow>> GetCollectionHealthAsync(int serverId)", health, StringComparison.Ordinal);
        Assert.Contains("GetCollectionHealthAsync(_serverId)", File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")), StringComparison.Ordinal);

        var durationChart = Regex.Match(File.ReadAllText(ControlsFile("ServerTab.Charts.cs")).Replace("\r\n", "\n"),
            @"private void UpdateCollectorDurationChart\(.*?\n    \}\n", RegexOptions.Singleline);
        Assert.True(durationChart.Success);
        Assert.Contains("SetLimitsX(xMin, xMax)", durationChart.Value, StringComparison.Ordinal);

        var systemCharts = File.ReadAllText(ControlsFile("ServerTab.SystemHealthCharts.cs"));
        Assert.Contains("ChartPalette.CyclingColor(3), xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Contains("RenderSickSpinlocksChart(SickSpinlocksChart, _sickSpinlocksHover, data, xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Contains("RenderCpuComparisonChart(CpuComparisonChart, _cpuComparisonHover, data, xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(File.ReadAllText(RepoFile("PerformanceMonitor.Ui", "SystemHealthChartRenderer.cs")),
            Regex.Escape("chart.Plot.Axes.SetLimitsX(xMin, xMax)")).Count);
    }

    private static string StripComments(string lfSource) =>
        Regex.Replace(Regex.Replace(lfSource, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);

    /// <summary>WPF objects require STA; same shape as DataStartBannerTests.</summary>
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }

    private static string ControlsFile(string name) => RepoFile("Lite", "Controls", name);

    private static string RepoFile(string folder, string subFolderOrFile, string? file = null, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", folder, subFolderOrFile, file ?? string.Empty));
}
