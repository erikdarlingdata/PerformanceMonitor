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
/// the Health Summary is a fixed seven-day aggregate that does not read the toolbar's range, the Corruption Events and
/// Contention Events charts pin their time axis to the asked range, so an empty span is already drawn as one, and the
/// Duration Trends chart reads its own full-range buckets, with its axis pinned the same way.</para>
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
        QueryWindowRelation.DefaultTraceEvents,
        // #4966: the Blocking tab's grids filter on the report's event_time and the deadlock's deadlock_time, both UTC.
        QueryWindowRelation.BlockedProcessReports,
        QueryWindowRelation.Deadlocks
    };

    /// <summary>The Blocking tab's two capped lists: the newest <see cref="LocalDataService.BlockedProcessReportGridCap"/> reports
    /// and the newest <see cref="LocalDataService.DeadlockGridCap"/> deadlocks.</summary>
    public static TheoryData<QueryWindowRelation> BlockingRelations => new()
    {
        QueryWindowRelation.BlockedProcessReports,
        QueryWindowRelation.Deadlocks
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
                or QueryWindowRelation.BlockedProcessReports or QueryWindowRelation.Deadlocks
            ? new Dictionary<string, DateTime> { [LocalDataService.QueryWindowRelationTimeColumn(relation)] = at, ["collection_time"] = collectedAt ?? at }
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

    /// <summary>
    /// A long-query completion every <paramref name="everySeconds"/> seconds from <paramref name="firstEventUtc"/> to
    /// <paramref name="lastEventUtc"/> (the event's own time, <c>event_time</c>), each stored by a run
    /// <see cref="CollectedAfterSeconds"/> seconds later (<c>collection_time</c>). The two clocks differ on purpose: the
    /// grid windows on the run's time and orders and caps on the event's, so a notice worded from the wrong one is
    /// told apart from the right one.
    /// </summary>
    private async Task SeedLongQueriesEverySecondsAsync(DateTime firstEventUtc, DateTime lastEventUtc, int everySeconds)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, statement_text, duration_microseconds)
SELECT {_nextId} + row_number() OVER (), g.t + INTERVAL {CollectedAfterSeconds} SECOND, {ServerId}, '{ServerName}', g.t, 'rpc_completed', 'Db',
       'SELECT ' || CAST(row_number() OVER () AS VARCHAR), 5000000
FROM generate_series({Literal(firstEventUtc)}, {Literal(lastEventUtc)}, INTERVAL {everySeconds} SECOND) AS g(t)";
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    /// <summary>How long after a completion the run that stored it stamps its <c>collection_time</c>.</summary>
    private const int CollectedAfterSeconds = 90;

    private static string Since(DateTime instant) =>
        "Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(instant), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss");

    /// <summary>
    /// A capped grid's read at its cap, then the capped-grid banner step itself (<see cref="ServerTab.CappedGridBannerAsync{T}"/>,
    /// the decision inside the tab's <c>RefreshCappedGridBannerAsync</c>): (visible, text, oldest row the grid returned,
    /// rows it returned, whether the step handed the banner to the probing step). The probing step stands in for the shared
    /// <c>RefreshWindowTruncatedBannerAsync</c>: it words the banner from the real probe's answer, which is awaited BEFORE the
    /// STA block, because a continuation after an await runs on another thread and a WPF banner can only be written by the
    /// thread that made it.
    /// </summary>
    private async Task<(bool Visible, string Text, DateTime Oldest, int Rows, bool Probed)> CappedBannerOverTheRealReadAsync<T>(
        Func<LocalDataService, Task<List<T>>> read, QueryWindowRelation relation, int rowCap, Func<T, DateTime> rowTimeUtc,
        DateTime startUtc, DateTime endUtc, Func<DateTime?>? cappedSourceOldestUtc = null)
    {
        var service = new LocalDataService(_duckDb);
        var rows = await read(service);
        var cappedSource = cappedSourceOldestUtc?.Invoke();
        var probedFloor = await service.GetQueryWindowFloorAsync(relation, ServerId, startUtc, endUtc, ServerClock.Utc);
        var (visible, text, probed) = OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            var probeStepRan = false;
            ServerTab.CappedGridBannerAsync(rows, rowCap, rowTimeUtc,
                oldestRowShown => ServerTab.ApplyCappedWindowFloorToBanner(banner, oldestRowShown, startUtc, TimeZoneInfo.Utc),
                () =>
                {
                    probeStepRan = true;
                    ServerTab.ApplyWindowFloorToBanner(banner, probedFloor, startUtc, TimeZoneInfo.Utc);
                    return Task.CompletedTask;
                }, cappedSource).GetAwaiter().GetResult();
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text, probeStepRan);
        });
        return (visible, text, rows.Count == 0 ? default : rows.Min(rowTimeUtc), rows.Count, probed);
    }

    private Task<(bool Visible, string Text, DateTime Oldest, int Rows, bool Probed)> CappedLongQueriesBannerAsync(DateTime startUtc, DateTime endUtc) =>
        CappedBannerOverTheRealReadAsync(
            service => service.GetRecentLongQueryCompletionsAsync(ServerId, fromDate: startUtc, toDate: endUtc),
            QueryWindowRelation.LongQueryCompletions, LocalDataService.LongQueryGridCap, ServerTab.LongQueryRowTimeUtc, startUtc, endUtc);

    private Task<(bool Visible, string Text, DateTime Oldest, int Rows, bool Probed)> CappedCollectionLogBannerAsync(DateTime startUtc, DateTime endUtc) =>
        CappedBannerOverTheRealReadAsync(
            service => service.GetRecentCollectionLogAsync(ServerId, fromDate: startUtc, toDate: endUtc),
            QueryWindowRelation.CollectionLog, LocalDataService.CollectionLogGridCap, row => row.CollectionTime, startUtc, endUtc);

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
    /// <c>collection_time</c> (T0) while its <c>event_time</c> is older: days for the Default Trace, whose first run stores the
    /// history its trace files hold, and about 10 minutes for system_health, whose first run reads back
    /// <c>CollectorContext.EventFallbackWindow</c>. The tests seed the history at whatever depth they test. The grids filter on <c>event_time</c>, so a
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

    /// <summary>
    /// <paramref name="everySeconds"/>-spaced rows of one of the Blocking tab's event tables (or the DMV snapshots), in one
    /// statement: <paramref name="firstUtc"/> to <paramref name="lastUtc"/>, every time column of the table at the row's time
    /// (an event's two clocks agree), every other required column a placeholder of its type, so every row of the DMV table
    /// carries the same blocked/blocking pair (the merge keeps one of a pair per minute).
    /// </summary>
    private async Task SeedManyAsync(QueryWindowRelation relation, DateTime firstUtc, DateTime lastUtc, int everySeconds)
    {
        var table = LocalDataService.QueryWindowRelationView(relation)[2..];
        var timeColumns = new[] { "collection_time", "event_time", "deadlock_time" };

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
                var pk = reader.GetBoolean(3);
                var isTime = Array.IndexOf(timeColumns, name) >= 0;
                if (!isTime && (!(reader.GetBoolean(2) || pk) || reader.GetBoolean(4)))
                {
                    continue;
                }

                names.Add(name);
                values.Add(isTime ? "g.t"
                    : pk ? $"{_nextId} + row_number() OVER ()"
                    : PlaceholderFor(name, reader.GetString(1), false, new Dictionary<string, DateTime>(), firstUtc));
            }
        }

        using var insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {table} ({string.Join(", ", names)}) SELECT {string.Join(", ", values)} " +
            $"FROM generate_series({Literal(firstUtc)}, {Literal(lastUtc)}, INTERVAL {everySeconds} SECOND) AS g(t)";
        _nextId += await insert.ExecuteNonQueryAsync() + 1;
    }

    private static int GridCapOf(QueryWindowRelation relation) =>
        relation == QueryWindowRelation.BlockedProcessReports ? LocalDataService.BlockedProcessReportGridCap : LocalDataService.DeadlockGridCap;

    /// <summary>A Blocking-tab grid's read at its cap and the capped-grid banner step over it (#4966), the read being the grid's own:
    /// the Blocked Process Reports read hands the step the oldest event time of the XE or DMV read that filled its page.</summary>
    private Task<(bool Visible, string Text, DateTime Oldest, int Rows, bool Probed)> CappedBlockingBannerAsync(QueryWindowRelation relation, DateTime startUtc, DateTime endUtc)
    {
        if (relation == QueryWindowRelation.Deadlocks)
        {
            return CappedBannerOverTheRealReadAsync(
                service => service.GetRecentDeadlocksAsync(ServerId, fromDate: startUtc, toDate: endUtc),
                relation, LocalDataService.DeadlockGridCap, ServerTab.DeadlockRowTimeUtc, startUtc, endUtc);
        }

        DateTime? cappedSource = null;
        return CappedBannerOverTheRealReadAsync(
            async service =>
            {
                var read = await service.ReadRecentBlockedProcessReportsAsync(ServerId, fromDate: startUtc, toDate: endUtc);
                cappedSource = read.CappedSourceStartUtc;
                return read.Rows;
            },
            relation, LocalDataService.BlockedProcessReportGridCap, ServerTab.BlockedProcessRowTimeUtc, startUtc, endUtc, () => cappedSource);
    }

    /// <summary>
    /// #4966: the Blocked Process Reports and Deadlocks grids read the newest 200 and 50. A read that fills its page is worded from
    /// its oldest row, whatever the store covers (the collector ran for 30 days here): the rows older than it are not in the grid.
    /// The read's row count is the constant the notice caps by (<see cref="LocalDataService.BlockedProcessReportGridCap"/>,
    /// <see cref="LocalDataService.DeadlockGridCap"/>), and the step needs no probe.
    /// </summary>
    [Theory]
    [MemberData(nameof(BlockingRelations))]
    public async Task BlockingGrids_AReadThatFillsItsCap_NamesItsOldestRow_AlsoOverARangeTheStoreCovers(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var cap = GridCapOf(relation);
        await SeedLogRunsAsync(LocalDataService.QueryWindowRelationCollector(relation)!, end.AddDays(-30), end, 60);
        await SeedManyAsync(relation, end.AddMinutes(-cap - 40), end.AddMinutes(-2), 60);

        var (visible, text, oldest, rows, probed) = await CappedBlockingBannerAsync(relation, end.AddDays(-1), end);

        Assert.Equal(cap, rows);
        Assert.False(probed);
        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
    }

    /// <summary>A range of an hour, no longer than the probe's 90-minute slack, whose grid still fills its cap: the notice names the
    /// oldest row with no slack (it is later than the range's start), and no probe runs.</summary>
    [Theory]
    [MemberData(nameof(BlockingRelations))]
    public async Task BlockingGrids_AOneHourRange_WhoseGridHitsItsCap_ShowsTheNoticeAtItsOldestRow_WithNoSlack(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var cap = GridCapOf(relation);
        await SeedLogRunsAsync(LocalDataService.QueryWindowRelationCollector(relation)!, end.AddDays(-30), end, 60);
        await SeedManyAsync(relation, end.AddMinutes(-50), end.AddMinutes(-1), 10);

        var (visible, text, oldest, rows, probed) = await CappedBlockingBannerAsync(relation, end.AddHours(-1), end);

        Assert.Equal(cap, rows);
        Assert.False(probed);
        Assert.True(oldest > end.AddHours(-1));
        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
    }

    /// <summary>A page under its cap holds everything the store has in the range: the coverage notice stands, and a range the
    /// collector covered (it ran for 30 days) shows none.</summary>
    [Theory]
    [MemberData(nameof(BlockingRelations))]
    public async Task BlockingGrids_AReadUnderItsCap_KeepsTheCoverageNotice(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync(LocalDataService.QueryWindowRelationCollector(relation)!, end.AddDays(-30), end, 60);
        await SeedManyAsync(relation, end.AddHours(-5), end.AddHours(-1), 600);

        var (visible, text, _, rows, probed) = await CappedBlockingBannerAsync(relation, end.AddDays(-7), end);

        Assert.InRange(rows, 1, GridCapOf(relation) - 1);
        Assert.True(probed);
        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>The grid also lists the always-on DMV blocking snapshots, so with XE collection off (no blocked process threshold)
    /// a range that starts before the DMV collector's first run names where the DMV collector's coverage starts.</summary>
    [Fact]
    public async Task BlockedProcessReports_XeCollectionOff_ARangeStartingBeforeTheDmvCollector_NamesTheDmvCoverage()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var started = end.AddDays(-2);
        await SeedLogRunsAsync("dmv_blocking_snapshot", started, end, 60);

        var (visible, text) = await BannerForAsync(QueryWindowRelation.BlockedProcessReports, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(Since(started), text);
    }

    /// <summary>XE collection off, and a range the DMV collector covered from before its start: nothing is missing, no notice.</summary>
    [Fact]
    public async Task BlockedProcessReports_XeCollectionOff_ARangeTheDmvCollectorCovers_ShowsNoNotice()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("dmv_blocking_snapshot", end.AddDays(-30), end, 60);

        var (visible, text) = await BannerForAsync(QueryWindowRelation.BlockedProcessReports, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// A full read can merge to FEWER rows than its cap. The DMV read fetches the cap plus the XE rows in hand, and the merge keeps
    /// one DMV row per blocked/blocking pair per minute, so 200 snapshots of one pair taken every 10 seconds merge to about 34
    /// rows while the DMV read stopped at its LIMIT and left older snapshots out. The merged count (under the cap) cannot show
    /// it, so the notice is worded from the oldest snapshot the DMV read returned, over a range the collector covers.
    /// </summary>
    [Fact]
    public async Task BlockedProcessReports_ADmvReadThatFillsItsCap_MergingToFewerRows_NamesItsOldestSnapshot()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var cap = LocalDataService.BlockedProcessReportGridCap;
        await SeedLogRunsAsync("dmv_blocking_snapshot", end.AddDays(-30), end, 60);
        var last = end.AddMinutes(-5);
        var first = last.AddSeconds(-10 * (cap - 1));
        await SeedManyAsync(QueryWindowRelation.DmvBlockingSnapshots, first, last, 10);

        var (visible, text, _, rows, probed) = await CappedBlockingBannerAsync(QueryWindowRelation.BlockedProcessReports, end.AddDays(-7), end);

        Assert.InRange(rows, 1, cap - 1);
        Assert.False(probed);
        Assert.True(visible);
        Assert.Equal(Since(first), text);
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
    [InlineData(QueryWindowRelation.JobHistory, "v_job_history", "job_history", "collection_time")]
    [InlineData(QueryWindowRelation.BlockedProcessReports, "v_blocked_process_reports", "blocked_process_report", "event_time")]
    [InlineData(QueryWindowRelation.Deadlocks, "v_deadlocks", "deadlocks", "deadlock_time")]
    [InlineData(QueryWindowRelation.DmvBlockingSnapshots, "v_dmv_blocking_snapshots", "dmv_blocking_snapshot", "collection_time")]
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
    /// calls the same refresh. Comments are stripped first, so a sentence that names the call cannot satisfy the pin. The
    /// two grids that read a capped page, the Collection Log and Long Queries, are pinned by
    /// <see cref="CappedSurfaces_RefreshTheirBanner_ThroughTheCapAwareStep_AfterTheRowsAreBound"/> instead (#4989).
    /// </summary>
    [Theory]
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
    /// #4989: the two grids that read a capped page, the Collection Log (the newest
    /// <see cref="LocalDataService.CollectionLogGridCap"/> runs) and Long Queries (the newest
    /// <see cref="LocalDataService.LongQueryGridCap"/> completions), refresh their notice through the cap-aware step
    /// (<c>RefreshCappedGridBannerAsync</c>), not through the probe-only one: after the rows are bound, over the UTC window
    /// the read took, handing the step the rows the grid shows, the constant that is the read's cap and the time each
    /// row is capped on. Comments are stripped first. A grid that goes back to the probe-only step would again say
    /// nothing about the rows its cap dropped, on a range the store covers.
    /// </summary>
    [Theory]
    [InlineData("private async System.Threading.Tasks.Task RefreshCollectionHealthAsync(", "ServerTab.Refresh.cs", "CollectionLog", "CollectionLogWindowTruncatedBanner",
        "collectionLogTask.Result, LocalDataService.CollectionLogGridCap, row => row.CollectionTime")]
    [InlineData("private async Task RefreshLongQueriesAsync(", "ServerTab.LongQueries.cs", "LongQueryCompletions", "LongQueriesWindowTruncatedBanner",
        "task.Result, LocalDataService.LongQueryGridCap, LongQueryRowTimeUtc")]
    public void CappedSurfaces_RefreshTheirBanner_ThroughTheCapAwareStep_AfterTheRowsAreBound(
        string signature, string file, string relation, string banner, string rowsCapAndTime)
    {
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        Assert.Contains($"x:Name=\"{banner}\"", xaml, StringComparison.Ordinal);

        var code = StripComments(File.ReadAllText(ControlsFile(file)).Replace("\r\n", "\n"));
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} is not in {file}");
        var next = Regex.Match(code[(start + signature.Length)..], @"\n    (private|internal|public) ");
        var body = next.Success ? code.Substring(start, signature.Length + next.Index) : code[start..];

        var window = "var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);";
        var call = $"RefreshCappedGridBannerAsync(QueryWindowRelation.{relation}, {banner}, windowStart, windowEnd, {rowsCapAndTime})";
        Assert.Contains(window, body, StringComparison.Ordinal);
        Assert.Contains(call, body, StringComparison.Ordinal);
        Assert.DoesNotContain("RefreshStoredWindowBannerAsync(", body, StringComparison.Ordinal);
        var bind = body.IndexOf("UpdateData(", StringComparison.Ordinal);
        Assert.True(bind >= 0 && bind < body.IndexOf(window, StringComparison.Ordinal) && body.IndexOf(window, StringComparison.Ordinal) < body.IndexOf(call, StringComparison.Ordinal),
            $"{banner} must be refreshed after the grid's rows are bound, over the read's window");
    }

    /// <summary>
    /// The read's <c>LIMIT</c> and the notice's cap are ONE value. The Long Queries read is written with
    /// <see cref="LocalDataService.LongQueryGridCap"/> and not with a literal, so a read seeded past the cap comes back with
    /// exactly that many rows, and the tab hands the same constant to the cap-aware step
    /// (<see cref="CappedSurfaces_RefreshTheirBanner_ThroughTheCapAwareStep_AfterTheRowsAreBound"/>). The Collection Log's
    /// default <c>maxRows</c> is <see cref="LocalDataService.CollectionLogGridCap"/> the same way. The MCP read that ranks by
    /// duration, <c>GetSlowestLongQueryCompletionsAsync</c>, caps nothing by time and keeps its own limit.
    /// </summary>
    [Fact]
    public async Task TheGridReads_LimitIsTheSameConstantTheBannerCaps()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLongQueriesEverySecondsAsync(end.AddMinutes(-LocalDataService.LongQueryGridCap - 40), end.AddMinutes(-2), 60);
        await SeedLogRunsAsync("wait_stats", end.AddMinutes(-LocalDataService.CollectionLogGridCap - 40), end, 1);

        var service = new LocalDataService(_duckDb);
        Assert.Equal(LocalDataService.LongQueryGridCap, (await service.GetRecentLongQueryCompletionsAsync(ServerId, fromDate: end.AddDays(-1), toDate: end)).Count);
        Assert.Equal(LocalDataService.CollectionLogGridCap, (await service.GetRecentCollectionLogAsync(ServerId, fromDate: end.AddDays(-1), toDate: end)).Count);

        var longQueries = File.ReadAllText(RepoFile("Lite", "Services", "LocalDataService.LongQueries.cs")).Replace("\r\n", "\n");
        var grid = longQueries[longQueries.IndexOf("GetRecentLongQueryCompletionsAsync(int serverId", StringComparison.Ordinal)..longQueries.IndexOf("GetSlowestLongQueryCompletionsAsync(int serverId", StringComparison.Ordinal)];
        Assert.Contains("LIMIT {LongQueryGridCap}", grid, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"LIMIT \d", grid);
    }

    /// <summary>
    /// A Long Queries read that fills its cap names its OLDEST returned completion, worded at its own time
    /// (<c>event_time</c>, 90 seconds before the run that stored it stamped the row), even over a range the store covers:
    /// the collector's runs reach 30 days back, so the probe alone shows nothing for a 7-day range, but the grid shows only
    /// the newest 200 of the 3 days of completions it holds, the oldest about 16.6 hours back. The probing step is not asked.
    /// </summary>
    [Fact]
    public async Task LongQueries_AReadThatFillsItsCap_NamesItsOldestRow_AlsoOverARangeTheStoreCovers()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("long_query_completions", end.AddDays(-30), end, 360);
        await SeedLongQueriesEverySecondsAsync(end.AddDays(-3), end.AddMinutes(-2), 5 * 60);

        Assert.False((await BannerForAsync(QueryWindowRelation.LongQueryCompletions, end.AddDays(-7), end)).Visible);

        var (visible, text, oldest, rows, probed) = await CappedLongQueriesBannerAsync(end.AddDays(-7), end);

        Assert.Equal(LocalDataService.LongQueryGridCap, rows);
        Assert.InRange((end - oldest).TotalHours, 16, 17);
        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
        Assert.False(probed);
    }

    /// <summary>
    /// The notice names the completion's own time, not the time its run stored it: the oldest row's
    /// <c>collection_time</c> is 90 seconds later, and a notice worded from it would name a time after the oldest row shown.
    /// </summary>
    [Fact]
    public async Task LongQueries_TheCappedNotice_IsWordedAtTheEventTime_NotTheTimeTheRunStoredIt()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLongQueriesEverySecondsAsync(end.AddHours(-30), end.AddMinutes(-2), 5 * 60);

        var rows = await new LocalDataService(_duckDb).GetRecentLongQueryCompletionsAsync(ServerId, fromDate: end.AddDays(-7), toDate: end);
        var (visible, text, oldest, _, _) = await CappedLongQueriesBannerAsync(end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
        Assert.Equal(oldest, rows.Min(row => row.EventTime));
        Assert.NotEqual(Since(rows.Min(row => row.CollectionTime)), text);
    }

    /// <summary>
    /// The row's time is its event time, and a completion with no event time falls back to the time its run was collected.
    /// </summary>
    [Fact]
    public void LongQueryRowTimeUtc_IsTheEventTime_AndFallsBackToTheCollectionTime()
    {
        var collected = new DateTime(2026, 6, 3, 10, 2, 0);
        var eventTime = new DateTime(2026, 6, 3, 10, 0, 30);

        Assert.Equal(eventTime, ServerTab.LongQueryRowTimeUtc(new LongQueryCompletionRow { CollectionTime = collected, EventTime = eventTime }));
        Assert.Equal(collected, ServerTab.LongQueryRowTimeUtc(new LongQueryCompletionRow { CollectionTime = collected, EventTime = null }));
    }

    /// <summary>
    /// A read under its cap holds everything the store has in the range, so the coverage notice stands as before: a server
    /// whose trace was added 48 hours ago says it shows since then, and the probing step is the one that words it.
    /// </summary>
    [Fact]
    public async Task LongQueries_AReadUnderItsCap_KeepsTheCoverageNotice()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddHours(-48);
        await SeedLogRunsAsync("long_query_completions", added, end, 5);
        await SeedLongQueriesEverySecondsAsync(end.AddMinutes(-2 - 5 * 149), end.AddMinutes(-2), 5 * 60);

        var (visible, text, _, rows, probed) = await CappedLongQueriesBannerAsync(end.AddDays(-7), end);

        Assert.Equal(150, rows);
        Assert.True(probed);
        Assert.True(visible);
        Assert.Equal(Since(added), text);
    }

    /// <summary>A range the store covers, read under the cap, shows no notice: the probing step answers the range's start.</summary>
    [Fact]
    public async Task LongQueries_ACoveredRangeUnderTheCap_ShowsNoNotice()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("long_query_completions", end.AddDays(-30), end, 360);
        await SeedLongQueriesEverySecondsAsync(end.AddHours(-5), end.AddHours(-4), 20 * 60);

        var (visible, text, _, rows, probed) = await CappedLongQueriesBannerAsync(end.AddDays(-7), end);

        Assert.InRange(rows, 1, LocalDataService.LongQueryGridCap - 1);
        Assert.True(probed);
        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// A capped Long Queries read on a range of ONE HOUR whose oldest completion is later than the start shows its notice, at
    /// that completion: a completion every 10 seconds fills the 200-row cap in about 33 minutes, so the grid starts about 25
    /// minutes after the range does. The capped verdict gets no slack (the 90-minute slack is the coverage probe's, and
    /// the probe is not asked).
    /// </summary>
    [Fact]
    public async Task LongQueries_AOneHourRange_WhoseGridHitsItsCap_ShowsTheNoticeAtItsOldestRow_WithNoSlack()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLongQueriesEverySecondsAsync(end.AddMinutes(-40), end.AddMinutes(-2), 10);

        /* What the probe-only step said of this read: nothing, the first row is inside the slack. */
        Assert.False((await BannerForAsync(QueryWindowRelation.LongQueryCompletions, end.AddHours(-1), end)).Visible);

        var (visible, text, oldest, rows, probed) = await CappedLongQueriesBannerAsync(end.AddHours(-1), end);

        Assert.Equal(LocalDataService.LongQueryGridCap, rows);
        Assert.InRange((end - oldest).TotalMinutes, 34, 36);
        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
        Assert.False(probed);
    }

    /// <summary>
    /// A range of 90 minutes or less makes no probe call for Long Queries, which gets its coverage notice from the shared
    /// probing step (<see cref="ServerTab.ProbeWindowFloorOrNullAsync"/>): such a window can never get one. A range past the
    /// slack asks the probe once.
    /// </summary>
    [Theory]
    [InlineData(60, 0)]
    [InlineData(90, 0)]
    [InlineData(91, 1)]
    [InlineData(1440, 1)]
    public async Task LongQueries_ARangeNoLongerThanTheSlack_MakesNoProbeCall(int rangeMinutes, int expectedProbeCalls)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("long_query_completions", end.AddDays(-2), end, 5);
        var service = new LocalDataService(_duckDb);
        var start = end.AddMinutes(-rangeMinutes);
        var probeCalls = 0;

        var floor = await ServerTab.ProbeWindowFloorOrNullAsync(
            () =>
            {
                probeCalls++;
                return service.GetQueryWindowFloorAsync(QueryWindowRelation.LongQueryCompletions, ServerId, start, end, ServerClock.Utc);
            },
            "Long Queries", start, end);

        Assert.Equal(expectedProbeCalls, probeCalls);
        Assert.Equal(expectedProbeCalls == 0, floor is null);
    }

    /// <summary>
    /// The Collection Log read fills its cap on a dense log (a run every minute for 12 hours is 720 rows, the cap 500), so
    /// the notice names the oldest run the grid returned, over a range the store covers (an older run sits 20 days back,
    /// so the probe alone shows nothing), and the probing step is not asked.
    /// </summary>
    [Fact]
    public async Task CollectionLog_AReadThatFillsItsCap_NamesItsOldestRow_AlsoOverARangeTheStoreCovers()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("wait_stats", end.AddDays(-20), end.AddDays(-20), 1);
        await SeedLogRunsAsync("wait_stats", end.AddHours(-12), end, 1);

        Assert.False((await BannerForAsync(QueryWindowRelation.CollectionLog, end.AddDays(-7), end)).Visible);

        var (visible, text, oldest, rows, probed) = await CappedCollectionLogBannerAsync(end.AddDays(-7), end);

        Assert.Equal(LocalDataService.CollectionLogGridCap, rows);
        Assert.InRange((end - oldest).TotalMinutes, 498, 501);
        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
        Assert.False(probed);
    }

    /// <summary>A Collection Log read under its cap keeps the coverage notice, worded at the first run in the range.</summary>
    [Fact]
    public async Task CollectionLog_AReadUnderItsCap_KeepsTheCoverageNotice()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddHours(-3);
        await SeedLogRunsAsync("wait_stats", added, end, 5);

        var (visible, text, _, rows, probed) = await CappedCollectionLogBannerAsync(end.AddDays(-7), end);

        Assert.InRange(rows, 1, LocalDataService.CollectionLogGridCap - 1);
        Assert.True(probed);
        Assert.True(visible);
        Assert.Equal(Since(added), text);
    }

    /// <summary>
    /// The other twelve banners (the eight system_health grids, Default Trace and the three Config Changes grids) read
    /// their whole window: none of their reads carries a row cap, so the probe alone words their notice. A <c>LIMIT</c>
    /// added to one of these reads would need the cap-aware step the Collection Log and Long Queries use, and fails here
    /// until it has it.
    /// </summary>
    [Theory]
    [InlineData("LocalDataService.SystemEvents.cs")]
    [InlineData("LocalDataService.ConfigChanges.cs")]
    public void TheUncappedSurfaces_ReadNoRowCap(string file)
    {
        var code = StripComments(File.ReadAllText(RepoFile("Lite", "Services", file)).Replace("\r\n", "\n"));

        Assert.DoesNotMatch(@"\bLIMIT\b", code);
        Assert.DoesNotMatch(@"\bTOP\s*\(?\s*\d", code);
    }

    /// <summary>
    /// The surfaces that need no notice, and the shape that makes it true. The Health Summary reads a fixed seven
    /// days per collector, with no range parameter, so a picked range never starts before its data. The Corruption Events
    /// and Contention Events charts set their X axis to the asked range, so a span with no data is drawn as the empty
    /// span it is. The Duration Trends chart needs none for its own reason (#4989): it reads its own full-range buckets
    /// (<see cref="LocalDataService.GetCollectorDurationTrendAsync"/>, not the Collection Log grid's capped page) with its
    /// X axis pinned to the asked range, so what it draws is the range's data and the empty span is the span with none; the
    /// <c>DurationTrends_*</c> tests below pin that. If one of the others starts reading the toolbar's range as a grid, or
    /// stops pinning its axis, it needs a notice and this test says so.
    /// </summary>
    [Fact]
    public void SurfacesWithoutANotice_KeepTheShapeThatNeedsNone()
    {
        var health = File.ReadAllText(RepoFile("Lite", "Services", "LocalDataService.CollectionHealth.cs"));
        Assert.Contains("public async Task<List<CollectorHealthRow>> GetCollectionHealthAsync(int serverId)", health, StringComparison.Ordinal);
        Assert.Contains("GetCollectionHealthAsync(_serverId)", File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")), StringComparison.Ordinal);

        /* The Duration Trends chart is no longer pinned here: it reads its own buckets over the whole range, and the tests
           below (DurationTrends_*) say what it draws. */

        var systemCharts = File.ReadAllText(ControlsFile("ServerTab.SystemHealthCharts.cs"));
        Assert.Contains("ChartPalette.CyclingColor(3), xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Contains("RenderSickSpinlocksChart(SickSpinlocksChart, _sickSpinlocksHover, data, xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Contains("RenderCpuComparisonChart(CpuComparisonChart, _cpuComparisonHover, data, xMin, xMax)", systemCharts, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(File.ReadAllText(RepoFile("PerformanceMonitor.Ui", "SystemHealthChartRenderer.cs")),
            Regex.Escape("chart.Plot.Axes.SetLimitsX(xMin, xMax)")).Count);
    }

    /// <summary>What a chart draws, as (first X, last X) of each plotted line: the series drawn on a real chart.</summary>
    private static List<(double First, double Last)> DrawnSpans(IReadOnlyList<ServerTab.CollectorDurationSeries> series) =>
        OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            ServerTab.PlotCollectorDurationSeries(chart, null, series);
            return chart.Plot.PlottableList.OfType<ScottPlot.Plottables.Scatter>()
                .Select(scatter => scatter.Data.GetScatterPoints().Select(p => p.X).ToList())
                .Select(xs => (xs.Min(), xs.Max()))
                .ToList();
        });

    /// <summary>
    /// #4989: a 24-hour range holding more runs than the Collection Log grid's page (the newest 500) draws points across
    /// the WHOLE range. The chart used to be handed that page, so at three runs a minute it drew the newest 2 hours 47
    /// minutes of a day (and at twenty a minute, 25 minutes) and left the rest of its pinned axis empty though the store
    /// held those runs. The first assertions say the page is the sliver it is; the rest, that each collector's line starts
    /// at the range's start and ends at its end.
    /// </summary>
    [Fact]
    public async Task DurationTrends_ADayHoldingMoreRunsThanTheGridsPage_DrawsPointsAcrossTheWholeRange()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var start = end.AddHours(-24);
        foreach (var collector in new[] { "wait_stats", "cpu_utilization_stats", "memory_stats" })
        {
            await SeedLogRunsAsync(collector, start, end, 1);
        }

        var service = new LocalDataService(_duckDb);
        var page = await service.GetRecentCollectionLogAsync(ServerId, fromDate: start, toDate: end);
        Assert.Equal(LocalDataService.CollectionLogGridCap, page.Count);
        Assert.True(page.Max(r => r.CollectionTime) - page.Min(r => r.CollectionTime) < TimeSpan.FromHours(4),
            "the grid's page is the newest runs only");

        var spans = DrawnSpans(ServerTab.BuildCollectorDurationSeries(await service.GetCollectorDurationTrendAsync(ServerId, fromDate: start, toDate: end)));

        Assert.Equal(3, spans.Count);
        foreach (var (first, last) in spans)
        {
            Assert.True(first <= start.AddMinutes(2).ToOADate(), $"a line starts {DateTime.FromOADate(first):O}, after the range's start {start:O}");
            Assert.True(last >= end.AddMinutes(-2).ToOADate(), $"a line ends {DateTime.FromOADate(last):O}, before the range's end {end:O}");
        }
    }

    /// <summary>
    /// #4989: the chart draws each bucket's slowest run, so one slow run still shows as it did when every run was a point,
    /// and the hover says what stands behind the point: how many runs it is the slowest of and their average. A 7-day range
    /// of one run a minute is bucketed (far fewer points than runs, and every run counted once), and the one 9-second run
    /// in it is the height of its bucket's point.
    /// </summary>
    [Fact]
    public async Task DurationTrends_ASevenDayRange_DrawsEachBucketsSlowestRun_AndNamesTheAverageAndCountInTheHover()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var start = end.AddDays(-7);
        await SeedLogRunsAsync("wait_stats", start, end, 1);
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            using var readLock = _duckDb.AcquireReadLock();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
UPDATE collection_log SET duration_ms = 9000
WHERE collector_name = 'wait_stats' AND collection_time = (SELECT MIN(collection_time) FROM collection_log WHERE collection_time >= {Literal(start.AddDays(3))})";
            await cmd.ExecuteNonQueryAsync();
        }

        var buckets = await new LocalDataService(_duckDb).GetCollectorDurationTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(buckets.Count < 2000, $"{buckets.Count} points for 10,081 runs: the read buckets");
        Assert.Equal(10081, buckets.Sum(b => b.RunCount));
        Assert.True(buckets.All(b => b.BucketStart >= start.AddTicks(-10)), "no bucket starts before the range");
        var slow = Assert.Single(buckets, b => b.MaxDurationMs == 9000);
        Assert.True(slow.RunCount >= 2);
        Assert.Equal((9000 + 12.0 * (slow.RunCount - 1)) / slow.RunCount, slow.AverageDurationMs, 6);

        var series = Assert.Single(ServerTab.BuildCollectorDurationSeries(buckets));
        var at = Array.IndexOf(series.MaxMs, 9000d);
        Assert.True(at >= 0, "the slow run is a point's height");
        var culture = System.Globalization.CultureInfo.CurrentCulture;
        var average = slow.AverageDurationMs == Math.Floor(slow.AverageDurationMs) ? slow.AverageDurationMs.ToString("N0", culture) : slow.AverageDurationMs.ToString("N1", culture);
        Assert.Equal($"Slowest of {slow.RunCount.ToString("N0", culture)} runs; average {average} ms", series.Details[at]);

        Assert.Equal("wait_stats\n9,000 ms\n10:00:00\nSlowest of 10 runs; average 910.8 ms",
            PerformanceMonitor.Ui.ChartHoverHelper.HoverText("wait_stats", "9,000", "ms", "10:00:00", "Slowest of 10 runs; average 910.8 ms"));
        Assert.Equal("wait_stats\n9,000 ms\n10:00:00", PerformanceMonitor.Ui.ChartHoverHelper.HoverText("wait_stats", "9,000", "ms", "10:00:00"));
    }

    /// <summary>
    /// #4989: the Refresh hands the chart its own read (<c>GetCollectorDurationTrendAsync</c>), started beside the grid's
    /// read and awaited with it, and never the grid's page.
    /// </summary>
    [Fact]
    public void DurationTrends_TheChartIsFedItsOwnRead_StartedBesideTheGridsRead()
    {
        var code = StripComments(File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")).Replace("\r\n", "\n"));
        var start = code.IndexOf("private async System.Threading.Tasks.Task RefreshCollectionHealthAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = code[start..code.IndexOf("private static async Task<List<T>> SafeQueryAsync<T>", start, StringComparison.Ordinal)];

        Assert.Contains("_dataService.GetCollectorDurationTrendAsync(_serverId, hoursBack, fromDate, toDate)", body, StringComparison.Ordinal);
        Assert.Contains("Task.WhenAll(collectionHealthTask, collectionLogTask, collectorDurationTask)", body, StringComparison.Ordinal);
        Assert.Contains("UpdateCollectorDurationChart(collectorDurationTask.Result, hoursBack, fromDate, toDate)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("UpdateCollectorDurationChart(collectionLogTask", body, StringComparison.Ordinal);
    }

    private async Task SeedLongQueryAsync(DateTime eventUtc, DateTime collectedUtc)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, statement_text, duration_microseconds)
VALUES ({_nextId++}, {Literal(collectedUtc)}, {ServerId}, '{ServerName}', {Literal(eventUtc)}, 'rpc_completed', 'Db', 'SELECT 1', 5000000)";
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The Long Queries notice for a read UNDER its cap: the real read, the real probe, then the tab's own rule for
    /// the floor (<see cref="ServerTab.EarlierOfFloorAndRowShown"/> over <see cref="ServerTab.EarliestRowShown{T}"/>, the pair
    /// <c>RefreshCappedGridBannerAsync</c> hands the shared step) and its banner step: (visible, text, rows shown).</summary>
    private async Task<(bool Visible, string Text, int Rows)> UnderCapLongQueriesBannerAsync(DateTime startUtc, DateTime endUtc)
    {
        var service = new LocalDataService(_duckDb);
        var rows = await service.GetRecentLongQueryCompletionsAsync(ServerId, fromDate: startUtc, toDate: endUtc);
        var probed = await service.GetQueryWindowFloorAsync(QueryWindowRelation.LongQueryCompletions, ServerId, startUtc, endUtc, ServerClock.Utc);
        var floor = ServerTab.EarlierOfFloorAndRowShown(probed, ServerTab.EarliestRowShown(rows, ServerTab.LongQueryRowTimeUtc));
        var (visible, text) = OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.ApplyWindowFloorToBanner(banner, floor, startUtc, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });
        return (visible, text, rows.Count);
    }

    /// <summary>
    /// #4989: a server's first run (T0, 48 hours ago) stores a completion from 8 minutes before itself (a first run reads
    /// back <see cref="PerformanceMonitor.Collectors.CollectorContext.EventFallbackWindow"/>), and the grid shows it at its
    /// event time. Under the cap, on a 7-day range, the notice names that completion's time, T0 less 8 minutes, and not the
    /// run's T0.
    /// </summary>
    [Fact]
    public async Task LongQueries_UnderTheCap_NameTheOldestCompletionShown_WhenItPredatesTheFirstRun()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var firstRun = new DateTime(end.AddHours(-48).Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);
        await SeedLogRunsAsync("long_query_completions", firstRun, end, 60);
        await SeedLongQueryAsync(firstRun.AddMinutes(-8), firstRun);
        await SeedLongQueryAsync(firstRun.AddHours(2), firstRun.AddHours(2).AddSeconds(CollectedAfterSeconds));

        var (visible, text, rows) = await UnderCapLongQueriesBannerAsync(end.AddDays(-7), end);

        Assert.InRange(rows, 1, LocalDataService.LongQueryGridCap - 1);
        Assert.True(visible);
        Assert.Equal(Since(firstRun.AddMinutes(-8)), text);
    }

    /// <summary>
    /// #4989: with no completion older than the first run, the notice still names the run, as before.
    /// </summary>
    [Fact]
    public async Task LongQueries_UnderTheCap_StillNameTheFirstRun_WhenNoCompletionShownIsOlderThanIt()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var firstRun = new DateTime(end.AddHours(-48).Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);
        await SeedLogRunsAsync("long_query_completions", firstRun, end, 60);
        await SeedLongQueryAsync(firstRun.AddMinutes(30), firstRun.AddMinutes(30).AddSeconds(CollectedAfterSeconds));

        var (visible, text, _) = await UnderCapLongQueriesBannerAsync(end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(Since(firstRun), text);
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
