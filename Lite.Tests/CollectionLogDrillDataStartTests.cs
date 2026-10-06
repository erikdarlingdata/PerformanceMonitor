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
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: the per-collector run-history window (<c>CollectionLogWindow</c>, opened from a Collection Health row) says where
/// the data starts. It lists one collector's runs over the trailing week, and when this SERVER's collection log starts
/// after that week began, a "Showing since" note names the first run. The rule is the Collection Log grid's, over the
/// server's log and never the collector's own first run: a collector that started late in a week the server's log covers
/// says nothing, and a quiet start shows no note. The read has no row cap, so only the coverage rule applies.
///
/// <para>The week is worked out once and handed to the read and to the probe, so the rows and the note cannot disagree.
/// A probe that throws costs the note and nothing else. The note is worded in the zone the window's grid prints its
/// times in.</para>
///
/// <para>Each case runs the window's own steps (<c>ReadDrillAsync</c>, then <c>ShowDrillDataStart</c> on a banner), over a
/// real DuckDB store for the coverage cases and over passed-in steps for the cases that need a probe that throws or a
/// record of the window each step was given. The banner is written on its own STA thread, after the awaits, as the other
/// banner tests do: a continuation after an await runs on another thread, and a WPF object can only be written by the
/// thread that made it.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class CollectionLogDrillDataStartTests : IDisposable
{
    private const int ServerId = 4343;
    private const string ServerName = "DrillServer";
    private const string Collector = "wait_stats";
    private const string OtherCollector = "query_snapshots";

    /// <summary>The pinned end of the window, so the week is the same on every day the suite runs.</summary>
    private static readonly DateTime AsOf = new(2026, 9, 20, 12, 0, 0, DateTimeKind.Utc);

    private static readonly DateTime WeekStart = AsOf.AddHours(-168);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;
    private long _nextId = 1;

    public CollectionLogDrillDataStartTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "CollectionLogDrill_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));

        /* UTC unless a case names another zone, and the invariant culture, so "g" and the banner's pattern are fixed. */
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        ServerTimeHelper.ActiveServerClock = ServerClock.Utc;
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        CultureInfo.CurrentCulture = _savedCulture;
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    private static ServerClock India() => ServerClock.Resolve("India Standard Time", 330);

    /// <summary>The collector's runs in <c>collection_log</c>, every <paramref name="everyMinutes"/> minutes from first to last.</summary>
    private async Task SeedRunsAsync(string collector, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, $4, g.t, 12, 'SUCCESS', 0
FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL {everyMinutes} MINUTE) AS g(t)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(firstUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(lastUtc) });
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    /// <summary>The window's own load over the real store, ending at <see cref="AsOf"/>.</summary>
    private Task<CollectionLogWindow.DrillRead> LoadAsync() =>
        CollectionLogWindow.ReadDrillAsync(new LocalDataService(_duckDb), ServerId, Collector, AsOf);

    /// <summary>
    /// The note the window shows for <paramref name="load"/>: (visible, text). The banner starts visible with a stale text,
    /// so a step that never touched it cannot pass as "no note".
    /// </summary>
    private static (bool Visible, string Text) NoteFor(CollectionLogWindow.DrillRead load, ServerClock? clock = null) =>
        OnStaThread(() =>
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            var banner = new System.Windows.Controls.TextBlock
            {
                Visibility = System.Windows.Visibility.Visible,
                Text = "Showing since 2000-01-01 00:00:00",
            };
            CollectionLogWindow.ShowDrillDataStart(banner, load, clock);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });

    /// <summary>
    /// The server's log began before the week (the other collector ran all month) and the collector under the drill began two
    /// days into it: far past the 90-minute slack, so a note worded from the COLLECTOR's first run would show. The server's log
    /// covers the week, so the probe answers the week's start and the window says nothing.
    /// </summary>
    [Fact]
    public async Task ACollectorThatStartedLateInACoveredWeek_ShowsNoNote()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(OtherCollector, new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc), AsOf, 360);
        await SeedRunsAsync(Collector, AsOf.AddDays(-2), AsOf, 120);

        var load = await LoadAsync();

        Assert.Equal(25, load.Rows.Count);
        Assert.All(load.Rows, r => Assert.Equal(Collector, r.CollectorName));
        Assert.Equal(WeekStart, load.StartUtc);
        Assert.Equal(WeekStart, load.Floor);
        var (visible, text) = NoteFor(load);
        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>A server whose first collection falls inside the week: the note names that first collection.</summary>
    [Fact]
    public async Task AServerWhoseFirstCollectionFallsInsideTheWeek_NamesIt()
    {
        await _duckDb.InitializeAsync();
        var first = new DateTime(2026, 9, 15, 9, 30, 0, DateTimeKind.Utc);
        await SeedRunsAsync(OtherCollector, first, AsOf, 60);
        await SeedRunsAsync(Collector, first, AsOf, 60);

        var load = await LoadAsync();

        Assert.NotEmpty(load.Rows);
        Assert.Equal(first, load.Floor);
        var (visible, text) = NoteFor(load);
        Assert.True(visible);
        Assert.Equal("Showing since 2026-09-15 09:30:00", text);
    }

    /// <summary>
    /// A quiet start: the server's log reaches back past the week's start, and the collector's own first run is only a little
    /// after it (inside the 90-minute slack of a first collection that lands just after the window starts). No note.
    /// </summary>
    [Fact]
    public async Task AFirstCollectionJustInsideTheSlack_ShowsNoNote()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(Collector, WeekStart.AddMinutes(45), AsOf, 60);

        var load = await LoadAsync();

        Assert.NotEmpty(load.Rows);
        Assert.Equal(WeekStart.AddMinutes(45), load.Floor);
        var (visible, text) = NoteFor(load);
        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// The note's time is worded in the zone the grid's rows print theirs in (<c>CollectionHealthTime</c>): the window's own
    /// server clock in Server time, else the active tab's when it is handed none, this machine's zone in Local time, and UTC
    /// in UTC. US Eastern is the window's server (EDT, -4 in September) and India (+5:30) the active tab's, so a note worded in
    /// the wrong one reads a different hour.
    /// </summary>
    [Fact]
    public async Task TheNote_IsWordedInTheZoneTheGridPrintsItsTimesIn()
    {
        await _duckDb.InitializeAsync();
        var first = new DateTime(2026, 9, 15, 13, 30, 0, DateTimeKind.Utc);
        await SeedRunsAsync(Collector, first, AsOf, 60);
        var load = await LoadAsync();
        Assert.Equal(first, load.Floor);

        ServerTimeHelper.ActiveServerClock = India();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var windowRow = new CollectionLogRow { CollectionTime = Naive(first), Clock = Eastern() };
        var unstampedRow = new CollectionLogRow { CollectionTime = Naive(first) };
        Assert.Equal("Showing since 2026-09-15 09:30:00", NoteFor(load, Eastern()).Text);
        Assert.Equal("09/15/2026 09:30", windowRow.CollectionTimeFormatted);
        Assert.Equal("Showing since 2026-09-15 19:00:00", NoteFor(load, clock: null).Text);
        Assert.Equal("09/15/2026 19:00", unstampedRow.CollectionTimeFormatted);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("Showing since 2026-09-15 13:30:00", NoteFor(load, Eastern()).Text);
        Assert.Equal("09/15/2026 13:30", windowRow.CollectionTimeFormatted);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        var local = DisplayZone.Format(Naive(first), TimeZoneInfo.Local, "yyyy-MM-dd HH:mm:ss");
        Assert.Equal("Showing since " + local, NoteFor(load, Eastern()).Text);
    }

    /// <summary>
    /// The read takes the window the probe takes: runs from the week's start to its end, both ends inclusive, newest first,
    /// and a run after the end is in neither the rows nor the note. The collector runs every 12 hours from a day before the
    /// week to a day after it; the other collector's log reaches back a month, so the week is covered.
    /// </summary>
    [Fact]
    public async Task TheRows_AreTheCollectorsRunsInsideTheWindow_BothEndsInclusive()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(OtherCollector, AsOf.AddDays(-30), AsOf.AddDays(1), 720);
        await SeedRunsAsync(Collector, WeekStart.AddDays(-1), AsOf.AddDays(1), 720);

        var load = await LoadAsync();

        Assert.Equal(15, load.Rows.Count);
        Assert.Equal(Naive(AsOf), load.Rows[0].CollectionTime);
        Assert.Equal(Naive(WeekStart), load.Rows[^1].CollectionTime);
        Assert.Equal(load.Rows.OrderByDescending(r => r.CollectionTime).Select(r => r.CollectionTime), load.Rows.Select(r => r.CollectionTime));
        Assert.Equal(WeekStart, load.Floor);
        Assert.False(NoteFor(load).Visible);
    }

    /// <summary>
    /// One window, worked out once: the read and the probe are each given the same start and the same end, the start a
    /// week before the end. A pinned end is the window's end; with none, it is one reading of the clock.
    /// </summary>
    [Fact]
    public async Task TheReadAndTheProbe_AreGivenTheSameWindow()
    {
        var read = new List<(DateTime Start, DateTime End)>();
        var probed = new List<(DateTime Start, DateTime End)>();
        Task<List<CollectionLogRow>> ReadRuns(DateTime start, DateTime end)
        {
            read.Add((start, end));
            return Task.FromResult(new List<CollectionLogRow>());
        }
        Task<DateTime?> ProbeFloor(DateTime start, DateTime end)
        {
            probed.Add((start, end));
            return Task.FromResult<DateTime?>(null);
        }

        await CollectionLogWindow.ReadDrillAsync(ReadRuns, ProbeFloor, "drill test", AsOf);

        Assert.Equal((WeekStart, AsOf), Assert.Single(read));
        Assert.Equal((WeekStart, AsOf), Assert.Single(probed));

        read.Clear();
        probed.Clear();
        var before = DateTime.UtcNow;
        await CollectionLogWindow.ReadDrillAsync(ReadRuns, ProbeFloor, "drill test");
        var after = DateTime.UtcNow;

        var window = Assert.Single(read);
        Assert.Equal(window, Assert.Single(probed));
        Assert.InRange(window.End, before, after);
        Assert.Equal(TimeSpan.FromDays(7), window.End - window.Start);
    }

    /// <summary>
    /// The probe is only the note: a probe that throws shows no note (a stale note from a last read is cleared too), the
    /// rows still load, and the failure is not rethrown to the window's own catch, which would put up a "Failed to load" box.
    /// </summary>
    [Fact]
    public async Task AProbeThatThrows_ShowsNoNote_AndTheRowsStillLoad()
    {
        var probeCalls = 0;
        Task<List<CollectionLogRow>> ReadRuns(DateTime start, DateTime end) => Task.FromResult(new List<CollectionLogRow>
        {
            new() { CollectorName = Collector, CollectionTime = Naive(AsOf.AddHours(-1)), Status = "SUCCESS" },
            new() { CollectorName = Collector, CollectionTime = Naive(AsOf.AddHours(-2)), Status = "ERROR" },
            new() { CollectorName = Collector, CollectionTime = Naive(AsOf.AddHours(-3)), Status = "SUCCESS" },
        });
        Task<DateTime?> ProbeFloor(DateTime start, DateTime end)
        {
            probeCalls++;
            return Task.FromException<DateTime?>(new InvalidOperationException("the probe failed"));
        }

        var load = await CollectionLogWindow.ReadDrillAsync(ReadRuns, ProbeFloor, "drill test", AsOf);

        Assert.Equal(1, probeCalls);
        Assert.Equal(3, load.Rows.Count);
        Assert.Null(load.Floor);
        var (visible, text) = NoteFor(load);
        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// A probe that has nothing to say (no run of the server's log inside the week) is a window with no note; and the rows
    /// the read found still load.
    /// </summary>
    [Fact]
    public async Task AWeekWithNoRunAtAll_ShowsNoNote_AndNoRows()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(Collector, AsOf.AddMinutes(5), AsOf.AddDays(1), 60);

        var load = await LoadAsync();

        Assert.Empty(load.Rows);
        Assert.Null(load.Floor);
        Assert.False(NoteFor(load).Visible);
    }

    /// <summary>
    /// The read is the one reading of the clock the window takes, and there is one of it: the hours-back overload that read
    /// its own is gone, so no caller can hand the read a start the probe was not given.
    /// </summary>
    [Fact]
    public void TheReadTakesTheWindow_NotAnHoursBack()
    {
        var overloads = typeof(LocalDataService).GetMethods()
            .Where(m => m.Name == nameof(LocalDataService.GetCollectionLogByCollectorAsync))
            .ToList();

        var read = Assert.Single(overloads);
        Assert.Equal(new[] { "serverId", "collectorName", "startUtc", "endUtc" }, read.GetParameters().Select(p => p.Name).ToArray());
        Assert.Equal(typeof(DateTime), read.GetParameters()[2].ParameterType);
        Assert.Equal(typeof(DateTime), read.GetParameters()[3].ParameterType);
    }

    /// <summary>
    /// The window's wiring, text-scanned from source (it cannot be built without the app's resources): it loads through the
    /// one drill step and words its banner through it, keeps stamping the server's clock on its rows, reads the clock once,
    /// takes the shared probe guard and banner rule (never a copy), probes the collection log relation, and names no row cap
    /// (the read has none, so the cap-aware steps do not apply). Its banner is declared collapsed, in the amber of the tab's.
    /// </summary>
    [Fact]
    public void TheWindow_LoadsAndWordsItsNoteThroughTheSharedSteps()
    {
        var code = Code(WindowFile("CollectionLogWindow.xaml.cs"));

        Assert.Equal(1, Matches(code, @"var load = await ReadDrillAsync\(_dataService,\s*_serverId,\s*_collectorName\);"));
        Assert.Equal(1, Matches(code, @"ShowDrillDataStart\(CollectionLogDrillTruncatedBanner,\s*load,\s*_serverClock\);"));
        Assert.Equal(1, Matches(code, @"foreach \(var log in logs\) log\.Clock = _serverClock;"));

        /* The one call to the read is in the drill step's data-service overload, beside its probe. */
        Assert.Equal(1, Matches(code, @"\.GetCollectionLogByCollectorAsync\("));
        Assert.Equal(1, Matches(code, @"\.GetQueryWindowFloorAsync\(\s*QueryWindowRelation\.CollectionLog,\s*serverId,\s*startUtc,\s*endUtc\)"));

        /* One clock reading and one span, and the same pair of instants goes to the read and to the probe. */
        Assert.Equal(1, Matches(code, @"DateTime\.UtcNow"));
        Assert.Equal(1, Matches(code, @"AddHours\("));
        Assert.Equal(1, Matches(code, @"var endUtc = asOfUtc \?\? DateTime\.UtcNow;"));
        Assert.Equal(1, Matches(code, @"var startUtc = endUtc\.AddHours\(-DrillHours\);"));
        Assert.Equal(1, Matches(code, @"await readRuns\(startUtc,\s*endUtc\)"));
        Assert.Equal(1, Matches(code, @"probeFloor\(startUtc,\s*endUtc\)"));
        Assert.Equal(168, CollectionLogWindow.DrillHours);

        /* The shared guard and rule, in the zone the grid prints its times in; no cap-aware step and no row-shown floor. */
        Assert.Equal(1, Matches(code, @"ServerTab\.ProbeWindowFloorOrNullAsync\("));
        Assert.Equal(1, Matches(code, @"ServerTab\.ApplyWindowFloorToBanner\(banner,\s*read\.Floor,\s*read\.StartUtc,\s*CollectionHealthTime\.Zone\(clock\)\);"));
        Assert.DoesNotContain("ApplyCappedWindowFloorToBanner", code, StringComparison.Ordinal);
        Assert.DoesNotContain("CapAwareWindowFloor", code, StringComparison.Ordinal);
        Assert.DoesNotContain("EarlierOfFloorAndRowShown", code, StringComparison.Ordinal);

        var xaml = WindowFile("CollectionLogWindow.xaml");
        var declared = Regex.Match(xaml, @"<TextBlock x:Name=""CollectionLogDrillTruncatedBanner""[^>]*/>", RegexOptions.Singleline);
        Assert.True(declared.Success, "CollectionLogDrillTruncatedBanner is not declared in CollectionLogWindow.xaml");
        Assert.Contains("Visibility=\"Collapsed\"", declared.Value, StringComparison.Ordinal);
        Assert.Contains("FontWeight=\"Bold\"", declared.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", declared.Value, StringComparison.Ordinal);
    }

    private static int Matches(string text, string pattern) => Regex.Matches(text, pattern).Count;

    private static string Code(string source) =>
        Regex.Replace(source, @"/\*.*?\*/|//[^\r\n]*", "", RegexOptions.Singleline);

    private static string WindowFile(string name, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Windows", name)));

    /// <summary>WPF objects require STA; same shape as the other banner tests.</summary>
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
}
