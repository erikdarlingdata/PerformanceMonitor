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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: the "Showing since" note of the Job History tab. The grid lists runs by the time each ran, and a server's first
/// collection copies the history msdb already holds, so a run can sit long before the collection that stored it. The note names
/// the earlier of the collector's coverage (the later of the first collection and the purge edge, from the shared probe over
/// <see cref="QueryWindowRelation.JobHistory"/>) and the earliest run the read returned, the way the Darling viewer's twin does.
/// A full page of the newest 2,000 runs names its oldest run, strictly, with no slack. A range of 90 minutes or less makes no
/// probe call, and a probe that fails shows no note and never costs the grid its rows.
///
/// <para>Most tests read real rows from DuckDB (the start overload of <see cref="LocalDataService.GetJobHistoryAsync(DateTime, int, int?, IReadOnlyDictionary{int, ServerClock}?)"/>),
/// ask the real probe (<see cref="LocalDataService.GetJobHistoryDataStartAsync"/>) and run the tab's real banner step
/// (<see cref="JobHistoryTab.ShowJobHistoryDataStartAsync"/>) on the answer. The step takes the probe's answer already completed:
/// a WPF banner is written on the thread that built it, and an answer that has completed keeps the continuation on that thread.
/// Each server's runs are stored on its own wall clock (a collected fixed offset), so the frame of every note is checked: one
/// server's note reads on that server's clock, as the Run Time column prints it, and the All Servers note reads in UTC and says so.</para>
/// </summary>
public sealed class JobHistoryDataStartTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerA = 701;
    private const int ServerB = 702;
    private const int OffsetA = 300;
    private const int OffsetB = -300;
    private const string Format = "yyyy-MM-dd HH:mm:ss";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    /// <summary>The window's end, to the second: every seeded time hangs off it.</summary>
    private readonly DateTime _end = Whole(DateTime.UtcNow);

    public JobHistoryDataStartTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>The default range of the tab's picker that matters here: seven days.</summary>
    private DateTime Start => _end.AddDays(-7);

    private static DateTime Whole(DateTime t) => new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    /// <summary>The server's wall clock at <paramref name="utc"/> for a fixed offset, to the second, the way sysjobhistory stores a run.</summary>
    private static DateTime Wall(DateTime utc, int offsetMinutes) => Whole(utc.AddMinutes(offsetMinutes));

    private static string Since(DateTime utc, int offsetMinutes) => "Showing since " + Wall(utc, offsetMinutes).ToString(Format, CultureInfo.InvariantCulture);

    private static string SinceUtc(DateTime utc) => "Showing since " + Whole(utc).ToString(Format, CultureInfo.InvariantCulture) + " UTC";

    // ── Seeding ──

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    /// <summary>One collected clock for the server: a fixed offset, no zone id.</summary>
    private async Task SeedClockAsync(int serverId, int offsetMinutes)
    {
        var connection = await SeedConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, 'TestSrv', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $4, NULL)";
        cmd.Parameters.Add(new DuckDBParameter { Value = -_nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _end });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = offsetMinutes });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One successful step-0 outcome that ran at <paramref name="runUtc"/> (stored on the server's wall clock) and that a
    /// collection stored at <paramref name="collectedUtc"/>.</summary>
    private async Task SeedRunAsync(int serverId, int offsetMinutes, DateTime runUtc, DateTime collectedUtc)
    {
        var connection = await SeedConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, category_name, step_id, step_name, run_status, run_status_desc, run_datetime,
     run_duration_seconds, retries_attempted, message)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, TRUE, 'Uncategorized (Local)', 0, '(Job outcome)', 1, 'The job succeeded.', $8, 30, 0, NULL)";
        var id = _nextId++;
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectedUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"S{serverId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"job_{id}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"job_{id}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Wall(runUtc, offsetMinutes) });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// <paramref name="count"/> runs a minute apart, the newest at <paramref name="newestRunUtc"/>, all stored by one collection at
    /// <paramref name="collectedUtc"/>: one statement, since a full page is 2,000 rows.
    /// </summary>
    private async Task SeedPageAsync(int serverId, int offsetMinutes, int count, DateTime newestRunUtc, DateTime collectedUtc)
    {
        var connection = await SeedConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, category_name, step_id, step_name, run_status, run_status_desc, run_datetime,
     run_duration_seconds, retries_attempted, message)
SELECT $1 + g.n, $2, $3, $4, $1 + g.n, 'job_' || CAST(g.n % 20 AS VARCHAR), 'job_' || CAST(g.n % 20 AS VARCHAR), TRUE, 'Uncategorized (Local)', 0, '(Job outcome)', 1,
       'The job succeeded.', $5::TIMESTAMP - to_minutes(CAST(g.n AS INTEGER)), 30, 0, NULL
FROM generate_series(0, $6 - 1) AS g(n)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectedUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"S{serverId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Wall(newestRunUtc, offsetMinutes) });
        cmd.Parameters.Add(new DuckDBParameter { Value = count });
        await cmd.ExecuteNonQueryAsync();
        _nextId += count;
    }

    /// <summary>A logged run of the job_history collector: the coverage the probe counts when no row sits near the window's start.</summary>
    private async Task SeedCollectorRunAsync(int serverId, DateTime at)
    {
        var connection = await SeedConnectionAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1, $2, $3, 'job_history', $4, 12, 'SUCCESS', 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"S{serverId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        await cmd.ExecuteNonQueryAsync();
    }

    // ── Running the read, the probe and the tab's step ──

    /// <summary>
    /// What the tab shows for the server (or, with <paramref name="serverId"/> null, for all servers): the real start-overload read,
    /// the real probe over <paramref name="asked"/>, then the tab's real banner step in the frame the tab words it in. The note's
    /// text, or null when the banner is hidden, and the number of rows the read returned.
    /// </summary>
    private async Task<(string? Note, int Rows)> NoteAsync(int? serverId, params int[] asked)
    {
        var service = new LocalDataService(_duckDb);
        var read = await service.GetJobHistoryAsync(Start, JobHistoryTab.RowCap, serverId);
        var floor = await service.GetJobHistoryDataStartAsync(asked, Start, _end);
        var zone = serverId is int id ? (await service.ReadJobHistoryClockAsync(id)).AsTimeZone() : TimeZoneInfo.Utc;

        return (BannerText(() => Task.FromResult(floor), Start, _end, read, zone, inUtc: serverId is null), read.Count);
    }

    /// <summary>The banner the tab's step leaves for these inputs, read off the control: its text, or null when it is hidden. Seeded
    /// visible with stale text, so a step that writes nothing cannot pass as a hidden banner.</summary>
    private static string? BannerText(
        Func<Task<DateTime?>> probe, DateTime startUtc, DateTime endUtc, IReadOnlyCollection<JobHistoryRow> read, TimeZoneInfo zone, bool inUtc)
    {
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock { Visibility = System.Windows.Visibility.Visible, Text = "stale" };
            JobHistoryTab.ShowJobHistoryDataStartAsync(banner, probe, startUtc, endUtc, read, zone, inUtc).GetAwaiter().GetResult();
            return banner.Visibility == System.Windows.Visibility.Visible ? banner.Text : null;
        });
    }

    private static List<JobHistoryRow> FakePage(DateTime oldestUtc, int count) =>
        Enumerable.Range(0, count).Select(i => new JobHistoryRow { RunDateTimeUtc = oldestUtc.AddMinutes(i) }).ToList();

    // ── The cap and the relation ──

    [Fact]
    public void TheCap_IsThe2000TheReadTakes_AndTheNoteStepGetsTheSameConstant()
    {
        Assert.Equal(2000, JobHistoryTab.RowCap);

        var tab = StripComments(File.ReadAllText(RepoFile("Lite", "Controls", "JobHistoryTab.xaml.cs")).Replace("\r\n", "\n"));
        Assert.Single(Regex.Matches(tab, @"GetJobHistoryAsync\(startUtc, RowCap, serverId, openTabClocks\)"));
        Assert.Single(Regex.Matches(tab, @"ServerTab\.CappedGridBannerAsync\(runTimes, RowCap,"));
    }

    [Fact]
    public void TheProbe_ReadsTheJobHistoryView_OnCollectionTime_WithTheCollectorsRunsAsCoverage()
    {
        Assert.Equal("v_job_history", LocalDataService.QueryWindowRelationView(QueryWindowRelation.JobHistory));
        Assert.Equal("job_history", LocalDataService.QueryWindowRelationCollector(QueryWindowRelation.JobHistory));
        Assert.Equal("collection_time", LocalDataService.QueryWindowRelationTimeColumn(QueryWindowRelation.JobHistory));
        Assert.False(LocalDataService.QueryWindowRelationTimeIsServerLocal(QueryWindowRelation.JobHistory));
    }

    // ── What the note says ──

    /// <summary>The range reaches before the coverage: the collector was added 3 days ago, in a 7-day range, and its first run
    /// came 6 hours after that. The note names the coverage start, on the server's own clock.</summary>
    [Fact]
    public async Task ARangePastTheCoverage_NamesTheCoverageStart()
    {
        await SeedClockAsync(ServerA, OffsetA);
        var firstCollection = _end.AddDays(-3);
        await SeedCollectorRunAsync(ServerA, firstCollection);
        await SeedRunAsync(ServerA, OffsetA, firstCollection.AddHours(6), firstCollection.AddHours(6).AddMinutes(2));
        await SeedRunAsync(ServerA, OffsetA, _end.AddHours(-1), _end.AddMinutes(-58));

        var (note, rows) = await NoteAsync(ServerA, ServerA);

        Assert.Equal(2, rows);
        Assert.Equal(Since(firstCollection, OffsetA), note);
    }

    /// <summary>A quiet start: the collector covered the whole range and the first run came 5 hours in. No note, whether what proves
    /// the coverage is an older logged run of the collector or an older stored row.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AQuietStart_ShowsNoNote(bool anOlderRowProvesIt)
    {
        await SeedClockAsync(ServerA, OffsetA);
        if (anOlderRowProvesIt)
        {
            await SeedRunAsync(ServerA, OffsetA, Start.AddDays(-10), Start.AddDays(-10));
        }
        else
        {
            await SeedCollectorRunAsync(ServerA, Start.AddDays(-10));
        }

        await SeedRunAsync(ServerA, OffsetA, Start.AddHours(5), Start.AddHours(5).AddMinutes(2));

        var (note, rows) = await NoteAsync(ServerA, ServerA);

        Assert.Equal(1, rows);
        Assert.Null(note);
    }

    /// <summary>
    /// Job history is an event surface: the first collection, 3 days into the range, copied a run that ran 5 days into it, 2 days
    /// BEFORE the collection. The coverage starts at the collection, but the grid shows a run from before it, so the note names that
    /// run's time, to the second, never a time later than a run on the screen.
    /// </summary>
    [Fact]
    public async Task ARunStampedBeforeTheFirstCollection_IsTheTimeNamed()
    {
        await SeedClockAsync(ServerA, OffsetA);
        var firstCollection = _end.AddDays(-3);
        var copiedRun = _end.AddDays(-5).AddSeconds(-37);
        await SeedCollectorRunAsync(ServerA, firstCollection);
        await SeedRunAsync(ServerA, OffsetA, copiedRun, firstCollection);
        await SeedRunAsync(ServerA, OffsetA, firstCollection.AddHours(6), firstCollection.AddHours(6).AddMinutes(2));

        var (note, rows) = await NoteAsync(ServerA, ServerA);

        Assert.Equal(2, rows);
        Assert.Equal(Since(copiedRun, OffsetA), note);
    }

    /// <summary>The same history, but the copied run sits inside the 90-minute slack of the range's start: the note stays hidden.</summary>
    [Fact]
    public async Task ACopiedRunWithinTheSlackOfTheStart_ShowsNoNote()
    {
        await SeedClockAsync(ServerA, OffsetA);
        var firstCollection = _end.AddDays(-3);
        await SeedCollectorRunAsync(ServerA, firstCollection);
        await SeedRunAsync(ServerA, OffsetA, Start.AddMinutes(30), firstCollection);

        // The coverage starts 4 days after the range does, but the earliest run shown is 30 minutes after it: the note would name that
        // run, and 30 minutes is inside the 90-minute slack.
        var (note, rows) = await NoteAsync(ServerA, ServerA);

        Assert.Equal(1, rows);
        Assert.Null(note);
    }

    // ── The cap ──

    /// <summary>
    /// A full page of the newest 2,000 runs names its oldest run, whatever the store covers (the collector has run for a month, so the
    /// probe would say covered), and the probe is never asked. A page one row short is judged by the probe, which finds the range
    /// covered: no note. The oldest run is named to the second, on the server's own clock.
    /// </summary>
    [Fact]
    public async Task AFullPage_NamesItsOldestRun_AndAPageOneRowShortDoesNot()
    {
        await SeedClockAsync(ServerA, OffsetA);
        await SeedClockAsync(ServerB, OffsetB);
        var collectedLongAgo = Start.AddDays(-30);
        await SeedCollectorRunAsync(ServerA, collectedLongAgo);
        await SeedCollectorRunAsync(ServerB, collectedLongAgo);
        await SeedCollectorRunAsync(ServerA, _end.AddMinutes(-5));
        await SeedCollectorRunAsync(ServerB, _end.AddMinutes(-5));
        await SeedPageAsync(ServerA, OffsetA, JobHistoryTab.RowCap, _end.AddMinutes(-1), collectedLongAgo);
        await SeedPageAsync(ServerB, OffsetB, JobHistoryTab.RowCap - 1, _end.AddMinutes(-1), collectedLongAgo);

        var full = await NoteAsync(ServerA, ServerA);
        var oneShort = await NoteAsync(ServerB, ServerB);

        Assert.Equal(JobHistoryTab.RowCap, full.Rows);
        Assert.Equal(Since(_end.AddMinutes(-1 - (JobHistoryTab.RowCap - 1)), OffsetA), full.Note);
        Assert.Equal(JobHistoryTab.RowCap - 1, oneShort.Rows);
        Assert.Null(oneShort.Note);
    }

    /// <summary>A full page asks no probe at all: its reach is its oldest row.</summary>
    [Fact]
    public void AFullPage_NeedsNoProbe()
    {
        var calls = 0;
        var oldest = Start.AddHours(8);

        var note = BannerText(() => { calls++; return Task.FromResult<DateTime?>(Start); }, Start, _end, FakePage(oldest, JobHistoryTab.RowCap), TimeZoneInfo.Utc, inUtc: false);

        Assert.Equal(0, calls);
        Assert.Equal("Showing since " + Whole(oldest).ToString(Format, CultureInfo.InvariantCulture), note);
    }

    /// <summary>
    /// The cap rule has no slack, and is strict: the probe's 90-minute slack absorbs a first collection a little after the range
    /// starts, but a full page dropped runs for certain. An oldest run 10 minutes after the start names it; one AT the start does
    /// not; and a page one row short with the same oldest run is the probe's to judge (a coverage that reaches the start shows nothing).
    /// </summary>
    [Fact]
    public void AFullPage_HasNoSlack_AndTheComparisonIsStrict()
    {
        var oldest = Start.AddMinutes(10);
        Func<Task<DateTime?>> covered = () => Task.FromResult<DateTime?>(Start);

        Assert.Equal("Showing since " + Whole(oldest).ToString(Format, CultureInfo.InvariantCulture),
            BannerText(covered, Start, _end, FakePage(oldest, JobHistoryTab.RowCap), TimeZoneInfo.Utc, inUtc: false));
        Assert.Null(BannerText(covered, Start, _end, FakePage(Start, JobHistoryTab.RowCap), TimeZoneInfo.Utc, inUtc: false));
        Assert.Null(BannerText(covered, Start, _end, FakePage(oldest, JobHistoryTab.RowCap - 1), TimeZoneInfo.Utc, inUtc: false));
    }

    // ── A probe that fails, and a range too short to probe ──

    /// <summary>A probe that throws shows no note and the step does not throw, and the rows are the read's own: the read does not depend on the probe.</summary>
    [Fact]
    public async Task AProbeThatThrows_ShowsNoNote_AndTheRowsStillLoad()
    {
        await SeedClockAsync(ServerA, OffsetA);
        await SeedRunAsync(ServerA, OffsetA, _end.AddHours(-2), _end.AddHours(-2));
        var read = await new LocalDataService(_duckDb).GetJobHistoryAsync(Start, JobHistoryTab.RowCap, ServerA);

        string? faulted = BannerText(() => Task.FromException<DateTime?>(new InvalidOperationException("probe failed")), Start, _end, read, TimeZoneInfo.Utc, inUtc: false);
        string? thrown = BannerText(() => throw new InvalidOperationException("probe failed"), Start, _end, read, TimeZoneInfo.Utc, inUtc: false);

        Assert.Single(read);
        Assert.Null(faulted);
        Assert.Null(thrown);
    }

    /// <summary>A range of 90 minutes or less can never get a coverage note, so the probe is not called for it (60 and 90); 91 calls it once.</summary>
    [Theory]
    [InlineData(60, 0)]
    [InlineData(90, 0)]
    [InlineData(91, 1)]
    public void ARangeNoLongerThanTheSlack_MakesNoProbeCall(int minutes, int expectedCalls)
    {
        var calls = 0;
        var start = _end.AddMinutes(-minutes);

        var note = BannerText(() => { calls++; return Task.FromResult<DateTime?>(_end); }, start, _end, [], TimeZoneInfo.Utc, inUtc: false);

        Assert.Equal(expectedCalls, calls);
        if (expectedCalls == 0)
        {
            Assert.Null(note);
        }
    }

    /// <summary>A superseded load writes nothing: the newer load owns the banner.</summary>
    [Fact]
    public void ASupersededLoad_WritesNoNote()
    {
        var note = OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock { Visibility = System.Windows.Visibility.Collapsed, Text = "newer" };
            JobHistoryTab.ShowJobHistoryDataStartAsync(
                banner, () => Task.FromResult<DateTime?>(_end.AddDays(-1)), Start, _end, [], TimeZoneInfo.Utc, inUtc: false, superseded: () => true).GetAwaiter().GetResult();
            return (banner.Visibility, banner.Text);
        });

        Assert.Equal(System.Windows.Visibility.Collapsed, note.Visibility);
        Assert.Equal("newer", note.Text);
    }

    // ── The frame and the All Servers view ──

    /// <summary>
    /// Two servers on different clocks, one in a 7-day range. One server's note reads on THAT server's wall clock (A is 5 hours ahead
    /// of UTC) and carries no zone name, like the Run Time column beside it. The All Servers note reads in UTC and says so, because
    /// the rows beside it sit on different servers' clocks, and it names the EARLIEST coverage among the servers the Server combo
    /// lists: B's collection came 5 days ago, A's 3.
    /// </summary>
    [Fact]
    public async Task OneServerReadsOnItsOwnClock_AndAllServersReadInUtc_AtTheEarliestCoverage()
    {
        await SeedClockAsync(ServerA, OffsetA);
        await SeedClockAsync(ServerB, OffsetB);
        var firstA = _end.AddDays(-3);
        var firstB = _end.AddDays(-5);
        await SeedCollectorRunAsync(ServerA, firstA);
        await SeedCollectorRunAsync(ServerB, firstB);
        await SeedRunAsync(ServerA, OffsetA, firstA.AddHours(6), firstA.AddHours(6).AddMinutes(2));
        await SeedRunAsync(ServerB, OffsetB, firstB.AddHours(6), firstB.AddHours(6).AddMinutes(2));

        var oneA = await NoteAsync(ServerA, ServerA);
        var oneB = await NoteAsync(ServerB, ServerB);
        var all = await NoteAsync(null, ServerA, ServerB);

        Assert.Equal(Since(firstA, OffsetA), oneA.Note);
        Assert.Equal(Since(firstB, OffsetB), oneB.Note);
        Assert.Equal(SinceUtc(firstB), all.Note);
        Assert.Equal(2, all.Rows);
        Assert.DoesNotContain("UTC", oneA.Note, StringComparison.Ordinal);
    }

    /// <summary>One server that covers the whole range is enough: the earliest coverage among the servers reaches the start, so the All
    /// Servers view shows no note, although the other server's collection began late.</summary>
    [Fact]
    public async Task AllServers_OneServerThatCoversTheRange_ShowsNoNote()
    {
        await SeedClockAsync(ServerA, OffsetA);
        await SeedClockAsync(ServerB, OffsetB);
        await SeedCollectorRunAsync(ServerA, Start.AddDays(-20));
        await SeedRunAsync(ServerA, OffsetA, Start.AddHours(30), Start.AddHours(30).AddMinutes(2));
        await SeedCollectorRunAsync(ServerB, _end.AddDays(-2));
        await SeedRunAsync(ServerB, OffsetB, _end.AddDays(-1), _end.AddDays(-1).AddMinutes(2));

        var (note, rows) = await NoteAsync(null, ServerB, ServerA);

        Assert.Equal(2, rows);
        Assert.Null(note);
    }

    /// <summary>
    /// How the All Servers probe is bounded. Servers are asked in order and the loop stops at the first one whose coverage reaches the
    /// range's start (the earliest coverage cannot be later), so a fleet whose first server covers costs one probe; with none covering, every
    /// listed server is asked once; a server with nothing in the window answers nothing and is skipped; and a probe that throws fails the
    /// whole answer (no note), because a floor from some of the servers could be later than one a server it never reached covers.
    /// </summary>
    [Fact]
    public async Task AllServers_AsksOnlyTheListedServers_StopsAtTheFirstThatCovers_AndAThrowFailsTheNote()
    {
        var asked = new List<int>();
        Func<Dictionary<int, DateTime?>, Func<int, Task<DateTime?>>> probeOf = answers => id => { asked.Add(id); return Task.FromResult(answers[id]); };

        var covering = await LocalDataService.EarliestCoverageAsync([1, 2, 3], probeOf(new() { [1] = Start, [2] = _end.AddDays(-1), [3] = _end.AddDays(-2) }), Start);
        Assert.Equal(Start, covering);
        Assert.Equal(new[] { 1 }, asked.ToArray());

        asked.Clear();
        var earliest = await LocalDataService.EarliestCoverageAsync([1, 2, 3], probeOf(new() { [1] = _end.AddDays(-1), [2] = null, [3] = _end.AddDays(-2) }), Start);
        Assert.Equal(_end.AddDays(-2), earliest);
        Assert.Equal(new[] { 1, 2, 3 }, asked.ToArray());

        asked.Clear();
        Assert.Null(await LocalDataService.EarliestCoverageAsync([1, 2], probeOf(new() { [1] = null, [2] = null }), Start));
        Assert.Null(await LocalDataService.EarliestCoverageAsync([], probeOf(new()), Start));

        var note = BannerText(
            () => LocalDataService.EarliestCoverageAsync([1, 2], id => id == 2 ? Task.FromException<DateTime?>(new InvalidOperationException("probe failed")) : Task.FromResult<DateTime?>(_end.AddDays(-1)), Start),
            Start, _end, FakePage(_end.AddDays(-1), 3), TimeZoneInfo.Utc, inUtc: true);
        Assert.Null(note);
    }

    // ── The tab's wiring, pinned from source ──

    private static string LoadJobsBody(string tab)
    {
        var start = tab.IndexOf("private async System.Threading.Tasks.Task LoadJobsAsync()", StringComparison.Ordinal);
        Assert.True(start >= 0, "LoadJobsAsync is not in Lite/Controls/JobHistoryTab.xaml.cs");
        var next = Regex.Match(tab[(start + 20)..], @"\n    (private|internal|public) ");
        return next.Success ? tab.Substring(start, 20 + next.Index) : tab[start..];
    }

    private static string TabSource() =>
        StripComments(File.ReadAllText(RepoFile("Lite", "Controls", "JobHistoryTab.xaml.cs")).Replace("\r\n", "\n"));

    /// <summary>The window's start is worked out once and the read and the probe both take it: one clock read, no second <c>DateTime.UtcNow</c>.</summary>
    [Fact]
    public void TheTab_WorksOutTheWindowsStartOnce_AndHandsItToTheReadAndTheProbe()
    {
        var tab = TabSource();
        var load = LoadJobsBody(tab);

        Assert.Single(Regex.Matches(load, @"var nowUtc = DateTime\.UtcNow;"));
        Assert.Single(Regex.Matches(load, @"var startUtc = nowUtc\.AddHours\(-hoursBack\);"));
        Assert.Empty(Regex.Matches(load, @"DateTime\.UtcNow\.AddHours"));
        Assert.Single(Regex.Matches(load, @"_dataService\.GetJobHistoryAsync\(startUtc,\s*RowCap,\s*serverId,\s*openTabClocks\)"));
        Assert.Single(Regex.Matches(load, @"ShowDataStartNoteAsync\(serverId,\s*openTabClocks,\s*startUtc,\s*nowUtc,\s*all,\s*gen\)"));
        Assert.Single(Regex.Matches(tab, @"service\.GetJobHistoryDataStartAsync\(asked,\s*startUtc,\s*endUtc\)"));
        Assert.Single(Regex.Matches(tab, @"var openTabClocks = _openTabClocks\?\.Invoke\(\);"));
    }

    /// <summary>The note comes last: after the rows are bound and the loading note is down, from the read before the Status and Category
    /// filters (<c>all</c>, never <c>filtered</c>), and the cap label stays in the count text.</summary>
    [Fact]
    public void TheNote_ComesAfterTheRowsAreBound_FromTheUnfilteredRead()
    {
        var load = LoadJobsBody(TabSource());

        var bind = load.IndexOf("_filterManager.UpdateData(filtered)", StringComparison.Ordinal);
        var loadingDown = load.IndexOf("LoadingMessage.Visibility = Visibility.Collapsed;", Math.Max(bind, 0), StringComparison.Ordinal);
        var note = load.IndexOf("await ShowDataStartNoteAsync(", StringComparison.Ordinal);
        Assert.True(bind >= 0 && loadingDown > bind && note > loadingDown, "the note must come after the rows and the loading note");
        Assert.DoesNotContain("ShowDataStartNoteAsync(serverId, openTabClocks, startUtc, nowUtc, filtered", load, StringComparison.Ordinal);
        Assert.Matches(@"JobCountIndicator\.Text = JobHistoryCap\.CountText\(displayCount, all\.Count, RowCap\);", load);
    }

    /// <summary>The tab's step goes through the shared steps (the cap-aware decision, the guarded probe, the event-time rule and the two
    /// banner verdicts), not a second copy of them, and asks the data service for the one probe.</summary>
    [Fact]
    public void TheNoteStep_GoesThroughTheSharedSteps_AndTheFrameFollowsTheView()
    {
        var tab = TabSource();
        var start = tab.IndexOf("internal static System.Threading.Tasks.Task ShowJobHistoryDataStartAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var step = tab[start..tab.IndexOf("private async System.Threading.Tasks.Task UpdateAgentStatusAsync(", start, StringComparison.Ordinal)];

        Assert.Contains("ServerTab.CappedGridBannerAsync(runTimes, RowCap, t => t,", step, StringComparison.Ordinal);
        Assert.Contains("ServerTab.ApplyCappedWindowFloorToBanner(banner, oldestRowShown, startUtc, zone);", step, StringComparison.Ordinal);
        Assert.Contains("ServerTab.ProbeWindowFloorOrNullAsync(probe, \"Job History\", startUtc, endUtc)", step, StringComparison.Ordinal);
        Assert.Contains("ServerTab.ApplyWindowFloorToBanner(banner, ServerTab.EarlierOfFloorAndRowShown(floor, ServerTab.EarliestRowShown(runTimes, t => t)), startUtc, zone);", step, StringComparison.Ordinal);
        Assert.DoesNotContain("GetQueryWindowFloorAsync(", tab, StringComparison.Ordinal);

        var note = tab[tab.IndexOf("private async System.Threading.Tasks.Task ShowDataStartNoteAsync(", StringComparison.Ordinal)..start];
        Assert.Contains("var clock = await System.Threading.Tasks.Task.Run(() => service.ReadJobHistoryClockAsync(one, tabClock));", note, StringComparison.Ordinal);
        Assert.Contains("zone = clock.AsTimeZone();", note, StringComparison.Ordinal);
        Assert.Contains("zone = TimeZoneInfo.Utc;", note, StringComparison.Ordinal);
        Assert.Contains("asked = ListedServerIds();", note, StringComparison.Ordinal);
        Assert.Contains("inUtc: serverId is null", note, StringComparison.Ordinal);
    }

    /// <summary>The banner sits in its own row above the grid, in the style of the other "Showing since" banners, and the shell hands the
    /// tab the open tabs' clocks.</summary>
    [Fact]
    public void TheBanner_SitsAboveTheGrid_InTheExistingStyle_AndTheShellHandsTheTabsClocks()
    {
        var xaml = File.ReadAllText(RepoFile("Lite", "Controls", "JobHistoryTab.xaml")).Replace("\r\n", "\n");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""1"" x:Name=""JobHistoryWindowTruncatedBanner""[^>]*/>", RegexOptions.Singleline);
        Assert.True(banner.Success, "JobHistoryWindowTruncatedBanner is not declared in JobHistoryTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("FontWeight=\"Bold\"", banner.Value, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(xaml, @"<RowDefinition Height=").Count);
        Assert.Matches(@"<Grid Grid\.Row=""2"" Margin=""10,0,10,10"">\s*<DataGrid x:Name=""JobHistoryDataGrid""", xaml);

        var shell = StripComments(File.ReadAllText(RepoFile("Lite", "MainWindow.xaml.cs")).Replace("\r\n", "\n"));
        Assert.Matches(@"JobHistoryContent\.Initialize\(_dataService, \(\) =>.*?return map;\s*\}, OpenTabClocks\);", shell.Replace("\n", " "));
    }

    // ── Helpers ──

    private static string StripComments(string lfSource) =>
        Regex.Replace(Regex.Replace(lfSource, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);

    private static string RepoFile(string folder, string subFolderOrFile, string? file = null, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", folder, subFolderOrFile, file ?? string.Empty));

    /// <summary>WPF objects require STA, and a probe answer that has already completed keeps the continuation on this thread.</summary>
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
