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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's Job History tab says where its data starts (#4966). Job history is an event surface: the read windows on the
/// run's own time, and a server's first collection copies the history msdb already holds, so a run can sit long before the coverage
/// the probe found. The note names the earlier of the coverage start and the earliest run shown. The read keeps the newest 2,000 runs,
/// so a full page names its oldest run, whatever the store covers. These tests pin the tab's wiring and run its banner step on the
/// real control; <c>ViewerJobHistoryDataStartLiveTests</c> runs the same step against a store.
/// </summary>
/* The banner's time text reads the process-wide display mode, which these tests set (restored in Dispose); one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerJobHistoryDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    /* A store nothing listens on: a probe that starts a query against it fails, one that does not returns at once. */
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    private static DateTime? At(int days, int hours = 0) => RangeStart.AddDays(days).AddHours(hours);

    // ── The cap and the probe ──

    [Fact]
    public void TheCap_IsThe2000TheReadTakes()
    {
        Assert.Equal(2000, JobHistoryTab.RowCap);

        var read = MethodBody(ViewerFile("ViewerDataService.JobHistory.cs"), @"public async Task<List<ViewerJobHistoryRow>> GetJobHistoryAsync\(");
        Assert.Contains("int limit = 2000", read, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbe_GoesThroughTheSharedFloor_OverTheJobHistoryCollectorTable_OnCollectionTime()
    {
        /* The probe asks about the collector's coverage, which the table's own time column (collection_time) carries; the run's own
           time is the caller's to compare. If the catalog stops listing the table with that index, this fails here first. */
        Assert.True(DataWindowFloor.Source.TryForCollectorTable("job_history", out var source));
        Assert.Equal("collection_time", source.TimeColumn);

        var probe = MethodBody(ViewerFile("ViewerDataService.JobHistory.cs"), @"public async Task<DateTime\?> GetJobHistoryDataStartAsync\(");
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"job_history\")", probe, StringComparison.Ordinal);
        /* One server through the server form; every server through the fleet form (the earliest coverage among the servers that count). */
        Assert.Equal(1, Matches(probe, @"DataWindowFloor\.GetForServerAsync\("));
        Assert.Equal(1, Matches(probe, @"DataWindowFloor\.GetAsync\("));
    }

    // ── A window no longer than the slack starts no probe ──

    /* The 90-minute slack belongs to the coverage probe alone: a window no longer than it can never get a coverage note. The server form
       says so itself; the fleet form (the tab's All Servers view) has no such guard, so the tab's probe adds it. */
    [Theory]
    [InlineData(60, true)]
    [InlineData(60, false)]
    [InlineData(90, true)]
    [InlineData(90, false)]
    public async Task AWindowNoLongerThanTheSlack_StartsNoProbe_ForOneServerAndForAllServers_AgainstNoStore(int minutes, bool oneServer)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        Assert.Null(await viewer.GetJobHistoryDataStartAsync(oneServer ? 1 : null, RangeStart, RangeStart.AddMinutes(minutes), ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AWindowOneMinuteOverTheSlack_StartsItsProbe_ForOneServerAndForAllServers_AgainstNoStore(bool oneServer)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetJobHistoryDataStartAsync(oneServer ? 1 : null, RangeStart, RangeStart.AddMinutes(91), ct));
    }

    // ── The tab: one start for the read and the probe, the note after the rows ──

    [Fact]
    public void TheTab_WorksOutTheWindowsStartOnce_AndHandsItToTheReadAndTheProbe()
    {
        var load = MethodBody(ViewerFile("JobHistoryTab.xaml.cs"), @"private async Task LoadJobsAsync\(");

        Assert.Equal(1, Matches(load, @"var nowUtc = DateTime\.UtcNow;"));
        Assert.Equal(1, Matches(load, @"var sinceUtc = nowUtc\.AddHours\(-hoursBack\);"));
        Assert.Equal(0, Matches(load, @"DateTime\.UtcNow\.AddHours"));
        Assert.Equal(1, Matches(load, @"var dataStartTask = _dataService\.GetJobHistoryDataStartAsync\(serverId,\s*sinceUtc,\s*nowUtc\);"));
        Assert.Equal(1, Matches(load, @"var readTask = _dataService\.GetJobHistoryAsync\(sinceUtc,\s*serverId,\s*RowCap\)"));
        /* The probe starts beside the read, not after it. */
        Assert.True(
            load.IndexOf("GetJobHistoryDataStartAsync(", StringComparison.Ordinal) < load.IndexOf("var readTask = _dataService.GetJobHistoryAsync(", StringComparison.Ordinal),
            "the probe starts after the read has been awaited");
    }

    [Fact]
    public void TheRead_IsAwaitedThroughTheProbeWatchHelper()
    {
        var load = MethodBody(ViewerFile("JobHistoryTab.xaml.cs"), @"private async Task LoadJobsAsync\(");

        Assert.Equal(1, Matches(load, @"ViewerServerTab\.AwaitReadWatchingProbeAsync\(readTask,\s*dataStartTask,\s*""Job History""\)"));
        Assert.Equal(1, Matches(load, @"var all = await readTask;"));
        Assert.Equal(0, Matches(load, @"await _dataService\.GetJobHistoryAsync\("));
    }

    [Fact]
    public void TheNote_ComesAfterTheRowsAreBound_FromTheUnfilteredRead_AndAfterASupersedeCheck()
    {
        var load = MethodBody(ViewerFile("JobHistoryTab.xaml.cs"), @"private async Task LoadJobsAsync\(");

        var bind = load.IndexOf("_filterManager.UpdateData(filtered)", StringComparison.Ordinal);
        var settle = load.IndexOf("await ((Task)dataStartTask).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);", StringComparison.Ordinal);
        Assert.DoesNotContain("Task.WhenAny", load, StringComparison.Ordinal);
        var loadingDown = load.LastIndexOf("LoadingMessage.Visibility = Visibility.Collapsed;", settle, StringComparison.Ordinal);
        var note = load.IndexOf("await ShowJobHistoryDataStartAsync(", StringComparison.Ordinal);
        Assert.True(bind >= 0 && loadingDown > bind && settle > loadingDown && note > settle, "the note must come after the rows, the loading note and the settled probe");

        /* The newest load is the only one allowed to write the note: a check, with no await between it and the step. */
        var between = load[settle..note];
        Assert.Matches(@"if \(_loads\.Superseded\(nameof\(LoadJobsAsync\), gen\)\) return;", between);

        /* The unfiltered read, the one the cap label counts: the Status and Category filters narrow the grid on the client and say
           nothing about where the data starts. */
        Assert.Equal(1, Matches(load, @"await ShowJobHistoryDataStartAsync\(JobHistoryTruncationBanner,\s*dataStartTask,\s*sinceUtc,\s*all\);"));
        /* #4966: the cap label is built by JobHistoryCap.CountText from the last read's row count, which the load takes from the unfiltered
           read (all), so the Status and Category filters cannot take the label away; the column-filter handler builds the same text from
           the same field (JobHistoryCountTextTests). */
        Assert.Equal(1, Matches(load, @"_lastReadRowCount\s*=\s*all\.Count;"));
        Assert.Equal(1, Matches(load, @"JobHistoryCap\.CountText\(displayCount,\s*_lastReadRowCount,\s*RowCap\)"));
        Assert.DoesNotContain("ShowJobHistoryDataStartAsync(JobHistoryTruncationBanner, dataStartTask, sinceUtc, filtered)", load, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNoteStep_GoesThroughTheSharedEventStep_WithTheRunTimeAndTheCap()
    {
        var step = MethodBody(ViewerFile("JobHistoryTab.xaml.cs"), @"internal static Task ShowJobHistoryDataStartAsync\(");

        Assert.Matches(
            @"ViewerServerTab\.ShowEventDataStartAsync\(banner,\s*probe,\s*""Job History"",\s*startUtc,\s*read\.Select\(r => r\.RunDateTimeUtc\),\s*RowCap\)", step);
    }

    [Fact]
    public void TheBanner_SitsAboveTheGrid_InTheExistingStyle()
    {
        var xaml = ViewerFile("JobHistoryTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""1"" x:Name=""JobHistoryTruncationBanner""[^>]*/>", RegexOptions.Singleline);
        Assert.True(banner.Success, "JobHistoryTruncationBanner is not declared in JobHistoryTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("FontWeight=\"Bold\"", banner.Value, StringComparison.Ordinal);
        Assert.Equal(3, Matches(xaml, @"<RowDefinition Height="));
        Assert.Matches(@"<Grid Grid\.Row=""2"" Margin=""10,0,10,10"">\s*<DataGrid x:Name=""JobHistoryDataGrid""", xaml);
    }

    // ── What the note says ──

    /* The note raised for the probe's answer and the runs the read returned (their times), read off the control; null when it is
       hidden. Seeded visible, so a no-op cannot pass as a hidden banner. */
    private static string? BannerFor(Task<DateTime?> probe, IEnumerable<DateTime?> runTimes, DateTime? startUtc = null)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };
            var read = runTimes.Select(t => new ViewerJobHistoryRow { RunDateTimeUtc = t }).ToList();

            JobHistoryTab.ShowJobHistoryDataStartAsync(banner, probe, startUtc ?? RangeStart, read).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    private static string? BannerFor(DateTime? coverageStartUtc, IEnumerable<DateTime?> runTimes, DateTime? startUtc = null) =>
        BannerFor(Task.FromResult(coverageStartUtc), runTimes, startUtc);

    /* A page of `count` runs a minute apart from `oldest`. */
    private static IEnumerable<DateTime?> Page(DateTime oldest, int count) => Enumerable.Range(0, count).Select(i => (DateTime?)oldest.AddMinutes(i));

    /* The range reaches before the coverage: the collector was added 3 days into it and the first run came after. The note names the
       coverage start. */
    [Fact]
    public void ARangePastTheCoverage_NamesTheCoverageStart()
    {
        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(At(3), [At(3, 6), At(5)]));
    }

    /* A quiet start: the store covered the whole range, and the first run came 5 hours in. No note. */
    [Fact]
    public void AQuietStart_ShowsNoNote()
    {
        Assert.Null(BannerFor(At(-20), [At(0, 5), At(2)]));
    }

    /* The first collection copied history from before itself: a run stamped a day before the coverage start is the time named. */
    [Fact]
    public void ARunStampedBeforeTheFirstCollection_IsTheTimeNamed()
    {
        Assert.Equal("Showing since 2026-09-02 00:00:00", BannerFor(At(3), [At(1), At(3, 6)]));
    }

    /* History that reaches the range's start gives no note, though the coverage starts days later. */
    [Fact]
    public void HistoryThatReachesTheRangeStart_ShowsNoNote()
    {
        Assert.Null(BannerFor(At(3), [At(0), At(3, 6)]));
    }

    /* A full page of the newest 2,000 runs whose oldest came 3 days into the range names that run, though the store covers the whole
       range (coverage 20 days before it); a page one row short is the whole range and shows none. */
    [Fact]
    public void AFullPage_NamesItsOldestRun_AndAPageOneRowShortDoesNot()
    {
        var oldest = RangeStart.AddDays(3);

        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(At(-20), Page(oldest, JobHistoryTab.RowCap)));
        Assert.Null(BannerFor(At(-20), Page(oldest, JobHistoryTab.RowCap - 1)));
        /* The note comes from the rows, so a probe with no answer does not hide it. */
        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(coverageStartUtc: null, Page(oldest, JobHistoryTab.RowCap)));
    }

    /* A full page names its oldest run with no slack: on a range of an hour, a page whose oldest run came 20 minutes in shows the
       note, and one whose oldest run is at the range's start shows none. The note names its time to the second. */
    [Fact]
    public void AFullPage_HasNoSlack_AndNamesItsTimeToTheSecond()
    {
        var rangeStart = new DateTime(2026, 9, 1, 10, 15, 0, DateTimeKind.Utc);

        Assert.Equal("Showing since 2026-09-01 10:35:42", BannerFor(coverageStartUtc: null, Page(rangeStart.AddMinutes(20).AddSeconds(42), JobHistoryTab.RowCap), rangeStart));
        Assert.Null(BannerFor(coverageStartUtc: null, Page(rangeStart, JobHistoryTab.RowCap), rangeStart));
    }

    /* A probe that throws shows no note, and the step does not throw: the tab binds its rows before it awaits the probe, so the
       grid keeps them. A run with no time cannot name a start. */
    [Fact]
    public void AProbeThatThrows_ShowsNoNote_AndTheStepDoesNotThrow()
    {
        var failed = Task.FromException<DateTime?>(new InvalidOperationException("the store is down"));

        Assert.Null(BannerFor(failed, [At(3, 6), At(5)]));
        Assert.Null(BannerFor(failed, [null, At(5)]));
        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(At(3), [null, At(4)]));
    }

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
