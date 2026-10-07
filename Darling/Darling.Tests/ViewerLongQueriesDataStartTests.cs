/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
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
/// The desktop viewer's Long Queries grid says where its data starts (#4966). The read windows on <c>collection_time</c> but
/// the grid shows <c>event_time</c>, and the first collection of the session stores the events its ring buffer still held, so
/// a row can carry an event time from before the coverage the probe found: the notice names the earlier of the coverage start
/// and the earliest event the grid shows. The read keeps the newest 200 rows, so a full page names its oldest row.
/// </summary>
/* The banner's time text reads the process-wide display mode, which these tests set (restored in Dispose); one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerLongQueriesDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    // ── The cap and the probe ──

    [Fact]
    public void TheCap_IsTheLimitOfTheRead()
    {
        Assert.Equal(200, ViewerDataService.LongQueriesRowCap);
        Assert.Contains($"LIMIT {ViewerDataService.LongQueriesRowCap}", ViewerDataService.LongQueryCompletionsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbe_GoesThroughTheSharedFloor_OverTheCollectorTable_OnCollectionTime()
    {
        var source = ViewerFile("ViewerDataService.LongQueries.cs");

        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"long_query_completions\")", source, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.GetForServerAsync(", source, StringComparison.Ordinal);
        /* The grid windows on collection_time, the probe's own column; the event time it shows is the caller's to compare. */
        Assert.Equal("collection_time", DataWindowFloor.Source.ForCollectorTable("long_query_completions").TimeColumn);
        Assert.Contains("collection_time >= $2", ViewerDataService.LongQueryCompletionsSql, StringComparison.Ordinal);
    }

    // ── The tab: the probe beside the read, the banner from the rows shown ──

    [Fact]
    public void LongQueries_AsksAboutTheWindowItDraws_AndNamesTheEventTimeOfEachRowItShows_UpToItsCap()
    {
        var load = MethodBody(ViewerFile("ViewerServerTab.LongQueries.cs"), @"private async Task LoadLongQueriesAsync\(");

        Assert.Equal(1, Matches(load, @"var dataStartTask = _dataService\.GetLongQueriesDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load,
            @"await ShowEventDataStartAsync\(LongQueriesTruncationBanner,\s*dataStartTask,\s*""Long Queries"",\s*startUtc,\s*rows\.Select\(r => r\.EventTime\),\s*ViewerDataService\.LongQueriesRowCap\);"));
    }

    /* A census over every server-tab file: each read of the grid's rows is paired with its probe, so a second read path added
       later without one fails here instead of shipping with a stale banner. */
    [Fact]
    public void EveryTabRead_OfTheGrid_HasItsProbe_AndNoneIsAwaitedBare()
    {
        var tabs = string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetRecentLongQueryCompletionsAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetLongQueriesDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"ShowEventDataStartAsync\(LongQueriesTruncationBanner,"));
        Assert.DoesNotContain("await dataStartTask", ViewerFile("ViewerServerTab.LongQueries.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheBanner_SitsAboveTheGrid_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""2"" x:Name=""LongQueriesTruncationBanner""[^>]*/>");
        Assert.True(banner.Success, "LongQueriesTruncationBanner is not declared in ViewerServerTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Matches(@"<DataGrid Grid\.Row=""3"" x:Name=""LongQueryCompletionsGrid""", xaml);
    }

    // ── What the banner shows ──

    /* The banner raised for the probe's answer and the rows the grid shows (their event times), read off the control; null when
       it is hidden. Seeded visible, so a no-op cannot pass as a hidden banner. */
    private static string? BannerFor(DateTime? coverageStartUtc, IEnumerable<DateTime?> eventTimes, int? rowCap = ViewerDataService.LongQueriesRowCap)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };
            var rows = eventTimes.Select(t => new ViewerLongQueryRow { EventTime = t }).ToList();

            ViewerServerTab.ShowEventDataStartAsync(
                banner, Task.FromResult(coverageStartUtc), "Long Queries", RangeStart, rows.Select(r => r.EventTime), rowCap).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    private static DateTime? At(int days, int hours = 0) => RangeStart.AddDays(days).AddHours(hours);

    /* Rows start inside the range: the first event came after the coverage began. The notice reads back the coverage start. */
    [Fact]
    public void RowsThatStartInsideTheRange_RaiseTheNotice_AtTheCoverageStart()
    {
        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(At(3), [At(3, 6), At(5)]));
    }

    /* A quiet start: the store covered the whole range, and the first event came 5 hours in. No notice. */
    [Fact]
    public void AQuietStart_RaisesNoNotice()
    {
        Assert.Null(BannerFor(At(-20), [At(0, 5), At(2)]));
    }

    /* The first collection stored events from before the coverage: the notice names the grid's earliest event, not the later coverage. */
    [Fact]
    public void HistoryThatReachesBeforeTheCoverage_RaisesTheNotice_AtTheHistorysStart()
    {
        Assert.Equal("Showing since 2026-09-02 00:00:00", BannerFor(At(3), [At(1), At(3, 6)]));
    }

    /* History that reaches the range start gives no notice, though the coverage starts days later. */
    [Fact]
    public void HistoryThatReachesTheRangeStart_RaisesNoNotice()
    {
        Assert.Null(BannerFor(At(3), [At(0), At(3, 6)]));
    }

    /* A row with no event time cannot name a start, so it is ignored. */
    [Fact]
    public void ARowWithNoEventTime_IsIgnored()
    {
        Assert.Equal("Showing since 2026-09-03 00:00:00", BannerFor(At(3), [null, At(2)]));
    }

    /* A full page of the newest 200 rows whose oldest event came 3 days into the range names that event, though the store covers
       the whole range (coverage 20 days before it); one row fewer is the whole range and raises none. */
    [Fact]
    public void AFullPage_RaisesTheNotice_AtItsOldestRow_AndOneRowFewerRaisesNone()
    {
        var oldest = RangeStart.AddDays(3);
        IEnumerable<DateTime?> Page(int count) => Enumerable.Range(0, count).Select(i => (DateTime?)oldest.AddMinutes(i));

        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(At(-20), Page(ViewerDataService.LongQueriesRowCap)));
        Assert.Null(BannerFor(At(-20), Page(ViewerDataService.LongQueriesRowCap - 1)));
        /* The notice comes from the rows, so a probe with no answer does not hide it. */
        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(null, Page(ViewerDataService.LongQueriesRowCap)));
    }

    /* WPF objects require STA; same shape as ViewerEventDataStartTests. */
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
