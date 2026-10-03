/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The System Events grids and the Default Trace grid say where their data starts (#4966): one banner above the sub-tab
/// control covers the nine, the eight system_health grids on one probe and Default Trace on its own. The notice names the
/// earlier of the collector's coverage start and the earliest event a grid shows, and the Default Trace events, stored on
/// the server's local clock, are compared in UTC after the read's own per-row conversion.
/// </summary>
public sealed class ViewerSystemEventsDataStartTests
{
    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string TabSources() =>
        string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

    // ── Every read path asks the probe ──

    [Theory]
    [InlineData("LoadSchedulerIssuesAsync")]
    [InlineData("LoadSevereErrorsAsync")]
    [InlineData("LoadMemoryConditionsAsync")]
    [InlineData("LoadMemoryBrokerAsync")]
    [InlineData("LoadMemoryNodeOomAsync")]
    [InlineData("LoadSignificantWaitsAsync")]
    [InlineData("LoadCpuTasksAsync")]
    [InlineData("LoadIoIssuesAsync")]
    public void EachSystemHealthGrid_AsksAboutTheWindowItDraws_AndRaisesTheOneBanner(string load)
    {
        var body = MethodBody(ViewerFile("ViewerServerTab.SystemEvents.cs"), @"private async Task " + load + @"\(");

        Assert.Equal(1, Matches(body, @"var dataStartTask = _dataService\.GetSystemHealthEventsDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(body,
            @"await ShowEventDataStartAsync\(SystemEventsTruncationBanner,\s*dataStartTask,\s*""System Events"",\s*startUtc,\s*data\.Select\(r => r\.EventTime\)\);"));
    }

    [Fact]
    public void DefaultTrace_AsksAboutTheWindowItDraws_AndRaisesTheOneBanner_OverTheEventsInUtc()
    {
        var body = MethodBody(ViewerFile("ViewerServerTab.SystemEvents.cs"), @"private async Task LoadDefaultTraceEventsAsync\(");

        Assert.Equal(1, Matches(body, @"var dataStartTask = _dataService\.GetDefaultTraceDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(body,
            @"await ShowEventDataStartAsync\(SystemEventsTruncationBanner,\s*dataStartTask,\s*""Default Trace"",\s*startUtc,\s*data\.Select\(r => r\.EventTimeUtc\)\);"));
    }

    /* A census over every server-tab file: the nine reads are paired with the nine banner calls, so a tenth read added without
       one fails here instead of shipping with a stale banner. */
    [Fact]
    public void EveryTabRead_OfTheNineGrids_HasItsProbe_AndNoneIsAwaitedBare()
    {
        var tabs = TabSources();

        Assert.Equal(8, Matches(tabs, @"_dataService\.GetSystemHealthEventsDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetDefaultTraceDataStartAsync\("));
        Assert.Equal(9, Matches(tabs, @"ShowEventDataStartAsync\(SystemEventsTruncationBanner,"));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetDefaultTraceEventsAsync\("));
        Assert.DoesNotContain("await dataStartTask", ViewerFile("ViewerServerTab.SystemEvents.cs"), StringComparison.Ordinal);
    }

    /* The Corruption and Contention sub-tabs are charts on a time axis that already shows the empty span, so they take the
       banner down instead of leaving the last grid's notice over them. */
    [Fact]
    public void TheChartSubTabs_TakeTheBannerDown()
    {
        var body = MethodBody(ViewerFile("ViewerServerTab.SystemEvents.cs"), @"private async Task LoadSystemEventsAsync\(");

        Assert.Equal(1, Matches(body,
            @"case SystemEventsContentionSubTabIndex:\s*default:\s*SystemEventsTruncationBanner\.Visibility = Visibility\.Collapsed;\s*await LoadSystemHealthChartsAsync\(\);"));
    }

    [Fact]
    public void TheBanner_SitsAboveTheSubTabControl_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock[^>]*x:Name=""SystemEventsTruncationBanner""[^>]*/>");
        Assert.True(banner.Success, "SystemEventsTruncationBanner is not declared in ViewerServerTab.xaml");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.True(banner.Index < xaml.IndexOf("x:Name=\"SystemEventsSubTabControl\"", StringComparison.Ordinal), "the banner must sit above the sub-tab control");
    }

    // ── The probes ──

    [Fact]
    public void TheProbes_GoThroughTheSharedFloor_OverTheCollectorTablesTheGridsReadFrom_OnTheCollectorsUtcClock()
    {
        var source = ViewerFile("ViewerDataService.SystemEvents.cs");
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"system_health_events\")", source, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"default_trace_events\")", source, StringComparison.Ordinal);

        /* The probe reads collection_time (UTC), never the Default Trace's server-local event_time, so the coverage needs no clock. */
        Assert.Equal("collection_time", DataWindowFloor.Source.ForCollectorTable("system_health_events").TimeColumn);
        Assert.Equal("collection_time", DataWindowFloor.Source.ForCollectorTable("default_trace_events").TimeColumn);
    }

    /* Neither read keeps a newest-first page, so no row cap applies: a LIMIT added later has to be wired to the cap rule. */
    [Fact]
    public void TheReads_CarryNoRowCap()
    {
        Assert.DoesNotMatch(@"LIMIT\s+\d{2,}", ViewerDataService.SystemHealthEventsByTypeSql);
        Assert.DoesNotMatch(@"LIMIT\s+\d{2,}", ViewerDataService.DefaultTraceEventsByWindowSql);
    }

    // ── Default Trace: the server's clock, compared in UTC ──

    private const string EasternWindowsId = "Eastern Standard Time";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    /// <summary>The notice the tab names for Default Trace events stored at <paramref name="storedLocal"/> on the server's own clock.</summary>
    private static async Task<DateTime?> NoticeAsync(ServerClock clock, DateTime startUtc, DateTime endUtc, DateTime coverageStartUtc, params DateTime[] storedLocal)
    {
        var table = new DataTable();
        table.Columns.Add("event_time_local", typeof(DateTime));
        table.Columns.Add("event_name", typeof(string));
        foreach (var name in new[] { "database_name", "object_name", "login_name", "host_name", "application_name" })
        {
            table.Columns.Add(name, typeof(string));
        }

        table.Columns.Add("spid", typeof(int));
        table.Columns.Add("duration_us", typeof(long));
        table.Columns.Add("integer_data", typeof(long));
        table.Columns.Add("severity", typeof(int));
        table.Columns.Add("error_number", typeof(int));
        table.Columns.Add("text_data", typeof(string));
        foreach (var local in storedLocal)
        {
            table.Rows.Add(local, "Data File Auto Grow", "db", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, 55, 1_500_000L, 256L, DBNull.Value, DBNull.Value, DBNull.Value);
        }

        using var reader = table.CreateDataReader();
        var rows = await ViewerDataService.ReadDefaultTraceEventsAsync(reader, clock, startUtc, endUtc, TestContext.Current.CancellationToken);
        Assert.NotEmpty(rows);
        return ViewerEventDataStart.Of(coverageStartUtc, ViewerEventDataStart.EarliestOf(rows.Select(r => r.EventTimeUtc)));
    }

    /* A clock 5 hours behind UTC: the server was added on Sep 5 (coverage), and its first collection stored trace history from
       Sep 2 at 00:00 on the server's own clock. The notice is that moment in UTC, 05:00, not the stored 00:00. */
    [Fact]
    public async Task WithAClock5HoursBehindUtc_HistoryBeforeTheCoverage_GivesANoticeAtItsStartInUtc()
    {
        var notice = await NoticeAsync(ServerClock.Resolve(null, -300), RangeStart, RangeStart.AddDays(9), Naive(2026, 9, 5, 0),
            Naive(2026, 9, 2, 0), Naive(2026, 9, 6, 10));

        Assert.Equal(Naive(2026, 9, 2, 5), notice);
        Assert.True(RawWindowFloor.IsTruncated(notice, RangeStart));
    }

    /* History that reaches the range start, on the same clock: the oldest stored event is Aug 31 at 20:00 local, 01:00 UTC on Sep 1.
       No notice, though the coverage starts days later. */
    [Fact]
    public async Task WithAClock5HoursBehindUtc_HistoryThatReachesTheRangeStart_GivesNoNotice()
    {
        var notice = await NoticeAsync(ServerClock.Resolve(null, -300), RangeStart, RangeStart.AddDays(9), Naive(2026, 9, 5, 0),
            Naive(2026, 8, 31, 20), Naive(2026, 9, 6, 10));

        Assert.Equal(Naive(2026, 9, 1, 1), notice);
        Assert.False(RawWindowFloor.IsTruncated(notice, RangeStart));
    }

    /* A quiet start: the store covered the range from before it, and the first event came 5 hours in (stored 00:00 local on Sep 1,
       which is 05:00 UTC). No notice. */
    [Fact]
    public async Task WithAClock5HoursBehindUtc_AQuietStart_GivesNoNotice()
    {
        var notice = await NoticeAsync(ServerClock.Resolve(null, -300), RangeStart, RangeStart.AddDays(9), RangeStart.AddDays(-20),
            Naive(2026, 9, 1, 0), Naive(2026, 9, 6, 10));

        Assert.False(RawWindowFloor.IsTruncated(notice, RangeStart));
    }

    /* A day that crosses the spring change (Mar 8, 2026, 02:00 local): the event stored at 01:30 is standard time, 06:30 UTC, and the
       one at 03:30 is daylight time. The snapshot's offset says -240; a single subtracted offset would put the first event at 05:30. */
    [Fact]
    public async Task ADayThatCrossesTheSpringChange_UsesThatDatesOffset()
    {
        var notice = await NoticeAsync(ServerClock.Resolve(EasternWindowsId, -240), Naive(2026, 3, 1, 0), Naive(2026, 3, 20, 0), Naive(2026, 3, 9, 0),
            Naive(2026, 3, 8, 1, 30), Naive(2026, 3, 8, 3, 30));

        Assert.Equal(Naive(2026, 3, 8, 6, 30), notice);
    }

    /* The fall change (Nov 1, 2026, 02:00 local): the event at 00:30 is daylight time, 04:30 UTC. The snapshot says -300. */
    [Fact]
    public async Task ADayThatCrossesTheFallChange_UsesThatDatesOffset()
    {
        var notice = await NoticeAsync(ServerClock.Resolve(EasternWindowsId, -300), Naive(2026, 10, 25, 0), Naive(2026, 11, 10, 0), Naive(2026, 11, 2, 0),
            Naive(2026, 11, 1, 0, 30), Naive(2026, 11, 1, 3, 30));

        Assert.Equal(Naive(2026, 11, 1, 4, 30), notice);
    }
}
