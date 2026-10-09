/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

/* Darling.Tests only: a Darling file Lite.Tests compiles must be named in build.yml's Lite path filter. */
namespace Darling.Tests;

/// <summary>
/// #5562, the four gaps the first Viewer pass left. R5: a FinOps picker offers only a range its "hours back from now" read can
/// honor. R6: Job History's finished range is bounded in the SQL, before the row cap. R7: every "Showing since" banner site feeds
/// the picker's data-start note. R8: Alert History's longest choice is the span its table keeps.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerTimeRangeGapFixTests
{
    private static string ViewerFile(string file) => RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static string Code(string file) => ViewerTypedRangeTests.StripComments(ViewerFile(file));

    private static TimeRangeSpec Spec(string id)
    {
        Assert.True(TimeRangeSpec.TryFromId(id, out var spec), id);
        return spec!;
    }

    // ── R5: FinOps pickers are rolling-only, in whole hours (the heatmap in whole days) ──

    [Theory]
    [InlineData("1h", true)]
    [InlineData("4h", true)]
    [InlineData("1d", true)]
    [InlineData("1w", true)]
    [InlineData("1mo", true)]
    [InlineData("90m", false)]
    [InlineData("30m", false)]
    [InlineData("5m", false)]
    [InlineData("today", false)]
    [InlineData("previous-week", false)]
    [InlineData("since:2026-10-01T04:00:00Z", false)]
    [InlineData("fixed:2026-10-01T04:00:00Z/2026-10-02T04:00:00Z", false)]
    public void TheHourRule_AdmitsOnlyAWholeNumberOfHoursBackFromNow(string id, bool admitted)
    {
        var spec = id == "90m" ? TimeRangeSpec.Relative(TimeSpan.FromMinutes(90)) : Spec(id);
        Assert.Equal(admitted, RollingUnitRule.Admits(spec, RollingUnitRule.Hour));
        Assert.Equal(admitted, RollingUnitRule.Refusal(spec, RollingUnitRule.Hour) is null);
    }

    [Theory]
    [InlineData("1d", true)]
    [InlineData("2d", true)]
    [InlineData("1mo", true)]
    [InlineData("1h", false)]
    [InlineData("4h", false)]
    public void TheDayRule_AdmitsOnlyAWholeNumberOfDaysBackFromNow(string id, bool admitted)
    {
        var spec = Spec(id);
        Assert.Equal(admitted, RollingUnitRule.Admits(spec, RollingUnitRule.Day));
        Assert.False(RollingUnitRule.Admits(TimeRangeSpec.Relative(TimeSpan.FromHours(36)), RollingUnitRule.Day));
        Assert.False(RollingUnitRule.Admits(TimeRangeSpec.ForPeriod(CalendarPeriod.MonthToDate), RollingUnitRule.Day));
    }

    [Fact]
    public void TheRefusal_SaysWhatTheReadTakes()
    {
        Assert.Equal("The shortest range for this read is 1 hour.", RollingUnitRule.Refusal(TimeRangeSpec.Relative(TimeSpan.FromMinutes(30)), RollingUnitRule.Hour));
        Assert.Equal("This read takes whole hours; try 2h.", RollingUnitRule.Refusal(TimeRangeSpec.Relative(TimeSpan.FromMinutes(90)), RollingUnitRule.Hour));
        Assert.Contains("length back from now", RollingUnitRule.Refusal(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), RollingUnitRule.Hour), StringComparison.Ordinal);
        Assert.Equal("The shortest range for this read is 1 day.", RollingUnitRule.Refusal(TimeRangeSpec.Relative(TimeSpan.FromHours(4)), RollingUnitRule.Day));
    }

    [Fact]
    public void ARollingOnlyPicker_RefusesWhatItsReadCannotHonor_AndHidesTheCalendar()
    {
        StaTestThread.Run(() =>
        {
            var now = new DateTime(2026, 10, 8, 11, 1, 0, DateTimeKind.Unspecified);
            var picker = new TimeRangePicker { ZoneProvider = () => TimeZoneInfo.Utc, NowProvider = () => now, Compact = true };
            picker.RollingUnit = RollingUnitRule.Hour;
            var raised = 0;
            picker.RangeChanged += (_, _) => raised++;

            Assert.Equal(System.Windows.Visibility.Collapsed, picker.PeriodPanel.Visibility);
            Assert.Equal(System.Windows.Visibility.Collapsed, picker.PickCalendar.Visibility);
            Assert.Equal(System.Windows.Visibility.Collapsed, picker.CalendarHeader.Visibility);

            /* Presets under an hour are greyed; the others stay. */
            var enabled = picker.RollingPanel.Children.OfType<Button>().Where(b => b.IsEnabled).Select(b => ((TimeRangeSpec)b.Tag).Id).ToArray();
            Assert.Equal(new[] { "1h", "4h", "1d", "2d", "1w", "1mo" }, enabled);

            Assert.False(picker.Select(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek)));
            Assert.False(picker.Select(TimeRangeSpec.FixedRange(now.AddDays(-2), now.AddDays(-1))));
            Assert.False(picker.Select(TimeRangeSpec.SinceInstant(now.AddDays(-2))));
            Assert.False(picker.Select(TimeRangeSpec.Relative(TimeSpan.FromMinutes(45))));
            Assert.Equal(0, raised);
            Assert.Equal("4h", picker.Value.Id);

            Assert.True(picker.Select(TimeRangeSpec.Relative(TimeSpan.FromHours(6))));
            Assert.Equal(1, raised);

            /* Typed text goes through the same rule: a calendar period and a part hour are refused with the reason. */
            picker.InputBox.Text = "last month";
            Assert.False(picker.ApplyButton.IsEnabled);
            Assert.Contains("length back from now", picker.EchoText.Text, StringComparison.Ordinal);
            picker.InputBox.Text = "90m";
            Assert.False(picker.ApplyButton.IsEnabled);
            Assert.Equal("This read takes whole hours; try 2h.", picker.EchoText.Text);
            picker.InputBox.Text = "3 days";
            Assert.True(picker.ApplyButton.IsEnabled);

            /* A plain picker is unchanged. */
            picker.RollingUnit = null;
            Assert.Equal(System.Windows.Visibility.Visible, picker.PeriodPanel.Visibility);
            Assert.True(picker.Select(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek)));
        });
    }

    [Fact]
    public void EveryFinOpsPicker_IsCompactAndRollingOnly_TheListsInHoursAndTheHeatmapInDays()
    {
        var xaml = ViewerFile("FinOpsTab.xaml");
        var pickers = Regex.Matches(xaml, @"<ui:TimeRangePicker x:Name=""(?<name>\w+)""[^>]*>");
        Assert.Equal(5, pickers.Count);
        foreach (Match picker in pickers)
        {
            Assert.Contains("Compact=\"True\"", picker.Value, StringComparison.Ordinal);
        }

        var code = Code("FinOpsTab.xaml.cs");
        /* The four lists share one loop: one assignment, in whole hours; the heatmap sets its own, in whole days. */
        Assert.Single(Regex.Matches(code, @"picker\.RollingUnit = RollingUnitRule\.Hour;"));
        Assert.Single(Regex.Matches(code, @"FinOpsObjectHeatmapWindowCombo\.RollingUnit = RollingUnitRule\.Day;"));
        foreach (var name in new[] { "FinOpsResourceUsageTimeRangeCombo", "FinOpsWaitStatsTimeRangeCombo", "FinOpsExpensiveQueriesTimeRangeCombo", "FinOpsHighImpactTimeRangeCombo" })
        {
            Assert.Contains(name, code, StringComparison.Ordinal);
        }

        /* Nothing else sets the five pickers' Value to a range the rule refuses. */
        foreach (Match assignment in Regex.Matches(code, @"\.Value = TimeRangePresets\.Find\(""(?<id>\w+)""\)"))
        {
            Assert.True(RollingUnitRule.Admits(TimeRangePresets.Find(assignment.Groups["id"].Value)!, RollingUnitRule.Hour), assignment.Value);
        }
    }

    // ── R7: every banner site feeds the picker's data-start note ──

    [Fact]
    public void TheBannerSites_AreCountedAndEveryOneFeedsThePickersNote()
    {
        var updateSites = 0;
        var eventSites = 0;
        var viewerDir = RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Viewer");
        foreach (var file in Directory.EnumerateFiles(viewerDir, "*.cs"))
        {
            var code = ViewerTypedRangeTests.StripComments(File.ReadAllText(file));
            updateSites += Regex.Matches(code, @"\bUpdateTruncationBanner\(").Count - Regex.Matches(code, @"void UpdateTruncationBanner\(").Count;
            eventSites += Regex.Matches(code, @"\bShowEventDataStartAsync\(").Count - Regex.Matches(code, @"Task ShowEventDataStartAsync\(").Count;
        }

        /* A new banner site changes these counts: it must go through UpdateTruncationBanner (the event surfaces through
           ShowEventDataStartAsync, which ends in it), which feeds the note below. Raise the count with the site. */
        Assert.Equal(35, updateSites);
        Assert.Equal(29, eventSites);

        var update = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(ViewerFile("ViewerServerTab.Queries.cs"), "UpdateTruncationBanner"));
        Assert.Matches(@"\{\s*ViewerDataStartNote\.Feed\(banner, floor\);", update);

        var show = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(ViewerFile("ViewerServerTab.EventDataStart.cs"), "ShowEventDataStartAsync"));
        Assert.Single(Regex.Matches(show, @"\bUpdateTruncationBanner\("));

        /* The feed reaches the two tabs that own a picker and a banner, and sets the note from the floor with no query. */
        var note = ViewerTypedRangeTests.StripComments(ViewerFile("ViewerDataStartNote.cs"));
        Assert.Contains("serverTab.RecordDataStart(floor)", note, StringComparison.Ordinal);
        Assert.Contains("jobHistory.RecordDataStart(floor)", note, StringComparison.Ordinal);
        Assert.DoesNotContain("await", note, StringComparison.Ordinal);
        Assert.Contains("TimeRangePickerControl.DataStartUtc = floor", Code("JobHistoryTab.xaml.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheFeed_ReachesOnlyABannerThatIsOnScreenInATab()
    {
        StaTestThread.Run(() =>
        {
            var banner = new TextBlock();
            var host = new Border();
            var outer = new StackPanel();
            Assert.Null(ViewerDataStartNote.FindAncestor<StackPanel>(banner));

            host.Child = banner;
            outer.Children.Add(host);
            Assert.Same(host, ViewerDataStartNote.FindAncestor<Border>(banner));
            Assert.Same(outer, ViewerDataStartNote.FindAncestor<StackPanel>(banner));

            /* A bare banner (a test's control, or a tab that is not on screen) feeds nothing and throws nothing. */
            ViewerDataStartNote.Feed(new TextBlock(), new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Unspecified));
            ViewerDataStartNote.Feed(banner, null);
        });
    }

    // ── R8: Alert History's longest choice ──

    [Fact]
    public void AlertHistory_LongestChoice_IsTheSpanTheAlertTableKeeps()
    {
        /* The retention is DarlingRetentionHorizons.AlertHistoryRetentionDays (config_alert_log is purged past it, 90 days). The
           choice is that span, capped at a year if the retention is ever raised. */
        var days = Math.Min(DarlingRetentionHorizons.AlertHistoryRetentionDays, 365);
        Assert.Equal(TimeSpan.FromDays(days), ViewerTimeRangeWindow.AlertHistoryLongestChoice.Span);
        Assert.Equal(TimeRangeKind.Relative, ViewerTimeRangeWindow.AlertHistoryLongestChoice.Kind);
        Assert.True(DarlingRetentionHorizons.AlertHistoryRetentionDays <= 365, "retention above a year: re-read #5562 R8");
        Assert.Contains("SetLongestChoice(ViewerTimeRangeWindow.AlertHistoryLongestChoice", Code("AlertsHistoryTab.xaml.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("365d\")", Code("AlertsHistoryTab.xaml.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void ThePickersLongestChoice_IsOneMoreButton_ThatSelectsItsSpan()
    {
        StaTestThread.Run(() =>
        {
            var now = new DateTime(2026, 10, 8, 11, 1, 0, DateTimeKind.Unspecified);
            var picker = new TimeRangePicker { ZoneProvider = () => TimeZoneInfo.Utc, NowProvider = () => now, Compact = true };
            Assert.Equal(9, picker.RollingPanel.Children.Count);

            picker.SetLongestChoice(ViewerTimeRangeWindow.AlertHistoryLongestChoice, "All");
            Assert.Equal(10, picker.RollingPanel.Children.Count);
            var all = (Button)picker.RollingPanel.Children[9];
            Assert.Equal("All", all.Content);
            Assert.Equal("90d", ((TimeRangeSpec)all.Tag).Id);

            TimeRangeChangedEventArgs? last = null;
            picker.RangeChanged += (_, e) => last = e;
            Assert.True(picker.Select((TimeRangeSpec)all.Tag));
            Assert.Equal(TimeSpan.FromDays(90), last!.Range.Span);

            picker.SetLongestChoice(null);
            Assert.Equal(9, picker.RollingPanel.Children.Count);
        });
    }

    // ── R6: Job History's end bound, applied before the row cap ──

    private const int EndBoundServer = -556_201;
    private const long EndBoundIdBase = 5_562_000_000L;

    [Fact]
    public async Task AFinishedRange_KeepsEveryRunInsideIt_WhenMoreRunsAfterItsEndThanTheCap()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #5562 job-history end-bound live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, EndBoundServer, "darling-5562-endbound", ct);

            /* The range: two days ago to one day ago. The server has no server_properties row, so its clock is UTC. */
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddDays(-1);
            var start = end.AddDays(-1);
            var id = 0L;
            var inside = new System.Collections.Generic.List<(string Job, DateTime At)>();

            for (var i = 1; i <= 6; i++)
            {
                var at = start.AddHours(i * 3);
                await InsertAsync(connection, ct, ++id, at, "inside_" + i);
                inside.Add(("inside_" + i, at));
            }

            /* The newest run before the end, with microseconds the store keeps, and the start itself. */
            var lastInside = end.AddTicks(-10);
            await InsertAsync(connection, ct, ++id, lastInside, "inside_last_microsecond");
            inside.Add(("inside_last_microsecond", lastInside));
            await InsertAsync(connection, ct, ++id, start, "inside_at_start");
            inside.Add(("inside_at_start", start));

            /* Outside: a run exactly at the end (the end is exclusive), one before the start, and 40 after the end, more than
               the cap of 10 used below. */
            await InsertAsync(connection, ct, ++id, end, "outside_at_end");
            await InsertAsync(connection, ct, ++id, start.AddMinutes(-1), "outside_before_start");
            for (var i = 1; i <= 40; i++)
            {
                await InsertAsync(connection, ct, ++id, end.AddMinutes(i * 10), "after_" + i);
            }

            const int cap = 10;

            /* The start-only read (the old shape) fills its cap from the runs after the end: nothing of the range survives a client trim. */
            var startOnly = await viewer.GetJobHistoryAsync(start, EndBoundServer, cap, cancellationToken: ct);
            Assert.Equal(cap, startOnly.Count);
            Assert.DoesNotContain(startOnly, r => r.RunDateTimeUtc is { } ran && ran < end);

            /* With the end bound the SQL applies it before the cap: every run inside the range comes back, newest first. */
            var bounded = await viewer.GetJobHistoryAsync(start, EndBoundServer, cap, untilUtc: end, cancellationToken: ct);
            Assert.Equal(inside.Count, bounded.Count);
            Assert.All(bounded, r => Assert.True(r.RunDateTimeUtc is { } ran && ran >= start && ran < end, r.JobName));
            Assert.Equal(inside.OrderByDescending(r => r.At).Select(r => r.Job).ToArray(), bounded.Select(r => r.JobName).ToArray());
            Assert.Equal(lastInside, bounded[0].RunDateTimeUtc);
            Assert.DoesNotContain(bounded, r => r.JobName is "outside_at_end" or "outside_before_start");

            /* A cap smaller than the range still cuts the newest of the range, not of everything since the start. */
            var capped = await viewer.GetJobHistoryAsync(start, EndBoundServer, 3, untilUtc: end, cancellationToken: ct);
            Assert.Equal(new[] { "inside_last_microsecond", "inside_6", "inside_5" }, capped.Select(r => r.JobName).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, long id, DateTime at, string job) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id,
                                       job_id, job_name, job_enabled, step_id, step_name, run_status,
                                       run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
              VALUES ($1,$2,$3,$4,$5,$6,$7,true,0,'(Job outcome)',1,'The job succeeded.',$8,5,0,NULL)",
            EndBoundIdBase + id, at, EndBoundServer, "srv" + EndBoundServer.ToString(CultureInfo.InvariantCulture),
            EndBoundIdBase + id, job, job, at);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var jh = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = -556201", connection);
        await jh.ExecuteNonQueryAsync(ct);
        await using var svr = new NpgsqlCommand("DELETE FROM servers WHERE server_id = -556201", connection);
        await svr.ExecuteNonQueryAsync(ct);
    }
}
