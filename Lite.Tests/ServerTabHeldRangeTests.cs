/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5562: the Server tab holds its range on the shared picker, and the window a refresh reads comes from
/// <see cref="LiteTimeRange.WindowFor"/>: a rolling range of whole hours is still (hours, null, null), exactly as the
/// old presets were, and everything else (a sub-hour span, a calendar period, a typed range) carries the two instants
/// it resolves to, so the ~60 consumers of <c>GetCurrentWindowUtc</c> / <c>GetHoursBack</c> / <c>GetChartWindow</c>
/// need no change. The tab is a WPF control this suite does not instantiate (the shared picker has its own STA tests in
/// Darling.Tests): the mapping, the settings split and the zone are pure and tested here directly, and the wiring
/// around them is a source pin.
///
/// <para>#4766 still holds: a fixed range holds its instants itself, so the zone only changes the text. A range from
/// 06:30 UTC on 1 November is the second 01:30 on a US Eastern server, and it reads 06:30 UTC in every display mode.</para>
/// </summary>
public sealed class ServerTabHeldRangeTests
{
    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    private static DateTime At(int month, int day, int hour, int minute) =>
        new(2026, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static ResolvedTimeRange Resolve(TimeRangeSpec spec, DateTime nowUtc, TimeZoneInfo zone)
    {
        Assert.True(spec.TryResolve(nowUtc, zone, out var range, out var error), error?.Message);
        return range!;
    }

    // ── The window a refresh reads ──

    [Theory]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.ServerTime)]
    public void AFixedRange_IsTheWindow_WhateverZoneThePickerIsDrawnIn(TimeDisplayMode mode)
    {
        var zone = ServerTab.PickerZone(mode, Eastern());
        var spec = TimeRangeSpec.FixedRange(At(11, 1, 6, 30), At(11, 1, 7, 30));

        var (hoursBack, fromUtc, toUtc) = LiteTimeRange.WindowFor(Resolve(spec, At(11, 5, 12, 0), zone));

        Assert.Equal(At(11, 1, 6, 30), fromUtc);
        Assert.Equal(At(11, 1, 7, 30), toUtc);
        /* Hours from the start to now, rounded up: 4 days and 5.5 hours. */
        Assert.Equal(102, hoursBack);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(12)]
    [InlineData(24)]
    [InlineData(168)]
    public void ARollingRangeOfWholeHours_HasNoBounds_LikeTheOldPresets(int hours)
    {
        var spec = TimeRangePresets.FromLegacyHours(hours)!;

        Assert.Equal((hours, (DateTime?)null, (DateTime?)null), LiteTimeRange.WindowFor(Resolve(spec, At(10, 8, 12, 0), TimeZoneInfo.Utc)));
    }

    [Theory]
    [InlineData(5, 1)]
    [InlineData(15, 1)]
    [InlineData(30, 1)]
    [InlineData(90, 2)]
    public void ASubHourOrFractionalSpan_CarriesItsInstants_AndHoursBackRoundsUp(int minutes, int expectedHoursBack)
    {
        var now = At(10, 8, 12, 0);
        var spec = TimeRangeSpec.Relative(TimeSpan.FromMinutes(minutes));

        var (hoursBack, fromUtc, toUtc) = LiteTimeRange.WindowFor(Resolve(spec, now, TimeZoneInfo.Utc));

        Assert.Equal(expectedHoursBack, hoursBack);
        Assert.Equal(now.AddMinutes(-minutes), fromUtc);
        Assert.Equal(now, toUtc);
    }

    [Fact]
    public void ACalendarPeriod_FlowsThroughAsTheDisplayZonesWallClockDay()
    {
        /* 'Yesterday' on an Eastern server read at 08 Oct 2026 12:00 UTC (08:00 there) is 7 Oct 00:00 to 8 Oct 00:00 Eastern. */
        var zone = ServerTab.PickerZone(TimeDisplayMode.ServerTime, Eastern());
        var now = At(10, 8, 12, 0);
        var spec = TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday);
        var range = Resolve(spec, now, zone);

        var (hoursBack, fromUtc, toUtc) = LiteTimeRange.WindowFor(range);

        Assert.Equal(At(10, 7, 4, 0), fromUtc);
        Assert.Equal(At(10, 8, 4, 0), toUtc);
        Assert.Equal(32, hoursBack);

        /* The chart axis for it is that pair, not 'now - hoursBack'. */
        Assert.Equal((At(10, 7, 4, 0), At(10, 8, 4, 0)), ServerTab.GetChartWindow(hoursBack, fromUtc, toUtc, now));
        Assert.True(LiteTimeRange.HasExplicitInstants(range));
    }

    [Fact]
    public void ARollingWholeHourRange_IsNotExplicit_ASlidingOneIs()
    {
        var now = At(10, 8, 12, 0);
        Assert.False(LiteTimeRange.HasExplicitInstants(Resolve(TimeRangeSpec.Relative(TimeSpan.FromHours(4)), now, TimeZoneInfo.Utc)));
        Assert.True(LiteTimeRange.HasExplicitInstants(Resolve(TimeRangeSpec.Relative(TimeSpan.FromMinutes(30)), now, TimeZoneInfo.Utc)));
        Assert.True(LiteTimeRange.HasExplicitInstants(Resolve(TimeRangeSpec.SinceInstant(At(10, 7, 0, 0)), now, TimeZoneInfo.Utc)));
    }

    [Fact]
    public void ThePickerZone_IsUtc_ThisMachinesZone_OrTheTabsOwnServerClock()
    {
        var clock = Eastern();

        Assert.Equal(TimeZoneInfo.Utc, ServerTab.PickerZone(TimeDisplayMode.UTC, clock));
        Assert.Equal(TimeZoneInfo.Local, ServerTab.PickerZone(TimeDisplayMode.LocalTime, clock));

        /* The zone of the clock the tab was handed, so a tab that is not the selected one keeps its own server's zone. */
        var server = ServerTab.PickerZone(TimeDisplayMode.ServerTime, clock);
        Assert.Equal(TimeSpan.FromHours(-4), server.GetUtcOffset(DateTime.SpecifyKind(At(11, 1, 5, 30), DateTimeKind.Utc)));
        Assert.Equal(TimeSpan.FromHours(-5), server.GetUtcOffset(DateTime.SpecifyKind(At(11, 1, 6, 30), DateTimeKind.Utc)));
    }

    [Fact]
    public void ADrillWindowUnderTheFiveMinuteFloor_IsCentredAndHeldAtFiveMinutes()
    {
        var (from, to) = LiteTimeRange.AtLeastMinimumSpan(At(10, 8, 12, 0), At(10, 8, 12, 2));
        Assert.Equal((At(10, 8, 11, 58).AddSeconds(30), At(10, 8, 12, 3).AddSeconds(30)), (from, to));

        var longer = (At(10, 8, 12, 0), At(10, 8, 12, 30));
        Assert.Equal(longer, LiteTimeRange.AtLeastMinimumSpan(longer.Item1, longer.Item2));
    }

    // ── Settings: the legacy hours key keeps working, default_time_range is optional ──

    [Theory]
    [InlineData(1, "1h")]
    [InlineData(4, "4h")]
    [InlineData(12, "12h")]
    [InlineData(24, "1d")]
    [InlineData(168, "1w")]
    [InlineData(48, "2d")]
    public void TheLegacyHoursKey_MapsThroughFromLegacyHours(int hours, string expectedId)
    {
        Assert.Equal(expectedId, LiteTimeRange.FromSettings(null, hours).Id);
        Assert.Equal(expectedId, LiteTimeRange.FromSettings("", hours).Id);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-3)]
    public void ANonsenseLegacyValue_OpensOnFourHours(int hours)
    {
        Assert.Equal("4h", LiteTimeRange.FromSettings(null, hours).Id);
    }

    [Fact]
    public void TheNewKey_WinsForAPresetOrAPeriod_AndANonPersistableOneFallsBackToTheHours()
    {
        Assert.Equal("30m", LiteTimeRange.FromSettings("30m", 24).Id);
        Assert.Equal("previous-week", LiteTimeRange.FromSettings("previous-week", 24).Id);
        Assert.Equal("1mo", LiteTimeRange.FromSettings("1mo", 4).Id);

        /* A typed range is never taken from a file, and neither is text that is not a range. */
        Assert.Equal("12h", LiteTimeRange.FromSettings("fixed:2026-10-01T04:00:00Z/2026-10-02T04:00:00Z", 12).Id);
        Assert.Equal("12h", LiteTimeRange.FromSettings("since:2026-10-01T04:00:00Z", 12).Id);
        Assert.Equal("12h", LiteTimeRange.FromSettings("nonsense", 12).Id);
    }

    [Fact]
    public void WhatIsWritten_WholeHoursGoToTheLegacyKey_OtherPresetsToTheNewOne_ATypedRangeToNeither()
    {
        Assert.Equal(((string?)null, (int?)4), LiteTimeRange.SettingsFor(TimeRangePresets.FromLegacyHours(4)!));
        Assert.Equal(((string?)null, (int?)24), LiteTimeRange.SettingsFor(TimeRangePresets.FromLegacyHours(24)!));
        Assert.Equal(("15m", (int?)null), LiteTimeRange.SettingsFor(TimeRangeSpec.Relative(TimeSpan.FromMinutes(15))));
        Assert.Equal(("previous-week", (int?)null), LiteTimeRange.SettingsFor(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek)));
        Assert.Equal(((string?)null, (int?)null), LiteTimeRange.SettingsFor(TimeRangeSpec.FixedRange(At(10, 1, 0, 0), At(10, 2, 0, 0))));
        Assert.Equal(((string?)null, (int?)null), LiteTimeRange.SettingsFor(TimeRangeSpec.SinceInstant(At(10, 1, 0, 0))));
    }

    [Fact]
    public void TheTwoKeysNeverDisagree_ChoosingAWholeHourRangeClearsTheNewKey()
    {
        var root = new System.Text.Json.Nodes.JsonObject { ["default_time_range"] = "previous-week", ["default_time_range_hours"] = 4 };

        App.WriteDefaultTimeRange(root, null, 12);
        Assert.Equal(12, (int)root["default_time_range_hours"]!);
        Assert.False(root.ContainsKey("default_time_range"));

        App.WriteDefaultTimeRange(root, "30m", null);
        Assert.Equal("30m", (string?)root["default_time_range"]);
        Assert.Equal(12, (int)root["default_time_range_hours"]!);
    }

    // ── Notes: the retention start and the collector's interval ──

    [Fact]
    public void TheDataStart_IsTheProbedFloor_ElseTheStaticRetentionEdge()
    {
        var now = At(10, 8, 12, 0);
        var floor = At(10, 3, 14, 0);

        Assert.Equal(floor, LiteTimeRange.DataStartFor(floor, now));
        Assert.Equal(RetentionService.OldestRetainedInstant(now), LiteTimeRange.DataStartFor(null, now));

        /* The picker words it from the range: a start well before the data start shows the note, one after it does not. */
        var long30 = Resolve(TimeRangeSpec.Relative(TimeSpan.FromDays(30)), now, TimeZoneInfo.Utc);
        Assert.StartsWith("Data starts", TimeRangeNotes.DataStartNote(long30, floor));
        Assert.Null(TimeRangeNotes.DataStartNote(Resolve(TimeRangeSpec.Relative(TimeSpan.FromHours(4)), now, TimeZoneInfo.Utc), floor));
    }

    [Theory]
    [InlineData(1, 5, false)]
    [InlineData(15, 30, true)]
    [InlineData(5, 10, true)]
    [InlineData(5, 15, false)]
    [InlineData(0, 5, false)]
    public void TheSampleInterval_IsTheCollectorsActualCadence_AndAShortSpanShowsTheNote(int minutes, int spanMinutes, bool noteShown)
    {
        var interval = LiteTimeRange.SampleIntervalFor(minutes);
        var span = TimeSpan.FromMinutes(spanMinutes);

        if (minutes == 0)
        {
            Assert.Null(interval);
        }
        else
        {
            Assert.Equal(TimeSpan.FromMinutes(minutes), interval);
        }

        Assert.Equal(noteShown, TimeRangeNotes.SampleIntervalNote(span, interval) != null);
    }

    // ── The wiring: source pins on the tab's files ──

    [Fact]
    public void TheDisplayModeSwitch_DrawsThePickerAgain_AndParsesNothing()
    {
        var body = MethodBody(CodeOnly(ReadControl("ServerTab.TimeRange.cs")), "private async void TimeDisplayMode_SelectionChanged(");

        Assert.Contains("RangePicker.Refresh();", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheToolbar_HoldsTheSharedPicker_AndNoneOfTheOldControls()
    {
        var xaml = ReadControl("ServerTab.xaml");

        Assert.Contains("<ui:TimeRangePicker x:Name=\"RangePicker\"", xaml, StringComparison.Ordinal);
        foreach (var gone in new[] { "TimeRangeCombo", "FromDatePicker", "ToDatePicker", "FromHourCombo", "ToHourCombo", "FromMinuteCombo", "ToMinuteCombo" })
        {
            Assert.DoesNotContain(gone, xaml, StringComparison.Ordinal);
        }

        /* Apply to All, Compare and the display-mode combo stay beside it. */
        Assert.Contains("ApplyTimeRangeToAll_Click", xaml, StringComparison.Ordinal);
        Assert.Contains("CompareToCombo", xaml, StringComparison.Ordinal);
        Assert.Contains("TimeDisplayModeBox", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePickersChoice_IsRemembered_AndNeverDropsAChangeBecauseARefreshIsRunning()
    {
        var body = MethodBody(CodeOnly(ReadControl("ServerTab.TimeRange.cs")), "private async void RangePicker_RangeChanged(");

        Assert.DoesNotContain("_isRefreshing", body, StringComparison.Ordinal);
        Assert.Contains("_suppressRangeRefresh", body, StringComparison.Ordinal);
        Assert.Contains("PersistSelectedTimeRange(e.Spec);", body, StringComparison.Ordinal);
        Assert.Contains("RefreshAllDataAsync()", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheQueryStatsProbe_FeedsThePickersDataStart_WithoutANewQuery()
    {
        var refresh = CodeOnly(ReadControl("ServerTab.Refresh.cs"));
        var body = MethodBody(refresh, "private async System.Threading.Tasks.Task RefreshWindowTruncatedBannerAsync(");

        /* #5562 R7: every relation feeds, keyed by its collector, and the tab shows the floor of the page on screen. */
        Assert.Contains("FeedDataStart(LiteTimeRange.CollectorOfRelation(relation), floor);", body, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(body, @"GetQueryWindowFloorAsync\("));
    }

    [Fact]
    public void TheCurrentWindow_ComesFromThePicker_AndTheTabSetsItsZoneAndDefault()
    {
        var refresh = CodeOnly(ReadControl("ServerTab.Refresh.cs"));
        Assert.Contains("LiteTimeRange.WindowFor(CurrentRange())", MethodBody(refresh, "private (int hoursBack, DateTime? fromUtc, DateTime? toUtc) GetCurrentWindowUtc("), StringComparison.Ordinal);

        var ctor = CodeOnly(ReadControl("ServerTab.xaml.cs"));
        Assert.Contains("RangePicker.ZoneProvider = GetPickerZone;", ctor, StringComparison.Ordinal);
        Assert.Contains("RangePicker.Value = App.DefaultTimeRange;", ctor, StringComparison.Ordinal);
    }

    /* The text of the method that starts at the signature, up to its closing brace: these files indent members by four
       spaces, so the first "\n    }\n" after the signature is the end of that member. */
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' is no longer in the source; update this pin.");
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        return source[start..end];
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ReadControl(string name, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", name)));
}
