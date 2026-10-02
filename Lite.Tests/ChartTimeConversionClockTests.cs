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
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a chart's X is the sample's naive-UTC instant, as stored, and only the text drawn on the chart is in the
/// display zone. The axis ticks, the hover and the crosshair turn the instant into a wall clock in the zone the user
/// reads (UTC, this machine's, or the server's own) through <c>DisplayZone</c>, so a sample from before a daylight
/// saving change is labelled with the wall clock it had then, whatever offset is in force today, and the two 01:30s of
/// the autumn change day stay two points an hour apart. The Overview lanes and three History windows used to build the
/// X by adding today's offset to every sample, which put a sample from before a change an hour off the instant the
/// axis read it back as: a US Eastern server's 15:00 UTC sample from 1 March, viewed in September, was labelled
/// 16:00 UTC.
///
/// <para>The tests hold that two ways. Pure checks on what a chart composes: the instant read back in each display
/// zone in winter, in summer and across the repeated hour, and the old one-offset shape for contrast. And source pins
/// that the Overview lanes plot each sample's instant, and that the three History windows plot the instant and word
/// their axis, hover and summary in the display zone; those are WPF controls this suite does not instantiate.</para>
/// </summary>
/* Names ServerTimeHelper, whose clock and display mode are process-wide mutable statics that other classes write;
   joins the collection every class that touches them uses, so none of those runs between two reads of this one. */
[Collection("server-time-helper")]
public sealed class ChartTimeConversionClockTests
{
    /// <summary>
    /// A lane plots the sample's UTC instant as X (<c>d.X.ToOADate()</c>), and the label of that X is the instant in the
    /// display zone (<c>DisplayZone.ToDisplay</c>, which the tick generator and the crosshair both use): in UTC display
    /// the sample's own UTC time, in local display this machine's wall clock at that instant, on the server's clock the
    /// server's wall clock at it. That holds in winter and in summer, whatever offset is in force today.
    /// </summary>
    [Theory]
    [InlineData(2026, 3, 1, 15, 0)]    /* EST: today's offset (September, EDT) is an hour out for this one */
    [InlineData(2026, 3, 8, 6, 30)]    /* 01:30 EST, the half hour before the change */
    [InlineData(2026, 3, 8, 7, 30)]    /* 03:30 EDT, the half hour after it */
    [InlineData(2026, 7, 1, 12, 0)]    /* EDT */
    [InlineData(2026, 11, 1, 7, 30)]   /* 02:30 EST, after the fall-back */
    public void APlottedX_IsTheSamplesOwnInstant_AndItsLabelIsTheInstantInTheDisplayZone(int y, int mo, int d, int h, int mi)
    {
        var eastern = ServerClock.Resolve("Eastern Standard Time", -300);
        var utc = new DateTime(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

        var plotted = DateTime.FromOADate(utc.ToOADate());

        Assert.Equal(utc, plotted);
        Assert.Equal(utc, DisplayZone.ToDisplay(plotted, TimeZoneInfo.Utc));
        Assert.Equal(
            TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(utc, DateTimeKind.Utc), TimeZoneInfo.Local),
            DisplayZone.ToDisplay(plotted, TimeZoneInfo.Local));
        Assert.Equal(eastern.ToServerLocal(utc), DisplayZone.ToDisplay(plotted, eastern.AsTimeZone()));
    }

    /// <summary>
    /// The repeated autumn hour, which a wall-clock X could not tell apart: 05:30 and 06:30 UTC on 1 November 2026 both
    /// read 01:30 on a US Eastern clock. X is the instant, so each is plotted at its own place an hour apart, and the
    /// crosshair says which one it is: 06:30 UTC is the SECOND 01:30 (-05:00), 05:30 UTC the first (-04:00).
    /// </summary>
    [Fact]
    public void APlottedX_InTheRepeatedAutumnHour_ReadsBackAsTheOccurrenceItWas()
    {
        var eastern = ServerClock.Resolve("Eastern Standard Time", -300).AsTimeZone();
        var first = new DateTime(2026, 11, 1, 5, 30, 0, DateTimeKind.Unspecified);
        var second = new DateTime(2026, 11, 1, 6, 30, 0, DateTimeKind.Unspecified);

        var plottedFirst = DateTime.FromOADate(first.ToOADate());
        var plottedSecond = DateTime.FromOADate(second.ToOADate());

        Assert.Equal(TimeSpan.FromHours(1), plottedSecond - plottedFirst);
        Assert.Equal(new DateTime(2026, 11, 1, 1, 30, 0), DisplayZone.ToDisplay(plottedSecond, eastern));
        Assert.Equal("2026-11-01 01:30:00 -04:00", CorrelatedCrosshairManager.FormatCrosshairTime(plottedFirst, () => eastern));
        Assert.Equal("2026-11-01 01:30:00 -05:00", CorrelatedCrosshairManager.FormatCrosshairTime(plottedSecond, () => eastern));
        Assert.Equal("2026-11-01 06:30:00", CorrelatedCrosshairManager.FormatCrosshairTime(plottedSecond, () => TimeZoneInfo.Utc));
    }

    /// <summary>
    /// The one-offset shape this replaced, for contrast: the same sample plotted with today's fixed offset does NOT
    /// read back as its own instant once a change lies between it and today's offset.
    /// </summary>
    [Fact]
    public void AnXBuiltFromOneOffset_ReadsBackAnHourOut_ForASampleFromBeforeTheChange()
    {
        var clock = ServerClock.Resolve("Eastern Standard Time", -300);
        var utc = new DateTime(2026, 3, 1, 15, 0, 0, DateTimeKind.Unspecified);
        var summerOffset = clock.OffsetMinutesAt(new DateTime(2026, 9, 29, 12, 0, 0, DateTimeKind.Unspecified));
        Assert.Equal(-240, summerOffset);

        var plotted = utc.AddMinutes(summerOffset);

        Assert.Equal(utc.AddHours(1), ServerTimeHelper.ConvertForDisplay(plotted, TimeDisplayMode.UTC, clock));
    }

    /// <summary>
    /// The Overview lanes plot each sample's own instant and pin the axis to the window's instants: nothing is
    /// converted through the clock, and no offset is added to a whole series. Comments are stripped first, so a
    /// sentence that names an old shape cannot fail the pin.
    /// </summary>
    [Fact]
    public void OverviewLanes_PlotEachSamplesOwnInstant_AndPinTheAxisToTheWindowsInstants()
    {
        var source = CodeOnly(ReadLite("Controls", "CorrelatedTimelineLanesControl.xaml.cs"));

        var refresh = MethodBody(source, "public async Task RefreshAsync(");
        Assert.DoesNotContain("AddMinutes(", refresh, StringComparison.Ordinal);
        Assert.DoesNotContain("utcOffset", refresh, StringComparison.Ordinal);
        Assert.DoesNotContain("UtcOffsetMinutes", refresh, StringComparison.Ordinal);
        Assert.DoesNotContain("ToServerTime(", refresh, StringComparison.Ordinal);
        Assert.Contains("d.SampleTimeUtc.ToOADate()", refresh, StringComparison.Ordinal);
        Assert.Contains("d.CollectionTime.ToOADate()", refresh, StringComparison.Ordinal);

        var sync = MethodBody(source, "private void SyncXAxes(");
        Assert.DoesNotContain("AddMinutes(", sync, StringComparison.Ordinal);
        Assert.DoesNotContain("utcOffset", sync, StringComparison.Ordinal);
        Assert.Contains("GetXAxisWindow(hoursBack, fromDate, toDate, DateTime.UtcNow)", sync, StringComparison.Ordinal);
    }

    /// <summary>
    /// Each History window plots the collection time as it is stored (the naive-UTC instant), draws its ticks on the
    /// display-zone axis, hands its hover the display zone, and words its first/last sample summary in that zone (#4766).
    /// </summary>
    [Theory]
    [InlineData("ProcedureHistoryWindow.xaml.cs")]
    [InlineData("QueryStatsHistoryWindow.xaml.cs")]
    [InlineData("QueryStoreHistoryWindow.xaml.cs")]
    public void HistoryWindows_PlotTheCollectionTimeAsTheInstant_AndWordItInTheDisplayZone(string file)
    {
        var source = CodeOnly(ReadLite("Windows", file));

        Assert.DoesNotContain("UtcOffsetMinutes", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ToServerTime(", source, StringComparison.Ordinal);
        Assert.Contains("Select(r => r.CollectionTime.ToOADate())", source, StringComparison.Ordinal);
        Assert.Contains("Axes.DateTimeTicksBottomUtc(_displayZone)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTimeTicksBottomDateChange", source, StringComparison.Ordinal);
        Assert.Contains("new ChartHoverHelper(HistoryChart, unit, displayZone: _displayZone)", source, StringComparison.Ordinal);

        /* The summary range is worded in that same zone, read once, through FormatInstant, which adds the instant's UTC
           offset in the repeated autumn hour (#4766); HistoryGridZoneTests pins it further. */
        Assert.Contains("var zone = _displayZone();", source, StringComparison.Ordinal);
        Assert.Contains("ServerTimeHelper.FormatInstant(_historyData.First().CollectionTime, zone, \"MM/dd HH:mm\")", source, StringComparison.Ordinal);
        Assert.Contains("ServerTimeHelper.FormatInstant(_historyData.Last().CollectionTime, zone, \"MM/dd HH:mm\")", source, StringComparison.Ordinal);
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

    private static string ReadLite(string folder, string file, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", folder, file)));
}
