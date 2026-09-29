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
/// #4766: a chart X is the sample's UTC instant on the server's wall clock, and the axis and the crosshair turn it
/// back into the display frame through the SAME clock (<c>AxesExtensions</c> -> <c>UiTimeContext.ConvertForDisplay</c>
/// -> <c>ServerTimeHelper.ConvertForDisplay</c>). The Overview lanes and three History windows built the X by adding
/// today's offset to every sample, so a sample from before a daylight saving change was plotted an hour off the
/// instant the axis then reads it back as: a US Eastern server's 15:00 UTC sample from 1 March, viewed in September,
/// was labelled 16:00 UTC. They now convert each sample through <c>ServerTimeHelper.ToServerTime</c>.
///
/// <para>The lanes and windows are WPF controls this suite does not instantiate, so the wiring is source pins; the
/// invariant they protect is a pure test on the two conversions the chart composes.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock, a process-wide mutable static; joins the collection every other class
   that writes it uses. */
[Collection("server-time-helper")]
public sealed class ChartTimeConversionClockTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;

    public void Dispose() => ServerTimeHelper.ActiveServerClock = _savedClock;

    /// <summary>
    /// The composed conversion is the identity on the instant, except in the repeated autumn hour (its one exception
    /// has its own case below): plot <c>ToServerTime(utc)</c>, read it back through the display conversion, and the
    /// UTC display shows the sample's own UTC time, in winter and in summer, with the clock that is in force today or
    /// not.
    /// </summary>
    [Theory]
    [InlineData(2026, 3, 1, 15, 0)]    /* EST: today's offset (September, EDT) is an hour out for this one */
    [InlineData(2026, 3, 8, 6, 30)]    /* 01:30 EST, the half hour before the change */
    [InlineData(2026, 3, 8, 7, 30)]    /* 03:30 EDT, the half hour after it */
    [InlineData(2026, 7, 1, 12, 0)]    /* EDT */
    [InlineData(2026, 11, 1, 7, 30)]   /* 02:30 EST, after the fall-back */
    public void APlottedX_ReadsBackAsTheSamplesOwnInstant_InUtcAndLocalDisplay(int y, int mo, int d, int h, int mi)
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);
        var utc = new DateTime(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

        var x = ServerTimeHelper.ToServerTime(utc).ToOADate();
        var plotted = DateTime.FromOADate(x);

        Assert.Equal(utc, ServerTimeHelper.ConvertForDisplay(plotted, TimeDisplayMode.UTC));
        Assert.Equal(
            TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(utc, DateTimeKind.Utc), TimeZoneInfo.Local),
            ServerTimeHelper.ConvertForDisplay(plotted, TimeDisplayMode.LocalTime));
    }

    /// <summary>
    /// The exception to the identity above: the second 01:30 of the fall-back day (06:30 UTC). The plotted X reads
    /// 01:30, and a wall-clock X cannot say which 01:30 it was, so the display conversion takes the first (05:30
    /// UTC). This test records what happens today; it is not a fix.
    /// </summary>
    [Fact]
    public void APlottedX_InTheRepeatedAutumnHour_ReadsBackAsTheFirstOccurrence_KnownLimit()
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);
        var utc = new DateTime(2026, 11, 1, 6, 30, 0, DateTimeKind.Unspecified);
        var firstOccurrence = new DateTime(2026, 11, 1, 5, 30, 0, DateTimeKind.Unspecified);

        var plotted = DateTime.FromOADate(ServerTimeHelper.ToServerTime(utc).ToOADate());

        Assert.Equal(new DateTime(2026, 11, 1, 1, 30, 0, DateTimeKind.Unspecified), plotted);
        // Known limit (#4766): in the repeated autumn hour a server-local time resolves to its first occurrence.
        Assert.Equal(firstOccurrence, ServerTimeHelper.ConvertForDisplay(plotted, TimeDisplayMode.UTC));
        Assert.Equal(
            TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(firstOccurrence, DateTimeKind.Utc), TimeZoneInfo.Local),
            ServerTimeHelper.ConvertForDisplay(plotted, TimeDisplayMode.LocalTime));
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
    /// The Overview lanes convert every plotted sample, the comparison ghost lines and the axis window through the
    /// clock. Comments are stripped first, so a sentence that names the old shape cannot fail the pin.
    /// </summary>
    [Fact]
    public void OverviewLanes_ConvertEachSampleAndTheAxisWindowThroughTheClock()
    {
        var source = CodeOnly(ReadLite("Controls", "CorrelatedTimelineLanesControl.xaml.cs"));

        var refresh = MethodBody(source, "public async Task RefreshAsync(");
        Assert.DoesNotContain("AddMinutes(", refresh, StringComparison.Ordinal);
        Assert.DoesNotContain("utcOffset", refresh, StringComparison.Ordinal);
        Assert.DoesNotContain("UtcOffsetMinutes", refresh, StringComparison.Ordinal);
        Assert.Contains("ServerTimeHelper.ToServerTime(", refresh, StringComparison.Ordinal);

        var sync = MethodBody(source, "private void SyncXAxes(");
        Assert.DoesNotContain("AddMinutes(", sync, StringComparison.Ordinal);
        Assert.DoesNotContain("utcOffset", sync, StringComparison.Ordinal);
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
        Assert.Contains("DisplayZone.ToDisplay(_historyData.First().CollectionTime, _displayZone())", source, StringComparison.Ordinal);
        Assert.Contains("DisplayZone.ToDisplay(_historyData.Last().CollectionTime, _displayZone())", source, StringComparison.Ordinal);
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
