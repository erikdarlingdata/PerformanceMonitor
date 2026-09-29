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
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a chart drill opens a window of real minutes around the clicked point. The drills used to add the minutes
/// to the server's wall clock, so on a US Eastern server a click at 03:10 on 8 March (the day the clock skips
/// 02:00-03:00) got 02:40 for its start, a time that never happened. It converts to 07:40 UTC, the same instant as
/// the end (03:40), and the drill window was empty. <see cref="ServerTab.GetDrillWindow"/> steps the clicked instant
/// instead and reads each end on the server's clock.
///
/// <para>The drills are WPF handlers this suite does not instantiate, so the wiring is a source pin; the invariant
/// it protects is a pure test on the method they call.</para>
/// </summary>
public sealed class ServerTabDrillWindowTests
{
    /* The clock the server reports: the zone wins over the winter offset. Spring-forward 2026 is 8 March at 07:00 UTC
       and the fall-back is 1 November at 06:00 UTC. */
    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    private static DateTime At(int year, int month, int day, int hour, int minute) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// The click that failed: 03:10 on the spring-forward day. The window is an hour of real time, from 01:40 (before
    /// the change) to 03:40 (after it), and both ends convert back to instants exactly an hour apart.
    /// </summary>
    [Fact]
    public void AClickJustAfterTheSpringForward_KeepsSixtyRealMinutes()
    {
        var clock = Eastern();
        var center = clock.ToUtc(At(2026, 3, 8, 3, 10));   /* 07:10 UTC */

        var (from, to) = ServerTab.GetDrillWindow(center, 30, 30);

        Assert.Equal(At(2026, 3, 8, 6, 40), from);
        Assert.Equal(At(2026, 3, 8, 7, 40), to);
        Assert.Equal(TimeSpan.FromMinutes(60), to - from);
    }

    /// <summary>
    /// The shape this replaced, for contrast: 30 minutes of wall clock before 03:10 is 02:40, which is inside the
    /// skipped hour and converts to the same instant as 03:40. Nothing in the drill's window survived.
    /// </summary>
    [Fact]
    public void WallClockArithmetic_AcrossTheSkippedHour_GivesAnEmptyWindow()
    {
        var clock = Eastern();
        var click = At(2026, 3, 8, 3, 10);

        var from = clock.ToUtc(click.AddMinutes(-30));
        var to = clock.ToUtc(click.AddMinutes(30));

        Assert.Equal(At(2026, 3, 8, 7, 40), from);
        Assert.Equal(from, to);
    }

    /// <summary>An ordinary date: half an hour of wall clock either side.</summary>
    [Fact]
    public void AnOrdinaryClick_GivesHalfAnHourEitherSide()
    {
        var clock = Eastern();
        var center = clock.ToUtc(At(2026, 6, 15, 12, 0));   /* 16:00 UTC */

        var (from, to) = ServerTab.GetDrillWindow(center, 30, 30);

        Assert.Equal(At(2026, 6, 15, 15, 30), from);
        Assert.Equal(At(2026, 6, 15, 16, 30), to);
    }

    /// <summary>
    /// The heatmap passes its bucket's UTC instant and a 5-before, 10-after window, and the ends read back as the
    /// instants 5 minutes before and 10 after. A bucket at 07:05 UTC on the spring-forward day starts exactly at the
    /// change; one at 07:02 UTC starts before it, where wall-clock arithmetic (03:02 less 5 minutes is 02:57, a time
    /// that never happened) reads back an hour late.
    /// </summary>
    [Theory]
    [InlineData(7, 5, 7, 0, 7, 15)]
    [InlineData(7, 2, 6, 57, 7, 12)]
    public void AHeatmapBucketAtTheSpringForward_KeepsItsFiveBeforeAndTenAfter(
        int bucketHour, int bucketMinute, int fromHour, int fromMinute, int toHour, int toMinute)
    {
        var (from, to) = ServerTab.GetDrillWindow(At(2026, 3, 8, bucketHour, bucketMinute), 5, 10);

        Assert.Equal(At(2026, 3, 8, fromHour, fromMinute), from);
        Assert.Equal(At(2026, 3, 8, toHour, toMinute), to);
    }

    /// <summary>
    /// A click at 01:30 on the fall-back day. The wall clock reads 01:30 twice and a server-local time cannot say
    /// which, so the click resolves to the first, 05:30 UTC. The window is real minutes around that instant, but its
    /// far end (06:00 UTC, the second 01:00) reads the same as its near end (05:00 UTC, the first), and read back it
    /// is the same instant. This test records what happens today; it is not a fix.
    /// </summary>
    [Fact]
    public void AClickInTheRepeatedAutumnHour_OpensSixtyRealMinutes()
    {
        /* The point clicked is a UTC instant (#4766), so a click in either occurrence of the repeated hour opens the
           sixty real minutes around it: 05:30 UTC (01:30 EDT) opens 05:00 to 06:00, and 06:30 UTC (01:30 EST) opens
           06:00 to 07:00. The server-local drill used to resolve both to the first occurrence and, for the second,
           opened a window that collapsed to one instant. */
        var (from, to) = ServerTab.GetDrillWindow(At(2026, 11, 1, 5, 30), 30, 30);

        Assert.Equal(At(2026, 11, 1, 5, 0), from);
        Assert.Equal(At(2026, 11, 1, 6, 0), to);

        var (secondFrom, secondTo) = ServerTab.GetDrillWindow(At(2026, 11, 1, 6, 30), 30, 30);

        Assert.Equal(At(2026, 11, 1, 6, 0), secondFrom);
        Assert.Equal(At(2026, 11, 1, 7, 0), secondTo);
        Assert.Equal(TimeSpan.FromMinutes(60), secondTo - secondFrom);
    }

    /// <summary>
    /// Every drill in <c>ServerTab.DrillDown.cs</c> builds its window through <c>GetDrillWindow</c>: the four that
    /// take a chart time pass it as an instant, and the heatmap passes its bucket's UTC time as it is. Comments are
    /// stripped first, so a sentence that names the old shape cannot fail the pin.
    /// </summary>
    [Theory]
    [InlineData("private void ShowQueriesForWaitType_Click(", "ToUtcFromServerLocal(time), 30, 30, _serverClock")]
    [InlineData("private async void OnActiveQueriesDrillDown(", "ToUtcFromServerLocal(time), 30, 30, _serverClock")]
    [InlineData("private async void OnBlockingDrillDown(", "ToUtcFromServerLocal(time), 30, 30, _serverClock")]
    [InlineData("private async void OnDeadlockDrillDown(", "ToUtcFromServerLocal(time), 30, 30, _serverClock")]
    [InlineData("private async void OnHeatmapDrillDown(", "bucketTimeUtc, 5, 10, _serverClock")]
    public void EveryDrillSite_BuildsItsWindowThroughGetDrillWindow(string signature, string arguments)
    {
        var body = MethodBody(CodeOnly(ReadDrillDownSource()), signature);

        Assert.Contains("= GetDrillWindow(" + arguments + ");", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The file adds minutes in one place, inside <c>GetDrillWindow</c>, and calls it from exactly the five drills
    /// above. A drill added later that steps a wall-clock time by hand, or one that calls the method without a row
    /// in the pin above, fails here.
    /// </summary>
    [Fact]
    public void TheDrillFile_AddsMinutesOnlyInsideGetDrillWindow()
    {
        var source = CodeOnly(ReadDrillDownSource());
        var start = source.IndexOf("internal static (DateTime From, DateTime To) GetDrillWindow(", StringComparison.Ordinal);
        Assert.True(start >= 0, "GetDrillWindow is no longer in the source; update this pin.");
        var end = source.IndexOf(';', start);
        Assert.True(end > start, "the end of GetDrillWindow was not found.");
        var definition = source[start..(end + 1)];
        var rest = source.Remove(start, definition.Length);

        Assert.Contains("centerUtc.AddMinutes(-minutesBefore)", definition, StringComparison.Ordinal);
        Assert.Contains("centerUtc.AddMinutes(minutesAfter)", definition, StringComparison.Ordinal);
        Assert.DoesNotContain("AddMinutes(", rest, StringComparison.Ordinal);
        Assert.Equal(5, Regex.Matches(rest, @"\bGetDrillWindow\(").Count);
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

    private static string ReadDrillDownSource([CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", "ServerTab.DrillDown.cs")));
}
