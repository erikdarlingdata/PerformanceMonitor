/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a preset chart window's axis spans the same real hours the reads fetch. The reads take the last
/// <c>hoursBack</c> real hours, and every row plots at its own UTC instant, so the axis is those instants: the last
/// <c>hoursBack</c> real hours ending now. It used to be the server's wall-clock now minus <c>hoursBack</c> hours,
/// which across a daylight saving change starts an hour off the first row: in spring the first real hour lay left of
/// the axis and was cropped, in autumn the axis began with an hour that holds no rows. <c>ServerTab.GetChartWindow</c>
/// now returns the UTC window, so a 24 hour preset is 24 hours across a change and the server's clock decides only
/// how the ticks and the hover are worded.
/// </summary>
public sealed class ServerTabChartWindowTests
{
    /* US Eastern is where the old axis went wrong. The 2026 spring change is 8 March at 07:00 UTC (02:00 EST jumps to
       03:00 EDT) and the autumn change is 1 November at 06:00 UTC (02:00 EDT falls back to 01:00 EST). */
    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Utc);

    /* One row every 15 minutes, from hoursBack real hours before utcNow to utcNow, inclusive at both ends. */
    private static List<DateTime> RowInstants(DateTime utcNow, int hoursBack)
    {
        var rows = new List<DateTime>();
        for (var t = utcNow.AddHours(-hoursBack); t <= utcNow; t = t.AddMinutes(15))
        {
            rows.Add(t);
        }

        return rows;
    }

    /* A row plots at its own instant, so it is on the axis when the instant lies between the axis ends. */
    private static void AssertEveryRowIsOnTheAxis(List<DateTime> rowInstants, DateTime start, DateTime end)
    {
        var outside = rowInstants
            .Where(t => t < start || t > end)
            .Select(t => $"{t:yyyy-MM-dd HH:mm}Z")
            .ToList();

        Assert.True(
            outside.Count == 0,
            $"{outside.Count} of {rowInstants.Count} rows plot outside the axis [{start:yyyy-MM-dd HH:mm}, " +
            $"{end:yyyy-MM-dd HH:mm}]; the first is {outside.FirstOrDefault()}.");
    }

    /// <summary>
    /// A 24 hour window that starts before the spring change and ends after it: the axis starts at the first row's
    /// own instant and ends at the last, and no row lies left of it. It spans the 24 real hours, not the 25 the
    /// server's wall clock counts across the change.
    /// </summary>
    [Fact]
    public void ASpringChangeInsideThePresetWindow_LeavesNoRowLeftOfTheAxis()
    {
        var utcNow = Utc(2026, 3, 8, 16, 0);
        var rows = RowInstants(utcNow, 24);

        var (start, end) = ServerTab.GetChartWindow(24, null, null, utcNow);

        AssertEveryRowIsOnTheAxis(rows, start, end);
        Assert.Equal(rows.First(), start);
        Assert.Equal(rows.Last(), end);
        Assert.Equal(TimeSpan.FromHours(24), end - start);
    }

    /// <summary>
    /// A 24 hour window that starts before the autumn change and ends after it: the axis starts at the first row's
    /// own instant, with no empty hour at the left. It spans the 24 real hours, not the 23 the server's wall clock
    /// counts across the change.
    /// </summary>
    [Fact]
    public void AnAutumnChangeInsideThePresetWindow_LeavesNoEmptyHourAtTheLeftOfTheAxis()
    {
        var utcNow = Utc(2026, 11, 1, 16, 0);
        var rows = RowInstants(utcNow, 24);

        var (start, end) = ServerTab.GetChartWindow(24, null, null, utcNow);

        AssertEveryRowIsOnTheAxis(rows, start, end);
        Assert.Equal(rows.First(), start);
        Assert.Equal(rows.Last(), end);
        Assert.Equal(TimeSpan.FromHours(24), end - start);
    }

    /// <summary>A window that holds no clock change: the axis is exactly the hours asked for, ending now.</summary>
    [Fact]
    public void AWindowWithNoChangeInIt_GetsExactlyHoursBackAcrossTheAxis()
    {
        var utcNow = Utc(2026, 7, 1, 16, 0);
        var rows = RowInstants(utcNow, 24);

        var (start, end) = ServerTab.GetChartWindow(24, null, null, utcNow);

        AssertEveryRowIsOnTheAxis(rows, start, end);
        Assert.Equal(TimeSpan.FromHours(24), end - start);
        Assert.Equal(utcNow.AddHours(-24), start);
        Assert.Equal(utcNow, end);
    }

    /// <summary>
    /// A custom range is a pair of UTC instants and is the axis as it is: both bounds come through unchanged, on
    /// either side of a clock change. With only one bound set the preset is used, as it is for the reads.
    /// </summary>
    [Fact]
    public void ACustomUtcRange_IsTheAxisAsHeld_AndOneBoundFallsBackToThePreset()
    {
        var from = new DateTime(2026, 3, 7, 9, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 3, 8, 14, 30, 0, DateTimeKind.Unspecified);
        var utcNow = Utc(2026, 9, 29, 12, 0);

        var (start, end) = ServerTab.GetChartWindow(24, from, to, utcNow);

        Assert.Equal(from, start);
        Assert.Equal(to, end);

        var preset = ServerTab.GetChartWindow(24, null, null, utcNow);
        Assert.Equal(preset, ServerTab.GetChartWindow(24, from, null, utcNow));
        Assert.Equal(preset, ServerTab.GetChartWindow(24, null, to, utcNow));
    }

    /// <summary>
    /// No chart in a ServerTab file builds its start by taking hours off a server-local end. Only the UTC range
    /// in the slicer, which takes them off <c>DateTime.UtcNow</c>, may subtract <c>hoursBack</c> that way. Comments
    /// are stripped first, so a sentence that names the old shape cannot fail the pin.
    /// </summary>
    [Fact]
    public void NoServerTabFile_TakesHoursBackOffAnEndThatIsNotUtcNow()
    {
        var files = Directory.GetFiles(ControlsFolder(), "ServerTab*.cs");
        Assert.True(files.Length >= 10, $"expected the ServerTab partial files, found {files.Length}.");

        var offenders = new List<string>();
        foreach (var file in files)
        {
            var code = CodeOnly(File.ReadAllText(file));
            foreach (Match m in Regex.Matches(code, @"(?<!DateTime\.UtcNow)\.AddHours\(\s*-\s*hoursBack\s*\)"))
            {
                var line = code.Take(m.Index).Count(c => c == '\n') + 1;
                offenders.Add($"{Path.GetFileName(file)} (code line {line})");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "These take hoursBack off a server-local end, which starts the axis an hour off the first row across a " +
            "daylight saving change; use GetChartWindow: " + string.Join(", ", offenders));
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ControlsFolder([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
