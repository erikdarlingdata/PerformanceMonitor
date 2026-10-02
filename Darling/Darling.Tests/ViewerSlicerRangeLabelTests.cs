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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the two time-range slicers word the range they hold the same way. Lite's caption (the start and the end of the
/// selection) goes through <see cref="DisplayZone.Format"/>, so the two instants of a repeated autumn hour read "01:30
/// -04:00" and "01:30 -05:00". The viewer's slicer is a copy of Lite's and used to build the caption with
/// <c>ForDisplay(...).ToString(...)</c>, which read both as "01:30". The tick labels along the axis stay the plain wall time
/// in both.
///
/// <para>The caption is built in a private method of a WPF control, so the tests read the two sources: the viewer's call
/// must be Lite's call (same method, same instants, same format, so the same culture), and the text that call gives for
/// the pair is checked on US Eastern (autumn change 2026-11-01 at 06:00 UTC).</para>
/// </summary>
public sealed class ViewerSlicerRangeLabelTests
{
    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    /// <summary>The caption's two Format calls, as written, from a slicer's UpdateRangeLabel.</summary>
    private static string[] FormatCalls(string source)
    {
        var body = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(source, "UpdateRangeLabel"))
            .Replace("UiDisplayZone.", "DisplayZone.", StringComparison.Ordinal);
        return Regex.Matches(body, @"DisplayZone\.Format\(UtcAtNorm\(_range(?:Start|End)\),[^;]*;")
            .Select(m => Regex.Replace(m.Value, @"\s+", " "))
            .ToArray();
    }

    /// <summary>
    /// The viewer's range caption is Lite's, call for call: two <c>DisplayZone.Format</c> calls, one per end of the range,
    /// in a zone value, with the same "yyyy-MM-dd HH:mm" format. That method words the text on the invariant culture in
    /// both products.
    /// </summary>
    [Fact]
    public void TheViewersRangeCaption_IsLitesCall_ForCall()
    {
        var viewer = FormatCalls(ViewerTypedRangeTests.ViewerSource("TimeRangeSlicerControl.xaml.cs", ThisFile()));
        var lite = FormatCalls(File.ReadAllText(LitePath("Controls", "TimeRangeSlicerControl.xaml.cs")));

        Assert.Equal(2, lite.Length);
        Assert.Equal(lite, viewer);
        Assert.Equal("DisplayZone.Format(UtcAtNorm(_rangeStart), zone, \"yyyy-MM-dd HH:mm\");", viewer[0]);
        Assert.Equal("DisplayZone.Format(UtcAtNorm(_rangeEnd), zone, \"yyyy-MM-dd HH:mm\");", viewer[1]);
    }

    /// <summary>
    /// The caption's zone is the display mode's zone on the active server's clock, and the viewer's caption no longer
    /// words the range from <c>ForDisplay</c>. The tick labels are the one place the slicer still does, so they stay
    /// the plain wall time.
    /// </summary>
    [Fact]
    public void TheViewersRangeCaption_ReadsTheCurrentDisplayZone_AndTheTickLabelsStayPlain()
    {
        var source = ViewerTypedRangeTests.ViewerSource("TimeRangeSlicerControl.xaml.cs", ThisFile());
        var caption = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(source, "UpdateRangeLabel"));

        Assert.Contains("var zone = ViewerTimeHelper.CurrentDisplayZone();", caption);
        Assert.DoesNotMatch(@"\bForDisplay\b", caption);

        var code = ViewerTypedRangeTests.StripComments(source);
        Assert.Single(Regex.Matches(code, @"ViewerTimeHelper\.ForDisplay\(tickTime\)\.ToString\(""MM/dd HH:mm""\)"));
        Assert.Single(Regex.Matches(code, @"\bForDisplay\("));
    }

    /// <summary>
    /// The text that shared call gives for the pair, on the culture Lite's caption uses (the invariant one, whatever the
    /// machine's): 05:30Z is the first 01:30 (-04:00), 06:30Z the second (-05:00), and the hour after is bare.
    /// </summary>
    [Fact]
    public void TheRangeCaptionsCall_NamesTheOffsetOfEachOccurrenceOfTheRepeatedHour()
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            /* A culture with its own time separator would change ToString(format) but not the invariant Format call. */
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fi-FI");
            var zone = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, Eastern);

            Assert.Equal("2026-11-01 01:30 -04:00", DisplayZone.Format(Utc(2026, 11, 1, 5, 30), zone, "yyyy-MM-dd HH:mm"));
            Assert.Equal("2026-11-01 01:30 -05:00", DisplayZone.Format(Utc(2026, 11, 1, 6, 30), zone, "yyyy-MM-dd HH:mm"));
            Assert.Equal("2026-11-01 02:30", DisplayZone.Format(Utc(2026, 11, 1, 7, 30), zone, "yyyy-MM-dd HH:mm"));
            Assert.Equal("2026-11-01 05:30", DisplayZone.Format(Utc(2026, 11, 1, 5, 30), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm"));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    /// <summary>A Lite source file, found by walking up from this test file to the repo root.</summary>
    private static string LitePath(string folder, string file, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Lite", folder, file);
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return Path.Combine(dir!, relative);
    }

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;
}
