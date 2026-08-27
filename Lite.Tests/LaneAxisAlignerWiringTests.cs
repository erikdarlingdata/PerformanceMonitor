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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2535: Lite's Overview control is a third copy of the same five-separate-<c>WpfPlot</c> surface that
/// carried #2533 on the Darling viewer - identical X-axis LIMITS across the lanes, but nothing gave them
/// identical pixel geometry, so a lane whose Y ticks read six digits started its data area further right
/// than one reading a fraction. Values stayed correct throughout on Darling (tooltips read true, only the
/// data-&gt;pixel mapping skewed), and the same is true here: this is a layout defect, not a data one.
///
/// <para>The fix already exists in the shared <c>PerformanceMonitor.Ui.LaneAxisAligner</c> that Lite
/// already references - see <c>Darling.Tests/LaneAxisAlignerTests.cs</c> for the behavioral coverage
/// (headless ScottPlot renders proving the gutter is independent of plot width and non-decreasing in plot
/// height). Those are properties of the shared helper, not of either SKU, so they are not duplicated here.
/// What IS SKU-specific is whether Lite's control actually calls it - the defect on Darling was an absent
/// call site, not a wrong calculation, so only a wiring pin against Lite's own control proves anything.</para>
///
/// <para>This lives in <c>Lite.Tests</c> rather than beside the behavioral tests in <c>Darling.Tests</c>:
/// the CI path filters in <c>.github/workflows/build.yml</c> gate on directories, and a Lite-only edit to
/// this control fires the <c>lite</c> filter, which this suite is already triggered by. Parsed from
/// <c>Darling.Tests</c> instead, this would be a guard that silently stops guarding on exactly the change
/// it exists to catch, unless the <c>darling</c> filter also named this file - the same trap the parity
/// pins already documented there (<c>Lite/Controls/ServerTab.xaml</c> et al.) exist to avoid.</para>
/// </summary>
public sealed class LaneAxisAlignerWiringTests
{
    /// <summary>
    /// The helper working proves nothing if the control never calls it. Parse Lite's real control and
    /// assert the aligner runs inside <c>SyncXAxes</c> and BEFORE the loop that refreshes the lanes - a
    /// floor applied after the render would not take effect until something else redrew.
    /// </summary>
    [Fact]
    public void TheLiteOverviewControl_AlignsItsLanes_BeforeItRefreshesThem()
    {
        var source = File.ReadAllText(ControlSourcePath());
        const string What = "Lite's Overview control";

        string body = SyncXAxesBody(source, What);

        int align = body.IndexOf("LaneAxisAligner.AlignLeftGutters(", StringComparison.Ordinal);
        Assert.True(align >= 0,
            $"{What} sets identical X LIMITS but never gives the lanes one left gutter (#2535) - they will skew again the moment one lane's Y labels get wide");

        int refresh = body.IndexOf(".Refresh();", StringComparison.Ordinal);
        Assert.True(refresh > align,
            $"{What} aligns the lanes after refreshing them - the shared gutter would not appear until something else redrew");
    }

    /// <summary>
    /// The body of <c>SyncXAxes</c>, bounded by brace matching rather than by the next member's name.
    /// Lite's control has an <c>AddGhostLine</c> helper a few members after <c>SyncXAxes</c> that also
    /// calls <c>Refresh()</c>, so anchoring on "the next Refresh() call in the file" instead of on the
    /// method's own closing brace would let the wrong method's code satisfy the assertions above.
    /// </summary>
    private static string SyncXAxesBody(string source, string what)
    {
        int start = source.IndexOf("private void SyncXAxes(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{what} no longer has a SyncXAxes - re-anchor this pin rather than deleting it");

        int open = source.IndexOf('{', start);
        Assert.True(open > start, $"{what}: SyncXAxes has no body");

        int depth = 0;
        int end = -1;
        for (int i = open; i < source.Length && end < 0; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                end = i;
            }
        }

        Assert.True(end > open, $"{what}: SyncXAxes body is unbalanced");
        return source[open..(end + 1)];
    }

    /// <summary>Lite's control, resolved from this test file's compile-time path (Lite.Tests is a sibling
    /// of the Lite project).</summary>
    private static string ControlSourcePath([CallerFilePath] string thisFile = "")
    {
        var testDir = Path.GetDirectoryName(thisFile)!;
        return Path.GetFullPath(Path.Combine(testDir, "..", "Lite", "Controls", "CorrelatedTimelineLanesControl.xaml.cs"));
    }
}
