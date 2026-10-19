/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The status line after "Apply Default to All Servers" (#4938): the per-server schedule overrides and the per-server run times the
/// call removed are counted apart and each named, singular or plural, so a reset of run times alone no longer reads as a reset of
/// schedule overrides. The store's own counts are checked in <see cref="CollectorRunTimeViewerLiveTests"/>.
/// </summary>
public sealed class CollectorScheduleResetStatusTests
{
    private const string ResetTail = " — every server now follows the fleet default.";

    [Theory]
    [InlineData(0, 0, "No per-server overrides to reset — every server already follows the fleet default.")]
    [InlineData(1, 0, "Reset 1 per-server schedule override" + ResetTail)]
    [InlineData(3, 0, "Reset 3 per-server schedule overrides" + ResetTail)]
    [InlineData(0, 1, "Reset 1 per-server run time" + ResetTail)]
    [InlineData(0, 2, "Reset 2 per-server run times" + ResetTail)]
    [InlineData(1, 2, "Reset 1 per-server schedule override and 2 per-server run times" + ResetTail)]
    [InlineData(2, 1, "Reset 2 per-server schedule overrides and 1 per-server run time" + ResetTail)]
    public void TheResetStatus_CountsScheduleOverridesAndRunTimesApart_AndNamesEach(int scheduleOverrides, int runTimes, string expected)
    {
        Assert.Equal(expected, CollectorScheduleOverlay.FormatResetStatus(new CollectorScheduleResetCounts(scheduleOverrides, runTimes)));
    }

    /// <summary>The window shows the text the formatter builds from the call's two counts, and no longer one number for both.</summary>
    [Fact]
    public void TheWindow_ShowsTheCountedResetStatus_NotOneNumberForBothKinds()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "CollectorScheduleEditorWindow.xaml.cs");

        Assert.Contains("CollectorScheduleOverlay.FormatResetStatus(removed)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("schedule override(s)", source, StringComparison.Ordinal);
    }
}
