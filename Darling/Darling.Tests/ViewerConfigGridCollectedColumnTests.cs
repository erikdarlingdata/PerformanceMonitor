/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4966: the viewer's five Configuration grids show the newest snapshot, so each ends with a <c>Collected</c> column that says when
/// that snapshot was captured, the same last column Lite's twin grids carry. The column shows the time to the second in the display
/// zone and sorts by the stored UTC instant.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerConfigGridCollectedColumnTests
{
    [Theory]
    [InlineData("ServerConfigGrid")]
    [InlineData("DatabaseConfigGrid")]
    [InlineData("DatabaseScopedConfigGrid")]
    [InlineData("QueryStoreHealthGrid")]
    [InlineData("TraceFlagsGrid")]
    public void EachConfigGrid_EndsWithACollectedColumn_SortingOnTheStoredInstant(string gridName)
    {
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        var start = xaml.IndexOf($"x:Name=\"{gridName}\"", StringComparison.Ordinal);
        Assert.True(start >= 0, $"no {gridName} in the viewer XAML");
        var end = xaml.IndexOf("</DataGrid>", start, StringComparison.Ordinal);
        Assert.True(end > start, $"{gridName} is unterminated");
        var grid = xaml[start..end];

        var columns = Regex.Matches(grid, @"<DataGridTextColumn [^>]*Binding=""\{Binding ([A-Za-z]+)\}""[^>]*>").ToList();
        var last = columns[^1];
        Assert.Equal("CaptureTimeLocal", last.Groups[1].Value);
        Assert.Contains("SortMemberPath=\"CaptureTime\"", last.Value, StringComparison.Ordinal);
        Assert.Contains("<TextBlock Text=\"Collected\"", grid[last.Index..], StringComparison.Ordinal);
    }

    [Fact]
    public void CaptureTimeLocal_WordsTheUtcInstant_InTheDisplayZone_ToTheSecond()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        var savedCulture = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            ViewerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);
            var at = new DateTime(2026, 1, 15, 17, 4, 59, DateTimeKind.Unspecified);

            Assert.Equal("2026-01-15 12:04:59", new ServerConfigRow { CaptureTime = at }.CaptureTimeLocal);
            Assert.Equal("2026-01-15 12:04:59", new DatabaseConfigRow { CaptureTime = at }.CaptureTimeLocal);
            Assert.Equal("2026-01-15 12:04:59", new DatabaseScopedConfigRow { CaptureTime = at }.CaptureTimeLocal);
            Assert.Equal("2026-01-15 12:04:59", new QueryStoreHealthRow { CaptureTime = at }.CaptureTimeLocal);
            Assert.Equal("2026-01-15 12:04:59", new TraceFlagRow { CaptureTime = at }.CaptureTimeLocal);

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            Assert.Equal("2026-01-15 17:04:59", new TraceFlagRow { CaptureTime = at }.CaptureTimeLocal);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
            CultureInfo.CurrentCulture = savedCulture;
        }
    }
}
