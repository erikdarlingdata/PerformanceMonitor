/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>Source pins: the viewer's application-connections read stays a thin delegate to the storage reader.</summary>
public sealed class ViewerFinOpsApplicationConnectionsDelegatesTests
{
    private static string ViewerFile(string name)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !Directory.Exists(Path.Combine(dir, "PerformanceMonitor.Darling.Viewer")))
            dir = Path.GetDirectoryName(dir);
        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, "PerformanceMonitor.Darling.Viewer", name)).ReplaceLineEndings("\n");
    }

    [Fact]
    public void TheViewerPartial_HoldsNoSqlTextForTheApplicationConnectionsRead()
    {
        var src = ViewerFile("ViewerDataService.FinOps.Workload.cs");
        var start = src.IndexOf("ApplicationConnectionsSql", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = src.IndexOf("GetApplicationConnectionsAsync(", start, StringComparison.Ordinal);
        Assert.True(end > start);
        var alias = src[start..end];
        Assert.DoesNotContain("@\"", alias);
        Assert.DoesNotContain("v_session_stats", src, StringComparison.Ordinal);
        Assert.DoesNotContain("command.Parameters", src[end..src.IndexOf("return items.ConvertAll", end, StringComparison.Ordinal)]);
    }

    [Fact]
    public void GetApplicationConnectionsAsync_ReadsTheClock_ThenCallsTheStorageReader_AndMapsOnThatClock()
    {
        var src = ViewerFile("ViewerDataService.FinOps.Workload.cs");
        var at = src.IndexOf("GetApplicationConnectionsAsync(int serverId", StringComparison.Ordinal);
        Assert.True(at >= 0);
        var body = src[at..];
        var cutoff = body.IndexOf("var cutoff = DateTime.UtcNow.AddHours(-24)", StringComparison.Ordinal);
        var clock = body.IndexOf("ClockForServerOrMachine(", StringComparison.Ordinal);
        var read = body.IndexOf("DarlingFinOpsApplicationConnectionsReader.GetApplicationConnectionsAsync(", StringComparison.Ordinal);
        var map = body.IndexOf("ApplicationConnectionRow.From(d, clock)", StringComparison.Ordinal);
        Assert.True(cutoff >= 0 && cutoff < clock && clock < read && read < map, "Expected the order: cutoff, clock, reader, mapper.");
    }

    [Fact]
    public void TheSqlAlias_PointsAtTheReaderConstant()
    {
        Assert.Contains(
            "public const string ApplicationConnectionsSql = DarlingFinOpsApplicationConnectionsReader.ApplicationConnectionsSql;",
            ViewerFile("ViewerDataService.FinOps.Workload.cs"));
        Assert.Equal(DarlingFinOpsApplicationConnectionsReader.ApplicationConnectionsSql, ViewerDataService.ApplicationConnectionsSql);
    }
}
