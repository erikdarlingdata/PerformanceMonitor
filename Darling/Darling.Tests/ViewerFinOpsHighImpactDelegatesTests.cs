/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>Source pins: the viewer's high-impact read stays a thin delegate to the storage reader.</summary>
public sealed class ViewerFinOpsHighImpactDelegatesTests
{
    private static string ViewerFile(string name)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !Directory.Exists(Path.Combine(dir, "PerformanceMonitor.Darling.Viewer")))
            dir = Path.GetDirectoryName(dir);
        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, "PerformanceMonitor.Darling.Viewer", name)).Replace("\r\n", "\n");
    }

    [Fact]
    public void TheViewerPartial_HoldsNoSqlLiteralForTheHighImpactRead()
    {
        var src = ViewerFile("ViewerDataService.FinOps.Workload.cs");
        var start = src.IndexOf("HighImpactQueriesSql", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var tail = src[start..];
        Assert.DoesNotContain("@\"", tail);
        Assert.DoesNotContain("$@\"", tail);
    }

    [Fact]
    public void GetHighImpactQueriesAsync_CallsTheStorageReader()
    {
        var src = ViewerFile("ViewerDataService.FinOps.Workload.cs");
        var at = src.IndexOf("GetHighImpactQueriesAsync(", StringComparison.Ordinal);
        Assert.True(at >= 0);
        Assert.Contains("DarlingFinOpsHighImpactReader.ReadAsync(", src[at..]);
        Assert.Contains("HighImpactQueryRow.From", src[at..]);
    }

    [Fact]
    public void TheSqlAlias_PointsAtTheReaderConstant()
    {
        Assert.Contains(
            "public const string HighImpactQueriesSql = DarlingFinOpsHighImpactReader.HighImpactQueriesSql;",
            ViewerFile("ViewerDataService.FinOps.Workload.cs"));
    }
}
