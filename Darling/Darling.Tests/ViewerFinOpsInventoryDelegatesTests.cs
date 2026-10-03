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

/// <summary>
/// The FinOps Inventory reads live once, in <see cref="DarlingFinOpsInventoryReader"/>. The viewer's partial holds no
/// SQL text, calls the reader, and keeps each <c>XSql</c> constant as an alias of the Storage constant so the viewer's
/// SQL pins read the same text.
/// </summary>
public sealed class ViewerFinOpsInventoryDelegatesTests
{
    private static string ViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer",
            "ViewerDataService.FinOps.Inventory.cs")).ReplaceLineEndings("\n");

    [Fact]
    public void TheInventoryPartial_HoldsNoSqlText()
    {
        Assert.DoesNotContain("@\"", ViewerSource(), StringComparison.Ordinal);
        Assert.DoesNotContain("\"\"\"", ViewerSource(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheInventoryPartial_CallsTheStorageReader()
    {
        var source = ViewerSource();
        Assert.Contains("DarlingFinOpsInventoryReader.GetServerMetricsAsync(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingFinOpsInventoryReader.GetServerInventoryAsync(", source, StringComparison.Ordinal);
    }

    [Fact]
    public void EachViewerSqlConstant_EqualsTheStorageConstant()
    {
        Assert.Equal(DarlingFinOpsInventoryReader.ServerMetricsSql, ViewerDataService.ServerMetricsSql);
        Assert.Equal(DarlingFinOpsInventoryReader.ServerInventorySql, ViewerDataService.ServerInventorySql);
    }
}
