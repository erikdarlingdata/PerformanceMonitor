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
/// The FinOps Utilization reads live once, in <see cref="DarlingFinOpsUtilizationReader"/>. The viewer's partial
/// holds no SQL text, calls the reader, and keeps each <c>XSql</c> constant as an alias of the Storage constant so
/// the viewer's SQL pins read the same text.
/// </summary>
public sealed class ViewerFinOpsUtilizationDelegatesTests
{
    private static string ViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer",
            "ViewerDataService.FinOps.Utilization.cs")).ReplaceLineEndings("\n");

    [Fact]
    public void TheUtilizationPartial_HoldsNoSqlText()
    {
        Assert.DoesNotContain("@\"", ViewerSource(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheUtilizationPartial_CallsTheStorageReader()
    {
        var source = ViewerSource();
        Assert.Contains("DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(", source, StringComparison.Ordinal);
    }

    [Fact]
    public void EachViewerSqlConstant_EqualsTheStorageConstant()
    {
        Assert.Equal(DarlingFinOpsUtilizationReader.UtilizationEfficiencySql, ViewerDataService.UtilizationEfficiencySql);
        Assert.Equal(DarlingFinOpsUtilizationReader.ProvisioningTrendSql, ViewerDataService.ProvisioningTrendSql);
        Assert.Equal(DarlingFinOpsUtilizationReader.MemoryGrantEfficiencySql, ViewerDataService.MemoryGrantEfficiencySql);
    }
}
