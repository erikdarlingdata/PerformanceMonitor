/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Xunit;

namespace Darling.Tests;

/// <summary>The Utilization tab calls the shared figure helpers and holds none of their expressions.</summary>
public sealed class ViewerFinOpsUtilizationFiguresDelegatesTests
{
    private static string Read(string file, string thisFile) =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer", file))
            .ReplaceLineEndings("\n");

    [Fact]
    public void TheTab_CallsTheHelpers_AndHoldsNoMovedExpression()
    {
        var source = Read("FinOpsTab.Loaders.cs", ThisFile());
        Assert.Contains("FinOpsUtilizationFigures.StolenMemoryPct(", source, StringComparison.Ordinal);
        Assert.Contains("FinOpsUtilizationFigures.BufferPoolPct(", source, StringComparison.Ordinal);
        Assert.Contains("FinOpsUtilizationFigures.FreeSpacePct(", source, StringComparison.Ordinal);
        Assert.Contains("FinOpsUtilizationFigures.Explanation(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("TotalMemoryMb - data.BufferPoolMb", source, StringComparison.Ordinal);
        Assert.DoesNotContain("(double)data.BufferPoolMb / data.PhysicalMemoryMb", source, StringComparison.Ordinal);
        Assert.DoesNotContain("totalFreeMb / totalStorageMb", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ServerHardwareScope.RightSizedExplanation(", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRow_ComputesItsHealthScoreThroughTheHelper()
    {
        var source = Read("ViewerDataService.FinOps.cs", ThisFile());
        Assert.Contains("FinOpsUtilizationFigures.HealthScore(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("FinOpsHealthCalculator.Overall(", source, StringComparison.Ordinal);
    }

    private static string ThisFile([System.Runtime.CompilerServices.CallerFilePath] string f = "") => f;
}
