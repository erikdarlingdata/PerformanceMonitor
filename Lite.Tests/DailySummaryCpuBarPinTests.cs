/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3539 A2: Lite's daily aggregate counts "high-CPU samples" against a SQL literal, and that literal is the
/// card band's Warning bar (<see cref="ServerHealthThresholds.CpuWarningPercent"/>) restated — deliberately
/// NOT the alert engine's configurable CPU threshold, because the statement re-counts at read time and a knob
/// would recolour every past day the moment it moved. The SQL is a private constant, so this pins the source
/// text; Darling's twin pins <c>DailySummarySql.RangeSql</c> directly in <c>ViewerDailyHealthTests</c>.
/// Also pins that the #3539 <c>collection_runs</c> column is the TRAILING projection, so the eleven positional
/// reads before it stayed put.
/// </summary>
public sealed class DailySummaryCpuBarPinTests
{
    [Fact]
    public void TheHighCpuLiteral_IsTheCardBandsWarningBar_AndCollectionRunsTrails()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Services", "LocalDataService.DailySummary.cs"));

        Assert.Equal(80.0, ServerHealthThresholds.CpuWarningPercent);
        Assert.Contains(
            "(sqlserver_cpu_utilization + COALESCE(other_process_cpu_utilization, 0)) >= "
            + ServerHealthThresholds.CpuWarningPercent.ToString("0", CultureInfo.InvariantCulture) + ")",
            source, StringComparison.Ordinal);

        Assert.Contains("COALESCE(cl.runs, 0) AS collection_runs", source, StringComparison.Ordinal);
        Assert.True(
            source.IndexOf("AS peak_block_wait_ms", StringComparison.Ordinal) < source.IndexOf("AS collection_runs", StringComparison.Ordinal),
            "collection_runs must be the trailing column so the eleven positional reads before it stay put");
        Assert.Contains("CollectionRuns = reader.IsDBNull(12)", source, StringComparison.Ordinal);
        Assert.Contains("PeakBlockWaitMs = MaxBlockDurationMs", source, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"could not locate the repo root walking up from {thisFile}");
    }
}
