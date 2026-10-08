/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Utilization tab for a server whose last 24 hours hold no CPU sample (a stale server, or one whose CPU collector is off). The
/// read returns 0 for the three CPU figures in that case, and the tab used to print them as 0.00%, 0.00% and 0%, and to list the
/// server's database sizes as though they were current. The figures show a dash and the sizes chart is hidden.
/// </summary>
public sealed class ViewerFinOpsUtilizationNoCpuTests
{
    private static UtilizationEfficiencyRow Row(string provisioningStatus) => new()
    {
        ProvisioningStatus = provisioningStatus,
        AvgCpuPct = 0m,
        P95CpuPct = 0m,
        MaxCpuPct = 0
    };

    [Fact]
    public void NoCpuSample_ShowsDashesForEveryCpuFigure_NotZeros()
    {
        var row = Row("");

        Assert.False(row.HasCpuSample);
        Assert.Equal("-", row.AvgCpuText);
        Assert.Equal("-", row.P95CpuText);
        Assert.Equal("-", row.MaxCpuText);
    }

    [Fact]
    public void ACpuSample_ShowsTheMeasuredFigures_EvenWhenTheyAreZero()
    {
        var row = Row("OVER_PROVISIONED");
        row.AvgCpuPct = 12.5m;
        row.P95CpuPct = 40m;
        row.MaxCpuPct = 77;

        Assert.Equal($"{12.5m:N2}%", row.AvgCpuText);
        Assert.Equal($"{40m:N2}%", row.P95CpuText);
        Assert.Equal("77%", row.MaxCpuText);

        // A measured idle server is a real 0, not a dash.
        var idle = Row("OVER_PROVISIONED");
        Assert.Equal($"{0m:N2}%", idle.AvgCpuText);
        Assert.Equal("0%", idle.MaxCpuText);
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("RIGHT_SIZED", true)]
    [InlineData("NOT_APPLICABLE", true)]
    public void TheSizesChart_IsShownOnlyWhenTheWindowHeldACpuSample(string status, bool shown) =>
        Assert.Equal(shown, Row(status).ShowsDatabaseSizeChart);

    [Fact]
    public void TheTab_PaintsTheTextFromTheRow_EmptiesTheBars_AndHidesTheChart()
    {
        var loaders = Read("FinOpsTab.Loaders.cs", ThisFile());

        Assert.Contains("FinOpsAvgCpuText.Text = data.AvgCpuText;", loaders, StringComparison.Ordinal);
        Assert.Contains("FinOpsP95CpuText.Text = data.P95CpuText;", loaders, StringComparison.Ordinal);
        Assert.Contains("FinOpsMaxCpuText.Text = data.MaxCpuText;", loaders, StringComparison.Ordinal);
        Assert.DoesNotContain("$\"{data.AvgCpuPct:N2}%\"", loaders, StringComparison.Ordinal);
        Assert.Contains("data.HasCpuSample ? (double)data.AvgCpuPct : 0", loaders, StringComparison.Ordinal);
        Assert.Contains("FinOpsDbSizeChartGroup.Visibility = data.ShowsDatabaseSizeChart", loaders, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"FinOpsDbSizeChartGroup\"", Read("FinOpsTab.xaml", ThisFile()), StringComparison.Ordinal);
    }

    private static string Read(string file, string thisFile) =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer", file))
            .ReplaceLineEndings("\n");

    private static string ThisFile([CallerFilePath] string path = "") => path;
}
