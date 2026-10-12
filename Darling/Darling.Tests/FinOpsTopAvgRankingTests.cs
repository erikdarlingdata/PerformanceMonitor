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
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5644 W4: the "Top Databases by Avg CPU / Execution" ranking ran on avg_cpu_ms after Convert.ToInt64 had turned
/// 2.3, 1.95 and 1.56 into 2, so the tie broke on database name and the top-N cut dropped the busiest database.
/// The average now travels unrounded (<c>AvgCpuMs</c>) and is what the ranking and the wire row use.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class FinOpsTopAvgRankingTests
{
    private static string Read(string relative, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", relative)).ReplaceLineEndings("\n");

    [Fact]
    public void TopByAvgRow_CarriesTheUnroundedAverage()
    {
        var row = new TopResourceConsumer("busiest", 2L, 1_066_691L, 10m, 0m, 0m, 2_498_148L, 0.5m, 2.34m);

        var json = JsonSerializer.Serialize(DarlingMcpFinOpsTools.TopByAvgRow(row));

        Assert.Contains("\"avg_cpu_ms\":2.34", json, StringComparison.Ordinal);
    }

    [Fact]
    public void StorageReader_RanksTheAvgGridOnTheUnroundedAverage()
    {
        var source = Read("Darling/PerformanceMonitor.Darling.Storage/FinOps/DarlingFinOpsDatabaseResourcesReader.cs");

        Assert.Contains("avgCandidates.OrderByDescending(r => r.AvgCpuMs)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("avgCandidates.OrderByDescending(r => r.CpuTimeMs)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("new TopResourceConsumer(dbName, Convert.ToInt64(reader.GetValue(6))", source, StringComparison.Ordinal);
    }

    [Fact]
    public void LiteTwin_RanksTheAvgGridOnTheUnroundedAverage()
    {
        var source = Read("Lite/Services/LocalDataService.FinOps.Workload.cs");

        Assert.Contains("avgCandidates.OrderByDescending(r => r.AvgCpuMs)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("avgCandidates.OrderByDescending(r => r.CpuTimeMs)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("CpuTimeMs = ToInt64(reader.GetValue(6))", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Grids_BindTheAverageNotTheRoundedField()
    {
        Assert.Contains("Header=\"Avg CPU (ms)\" Binding=\"{Binding AvgCpuMs", Read("Darling/PerformanceMonitor.Darling.Viewer/FinOpsTab.xaml"), StringComparison.Ordinal);
        Assert.Contains("Header=\"Avg CPU (ms)\" Binding=\"{Binding AvgCpuMs", Read("Lite/Controls/FinOpsTab.xaml"), StringComparison.Ordinal);
        Assert.Contains("{ key: \"avg_cpu_ms\", label: \"Avg CPU (ms)\", format: \"num2\" }",
            Read("Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/finops/utilization.js"), StringComparison.Ordinal);
    }
}
