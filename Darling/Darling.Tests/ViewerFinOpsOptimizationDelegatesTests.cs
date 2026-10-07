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

/// <summary>Source pins: the viewer's four Optimization reads stay thin delegates to the storage reader.</summary>
public sealed class ViewerFinOpsOptimizationDelegatesTests
{
    private static string ViewerFile(string name)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !Directory.Exists(Path.Combine(dir, "PerformanceMonitor.Darling.Viewer")))
            dir = Path.GetDirectoryName(dir);
        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, "PerformanceMonitor.Darling.Viewer", name)).ReplaceLineEndings("\n");
    }

    private static readonly string[] SqlMarkers =
    {
        "db_activity AS", "v_tempdb_stats", "WITH per_spelling AS", "LEFT(query_text, 200)",
    };

    [Fact]
    public void TheViewerPartials_HoldNoneOfTheFourSqlTexts()
    {
        var combined = ViewerFile("ViewerDataService.FinOps.Storage.cs") + ViewerFile("ViewerDataService.FinOps.Workload.cs");
        foreach (var marker in SqlMarkers)
            Assert.DoesNotContain(marker, combined);
    }

    [Theory]
    [InlineData("ViewerDataService.FinOps.Storage.cs", "GetIdleDatabasesAsync(", "DarlingFinOpsOptimizationReader.GetIdleDatabasesAsync(", "IdleDatabaseRow.From")]
    [InlineData("ViewerDataService.FinOps.Storage.cs", "GetTempdbSummaryAsync(", "DarlingFinOpsOptimizationReader.GetTempdbSummaryAsync(", "TempdbSummaryRow.From")]
    [InlineData("ViewerDataService.FinOps.Workload.cs", "GetWaitCategorySummaryAsync(", "DarlingFinOpsOptimizationReader.GetWaitCategorySummaryAsync(", "WaitCategorySummaryRow.From")]
    [InlineData("ViewerDataService.FinOps.Workload.cs", "GetExpensiveQueriesAsync(", "DarlingFinOpsOptimizationReader.GetExpensiveQueriesAsync(", "ExpensiveQueryRow.From")]
    public void EachRead_CallsTheStorageReaderAndMapsThroughFrom(string file, string method, string call, string map)
    {
        var src = ViewerFile(file);
        var at = src.IndexOf("public async Task<", src.IndexOf(method, StringComparison.Ordinal) - 80, StringComparison.Ordinal);
        Assert.True(at >= 0);
        var tail = src[at..];
        Assert.Contains(call, tail);
        Assert.Contains(map, tail);
        Assert.Contains("ViewerCommandDeadlines.CurrentInteractiveReadSeconds", tail);
    }

    [Fact]
    public void EachViewerConstant_IsTheStorageConstant()
    {
        Assert.Equal(DarlingFinOpsOptimizationReader.IdleDatabasesSql, ViewerDataService.IdleDatabasesSql);
        Assert.Equal(DarlingFinOpsOptimizationReader.TempdbSummarySql, ViewerDataService.TempdbSummarySql);
        Assert.Equal(DarlingFinOpsOptimizationReader.WaitCategorySummarySql, ViewerDataService.WaitCategorySummarySql);
        Assert.Equal(DarlingFinOpsOptimizationReader.ExpensiveQueriesSql, ViewerDataService.ExpensiveQueriesSql);
        var combined = ViewerFile("ViewerDataService.FinOps.Storage.cs") + ViewerFile("ViewerDataService.FinOps.Workload.cs");
        foreach (var name in new[] { "IdleDatabasesSql", "TempdbSummarySql", "WaitCategorySummarySql", "ExpensiveQueriesSql" })
            Assert.Contains($"public const string {name} = DarlingFinOpsOptimizationReader.{name};", combined);
    }
}
