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
public sealed class ViewerFinOpsStorageGrowthDelegatesTests
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
        "WITH log_service_files AS", "JOIN ranked r ON", "COALESCE(user_seeks", "FROM v_index_object_stats",
    };

    [Fact]
    public void TheViewerPartial_HoldsNoneOfTheMovedSqlTexts()
    {
        var src = ViewerFile("ViewerDataService.FinOps.Storage.cs");
        foreach (var marker in SqlMarkers)
            Assert.DoesNotContain(marker, src);
    }

    [Theory]
    [InlineData("GetStorageGrowthAsync(", "DarlingFinOpsStorageGrowthReader.GetStorageGrowthAsync(", "StorageGrowthRow.From")]
    [InlineData("GetObjectGrowthHeatmapDataAsync(", "DarlingFinOpsStorageGrowthReader.GetObjectGrowthHeatmapDataAsync(", "ObjectSizeGrowthRow.From")]
    [InlineData("GetObjectIndexDetailAsync(", "DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(", "IndexUsageRow.From")]
    public void EachRead_CallsTheStorageReaderAndMapsThroughFrom(string method, string call, string map)
    {
        var src = ViewerFile("ViewerDataService.FinOps.Storage.cs");
        var at = src.LastIndexOf("public async Task<", src.IndexOf("> " + method, StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.True(at >= 0);
        var tail = src[at..];
        Assert.Contains(call, tail);
        Assert.Contains(map, tail);
        Assert.Contains("ViewerCommandDeadlines.CurrentInteractiveReadSeconds", tail);
    }

    [Fact]
    public void TheStayingDatabaseSizeReads_CallTheStorageProbe()
    {
        var src = ViewerFile("ViewerDataService.FinOps.Storage.cs");
        foreach (var method in new[] { "GetDatabaseSizeLatestAsync(", "GetDatabaseSizeSummaryAsync(" })
        {
            var at = src.LastIndexOf("public async Task<", src.IndexOf("Task<", src.IndexOf(method, StringComparison.Ordinal) - 140, StringComparison.Ordinal) + 5, StringComparison.Ordinal);
            var end = src.IndexOf("public async Task<", at + 10, StringComparison.Ordinal);
            var body = end < 0 ? src[at..] : src[at..end];
            Assert.Contains("DarlingFinOpsStorageGrowthReader.GetLatestDatabaseSizeSnapshotAsync(", body);
        }
        Assert.DoesNotContain("private async Task<DateTime?>", src);
        Assert.DoesNotContain("TimestampOrNull", src);
    }

    [Fact]
    public void EachViewerConstant_IsTheStorageConstant()
    {
        Assert.Equal(DarlingFinOpsStorageGrowthReader.StorageGrowthSql, ViewerDataService.StorageGrowthSql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.ObjectGrowthBoundsSql, ViewerDataService.ObjectGrowthBoundsSql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.ObjectGrowthSummarySql, ViewerDataService.ObjectGrowthSummarySql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.ObjectGrowthSeriesSql, ViewerDataService.ObjectGrowthSeriesSql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.ObjectIndexDetailSql, ViewerDataService.ObjectIndexDetailSql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.DatabaseSizeSnapshotWindowedProbeSql, ViewerDataService.DatabaseSizeSnapshotWindowedProbeSql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.DatabaseSizeSnapshotFallbackProbeSql, ViewerDataService.DatabaseSizeSnapshotFallbackProbeSql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.DatabaseSizeLatestSnapshotWindowedProbeSql, ViewerDataService.DatabaseSizeLatestSnapshotWindowedProbeSql);
        Assert.Equal(DarlingFinOpsStorageGrowthReader.DatabaseSizeLatestSnapshotFallbackProbeSql, ViewerDataService.DatabaseSizeLatestSnapshotFallbackProbeSql);
        var src = ViewerFile("ViewerDataService.FinOps.Storage.cs");
        foreach (var name in new[] { "StorageGrowthSql", "ObjectGrowthBoundsSql", "ObjectGrowthSummarySql", "ObjectGrowthSeriesSql", "ObjectIndexDetailSql",
                     "DatabaseSizeSnapshotWindowedProbeSql", "DatabaseSizeSnapshotFallbackProbeSql", "DatabaseSizeLatestSnapshotWindowedProbeSql", "DatabaseSizeLatestSnapshotFallbackProbeSql" })
            Assert.Contains($"public const string {name} = DarlingFinOpsStorageGrowthReader.{name};", src);
    }
}
