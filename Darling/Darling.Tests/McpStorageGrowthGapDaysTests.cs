/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8531: the storage growth object rows the MCP tool and the web page share keep a place for a day with no sample. An object
/// sampled on 10/30 and 11/3 has five days and five cells, the three between them empty, so a collection gap reads as a gap and
/// not as two neighbouring days.
/// </summary>
public sealed class McpStorageGrowthGapDaysTests
{
    [Fact]
    public void ADayWithNoSample_KeepsItsPlaceInTheDays_WithEmptyCells()
    {
        var ranked = new List<ObjectSizeGrowthDto> { new("dbo", "Orders", 150m, 120m, 1000L, 2, 30m, 1m, 25m) };
        var samples = new[]
        {
            new FinOpsObjectDaySample("dbo.Orders", new DateTime(2026, 10, 30), 120),
            new FinOpsObjectDaySample("dbo.Orders", new DateTime(2026, 11, 3), 150),
        };

        var (days, rows) = DarlingMcpFinOpsTools.StorageGrowthObjectRows(ranked, samples);

        Assert.Equal(5, days.Count);
        using var row = JsonDocument.Parse(JsonSerializer.Serialize(Assert.Single(rows), McpHelpers.JsonOptions));
        var cells = row.RootElement.GetProperty("cells");
        Assert.Equal(5, cells.GetArrayLength());
        Assert.Equal(JsonValueKind.Number, cells[0][0].ValueKind);
        for (var gap = 1; gap <= 3; gap++)
        {
            Assert.Equal(JsonValueKind.Null, cells[gap][0].ValueKind);
        }

        Assert.Equal(JsonValueKind.Number, cells[4][0].ValueKind);
    }

    /// <summary>The hover's rule for a blank cell: a day no object has a sample on is a gap day, and one object's blank day is not.</summary>
    [Fact]
    public void ADayNoObjectHasASampleOn_IsAGapDay_AndOneObjectsBlankDayIsNot()
    {
        var matrix = FinOpsHeatmapBuilder.WithEveryCalendarDay(FinOpsHeatmapBuilder.BuildMatrix(new[] { "a", "b" }, new[]
        {
            new FinOpsObjectDaySample("a", new DateTime(2026, 10, 30), 5),
            new FinOpsObjectDaySample("b", new DateTime(2026, 10, 31), 7),
            new FinOpsObjectDaySample("a", new DateTime(2026, 11, 2), 6),
        }));

        Assert.Equal(4, matrix.Days.Length);
        Assert.False(FinOpsHeatmapBuilder.IsGapDay(matrix, 0));
        Assert.False(FinOpsHeatmapBuilder.IsGapDay(matrix, 1));
        Assert.True(FinOpsHeatmapBuilder.IsGapDay(matrix, 2));
        Assert.False(FinOpsHeatmapBuilder.IsGapDay(matrix, 3));
        Assert.False(FinOpsHeatmapBuilder.IsGapDay(matrix, 9));
    }
}
