/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>#7467: the get_finops reply rounds the store's twenty-decimal numerics, prints a zero as 0, and the inventory host OS version has no trailing line break.</summary>
public sealed class FinOpsWireFiguresTests
{
    private static string Json(object value) => JsonSerializer.Serialize(value, McpHelpers.JsonOptions);

    [Fact]
    public void UtilizationFigures_PrintRoundedNotTwentyDecimals()
    {
        /* The values the issue shows from a test server. */
        Assert.Equal("0.934", Json(FinOpsWireFigures.Ratio(0.93362340638876951726m)));
        Assert.Equal("10.4", Json(FinOpsWireFigures.Pct(10.4188665962689194m)));
    }

    [Fact]
    public void MemoryGrantRows_RoundMegabytesAndKeepASmallValueVisible()
    {
        var rows = DarlingMcpFinOpsTools.MemoryGrantRows(
        [
            new MemoryGrantEfficiencyDto(new DateTime(2026, 10, 1), 0.02276923076923076923m, 0.00082051282051282051m,
                3.6m, 0.5m, 3, 0, 0, 0),
        ]);
        var json = Json(rows);
        Assert.Contains("\"avg_granted_mb\":0.02,", json);
        Assert.Contains("\"avg_used_mb\":0.00082,", json);
        Assert.Contains("\"wasted_mb\":0.02,", json);
        Assert.DoesNotContain("0.0227692", json);
    }

    [Fact]
    public void ZeroPrintsAsZero()
    {
        var rows = DarlingMcpFinOpsTools.MemoryGrantRows(
        [
            new MemoryGrantEfficiencyDto(new DateTime(2026, 10, 1), 0.00000000000000000000m, 0.00000000000000000000m,
                0.0m, 0.00m, 0, 0, 0, 0),
        ]);
        var json = Json(rows);
        Assert.Contains("\"avg_granted_mb\":0,", json);
        Assert.Contains("\"avg_used_mb\":0,", json);
        Assert.Contains("\"efficiency_pct\":0,", json);
        Assert.Contains("\"peak_granted_mb\":0,", json);
        Assert.Contains("\"wasted_mb\":0,", json);
        Assert.Equal("0", Json(FinOpsWireFigures.Ratio(0.000m)));
        Assert.Equal("0", Json(FinOpsWireFigures.Pct(0.0000000m)));
    }

    [Theory]
    [InlineData("12.3456", "12.35")]
    [InlineData("0.01", "0.01")]
    [InlineData("0.0096", "0.0096")]
    [InlineData("0.00082051", "0.00082")]
    [InlineData("100.000", "100")]
    [InlineData("-0.00082051", "-0.00082")]
    public void Mb_TwoPlacesAndTwoSignificantDigitsUnderOneHundredth(string input, string expected)
    {
        Assert.Equal(expected, Json(FinOpsWireFigures.Mb(decimal.Parse(input, CultureInfo.InvariantCulture))));
    }

    [Fact]
    public void IdleDatabaseAndTempdbRows_Round()
    {
        var idle = Json(DarlingMcpFinOpsTools.IdleDatabaseRow(new IdleDatabase("d", 1.23456789012345678901m, 2, null)));
        Assert.Contains("\"total_size_mb\":1.23,", idle);
        var tempdb = Json(DarlingMcpFinOpsTools.TempdbRow(new TempdbSummaryMetric("User Objects", 5.00000000000000000000m, 0.00600000000000000000m, "")));
        Assert.Contains("\"current_mb\":5,", tempdb);
        Assert.Contains("\"peak_24h_mb\":0.006,", tempdb);
    }

    [Theory]
    [InlineData("Windows Server 2022\n", "Windows Server 2022")]
    [InlineData("  Windows Server 2022\r\n", "Windows Server 2022")]
    [InlineData("\n", "")]
    [InlineData(null, "")]
    public void HostOsVersion_ComesBackTrimmed(string? raw, string expected)
    {
        Assert.Equal(expected, DarlingFinOpsInventoryReader.CleanHostOsVersion(raw));
    }
}
