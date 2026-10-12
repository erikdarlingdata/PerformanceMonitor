/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// FinOpsCost must return the same decimal as the inline expressions the viewer used before the math moved.
/// Each fact writes the old expression out as the reference and compares exactly.
/// </summary>
public sealed class FinOpsCostTests
{
    private static readonly decimal[] Monthlies = { 0m, 1m, 1234.56m, 99999.99m, 0.07m };

    [Theory]
    [InlineData(1)]
    [InlineData(24)]
    [InlineData(168)]
    [InlineData(720)]
    [InlineData(7)]
    public void WindowBudget_MatchesTheInlineExpression_Exactly(int hoursBack)
    {
        foreach (var monthly in Monthlies)
            Assert.Equal(monthly * (hoursBack / 730.0m), FinOpsCost.WindowBudget(monthly, hoursBack));
    }

    [Fact]
    public void WindowBudget_KeepsTheDivisionBeforeTheMultiply()
    {
        // 1234.56 * (1 / 730) and 1234.56 * 1 / 730 differ in the last decimal places.
        Assert.Equal(1234.56m * (1 / 730.0m), FinOpsCost.WindowBudget(1234.56m, 1));
        Assert.NotEqual(1234.56m * 1 / 730.0m, FinOpsCost.WindowBudget(1234.56m, 1));
    }

    [Theory]
    [InlineData(1L, 3L)]
    [InlineData(2L, 3L)]
    [InlineData(7L, 9L)]
    [InlineData(123456789L, 987654321L)]
    public void Share_MatchesTheInlineExpression_Exactly(long part, long total)
    {
        var budget = 1234.56m * (24 / 730.0m);
        Assert.Equal((part / (decimal)total) * budget, FinOpsCost.Share(part, total, budget));
    }

    [Theory]
    [InlineData(1.5, 7.0, 1234.56)]
    [InlineData(10.0, 30.0, 99999.99)]
    [InlineData(0.0, 5.0, 12.34)]
    [InlineData(2.0, 3.0, 0.07)]
    public void StorageShare_MatchesTheInlineExpression_Exactly(double size, double total, double monthly)
    {
        var s = (decimal)size;
        var t = (decimal)total;
        var m = (decimal)monthly;
        Assert.Equal((s / t) * m, FinOpsCost.StorageShare(s, t, m));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1234.56)]
    [InlineData(99999.99)]
    public void Annual_MatchesTheInlineExpression_Exactly(double monthly)
    {
        var m = (decimal)monthly;
        Assert.Equal(m * 12m, FinOpsCost.Annual(m));
    }

    [Fact]
    public void TheLoaders_CallTheHelper_AndHoldNoHoursPerMonthLiteral()
    {
        var tab = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        Assert.DoesNotContain("730", tab, StringComparison.Ordinal);
        Assert.Equal(2, Count(tab, "FinOpsCost.WindowBudget(_server.MonthlyCostUsd, hoursBack)"));
        Assert.Contains("FinOpsCost.Share(w.TotalWaitTimeMs, (decimal)totalWait, windowBudget)", tab, StringComparison.Ordinal);
        Assert.Contains("FinOpsCost.Share(q.TotalCpuMs, (decimal)totalCpu, windowBudget)", tab, StringComparison.Ordinal);
        Assert.Contains("FinOpsCost.StorageShare(d.TotalSizeMb ?? 0m, totalMb, _server.MonthlyCostUsd)", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void AnnualCost_CallsTheHelper_InBothRowModels()
    {
        var rows = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.cs");

        Assert.Equal(2, Count(rows, "AnnualCost => FinOpsCost.Annual(MonthlyCost);"));
        Assert.DoesNotContain("MonthlyCost * 12m", rows, StringComparison.Ordinal);
    }

    private static int Count(string text, string needle)
    {
        var n = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal)) n++;
        return n;
    }
}
