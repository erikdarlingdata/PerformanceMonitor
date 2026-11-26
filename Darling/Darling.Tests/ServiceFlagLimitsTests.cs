/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The shared service-flag ranges (#6393) at each bound: the Settings window and update_service_flags both ask this class.</summary>
public sealed class ServiceFlagLimitsTests
{
    [Theory]
    [InlineData(3, false)]
    [InlineData(4, true)]
    [InlineData(256, true)]
    [InlineData(257, false)]
    public void TextBudgetMb_BoundsAreFourAndTwoFiftySix(int value, bool valid) =>
        Assert.Equal(valid, ServiceFlagLimits.TextBudgetMbError(value) is null);

    [Theory]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(16, true)]
    [InlineData(17, false)]
    public void ConcurrentSweeps_BoundsAreOneAndSixteen(int value, bool valid) =>
        Assert.Equal(valid, ServiceFlagLimits.ConcurrentSweepsError(value) is null);

    [Theory]
    [InlineData(-1, false)]
    [InlineData(0, true)]
    [InlineData(100000, true)]
    [InlineData(100001, false)]
    public void FetchTopN_BoundsAreZeroAndOneHundredThousand(int value, bool valid) =>
        Assert.Equal(valid, ServiceFlagLimits.FetchTopNError(value) is null);

    [Theory]
    [InlineData(-1, false)]
    [InlineData(0, true)]
    [InlineData(6, false)]
    [InlineData(7, true)]
    [InlineData(365, true)]
    [InlineData(366, false)]
    public void PlanContentRetentionDays_AreOffOrSevenToThreeHundredSixtyFive(int value, bool valid) =>
        Assert.Equal(valid, ServiceFlagLimits.PlanContentRetentionDaysError(value) is null);

    [Theory]
    [InlineData(0, false)]
    [InlineData(4, false)]
    [InlineData(5, true)]
    [InlineData(600, true)]
    [InlineData(601, false)]
    public void ComposeStatementTimeoutSeconds_BoundsAreFiveAndSixHundred(int value, bool valid) =>
        Assert.Equal(valid, ServiceFlagLimits.ComposeStatementTimeoutSecondsError(value) is null);

    [Theory]
    [InlineData("gzip", "gzip")]
    [InlineData(" None ", "none")]
    [InlineData("GZIP", "gzip")]
    [InlineData("zstd", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void PlanXmlCompression_IsGzipOrNone_AnyCase_AndNothingElse(string? mode, string? stored)
    {
        Assert.Equal(stored, ServiceFlagLimits.NormalizePlanXmlCompression(mode));
        Assert.Equal(stored is null, ServiceFlagLimits.PlanXmlCompressionError(mode) is not null);
    }

    [Fact]
    public void TheRefusalTextNamesTheRange()
    {
        Assert.Contains("between 4 and 256 MB", ServiceFlagLimits.TextBudgetMbError(3), StringComparison.Ordinal);
        Assert.Contains("between 1 and 16", ServiceFlagLimits.ConcurrentSweepsError(0), StringComparison.Ordinal);
        Assert.Contains("between 0 and 100000", ServiceFlagLimits.FetchTopNError(-1), StringComparison.Ordinal);
    }
}
