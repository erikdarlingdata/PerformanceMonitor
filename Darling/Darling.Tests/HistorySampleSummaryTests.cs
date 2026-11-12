/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>#8540: the opening of a history window's summary line, shared by the six history windows of Lite and the Viewer.</summary>
public sealed class HistorySampleSummaryTests
{
    [Fact]
    public void OneSample_IsSingular_AndNamesItsOneTime() =>
        Assert.Equal("1 sample at 11/08 12:40", HistorySampleSummary.Span(1, "11/08 12:40", "11/08 12:40"));

    [Theory]
    [InlineData(0, "0 samples from 11/08 12:40 to 11/08 13:10")]
    [InlineData(2, "2 samples from 11/08 12:40 to 11/08 13:10")]
    [InlineData(48, "48 samples from 11/08 12:40 to 11/08 13:10")]
    public void AnyOtherCount_IsPlural_AndNamesTheSpan(int count, string expected) =>
        Assert.Equal(expected, HistorySampleSummary.Span(count, "11/08 12:40", "11/08 13:10"));
}
