/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared "enough history for recommendations" rule: the threshold edges, the wording, and that the
/// Darling analysis service reads the same constant Lite's service and Recommendations tab read.
/// </summary>
public sealed class AnalysisHistoryGateTests
{
    [Fact]
    public void Threshold_IsTwentyFourHours() =>
        Assert.Equal(24.0, AnalysisHistoryGate.MinimumDataHours);

    [Theory]
    [InlineData(0.0, false)]
    [InlineData(4.0, false)]
    [InlineData(23.99, false)]
    [InlineData(24.0, true)]
    [InlineData(24.01, true)]
    [InlineData(500.0, true)]
    public void HasEnoughHistory_FlipsAtTheThreshold(double hours, bool expected) =>
        Assert.Equal(expected, AnalysisHistoryGate.HasEnoughHistory(hours));

    [Fact]
    public void HasEnoughHistory_HonoursAnExplicitMinimum()
    {
        Assert.False(AnalysisHistoryGate.HasEnoughHistory(30, 72));
        Assert.True(AnalysisHistoryGate.HasEnoughHistory(72, 72));
        Assert.True(AnalysisHistoryGate.HasEnoughHistory(0, 0));
    }

    [Fact]
    public void Message_UsesTheWordingBothAppsShow()
    {
        Assert.Equal(
            "Not enough data for reliable analysis. Need 1.0 days of collected data, have 4.0 hours. Keep the collector running and try again later.",
            AnalysisHistoryGate.InsufficientDataMessage(4.0));
        Assert.Equal(
            "Not enough data for reliable analysis. Need 3.0 days of collected data, have 1.3 days. Keep the collector running and try again later.",
            AnalysisHistoryGate.InsufficientDataMessage(31.2, 72));
        Assert.StartsWith("Not enough data for reliable analysis. Need 6 hours of collected data, have 0.0 hours.",
            AnalysisHistoryGate.InsufficientDataMessage(0, 6));
    }

    [Fact]
    public void DarlingService_DefaultsToTheSharedMinimum()
    {
        using var neverOpened = NpgsqlDataSource.Create("Host=localhost;Database=never-opened");
        var service = new DarlingAnalysisService(neverOpened);
        Assert.Equal(AnalysisHistoryGate.MinimumDataHours, service.MinimumDataHours);
    }
}
