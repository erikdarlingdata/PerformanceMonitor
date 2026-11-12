/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// What Lite's Wait Stats, Memory Clerks and Perfmon picker charts say when their batched read fails, through the shared
/// <see cref="PickerChartReadFailure"/>. The chart is cleared before the read, so a failure that says nothing leaves a blank
/// chart that reads as "nothing selected"; the failure reaches the tab's status line instead, named for its picker. A run a
/// newer selection has superseded says nothing, since the current run answers for the chart.
/// </summary>
public sealed class PickerChartReadFailureTests
{
    [Fact]
    public void TheCurrentRunsFailure_ReachesTheStatusLine_NamedForItsPicker()
    {
        var text = PickerChartReadFailure.StatusText("memory-clerks", new TimeoutException("the read missed its deadline"), runGeneration: 2, currentGeneration: 2);

        Assert.Equal("memory-clerks picker failed: the read missed its deadline", text);
    }

    [Fact]
    public void ASupersededRunsFailure_SaysNothing()
    {
        Assert.Null(PickerChartReadFailure.StatusText("wait-stats", new TimeoutException("stale"), runGeneration: 2, currentGeneration: 3));
    }
}
