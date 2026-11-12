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

namespace Darling.Tests;

/// <summary>
/// What the Wait Stats, Perfmon and Memory Clerks picker charts say when their batched read fails. The chart is cleared before
/// the read, so a failure that says nothing leaves a blank chart that reads as "nothing selected"; the failure reaches the tab's
/// status line instead, named for its picker, as a slicer's does. A run a newer selection has superseded says nothing, since the
/// current run answers for the chart. Pure logic, so no store and no WPF object is needed.
/// </summary>
public sealed class ViewerPickerChartReadFailureTests
{
    [Fact]
    public void TheCurrentRunsFailure_ReachesTheStatusLine_NamedForItsPicker()
    {
        var text = PickerChartReadFailure.StatusText("wait-stats", new TimeoutException("the read missed its deadline"), runGeneration: 3, currentGeneration: 3);

        Assert.Equal("wait-stats picker failed: the read missed its deadline", text);
    }

    [Fact]
    public void ASupersededRunsFailure_SaysNothing()
    {
        Assert.Null(PickerChartReadFailure.StatusText("perfmon", new TimeoutException("stale"), runGeneration: 3, currentGeneration: 4));
    }
}
