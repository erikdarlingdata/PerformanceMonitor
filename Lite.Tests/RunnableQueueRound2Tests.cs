/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>Review round 2 of #5709 (L2): the Runnable Queue card's CPU window is capped at the lookback Lite reads its
/// CPU samples with, so the heading never names a longer window than the samples cover.</summary>
public sealed class RunnableQueueRound2Tests
{
    [Fact]
    public void TheCpuWindowCap_IsTheLookbackLiteReadsItsCpuSamplesWith()
    {
        Assert.Equal(AlertContextBuilders.RunnableQueueCpuSampleLookbackMinutes, LocalDataService.AlertCpuSampleLookback.TotalMinutes);
    }
}
