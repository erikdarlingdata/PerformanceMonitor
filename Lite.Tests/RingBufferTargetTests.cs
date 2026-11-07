/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8146: the ring_buffer target clause every deadlock, blocked-process and long-query create writes, the clamp of the
/// setting that feeds it, and what the on-prem creates of Lite send.
/// </summary>
public sealed class RingBufferTargetTests
{
    [Fact]
    public void Sql_WithLimit_CarriesBothOptions()
    {
        var sql = RingBufferTarget.Sql(5000);

        Assert.Contains("SET max_memory = 4096, max_events_limit = 5000", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Sql_Null_HasNoLimit()
    {
        var sql = RingBufferTarget.Sql(null);

        Assert.Contains("SET max_memory = 4096", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("max_events_limit", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, 1000)]
    [InlineData(500, 1000)]
    [InlineData(1000, 1000)]
    [InlineData(20000, 10000)]
    [InlineData(4321, 4321)]
    public void Clamp_Bounds(int? configured, int expected)
    {
        Assert.Equal(expected, RingBufferTarget.Clamp(configured));
    }

    [Fact]
    public void OnPremDeadlockCreate_CarriesLimit()
    {
        var sql = RemoteCollectorService.BuildDeadlockCreateSql(7500);

        Assert.Contains("max_events_limit = 7500", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE EVENT SESSION [" + DeadlocksCollector.XeSessionName + "]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPremBlockedProcessCreate_CarriesLimit()
    {
        var sql = RemoteCollectorService.BuildBlockedProcessCreateSql(7500);

        Assert.Contains("max_events_limit = 7500", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE EVENT SESSION [" + BlockedProcessReportCollector.XeSessionName + "]", sql, StringComparison.Ordinal);
    }
}
