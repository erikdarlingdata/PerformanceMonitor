/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8146: <c>xeRingBufferMaxEvents</c> in darling.json, the clamp the service applies once at load, and what the on-prem
/// creates of the deadlock and blocked-process sessions send.
/// </summary>
public sealed class XeRingBufferMaxEventsTests
{
    [Fact]
    public void XeRingBufferMaxEvents_ClampsAndLogs()
    {
        var logger = new CapturingTestLogger();

        Assert.Null(new DarlingConfig().XeRingBufferMaxEvents);
        Assert.Equal(1000, StoreConfigProvider.ClampXeRingBufferMaxEvents(new DarlingConfig().XeRingBufferMaxEvents, logger));
        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

        Assert.Equal(1000, StoreConfigProvider.ClampXeRingBufferMaxEvents(50, logger));
        Assert.Contains("xeRingBufferMaxEvents", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("50", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("1000", logger.Joined, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1000, 1000)]
    [InlineData(4321, 4321)]
    [InlineData(10000, 10000)]
    [InlineData(50000, 10000)]
    public void XeRingBufferMaxEvents_InRangeIsKept_AboveRangeIsCapped(int configured, int expected)
    {
        Assert.Equal(expected, StoreConfigProvider.ClampXeRingBufferMaxEvents(configured, logger: null));
    }

    /* #8146: darling.json takes any JSON whole number, and a value too large for an int lands on the nearest bound, as Lite's does,
       instead of failing the file's bind. */
    [Theory]
    [InlineData("{ \"xeRingBufferMaxEvents\": 20000000000 }", 10000)]
    [InlineData("{ \"xeRingBufferMaxEvents\": -5 }", 1000)]
    [InlineData("{ }", 1000)]
    public void XeRingBufferMaxEvents_FromDarlingJson_ClampsAnyWholeNumber(string json, int expected)
    {
        var config = DarlingConfig.Parse(json);

        Assert.Equal(expected, StoreConfigProvider.ClampXeRingBufferMaxEvents(config.XeRingBufferMaxEvents, logger: null));
    }

    [Fact]
    public void XeRingBufferMaxEvents_TooLargeForAnInt_LogsTheValueGiven()
    {
        var logger = new CapturingTestLogger();

        Assert.Equal(10000, StoreConfigProvider.ClampXeRingBufferMaxEvents(DarlingConfig.Parse("{ \"xeRingBufferMaxEvents\": 20000000000 }").XeRingBufferMaxEvents, logger));
        Assert.Contains("20000000000", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("10000", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void XeRingBufferMaxEvents_InRange_LogsNothing()
    {
        var logger = new CapturingTestLogger();

        Assert.Equal(2000, StoreConfigProvider.ClampXeRingBufferMaxEvents(2000, logger));

        Assert.Equal(0, logger.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));
    }

    [Fact]
    public void OnPremDeadlockCreate_CarriesLimit()
    {
        var sql = DarlingXeSessions.BuildDeadlockOnPremCreateSql(7500);

        Assert.Contains("max_events_limit = 7500", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE EVENT SESSION [" + DeadlocksCollector.XeSessionName + "]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPremBlockedProcessCreate_CarriesLimit()
    {
        var sql = DarlingXeSessions.BuildBlockedProcessOnPremCreateSql(7500);

        Assert.Contains("max_events_limit = 7500", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE EVENT SESSION [" + BlockedProcessReportCollector.XeSessionName + "]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRunnerHoldsTheClampedSetting()
    {
        using var store = Npgsql.NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");
        var runner = new DarlingCollectorRunner(
            store,
            new CollectorDeltaCalculator(),
            xeRingBufferMaxEvents: 3000);

        Assert.Equal(3000, runner.RingBufferMaxEvents);
    }
}
