/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5632 part 4: the head blocker's four values in Lite's Blocking grid. The two times are the monitored server's own clock, so they
/// take the server-clock renderer; a NULL (an older row, or an XE report row) is an empty cell and never 0; the log bytes show in
/// human units while the column sorts on the number.
/// </summary>
[Collection("server-time-helper")]
public sealed class BlockingHeadColumnsRowTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ServerTimeHelper.CurrentDisplayMode = _savedMode;

    [Fact]
    public void TheFourValues_RenderAsTheServerClockCountAndHumanBytes()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var row = new BlockedProcessReportRow
        {
            BlockingLoginTime = new DateTime(2026, 7, 1, 8, 15, 30),
            BlockingLastRequestEndTime = new DateTime(2026, 7, 1, 13, 45, 7),
            BlockingOpenTransactionCount = 2,
            BlockingTranLogBytesUsed = 5L * 1024 * 1024 + 512 * 1024,
        };

        Assert.Equal("2026-07-01 08:15:30", row.BlockingLoginTimeLocal);
        Assert.Equal("2026-07-01 13:45:07", row.BlockingLastRequestEndTimeLocal);
        Assert.Equal("2", row.BlockingOpenTransactionCountText);
        Assert.Equal("5.5 MB", row.BlockingTranLogBytesUsedText);
    }

    [Fact]
    public void AFoldedDmvRow_FillsTheOpenTransactionCountAndLastTranStartOnlyWhereTheReportRowHasNone()
    {
        var t = new DateTime(2026, 7, 1, 12, 0, 0);
        var own = new BlockedProcessReportRow
        {
            Source = PerformanceMonitor.Alerting.BlockedProcessAlertRow.XeReportSource, EventTime = t, BlockedSpid = 51, BlockingSpid = 60,
            BlockingOpenTransactionCount = 1, BlockingLastTranStarted = t.AddHours(-1),
        };
        var bare = new BlockedProcessReportRow
        {
            Source = PerformanceMonitor.Alerting.BlockedProcessAlertRow.XeReportSource, EventTime = t, BlockedSpid = 52, BlockingSpid = 61,
        };
        BlockedProcessReportRow Dmv(int blocked, int blocking, int count) => new()
        {
            Source = PerformanceMonitor.Alerting.BlockedProcessAlertRow.DmvSnapshotSource, EventTime = t.AddSeconds(20), BlockedSpid = blocked, BlockingSpid = blocking,
            BlockingOpenTransactionCount = count, BlockingLastTranStarted = t.AddHours(-3),
        };
        var items = new System.Collections.Generic.List<BlockedProcessReportRow> { own, bare };

        PerformanceMonitor.Alerting.BlockedProcessReportMerge.AppendDmvFallbackRows(items,
            new System.Collections.Generic.List<BlockedProcessReportRow> { Dmv(51, 60, 5), Dmv(52, 61, 3) });

        Assert.Equal(1, own.BlockingOpenTransactionCount);
        Assert.Equal(t.AddHours(-1), own.BlockingLastTranStarted);
        Assert.Equal(3, bare.BlockingOpenTransactionCount);
        Assert.Equal(t.AddHours(-3), bare.BlockingLastTranStarted);
    }

    [Fact]
    public void ANullIsAnEmptyCell_AndARealZeroIsZero()
    {
        var empty = new BlockedProcessReportRow();

        Assert.Equal("", empty.BlockingLoginTimeLocal);
        Assert.Equal("", empty.BlockingLastRequestEndTimeLocal);
        Assert.Equal("", empty.BlockingOpenTransactionCountText);
        Assert.Equal("", empty.BlockingTranLogBytesUsedText);

        var zero = new BlockedProcessReportRow { BlockingOpenTransactionCount = 0, BlockingTranLogBytesUsed = 0 };

        Assert.Equal("0", zero.BlockingOpenTransactionCountText);
        Assert.Equal("0 B", zero.BlockingTranLogBytesUsedText);
    }
}
