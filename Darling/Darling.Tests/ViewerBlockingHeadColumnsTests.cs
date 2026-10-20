/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 part 4: the head blocker's login time, last request end, open transaction count and transaction-log bytes in the Viewer's
/// Blocking grid. The two times are the monitored server's own clock (stored raw), so they render through the server-clock formatter
/// and move with the display mode; the count and bytes read as the numbers the columns sort on; a NULL is an empty cell, never 0.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerBlockingHeadColumnsTests
{
    private static DataTable DmvTable()
    {
        var table = new DataTable();
        for (var i = 0; i < 26; i++)
        {
            table.Columns.Add("c" + i, i switch
            {
                0 or 1 or 16 or 17 or 22 or 23 => typeof(DateTime),
                7 or 25 => typeof(long),
                3 or 4 or 5 or 6 or 18 or 24 => typeof(int),
                _ => typeof(string),
            });
        }

        return table;
    }

    private static ViewerBlockedProcessRow Map(DataTable table)
    {
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return ViewerDataService.MapDmvRow(reader);
    }

    [Fact]
    public void TheDmvRow_MapsTheFourValuesFromColumns22To25_AndRendersTheTimesOnTheServerClock()
    {
        var table = DmvTable();
        var row = table.NewRow();
        row[0] = new DateTime(2026, 7, 1, 17, 50, 0);
        row[22] = new DateTime(2026, 7, 1, 8, 15, 30);
        row[23] = new DateTime(2026, 7, 1, 13, 45, 7);
        row[24] = 3;
        row[25] = 5L * 1024 * 1024 + 512 * 1024;
        table.Rows.Add(row);

        var mapped = Map(table);

        Assert.Equal(new DateTime(2026, 7, 1, 8, 15, 30), mapped.BlockingLoginTime);
        Assert.Equal(new DateTime(2026, 7, 1, 13, 45, 7), mapped.BlockingLastRequestEndTime);
        Assert.Equal(3, mapped.BlockingOpenTransactionCount);
        Assert.Equal(5L * 1024 * 1024 + 512 * 1024, mapped.BlockingTranLogBytesUsed);
        Assert.Equal("3", mapped.BlockingOpenTransactionCountText);
        Assert.Equal("5.5 MB", mapped.BlockingTranLogBytesUsedText);

        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedOffset = ViewerTimeHelper.UtcOffsetMinutes;
        try
        {
            ViewerTimeHelper.UtcOffsetMinutes = -240;

            /* Server mode shows the stored value as it is; UTC mode is four hours LATER on a -240 server (the stored value is utc + offset). */
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            Assert.Equal("2026-07-01 08:15:30", mapped.BlockingLoginTimeLocal);
            Assert.Equal("2026-07-01 13:45:07", mapped.BlockingLastRequestEndTimeLocal);

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            Assert.Equal("2026-07-01 12:15:30", mapped.BlockingLoginTimeLocal);
            Assert.Equal("2026-07-01 17:45:07", mapped.BlockingLastRequestEndTimeLocal);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    [Fact]
    public void ARowCapturedBeforeTheColumns_ShowsEmptyCells_NotZero()
    {
        var table = DmvTable();
        var row = table.NewRow();
        row[0] = new DateTime(2026, 7, 1, 17, 50, 0);
        table.Rows.Add(row);

        var mapped = Map(table);

        Assert.Null(mapped.BlockingLoginTime);
        Assert.Null(mapped.BlockingLastRequestEndTime);
        Assert.Null(mapped.BlockingOpenTransactionCount);
        Assert.Null(mapped.BlockingTranLogBytesUsed);
        Assert.Equal("", mapped.BlockingLoginTimeLocal);
        Assert.Equal("", mapped.BlockingLastRequestEndTimeLocal);
        Assert.Equal("", mapped.BlockingOpenTransactionCountText);
        Assert.Equal("", mapped.BlockingTranLogBytesUsedText);
    }

    [Fact]
    public void ARealZero_IsShownAsZero_AndAnXeReportRowHasNoneOfTheFour()
    {
        Assert.Equal("0", BlockingHeadEvidenceText.FormatCount(0));
        Assert.Equal("0 B", BlockingHeadEvidenceText.FormatLogBytes(0));
        Assert.Equal("", new ViewerBlockedProcessRow().BlockingTranLogBytesUsedText);
    }

    [Theory]
    [InlineData(512L, "512 B")]
    [InlineData(1024L, "1.0 KB")]
    [InlineData(1536L, "1.5 KB")]
    [InlineData(900L * 1024, "900.0 KB")]
    [InlineData(2L * 1024 * 1024, "2.0 MB")]
    [InlineData(3L * 1024 * 1024 * 1024, "3.0 GB")]
    public void TheLogBytes_UseHumanUnits(long bytes, string expected) =>
        Assert.Equal(expected, BlockingHeadEvidenceText.FormatLogBytes(bytes));

    [Fact]
    public void TheLogBytesColumn_SortsOnTheNumber_NotTheText()
    {
        /* "900.0 KB" sorts after "2.0 MB" as text; the grid's SortMemberPath is the number, so the order is the bytes' order. */
        var small = new ViewerBlockedProcessRow { BlockingTranLogBytesUsed = 900L * 1024 };
        var big = new ViewerBlockedProcessRow { BlockingTranLogBytesUsed = 2L * 1024 * 1024 };

        Assert.True(string.CompareOrdinal(small.BlockingTranLogBytesUsedText, big.BlockingTranLogBytesUsedText) > 0);
        Assert.True(small.BlockingTranLogBytesUsed < big.BlockingTranLogBytesUsed);
    }
}
