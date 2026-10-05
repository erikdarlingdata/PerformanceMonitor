/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.McpServerClockTestSupport;

namespace Darling.Tests;

/// <summary>
/// #4793: the blocked-process readers no longer subtract the ONE newest offset in SQL. The six transaction and
/// batch stamps come back as the server's own wall clock and are converted in C# with the server's
/// <see cref="ServerClock"/>, so a stamp on either side of a daylight saving change lands at its real UTC time.
/// <c>event_time</c> is already UTC on both arms and is left alone. US Eastern springs forward on 2026-03-08
/// and falls back on 2026-11-01.
/// </summary>
public sealed class DarlingBlockingReaderServerClockTests
{
    /* The XE select is 37 columns wide: event_time first, the six server-local stamps at 25..30, and the two plan flags last (35, 36; #5236). */
    private static readonly int[] XeStamps = [25, 26, 27, 28, 29, 30];

    /* The DMV select is 20 columns wide: event_time first, the two server-local stamps last. */
    private static readonly int[] DmvStamps = [18, 19];

    private static DataTable XeTable() => Table(37, [0, .. XeStamps]);

    private static DataTable DmvTable() => Table(20, [0, .. DmvStamps]);

    private static DarlingBlockingReader.BlockedProcessReadRow ReadXe(DataTable table, ServerClock clock)
    {
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return DarlingBlockingReader.MapXeRow(reader, clock);
    }

    private static DarlingBlockingReader.BlockedProcessReadRow ReadDmv(DataTable table, ServerClock clock)
    {
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return DarlingBlockingReader.MapDmvRow(reader, clock);
    }

    /// <summary>A report row whose six stamps are stored at 08:01 to 08:13 server-local on the given day.</summary>
    private static DataTable XeRowOn(int year, int month, int day)
    {
        var table = XeTable();
        AddRow(table,
            (0, Naive(year, month, day, 15)),
            (25, Naive(year, month, day, 8, 1)), (26, Naive(year, month, day, 8, 11)),
            (27, Naive(year, month, day, 8, 2)), (28, Naive(year, month, day, 8, 12)),
            (29, Naive(year, month, day, 8, 3)), (30, Naive(year, month, day, 8, 13)));
        return table;
    }

    [Theory]
    [InlineData(2026, 3, 7, 13)]   // standard time (-5 h), before the spring forward
    [InlineData(2026, 3, 9, 12)]   // daylight time (-4 h), after it
    [InlineData(2026, 10, 31, 12)] // daylight time, before the fall back
    [InlineData(2026, 11, 2, 13)]  // standard time, after it
    public void ABlockedProcessReport_WithStampsOnEachSideOfAChange_LandsAtTheRightUtcTimes(
        int year, int month, int day, int expectedUtcHour)
    {
        /* The newest snapshot says -240 (daylight time). One subtracted offset puts the two winter days an
           hour off; the zone gets all four right. */
        var row = ReadXe(XeRowOn(year, month, day), Eastern(snapshotOffsetMinutes: -240));

        Assert.Equal(Naive(year, month, day, expectedUtcHour, 1), row.BlockedLastTranStartedUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 11), row.BlockingLastTranStartedUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 2), row.BlockedLastBatchStartedUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 12), row.BlockingLastBatchStartedUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 3), row.BlockedLastBatchCompletedUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 13), row.BlockingLastBatchCompletedUtc);
    }

    [Theory]
    [InlineData(2026, 3, 7, 13)]
    [InlineData(2026, 3, 9, 12)]
    [InlineData(2026, 10, 31, 12)]
    [InlineData(2026, 11, 2, 13)]
    public void ADmvBlockingSnapshot_WithStampsOnEachSideOfAChange_LandsAtTheRightUtcTimes(
        int year, int month, int day, int expectedUtcHour)
    {
        var table = DmvTable();
        AddRow(table,
            (0, Naive(year, month, day, 15)),
            (18, Naive(year, month, day, 8, 1)), (19, Naive(year, month, day, 8, 11)));

        var row = ReadDmv(table, Eastern(snapshotOffsetMinutes: -300));

        Assert.Equal(Naive(year, month, day, expectedUtcHour, 1), row.BlockedLastTranStartedUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 11), row.BlockingLastTranStartedUtc);
    }

    /// <summary>
    /// #5236: the two plan-presence flags are the XE select's LAST columns (ordinals 35 and 36, blocked then blocking) and map to
    /// <c>HasBlockedPlan</c> / <c>HasBlockingPlan</c>. A NULL flag reads false, and a DMV-snapshot row (its select has no such
    /// columns) leaves both false.
    /// </summary>
    [Fact]
    public void MapXeRow_ReadsThePlanFlags_AtOrdinals35And36()
    {
        static DataTable Flags(params (int Ordinal, object Value)[] flags)
        {
            var table = Table(35, [0, .. XeStamps]);
            table.Columns.Add("c35", typeof(bool));
            table.Columns.Add("c36", typeof(bool));
            AddRow(table, [(0, Naive(2026, 11, 2, 15)), .. flags]);
            return table;
        }

        var both = ReadXe(Flags((35, true), (36, true)), Eastern());
        Assert.True(both.HasBlockedPlan);
        Assert.True(both.HasBlockingPlan);

        var blockedOnly = ReadXe(Flags((35, true), (36, false)), Eastern());
        Assert.True(blockedOnly.HasBlockedPlan);
        Assert.False(blockedOnly.HasBlockingPlan);

        var blockingOnly = ReadXe(Flags((35, false), (36, true)), Eastern());
        Assert.False(blockingOnly.HasBlockedPlan);
        Assert.True(blockingOnly.HasBlockingPlan);

        var nulls = ReadXe(Flags(), Eastern());
        Assert.False(nulls.HasBlockedPlan);
        Assert.False(nulls.HasBlockingPlan);

        var dmvTable = DmvTable();
        AddRow(dmvTable, (0, Naive(2026, 3, 9, 15)));
        var dmv = ReadDmv(dmvTable, Eastern());
        Assert.False(dmv.HasBlockedPlan);
        Assert.False(dmv.HasBlockingPlan);
    }

    [Fact]
    public void TheEventTime_IsAlreadyUtcOnBothArms_AndIsNotConverted()
    {
        var xe = ReadXe(XeRowOn(2026, 11, 2), Eastern());
        Assert.Equal(Naive(2026, 11, 2, 15), xe.EventTime);

        var table = DmvTable();
        AddRow(table, (0, Naive(2026, 3, 9, 15)));
        Assert.Equal(Naive(2026, 3, 9, 15), ReadDmv(table, Eastern()).EventTime);
    }

    [Fact]
    public void AStampThatIsNull_StaysNull()
    {
        var xeTable = XeTable();
        AddRow(xeTable, (0, Naive(2026, 11, 2, 15)));
        var xe = ReadXe(xeTable, Eastern());

        Assert.Null(xe.BlockedLastTranStartedUtc);
        Assert.Null(xe.BlockingLastTranStartedUtc);
        Assert.Null(xe.BlockedLastBatchStartedUtc);
        Assert.Null(xe.BlockingLastBatchStartedUtc);
        Assert.Null(xe.BlockedLastBatchCompletedUtc);
        Assert.Null(xe.BlockingLastBatchCompletedUtc);

        var dmvTable = DmvTable();
        AddRow(dmvTable, (0, Naive(2026, 11, 2, 15)));
        var dmv = ReadDmv(dmvTable, Eastern());

        Assert.Null(dmv.BlockedLastTranStartedUtc);
        Assert.Null(dmv.BlockingLastTranStartedUtc);
    }

    [Fact]
    public void AStampInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var table = XeTable();
        AddRow(table,
            (0, Naive(2026, 3, 8, 12)),
            (25, Naive(2026, 3, 8, 2, 30)),   // skipped by the spring forward
            (26, Naive(2026, 11, 1, 1, 30))); // repeated by the fall back

        var row = ReadXe(table, Eastern());

        /* A skipped time moves forward by the gap (03:30 daylight time); a repeated time takes the first
           occurrence (the daylight offset). */
        Assert.Equal(Naive(2026, 3, 8, 7, 30), row.BlockedLastTranStartedUtc);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), row.BlockingLastTranStartedUtc);
    }

    [Fact]
    public void AFixedOffsetServerAndAUtcServer_ConvertWithoutAZone()
    {
        var fixedRow = ReadXe(XeRowOn(2026, 11, 2), ServerClock.FixedOffset(-240));
        Assert.Equal(Naive(2026, 11, 2, 12, 1), fixedRow.BlockedLastTranStartedUtc);

        var utcRow = ReadXe(XeRowOn(2026, 11, 2), ServerClock.Utc);
        Assert.Equal(Naive(2026, 11, 2, 8, 1), utcRow.BlockedLastTranStartedUtc);
    }

    /// <summary>
    /// The SQL hands the local stamps back untouched. A stamp subtracted by the newest offset in SQL is the
    /// defect; the conversion lives in C# where the zone can follow a change. None of these reads windows on a
    /// stamp (the window is the naive-UTC <c>collection_time</c>), so nothing in them needs the offset at all.
    /// </summary>
    [Theory]
    [InlineData("xe")]
    [InlineData("xe-with-xml")]
    [InlineData("dmv")]
    public void TheSql_ReturnsTheRawLocalStamps_AndReadsNoOffset(string read)
    {
        var (sql, stamps) = read switch
        {
            "xe" => (DarlingBlockingReader.BlockedProcessReportsSql, XeStampNames),
            "xe-with-xml" => (DarlingBlockingReader.BlockedProcessReportsWithXmlSql, XeStampNames),
            _ => (DarlingBlockingReader.DmvBlockingSnapshotsSql, DmvStampNames),
        };

        Assert.DoesNotContain("make_interval", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("offset_minutes", sql, StringComparison.Ordinal);

        var projected = sql.Split('\n').Select(static l => l.Trim().TrimEnd(',')).ToHashSet(StringComparer.Ordinal);
        foreach (var stamp in stamps)
        {
            Assert.Contains(stamp, projected);
        }
    }

    private static readonly string[] XeStampNames =
    [
        "blocked_last_tran_started", "blocking_last_tran_started",
        "blocked_last_batch_started", "blocking_last_batch_started",
        "blocked_last_batch_completed", "blocking_last_batch_completed",
    ];

    private static readonly string[] DmvStampNames = ["blocked_last_tran_started", "blocking_last_tran_started"];
}
