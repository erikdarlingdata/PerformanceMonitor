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
/// #4793: the running jobs read no longer subtracts the ONE newest offset in SQL. A running job's
/// <c>start_time</c> is the Agent's own wall clock and is converted in C# with the server's
/// <see cref="ServerClock"/>, so a job that started before a daylight saving change (a long-running job can
/// reach back across one) lands at its real UTC time. <c>collection_time</c> is naive UTC and is left alone.
/// US Eastern springs forward on 2026-03-08 and falls back on 2026-11-01.
/// </summary>
public sealed class DarlingJobReaderServerClockTests
{
    /* The select is 11 columns wide: collection_time first, start_time fifth. */
    private static DataTable NewTable() => Table(11, 0, 4);

    private static DarlingJobReader.RunningJobRow Read(DataTable table, ServerClock clock)
    {
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return DarlingJobReader.MapRunningJobRow(reader, clock);
    }

    [Theory]
    [InlineData(2026, 3, 7, 13)]   // standard time (-5 h), before the spring forward
    [InlineData(2026, 3, 9, 12)]   // daylight time (-4 h), after it
    [InlineData(2026, 10, 31, 12)] // daylight time, before the fall back
    [InlineData(2026, 11, 2, 13)]  // standard time, after it
    public void TheStartTime_StoredOnEachSideOfAChange_LandsAtTheRightUtcTime(
        int year, int month, int day, int expectedUtcHour)
    {
        var table = NewTable();
        AddRow(table, (0, Naive(year, month, day, 15)), (1, "nightly"), (4, Naive(year, month, day, 8, 20)));

        /* The newest snapshot says -240 (daylight time): one subtracted offset is an hour off in winter. */
        var row = Read(table, Eastern(snapshotOffsetMinutes: -240));

        Assert.Equal(Naive(year, month, day, expectedUtcHour, 20), row.StartTimeUtc);
    }

    [Fact]
    public void TheCollectionTime_IsAlreadyUtc_AndIsNotConverted()
    {
        var table = NewTable();
        AddRow(table, (0, Naive(2026, 11, 2, 15)), (4, Naive(2026, 11, 2, 8)));

        Assert.Equal(Naive(2026, 11, 2, 15), Read(table, Eastern()).CollectionTime);
    }

    [Fact]
    public void AStartTimeInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var skipped = NewTable();
        AddRow(skipped, (0, Naive(2026, 3, 8, 12)), (4, Naive(2026, 3, 8, 2, 30)));
        var repeated = NewTable();
        AddRow(repeated, (0, Naive(2026, 11, 1, 12)), (4, Naive(2026, 11, 1, 1, 30)));

        Assert.Equal(Naive(2026, 3, 8, 7, 30), Read(skipped, Eastern()).StartTimeUtc);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), Read(repeated, Eastern()).StartTimeUtc);
    }

    [Fact]
    public void AFixedOffsetServerAndAUtcServer_ConvertWithoutAZone()
    {
        var table = NewTable();
        AddRow(table, (0, Naive(2026, 11, 2, 15)), (4, Naive(2026, 11, 2, 8, 20)));

        Assert.Equal(Naive(2026, 11, 2, 12, 20), Read(table, ServerClock.FixedOffset(-240)).StartTimeUtc);
        Assert.Equal(Naive(2026, 11, 2, 8, 20), Read(table, ServerClock.Utc).StartTimeUtc);
    }

    /// <summary>The SQL hands the local start time back untouched and reads no offset: the snapshot
    /// self-subquery is on the naive-UTC <c>collection_time</c> and the ordering on the collector-computed
    /// duration.</summary>
    [Fact]
    public void TheSql_ReturnsTheRawLocalStartTime_AndReadsNoOffset()
    {
        var sql = DarlingJobReader.RunningJobsSql;

        Assert.DoesNotContain("make_interval", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("offset_minutes", sql, StringComparison.Ordinal);

        var projected = sql.Split('\n').Select(static l => l.Trim().TrimEnd(',')).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("start_time", projected);
    }
}
