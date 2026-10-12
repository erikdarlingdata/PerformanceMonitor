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
/// #4793: the persistent version store read no longer subtracts the ONE newest offset in SQL. The four cleaner
/// times come back as the server's own wall clock and are converted in C# with the server's
/// <see cref="ServerClock"/>, so a cleaner run on either side of a daylight saving change lands at its real UTC
/// time (<c>aborted_version_cleaner_*</c> can reach back weeks on a quiet database, across a change).
/// <c>collection_time</c> is the collector's own naive UTC and is left alone. US Eastern springs forward on
/// 2026-03-08 and falls back on 2026-11-01.
/// </summary>
public sealed class DarlingPvsReaderServerClockTests
{
    /* The select is 16 columns wide: four server-local cleaner times at 8..11, collection_time, then the three skipped counters. */
    private static DataTable NewTable() => Table(16, 8, 9, 10, 11, 12);

    private static DarlingPvsReader.PvsStatsRow Read(DataTable table, ServerClock clock)
    {
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return DarlingPvsReader.MapPvsStatsRow(reader, clock);
    }

    [Theory]
    [InlineData(2026, 3, 7, 13)]   // standard time (-5 h), before the spring forward
    [InlineData(2026, 3, 9, 12)]   // daylight time (-4 h), after it
    [InlineData(2026, 10, 31, 12)] // daylight time, before the fall back
    [InlineData(2026, 11, 2, 13)]  // standard time, after it
    public void TheCleanerTimes_StoredOnEachSideOfAChange_LandAtTheRightUtcTimes(
        int year, int month, int day, int expectedUtcHour)
    {
        var table = NewTable();
        AddRow(table,
            (0, "db1"),
            (8, Naive(year, month, day, 8, 1)), (9, Naive(year, month, day, 8, 2)),
            (10, Naive(year, month, day, 8, 3)), (11, Naive(year, month, day, 8, 4)),
            (12, Naive(year, month, day, 15)));

        /* The newest snapshot says -240 (daylight time): one subtracted offset is an hour off in winter. */
        var row = Read(table, Eastern(snapshotOffsetMinutes: -240));

        Assert.Equal(Naive(year, month, day, expectedUtcHour, 1), row.AbortedCleanerStartTimeUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 2), row.AbortedCleanerEndTimeUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 3), row.OffrowCleanerStartTimeUtc);
        Assert.Equal(Naive(year, month, day, expectedUtcHour, 4), row.OffrowCleanerEndTimeUtc);
    }

    [Fact]
    public void TheCollectionTime_IsAlreadyUtc_AndIsNotConverted()
    {
        var table = NewTable();
        AddRow(table, (0, "db1"), (12, Naive(2026, 11, 2, 15)));

        var row = Read(table, Eastern());

        Assert.Equal(Naive(2026, 11, 2, 15), row.CollectionTime);
    }

    [Fact]
    public void ACleanerTimeThatIsNull_StaysNull()
    {
        var table = NewTable();
        AddRow(table, (0, "db1"), (10, Naive(2026, 11, 2, 8)), (12, Naive(2026, 11, 2, 15)));

        var row = Read(table, Eastern());

        Assert.Null(row.AbortedCleanerStartTimeUtc);
        Assert.Null(row.AbortedCleanerEndTimeUtc);
        Assert.Equal(Naive(2026, 11, 2, 13), row.OffrowCleanerStartTimeUtc);
        Assert.Null(row.OffrowCleanerEndTimeUtc);
    }

    [Fact]
    public void ACleanerTimeInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var table = NewTable();
        AddRow(table,
            (0, "db1"),
            (8, Naive(2026, 3, 8, 2, 30)),   // skipped by the spring forward
            (9, Naive(2026, 11, 1, 1, 30)),  // repeated by the fall back
            (12, Naive(2026, 11, 2, 15)));

        var row = Read(table, Eastern());

        Assert.Equal(Naive(2026, 3, 8, 7, 30), row.AbortedCleanerStartTimeUtc);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), row.AbortedCleanerEndTimeUtc);
    }

    /// <summary>The SQL hands the local cleaner times back untouched and reads no offset: the snapshot
    /// self-subquery and the ordering are on <c>collection_time</c> and the store size, never on a cleaner
    /// time.</summary>
    [Fact]
    public void TheSql_ReturnsTheRawLocalCleanerTimes_AndReadsNoOffset()
    {
        var sql = DarlingPvsReader.PvsStatsLatestSql;

        Assert.DoesNotContain("make_interval", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("offset_minutes", sql, StringComparison.Ordinal);

        var projected = sql.Split('\n').Select(static l => l.Trim().TrimEnd(',')).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("aborted_version_cleaner_start_time", projected);
        Assert.Contains("aborted_version_cleaner_end_time", projected);
        Assert.Contains("offrow_version_cleaner_start_time", projected);
        Assert.Contains("offrow_version_cleaner_end_time", projected);
    }
}
