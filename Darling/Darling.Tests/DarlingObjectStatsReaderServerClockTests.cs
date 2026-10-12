/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.McpServerClockTestSupport;

namespace Darling.Tests;

/// <summary>
/// #4793: the index usage read no longer subtracts the ONE newest offset in SQL. <c>last_user_access</c> (the
/// latest of the four index usage stamps) comes back as the server's own wall clock and is converted in C# with
/// the server's <see cref="ServerClock"/>. This is the read where the single offset hurt most: the index usage
/// counters persist since the instance restarted, so a large share of the values predate the newest daylight
/// saving change. US Eastern springs forward on 2026-03-08 and falls back on 2026-11-01.
/// </summary>
public sealed class DarlingObjectStatsReaderServerClockTests
{
    /* The select is 14 columns wide: last_user_access is column 12. */
    private static DataTable NewTable() => Table(14, 12);

    private static DarlingObjectStatsReader.IndexUsageRow Read(DataTable table, ServerClock clock)
    {
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return DarlingObjectStatsReader.MapIndexUsageRow(reader, clock);
    }

    [Theory]
    [InlineData(2026, 3, 7, 13)]   // standard time (-5 h), before the spring forward
    [InlineData(2026, 3, 9, 12)]   // daylight time (-4 h), after it
    [InlineData(2026, 10, 31, 12)] // daylight time, before the fall back
    [InlineData(2026, 11, 2, 13)]  // standard time, after it
    public void TheLastUserAccess_StoredOnEachSideOfAChange_LandsAtTheRightUtcTime(
        int year, int month, int day, int expectedUtcHour)
    {
        var table = NewTable();
        AddRow(table, (0, "db1"), (12, Naive(year, month, day, 8, 15)));

        /* The newest snapshot says -240 (daylight time): one subtracted offset is an hour off in winter. */
        var row = Read(table, Eastern(snapshotOffsetMinutes: -240));

        Assert.Equal(Naive(year, month, day, expectedUtcHour, 15), row.LastUserAccessUtc);
    }

    [Fact]
    public void ALastUserAccessThatIsNull_StaysNull()
    {
        var table = NewTable();
        AddRow(table, (0, "db1"));

        Assert.Null(Read(table, Eastern()).LastUserAccessUtc);
    }

    [Fact]
    public void ALastUserAccessInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var skipped = NewTable();
        AddRow(skipped, (0, "db1"), (12, Naive(2026, 3, 8, 2, 30)));
        var repeated = NewTable();
        AddRow(repeated, (0, "db1"), (12, Naive(2026, 11, 1, 1, 30)));

        Assert.Equal(Naive(2026, 3, 8, 7, 30), Read(skipped, Eastern()).LastUserAccessUtc);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), Read(repeated, Eastern()).LastUserAccessUtc);
    }

    [Fact]
    public void AFixedOffsetServerAndAUtcServer_ConvertWithoutAZone()
    {
        var table = NewTable();
        AddRow(table, (0, "db1"), (12, Naive(2026, 11, 2, 8, 15)));

        Assert.Equal(Naive(2026, 11, 2, 12, 15), Read(table, ServerClock.FixedOffset(-240)).LastUserAccessUtc);
        Assert.Equal(Naive(2026, 11, 2, 8, 15), Read(table, ServerClock.Utc).LastUserAccessUtc);
    }

    /// <summary>The SQL hands the GREATEST of the four local stamps back untouched and reads no offset.
    /// Taking the GREATEST of local values before converting equals converting first, except inside the
    /// repeated hour of a fall back, where the local time cannot say which occurrence it was.</summary>
    [Fact]
    public void TheSql_ReturnsTheRawLocalLastAccess_AndReadsNoOffset()
    {
        var sql = DarlingObjectStatsReader.IndexUsageSql;

        Assert.DoesNotContain("make_interval", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains(
            "GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update) AS last_user_access,",
            sql, StringComparison.Ordinal);
    }
}
