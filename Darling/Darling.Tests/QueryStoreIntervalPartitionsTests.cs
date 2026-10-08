/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The day-partition background steps (#5571) without a store: the arm bound, the bound parsers, the days a promotion
/// and a create-ahead make, which partitions a drop may take (with the microsecond edges), the DDL text, and the
/// partitioned index path's inputs.
/// </summary>
public sealed class QueryStoreIntervalPartitionsTests
{
    private static readonly QueryStoreIntervalPartitions.IntervalTable Wide = QueryStoreIntervalPartitions.Wide;
    private static readonly DateTime Now = new(2026, 10, 8, 14, 55, 30, 123, DateTimeKind.Utc);

    private static DateTime Day(int month, int day) => new(2026, month, day, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheTables_HaveTheFixedNamesAndTheRetentionHorizons()
    {
        Assert.Equal("collect.query_store_interval_wide", Wide.Parent);
        Assert.Equal("collect.query_store_interval_wide_legacy", Wide.Legacy);
        Assert.Equal("collect.query_store_interval_wide_default", Wide.Default);
        Assert.Equal("ck_query_store_interval_wide_legacy_before", Wide.CheckName);
        Assert.Equal("collect.query_store_interval_wide_p20261010", Wide.DayPartition(Day(10, 10)));
        Assert.Equal("collect.query_store_interval_latest_p20261231", QueryStoreIntervalPartitions.Latest.DayPartition(Day(12, 31)));
        Assert.Equal("ck_query_store_interval_latest_legacy_before", QueryStoreIntervalPartitions.Latest.CheckName);

        /* The drop uses the same horizons as the row purge; one number, two homes, so they are pinned together. */
        Assert.Equal(DarlingRetention.QueryStoreIntervalWideRetentionDays, QueryStoreIntervalPartitions.Wide.HorizonDays);
        Assert.Equal(DarlingRetention.QueryStoreIntervalLatestRetentionDays, QueryStoreIntervalPartitions.Latest.HorizonDays);
    }

    [Theory]
    [InlineData(2026, 10, 8, 0, 0, 0, 2026, 10, 10)]
    [InlineData(2026, 10, 8, 23, 59, 59, 2026, 10, 10)]
    [InlineData(2026, 10, 30, 12, 0, 0, 2026, 11, 1)]
    [InlineData(2026, 12, 31, 1, 0, 0, 2027, 1, 2)]
    public void TheArmBound_IsTodaysUtcMidnightPlusTwoDays(int y, int mo, int d, int h, int mi, int s, int ey, int emo, int ed)
    {
        Assert.Equal(new DateTime(ey, emo, ed), QueryStoreIntervalPartitions.ArmBound(new DateTime(y, mo, d, h, mi, s, DateTimeKind.Utc)));
    }

    [Fact]
    public void ReArm_HappensOnlyForAnInvalidCheckWithLessThanTwelveHoursLeft()
    {
        var s = Day(10, 10);
        Assert.True(QueryStoreIntervalPartitions.ShouldReArm(false, s, new DateTime(2026, 10, 9, 12, 0, 1, DateTimeKind.Utc)));
        Assert.True(QueryStoreIntervalPartitions.ShouldReArm(false, s, new DateTime(2026, 10, 10, 1, 0, 0, DateTimeKind.Utc)));
        Assert.False(QueryStoreIntervalPartitions.ShouldReArm(false, s, new DateTime(2026, 10, 9, 12, 0, 0, DateTimeKind.Utc)));
        Assert.False(QueryStoreIntervalPartitions.ShouldReArm(false, s, new DateTime(2026, 10, 8, 0, 0, 0, DateTimeKind.Utc)));
        Assert.False(QueryStoreIntervalPartitions.ShouldReArm(true, s, new DateTime(2026, 10, 10, 1, 0, 0, DateTimeKind.Utc)));
    }

    [Theory]
    [InlineData("CHECK ((first_execution_time < '2026-10-10 00:00:00'::timestamp without time zone)) NOT VALID", true, "2026-10-10 00:00:00.000000")]
    [InlineData("CHECK ((first_execution_time < '2026-10-10 00:00:00.5'::timestamp without time zone))", true, "2026-10-10 00:00:00.500000")]
    [InlineData("CHECK ((first_execution_time < '2026-10-10 00:00:00.123456'::timestamp without time zone))", true, "2026-10-10 00:00:00.123456")]
    [InlineData("CHECK ((other_column < 5))", false, null)]
    [InlineData("", false, null)]
    public void TheCheckBound_IsReadBackFromTheConstraintText(string definition, bool expected, string? bound)
    {
        Assert.Equal(expected, QueryStoreIntervalPartitions.TryParseCheckBound(definition, out var value));
        if (expected)
        {
            Assert.Equal(bound, value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void ThePartitionBounds_AreParsedForTheShapesThisCodeWrites()
    {
        Assert.True(QueryStoreIntervalPartitions.TryParsePartitionBound("DEFAULT", out var isDefault, out var lo, out var hi, out var unbounded));
        Assert.True(isDefault);
        Assert.Null(lo);
        Assert.Null(hi);
        Assert.False(unbounded);

        Assert.True(QueryStoreIntervalPartitions.TryParsePartitionBound(
            "FOR VALUES FROM (MINVALUE) TO (MAXVALUE)", out isDefault, out lo, out hi, out unbounded));
        Assert.False(isDefault);
        Assert.Null(lo);
        Assert.Null(hi);
        Assert.True(unbounded);

        Assert.True(QueryStoreIntervalPartitions.TryParsePartitionBound(
            "FOR VALUES FROM (MINVALUE) TO ('2026-10-10 00:00:00')", out isDefault, out lo, out hi, out unbounded));
        Assert.Null(lo);
        Assert.Equal(Day(10, 10), hi);
        Assert.False(unbounded);

        Assert.True(QueryStoreIntervalPartitions.TryParsePartitionBound(
            "FOR VALUES FROM ('2026-10-10 00:00:00') TO ('2026-10-11 00:00:00')", out isDefault, out lo, out hi, out unbounded));
        Assert.Equal(Day(10, 10), lo);
        Assert.Equal(Day(10, 11), hi);

        Assert.False(QueryStoreIntervalPartitions.TryParsePartitionBound("FOR VALUES IN (1)", out _, out _, out _, out _));
        Assert.False(QueryStoreIntervalPartitions.TryParsePartitionBound(null, out _, out _, out _, out _));
    }

    [Fact]
    public void ThePromotion_CreatesEveryDayFromSThroughThreeDaysAhead()
    {
        var days = QueryStoreIntervalPartitions.PromotionDays(Day(10, 10), Now);
        Assert.Equal(new[] { Day(10, 10), Day(10, 11) }, days);

        /* Promoted late: no hole between S and the clock. */
        var late = QueryStoreIntervalPartitions.PromotionDays(Day(10, 10), new DateTime(2026, 10, 13, 5, 0, 0, DateTimeKind.Utc));
        Assert.Equal(new[] { Day(10, 10), Day(10, 11), Day(10, 12), Day(10, 13), Day(10, 14), Day(10, 15), Day(10, 16) }, late);

        /* A clock that went backwards: S is past the horizon of days to make, so DEFAULT alone takes over. */
        Assert.Empty(QueryStoreIntervalPartitions.PromotionDays(Day(10, 20), Now));
    }

    [Fact]
    public void TheCreateAhead_FillsFromTheNewestPartitionThroughThreeDaysAheadWithNoHoles()
    {
        /* Newest upper bound is the 12th: the 12th, 13th and 14th... through today (8th) + 3 = the 11th is already covered. */
        Assert.Empty(QueryStoreIntervalPartitions.CreateAheadDays(Day(10, 12), Now, 9));

        /* Newest ends at the 10th (its upper bound): the 10th and the 11th are missing. */
        Assert.Equal(new[] { Day(10, 10), Day(10, 11) }, QueryStoreIntervalPartitions.CreateAheadDays(Day(10, 10), Now, 9));

        /* An outage: the newest day ended on the 3rd, so the 3rd to the 11th are all made, none skipped. */
        var outage = QueryStoreIntervalPartitions.CreateAheadDays(Day(10, 3), Now, 9);
        Assert.Equal(Enumerable.Range(3, 9).Select(d => Day(10, d)).ToArray(), outage);

        /* Older than the horizon is never created (the next drop would remove it at once): 9 days before the 8th is the 29th. */
        var stale = QueryStoreIntervalPartitions.CreateAheadDays(Day(9, 1), Now, 9);
        Assert.Equal(Day(9, 29), stale[0]);
        Assert.Equal(Day(10, 11), stale[^1]);
        Assert.Equal(stale.Count, stale.Distinct().Count());

        /* No partition at all: today through today + 3. */
        Assert.Equal(new[] { Day(10, 8), Day(10, 9), Day(10, 10), Day(10, 11) }, QueryStoreIntervalPartitions.CreateAheadDays(null, Now, 9));
    }

    private static QueryStoreIntervalPartitions.PartitionInfo DayPartition(int month, int day) =>
        new($"collect.query_store_interval_wide_p2026{month:00}{day:00}", false, Day(month, day), Day(month, day).AddDays(1), false);

    [Fact]
    public void TheDrop_TakesWholeExpiredDaysAndKeepsAPartialOne()
    {
        var legacy = new QueryStoreIntervalPartitions.PartitionInfo("collect.query_store_interval_wide_legacy", false, null, Day(10, 2), false);
        var unboundedLegacy = new QueryStoreIntervalPartitions.PartitionInfo("collect.query_store_interval_wide_legacy", false, null, null, true);
        var dflt = new QueryStoreIntervalPartitions.PartitionInfo("collect.query_store_interval_wide_default", true, null, null, false);
        var partitions = new[] { DayPartition(10, 5), DayPartition(10, 3), DayPartition(10, 4), DayPartition(10, 8), dflt, legacy };

        /* Cutoff = 2026-10-04 12:00 (a Now - 9 d): the 2nd (legacy) and the 3rd have fully expired; the 4th ends at 10-05 00:00 > cutoff. */
        var dropped = QueryStoreIntervalPartitions.ExpiredPartitions(partitions, new DateTime(2026, 10, 4, 12, 0, 0));
        Assert.Equal(
            new[] { "collect.query_store_interval_wide_legacy", "collect.query_store_interval_wide_p20261003" },
            dropped.Select(p => p.Name).ToArray());

        /* An unbounded legacy table and DEFAULT are never dropped, whatever the cutoff. */
        Assert.Empty(QueryStoreIntervalPartitions.ExpiredPartitions(new[] { unboundedLegacy, dflt }, DateTime.MaxValue));

        /* Legacy goes exactly when S <= cutoff, to the microsecond; a day goes when its upper bound is the cutoff. */
        var s = Day(10, 2);
        Assert.Single(QueryStoreIntervalPartitions.ExpiredPartitions(new[] { legacy }, s));
        Assert.Empty(QueryStoreIntervalPartitions.ExpiredPartitions(new[] { legacy }, s.AddTicks(-10)));
        Assert.Single(QueryStoreIntervalPartitions.ExpiredPartitions(new[] { DayPartition(10, 3) }, Day(10, 4)));
        Assert.Empty(QueryStoreIntervalPartitions.ExpiredPartitions(new[] { DayPartition(10, 3) }, Day(10, 4).AddTicks(-10)));
    }

    [Fact]
    public void TheCutoff_KeepsTheClocksMicroseconds()
    {
        var cutoff = QueryStoreIntervalPartitions.Cutoff(new DateTime(2026, 10, 8, 14, 55, 30, DateTimeKind.Utc).AddTicks(1230), 9);
        Assert.Equal(new DateTime(2026, 9, 29, 14, 55, 30).AddTicks(1230), cutoff);
        Assert.Equal(DateTimeKind.Unspecified, cutoff.Kind);
    }

    [Fact]
    public void TheDdl_UsesTheFixedNamesAndFillfactorFifty()
    {
        var s = Day(10, 10);
        Assert.Equal(
            "ALTER TABLE collect.query_store_interval_wide_legacy ADD CONSTRAINT ck_query_store_interval_wide_legacy_before "
            + "CHECK (first_execution_time < '2026-10-10 00:00:00'::timestamp) NOT VALID;",
            QueryStoreIntervalPartitions.AddCheckSql(Wide, s));
        Assert.Equal(
            "ALTER TABLE collect.query_store_interval_wide_legacy VALIDATE CONSTRAINT ck_query_store_interval_wide_legacy_before;",
            QueryStoreIntervalPartitions.ValidateSql(Wide));
        Assert.Equal(
            "CREATE TABLE collect.query_store_interval_wide_p20261010 PARTITION OF collect.query_store_interval_wide "
            + "FOR VALUES FROM ('2026-10-10 00:00:00') TO ('2026-10-11 00:00:00') WITH (fillfactor = 50);",
            QueryStoreIntervalPartitions.CreateDaySql(Wide, s));
        Assert.Equal(
            "CREATE TABLE collect.query_store_interval_wide_default PARTITION OF collect.query_store_interval_wide DEFAULT WITH (fillfactor = 50);",
            QueryStoreIntervalPartitions.CreateDefaultSql(Wide));
    }

    [Fact]
    public void ThePartitionedIndexPath_HasADefinitionForEverySpec_AndItMatchesThePlainBuild()
    {
        foreach (var spec in QueryStoreBackgroundIndexes.All)
        {
            Assert.False(string.IsNullOrEmpty(spec.IndexDefinition), spec.IndexName);
            Assert.Contains(spec.IndexDefinition, spec.PlainCreateSql, StringComparison.Ordinal);

            /* A leaf index name is the parent's short name + "_" + the longest leaf suffix ("p20261010", "legacy", "default"), <= 63. */
            var shortName = spec.IndexName[(spec.IndexName.LastIndexOf('.') + 1)..];
            Assert.True(shortName.Length + "_p20261010".Length <= 63, spec.IndexName);
        }
    }

    [Fact]
    public void ThePlainPath_StillNeverRunsOnAPartitionedTable()
    {
        /* The state read reports the table's relkind (the 5th column), which EnsureAsync routes on. */
        Assert.Contains("relkind", QueryStoreBackgroundIndexes.StateSql, StringComparison.Ordinal);
        Assert.Contains("table_kind", QueryStoreBackgroundIndexes.StateSql, StringComparison.Ordinal);
    }
}
