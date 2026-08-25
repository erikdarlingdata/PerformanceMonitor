/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2544: the write-side collector. Almost every assertion here is about the VERSION SURFACE, because that
/// is where this collector can actually fail — and it fails in the one way a text-only test cannot normally
/// catch: selecting a column that a major removed raises 42703 at run time on that major only, and takes the
/// whole collection with it. The fleet this was written for is an even split across the 16→17 break, so
/// "works on the version I tested" is not a property worth having.
/// </summary>
public class PgWriteStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(int major)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    private static string Sql(int major)
        => PgWriteStatsCollector.Instance.BuildQuery(MakeContext(major)).Text;

    /// <summary>
    /// The columns 17 REMOVED from <c>pg_stat_bgwriter</c> must not be referenced on 17 or later. Five of
    /// them were renamed into <c>pg_stat_checkpointer</c>; <c>buffers_backend</c> and
    /// <c>buffers_backend_fsync</c> went to <c>pg_stat_io</c> instead and have no successor here at all.
    /// </summary>
    [Theory]
    [InlineData(17)]
    [InlineData(18)]
    [InlineData(19)]
    public void PreSeventeenBgwriterColumns_AreNotSelectedOnSeventeenOrLater(int major)
    {
        var sql = Sql(major);

        foreach (var gone in new[]
        {
            "checkpoints_timed", "checkpoints_req", "checkpoint_write_time", "checkpoint_sync_time",
            "buffers_checkpoint", "buffers_backend", "buffers_backend_fsync",
        })
        {
            /* Word-boundary matched. A plain Contains would fire on the OUTPUT aliases, which deliberately
               keep some of these names (buffers_backend is a stored column on every major, it is simply
               NULL from 17) — the assertion is about what is READ FROM THE VIEW, not what is emitted. */
            Assert.DoesNotMatch(new Regex($@"pg_stat_bgwriter\)[^\n]*\b{gone}\b"), sql);
            Assert.DoesNotMatch(new Regex($@"\b{gone}\s+FROM\s+pg_catalog\.pg_stat_bgwriter"), sql);
        }
    }

    /// <summary>
    /// And the converse: below 17 those columns are exactly where the values must come from, because
    /// <c>pg_stat_checkpointer</c> does not exist there at all. A collector that referenced it on 16 would
    /// fail with 42P01 on half this fleet.
    /// </summary>
    [Theory]
    [InlineData(14)]
    [InlineData(15)]
    [InlineData(16)]
    public void BelowSeventeen_ReadsTheOldColumns_AndNeverTouchesPgStatCheckpointer(int major)
    {
        var sql = Sql(major);

        Assert.DoesNotContain("pg_stat_checkpointer", sql, StringComparison.Ordinal);
        Assert.Contains("checkpoints_timed", sql, StringComparison.Ordinal);
        Assert.Contains("checkpoints_req", sql, StringComparison.Ordinal);
        Assert.Contains("buffers_checkpoint", sql, StringComparison.Ordinal);
        Assert.Contains("buffers_backend", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The 18 hazard, and the specific one #2544 was reopened to describe: <c>pg_stat_wal</c> LOST
    /// <c>wal_write</c>, <c>wal_sync</c>, <c>wal_write_time</c> and <c>wal_sync_time</c>. A collector written
    /// and tested against 17 selects all four and raises 42703 on an 18 target every cycle.
    /// </summary>
    [Theory]
    [InlineData(18)]
    [InlineData(19)]
    public void EighteenAndLater_DoesNotSelectTheRemovedWalColumns(int major)
    {
        var sql = Sql(major);

        foreach (var gone in new[] { "wal_write", "wal_sync", "wal_write_time", "wal_sync_time" })
        {
            /* Qualified with the view's alias, so this fires on a READ of the column and not on the output
               alias of the same name, which is retained on every major and simply carries NULL here. */
            Assert.DoesNotContain($"w.{gone}", sql, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(14)]
    [InlineData(16)]
    [InlineData(17)]
    public void BelowEighteen_StillReadsTheWalTimingColumns(int major)
    {
        var sql = Sql(major);

        Assert.Contains("w.wal_write_time", sql, StringComparison.Ordinal);
        Assert.Contains("w.wal_sync_time", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Columns that arrived at 18 must not be read below it — the mirror of the removal case, and the one
    /// that would fail on the 26 clusters of this fleet running 17.
    /// </summary>
    [Theory]
    [InlineData(14)]
    [InlineData(16)]
    [InlineData(17)]
    public void BelowEighteen_DoesNotSelectEighteenOnlyColumns(int major)
    {
        var sql = Sql(major);

        /* Asserted as "not READ FROM the view", not "the string is absent". Both names survive as OUTPUT
           ALIASES on every major - the stored shape is the union and simply carries NULL here - so a plain
           absence check fails against a perfectly correct query. */
        Assert.DoesNotMatch(new Regex(@"num_done\s+FROM\s+pg_catalog\.pg_stat_checkpointer"), sql);
        Assert.DoesNotMatch(new Regex(@"slru_written\s+FROM\s+pg_catalog\.pg_stat_checkpointer"), sql);
    }

    /// <summary>
    /// Every version gate is a <c>&gt;=</c> floor, so a future major that keeps 18's shape does not fall off
    /// the end into a column that no longer exists. Asserted by behaviour on a version that does not exist
    /// yet rather than by reading the source for a comparison operator.
    /// </summary>
    [Fact]
    public void AFutureMajor_BehavesLikeTheNewestKnownOne()
        => Assert.Equal(Sql(18), Sql(99));

    /// <summary>
    /// The STORED shape is identical on every major — that is what lets one store hold a 16 and an 18 target
    /// and mean the same thing in both rows. Only the SQL varies.
    /// </summary>
    [Fact]
    public void ThePayloadShape_DoesNotVaryByVersion()
    {
        var columns = PgWriteStatsCollector.Instance.PayloadColumns;

        Assert.Equal(26, columns.Count);
        Assert.Equal(columns.Count, columns.Select(c => c.Name).Distinct(StringComparer.Ordinal).Count());

        /* One row per SELECT column, in order — a mismatch here is a silently shifted binary COPY, which
           writes every value into the wrong column rather than failing.

           FROM and CROSS JOIN lines are excluded before matching: the two TABLE aliases (AS b, AS w) are
           spelled with the same keyword as the column aliases, and counting them shifted the whole list by
           two while looking like an ordering bug in the collector. */
        foreach (var major in new[] { 14, 16, 17, 18 })
        {
            var selected = Sql(major)
                .Split('\n')
                .Where(line => !line.TrimStart().StartsWith("FROM", StringComparison.Ordinal)
                            && !line.TrimStart().StartsWith("CROSS JOIN", StringComparison.Ordinal))
                .Select(line => Regex.Match(line, @"\bAS\s+([a-z_]+),?\s*$"))
                .Where(m => m.Success)
                .Select(m => m.Groups[1].Value)
                .ToArray();

            Assert.Equal(columns.Select(c => c.Name).ToArray(), selected);
        }
    }

    /// <summary>
    /// <c>wal_bytes</c> is <c>numeric</c> upstream, not <c>bigint</c>, precisely because cumulative WAL
    /// volume may exceed 2^63. Storing it as a bigint would work for years and then wrap.
    /// </summary>
    [Fact]
    public void WalBytes_IsDecimal_NotBigInt()
    {
        var column = PgWriteStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "wal_bytes");

        Assert.Equal(CollectorColumnType.Decimal, column.Type);
    }

    /// <summary>
    /// All three <c>stats_reset</c> stamps are stored, because <c>pg_stat_reset_shared</c> takes a target and
    /// the three families reset independently. A read that differenced across a reset it could not see would
    /// report a negative interval as an enormous positive one.
    /// </summary>
    [Fact]
    public void AllThreeStatsResetStamps_AreStored()
    {
        var names = PgWriteStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("checkpointer_stats_reset", names);
        Assert.Contains("bgwriter_stats_reset", names);
        Assert.Contains("wal_stats_reset", names);
    }

    /// <summary>
    /// <c>pg_stat_wal</c> is the binding floor at 14. Below that the collector must not dispatch at all
    /// rather than fail per cycle.
    /// </summary>
    [Theory]
    [InlineData(13, false)]
    [InlineData(14, true)]
    [InlineData(17, true)]
    public void AppliesTo_FloorsAtFourteen(int major, bool expected)
        => Assert.Equal(expected, PgWriteStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major }));

    /// <summary>
    /// Timestamps come back as naive UTC via <c>AT TIME ZONE 'UTC'</c>, never a bare <c>::timestamp</c> cast
    /// — the cast renders in the SESSION's TimeZone, so a store session east of UTC would record a stamp
    /// hours away from the one the server meant. Same rule <c>pg_stat_io</c> follows.
    /// </summary>
    [Fact]
    public void StatsResetStamps_AreConvertedToUtc_NotBareCast()
    {
        var sql = Sql(17);

        Assert.Equal(3, Regex.Matches(sql, @"AT TIME ZONE 'UTC'").Count);
        Assert.DoesNotMatch(new Regex(@"stats_reset\s*::\s*timestamp"), sql);
    }

    /// <summary>
    /// Catalog reads are schema-qualified. <c>pg_catalog</c> is searched implicitly but not necessarily
    /// FIRST, so an unqualified read can resolve to an object a user created in a schema earlier in the
    /// monitoring login's search_path.
    /// </summary>
    [Theory]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(18)]
    public void EveryCatalogRead_IsSchemaQualified(int major)
    {
        var sql = Sql(major);

        foreach (var view in new[] { "pg_stat_bgwriter", "pg_stat_wal", "pg_stat_checkpointer" })
        {
            foreach (Match match in Regex.Matches(sql, $@"(\S*){Regex.Escape(view)}"))
            {
                Assert.Equal("pg_catalog.", match.Groups[1].Value);
            }
        }
    }
}
