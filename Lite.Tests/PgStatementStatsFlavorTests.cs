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
/// #2625: <c>pg_statement_stats</c> reads TWO sources — Aurora's extended function, or the vanilla
/// <c>pg_stat_statements</c> view on any other PostgreSQL — and which one is a <c>BuildQuery</c> decision,
/// never an applicability one.
///
/// <para>
/// The gate it replaced cost more than a few columns. <c>AppliesTo</c> means permanent incapability, and the
/// capability machinery composes a deliberately final sentence from it: "does not collect per-query-shape
/// execution statistics, and never will". Every operator of a non-Aurora PostgreSQL target got that sentence
/// for the single question a database monitor exists to answer — while <c>pg_kernel_stats</c> collected OS
/// CPU and <c>pg_predicate_stats</c> collected selectivity, both keyed by the very queryids nothing was
/// identifying. It survived because no self-hosted PostgreSQL target existed to notice.
/// </para>
///
/// <para>
/// The ordinals are the load-bearing detail. Both queries select the same 27 columns in the same order — the
/// vanilla one fills Aurora's six with typed NULL literals — so <c>ReadAsync</c>, <c>PayloadColumns</c> and
/// <c>WritePayload</c> stay single implementations. A shorter vanilla SELECT would have meant a second reader
/// whose ordinals could drift from this one, which is exactly the failure the per-major column naming in this
/// collector already exists to prevent.
/// </para>
/// </summary>
public class PgStatementStatsFlavorTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(bool isAurora, int major = 17)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                IsAurora = isAurora,
                PostgresMajorVersion = major,
                PostgresVersionNum = major * 10000,
            },
        };

    private static string Sql(bool isAurora, int major = 17)
        => PgStatementStatsCollector.Instance.BuildQuery(MakeContext(isAurora, major)).Text;

    /// <summary>
    /// The whole point. This assertion is the one that would have caught the gap, and it could not have been
    /// written before a non-Aurora target existed to ask the question of.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ItAppliesToEveryPostgresTarget_AuroraOrNot(bool isAurora)
        => Assert.True(PgStatementStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = isAurora }));

    [Fact]
    public void AuroraReadsTheExtendedFunction()
    {
        var sql = Sql(isAurora: true);

        Assert.Contains("FROM aurora_stat_statements(false)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_stat_statements", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryOtherPostgresReadsTheVanillaView()
    {
        var sql = Sql(isAurora: false);

        Assert.Contains("FROM public.pg_stat_statements", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("aurora_stat_statements", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// One reader, one column list, one write order — so the two queries must agree on shape, not just on
    /// source. Counted at the top level so a comma inside a cast or a comment cannot inflate it.
    /// </summary>
    [Fact]
    public void BothFlavorsSelectTheSameColumnsInTheSameOrder()
    {
        var aurora = SelectAliases(Sql(isAurora: true));
        var vanilla = SelectAliases(Sql(isAurora: false));

        Assert.Equal(aurora, vanilla);
        Assert.Equal(PgStatementStatsCollector.Instance.PayloadColumns.Count - 3, aurora.Count);
    }

    /// <summary>
    /// NULL, not 0, and typed — an untyped NULL literal would arrive as text and Npgsql's strict type
    /// checking would throw on <c>GetInt64</c>, which is the same class of defect as the ordinal drift above.
    /// </summary>
    [Theory]
    [InlineData("storage_blks_read", "bigint")]
    [InlineData("orcache_blks_hit", "bigint")]
    [InlineData("storage_blk_read_time", "double precision")]
    [InlineData("orcache_blk_read_time", "double precision")]
    [InlineData("total_exec_peakmem", "bigint")]
    [InlineData("max_exec_peakmem", "bigint")]
    public void TheAuroraOnlyColumnsAreTypedNullsOnTheVanillaPath(string alias, string type)
    {
        Assert.Matches(new Regex($@"NULL::{Regex.Escape(type)}\s+AS {Regex.Escape(alias)}\b"), Sql(isAurora: false));
    }

    /// <summary>
    /// <c>toplevel</c> arrived in <c>pg_stat_statements</c> 1.9 (PostgreSQL 14). Before that, nested tracking
    /// did not exist, so every row IS a top-level statement — <c>true</c> is the CORRECT value on an older
    /// server, not a fallback, and the delta key stays four-part on every version.
    /// </summary>
    [Theory]
    [InlineData(13, "true")]
    [InlineData(14, "toplevel")]
    [InlineData(17, "toplevel")]
    public void ToplevelIsGuardedForPostgresBefore14(int major, string expected)
    {
        Assert.Matches(new Regex($@"{Regex.Escape(expected)}\s+AS toplevel\b"), Sql(isAurora: false, major));
    }

    /// <summary>
    /// The per-major block-time naming applies to the vanilla view too — it is a <c>pg_stat_statements</c>
    /// rename, not an Aurora one, and getting it wrong would silently shift every ordinal after it.
    /// </summary>
    [Theory]
    [InlineData(16, "blk_read_time", "blk_write_time")]
    [InlineData(17, "shared_blk_read_time", "shared_blk_write_time")]
    public void TheBlockTimeColumnsFollowTheMajorVersionOnBothFlavors(int major, string read, string write)
    {
        foreach (var isAurora in new[] { true, false })
        {
            var sql = Sql(isAurora, major);

            Assert.Matches(new Regex($@"(?<![a-z_]){Regex.Escape(read)}\s+AS blk_read_time\b"), sql);
            Assert.Matches(new Regex($@"(?<![a-z_]){Regex.Escape(write)}\s+AS blk_write_time\b"), sql);
        }
    }

    /// <summary>
    /// The vanilla query must not reach for anything Aurora-only by accident — a stray reference would fail
    /// at parse time on every non-Aurora target, which is a total outage of the read rather than a missing
    /// column.
    /// </summary>
    [Fact]
    public void TheVanillaQueryTouchesNoAuroraSurface()
        => Assert.DoesNotContain("aurora_", Sql(isAurora: false), StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Column aliases of the outermost SELECT, in order. Comments are stripped first so a comma inside one
    /// cannot be counted — the same correction the probe-arity guard needed.
    /// </summary>
    private static System.Collections.Generic.List<string> SelectAliases(string sql)
    {
        var body = Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        var start = body.IndexOf("SELECT", StringComparison.Ordinal) + "SELECT".Length;
        var end = body.IndexOf("FROM ", start, StringComparison.Ordinal);

        return Regex.Matches(body[start..end], @"AS\s+([a-z_]+)\s*(?:,|$)", RegexOptions.IgnoreCase)
            .Select(m => m.Groups[1].Value)
            .ToList();
    }
}
