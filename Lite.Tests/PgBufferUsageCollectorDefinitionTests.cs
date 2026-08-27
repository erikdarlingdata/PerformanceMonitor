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
/// #2544, the buffers slice. Every assertion here pins a correctness trap that was measured rather than
/// reasoned about — the filenode join in particular, where the query every published example writes loses
/// any table that has ever been rewritten.
/// </summary>
public class PgBufferUsageCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgBufferUsageCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
            },
            ExcludedDatabases = Array.Empty<string>(),
        }).Text;

    /// <summary>
    /// The filenode join must go through <c>pg_relation_filenode()</c>. Measured: after one
    /// <c>VACUUM FULL</c>, the naive <c>pg_class.oid = relfilenode</c> join reported ZERO buffers for a
    /// table holding 6,667 — a table silently vanishes from the report once rewritten. Mapped catalogs
    /// carry <c>relfilenode = 0</c>, so joining the raw column drops those too.
    /// </summary>
    [Fact]
    public void TheFilenodeJoin_GoesThroughPgRelationFilenode()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        Assert.Contains("pg_catalog.pg_relation_filenode(c.oid)", sql, StringComparison.Ordinal);

        /* And never the two wrong keys. Comments are stripped first because the query explains exactly
           these mistakes and would otherwise satisfy its own prohibition. */
        Assert.DoesNotMatch(new Regex(@"c\.oid\s*=\s*\S*relfilenode"), sql);
        Assert.DoesNotMatch(new Regex(@"c\.relfilenode\s*="), sql);
    }

    /// <summary>
    /// The relation name may be resolved ONLY for buffers belonging to the connected database. The pool is
    /// cluster-wide and <c>pg_class</c> is not, so a filenode from another database can collide with a local
    /// OID and produce a confidently WRONG name — measured.
    /// </summary>
    [Fact]
    public void TheRelationJoin_IsScopedToTheConnectedDatabase()
        => Assert.Matches(new Regex(@"reldatabase\s*=\s*\(SELECT oid FROM pg_catalog\.pg_database WHERE datname = current_database\(\)\)"), Sql());

    /// <summary>
    /// Buffers from other databases are KEPT, not filtered out. Dropping them would understate how full the
    /// pool is, which is the one number this collector exists to report.
    /// </summary>
    [Fact]
    public void ForeignDatabaseBuffers_AreKeptRatherThanFiltered()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        /* The only WHERE on the outer query excludes UNUSED buffers (relfilenode IS NULL), never foreign
           ones — a predicate on reldatabase in WHERE would silently drop them. */
        Assert.Matches(new Regex(@"WHERE\s+p\.relfilenode IS NOT NULL"), sql);
        Assert.DoesNotMatch(new Regex(@"WHERE[^)]*p\.reldatabase\s*="), sql);
    }

    /// <summary>
    /// The relation join must be LEFT. An inner join would drop every buffer this instance cannot name,
    /// which is precisely the foreign-database occupancy above.
    /// </summary>
    [Fact]
    public void TheRelationJoin_IsLeft()
        => Assert.Matches(new Regex(@"LEFT JOIN pg_catalog\.pg_class"), Sql());

    /// <summary>
    /// Pool totals ride on every row so a share never has to be computed against a second read of a pool
    /// that has moved on since.
    /// </summary>
    [Fact]
    public void PoolTotals_TravelOnEveryRow()
    {
        var names = PgBufferUsageCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("pool_buffers_total", names);
        Assert.Contains("pool_buffers_used", names);
    }

    /// <summary>
    /// Both totals come from a window over the SAME scan. A second query, or
    /// <c>pg_buffercache_summary()</c> alongside the view, would sample a moving pool twice and the
    /// disagreement would land in the percentage a reader computes.
    /// </summary>
    [Fact]
    public void PoolTotals_ComeFromTheSameScan()
    {
        var sql = Sql();

        Assert.Equal(2, Regex.Matches(sql, @"OVER \(\)").Count);
        Assert.DoesNotContain("pg_buffercache_summary", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AppliesTo_EveryPostgresTarget()
    {
        foreach (var major in new[] { 13, 14, 16, 17, 18 })
        {
            Assert.True(PgBufferUsageCollector.Instance.AppliesTo(
                new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major }));
        }
    }

    /// <summary>One SELECT alias per payload column, in order — a mismatch is a silently shifted binary
    /// COPY, which writes every value into the wrong column rather than failing.</summary>
    [Fact]
    public void SelectAliases_MatchThePayloadOrder()
    {
        var expected = PgBufferUsageCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        var lines = Sql().Split('\n');
        var outerSelect = Array.FindLastIndex(lines, l => l.TrimEnd() == "SELECT");

        Assert.True(outerSelect >= 0, "the outer SELECT is no longer where this test looks for it");

        var selected = lines
            .Skip(outerSelect)
            .Where(line => !line.TrimStart().StartsWith("FROM", StringComparison.Ordinal)
                        && !line.TrimStart().StartsWith("LEFT JOIN", StringComparison.Ordinal))
            .Select(line => Regex.Match(line, @"\bAS\s+([a-z_]+),?\s*$"))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value)
            .ToArray();

        Assert.Equal(expected, selected);
    }
}
