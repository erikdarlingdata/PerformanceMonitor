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
/// #2561: measured b-tree index bloat. The two assertions that matter are that non-btree indexes can never
/// reach <c>pgstatindex</c> (it RAISES on them, so one GIN index would take the whole collection down) and
/// that no derived bloat percentage is stored.
/// </summary>
public class PgIndexBloatCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgIndexBloatCollector.Instance.BuildQuery(new CollectorContext
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
    /// Only b-trees may reach the function. Verified against a live server: GIN, BRIN and hash each raise
    /// <c>relation "x" is not a btree index</c>, so a single one of them would fail the collection every
    /// cycle.
    /// </summary>
    [Fact]
    public void OnlyBtreeIndexes_AreCandidates()
        => Assert.Matches(new Regex(@"am\.amname\s*=\s*'btree'"), Sql());

    /// <summary>
    /// The btree filter sits behind an <c>OFFSET 0</c> fence so it is applied BEFORE the function call
    /// rather than alongside it. In testing the planner did filter first without the fence — but when the
    /// failure mode is the entire collection erroring, correctness should not depend on plan shape.
    /// </summary>
    [Fact]
    public void TheCandidateFilter_IsFencedFromTheFunctionCall()
    {
        var sql = Sql();

        Assert.Contains("OFFSET 0", sql, StringComparison.Ordinal);

        var fence = sql.IndexOf("OFFSET 0", StringComparison.Ordinal);
        var call = sql.IndexOf("pgstatindex", StringComparison.Ordinal);

        Assert.True(call > fence, "pgstatindex is called before the candidate fence, so the btree filter may not have been applied yet");
    }

    /// <summary>
    /// A LEFT join, so an index over the measurement ceiling still produces a row. A cross join would drop
    /// exactly the indexes most likely to be holding reclaimable space.
    /// </summary>
    [Fact]
    public void TheFunctionJoin_IsLeft_SoSkippedIndexesStillAppear()
        => Assert.Matches(new Regex(@"LEFT JOIN LATERAL\s+public\.pgstatindex"), Sql());

    /// <summary>
    /// Skipping is recorded, never silent. A size cap that made indexes disappear would read as "no bloat
    /// here" on precisely the biggest ones.
    /// </summary>
    [Fact]
    public void SkippedIndexes_CarryAReason()
    {
        Assert.Contains("skipped_reason", PgIndexBloatCollector.Instance.PayloadColumns.Select(c => c.Name));
        Assert.Contains("skipped_reason", Sql(), StringComparison.Ordinal);
    }

    /// <summary>
    /// The density is stored raw. A freshly built index measures near 90 — between 89.98 and 91.48 across
    /// the seven measured while designing this — so a stored "bloat percent" would bake in a false floor
    /// that also varies per index.
    /// </summary>
    [Fact]
    public void TheRawDensityIsStored_AndNoDerivedBloatPercent()
    {
        var names = PgIndexBloatCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("avg_leaf_density", names);
        Assert.DoesNotContain(names, n => n.Contains("bloat_pct", StringComparison.Ordinal)
                                       || n.Contains("bloat_percent", StringComparison.Ordinal));
    }

    /// <summary>
    /// <c>pgstatindex</c> is an EXTENSION function and lives where pgstattuple was created, so it is
    /// qualified <c>public.</c> — qualifying it <c>pg_catalog.</c> does not resolve at all (verified), and
    /// leaving it unqualified would let an object earlier in <c>search_path</c> shadow it.
    /// </summary>
    [Fact]
    public void TheExtensionFunction_IsQualifiedPublic_NotPgCatalog()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        Assert.Contains("public.pgstatindex", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_catalog.pgstatindex", sql, StringComparison.Ordinal);
    }

    /// <summary>Primaries only: a standby's index files are byte-identical by replication, so measuring
    /// both pays the full-index read twice for one answer.</summary>
    [Fact]
    public void AppliesTo_PrimariesOnly()
    {
        Assert.True(PgIndexBloatCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 }));
        Assert.False(PgIndexBloatCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsInRecovery = true }));
    }

    /// <summary>Only valid, ready indexes — an invalid one from a failed CREATE INDEX CONCURRENTLY has no
    /// meaningful bloat and is a different finding entirely.</summary>
    [Fact]
    public void InvalidIndexes_AreExcluded()
    {
        var sql = Sql();

        Assert.Contains("x.indisvalid", sql, StringComparison.Ordinal);
        Assert.Contains("x.indisready", sql, StringComparison.Ordinal);
    }
}
