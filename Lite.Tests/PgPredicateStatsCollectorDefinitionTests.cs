/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// Pins the predicate collector on the scope error that would corrupt it and the sampling fact that would
/// make its numbers lie.
/// </summary>
public class PgPredicateStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 8, 25, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170000,
            },
        };

    private static string Sql => PgPredicateStatsCollector.Instance.BuildQuery(MakeContext()).Text;

    [Fact]
    public void Identity_IsTheTableAndEngineTheStoreExpects()
    {
        Assert.Equal("pg_predicate_stats", PgPredicateStatsCollector.Instance.Name);
        Assert.Equal("pg_predicate_stats", PgPredicateStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgPredicateStatsCollector.Instance.TargetEngine);
    }

    /// <summary>
    /// <b>The assertion that matters most.</b> <c>pg_qualstats()</c> returns cluster-wide rows keyed by
    /// <c>dbid</c>, but <c>lrelid</c>/<c>lattnum</c> are OIDs meaningful only inside their own database and
    /// <c>pg_class</c>/<c>pg_attribute</c> are per-database catalogs. Without the scope filter the join
    /// either silently drops other databases' rows or resolves them against whatever local object shares
    /// the OID and reports a confident wrong column name. Measured: two cross-database rows present, neither
    /// colliding locally that day — so the failure would have been silent loss, and different OID luck would
    /// have produced the wrong name instead.
    /// </summary>
    [Fact]
    public void ItScopesToTheConnectedDatabase_AndRunsPerDatabase()
    {
        Assert.Contains("q.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())",
            Sql, StringComparison.Ordinal);

        Assert.True(PgPredicateStatsCollector.Instance.RunsPerDatabase(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 }));

        /* Per-database, so the rows must be attributable — the #2599 invariant. */
        Assert.Contains("database_name", PgPredicateStatsCollector.Instance.PayloadColumns.Select(c => c.Name));
    }

    /// <summary>
    /// The sample rate is stored, because the counts are a sample and the default is not 1.
    /// <c>pg_qualstats.sample_rate</c> defaults to <c>1/max_connections</c> — 0.01 on the rig, where the
    /// function returned ZERO rows on a server that had just run the queries it was meant to record.
    /// </summary>
    [Fact]
    public void TheSampleRate_IsStoredWithTheCounts()
    {
        Assert.Contains("pg_qualstats.sample_rate", Sql, StringComparison.Ordinal);
        Assert.Contains("sample_rate", PgPredicateStatsCollector.Instance.PayloadColumns.Select(c => c.Name));

        /* Missing-ok, like every GUC read here: an absent setting degrades one column, not the collection. */
        foreach (Match call in Regex.Matches(Sql, @"current_setting\(([^)]*)\)"))
        {
            Assert.Contains(", true", call.Groups[1].Value, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Ranked by rows filtered — the index-candidate signal. Ranking by sample count would order the grid
    /// by how often the SAMPLER happened to fire, which is a property of the extension's configuration
    /// rather than of the workload.
    /// </summary>
    [Fact]
    public void ItRanksByRowsFiltered_NotBySampleCount()
    {
        Assert.Matches(new Regex(@"ORDER BY\s+sum\(q\.nbfiltered\) DESC"), Sql);
    }

    /// <summary>
    /// An extension function lives where the extension was created, not in <c>pg_catalog</c>.
    /// </summary>
    [Fact]
    public void TheExtensionFunction_IsQualifiedWhereItActuallyLives()
    {
        Assert.Contains("public.pg_qualstats()", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_catalog.pg_qualstats", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The operator is stored as its SYMBOL. An OID would need a second lookup to mean anything, in a
    /// catalog that is per-database — and which operator it is decides whether an index would help at all.
    /// </summary>
    [Fact]
    public void TheOperatorIsStoredAsASymbol()
    {
        Assert.Contains("operator", PgPredicateStatsCollector.Instance.PayloadColumns.Select(c => c.Name));
        Assert.Contains("o.oprname", Sql, StringComparison.Ordinal);
    }
}
