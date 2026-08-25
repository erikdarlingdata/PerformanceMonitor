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
/// #2543: per-column planner statistics. The load-bearing assertion here is a NEGATIVE one — that the two
/// value-bearing columns are never collected — because those hold raw customer data and a later refactor
/// that "completed" the column set would be a data-handling regression rather than an improvement.
/// </summary>
public class PgColumnStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgColumnStatsCollector.Instance.BuildQuery(new CollectorContext
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
    /// The value-bearing columns must never be selected. Measured on a realistic table, they return customer
    /// names, identifier fragments and live email addresses — collecting them copies customer data into the
    /// monitoring store under our retention, which is the same exposure as the <c>auto_explain</c> literals
    /// on #2538.
    ///
    /// <para>Asserted against the SELECT list specifically, so the comment explaining the exclusion cannot
    /// satisfy it — the source-pin trap this repo has hit repeatedly.</para>
    /// </summary>
    [Fact]
    public void TheValueBearingColumns_AreNeverSelected()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        Assert.DoesNotContain("most_common_vals", sql.Replace("cardinality(s.most_common_vals)", " "), StringComparison.Ordinal);
        Assert.DoesNotContain("histogram_bounds", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>most_common_vals</c> may be touched ONLY through <c>cardinality</c>, which reads the array's
    /// length and never its contents. That distinction is the whole reason a "how skewed" answer is
    /// available without a "skewed toward what" answer.
    /// </summary>
    [Fact]
    public void MostCommonVals_IsUsedOnlyForItsLength()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        foreach (Match match in Regex.Matches(sql, @"(\S*)most_common_vals"))
        {
            Assert.Equal("cardinality(s.", match.Groups[1].Value);
        }
    }

    /// <summary>
    /// Only the HEAD of the frequency array is taken. The first element is the skew signal; storing the
    /// whole array would be storing a distribution nobody reads past its head — and unlike the values array,
    /// frequencies are safe, so the restraint here is about volume rather than exposure.
    /// </summary>
    [Fact]
    public void OnlyTheTopFrequency_IsTaken()
        => Assert.Matches(new Regex(@"most_common_freqs\[1\]"), Sql());

    /// <summary>
    /// <c>n_distinct</c> must not be stored as an integer. Negative values are a RATIO of row count, so an
    /// integer column would let a read render "-1 distinct values" for a unique key — the commonest column
    /// shape there is.
    /// </summary>
    [Fact]
    public void NDistinct_IsNotAnIntegerColumn()
    {
        var column = PgColumnStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "n_distinct");

        Assert.Equal(CollectorColumnType.Double, column.Type);
    }

    /// <summary>
    /// A size floor, because statistics on a tiny table cannot produce a misestimate anyone notices — the
    /// planner is choosing between two cheap paths — and this is the widest fan-out of any collector here
    /// (columns x tables x databases).
    /// </summary>
    [Fact]
    public void ThereIsASizeFloor_SoTheLongTailIsNotCollected()
        => Assert.Matches(new Regex(@"c\.relpages\s*>=\s*\d+"), Sql());

    /// <summary>
    /// System schemas are excluded. Their statistics describe the catalog rather than the user's data, and
    /// they would swamp the result on any database with few user tables.
    /// </summary>
    [Fact]
    public void SystemSchemas_AreExcluded()
    {
        var sql = Sql();

        Assert.Contains("pg_catalog", sql, StringComparison.Ordinal);
        Assert.Contains("information_schema", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Per-database, because <c>pg_stats</c> describes the CONNECTED database only. A cluster-wide claim
    /// built from one database's statistics would be silently missing every table in every other one — the
    /// same scope mistake that has now appeared three times in this effort.
    /// </summary>
    [Fact]
    public void ItRunsPerDatabase()
        => Assert.True(PgColumnStatsCollector.Instance.RunsPerDatabase(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 }));

    [Fact]
    public void AppliesTo_EveryPostgresTarget()
    {
        foreach (var major in new[] { 13, 14, 16, 17, 18 })
        {
            Assert.True(PgColumnStatsCollector.Instance.AppliesTo(
                new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major }));
        }
    }

    /// <summary>
    /// Catalog reads are schema-qualified: <c>pg_catalog</c> is searched implicitly but not necessarily
    /// FIRST, so an unqualified read can resolve to an object a user created in a schema earlier in the
    /// monitoring login's search_path.
    /// </summary>
    [Fact]
    public void EveryCatalogRead_IsSchemaQualified()
    {
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        foreach (var view in new[] { "pg_stats", "pg_class", "pg_namespace" })
        {
            /* Excludes the string-literal mentions of pg_catalog in the schema-exclusion predicate, which
               are data rather than object references. */
            foreach (Match match in Regex.Matches(sql, $@"(\S*)\b{Regex.Escape(view)}\b"))
            {
                Assert.Equal("pg_catalog.", match.Groups[1].Value);
            }
        }
    }

    /// <summary>One SELECT alias per payload column, in order — a mismatch is a silently shifted binary
    /// COPY, which writes every value into the wrong column rather than failing.</summary>
    [Fact]
    public void SelectAliases_MatchThePayloadOrder()
    {
        var expected = PgColumnStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        var selected = Sql()
            .Split('\n')
            .Where(line => !line.TrimStart().StartsWith("FROM", StringComparison.Ordinal)
                        && !line.TrimStart().StartsWith("JOIN", StringComparison.Ordinal))
            .Select(line => Regex.Match(line, @"\bAS\s+([a-z_]+),?\s*$"))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value)
            .ToArray();

        Assert.Equal(expected, selected);
    }
}
