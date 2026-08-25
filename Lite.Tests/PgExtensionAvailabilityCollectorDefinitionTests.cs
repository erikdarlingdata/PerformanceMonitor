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
/// #2545: the extension capability axis. The assertions are about the DISTINCTIONS the collector has to
/// draw — four states rather than a boolean, and the two catalogs' different scopes — not about wording.
/// </summary>
public class PgExtensionAvailabilityCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgExtensionAvailabilityCollector.Instance.BuildQuery(new CollectorContext
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
    /// All four states must be expressible. A boolean would collapse <c>available</c> into <c>absent</c>,
    /// and <c>available</c> is the only ACTIONABLE one — the entire reason this axis exists.
    /// </summary>
    [Fact]
    public void AllFourStates_AreExpressible()
    {
        var sql = Sql();

        foreach (var state in new[] { "absent", "available", "outdated", "installed" })
        {
            Assert.Contains($"'{state}'", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// <c>outdated</c> must be decided by comparing the installed version against the server's default, and
    /// by EQUALITY only. Extension versions are free-form strings, so ordering them needs a parser this has
    /// no business carrying — and the server already names which one is default.
    /// </summary>
    [Fact]
    public void Outdated_ComparesVersionsForInequality_NeverOrdersThem()
    {
        var sql = Sql();

        Assert.Matches(new Regex(@"installed_version\s*<>\s*p?\.?default_version"), sql);
        Assert.DoesNotMatch(new Regex(@"installed_version\s*[<>]\s*(?!>)"), sql.Replace("<>", "!="));
    }

    /// <summary>
    /// The scope trap. <c>pg_extension</c> is PER-DATABASE and <c>pg_available_extensions</c> is
    /// cluster-wide — measured on one cluster reporting an extension installed in one database and not in
    /// another while the available list said yes in both. Both catalogs must be read, because either alone
    /// answers a narrower question than it appears to.
    /// </summary>
    [Fact]
    public void BothCatalogs_AreRead_BecauseTheirScopesDiffer()
    {
        var sql = Sql();

        Assert.Contains("pg_catalog.pg_available_extensions", sql, StringComparison.Ordinal);
        Assert.Contains("pg_catalog.pg_extension", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A FULL OUTER join, not an inner or a left one. An extension installed in this database but no longer
    /// offered by the server (a downgraded binary, a dropped contrib package) has a row in one catalog and
    /// not the other, and dropping it would hide the case most worth seeing.
    /// </summary>
    [Fact]
    public void TheCatalogsAreFullOuterJoined_SoNeitherSideIsLost()
        => Assert.Equal(2, Regex.Matches(Sql(), @"FULL OUTER JOIN").Count);

    /// <summary>
    /// Preload-only modules must NOT be in the relevant roster. <c>auto_explain</c> and
    /// <c>pg_wait_sampling</c> never appear in <c>pg_available_extensions</c> on ANY server — including ones
    /// actively running them — so listing them here would manufacture a permanent false <c>absent</c>.
    ///
    /// <para>This exact defect shipped once already (#2564, fixed in #2584), which is why it is pinned
    /// rather than left to the comment that explains it.</para>
    /// </summary>
    [Fact]
    public void PreloadOnlyModules_AreNotInTheRelevantRoster()
    {
        /* Scoped to the VALUES roster rather than the whole query, so the explanatory comment naming these
           modules does not satisfy the assertion — the source-pin trap this repo has hit repeatedly. */
        var roster = Regex.Match(Sql(), @"WITH relevant \(name, comment\) AS \(\s*VALUES(.*?)\n\)", RegexOptions.Singleline);

        Assert.True(roster.Success, "the relevant-extension roster is no longer where this test looks for it");
        Assert.DoesNotContain("auto_explain", roster.Groups[1].Value, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_wait_sampling", roster.Groups[1].Value, StringComparison.Ordinal);
    }

    /// <summary>
    /// The roster exists ONLY so absence is reportable — absence is not a row in any catalog, so it cannot
    /// be derived. Everything else about the result set is derived from the catalogs, which is why the
    /// roster can stay small and why a server's other extensions still get recorded.
    /// </summary>
    [Fact]
    public void TheRoster_NamesTheExtensionsThisProductCanActuallyUse()
    {
        var sql = Sql();

        foreach (var name in new[] { "pg_stat_statements", "pgstattuple", "pg_buffercache", "pg_stat_kcache" })
        {
            Assert.Contains($"('{name}'", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Catalog reads are schema-qualified: <c>pg_catalog</c> is searched implicitly but not necessarily
    /// FIRST, so an unqualified read can resolve to an object a user created in a schema earlier in the
    /// monitoring login's search_path — which here would mean fabricating the capability answer.
    /// </summary>
    [Fact]
    public void EveryCatalogRead_IsSchemaQualified()
    {
        /* COMMENTS STRIPPED FIRST. The query explains the per-database/cluster-wide scope split in a comment
           that necessarily names both catalogs, and an unstripped scan matches those mentions and fails on a
           perfectly qualified query. This repo has now hit that trap several times — a guard that greps for
           an identifier finds the comment ABOUT the identifier — so the assertion is made against code. */
        var sql = Regex.Replace(Sql(), @"/\*.*?\*/", " ", RegexOptions.Singleline);

        foreach (Match match in Regex.Matches(sql, @"(\S*)pg_available_extensions"))
        {
            Assert.Equal("pg_catalog.", match.Groups[1].Value);
        }

        /* Word-boundary matched, so the pg_extension_availability table name is not read as an unqualified
           catalog reference. */
        foreach (Match match in Regex.Matches(sql, @"(\S*)\bpg_extension\b"))
        {
            Assert.Equal("pg_catalog.", match.Groups[1].Value);
        }
    }

    [Fact]
    public void AppliesTo_EveryPostgresTarget()
    {
        foreach (var major in new[] { 13, 14, 16, 17, 18 })
        {
            Assert.True(PgExtensionAvailabilityCollector.Instance.AppliesTo(
                new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major }));
        }
    }

    /// <summary>
    /// Version columns are TEXT. Typing them as numbers is how a collector starts failing on a server whose
    /// extension is versioned <c>2.0-beta</c>.
    /// </summary>
    [Fact]
    public void VersionColumns_AreText()
    {
        var columns = PgExtensionAvailabilityCollector.Instance.PayloadColumns;

        foreach (var name in new[] { "installed_version", "default_version" })
        {
            Assert.Equal(CollectorColumnType.Varchar, columns.Single(c => c.Name == name).Type);
        }
    }

    /// <summary>One SELECT alias per payload column, in order — a mismatch is a silently shifted binary
    /// COPY, which writes every value into the wrong column rather than failing.</summary>
    [Fact]
    public void SelectAliases_MatchThePayloadOrder()
    {
        var expected = PgExtensionAvailabilityCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        /* Scoped to the OUTER select. The CTEs above it alias their own columns with the same keyword, so
           scanning the whole query collects three extra names and reads as an ordering bug in the collector
           rather than as the test looking in the wrong place. The outer SELECT is the only one at column
           zero, which is what this finds. */
        var lines = Sql().Split('\n');
        var outerSelect = Array.FindLastIndex(lines, l => l.TrimEnd() == "SELECT");

        Assert.True(outerSelect >= 0, "the outer SELECT is no longer where this test looks for it");

        var selected = lines
            .Skip(outerSelect)
            .Where(line => !line.TrimStart().StartsWith("FROM", StringComparison.Ordinal)
                        && !line.TrimStart().StartsWith("FULL OUTER JOIN", StringComparison.Ordinal))
            .Select(line => Regex.Match(line, @"\bAS\s+([a-z_]+),?\s*$"))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value)
            .ToArray();

        Assert.Equal(expected, selected);
    }
}
