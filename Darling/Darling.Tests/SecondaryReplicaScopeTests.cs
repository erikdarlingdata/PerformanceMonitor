/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5558: <see cref="PgSecondaryReplicaScope"/>'s predicate builder and its fail-open read, with no database. The seeded
/// twins of Lite's fact tests are in <see cref="SecondaryReplicaScopeLiveTests"/>.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class PgSecondaryReplicaScopeTests
{
    private static AnalysisContext ContextWith(params string[] secondaries) => new()
    {
        SecondaryReplicaDatabases = new HashSet<string>(secondaries, StringComparer.Ordinal),
    };

    [Fact]
    public void Apply_WithNothingToSkip_JustRemovesTheMarker_AndAddsNoParameter()
    {
        using var cmd = new NpgsqlCommand("SELECT 1 FROM t WHERE a = $1 /*SEC*/ ORDER BY 1");
        cmd.Parameters.AddWithValue(1);

        PgSecondaryReplicaScope.Apply(cmd, ContextWith(), "DB_CONFIG", "database_name");

        Assert.DoesNotContain("/*SEC*/", cmd.CommandText, StringComparison.Ordinal);
        Assert.Equal("SELECT 1 FROM t WHERE a = $1  ORDER BY 1", cmd.CommandText);
        Assert.Single(cmd.Parameters);
    }

    [Fact]
    public void Apply_WithASecondary_BindsTheNamesAsTheyAreAsOneArray_NumberedAfterTheExistingParameters()
    {
        using var cmd = new NpgsqlCommand("SELECT 1 FROM t WHERE a = $1 /*SEC*/ ORDER BY 1");
        cmd.Parameters.AddWithValue(1);

        PgSecondaryReplicaScope.Apply(cmd, ContextWith("SecDb", "OtherDb"), "DB_CONFIG", "database_name");

        Assert.Contains("AND (database_name IS NULL OR database_name <> ALL($2::text[]))", cmd.CommandText, StringComparison.Ordinal);
        Assert.Equal(2, cmd.Parameters.Count);
        var names = Assert.IsType<string[]>(cmd.Parameters[1].Value);
        Assert.Equal(["OtherDb", "SecDb"], names.OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void Apply_MatchesTheNameExactly_ASecondarySalesDbDoesNotHideASeparateSalesdb()
    {
        /* #5558 round 2: on a case-sensitive server collation, SalesDb (a secondary copy) and salesdb (a standalone
           database) are two databases. The set carries only the secondary's exact name and the predicate compares
           it as is, so the standalone one keeps its rows. */
        using var cmd = new NpgsqlCommand("SELECT 1 FROM t WHERE a = $1 /*SEC*/");
        cmd.Parameters.AddWithValue(1);

        PgSecondaryReplicaScope.Apply(cmd, ContextWith("SalesDb"), "DB_CONFIG", "database_name");

        Assert.Equal(["SalesDb"], Assert.IsType<string[]>(cmd.Parameters[1].Value));
        Assert.DoesNotContain("lower(", cmd.CommandText, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5558 round 2 (L4): the regressed-queries drill-down has two SQL variants, the raw one and the interval-table one, and
    /// the live drill-down test runs whichever the seeded store picks. Each must carry the marker exactly once and before its
    /// <c>LIMIT 5</c>; a variant that lost it would leave <c>Apply</c> binding a parameter nothing references and the filter
    /// would silently not run there.
    /// </summary>
    [Theory]
    [InlineData("raw")]
    [InlineData("table")]
    public void EachRegressedQueriesVariant_CarriesTheMarkerExactlyOnce_BeforeItsLimit(string variant)
    {
        var sql = variant == "raw" ? PgDrillDownCollector.RegressedQueriesSql : PgDrillDownCollector.RegressedQueriesTableSql;

        var first = sql.IndexOf(PgSecondaryReplicaScope.Marker, StringComparison.Ordinal);
        Assert.True(first >= 0, variant + " variant lost the secondary-replica marker");
        Assert.Equal(-1, sql.IndexOf(PgSecondaryReplicaScope.Marker, first + PgSecondaryReplicaScope.Marker.Length, StringComparison.Ordinal));

        var limit = sql.IndexOf("LIMIT 5", first, StringComparison.Ordinal);
        Assert.True(limit > first, variant + " variant: the marker must come before its LIMIT 5, so the filter runs before the cut");
    }

    [Fact]
    public void Apply_UsesTheKeywordItIsGiven_ForAClauseThatOpensTheWhere()
    {
        using var cmd = new NpgsqlCommand("SELECT 1 FROM t /*SEC*/ GROUP BY 1");
        PgSecondaryReplicaScope.Apply(cmd, ContextWith("SecDb"), "PLAN_REGRESSION", "database_name", "WHERE");
        Assert.Contains(" WHERE (database_name IS NULL OR database_name <> ALL($1::text[]))", cmd.CommandText, StringComparison.Ordinal);
    }

    [Fact]
    public void Apply_LeavesANodeLocalKeyUnfiltered_EvenWithASecondarySet()
    {
        using var cmd = new NpgsqlCommand("SELECT 1 FROM t WHERE a = $1 /*SEC*/");
        cmd.Parameters.AddWithValue(1);

        PgSecondaryReplicaScope.Apply(cmd, ContextWith("SecDb"), "MISSING_INDEX", "database_name");

        Assert.DoesNotContain("/*SEC*/", cmd.CommandText, StringComparison.Ordinal);
        Assert.DoesNotContain("<> ALL(", cmd.CommandText, StringComparison.Ordinal);
        Assert.Single(cmd.Parameters);
    }

    [Fact]
    public void Apply_WithNoSetOnTheContext_SkipsNothing()
    {
        using var cmd = new NpgsqlCommand("SELECT 1 FROM t WHERE a = $1 /*SEC*/");
        cmd.Parameters.AddWithValue(1);

        PgSecondaryReplicaScope.Apply(cmd, new AnalysisContext(), "DB_CONFIG", "database_name");

        Assert.DoesNotContain("<> ALL(", cmd.CommandText, StringComparison.Ordinal);
        Assert.Single(cmd.Parameters);
    }

    [Fact]
    public async Task ReadAsync_FailsOpen_WhenTheStoreCannotBeReached()
    {
        /* A port nothing listens on: the connection fails at once, and the answer is the empty set, not a throw. */
        await using var unreachable = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=2;Pooling=false");

        var set = await PgSecondaryReplicaScope.ReadAsync(unreachable, 1, DateTime.UtcNow, logger: null, CancellationToken.None);
        Assert.Empty(set);

        Assert.Null(await PgSecondaryReplicaScope.NoteAsync(unreachable, 1, logger: null, CancellationToken.None));
    }

    [Fact]
    public async Task EnsureAsync_FillsTheSetOnce_AndLeavesAnAlreadyResolvedSetAlone()
    {
        await using var unreachable = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=2;Pooling=false");

        var context = new AnalysisContext { ServerId = 1, TimeRangeEnd = DateTime.UtcNow };
        await PgSecondaryReplicaScope.EnsureAsync(unreachable, context, logger: null);
        Assert.NotNull(context.SecondaryReplicaDatabases);
        Assert.Empty(context.SecondaryReplicaDatabases!);

        var resolved = new HashSet<string>(StringComparer.Ordinal) { "SecDb" };
        var already = new AnalysisContext { ServerId = 1, SecondaryReplicaDatabases = resolved };
        await PgSecondaryReplicaScope.EnsureAsync(unreachable, already, logger: null);
        Assert.Same(resolved, already.SecondaryReplicaDatabases);
    }

    [Fact]
    public void NoneSkipped_IsEmpty()
    {
        Assert.Empty(PgSecondaryReplicaScope.NoneSkipped);
    }
}

/// <summary>
/// #5558: every <c>new AnalysisContext</c> in the product code resolves the secondary replica set, either in its own
/// initializer or through <c>EnsureAsync</c> on the variable it is assigned to. A context built without it leaves
/// <see cref="AnalysisContext.SecondaryReplicaDatabases"/> null, which skips nothing, so the replicated findings of a
/// secondary copy would quietly come back for that one pass. A pass that reads only node-local facts says so by setting the
/// empty set on purpose. Scans Lite and Darling; test projects build contexts freely and are not scanned.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class AnalysisContextSecondaryScopeSourceScanTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "Directory.Build.props")))
            dir = Path.GetDirectoryName(dir);
        Assert.False(dir is null, "could not locate the repo root from the test source path");
        return dir!;
    }

    private static IEnumerable<string> ProductSources(string root)
    {
        foreach (var top in new[] { "Lite", "Darling" })
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(root, top), "*.cs", SearchOption.AllDirectories))
            {
                var relative = Path.GetRelativePath(root, file).Replace('\\', '/');
                if (relative.Contains("/bin/") || relative.Contains("/obj/") || relative.Contains(".Tests/")) continue;
                yield return file;
            }
        }
    }

    [Fact]
    public void EveryAnalysisContextInTheProduct_ResolvesTheSecondaryReplicaSet()
    {
        var root = RepoRoot();
        var construction = new Regex(@"(?:var\s+(?<name>\w+)\s*=\s*)?new\s+AnalysisContext\b", RegexOptions.CultureInvariant);
        var missing = new List<string>();
        var sites = 0;

        /* The end of the member a construction sits in: the next closing brace at member indentation (4 spaces).
           Each site is judged on its own text, so a second context in a file that already ensures a variable of the
           same name cannot borrow the first one's call. */
        var memberEnd = new Regex(@"\n    \}[ \t]*\n", RegexOptions.CultureInvariant);

        foreach (var file in ProductSources(root))
        {
            var text = File.ReadAllText(file).Replace("\r\n", "\n");
            foreach (Match m in construction.Matches(text))
            {
                sites++;
                var end = memberEnd.Match(text, m.Index);
                var limit = end.Success ? end.Index : text.Length;
                var member = text[m.Index..limit];
                /* The initializer's closing brace is searched only up to the member's end: an inline construction
                   such as Foo(new AnalysisContext { ... }); closes with "})", and an unbounded search would run on into
                   a LATER construction's initializer and borrow its SecondaryReplicaDatabases setting (#5558 round 2). */
                var close = text.IndexOf("};", m.Index, limit - m.Index, StringComparison.Ordinal);
                var initializer = close < 0 ? member : text[m.Index..close];
                var name = m.Groups["name"].Value;
                var inInitializer = initializer.Contains("SecondaryReplicaDatabases", StringComparison.Ordinal);
                var ensured = name.Length > 0 && Regex.IsMatch(member, @"SecondaryReplicaScope\.EnsureAsync\([^)]*\b" + Regex.Escape(name) + @"\b");

                /* A context handed straight to AnalyzeAsync(context) is resolved by that overload, provided the overload
                   itself ensures the set it receives (checked on the same file's declaration, so the hand-off cannot
                   be a loophole). */
                if (!inInitializer && !ensured && name.Length > 0 && Regex.IsMatch(member, @"\bAnalyzeAsync\(\s*" + Regex.Escape(name) + @"\s*\)"))
                {
                    var overload = Regex.Match(text, @"AnalyzeAsync\(AnalysisContext (?<p>\w+)\)");
                    if (overload.Success)
                    {
                        var overloadEnd = memberEnd.Match(text, overload.Index);
                        var body = overloadEnd.Success ? text[overload.Index..overloadEnd.Index] : text[overload.Index..];
                        ensured = Regex.IsMatch(body, @"SecondaryReplicaScope\.EnsureAsync\([^)]*\b" + overload.Groups["p"].Value + @"\b");
                    }
                }
                if (!inInitializer && !ensured)
                {
                    var line = text[..m.Index].Count(c => c == '\n') + 1;
                    missing.Add(Path.GetRelativePath(root, file).Replace('\\', '/') + ":" + line);
                }
            }
        }

        Assert.True(sites >= 10, "the scan found only " + sites + " construction sites, so its pattern no longer matches the code");
        Assert.True(missing.Count == 0,
            "an AnalysisContext that never resolves SecondaryReplicaDatabases would bring back a secondary copy's findings: " + string.Join(", ", missing));
    }
}

/// <summary>#5558: the viewer Recommendations view model carries the note only where a list or the all-clear shows.</summary>
[Trait("Reads", "Lite")]
public sealed class RecommendationsReplicaNoteTests
{
    [Fact]
    public void TheNote_IsCarriedOnTheEmptyAndLoadedStates_AndNullWhenNotSet()
    {
        var empty = RecommendationsViewModel.FromFindings(Array.Empty<ViewerFindingRow>(), "SQL2022");
        Assert.Null(empty.ReplicaNote);

        var note = AgReplicaScope.SkippedNote(2);
        Assert.Equal(note, empty.WithReplicaNote(note).ReplicaNote);
        Assert.Same(empty, empty.WithReplicaNote("  "));
        Assert.Null(empty.ReplicaNote);
        Assert.Equal(RecommendationsState.Empty, empty.State);
    }
}
