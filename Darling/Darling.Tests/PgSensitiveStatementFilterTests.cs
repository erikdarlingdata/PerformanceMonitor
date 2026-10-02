/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 census: <see cref="PgSensitiveStatementFilter"/> holds the ONE definition of the sensitive-statement
/// pattern and placeholder text, and every fetch/collection query that stores a monitored target's statement
/// text verbatim references it (not a copy). The corpus that judges what the pattern actually matches runs
/// against a real PostgreSQL (<c>PgSensitiveStatementFilterLiveTests</c>) — the pattern is POSIX ARE syntax
/// (<c>[[:&lt;:]]</c>/<c>[[:&gt;:]]</c>/<c>[[:space:]]</c>) .NET's <see cref="System.Text.RegularExpressions.Regex"/>
/// cannot parse (confirmed: constructing a <c>Regex</c> from the exact pattern text throws
/// <c>RegexParseException</c> on the POSIX class syntax), so this file only proves the census, not the match
/// behavior.
/// </summary>
public sealed class PgSensitiveStatementFilterTests
{
    /// <summary>
    /// One definition in the whole repo. Every occurrence of the pattern's opening token
    /// (<c>[[:&lt;:]](create|alter)</c>) must be the ONE declaration in <see cref="PgSensitiveStatementFilter"/>
    /// — a second literal copy anywhere else is exactly the drift #4348 exists to prevent. The needle itself
    /// necessarily also appears in THIS file's source (as the string literal below), so that one occurrence
    /// is excluded by file identity rather than counted as a second definition.
    /// </summary>
    [Fact]
    public void ThePatternHasExactlyOneDefinitionInTheRepo()
    {
        var selfFileFull = ThisFile();
        var repoRoot = FindRepoRoot();
        var needle = "[[:<:]](create|alter)";
        var hits = 0;
        string? onlyFile = null;

        foreach (var file in Directory.EnumerateFiles(repoRoot, "*.cs", SearchOption.AllDirectories))
        {
            if (file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) ||
                file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            // This test file itself contains the needle above (as this very string literal) — a census
            // hit on the test's own source is not a second definition, so it is excluded deliberately,
            // by CallerFilePath rather than a name check.
            if (Path.GetFullPath(file) == selfFileFull)
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (text.Contains(needle, StringComparison.Ordinal))
            {
                hits++;
                onlyFile = file;
            }
        }

        Assert.Equal(1, hits);
        Assert.NotNull(onlyFile);
        Assert.EndsWith("PgSensitiveStatementFilter.cs", onlyFile, StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="StoreStatementStats.SensitiveStatementPattern"/> is an alias, not a second copy — same
    /// string reference, so the store and every collector reading a target's own statement text can never
    /// drift apart on what counts as sensitive.
    /// </summary>
    [Fact]
    public void StoreStatementStats_AliasesTheSharedPattern()
    {
        Assert.Same(PgSensitiveStatementFilter.SensitiveStatementPattern, StoreStatementStats.SensitiveStatementPattern);
    }

    /// <summary>
    /// <c>PgStatementText</c>'s fetch queries reference the shared filter's pattern and placeholder text —
    /// literally present in the built SQL, not a re-typed copy.
    /// </summary>
    [Fact]
    public void PgStatementText_FetchSql_ReferencesTheSharedFilter()
    {
        var predicate = PgSensitiveStatementFilter.SqlPredicate("query_text");

        Assert.Contains(predicate, PerformanceMonitor.Darling.Storage.PgStatementText.AuroraFetchSql, StringComparison.Ordinal);
        Assert.Contains(predicate, PerformanceMonitor.Darling.Storage.PgStatementText.VanillaFetchSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>PgBlockingCollector</c>'s query references the shared filter's pattern and placeholder text for
    /// both the blocked and blocking sides.
    /// </summary>
    [Fact]
    public void PgBlockingCollector_QueryText_ReferencesTheSharedFilter()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "test",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
        };
        var query = PgBlockingCollector.Instance.BuildQuery(context).Text;

        Assert.Contains(PgSensitiveStatementFilter.SqlPredicate("blocked.query"), query, StringComparison.Ordinal);
        Assert.Contains(PgSensitiveStatementFilter.SqlPredicate("blocker.query"), query, StringComparison.Ordinal);
        Assert.Equal(2, CountOccurrences(query, PgSensitiveStatementFilter.PlaceholderText));
    }

    /// <summary>
    /// A literal, hard-coded expected string — not built from <see cref="PgSensitiveStatementFilter.SqlLiteral"/>
    /// or <see cref="PgSensitiveStatementFilter.SqlPredicate"/> themselves — so a quoting bug in either shared
    /// builder (say, a dropped doubled quote) changes what this test compares against without changing the
    /// value under test, and fails loudly instead of passing silently the way the other tests' own
    /// <c>Contains(SqlPredicate(...))</c> pins would (they build their expected value with the same builder
    /// they are checking).
    /// </summary>
    [Fact]
    public void SqlPredicate_MatchesTheLiteralExpectedSql()
    {
        const string expected =
            "CASE WHEN c ~* '[[:<:]](create|alter)([[:space:]]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^[:cntrl:]]*)+" +
            "(role|user|group|subscription|server)[[:>:]]|[[:<:]]password[[:>:]]([[:space:]]|/[*]([^*]|[*]+[^*/])*[*]+/|" +
            "--[^[:cntrl:]]*)*(=|to)?([[:space:]]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^[:cntrl:]]*)*(e?''|u&''|[$][^0-9])|" +
            "[[:<:]](pg)?password[[:space:]]*=[[:space:]]*[^$[:space:]]|[a-z][a-z0-9+.-]*://[^[:space:]/@:]+:[^[:space:]/@]+@' " +
            "THEN '-- statement text withheld (#4348)' ELSE c END";

        Assert.Equal(expected, PgSensitiveStatementFilter.SqlPredicate("c"));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    /// <summary>This test file's own full path, so the census below can exclude the needle's literal
    /// occurrence in this very source file without naming the file by string.</summary>
    private static string ThisFile([CallerFilePath] string thisFile = "") => Path.GetFullPath(thisFile);

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        Assert.NotNull(dir);
        return dir!.FullName;
    }
}
