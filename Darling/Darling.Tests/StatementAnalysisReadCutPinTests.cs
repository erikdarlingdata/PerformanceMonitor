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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320: no Darling analysis read cuts statement text in SQL. The text comes back whole, the statement filter
/// judges it, and <see cref="AnalysisStatementText"/> cuts it after. Plans are PR C's and relation names are not
/// statement text, so neither is scanned for.
/// </summary>
public sealed class StatementAnalysisReadCutPinTests
{
    [Fact]
    public void NoAnalysisReadCutsStatementTextInSql()
    {
        var dir = Path.GetDirectoryName(typeof(PgDrillDownCollector).Assembly.Location)!;
        var root = dir;
        while (root is not null && !Directory.Exists(Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Analysis")))
            root = Path.GetDirectoryName(root);
        Assert.NotNull(root);

        var offenders = Directory.GetFiles(Path.Combine(root!, "Darling", "PerformanceMonitor.Darling.Analysis"), "*.cs")
            .Where(f => !f.EndsWith("PgDrillDownCollector.Plans.cs", StringComparison.Ordinal))
            .SelectMany(f => CutSites(File.ReadAllText(f)).Select(line => Path.GetFileName(f) + ":" + line))
            .ToList();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// The 1-based line of every SQL cut of a statement-named column in one C# source. Reads the string literals
    /// through <see cref="CSharpSourceWalker.StringLiteralBodies"/> rather than splitting lines on a comment prefix,
    /// so a block comment's continuation lines (no leading asterisk) are not read as SQL, and the SQL in a
    /// verbatim or raw literal is (#3052).
    /// </summary>
    private static List<int> CutSites(string source)
    {
        var sites = new List<int>();

        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(body,
                @"LEFT\(\s*(MAX\(|COALESCE\()?\s*\w*\.?(query_text|query_sql_text|victim_sql_text|blocked_sql_text|blocking_sql_text|latest_victim_statement|latest_graph_text)\b"))
            {
                var line = 1;
                for (var i = 0; i < start && i < source.Length; i++)
                {
                    if (source[i] == '\n') line++;
                }

                sites.Add(line + body.Take(m.Index).Count(c => c == '\n'));
            }
        }

        return sites;
    }

    [Fact]
    public void TheCutScan_ReadsLiteralsNotComments_AndFindsACutInEitherLiteralForm()
    {
        const string fixture = """
            class Probe
            {
                /* A note that mentions LEFT(query_text, 200) in a block comment
                   LEFT(MAX(t.query_text), 5) on a line with no asterisk. */
                // LEFT(query_text, 9) in a line comment
                const string Plain = "SELECT LEFT(MAX(t.query_text), 200) FROM q";
                const string Verbatim = @"SELECT 1,
                    LEFT(COALESCE(d.query_text, ''), 10) FROM q";
            }
            """;

        var sites = CutSites(fixture);

        Assert.Equal(2, sites.Count);
        Assert.Equal(new[] { 6, 8 }, sites.ToArray());
    }

    [Fact]
    public void Preview_JudgesTheWholeText_ThenCutsByCodePoint()
    {
        var named = StatementScrubCanary.UriStatement(500);
        Assert.Equal(SensitiveStatements.PlaceholderText, AnalysisStatementText.Preview(named, 500));

        var plain = "SELECT " + new string('x', 700);
        Assert.Equal(plain[..500], AnalysisStatementText.Preview(plain, 500));
        Assert.Null(AnalysisStatementText.Preview(null, 500));

        /* PostgreSQL's LEFT counts code points: 3 astral characters are 6 UTF-16 units and still 3 characters. */
        var astral = "\U0001F600\U0001F600\U0001F600\U0001F600";
        Assert.Equal("\U0001F600\U0001F600\U0001F600", AnalysisStatementText.CutCodePoints(astral, 3));
        Assert.Equal("ab", AnalysisStatementText.CutCodePoints("abcd", 2));
        Assert.Equal("abcd", AnalysisStatementText.CutCodePoints("abcd", 4));
    }
}
