/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
            .SelectMany(f => File.ReadAllLines(f).Select((line, i) => (File: Path.GetFileName(f), Line: i + 1, Text: line)))
            .Where(x => !x.Text.TrimStart().StartsWith("///", StringComparison.Ordinal) && !x.Text.TrimStart().StartsWith("//", StringComparison.Ordinal))
            .Where(x => System.Text.RegularExpressions.Regex.IsMatch(x.Text,
                @"LEFT\(\s*(MAX\(|COALESCE\()?\s*\w*\.?(query_text|query_sql_text|victim_sql_text|blocked_sql_text|blocking_sql_text|latest_victim_statement|latest_graph_text)\b"))
            .Select(x => x.File + ":" + x.Line)
            .ToList();

        Assert.Empty(offenders);
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
