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
using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320: statement text is judged WHOLE, then cut (<c>McpHelpers.TruncateStatement</c> / <c>StatementPreview</c>). A cut
/// BEFORE the judge lets a value early in a batch pass while the text that names it sits past the cut. This scan fails
/// when either shape appears in the Lite MCP and service code or in the Darling service and analysis code:
/// <list type="number">
/// <item>C#: <c>Truncate(x, n)</c>, <c>x.Substring(...)</c> or <c>x[..n]</c> where <c>x</c> is named like statement text
/// (<c>sql</c>, <c>query_text</c>, <c>statement</c>, <c>inputbuf</c>, <c>graph</c>, <c>stmt</c>).</item>
/// <item>SQL in a C# string literal: <c>LEFT(</c>, <c>SUBSTRING(</c> or <c>SUBSTR(</c> over such a column.</item>
/// </list>
/// The scanner is pinned with planted sources; the real trees are held to a short, named list of exceptions and to the
/// <see cref="PendingD3"/> set (the Darling analysis readers lane D3 is rewriting).
/// </summary>
public sealed class StatementCutSourceScanTests
{
    /// <summary>Folders scanned, repo-root relative. Lite MCP and services; Darling service and analysis.</summary>
    private static readonly string[] ScannedTrees =
    {
        "Lite/Mcp",
        "Lite/Services",
        "Darling/PerformanceMonitor.Darling.Service",
        "Darling/PerformanceMonitor.Darling.Analysis",
    };

    /// <summary>A name that says the value is statement text. Lower-cased identifier or column, substring match.</summary>
    private static readonly Regex StatementName = new(
        "sql|query_?text|statement|inputbuf|graph|stmt", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex TruncateCall = new(@"\bTruncate\s*\(", RegexOptions.Compiled);
    private static readonly Regex SubstringCall = new(@"([\w][\w\.\?!]*)\s*\.\s*Substring\s*\(", RegexOptions.Compiled);
    private static readonly Regex RangeCut = new(@"([\w][\w\.\?!]*)\s*\[[^\]\[]*\.\.[^\]\[]*\]", RegexOptions.Compiled);
    private static readonly Regex SqlCut = new(@"\b(left|substring|substr)\s*\(", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    internal sealed record Hit(string Kind, int Line, string Expression);

    private sealed record Allowed(string File, string Expression, int Count, string Reason);

    /// <summary>
    /// Cuts of statement-named values that are allowed to stay, each by file and expression. The count is exact, so
    /// an exception that stops being needed (or doubles) fails here and is removed or re-argued on purpose.
    /// </summary>
    private static readonly Allowed[] Exceptions =
    {
        new("Lite/Services/LocalDataService.FinOps.Recommendations.cs", "left(@sql,len(@sql)-10)", 1,
            "trims the trailing UNION ALL off the dynamic script this method is building; the script is never shown as a statement"),
        new("Lite/Services/LocalDataService.FinOps.Workload.cs", "left(query_text,200)", 1,
            "QueryPreview: the desktop grid cell; the same row carries FullQueryText whole and no Lite MCP tool reads QueryPreview"),
        new("Lite/Services/LocalDataService.FinOps.Workload.cs", "left(qs2.query_text,200)", 1,
            "SampleQueryText: the desktop grid cell; the same row carries FullQueryText whole and no Lite MCP tool reads SampleQueryText"),
        new("Lite/Services/LocalDataService.QueryStats.cs", "left(query_text,120)", 1,
            "the desktop heatmap hover (GetQueryHeatmapAsync); the get_query_heatmap tool reads LocalDataService.QueryHeatmap.cs, which fetches whole text and cuts after the filter"),
        new("Lite/Services/LocalDataService.WaitStats.cs", "substring(r.query_text,1,300)", 1,
            "alert-candidate read: the text goes to AlertContextBuilders.TruncateText, which cuts the same 300; alert text is not an MCP answer and no statement judge runs on it"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingAlertReadAdapter.cs", "substring(r.query_text,1,300)", 1,
            "alert-candidate read: the text goes to AlertContextBuilders.TruncateText, which cuts the same 300; alert text is not an MCP answer and no statement judge runs on it"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWebDeadlockGraph.cs", "p.sqltext", 1,
            "the web viewer's side panel cuts one process's text out of deadlock_graph_xml for display; web responses carry no statement judge (only the two MCP hosts run the output filter), and the viewer gets the whole XML beside it"),
        new("Darling/PerformanceMonitor.Darling.Analysis/PgTargetDrillDownCollector.Deadlocks.cs",
            "left(encode(sha256(convert_to(latest_graph_text,'utf8')),'hex'),32)", 1,
            "an identity hash of the raw graph, compared with latest_hash and never shown"),
    };

    /// <summary>
    /// Sites in lane D3's files (the Darling analysis readers: <c>PgDrillDownCollector.*</c>, <c>PgFactCollector.Activity.cs</c>,
    /// <c>PgPileupSnapshotReader.cs</c>, <c>PgTargetDrillDownCollector.*</c>, <c>DarlingMcpBlockingTools.cs</c>) that cut
    /// statement text in SQL before any filter. The scan finding these on this branch is lane D3's RED proof. The count is a
    /// ceiling: the test tolerates fewer, and the seat EMPTIES this set when it merges D3.
    /// <para><c>PgDrillDownCollector.Plans.cs</c> (PR C's) holds none the scan finds.</para>
    /// </summary>
    private static readonly Dictionary<string, int> PendingD3 = new()
    {
        ["Darling/PerformanceMonitor.Darling.Analysis/PgDrillDownCollector.Blocking.cs"] = 12,
        ["Darling/PerformanceMonitor.Darling.Analysis/PgDrillDownCollector.Queries.cs"] = 9,
        ["Darling/PerformanceMonitor.Darling.Analysis/PgFactCollector.Activity.cs"] = 1,
        ["Darling/PerformanceMonitor.Darling.Analysis/PgPileupSnapshotReader.cs"] = 1,
        ["Darling/PerformanceMonitor.Darling.Analysis/PgTargetDrillDownCollector.Deadlocks.cs"] = 3,
        ["Darling/PerformanceMonitor.Darling.Analysis/PgTargetDrillDownCollector.Queries.cs"] = 2,
    };

    // ------------------------------------------------------------------ the scanner

    /// <summary>Every cut of a statement-named value in one C# source: the C# shapes and the SQL-in-literal shape.</summary>
    internal static List<Hit> Scan(string source)
    {
        var hits = new List<Hit>();

        var code = CSharpSourceWalker.StripCommentsAndStrings(source);

        foreach (Match m in TruncateCall.Matches(code))
        {
            var arg = FirstArgument(code, m.Index + m.Length - 1);
            if (StatementName.IsMatch(arg)) hits.Add(new Hit("C# Truncate", LineOf(code, m.Index), Normalize(arg)));
        }

        foreach (Match m in SubstringCall.Matches(code))
        {
            if (StatementName.IsMatch(m.Groups[1].Value))
                hits.Add(new Hit("C# Substring", LineOf(code, m.Index), Normalize(m.Groups[1].Value)));
        }

        foreach (Match m in RangeCut.Matches(code))
        {
            if (StatementName.IsMatch(m.Groups[1].Value))
                hits.Add(new Hit("C# range", LineOf(code, m.Index), Normalize(m.Groups[1].Value)));
        }

        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            foreach (Match m in SqlCut.Matches(body))
            {
                var open = m.Index + m.Length - 1;
                var arg = FirstArgument(body, open);
                if (!StatementName.IsMatch(arg)) continue;

                var whole = Normalize(m.Groups[1].Value + "(" + CallBody(body, open) + ")");
                hits.Add(new Hit("SQL " + m.Groups[1].Value.ToLowerInvariant(), LineOf(source, start + m.Index), whole));
            }
        }

        return hits;
    }

    private static string Normalize(string s) =>
        Regex.Replace(s, @"\s+", "").ToLowerInvariant();

    private static int LineOf(string text, int index)
    {
        var line = 1;
        for (var i = 0; i < index && i < text.Length; i++)
        {
            if (text[i] == '\n') line++;
        }

        return line;
    }

    /// <summary>The first argument of the call whose '(' is at <paramref name="open"/>: up to a top-level comma or the closing paren.</summary>
    private static string FirstArgument(string text, int open)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '(') depth++;
            else if (c == ')')
            {
                depth--;
                if (depth == 0) return text.Substring(open + 1, i - open - 1);
            }
            else if (c == ',' && depth == 1) return text.Substring(open + 1, i - open - 1);
        }

        return text.Substring(open + 1);
    }

    /// <summary>Everything between the '(' at <paramref name="open"/> and its closing paren.</summary>
    private static string CallBody(string text, int open)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            if (text[i] == '(') depth++;
            else if (text[i] == ')' && --depth == 0) return text.Substring(open + 1, i - open - 1);
        }

        return text.Substring(open + 1);
    }

    private static IEnumerable<string> ScannedFiles() =>
        ScannedTrees
            .SelectMany(t => Directory.EnumerateFiles(Path.Combine(RepoFile.Root, t), "*.cs", SearchOption.AllDirectories))
            .Select(f => Path.GetRelativePath(RepoFile.Root, f).Replace('\\', '/'))
            .Where(f => !f.Contains("/obj/", StringComparison.Ordinal) && !f.Contains("/bin/", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal);

    // ------------------------------------------------------------------ the real trees

    [Fact]
    public void NoStatementTextIsCutBeforeItIsJudged_ExceptTheNamedSitesAndPendingD3()
    {
        var files = ScannedFiles().ToList();
        Assert.True(files.Count > 150, "the scan walked the trees: " + files.Count);

        var perFile = new Dictionary<string, List<Hit>>();
        foreach (var file in files)
        {
            var hits = Scan(RepoFile.ReadRepoFile(file.Split('/')));
            if (hits.Count > 0) perFile[file] = hits;
        }

        var problems = new List<string>();

        foreach (var (file, hits) in perFile.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            var remaining = new List<Hit>(hits);

            foreach (var allowed in Exceptions.Where(e => e.File == file))
            {
                var matched = remaining.Where(h => h.Expression.Contains(allowed.Expression, StringComparison.Ordinal)).ToList();
                if (matched.Count != allowed.Count)
                    problems.Add($"{file}: exception '{allowed.Expression}' expected {allowed.Count} site(s), found {matched.Count}");
                foreach (var h in matched) remaining.Remove(h);
            }

            if (PendingD3.TryGetValue(file, out var ceiling))
            {
                if (remaining.Count > ceiling)
                    problems.Add($"{file}: {remaining.Count} cut site(s), pending-D3 ceiling is {ceiling}");
                continue;
            }

            foreach (var h in remaining)
                problems.Add($"{file}:{h.Line}: {h.Kind} cuts statement text before the filter judges it: {h.Expression}");
        }

        foreach (var allowed in Exceptions)
        {
            if (!perFile.ContainsKey(allowed.File))
                problems.Add($"{allowed.File}: exception '{allowed.Expression}' expected {allowed.Count} site(s), found 0 (remove it)");
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    // ------------------------------------------------------------------ the scanner itself, on planted sources

    [Theory]
    [InlineData("var p = McpHelpers.Truncate(row.QueryText, 400);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(sqlText, 400);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(r.victim_sql_text ?? \"\", 400);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(inputbuf, 400);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(deadlockGraph, 400);", "C# Truncate")]
    [InlineData("var p = statement.Substring(0, 200);", "C# Substring")]
    [InlineData("var p = row.Statement?.Substring(0, 200);", "C# Substring")]
    [InlineData("var p = sql_text[..200];", "C# range")]
    [InlineData("var p = batch.QueryText[0..200];", "C# range")]
    public void TheScanner_CatchesACSharpCutOfStatementText(string source, string kind)
    {
        var hits = Scan("class C { void M() { " + source + " } }");

        Assert.Contains(hits, h => h.Kind == kind);
    }

    [Theory]
    [InlineData("SELECT LEFT(query_text, 500) AS q FROM t")]
    [InlineData("SELECT left(blocked_sql_text, 5) FROM t")]
    [InlineData("SELECT SUBSTRING(r.query_text, 1, 300) FROM t r")]
    [InlineData("SELECT substr(query_text, 1, 200) FROM t")]
    [InlineData("SELECT LEFT(MAX(v.query_text), 500) FROM t v")]
    [InlineData("SELECT LEFT(COALESCE(x.query_sql_text, l.query_text), 500) FROM t")]
    [InlineData("SELECT LEFT(latest_graph_text, 4000) FROM t")]
    [InlineData("SELECT LEFT(inputbuf, 4000) FROM t")]
    [InlineData("SELECT LEFT(victim_statement, $5) FROM t")]
    public void TheScanner_CatchesASqlCutOfStatementText(string sql)
    {
        var hits = Scan("class C { const string Q = @\"" + sql + "\"; }");

        Assert.Contains(hits, h => h.Kind.StartsWith("SQL ", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("var p = McpHelpers.Truncate(relationName, 63);")]
    [InlineData("var p = McpHelpers.Truncate(serverName, 40);")]
    [InlineData("var p = tableName.Substring(0, 10);")]
    [InlineData("var p = schema_name[..8];")]
    [InlineData("var p = McpHelpers.TruncateStatement(sqlText, 400);")]
    [InlineData("var p = McpHelpers.StatementPreview(row.QueryText, 400);")]
    [InlineData("// McpHelpers.Truncate(sqlText, 400) is what this used to be")]
    [InlineData("var note = \"McpHelpers.Truncate(sqlText, 400)\";")]
    public void TheScanner_LeavesAnythingThatIsNotACutOfStatementText(string source)
    {
        var hits = Scan("class C { void M() { " + source + " } }");

        Assert.Empty(hits);
    }

    [Theory]
    [InlineData("SELECT LEFT(relname, 63) FROM pg_class")]
    [InlineData("SELECT LEFT(server_name, 20) FROM servers")]
    [InlineData("SELECT LEFT(query_hash, 8) FROM t")]
    [InlineData("SELECT a FROM t LEFT JOIN (SELECT query_text FROM u) x ON x.id = t.id")]
    [InlineData("SELECT query_text FROM t")]
    public void TheScanner_LeavesSqlThatDoesNotCutStatementText(string sql)
    {
        var hits = Scan("class C { const string Q = @\"" + sql + "\"; }");

        Assert.Empty(hits);
    }

    [Fact]
    public void TheScanner_ReadsAnInterpolatedAndMultiLineLiteral_AndReportsTheLine()
    {
        var source = new StringBuilder()
            .AppendLine("class C")
            .AppendLine("{")
            .AppendLine("    string Q(int n) => $@\"SELECT a,")
            .AppendLine("        LEFT(query_text, {n}) AS q FROM t\";")
            .AppendLine("}")
            .ToString();

        var hits = Scan(source);

        var hit = Assert.Single(hits);
        Assert.Equal(4, hit.Line);
    }
}
