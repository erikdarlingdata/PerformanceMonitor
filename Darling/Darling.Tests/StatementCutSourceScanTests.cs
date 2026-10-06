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
/// #5320: statement text is judged WHOLE, then cut (<c>McpHelpers.TruncateStatement</c> / <c>StatementPreview</c> /
/// <c>AnalysisStatementText.Preview</c>). Never cut, then judge: a cut BEFORE the judge lets a value early in a batch
/// pass while the text that names it sits past the cut. This scan fails when either shape appears in the Lite MCP,
/// service and analysis code or in the Darling service and analysis code:
/// <list type="number">
/// <item>C#: <c>Truncate(x, n)</c>, <c>x.Substring(...)</c>, <c>x[..n]</c>, <c>CutCodePoints(x, n)</c>, <c>RawCut(x)</c> or
/// <c>lines.Take(n)</c> (lines split out of a graph) where <c>x</c> is named like statement text (see
/// <see cref="StatementName"/>). A local that was assigned from a judge call in the same block, before the cut, is
/// judged text and its cut is the sanctioned order (<see cref="JudgedLocals"/>).</item>
/// <item>SQL in a C# string literal: <c>LEFT(</c>, <c>SUBSTRING(</c> or <c>SUBSTR(</c> over such a column, including a
/// column written as an interpolation hole (<c>LEFT({col}, n)</c>) whose name, or the constant it names, is statement text.</item>
/// </list>
/// The scanner is pinned with planted sources; the real trees are held to a short, named list of exceptions.
/// </summary>
public sealed class StatementCutSourceScanTests
{
    /// <summary>Folders scanned, repo-root relative. Lite MCP, services and analysis; Darling service and analysis.</summary>
    private static readonly string[] ScannedTrees =
    {
        "Lite/Mcp",
        "Lite/Services",
        "Lite/Analysis",
        "PerformanceMonitor.Alerting",
        "PerformanceMonitor.Notifications",
        "Darling/PerformanceMonitor.Darling.Service",
        "Darling/PerformanceMonitor.Darling.Analysis",
    };

    /// <summary>A name that says the value is statement text. Lower-cased identifier or column, substring match.</summary>
    private static readonly Regex StatementName = new(
        @"sql|query_?text|statement|inputbuf|graph|stmt|normalized|preview_?text|raw_?text|fetched_?text|text_?data"
        + @"|blocked_?process|current_?query|root_?query|query_?sample|sample_?text|(?:blocked|blocking|victim)_?query|\bquery\b|_query\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary><c>Truncate(x, n)</c> and the alert builders' <c>TruncateText(x, n)</c> (#5320: it cuts the alert
    /// context's statement text). <c>TruncateStatement</c> is the judge-then-cut helper and is not matched.</summary>
    private static readonly Regex TruncateCall = new(@"\bTruncate(?:Text)?\s*\(", RegexOptions.Compiled);
    private static readonly Regex SubstringCall = new(@"([\w][\w\.\?!]*)\s*\.\s*Substring\s*\(", RegexOptions.Compiled);
    private static readonly Regex RangeCut = new(@"([\w][\w\.\?!]*)\s*\[[^\]\[]*\.\.[^\]\[]*\]", RegexOptions.Compiled);
    private static readonly Regex SqlCut = new(@"\b(left|substring|substr)\s*\(", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The cut helpers that are not <c>Truncate</c>: the code-point cut and the pileup readers' <c>RawCut</c>.</summary>
    private static readonly Regex CutHelperCall = new(@"\b(CutCodePoints|RawCut)\s*\(", RegexOptions.Compiled);

    /// <summary><c>x.Take(n)</c>: the cut of a line list. Only a receiver that is statement text, or lines split out of it, counts.</summary>
    private static readonly Regex TakeCall = new(@"([\w][\w\.\?!]*)\s*\.\s*Take\s*\(", RegexOptions.Compiled);

    /// <summary>A call that judges statement text whole (the judge itself, or a helper that judges then cuts).</summary>
    private static readonly Regex JudgeCall = new(
        @"\b(?:SensitiveStatements\s*\.\s*(?:Text|Xml|Json)|AnalysisStatementText\s*\.\s*Preview|TruncateStatement|StatementPreview|FilterStoredPlan)\s*\(",
        RegexOptions.Compiled);

    private static readonly Regex Assignment = new(@"\b(\w+)\s*=(?![=>])", RegexOptions.Compiled);

    /// <summary>A string local, field or constant initialised from one literal: lets <c>LEFT({col}, n)</c> be read as the column <c>col</c> names.</summary>
    private static readonly Regex StringConstant = new(
        @"\b(?:string|var)\s+(\w+)\s*=\s*(\$?@?""[^;]*);", RegexOptions.Compiled);

    private static readonly Regex Identifier = new(@"[A-Za-z_]\w*", RegexOptions.Compiled);

    internal sealed record Hit(string Kind, int Line, string Expression);

    /// <param name="Ceiling">True for an exception the same wave retires: the count is then a ceiling, so it
    /// passes at 0 as well and cannot go stale when the owning change lands. The default is exact.</param>
    private sealed record Allowed(string File, string Expression, int Count, string Reason, bool Ceiling = false);

    /// <summary>
    /// Cuts of statement-named values that are allowed to stay, each by file and expression. The count is exact, so
    /// an exception that stops being needed (or doubles) fails here and is removed or re-argued on purpose, unless it
    /// is marked a ceiling.
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
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWebDeadlockGraph.cs", "p.sqltext", 1,
            "PR C judges it whole; drop at C's dev merge. Until then the web viewer's side panel cuts one process's text out of deadlock_graph_xml for display, and web responses carry no statement judge",
            Ceiling: true),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "row.victimstatement!", 1,
            "BuildPgDeadlockIncident cuts a PostgreSQL victim statement the collector already judged whole: PgDeadlockLogParser redacts the whole report (PgLogTextRedactor.RedactDetail) before it takes the victim's statement out of it, and PgDeadlockLogParser.NormalizeStatement re-reads a stored one. Nothing here sees raw text"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "row.rootquery!", 1,
            "BuildPgBlockingIncident cuts a PostgreSQL root query the target already judged whole: PgBlockingCollector wraps blocker.query in PgSensitiveStatementFilter.SqlPredicate on the target before the row leaves it (PgSensitiveStatementFilterTests), and PgStatementTextScrub covers rows stored before that. Nothing here sees raw text"),
        new("PerformanceMonitor.Notifications/AnalysisNotificationService.cs", "element.getrawtext()", 1,
            "ScalarText shortens a nested drill-down object or array for the finding notification's field. The drill-down JSON is what DrillDownCollector stores: its statement values are judge-then-cut there (AnalysisStatementText.Preview), so this cuts judged values and never sees raw statement text"),
        new("PerformanceMonitor.Notifications/WebhookAlertService.cs", "paragraph", 2,
            "SplitProseLabel splits a synthesized advice paragraph on its first ': ' into label and value; both halves are kept, nothing is cut off (the name matches 'graph')"),
        new("Lite/Analysis/BaselineProvider.cs", "eventbaselinesql", 3,
            "slices the event-baseline SQL script (our own query text) around its events CTE to swap one column; it is a script, not a statement read from a monitored server"),
        new("Lite/Analysis/PileupSnapshotReader.cs", "rawcut(rawtext)", 1,
            "identity only: the cut text is the same-statement pileup's grouping key and noise test, built from the raw read and never printed; the shown text is cut after the judge"),
        new("Darling/PerformanceMonitor.Darling.Analysis/PgPileupSnapshotReader.cs", "cutcodepoints(rawtext", 1,
            "identity only: the cut text is the same-statement pileup's grouping key and noise test (a null-hash surrogate), built from the raw read and never printed; the shown text is cut after the judge"),
        new("Darling/PerformanceMonitor.Darling.Analysis/PgTargetDrillDownCollector.Deadlocks.cs",
            "left(encode(sha256(convert_to(latest_graph_text,'utf8')),'hex'),32)", 1,
            "an identity hash of the raw graph, compared with latest_hash and never shown"),
        new("Darling/PerformanceMonitor.Darling.Analysis/PgTargetDrillDownCollector.Deadlocks.cs", "lines.take", 1,
            "BoundGraphText bounds a graph its caller already judged whole (graphNormalized = SensitiveStatements.Text(...)); it cuts whole lines of judged text and never sees raw"),
    };

    // ------------------------------------------------------------------ the scanner

    /// <summary>Every cut of statement-named text in one C# source: the C# shapes and the SQL-in-literal shape.</summary>
    internal static List<Hit> Scan(string source)
    {
        var hits = new List<Hit>();

        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var judged = JudgedLocals(code);

        foreach (Match m in TruncateCall.Matches(code))
        {
            var arg = FirstArgument(code, m.Index + m.Length - 1);
            if (StatementName.IsMatch(arg) && !IsJudgedLocal(judged, arg, m.Index))
                hits.Add(new Hit("C# Truncate", LineOf(code, m.Index), Normalize(arg)));
        }

        foreach (Match m in CutHelperCall.Matches(code))
        {
            var arg = FirstArgument(code, m.Index + m.Length - 1);
            if (StatementName.IsMatch(arg) && !IsJudgedLocal(judged, arg, m.Index))
                hits.Add(new Hit("C# " + m.Groups[1].Value, LineOf(code, m.Index), Normalize(m.Groups[1].Value + "(" + arg + ")")));
        }

        foreach (Match m in SubstringCall.Matches(code))
        {
            if (StatementName.IsMatch(m.Groups[1].Value) && !IsJudgedLocal(judged, m.Groups[1].Value, m.Index))
                hits.Add(new Hit("C# Substring", LineOf(code, m.Index), Normalize(m.Groups[1].Value)));
        }

        foreach (Match m in RangeCut.Matches(code))
        {
            if (StatementName.IsMatch(m.Groups[1].Value) && !IsJudgedLocal(judged, m.Groups[1].Value, m.Index))
                hits.Add(new Hit("C# range", LineOf(code, m.Index), Normalize(m.Groups[1].Value)));
        }

        foreach (Match m in TakeCall.Matches(code))
        {
            var receiver = m.Groups[1].Value;
            if (!TakesStatementLines(code, receiver, m.Index) || IsJudgedLocal(judged, receiver, m.Index)) continue;
            hits.Add(new Hit("C# Take", LineOf(code, m.Index), Normalize(receiver + ".Take")));
        }

        var constants = StringConstant.Matches(source)
            .GroupBy(c => c.Groups[1].Value, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First().Groups[2].Value, StringComparer.Ordinal);

        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            foreach (Match m in SqlCut.Matches(body))
            {
                var open = m.Index + m.Length - 1;
                var arg = FirstArgument(body, open);

                /* An interpolation hole is blank in the body. Read the same call out of the source (the body keeps the
                   source's offsets) so LEFT({col}, n) is judged by the hole's own name, or by the constant it names. */
                var inSource = start + open < source.Length && source[start + open] == '(';
                var rawArg = inSource ? FirstArgument(source, start + open) : arg;
                var viaHole = inSource && !StatementName.IsMatch(arg) && SameButForHoles(arg, rawArg)
                    && HoleNamesStatementText(rawArg, constants);
                if (!StatementName.IsMatch(arg) && !viaHole) continue;

                var whole = Normalize(m.Groups[1].Value + "(" + (viaHole ? CallBody(source, start + open) : CallBody(body, open)) + ")");
                hits.Add(new Hit("SQL " + m.Groups[1].Value.ToLowerInvariant(), LineOf(source, start + m.Index), whole));
            }
        }

        return hits;
    }

    /// <summary>
    /// True when <paramref name="raw"/> is <paramref name="body"/> with its blanked interpolation holes filled in: the same
    /// length, and every non-blank character equal. A raw read that runs past the literal's end (a C# concatenation) is not.
    /// </summary>
    private static bool SameButForHoles(string body, string raw)
    {
        if (body.Length != raw.Length) return false;
        for (var i = 0; i < body.Length; i++)
        {
            if (body[i] != ' ' && body[i] != raw[i]) return false;
        }

        return true;
    }

    /// <summary>True when an interpolation hole in a SQL column list names statement text, directly or through a string constant.</summary>
    private static bool HoleNamesStatementText(string rawArg, Dictionary<string, string> constants)
    {
        if (StatementName.IsMatch(rawArg)) return true;

        foreach (Match id in Identifier.Matches(rawArg))
        {
            if (constants.TryGetValue(id.Value, out var value) && StatementName.IsMatch(value)) return true;
        }

        return false;
    }

    /// <summary>A local assigned from a judge call, and the block it is in: the span where its cut is judge-then-cut.</summary>
    private sealed record JudgedLocal(string Name, int From, int To);

    /// <summary>
    /// Every <c>name = ... judge(...)</c> in the source, scoped to the block the assignment sits in and to the text after
    /// it. A judged local that is cut in that span is the sanctioned order; the same name cut in another method, or before
    /// the assignment, is not excused.
    /// </summary>
    private static List<JudgedLocal> JudgedLocals(string code)
    {
        var locals = new List<JudgedLocal>();
        foreach (Match m in Assignment.Matches(code))
        {
            var from = m.Index + m.Length;
            var statement = code.Substring(from, StatementEnd(code, from) - from);
            if (JudgeCall.IsMatch(statement)) locals.Add(new JudgedLocal(m.Groups[1].Value, from, BlockEnd(code, from)));
        }

        return locals;
    }

    private static bool IsJudgedLocal(List<JudgedLocal> judged, string operand, int at)
    {
        var name = operand.Trim().TrimEnd('!');
        return Identifier.Match(name) is { Success: true } id && id.Value == name
            && judged.Any(j => j.Name == name && j.From <= at && at < j.To);
    }

    /// <summary>The end of the statement or initialiser that starts at <paramref name="from"/>: a top-level <c>;</c> or <c>,</c>, or the bracket that closes the enclosing one.</summary>
    private static int StatementEnd(string code, int from)
    {
        var depth = 0;
        for (var i = from; i < code.Length; i++)
        {
            var c = code[i];
            if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}')
            {
                if (--depth < 0) return i;
            }
            else if (c is ';' or ',' && depth == 0) return i;
        }

        return code.Length;
    }

    /// <summary>The close of the innermost block that holds <paramref name="from"/> (the end of the text when braces never balance).</summary>
    private static int BlockEnd(string code, int from)
    {
        var depth = 0;
        for (var i = from; i < code.Length; i++)
        {
            if (code[i] == '{') depth++;
            else if (code[i] == '}' && --depth < 0) return i;
        }

        return code.Length;
    }

    /// <summary>
    /// True for <c>x.Take(n)</c> when <c>x</c> is named like statement text, or is a line list declared from statement text
    /// (<c>var lines = graphText.Split('\n')</c>): a cut of a graph's lines is a cut of statement text.
    /// </summary>
    private static bool TakesStatementLines(string code, string receiver, int at)
    {
        if (StatementName.IsMatch(receiver)) return true;
        if (!receiver.Contains("line", StringComparison.OrdinalIgnoreCase) || !Identifier.IsMatch(receiver)) return false;

        var declaration = new Regex(@"\b" + Regex.Escape(receiver) + @"\s*=(?![=>])\s*([^;]*);");
        return declaration.Matches(code.Substring(0, at)).Any(d => StatementName.IsMatch(d.Groups[1].Value));
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

    /// <summary>The problems left after the named exceptions are applied to what the scan found, per file.</summary>
    private static List<string> Reconcile(Dictionary<string, List<Hit>> perFile, IEnumerable<Allowed> exceptions)
    {
        var allExceptions = exceptions.ToList();
        var problems = new List<string>();

        foreach (var (file, hits) in perFile.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            var remaining = new List<Hit>(hits);

            foreach (var allowed in allExceptions.Where(e => e.File == file))
            {
                var matched = remaining.Where(h => h.Expression.Contains(allowed.Expression, StringComparison.Ordinal)).ToList();
                var wrong = allowed.Ceiling ? matched.Count > allowed.Count : matched.Count != allowed.Count;
                if (wrong)
                    problems.Add($"{file}: exception '{allowed.Expression}' expected {(allowed.Ceiling ? "at most " : "")}{allowed.Count} site(s), found {matched.Count}");
                foreach (var h in matched) remaining.Remove(h);
            }

            foreach (var h in remaining)
                problems.Add($"{file}:{h.Line}: {h.Kind} cuts statement text before the filter judges it: {h.Expression}");
        }

        foreach (var allowed in allExceptions.Where(e => !e.Ceiling))
        {
            if (!perFile.ContainsKey(allowed.File))
                problems.Add($"{allowed.File}: exception '{allowed.Expression}' expected {allowed.Count} site(s), found 0 (remove it)");
        }

        return problems;
    }

    // ------------------------------------------------------------------ the real trees

    [Fact]
    public void NoStatementTextIsCutBeforeItIsJudged_ExceptTheNamedSites()
    {
        var files = ScannedFiles().ToList();
        Assert.True(files.Count > 200, "the scan walked the trees: " + files.Count);
        Assert.Contains(files, f => f.StartsWith("Lite/Analysis/", StringComparison.Ordinal));

        var perFile = new Dictionary<string, List<Hit>>();
        foreach (var file in files)
        {
            var hits = Scan(RepoFile.ReadRepoFile(file.Split('/')));
            if (hits.Count > 0) perFile[file] = hits;
        }

        var problems = Reconcile(perFile, Exceptions);

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    // ------------------------------------------------------------------ the scanner itself, on planted sources

    [Theory]
    [InlineData("var p = McpHelpers.Truncate(row.QueryText, 400);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(sqlText, 400);", "C# Truncate")]
    [InlineData("var p = AlertContextBuilders.TruncateText(row.QueryText);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(query, 400);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(row.blocking_query, 400);", "C# Truncate")]
    [InlineData("var p = TruncateText(g.BlockedQuery, 300);", "C# Truncate")]
    [InlineData("var p = TruncateText(g.QueryText, 300);", "C# Truncate")]
    [InlineData("var p = TruncateText(blockedQuery, 300);", "C# Truncate")]
    [InlineData("var p = AlertContextBuilders.TruncateText(statement, 80);", "C# Truncate")]
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
    [InlineData("var p = AlertContextBuilders.TruncateStatement(row.QueryText);")]
    [InlineData("var p = AlertContextBuilders.TruncateText(j.Message, 300);")]
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

    // ------------------------------------------------------------------ the rules added in #5361, each with its planted RED

    private static List<Hit> ScanBody(string body) => Scan("class C { void M() { " + body + " } }");

    [Theory]
    [InlineData("var p = AnalysisStatementText.CutCodePoints(rawText, 500);", "C# CutCodePoints")]
    [InlineData("var p = RawCut(rawText);", "C# RawCut")]
    [InlineData("var p = victimNormalized[..400];", "C# range")]
    [InlineData("var p = McpHelpers.Truncate(row.PreviewText, 400);", "C# Truncate")]
    [InlineData("var p = McpHelpers.Truncate(r.TextData, 400);", "C# Truncate")]
    [InlineData("var p = row.RootQuery!.Substring(0, 200);", "C# Substring")]
    [InlineData("var lines = deadlockGraph.Split(','); var p = string.Join(',', lines.Take(40));", "C# Take")]
    [InlineData("var p = string.Join(',', graphLines.Take(40));", "C# Take")]
    public void TheScanner_CatchesTheWiderStatementNamesAndCutShapes(string body, string kind)
    {
        Assert.Contains(ScanBody(body), h => h.Kind == kind);
    }

    [Theory]
    [InlineData("var lines = packageNames.Split(','); var p = lines.Take(5);")]
    [InlineData("var p = ordered.Take(limit);")]
    [InlineData("var p = McpHelpers.Truncate(serverName, 40);")]
    public void TheScanner_LeavesALineListThatIsNotStatementText(string body)
    {
        Assert.Empty(ScanBody(body));
    }

    [Fact]
    public void TheScanner_CutThenJudge_IsCaught_AndJudgeThenCut_IsNot()
    {
        var cutThenJudge = ScanBody("var cut = McpHelpers.Truncate(sqlText, 100); var j = SensitiveStatements.Text(cut);");
        Assert.Contains(cutThenJudge, h => h.Kind == "C# Truncate");

        var judgeThenCut = ScanBody("var sqlText = SensitiveStatements.Text(raw); var p = McpHelpers.Truncate(sqlText, 100);");
        Assert.Empty(judgeThenCut);
    }

    [Theory]
    [InlineData("var graph = SensitiveStatements.Xml(raw, 100); var p = McpHelpers.Truncate(graph, 100);")]
    [InlineData("var graphNormalized = SensitiveStatements.Text(Normalize(raw)); var p = graphNormalized![..400];")]
    [InlineData("var judged = AnalysisStatementText.Preview(raw, 900); var p = judged.Substring(0, 5);")]
    [InlineData("var filteredGraph = full ? r.Graph : SensitiveStatements.Xml(r.Graph, 400) ?? \"x\"; var p = full ? filteredGraph : McpHelpers.Truncate(filteredGraph, 400);")]
    [InlineData("var fetchedText = SensitiveStatements.Text(reader.GetString(4)) ?? \"\"; var p = fetchedText[..McpHelpers.TextElementCutLength(fetchedText, 9)];")]
    [InlineData("var queryText = SensitiveStatements.Text(raw) ?? \"\"; var p = AlertContextBuilders.TruncateText(queryText, 80);")]
    public void TheScanner_LeavesALocalThatWasJudgedBeforeItsCut(string body)
    {
        Assert.Empty(ScanBody(body));
    }

    [Theory]
    // judged in another method: the name is not excused there
    [InlineData("class C { void A() { var sqlText = SensitiveStatements.Text(raw); } void B(string sqlText) { var p = McpHelpers.Truncate(sqlText, 5); } }")]
    // judged AFTER the cut: the cut still ran on raw text
    [InlineData("class C { void A() { var p = McpHelpers.Truncate(sqlText, 5); sqlText = SensitiveStatements.Text(raw); } }")]
    // judged inside an inner block that has closed
    [InlineData("class C { void A() { if (x) { var sqlText = SensitiveStatements.Text(raw); } var p = McpHelpers.Truncate(sqlText, 5); } }")]
    // the judge call belongs to a different local
    [InlineData("class C { void A() { var other = SensitiveStatements.Text(raw); var p = McpHelpers.Truncate(sqlText, 5); } }")]
    // a member of an object initializer is not judged by its neighbour
    [InlineData("class C { void A() { var o = new { sqlText = raw, b = SensitiveStatements.Text(raw) }; var p = McpHelpers.Truncate(sqlText, 5); } }")]
    public void TheScanner_DoesNotExcuseAJudgedNameOutsideItsBlockOrBeforeItsAssignment(string source)
    {
        Assert.Contains(Scan(source), h => h.Kind == "C# Truncate");
    }

    [Theory]
    [InlineData("const string Col = \"query_text\"; string Q() => $@\"SELECT LEFT({Col}, 500) FROM t\";")]
    [InlineData("string Q(string queryTextColumn) => $@\"SELECT SUBSTRING({queryTextColumn}, 1, 300) FROM t\";")]
    [InlineData("string Q(string a) => $@\"SELECT LEFT({a}.query_text, 500) FROM t {a}\";")]
    [InlineData("string Q() { var col = \"blocked_sql_text\"; return $@\"SELECT substr({col}, 1, 200) FROM t\"; }")]
    [InlineData("string Q(string statementCol) => $\"SELECT id, LEFT({statementCol}, 100) AS q, hash FROM t\";")]
    public void TheScanner_CatchesASqlCutOverAnInterpolatedColumn(string members)
    {
        var hits = Scan("class C { " + members + " }");

        Assert.Contains(hits, h => h.Kind.StartsWith("SQL ", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("const string Col = \"relname\"; string Q() => $@\"SELECT LEFT({Col}, 63) FROM t\";")]
    [InlineData("string Q(string nameColumn) => $@\"SELECT LEFT({nameColumn}, 63) FROM t\";")]
    [InlineData("string Q(string cols) => $@\"SELECT {cols} FROM t\";")]
    public void TheScanner_LeavesASqlCutOverAnInterpolatedColumnThatIsNotStatementText(string members)
    {
        Assert.Empty(Scan("class C { " + members + " }"));
    }

    [Fact]
    public void ACeilingException_PassesAtZero_AndFailsAboveItsCount_WhileAnExactOneFailsStale()
    {
        var none = new Dictionary<string, List<Hit>>();
        var one = new Dictionary<string, List<Hit>> { ["F.cs"] = new() { new Hit("C# range", 3, "p.sqltext") } };
        var two = new Dictionary<string, List<Hit>>
        {
            ["F.cs"] = new() { new Hit("C# range", 3, "p.sqltext"), new Hit("C# range", 9, "p.sqltext") },
        };

        var ceiling = new[] { new Allowed("F.cs", "p.sqltext", 1, "retires with its PR", Ceiling: true) };
        var exact = new[] { new Allowed("F.cs", "p.sqltext", 1, "stays") };

        Assert.Empty(Reconcile(none, ceiling));
        Assert.Empty(Reconcile(one, ceiling));
        Assert.NotEmpty(Reconcile(two, ceiling));
        Assert.NotEmpty(Reconcile(none, exact));
        Assert.Empty(Reconcile(one, exact));
        Assert.NotEmpty(Reconcile(two, exact));
    }

    [Fact]
    public void TheSurfaceOfTheScan_ReachesLiteAnalysis_AndTheIdentityOnlyExceptionsAreNamed()
    {
        Assert.Contains("Lite/Analysis", ScannedTrees);
        Assert.Contains(Exceptions, e => e.File == "Lite/Analysis/PileupSnapshotReader.cs" && !e.Ceiling);
        Assert.Contains(Exceptions, e => e.File.EndsWith("/PgPileupSnapshotReader.cs", StringComparison.Ordinal) && !e.Ceiling);
        Assert.Contains(Exceptions, e => e.File.EndsWith("/DarlingWebDeadlockGraph.cs", StringComparison.Ordinal) && e.Ceiling);
    }
}
