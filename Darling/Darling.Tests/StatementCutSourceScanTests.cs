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
/// <item>C# (#5361 round 2): <c>x.AsSpan(a, b)</c> (also inside <c>string.Concat</c>), <c>x.AsSpan()[..n]</c>,
/// <c>x.Slice(...)</c>, <c>x.Remove(n)</c> and a chained <c>x.Split(...).Take(n)</c>. SQL: a DuckDB slice
/// (<c>text[1:n]</c>, <c>array_slice</c>) and a PostgreSQL <c>::varchar(n)</c> or <c>CAST(... AS varchar(n))</c>.</item>
/// </list>
/// The scanner is pinned with planted sources; the real trees are held to a short, named list of exceptions.
/// <para><b>Known blind spots</b> (a regex over source text cannot read these reliably; they are named here on purpose,
/// not left silent, so a reviewer of a new statement-text cut checks them by eye):
/// (1) an alias local: <c>var t = row.QueryText; t[..300]</c> cuts under a name that is not statement-like, and the scan
/// reads names, not data flow. (2) a ternary, <c>??</c> arm or later re-assignment around a judged local:
/// <c>var s = flag ? SensitiveStatements.Text(a) : raw; s[..300]</c> is excused because the right side holds a judge call,
/// and so is a raw re-assignment after a judge. Also not seen: SQL built by concatenation (<c>"LEFT(" + col + ", 500)"</c>)
/// and a JSON-generic local.</para>
/// </summary>
public sealed class StatementCutSourceScanTests
{
    /// <summary>Folders scanned, repo-root relative. Lite MCP, services and analysis; the shared alerting, notification and analysis projects; Darling service and analysis.</summary>
    private static readonly string[] ScannedTrees =
    {
        "Lite/Mcp",
        "Lite/Services",
        "Lite/Analysis",
        "PerformanceMonitor.Alerting",
        "PerformanceMonitor.Notifications",
        "PerformanceMonitor.Analysis",
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
    private static readonly Regex SqlCut = new(@"\b(left|substring|substr|array_slice|list_slice)\s*\(", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The cut helpers that are not <c>Truncate</c>: the code-point cut and the pileup readers' <c>RawCut</c>.</summary>
    private static readonly Regex CutHelperCall = new(@"\b(CutCodePoints|RawCut)\s*\(", RegexOptions.Compiled);

    /// <summary><c>x.Take(n)</c>: the cut of a line list. Only a receiver that is statement text, or lines split out of it, counts.</summary>
    private static readonly Regex TakeCall = new(@"([\w][\w\.\?!]*)\s*\.\s*Take\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// <c>x.AsSpan(a, b)</c> / <c>x.AsSpan(n)</c> (a sliced span, also inside <c>string.Concat</c>), and the whole-text
    /// <c>x.AsSpan()</c> followed by a range index or <c>.Slice(...)</c>. #5361 F2.
    /// </summary>
    private static readonly Regex AsSpanCall = new(@"([\w][\w\.\?!]*)\s*\.\s*AsSpan\s*\(", RegexOptions.Compiled);

    /// <summary><c>x.Remove(n)</c>: drops the tail (or a middle run) of a string.</summary>
    private static readonly Regex RemoveCall = new(@"([\w][\w\.\?!]*)\s*\.\s*Remove\s*\(", RegexOptions.Compiled);

    /// <summary><c>x.Slice(a, b)</c> on a span or memory of statement text.</summary>
    private static readonly Regex SliceCall = new(@"([\w][\w\.\?!]*)\s*\.\s*Slice\s*\(", RegexOptions.Compiled);

    /// <summary>The <c>.Take(</c> of any chain, including one after a call (<c>x.Split('\n').Take(5)</c>).</summary>
    private static readonly Regex AnyTakeCall = new(@"\.\s*Take\s*\(", RegexOptions.Compiled);

    /// <summary>DuckDB slice in SQL: <c>col[1:n]</c>, <c>col[:n]</c>.</summary>
    private static readonly Regex SqlSlice = new(@"([\w][\w\.]*)\s*\[[^\]\[]*:[^\]\[]*\]", RegexOptions.Compiled);

    /// <summary>The fixed-length character types that truncate in PostgreSQL: <c>varchar(n)</c>, <c>char(n)</c>, <c>character varying(n)</c>.</summary>
    private const string LengthLimitedType = @"(?:character\s+varying|character|varchar|bpchar|char)\s*\(\s*\d+\s*\)";

    /// <summary>PostgreSQL <c>operand::varchar(n)</c>. The operand is a name, or a parenthesised expression read by <see cref="OperandBefore"/>.</summary>
    private static readonly Regex SqlCastOperator = new(@"::\s*" + LengthLimitedType, RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>PostgreSQL / DuckDB <c>CAST(operand AS varchar(n))</c>.</summary>
    private static readonly Regex SqlCastCall = new(@"\bcast\s*\(", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex SqlCastTail = new(@"\bas\s+" + LengthLimitedType + @"\s*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "row.victimstatement!", 1,
            "BuildPgDeadlockIncident cuts a PostgreSQL victim statement the collector already judged whole: PgDeadlockLogParser redacts the whole report (PgLogTextRedactor.RedactDetail) before it takes the victim's statement out of it, and PgDeadlockLogParser.NormalizeStatement re-reads a stored one. Nothing here sees raw text"),
        new("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs", "row.rootquery!", 1,
            "BuildPgBlockingIncident cuts a PostgreSQL root query the target already judged whole: PgBlockingCollector wraps blocker.query in PgSensitiveStatementFilter.SqlPredicate on the target before the row leaves it (PgSensitiveStatementFilterTests), and PgStatementTextScrub covers rows stored before that. Nothing here sees raw text"),
        new("PerformanceMonitor.Notifications/AnalysisNotificationService.cs", "element.getrawtext()", 1,
            "ScalarText shortens a nested drill-down object or array for the finding notification's field. The drill-down JSON is what DrillDownCollector stores: its statement values are judge-then-cut there (AnalysisStatementText.Preview), so this cuts judged values and never sees raw statement text"),
        new("PerformanceMonitor.Notifications/WebhookAlertService.cs", "paragraph", 2,
            "SplitProseLabel splits a synthesized advice paragraph on its first ': ' into label and value; both halves are kept, nothing is cut off (the name matches 'graph')"),
        new("Darling/PerformanceMonitor.Darling.Service/Mcp/TopRanking.cs", "sql", 2,
            "ReplaceOnce splices a ranking clause into the top-N query's own SQL script with string.Concat over AsSpan; the script is our text, not a statement read from a monitored server"),
        new("PerformanceMonitor.Notifications/MuteRule.cs", "querytext", 1,
            "SeedQueryTextPattern pre-fills the mute dialog's editable query-text pattern from an alert's QueryText, which is parsed out of the stored alert detail the alert sender already judged whole (#5360); a withheld statement's marker is refused above the cut, so a named statement is never seeded and the cut text is a pattern the user edits, not a shown statement (#5367)"),
        new("PerformanceMonitor.Analysis/SameStatementPileupDetector.cs", "normalized", 1,
            "Preview normalizes whitespace and cuts the finding's printed statement; its only caller passes SnapshotRow.PreviewText, which the reader judged whole before it cut (a null PreviewText prints an empty string, #5361 F1). Nothing here sees raw text"),
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

        foreach (Match m in AsSpanCall.Matches(code))
        {
            var receiver = m.Groups[1].Value;
            if (!StatementName.IsMatch(receiver) || IsJudgedLocal(judged, receiver, m.Index)) continue;

            var open = m.Index + m.Length - 1;
            var args = CallBody(code, open);
            if (args.Trim().Length == 0)
            {
                /* x.AsSpan() on its own is not a cut; x.AsSpan()[..n] and x.AsSpan().Slice(a, b) are. */
                var after = open + args.Length + 2;
                while (after < code.Length && char.IsWhiteSpace(code[after])) after++;
                var ranged = after < code.Length && code[after] == '['
                    && BracketBody(code, after).Contains("..", StringComparison.Ordinal);
                var sliced = Regex.IsMatch(code.Substring(after, Math.Min(code.Length - after, 12)), @"^\.\s*Slice\s*\(");
                if (!ranged && !sliced) continue;
            }

            hits.Add(new Hit("C# AsSpan", LineOf(code, m.Index), Normalize(receiver)));
        }

        foreach (Match m in SliceCall.Matches(code))
        {
            var receiver = m.Groups[1].Value;
            if (StatementName.IsMatch(receiver) && !IsJudgedLocal(judged, receiver, m.Index))
                hits.Add(new Hit("C# Slice", LineOf(code, m.Index), Normalize(receiver)));
        }

        foreach (Match m in RemoveCall.Matches(code))
        {
            var receiver = m.Groups[1].Value;
            if (StatementName.IsMatch(receiver) && !IsJudgedLocal(judged, receiver, m.Index))
                hits.Add(new Hit("C# Remove", LineOf(code, m.Index), Normalize(receiver)));
        }

        foreach (Match m in AnyTakeCall.Matches(code))
        {
            /* A Take after a call: x.Split('\n').Take(5). The chain's own names count, its arguments do not. */
            var before = m.Index;
            while (before > 0 && char.IsWhiteSpace(code[before - 1])) before--;
            if (before == 0 || code[before - 1] != ')') continue;

            var chain = ChainBefore(code, before);
            var root = Identifier.Match(chain);
            if (!StatementName.IsMatch(chain) || root.Success && IsJudgedLocal(judged, root.Value, m.Index)) continue;
            hits.Add(new Hit("C# Take", LineOf(code, m.Index), Normalize(chain + ".Take")));
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

        /* #5361 F2: the SQL cuts that are not LEFT/SUBSTRING: a DuckDB slice and a PostgreSQL fixed-length cast. */
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            foreach (Match m in SqlSlice.Matches(body))
            {
                if (StatementName.IsMatch(m.Groups[1].Value))
                    hits.Add(new Hit("SQL slice", LineOf(source, start + m.Index), Normalize(m.Value)));
            }

            foreach (Match m in SqlCastOperator.Matches(body))
            {
                var operand = OperandBefore(body, m.Index);
                if (StatementName.IsMatch(operand))
                    hits.Add(new Hit("SQL cast", LineOf(source, start + m.Index), Normalize(operand + m.Value)));
            }

            foreach (Match m in SqlCastCall.Matches(body))
            {
                var inner = CallBody(body, m.Index + m.Length - 1);
                var tail = SqlCastTail.Match(inner);
                if (tail.Success && StatementName.IsMatch(inner.Substring(0, tail.Index)))
                    hits.Add(new Hit("SQL cast", LineOf(source, start + m.Index), Normalize("cast(" + inner + ")")));
            }
        }

        return hits;
    }

    /// <summary>The operand of a <c>::</c> at <paramref name="at"/>: the name before it, or the parenthesised expression.</summary>
    private static string OperandBefore(string text, int at)
    {
        var end = at;
        while (end > 0 && char.IsWhiteSpace(text[end - 1])) end--;
        if (end == 0) return "";

        if (text[end - 1] == ')')
        {
            var depth = 0;
            for (var i = end - 1; i >= 0; i--)
            {
                if (text[i] == ')') depth++;
                else if (text[i] == '(' && --depth == 0) return text.Substring(i, end - i);
            }

            return text.Substring(0, end);
        }

        var begin = end;
        while (begin > 0 && (char.IsLetterOrDigit(text[begin - 1]) || text[begin - 1] is '_' or '.')) begin--;
        return text.Substring(begin, end - begin);
    }

    /// <summary>The call chain that ends at <paramref name="end"/> (just after a closing paren), with every call's arguments blanked.</summary>
    private static string ChainBefore(string code, int end)
    {
        var sb = new StringBuilder();
        var i = end;
        while (i > 0)
        {
            var c = code[i - 1];
            if (c == ')')
            {
                var depth = 0;
                var j = i - 1;
                for (; j >= 0; j--)
                {
                    if (code[j] == ')') depth++;
                    else if (code[j] == '(' && --depth == 0) break;
                }

                if (j < 0) break;
                sb.Insert(0, "()");
                i = j;
            }
            else if (char.IsLetterOrDigit(c) || c is '_' or '.' or '?' or '!')
            {
                sb.Insert(0, c);
                i--;
            }
            else if (char.IsWhiteSpace(c))
            {
                i--;
            }
            else break;
        }

        return sb.ToString();
    }

    /// <summary>Everything between the '[' at <paramref name="open"/> and its closing bracket.</summary>
    private static string BracketBody(string text, int open)
    {
        var depth = 0;
        for (var i = open; i < text.Length; i++)
        {
            if (text[i] == '[') depth++;
            else if (text[i] == ']' && --depth == 0) return text.Substring(open + 1, i - open - 1);
        }

        return text.Substring(open + 1);
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

    // ------------------------------------------------------------------ the shapes added in #5361 round 2 (F2), each with its planted RED

    [Theory]
    [InlineData("var p = row.QueryText.AsSpan(0, 200).ToString();", "C# AsSpan")]
    [InlineData("var p = sqlText.AsSpan(200).ToString();", "C# AsSpan")]
    [InlineData("var p = string.Concat(sqlText.AsSpan(0, 200), \"...\");", "C# AsSpan")]
    [InlineData("var p = sqlText.AsSpan()[..200].ToString();", "C# AsSpan")]
    [InlineData("var p = sqlText.AsSpan().Slice(0, 200).ToString();", "C# AsSpan")]
    [InlineData("var p = statementText.Slice(0, 200);", "C# Slice")]
    [InlineData("var p = row.Statement.Remove(200);", "C# Remove")]
    [InlineData("var p = string.Join('\\n', deadlockGraph.Split('\\n').Take(40));", "C# Take")]
    [InlineData("var p = deadlockGraph.Split('\\n', StringSplitOptions.RemoveEmptyEntries).Take(40).ToList();", "C# Take")]
    public void TheScanner_CatchesTheCutShapesAddedInRoundTwo(string body, string kind)
    {
        Assert.Contains(ScanBody(body), h => h.Kind == kind);
    }

    [Theory]
    [InlineData("var p = serverName.AsSpan(0, 3).ToString();")]
    [InlineData("var n = int.Parse(sqlText.AsSpan());")]
    [InlineData("var p = tableName.Remove(3);")]
    [InlineData("var p = names.Split(',').Take(3).ToList();")]
    [InlineData("var p = string.Concat(label.AsSpan(0, 3), \"x\");")]
    [InlineData("var sqlText = SensitiveStatements.Text(raw); var p = string.Concat(sqlText.AsSpan(0, 5), \"x\");")]
    [InlineData("var sqlText = SensitiveStatements.Text(raw); var p = sqlText.Remove(5);")]
    [InlineData("var graph = SensitiveStatements.Xml(raw, 100); var p = graph.Split('\\n').Take(5).ToList();")]
    public void TheScanner_LeavesTheRoundTwoShapesThatAreNotACutOfStatementText(string body)
    {
        Assert.Empty(ScanBody(body));
    }

    [Theory]
    [InlineData("SELECT query_text[1:500] AS q FROM t")]
    [InlineData("SELECT sql_text[:500] FROM t")]
    [InlineData("SELECT r.blocked_sql_text[1:200] FROM t r")]
    [InlineData("SELECT array_slice(query_text, 1, 500) FROM t")]
    [InlineData("SELECT query_text::varchar(500) FROM t")]
    [InlineData("SELECT r.query_text :: character varying(500) FROM t r")]
    [InlineData("SELECT (COALESCE(a.query_text, b.query_text))::varchar(500) FROM t")]
    [InlineData("SELECT query_text::char(80) FROM t")]
    [InlineData("SELECT CAST(query_text AS varchar(500)) FROM t")]
    [InlineData("SELECT CAST(COALESCE(x.statement, y.statement) AS VARCHAR(300)) FROM t")]
    public void TheScanner_CatchesASqlSliceOrFixedLengthCastOfStatementText(string sql)
    {
        var hits = Scan("class C { const string Q = @\"" + sql + "\"; }");

        Assert.Contains(hits, h => h.Kind.StartsWith("SQL ", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("SELECT relname[1:63] FROM t")]
    [InlineData("SELECT query_hash::varchar(16) FROM t")]
    [InlineData("SELECT query_text::text FROM t")]
    [InlineData("SELECT CAST(query_text AS text) FROM t")]
    [InlineData("SELECT CAST(query_hash AS varchar(16)) FROM t")]
    [InlineData("SELECT query_text::varchar FROM t")]
    [InlineData("SELECT x[1:2] FROM t")]
    public void TheScanner_LeavesASqlSliceOrCastThatIsNotAFixedLengthCutOfStatementText(string sql)
    {
        Assert.Empty(Scan("class C { const string Q = @\"" + sql + "\"; }"));
    }

    [Fact]
    public void TheScansDocComment_NamesItsKnownBlindSpots()
    {
        var doc = RepoFile.ReadRepoFile("Darling", "Darling.Tests", "StatementCutSourceScanTests.cs");
        Assert.Contains("Known blind spots", doc, StringComparison.Ordinal);
        Assert.Contains("alias local", doc, StringComparison.Ordinal);
        Assert.Contains("ternary", doc, StringComparison.Ordinal);
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

        /* #5361 F1: the shared analysis project is scanned, and the pileup finding's printed statement is the reader's
           judged PreviewText with an empty fallback, never the raw identity cut in QueryText. */
        Assert.Contains("PerformanceMonitor.Analysis", ScannedTrees);
        var detector = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "SameStatementPileupDetector.cs");
        Assert.Contains("Preview(leader.PreviewText ?? \"\")", detector, StringComparison.Ordinal);
        Assert.DoesNotContain("PreviewText ?? leader.QueryText", detector, StringComparison.Ordinal);
        Assert.DoesNotContain("string? PreviewText = null", detector, StringComparison.Ordinal);
        Assert.Contains(Exceptions, e => e.File == "Lite/Analysis/PileupSnapshotReader.cs" && !e.Ceiling);
        Assert.Contains(Exceptions, e => e.File.EndsWith("/PgPileupSnapshotReader.cs", StringComparison.Ordinal) && !e.Ceiling);
    }
}
