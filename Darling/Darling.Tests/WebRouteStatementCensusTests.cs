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
/// #4348, the web route census. Every route the Darling service maps (any <c>x.Map*</c> call, in any file of the
/// service project, <c>Mcp/</c> included) is either SWEPT or on the short named list below with the reason it carries
/// no tool string.
///
/// <para>A route is swept only when every value its handler returns comes from the sweep: the whole returned
/// expression is a call of <c>ToHttpResult</c>, <c>MuteRuleToolResult</c> or <c>DarlingWebStatementSweep.*</c>
/// (or, in the fleet-sweep file, its two pinned writers), at least one return is such a call, and no return is any
/// other writer. A sweep call whose result is dropped, a second return that writes raw text, a direct write to the
/// response, or a handler the scan cannot find (a method group is looked up by name across the project) all leave
/// the route unswept, and an unswept route that is not named fails. Two kinds of return are not tool text and are
/// listed with the reason: fixed-sentence request errors, and one listed names-only body.</para>
///
/// <para>The scan reads code only: <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> blanks comments and
/// literals first, so a call named in a comment or a string counts for nothing.</para>
/// </summary>
public sealed class WebRouteStatementCensusTests
{
    /// <summary>The service files that map a route, relative to the service project. A new mapper file is a
    /// deliberate act: it fails <see cref="NoOtherServiceFile_MapsARoute"/> until it is added here.</summary>
    private static readonly string[] MapperFilesExpected =
    {
        "AlertNotebookEndpoint.cs",
        "DarlingFleetSweepEndpoints.cs",
        "DarlingServerDatabasesEndpoint.cs",
        "DarlingTriageEndpoint.cs",
        "DarlingWebEndpoints.cs",
        "Mcp/DarlingMcpHostService.cs",
    };

    /// <summary>The routes that put no tool string on the wire, by the route argument as written in the source.</summary>
    private static readonly Dictionary<string, string> Named = new(StringComparer.Ordinal)
    {
        ["\"/api/ping\""] = "liveness, no data",
        ["\"/api/session\""] = "the seat's capabilities",
        ["\"/api/catalog\""] = "the compose vocabulary (labels; no statement or plan column, pinned by the L8 catalog census)",
        ["\"/api/fleet\""] = "fleet counts and server names",
        ["\"/api/ag\""] = "availability group health",
        ["\"/api/ag/count\""] = "availability group counts",
        ["\"/api/views\""] = "custom view definitions (composed from the catalog)",
        ["\"/api/views/{id:long}\""] = "custom view definitions (composed from the catalog)",
        ["\"/api/compose/run\""] = "composed panels read catalog measures only (pinned by the L8 catalog census)",
        ["\"/api/alerts\""] = "custom alert rule definitions",
        ["\"/api/alerts/{id:long}\""] = "custom alert rule definitions",
        ["\"/api/alerts/validate\""] = "rule validation messages",
        ["\"/api/servers\""] = "server registry",
        ["\"/api/servers/{id:int}\""] = "server registry",
        ["\"/api/admin/servers/{id:int}\""] = "server registry",
        ["DarlingAdminServersReader.Route"] = "server registry",
        ["\"/api/alert-history/dismiss\""] = "a dismissal receipt",
        ["\"/api/server-tags\""] = "server tags",
        ["\"/api/server-tags/{id:int}\""] = "server tags",
        ["\"/api/server-tags/{id:int}/servers\""] = "server tags",
        ["MapMcp()"] = "the MCP transport: every tool result passes the MCP output filter (pinned by the MCP filter tests)",
        ["MapMcp(\"/core\")"] = "the MCP transport, core profile: every tool result passes the MCP output filter",
    };

    /// <summary>
    /// The only returns, in a handler that does sweep, that are not a sweep call. Each is a writer whose text is a
    /// fixed sentence or the route's own request error, never a tool string. Matched on the whole returned expression.
    /// </summary>
    private static readonly (Regex Pattern, string Reason)[] FixedWriters =
    {
        (new Regex(@"^UnsupportedMediaTypeResult\s*\(\s*\)$", RegexOptions.Compiled), "a fixed 415 sentence"),
        (new Regex(@"^ErrorResult\s*\(", RegexOptions.Compiled), "the route's own request-validation message"),
        (new Regex(@"^(?:Alert)?NotFoundResult\s*\(\s*\)$", RegexOptions.Compiled), "a fixed 404 sentence"),
        (new Regex(@"^Results\s*\.\s*NoContent\s*\(\s*\)$", RegexOptions.Compiled), "no body"),
        (new Regex(@"^Results\s*\.\s*Json\s*\(\s*DarlingWebFailureLog\s*\.\s*Body\s*\(", RegexOptions.Compiled),
            "DarlingWebFailureLog.Body is one fixed sentence per failure class, never the exception text"),
        (new Regex(@"^ServerErrorResult\s*\(", RegexOptions.Compiled),
            "answers DarlingWebFailureLog.Body(sentence): a fixed sentence, the argument only reaches the log"),
    };

    /// <summary>Returns listed one by one, keyed by file and the returned expression as written (whitespace
    /// collapsed): a change to the expression fails the census until it is looked at again.</summary>
    private static readonly Dictionary<string, string> ListedReturns = new(StringComparer.Ordinal)
    {
        ["DarlingServerDatabasesEndpoint.cs|Results.Text(Render(resolved.ServerName, names, truncated), \"application/json\")"] =
            "the database picker's list: database names and a truncation flag, no statement text",
    };

    /// <summary>Writers that sweep, by the one file that defines and uses them (a same-named helper elsewhere does
    /// not count). <see cref="TheSharedWriters_CallTheSweepBeforeAnythingElse"/> pins each one's body.</summary>
    private static readonly Dictionary<string, string[]> FileWriters = new(StringComparer.Ordinal)
    {
        ["DarlingFleetSweepEndpoints.cs"] = new[] { "JsonResult", "SweepError" },
    };

    private static readonly Regex SweptWriter = new(
        @"^(?:DarlingWebEndpoints\s*\.\s*)?(?:ToHttpResult|MuteRuleToolResult)\s*\(|^DarlingWebStatementSweep\s*\.\s*(?:Apply|JsonText|Refusal)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Any <c>x.Map*(</c> or <c>).Map*(</c> call: <c>MapGet</c>, <c>MapPost</c>, <c>MapPut</c>, <c>MapPatch</c>,
    /// <c>MapDelete</c>, <c>MapMethods</c>, <c>Map</c>, <c>MapFallback</c>, <c>MapGroup</c>, <c>MapMcp</c> and whatever
    /// else the framework adds (not <c>MapTo*</c>, the IP address conversions). The receiver starts lower-case (or is a call result), so a static helper such as
    /// <c>DarlingWebEndpoints.MapAll(</c> or a bare <c>MapRole(</c> is not a route.</summary>
    private static readonly Regex MapCall = new(
        @"(?<recv>\b[a-z_]\w*|\))\s*\.\s*(?<m>Map(?!To[A-Z])\w*)\s*\(", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex DirectWrite = new(
        @"\bResponse\s*\.\s*(?:Write\w*|Body\w*|SendFileAsync|Redirect\w*|StartAsync|CompleteAsync)|\bWriteAsJsonAsync\b|\bWriteAsync\s*\(|\bResults\s*\.\s*(?:Stream|File|Redirect\w*|Bytes)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly HashSet<string> NotATypeWord = new(StringComparer.Ordinal)
    {
        "return", "await", "new", "else", "in", "is", "and", "or", "not", "case", "throw", "yield", "when", "using",
        "lock", "if", "while", "switch", "nameof", "typeof", "do", "for", "foreach", "ref", "out",
    };

    private sealed record RouteRow(string Route, bool Swept, string File, int Line, string Why);

    private static string Code(string source) => CSharpSourceWalker.StripCommentsAndStrings(source);

    /// <summary>Index of the bracket that closes the one at <paramref name="open"/> (any of <c>( [ {</c> counted
    /// together), or -1.</summary>
    private static int Close(string code, int open)
    {
        int depth = 0;
        for (int i = open; i < code.Length; i++)
        {
            char c = code[i];
            if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}' && --depth == 0) return i;
        }

        return -1;
    }

    /// <summary>The top-level arguments of the call whose <c>(</c> is at <paramref name="open"/>, as [start,end) ranges.</summary>
    private static List<(int S, int E)> Args(string code, string source, int open)
    {
        var args = new List<(int, int)>();
        int close = Close(code, open);
        if (close < 0) return args;
        int depth = 0, start = open + 1;
        for (int i = open + 1; i < close; i++)
        {
            char c = code[i];
            if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}') depth--;
            else if (c == ',' && depth == 0)
            {
                args.Add((start, i));
                start = i + 1;
            }
        }

        args.Add((start, close));
        return args.Where(a => source[a.Item1..a.Item2].Trim().Length > 0).ToList();
    }

    private static (int S, int E) Trim(string code, (int S, int E) r)
    {
        int s = r.S, e = r.E;
        while (s < e && char.IsWhiteSpace(code[s])) s++;
        while (e > s && char.IsWhiteSpace(code[e - 1])) e--;
        return (s, e);
    }

    private static int LineOf(string code, int index) => 1 + code.Take(index).Count(c => c == '\n');

    private static string Collapse(string s) => Regex.Replace(s.Trim(), @"\s+", " ");

    /// <summary>The offset of a top-level <c>=></c> in [s,e), or -1 (so a method group has none).</summary>
    private static int TopLevelArrow(string code, int s, int e)
    {
        int depth = 0;
        for (int i = s; i < e - 1; i++)
        {
            char c = code[i];
            if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}') depth--;
            else if (depth == 0 && c == '=' && code[i + 1] == '>') return i;
        }

        return -1;
    }

    /// <summary>One handler body: its text range in a code string, and whether it is an expression body.</summary>
    private sealed record Body(string Code, string Source, int S, int E, bool IsExpression);

    /// <summary>The handler argument's bodies: the lambda's own, or every method of that name in the project.</summary>
    private static List<Body>? HandlerBodies(string code, string source, (int S, int E) handler, IReadOnlyList<(string Code, string Source)> project)
    {
        var (s, e) = Trim(code, handler);
        int arrow = TopLevelArrow(code, s, e);
        if (arrow >= 0)
        {
            var (bs, be) = Trim(code, (arrow + 2, e));
            return new List<Body> { new(code, source, bs, be, code[bs] != '{') };
        }

        // A method group, possibly cast or wrapped in a delegate constructor: the last identifier names the method.
        var m = Regex.Match(code[s..e], @"([A-Za-z_]\w*)\s*\)*\s*$");
        if (!m.Success) return null;
        var bodies = new List<Body>();
        foreach (var (pc, ps) in project)
        {
            foreach (Match d in Regex.Matches(pc, @"\b" + Regex.Escape(m.Groups[1].Value) + @"\s*\("))
            {
                int k = d.Index - 1;
                while (k >= 0 && char.IsWhiteSpace(pc[k])) k--;
                if (k < 0 || !(char.IsLetterOrDigit(pc[k]) || pc[k] is '_' or '>' or ']' or '?')) continue;
                int w = k;
                while (w >= 0 && (char.IsLetterOrDigit(pc[w]) || pc[w] == '_')) w--;
                if (NotATypeWord.Contains(pc[(w + 1)..(k + 1)])) continue;
                int close = Close(pc, d.Index + d.Length - 1);
                if (close < 0) continue;
                int p = close + 1;
                while (p < pc.Length && char.IsWhiteSpace(pc[p])) p++;
                if (p < pc.Length && pc[p] == '{')
                {
                    bodies.Add(new Body(pc, ps, p, Close(pc, p) + 1, false));
                }
                else if (p + 1 < pc.Length && pc[p] == '=' && pc[p + 1] == '>')
                {
                    int q = p + 2;
                    int end = pc.IndexOf(';', q);
                    bodies.Add(new Body(pc, ps, q, end < 0 ? pc.Length : end, true));
                }
            }
        }

        return bodies.Count == 0 ? null : bodies;
    }

    /// <summary>The returned expressions of a body, as [start,end) ranges: the whole expression for an expression
    /// body, each <c>return ...;</c> for a block.</summary>
    private static List<(int S, int E)> Exits(Body b)
    {
        if (b.IsExpression) return new List<(int, int)> { Trim(b.Code, (b.S, b.E)) };
        var exits = new List<(int, int)>();
        foreach (Match r in Regex.Matches(b.Code[b.S..b.E], @"\breturn\b"))
        {
            int s = b.S + r.Index + r.Length, depth = 0, i = s;
            for (; i < b.E; i++)
            {
                char c = b.Code[i];
                if (c is '(' or '[' or '{') depth++;
                else if (c is ')' or ']' or '}') depth--;
                else if (c == ';' && depth == 0) break;
            }

            var range = Trim(b.Code, (s, i));
            if (range.E > range.S) exits.Add(range);
        }

        return exits;
    }

    private enum Verdict { Fixed, Swept, Bad }

    /// <summary>The offset of the first top-level <c>?</c> that opens a conditional (not <c>??</c>, <c>?.</c>,
    /// <c>?[</c>), and of the <c>:</c> that closes it.</summary>
    private static (int Q, int Colon) Conditional(string code, int s, int e)
    {
        int depth = 0, q = -1, nested = 0;
        for (int i = s; i < e; i++)
        {
            char c = code[i];
            if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}') depth--;
            else if (depth == 0 && c == '?')
            {
                char n = i + 1 < e ? code[i + 1] : ' ';
                if (n is '?' or '.' or '[') { if (n == '?') i++; continue; }
                if (q < 0) q = i; else nested++;
            }
            else if (depth == 0 && c == ':' && q >= 0)
            {
                if (nested == 0) return (q, i);
                nested--;
            }
        }

        return (-1, -1);
    }

    private static Verdict Join(Verdict a, Verdict b) =>
        a == Verdict.Bad || b == Verdict.Bad ? Verdict.Bad : a == Verdict.Swept || b == Verdict.Swept ? Verdict.Swept : Verdict.Fixed;

    private static Verdict Classify(string code, string source, int s, int e, string file, List<string> why)
    {
        (s, e) = Trim(code, (s, e));
        string text = code[s..e];

        // await / ConfigureAwait / wrapping parentheses do not change where the value comes from.
        var aw = Regex.Match(text, @"^await\s+");
        if (aw.Success) return Classify(code, source, s + aw.Length, e, file, why);
        if (text.StartsWith('(') && Close(code, s) == e - 1) return Classify(code, source, s + 1, e - 1, file, why);

        var (q, colon) = Conditional(code, s, e);
        if (q >= 0 && colon > q)
        {
            return Join(Classify(code, source, q + 1, colon, file, why), Classify(code, source, colon + 1, e, file, why));
        }

        int coalesce = -1, depth = 0;
        for (int i = s; i < e - 1; i++)
        {
            char c = code[i];
            if (c is '(' or '[' or '{') depth++;
            else if (c is ')' or ']' or '}') depth--;
            else if (depth == 0 && c == '?' && code[i + 1] == '?') { coalesce = i; break; }
        }

        if (coalesce >= 0)
        {
            return Join(Classify(code, source, s, coalesce, file, why), Classify(code, source, coalesce + 2, e, file, why));
        }

        // A writer counts only when the WHOLE expression is its call.
        bool WholeCall(Match m) => m.Success && Close(code, s + m.Length - 1) == e - 1;

        if (WholeCall(SweptWriter.Match(text))) return Verdict.Swept;
        if (FileWriters.TryGetValue(file, out var local))
        {
            foreach (string w in local)
            {
                if (WholeCall(Regex.Match(text, @"^" + w + @"\s*\("))) return Verdict.Swept;
            }
        }

        if (ListedReturns.ContainsKey(file + "|" + Collapse(source[s..e]))) return Verdict.Fixed;
        foreach (var (pattern, _) in FixedWriters)
        {
            var m = pattern.Match(text);

            // ErrorResult is a fixed writer only for a literal message or the route's own request-parse error.
            if (m.Success && text.StartsWith("ErrorResult", StringComparison.Ordinal)
                && !Regex.IsMatch(source[s..e], @"^ErrorResult\s*\(\s*(?:\$?@?""|bodyError\b)")) continue;
            if (m.Success && (pattern.ToString().EndsWith('$') || Close(code, s + text.IndexOf('(')) == e - 1)) return Verdict.Fixed;
        }

        why.Add("returns `" + Collapse(source[s..Math.Min(e, s + 70)]) + "` which is not a sweep call");
        return Verdict.Bad;
    }

    /// <summary>The routes one file maps. <paramref name="project"/> is every service file's (code, source), so a
    /// method-group handler is found wherever it is defined; it defaults to this file alone.</summary>
    private static List<RouteRow> Routes(string source, string file = "Planted.cs", IReadOnlyList<(string Code, string Source)>? project = null)
    {
        string code = Code(source);
        project ??= new[] { (code, source) };
        var rows = new List<RouteRow>();
        var groups = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (Match m in MapCall.Matches(code))
        {
            string method = m.Groups["m"].Value;
            int open = m.Index + m.Length - 1;
            var args = Args(code, source, open);
            int line = LineOf(code, m.Index);
            string recv = m.Groups["recv"].Value;

            // The group a map hangs off: `g.MapGet(` for a variable, `app.MapGroup("/x").MapGet(` for a chain.
            string prefix = "";
            if (recv == ")")
            {
                int k = m.Groups["recv"].Index;
                int depth = 0, o = k;
                for (; o >= 0; o--)
                {
                    if (code[o] == ')') depth++;
                    else if (code[o] == '(' && --depth == 0) break;
                }

                var head = o > 0 ? Regex.Match(code[..o], @"\bMapGroup\s*$") : Match.Empty;
                if (head.Success)
                {
                    var ga = Args(code, source, o);
                    if (ga.Count > 0) prefix = Collapse(source[ga[0].S..ga[0].E]) + " + ";
                }
            }
            else if (groups.TryGetValue(recv, out var gp))
            {
                prefix = gp;
            }

            if (method == "MapGroup")
            {
                string gArg = args.Count > 0 ? Collapse(source[args[0].S..args[0].E]) + " + " : "";
                var assigned = Regex.Match(code[..m.Groups["recv"].Index], @"(\w+)\s*=\s*$");
                if (assigned.Success) groups[assigned.Groups[1].Value] = prefix + gArg;
                continue;
            }

            string route;
            (int S, int E)? handler;
            if (method == "MapMcp")
            {
                route = "MapMcp(" + (args.Count > 0 ? Collapse(source[args[0].S..args[^1].E]) : "") + ")";
                rows.Add(new RouteRow(route, false, file, line, "an MCP endpoint is not a swept web route"));
                continue;
            }

            if (args.Count == 0)
            {
                rows.Add(new RouteRow(prefix + method + "()", false, file, line, "no handler found"));
                continue;
            }

            bool verbForm = method is "MapGet" or "MapPost" or "MapPut" or "MapPatch" or "MapDelete" or "MapMethods" or "Map";
            if (verbForm || args.Count > 1)
            {
                route = prefix + Collapse(source[args[0].S..args[0].E]);
                if (!verbForm) route = method + "(" + route + ")";
            }
            else
            {
                route = method + "()";
            }

            handler = args[^1];
            var bodies = HandlerBodies(code, source, handler.Value, project);
            var why = new List<string>();
            bool swept = false;
            if (bodies is null)
            {
                why.Add("the handler is not a lambda and no method of that name was found");
            }
            else
            {
                var verdict = Verdict.Fixed;
                foreach (var b in bodies)
                {
                    var exits = Exits(b);
                    if (exits.Count == 0) why.Add("the handler has no return");
                    if (exits.Count == 0) verdict = Verdict.Bad;
                    foreach (var (es, ee) in exits)
                    {
                        verdict = Join(verdict, Classify(b.Code, b.Source, es, ee, file, why));
                    }

                    if (DirectWrite.IsMatch(b.Code[b.S..b.E]))
                    {
                        why.Add("the handler writes to the response directly");
                        verdict = Verdict.Bad;
                    }
                }

                swept = verdict == Verdict.Swept;
                if (verdict == Verdict.Fixed) why.Add("no return is a sweep call");
            }

            rows.Add(new RouteRow(route, swept, file, line, string.Join("; ", why.Distinct())));
        }

        return rows;
    }

    private static string ServiceDir() => RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service");

    /// <summary>Every .cs file of the service project (Mcp/ included; bin and obj not), relative paths with / separators.</summary>
    private static List<(string Rel, string Source, string Code)> ServiceFiles(string serviceDir)
    {
        var files = new List<(string, string, string)>();
        foreach (string path in Directory.EnumerateFiles(serviceDir, "*.cs", SearchOption.AllDirectories))
        {
            string rel = Path.GetRelativePath(serviceDir, path).Replace('\\', '/');
            if (rel.StartsWith("obj/", StringComparison.Ordinal) || rel.StartsWith("bin/", StringComparison.Ordinal)) continue;
            string src = File.ReadAllText(path);
            files.Add((rel, src, Code(src)));
        }

        return files;
    }

    private static string[] MapperFiles(string serviceDir) =>
        ServiceFiles(serviceDir)
            .Where(f => MapCall.IsMatch(f.Code))
            .Select(f => f.Rel)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public void EveryMappedRoute_IsSwept_OrOnTheNamedList()
    {
        var files = ServiceFiles(ServiceDir());
        var project = files.Select(f => (f.Code, f.Source)).ToList();
        var problems = new List<string>();
        int swept = 0, named = 0;
        var seenNamed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var f in files.Where(f => MapCall.IsMatch(f.Code)))
        {
            foreach (var row in Routes(f.Source, Path.GetFileName(f.Rel), project))
            {
                if (row.Swept)
                {
                    swept++;
                    if (Named.ContainsKey(row.Route)) problems.Add(f.Rel + ":" + row.Line + " " + row.Route + " is on the named list but now sweeps: update the list");
                    continue;
                }

                if (Named.ContainsKey(row.Route)) { named++; seenNamed.Add(row.Route); continue; }
                problems.Add(f.Rel + ":" + row.Line + " " + row.Route + " does not answer through the sweep and is not on the named list (" + row.Why + ")");
            }
        }

        foreach (string stale in Named.Keys.Where(k => !seenNamed.Contains(k)))
        {
            problems.Add("named route " + stale + " is no longer mapped: update the list");
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));
        Assert.True(swept >= 8, "swept routes found: " + swept);
        Console.WriteLine("web routes: swept=" + swept + " named=" + named);
    }

    [Fact]
    public void NoOtherServiceFile_MapsARoute()
    {
        Assert.Equal(MapperFilesExpected.OrderBy(n => n, StringComparer.Ordinal).ToArray(), MapperFiles(ServiceDir()));
    }

    [Fact]
    public void ARouteUnderMcp_IsSeenByTheScan()
    {
        string dir = Path.Combine(Path.GetTempPath(), "c2-census-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(dir, "Mcp"));
        try
        {
            File.WriteAllText(Path.Combine(dir, "Mcp", "Host.cs"), "class H { void M(object app) { app.MapGet(\"/api/z\", () => 1); } }");

            Assert.Equal(new[] { "Mcp/Host.cs" }, MapperFiles(dir));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void TheSharedWriters_CallTheSweepBeforeAnythingElse()
    {
        string endpoints = File.ReadAllText(Path.Combine(ServiceDir(), "DarlingWebEndpoints.cs"));
        foreach (string method in new[] { "ToHttpResult", "MuteRuleToolResult" })
        {
            var bodies = StatementColumnCensusTests.BodiesOf(endpoints, "DarlingWebEndpoints", method);
            Assert.True(bodies.Count > 0, method + " was not found");
            Assert.All(bodies, b => Assert.Contains("DarlingWebStatementSweep.Apply(", b));
        }

        string sweepFile = Code(File.ReadAllText(Path.Combine(ServiceDir(), "DarlingFleetSweepEndpoints.cs")));
        foreach (string method in new[] { "JsonResult", "SweepError" })
        {
            Assert.Matches(@"IResult\s+" + method + @"\([^)]*\)\s*=>\s*DarlingWebStatementSweep\.JsonText\(", sweepFile);
        }
    }

    private static string[] Verdicts(string source, string file = "Planted.cs") =>
        Routes(source, file).Select(r => r.Route + "=" + (r.Swept ? "swept" : "unswept")).ToArray();

    [Fact]
    public void TheScan_FlagsARouteThatWritesAroundTheSweep()
    {
        const string Source = @"
class W {
  void M(object app) {
    app.MapGet(""/api/a"", async (HttpContext c) => { return ToHttpResult(x, ""/api/a"", l, 1); });
    app.MapGet(""/api/b"", async (HttpContext c) => { return Results.Text(body, ""application/json""); });
    app.MapGet(""/api/c"", () => JsonNodeResult(Build()));
    // app.MapGet(""/api/d"", () => 1);
  }
}";
        Assert.Equal(new[] { "\"/api/a\"=swept", "\"/api/b\"=unswept", "\"/api/c\"=unswept" }, Verdicts(Source));
    }

    [Fact]
    public void ASweepCallWhoseResultIsDiscarded_IsNotSwept()
    {
        const string Source = @"
class W {
  void M(object app) {
    app.MapGet(""/api/a"", async (HttpContext c) => { var _ = ToHttpResult(x, ""/api/a"", l, 1); return Results.Text(raw, ""application/json""); });
    app.MapGet(""/api/b"", (HttpContext c) => { ToHttpResult(x, ""/api/b"", l, 1); return Results.Ok(raw); });
    app.MapGet(""/api/c"", (HttpContext c) => ToHttpResult(x, ""/api/c"", l, 1).Foo(raw));
  }
}";
        Assert.Equal(new[] { "\"/api/a\"=unswept", "\"/api/b\"=unswept", "\"/api/c\"=unswept" }, Verdicts(Source));
    }

    [Fact]
    public void ASecondReturnThatIsNotTheSweep_FailsAnOtherwiseSweptRoute()
    {
        const string Source = @"
class W {
  void M(object app) {
    app.MapGet(""/api/a"", async (HttpContext c) => {
      if (bad) { return Results.Text(raw, ""application/json""); }
      return ToHttpResult(x, ""/api/a"", l, 1);
    });
    app.MapGet(""/api/b"", async (HttpContext c) => {
      if (bad) { return ErrorResult(""bad request"", 400); }
      return ToHttpResult(x, ""/api/b"", l, 1);
    });
    app.MapGet(""/api/e"", async (HttpContext c) => {
      if (bad) { return ErrorResult(bodyError, 400); }
      return ToHttpResult(x, ""/api/e"", l, 1);
    });
    app.MapGet(""/api/f"", async (HttpContext c) => {
      if (bad) { return ErrorResult(raw, 500); }
      return ToHttpResult(x, ""/api/f"", l, 1);
    });
    app.MapGet(""/api/c"", (HttpContext c) => ok ? ToHttpResult(x, ""/api/c"", l, 1) : Results.Text(raw, ""t""));
    app.MapGet(""/api/d"", async (HttpContext c) => { await c.Response.WriteAsync(raw); return ToHttpResult(x, ""/api/d"", l, 1); });
  }
}";
        Assert.Equal(
            new[]
            {
                "\"/api/a\"=unswept", "\"/api/b\"=swept", "\"/api/e\"=swept", "\"/api/f\"=unswept",
                "\"/api/c\"=unswept", "\"/api/d\"=unswept",
            },
            Verdicts(Source));
    }

    [Fact]
    public void AMapMethodsRoute_IsScanned()
    {
        const string Source = @"
class W {
  void M(object app) {
    app.MapMethods(""/api/m"", new[] { ""GET"" }, async (HttpContext c) => { return Results.Text(raw, ""application/json""); });
    app.MapMethods(""/api/n"", new[] { ""GET"", ""POST"" }, async (HttpContext c) => ToHttpResult(x, ""/api/n"", l, 1));
    app.Map(""/api/o"", () => Results.Text(raw, ""t""));
    app.MapFallback(() => Results.Text(raw, ""t""));
  }
}";
        Assert.Equal(
            new[] { "\"/api/m\"=unswept", "\"/api/n\"=swept", "\"/api/o\"=unswept", "MapFallback()=unswept" },
            Verdicts(Source));
    }

    [Fact]
    public void AGroupPrefixedMap_IsScannedUnderItsFullRoute()
    {
        const string Source = @"
class W {
  void M(object app) {
    var g = app.MapGroup(""/api/x"");
    g.MapGet(""/a"", () => Results.Text(raw, ""t""));
    g.MapPost(""/b"", async (HttpContext c) => ToHttpResult(x, ""/api/x/b"", l, 1));
    app.MapGroup(""/api/y"").MapDelete(""/c"", () => Results.Text(raw, ""t""));
  }
}";
        Assert.Equal(
            new[] { "\"/api/x\" + \"/a\"=unswept", "\"/api/x\" + \"/b\"=swept", "\"/api/y\" + \"/c\"=unswept" },
            Verdicts(Source));
    }

    [Fact]
    public void AMethodGroupHandler_IsJudgedByItsOwnBody()
    {
        const string Source = @"
class W {
  void M(object app) {
    app.MapGet(""/api/g"", Handle);
    app.MapGet(""/api/h"", async (HttpContext c) => ToHttpResult(x, ""/api/h"", l, 1));
    app.MapGet(""/api/i"", HandleSwept);
    app.MapGet(""/api/j"", W.HandleExpr);
    app.MapGet(""/api/k"", Missing);
  }
  static IResult Handle(HttpContext c) { return Results.Text(raw, ""application/json""); }
  static async Task<IResult> HandleSwept(HttpContext c) { var r = await Read(c); return ToHttpResult(r, ""/api/i"", l, 1); }
  static IResult HandleExpr(HttpContext c) => Results.Text(raw, ""application/json"");
}";
        Assert.Equal(
            new[] { "\"/api/g\"=unswept", "\"/api/h\"=swept", "\"/api/i\"=swept", "\"/api/j\"=unswept", "\"/api/k\"=unswept" },
            Verdicts(Source));
    }

    [Fact]
    public void AMethodGroupDefinedInAnotherFile_IsFound()
    {
        const string Mapper = @"class A { void M(object app) { app.MapGet(""/api/p"", Other.Handle); } }";
        const string Other = @"class Other { public static IResult Handle(HttpContext c) { return ToHttpResult(r, ""/api/p"", l, 1); } }";
        var project = new[] { (Code(Mapper), Mapper), (Code(Other), Other) };

        Assert.True(Routes(Mapper, "A.cs", project).Single().Swept);
        Assert.False(Routes(Mapper, "A.cs").Single().Swept);
    }

    [Fact]
    public void ALookalikeWriterName_DoesNotCountAsTheSweep()
    {
        const string Source = @"
class W {
  void M(object app) {
    app.MapGet(""/api/a"", () => MyJsonResult(raw));
    app.MapGet(""/api/b"", () => JsonResult(raw));
    app.MapGet(""/api/c"", () => MyToHttpResult(raw));
    app.MapGet(""/api/d"", () => Results.Text(""ToHttpResult(x)"", ""t"")); // ToHttpResult(x)
  }
}";
        Assert.Equal(new[] { "\"/api/a\"=unswept", "\"/api/b\"=unswept", "\"/api/c\"=unswept", "\"/api/d\"=unswept" }, Verdicts(Source));

        // The fleet-sweep file's own writers count there, and only there.
        Assert.Equal(new[] { "\"/api/a\"=unswept", "\"/api/b\"=swept", "\"/api/c\"=unswept", "\"/api/d\"=unswept" },
            Verdicts(Source, "DarlingFleetSweepEndpoints.cs"));
    }

    [Fact]
    public void ABranchOfATernaryOrCoalesce_MustBeSweptToo()
    {
        const string Source = @"
class W {
  void M(object app) {
    app.MapGet(""/api/a"", async (HttpContext c) => { return run is null ? SweepError(""none"", 404) : JsonResult(raw); });
    app.MapGet(""/api/b"", async (HttpContext c) => { return run is null ? SweepError(""none"", 404) : Results.Text(raw, ""t""); });
    app.MapGet(""/api/c"", async (HttpContext c) => { return await Pick() ?? Results.Text(raw, ""t""); });
  }
}";
        Assert.Equal(new[] { "\"/api/a\"=swept", "\"/api/b\"=unswept", "\"/api/c\"=unswept" }, Verdicts(Source, "DarlingFleetSweepEndpoints.cs"));
    }
}
