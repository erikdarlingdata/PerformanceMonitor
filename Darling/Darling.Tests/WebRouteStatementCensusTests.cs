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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, the web route census. Every <c>app.Map*</c> route of the Darling web host answers through a writer that
/// sweeps its body with the statement filter (<c>ToHttpResult</c>, <c>MuteRuleToolResult</c>,
/// <c>DarlingWebStatementSweep</c>), or is on the short named list below, each with the reason it carries no tool
/// string. A new route fails here until it is one or the other, so a route added later cannot write statement text
/// around the sweep. The writers themselves are pinned to call the sweep first.
/// </summary>
public sealed class WebRouteStatementCensusTests
{
    private static readonly string[] RouteFiles =
    {
        "DarlingWebEndpoints.cs",
        "DarlingTriageEndpoint.cs",
        "AlertNotebookEndpoint.cs",
        "DarlingFleetSweepEndpoints.cs",
        "DarlingServerDatabasesEndpoint.cs",
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
    };

    private static readonly string[] SweptCalls =
    {
        "ToHttpResult(", "MuteRuleToolResult(", "DarlingWebStatementSweep.", "JsonResult(", "SweepError(",
    };

    private static readonly Regex MapCall = new(
        @"\.Map(?:Get|Post|Put|Patch|Delete)\(", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private sealed record RouteRow(string Route, bool Swept);

    private static List<RouteRow> Routes(string source)
    {
        string code = StatementColumnCensusTests.CodeOnly(source);
        var rows = new List<RouteRow>();
        foreach (Match m in MapCall.Matches(code))
        {
            int comma = source.IndexOf(',', m.Index + m.Length);
            if (comma < 0) continue;
            string route = source[(m.Index + m.Length)..comma].Trim();
            int arrow = code.IndexOf("=>", comma, StringComparison.Ordinal);
            if (arrow < 0) continue;
            int start = arrow + 2;
            while (start < code.Length && char.IsWhiteSpace(code[start])) start++;
            int end;
            if (start < code.Length && code[start] == '{')
            {
                int depth = 0;
                end = start;
                for (; end < code.Length; end++)
                {
                    if (code[end] == '{') depth++;
                    else if (code[end] == '}' && --depth == 0) break;
                }
            }
            else
            {
                end = code.IndexOf(';', start);
                if (end < 0) end = code.Length - 1;
            }

            string body = code[start..Math.Min(code.Length, end + 1)];
            rows.Add(new RouteRow(route, SweptCalls.Any(c => body.Contains(c, StringComparison.Ordinal))));
        }

        return rows;
    }

    private static string ServiceDir() => RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service");

    [Fact]
    public void EveryMappedRoute_IsSwept_OrOnTheNamedList()
    {
        var problems = new List<string>();
        int swept = 0, named = 0;
        var seenNamed = new HashSet<string>(StringComparer.Ordinal);
        foreach (string file in RouteFiles)
        {
            foreach (var row in Routes(File.ReadAllText(Path.Combine(ServiceDir(), file))))
            {
                if (row.Swept) { swept++; continue; }
                if (Named.ContainsKey(row.Route)) { named++; seenNamed.Add(row.Route); continue; }
                problems.Add(file + ": " + row.Route + " writes its body around the statement sweep and is not on the named list");
            }
        }

        foreach (string stale in Named.Keys.Where(k => !seenNamed.Contains(k)))
        {
            problems.Add("named route " + stale + " is no longer mapped (or now sweeps): update the list");
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));
        Assert.True(swept >= 8, "swept routes found: " + swept);
        Console.WriteLine("web routes: swept=" + swept + " named=" + named);
    }

    [Fact]
    public void NoOtherServiceFile_MapsARoute()
    {
        var mappers = Directory.EnumerateFiles(ServiceDir(), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "Mcp" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .Where(f => MapCall.IsMatch(StatementColumnCensusTests.CodeOnly(File.ReadAllText(f))))
            .Select(Path.GetFileName)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(RouteFiles.OrderBy(n => n, StringComparer.Ordinal).ToArray(), mappers);
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

        string sweepFile = StatementColumnCensusTests.CodeOnly(File.ReadAllText(Path.Combine(ServiceDir(), "DarlingFleetSweepEndpoints.cs")));
        foreach (string method in new[] { "JsonResult", "SweepError" })
        {
            Assert.Matches(@"IResult\s+" + method + @"\([^)]*\)\s*=>\s*DarlingWebStatementSweep\.JsonText\(", sweepFile);
        }
    }

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
        var rows = Routes(Source);

        Assert.Equal(new[] { "\"/api/a\"", "\"/api/b\"", "\"/api/c\"" }, rows.Select(r => r.Route).ToArray());
        Assert.Equal(new[] { true, false, false }, rows.Select(r => r.Swept).ToArray());
    }
}
