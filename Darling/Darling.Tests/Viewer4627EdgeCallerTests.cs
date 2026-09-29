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
/// #4627 part 3: the plan viewer draws its edges in two places, <c>GetLinkColorBrush(PlanNode)</c> in
/// <c>PlanViewerControl.Rendering.cs</c> and the minimap's <c>GetLinkColorBrush(PlanNode, double)</c> in
/// <c>PlanViewerControl.Minimap.cs</c>. Both live in <c>PerformanceMonitor.Ui</c>, which is WPF and cannot run in
/// this project, so what they hand <c>PlanEdgeColour.ForChild</c> is pinned by reading the source.
///
/// <para>The rule is that a caller passes the NODE (<c>ForChild(child, limit)</c>) and lets
/// <c>RowEstimateHelper</c> decide what "expected rows" means from where the node sits in the tree. The numeric
/// overload (<c>hasActualStats, actualRows, expectedRows, limit</c>) takes four arguments, and a caller that
/// reaches for it has to invent the expected figure itself. The obvious invention, <c>child.EstimateRows</c>, is
/// the original #4627 bug: a total row count against a single execution's estimate. Its sibling,
/// <c>ActualRows / ActualExecutions</c>, is the parallel-zone bug that follows. The behaviour of the node
/// overload is pinned in <see cref="Viewer4627EdgeColourTests"/>; this class pins that the viewer uses it.</para>
///
/// <para>The scan reads comments and string literals as blanks, so prose that mentions a call, like the doc
/// comment on <c>GetLinkColorBrush</c>, cannot satisfy or trip a pin. It counts a call's arguments at the top
/// nesting level only, so a nested call's own commas are not miscounted; a self-check below feeds it both
/// shapes.</para>
/// </summary>
public sealed class Viewer4627EdgeCallerTests
{
    /// <summary>The two files that turn a plan edge into a brush.</summary>
    private static readonly string[] EdgeCallerFiles =
    {
        "PlanViewerControl.Rendering.cs",
        "PlanViewerControl.Minimap.cs",
    };

    /// <summary>
    /// Product code that could host a plan viewer. Tests are left out on purpose: they call the numeric
    /// overload to pin its tiers, and pinning them here would make that a violation.
    /// </summary>
    private static readonly string[][] ScannedRoots =
    {
        new[] { "PerformanceMonitor.Ui" },
        new[] { "Lite" },
        new[] { "Darling", "PerformanceMonitor.Darling.Viewer" },
    };

    [Theory]
    [InlineData("PlanViewerControl.Rendering.cs")]
    [InlineData("PlanViewerControl.Minimap.cs")]
    public void EdgeCaller_PassesTheNodeToPlanEdgeColour(string fileName)
    {
        var calls = ForChildCalls(RepoFile.ReadRepoFile("PerformanceMonitor.Ui", fileName));

        Assert.True(calls.Count >= 1,
            $"{fileName} no longer calls PlanEdgeColour.ForChild, so this pin has nothing to check. If the edge colour " +
            "moved, move the pin with it rather than dropping it.");

        foreach (var call in calls)
        {
            Assert.True(call.Qualified,
                $"{fileName}:{call.Line} calls ForChild without naming PlanEdgeColour, so this pin cannot tell which one it is.");
            Assert.True(call.Arguments.Count == 2 && call.Arguments[0] == "child",
                $"{fileName}:{call.Line} calls PlanEdgeColour.ForChild({string.Join(", ", call.Arguments)}). The edge colour " +
                "must be given the node, ForChild(child, limit), so RowEstimateHelper judges its expected rows from where " +
                "it sits in the tree (#4627). Passing numbers means choosing an 'expected' by hand, and child.EstimateRows " +
                "or ActualRows / ActualExecutions is wrong on one side of a Nested Loops join or the other.");
        }
    }

    [Fact]
    public void NoProductCode_CallsTheNumericOverload()
    {
        var violations = new List<string>();
        var callerFilesSeen = new HashSet<string>(StringComparer.Ordinal);
        var callsSeen = 0;

        foreach (var root in ScannedRoots)
        {
            var directory = RepoFile.PathTo(root);
            Assert.True(Directory.Exists(directory), $"{string.Join('/', root)} is not where this pin expects it, so the scan would read nothing.");

            foreach (var file in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories))
            {
                var relative = Path.GetRelativePath(directory, file);
                if (relative.Split(Path.DirectorySeparatorChar).Any(s => s is "bin" or "obj"))
                    continue;

                var source = File.ReadAllText(file);
                if (!source.Contains("ForChild", StringComparison.Ordinal))
                    continue;

                foreach (var call in ForChildCalls(source))
                {
                    callsSeen++;
                    if (EdgeCallerFiles.Contains(Path.GetFileName(file)))
                        callerFilesSeen.Add(Path.GetFileName(file));

                    if (!call.Qualified || call.Arguments.Count != 2)
                        violations.Add($"{Path.GetRelativePath(RepoFile.Root, file)}:{call.Line}: ForChild({string.Join(", ", call.Arguments)})");
                }
            }
        }

        Assert.True(callsSeen >= EdgeCallerFiles.Length && callerFilesSeen.SetEquals(EdgeCallerFiles),
            $"the scan saw {callsSeen} ForChild call(s) in {string.Join(", ", callerFilesSeen)}; it should see the calls in both " +
            $"{string.Join(" and ", EdgeCallerFiles)}, or it is not reading the viewer.");
        Assert.True(violations.Count == 0,
            "PlanEdgeColour.ForChild must be called with the node, ForChild(child, limit) (#4627). These calls pass numbers " +
            "or cannot be identified:" + Environment.NewLine + string.Join(Environment.NewLine, violations));
    }

    // ---- the scanner itself ------------------------------------------------------------------------

    /// <summary>
    /// A pin that reads nothing passes forever, so feed the scanner the shapes it has to get right: a call in a
    /// comment and one in a string (neither is code), the node form, the numeric form with a nested call whose
    /// own comma must not count, and a call that does not name the type.
    /// </summary>
    [Fact]
    public void Scanner_SeesOnlyCode_AndCountsOnlyTopLevelArguments()
    {
        var calls = ForChildCalls("""
            // PlanEdgeColour.ForChild(a, b, c, d) in a line comment
            /* PlanEdgeColour.ForChild(a, b, c, d) in a block comment */
            var text = "PlanEdgeColour.ForChild(a, b, c, d) in a string";
            var byNode = PlanEdgeColour.ForChild(child, limit);
            var byNumbers = PlanEdgeColour.ForChild(child.HasActualStats,
                child.ActualRows,
                Math.Max(1, child.EstimateRows),
                limit);
            var bare = ForChild(child, limit);
            """);

        Assert.Equal(3, calls.Count);

        Assert.True(calls[0].Qualified);
        Assert.Equal(new[] { "child", "limit" }, calls[0].Arguments);

        Assert.True(calls[1].Qualified);
        Assert.Equal(4, calls[1].Arguments.Count);
        Assert.Equal("Math.Max(1, child.EstimateRows)", calls[1].Arguments[2]);

        Assert.False(calls[2].Qualified);
        Assert.Equal(new[] { 4, 5, 9 }, calls.Select(c => c.Line).ToArray());
    }

    private sealed record ForChildCall(int Line, bool Qualified, IReadOnlyList<string> Arguments);

    /// <summary>Every <c>ForChild(...)</c> call in the code of <paramref name="source"/>, in order.</summary>
    private static List<ForChildCall> ForChildCalls(string source)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var calls = new List<ForChildCall>();

        foreach (Match match in Regex.Matches(code, @"\bForChild\s*\(", RegexOptions.CultureInvariant))
        {
            var open = match.Index + match.Length - 1;
            var arguments = new List<string>();
            var depth = 0;
            var argumentStart = open + 1;
            var close = -1;

            for (var i = open; i < code.Length && close < 0; i++)
            {
                switch (code[i])
                {
                    case '(' or '[' or '{':
                        depth++;
                        break;
                    case ')' or ']' or '}':
                        depth--;
                        if (depth == 0)
                            close = i;
                        break;
                    case ',' when depth == 1:
                        arguments.Add(Squash(code[argumentStart..i]));
                        argumentStart = i + 1;
                        break;
                }
            }

            Assert.True(close > open, $"a ForChild( call at offset {match.Index} never closes, so the scan cannot read its arguments.");
            var last = Squash(code[argumentStart..close]);
            if (last.Length > 0 || arguments.Count > 0)
                arguments.Add(last);

            var line = 1 + code.Take(match.Index).Count(c => c == '\n');
            var qualified = Regex.IsMatch(code[..match.Index], @"\bPlanEdgeColour\s*\.\s*$", RegexOptions.CultureInvariant);
            calls.Add(new ForChildCall(line, qualified, arguments));
        }

        return calls;
    }

    private static string Squash(string text) => Regex.Replace(text.Trim(), @"\s+", " ");
}
