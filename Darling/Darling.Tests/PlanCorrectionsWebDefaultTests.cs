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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198 cut <c>get_plan_corrections</c>'s default <c>query_text</c> to a 150-character preview, gated behind
/// a new <c>full_text</c> opt-in that defaults to <c>false</c> on the MCP signature. The web viewer's
/// <c>/api/read</c> dispatch calls that same MCP method, so without its own default it would silently inherit
/// the preview. The viewer has always rendered the whole query text, and #4198 changed an MCP default only -
/// not what the viewer should show. This pins the web row's <c>full_text</c> default at <c>true</c>,
/// independent of (and opposite to) the MCP signature's own default.
/// </summary>
public sealed class PlanCorrectionsWebDefaultTests
{
    private const string WebEndpoints =
        "Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs";

    [Fact]
    public void GetPlanCorrections_WebRowKeepsTheFullQueryTextByDefault()
    {
        var web = StripComments(ReadSource(WebEndpoints));

        /* get_store_query_stats ALSO has a full_text opt-in, defaulting false to match ITS OWN MCP
           signature - a bare Assert.DoesNotContain over the whole file would false-positive on that
           unrelated row. Scope to get_plan_corrections' own dispatch line, the only line that calls
           DarlingMcpPlanCorrectionTools.GetPlanCorrections. */
        var dispatchLine = Assert.Single(
            web.Split('\n'),
            l => l.Contains("DarlingMcpPlanCorrectionTools.GetPlanCorrections(", StringComparison.Ordinal));

        Assert.Contains("full_text: QueryBool(c, \"full_text\", true)", dispatchLine, StringComparison.Ordinal);
        Assert.DoesNotContain("full_text: QueryBool(c, \"full_text\", false)", dispatchLine, StringComparison.Ordinal);
        Assert.Contains("PBool(\"full_text\", true)", web, StringComparison.Ordinal);
    }

    private static string StripComments(string source)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source).ToCharArray();

        foreach (var (start, text) in CSharpSourceWalker.StringLiteralBodies(source))
        {
            for (var i = 0; i < text.Length && start + i < stripped.Length; i++)
            {
                stripped[start + i] = text[i];
            }
        }

        var code = CSharpSourceWalker.CodeMask(source);

        for (var i = 0; i < source.Length; i++)
        {
            if (!code[i] && source[i] is '"' or '@' or '$')
            {
                stripped[i] = source[i];
            }
        }

        return new string(stripped);
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);

        Assert.True(File.Exists(path), $"#4257 scan target not found: {path}");

        return File.ReadAllText(path);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
