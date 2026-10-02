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
/// #4198 cut <c>get_query_store_top</c>'s default <c>query_text</c> to a 400-character preview, gated behind
/// a new <c>full_text</c> opt-in that defaults to <c>false</c> on the MCP signature. Unlike
/// <c>get_deadlock_detail</c>'s <c>deadlock_graph_xml</c> (<see cref="DeadlockDetailWebDefaultTests"/>), this
/// field already had a cap before #4198: a blanket, undisclosed 2,000-character truncation. The web viewer's
/// <c>/api/read</c> dispatch calls the same MCP method, so without its own default it would silently inherit
/// the new 400-character preview -- a real shrink, not just a change of number, since the viewer has always
/// shown up to 2,000 characters. This pins the web row to that exact old number through the internal
/// <c>previewLength</c> overload (the same shape #3897's trend tools use for <c>TrendBudget.Chart</c>), not to
/// <c>full_text: true</c>: full text would be MORE than the viewer ever rendered, which is still a change.
/// </summary>
public sealed class QueryStoreTopWebDefaultTests
{
    private const string WebEndpoints =
        "Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs";

    [Fact]
    public void GetQueryStoreTop_WebRowKeepsTheOld2000CharacterPreview()
    {
        var web = StripComments(ReadSource(WebEndpoints));

        var marker = "[\"get_query_store_top\"] = (c, pg, an) => DarlingMcpDataTools.GetQueryStoreTop(";
        var start = web.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(start >= 0, "get_query_store_top's /api/read row was not found (renamed or moved?).");
        var end = web.IndexOf("),\r\n", start, StringComparison.Ordinal);
        if (end < 0) end = web.IndexOf("),\n", start, StringComparison.Ordinal);
        Assert.True(end > start, "could not find the end of get_query_store_top's /api/read row.");
        var row = web[start..end];

        /* Other rows on this same page (get_plan_corrections, get_deadlock_detail) legitimately default
           their own full_text/full_graph to true, because THEIR field had no cap before #4198 — so the
           negative check below is scoped to this tool's own row, not the whole file. */
        Assert.Contains("previewLength: 2000", row, StringComparison.Ordinal);
        Assert.Contains("full_text: QueryBool(c, \"full_text\", false)", row, StringComparison.Ordinal);
        Assert.DoesNotContain("full_text: QueryBool(c, \"full_text\", true)", row, StringComparison.Ordinal);
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

        Assert.True(File.Exists(path), $"#4198 scan target not found: {path}");

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
