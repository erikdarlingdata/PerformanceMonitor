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
/// #4198 gave <c>get_object_locking</c> a new <c>limit</c> parameter (MCP default 75), replacing a hardcoded
/// 200-row fetch that had no override. The web viewer's <c>/api/read</c> dispatch calls that same MCP method,
/// so without its own default it would silently drop from 200 rows to 75. This pins the web row's own
/// <c>limit</c> default at 200 - the old effective ceiling, and the same value <c>get_index_usage</c>'s
/// neighboring row already pins for the identical reason - independent of (and higher than) the MCP
/// signature's own default.
/// </summary>
public sealed class ObjectLockingWebDefaultTests
{
    private const string WebEndpoints =
        "Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs";

    [Fact]
    public void GetObjectLocking_WebRowKeepsTheOldTwoHundredRowCeiling()
    {
        var web = StripComments(ReadSource(WebEndpoints));

        var dispatchLine = Assert.Single(
            web.Split('\n'),
            l => l.Contains("DarlingMcpObjectStatsTools.GetObjectLocking(", StringComparison.Ordinal));

        Assert.Contains("Rows(c, \"limit\", 200)", dispatchLine, StringComparison.Ordinal);

        /* get_index_usage's neighboring catalog row ALSO carries PLimit(200), for the same reason - a bare
           Assert.Contains over the whole file would pass even if THIS row's own declaration were deleted.
           Scope to the catalog line that declares get_object_locking, the only line with its description. */
        var catalogLine = Assert.Single(
            web.Split('\n'),
            l => l.Contains("[\"get_object_locking\"] = R(", StringComparison.Ordinal));

        Assert.Contains("PLimit(200)", catalogLine, StringComparison.Ordinal);
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

        Assert.True(File.Exists(path), $"#4258 scan target not found: {path}");

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
