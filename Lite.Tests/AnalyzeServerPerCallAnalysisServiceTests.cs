/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4726: the MCP host must hand every tool call its OWN analysis service. One shared instance answers a second,
/// overlapping analyze_server call with an empty list (the busy check) and the tool then reads the first call's
/// running state, so the second server got "No significant findings" for a server it never analyzed.
/// </summary>
public sealed class AnalyzeServerPerCallAnalysisServiceTests
{
    [Fact]
    public void TheMcpHostRegistersOneAnalysisServicePerCall_NotOneSharedInstance()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHostService.cs"));

        Assert.Contains("AddTransient<AnalysisService>(_ => new AnalysisService(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AddSingleton(new AnalysisService", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AddSingleton<AnalysisService>", source, StringComparison.Ordinal);

        /* Every per-call instance still gets the store's ONE shared baseline tier (#3941). */
        Assert.Contains("baselineCache: BaselineCache.For(_duckDb)", source, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
