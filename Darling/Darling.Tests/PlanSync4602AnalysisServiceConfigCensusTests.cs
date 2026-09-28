/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4602: every production site that constructs a <c>DarlingAnalysisService</c> must pass it an
/// <c>analyzerConfig</c> argument, so MCP's plan advisories (<c>get_analysis_facts</c>, <c>analyze_server</c>,
/// drill-down) honor the same per-rule <c>darling.json</c> "analyzer" section the worker's scheduled pass
/// and the web endpoints already do. Before this fix, <c>DarlingMcpHostService.cs</c>'s construction call
/// passed no fifth argument and silently fell back to <c>AnalyzerConfig.Default</c>, so a rule a user
/// disabled kept firing over MCP while the same install's worker and web paths honored the override.
///
/// <para>Scanned with <see cref="CSharpSourceWalker"/> (comments and strings stripped first), the same
/// discipline <see cref="SharedBaselineCacheTests"/> uses for the same three call sites, so a construction
/// call named only inside a doc comment is not mistaken for a real site.</para>
/// </summary>
public sealed class PlanSync4602AnalysisServiceConfigCensusTests
{
    /// <summary>Matches <c>new DarlingAnalysisService(</c> and captures everything up to the matching close
    /// paren is unnecessary here — the census only needs to know an <c>analyzerConfig</c>-shaped token
    /// appears somewhere on the same call, so it greps for the constructor name and asserts a nearby
    /// analyzer token, mirroring <see cref="SharedBaselineCacheTests"/>'s exact-string pins rather than
    /// re-deriving a weaker regex-based one.</summary>
    private static readonly Regex ConstructionSite = new(
        @"new DarlingAnalysisService\(([^;]*?)\)",
        RegexOptions.Singleline | RegexOptions.Compiled);

    [Theory]
    [InlineData("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs")]
    public void EveryProductionConstructionSite_PassesAnAnalyzerConfig(params string[] path)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(path));

        var matches = ConstructionSite.Matches(code);

        Assert.NotEmpty(matches);

        foreach (Match match in matches)
        {
            var args = match.Groups[1].Value;

            Assert.True(
                args.Contains("analyzerConfig", StringComparison.Ordinal) ||
                args.Contains("_analyzerConfig", StringComparison.Ordinal) ||
                args.Contains("AnalyzerConfig.Default", StringComparison.Ordinal) ||
                args.Contains("config.Analyzer", StringComparison.Ordinal),
                $"{string.Join("/", path)}: 'new DarlingAnalysisService({args})' has no analyzer-config argument — MCP's #4602 gap.");
        }
    }
}
