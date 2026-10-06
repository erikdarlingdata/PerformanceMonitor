/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348: the cross-app source pin of the statement filter's MCP half. Both hosts register the SAME shared filter
/// object, last in their call-tool list, and Lite's plan tools read a stored plan only through the one filtered seam.
/// Source pins, because the defect they hold off is a wiring omission: every tool would still answer, unfiltered.
/// </summary>
public sealed class CrossAppStatementFilterPinTests
{
    private const string Registration = "AddCallToolFilter(SensitiveStatementOutputFilter.Instance)";

    [Fact]
    public void BothHosts_RegisterTheSharedStatementFilter_Once_AndLast()
    {
        foreach (string relative in new[]
        {
            Path.Combine("Lite", "Mcp", "McpHostService.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs"),
        })
        {
            string source = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(RepoRoot(), relative)));
            string raw = File.ReadAllText(Path.Combine(RepoRoot(), relative));

            Assert.Single(Regex.Matches(source, @"AddCallToolFilter\(\s*SensitiveStatementOutputFilter\.Instance\s*\)"));
            int last = raw.LastIndexOf(".AddCallToolFilter(", StringComparison.Ordinal);
            Assert.True(
                last > 0 && string.CompareOrdinal(raw, last + 1, Registration, 0, Registration.Length) == 0,
                relative + " does not register the shared statement filter as its LAST call-tool filter.");
        }

        Assert.Equal("PerformanceMonitor.Common", typeof(PerformanceMonitor.Common.SensitiveStatementOutputFilter).Namespace);
    }

    [Fact]
    public void LitesHost_RegistersItsFilterList_ThroughTheOneMethodTheTestsRun()
    {
        string source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHostService.cs"));

        Assert.Matches(@"\.WithRequestFilters\(\s*AddCallToolFilters\s*\)", source);
    }

    [Fact]
    public void LitesPlanTools_ReadAStoredPlan_OnlyThroughTheFilteredSeam()
    {
        string dir = Path.Combine(RepoRoot(), "Lite", "Mcp");
        foreach (string read in new[] { "GetCachedQueryPlanAsync", "GetCachedProcedurePlanAsync", "FetchQueryStorePlanAsync" })
        {
            var callers = Directory.GetFiles(dir, "*.cs")
                .Select(f => (File: Path.GetFileName(f), Count: Regex.Matches(CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(f)), Regex.Escape(read) + @"\s*\(").Count))
                .Where(c => c.Count > 0)
                .ToList();

            Assert.Equal(new[] { ("McpPlanTools.cs", 1) }, callers.Select(c => (c.File, c.Count)).ToArray());
        }

        string plans = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(dir, "McpPlanTools.cs")));
        Assert.Contains("SensitiveStatements.Xml(", plans);
        Assert.Contains("ReadQueryStatsPlanAsync(dataService, resolved.ServerId, query_hash, PlanXmlOutputChars)", plans);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
