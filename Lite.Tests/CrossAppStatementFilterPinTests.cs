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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
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

    /// <summary>The three plan reads the filtered seam (<c>McpPlanTools.Read*PlanAsync</c>) wraps: each is called exactly
    /// once under <c>Lite/Mcp</c>, in <c>McpPlanTools.cs</c>.</summary>
    private static readonly string[] SeamReads = { "GetCachedQueryPlanAsync", "GetCachedProcedurePlanAsync", "FetchQueryStorePlanAsync" };

    /// <summary>Every plan read <c>LocalDataService</c> has, found by reflection (a method returning
    /// <c>Task&lt;string?&gt;</c> whose name holds "Plan"), so a plan read added later joins the pin with no edit here.</summary>
    private static string[] AllPlanReads() => typeof(PerformanceMonitorLite.Services.LocalDataService)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
        .Where(m => m.ReturnType == typeof(Task<string>) && m.Name.Contains("Plan", StringComparison.Ordinal))
        .Select(m => m.Name)
        .Distinct(StringComparer.Ordinal)
        .OrderBy(n => n, StringComparer.Ordinal)
        .ToArray();

    /// <summary>The offences in <paramref name="sources"/> (file name, source text): a seam read called anywhere but
    /// <c>McpPlanTools.cs</c> (or not exactly once there), and any other plan read called at all.</summary>
    private static List<string> UnseamedPlanReads(IEnumerable<(string File, string Source)> sources)
    {
        var stripped = sources.Select(f => (f.File, Source: CSharpSourceWalker.StripCommentsAndStrings(f.Source))).ToList();
        var offences = new List<string>();
        foreach (string read in AllPlanReads())
        {
            var callers = stripped
                .Select(f => (f.File, Count: Regex.Matches(f.Source, Regex.Escape(read) + @"\s*\(").Count))
                .Where(c => c.Count > 0)
                .ToList();
            bool ok = SeamReads.Contains(read, StringComparer.Ordinal)
                ? callers.Count == 1 && callers[0] == ("McpPlanTools.cs", 1)
                : callers.Count == 0;
            if (!ok) offences.Add(read + " <- " + string.Join(", ", callers.Select(c => c.File + " x" + c.Count)));
        }

        return offences;
    }

    [Fact]
    public void LitesPlanTools_ReadAStoredPlan_OnlyThroughTheFilteredSeam()
    {
        string dir = Path.Combine(RepoRoot(), "Lite", "Mcp");
        var sources = Directory.GetFiles(dir, "*.cs").Select(f => (Path.GetFileName(f), File.ReadAllText(f))).ToList();

        // the reflection finds all nine reads today, the seam's three among them: a rename that loses one fails here
        string[] all = AllPlanReads();
        Assert.True(all.Length >= 9, "the reflection should find every LocalDataService plan read, found: " + string.Join(", ", all));
        foreach (string expected in SeamReads.Concat(new[]
        {
            "GetSnapshotPlanTextAsync", "ResolveSnapshotEstimatedPlanAsync", "ResolveSnapshotLivePlanAsync",
            "FetchQueryPlanOnDemandAsync", "FetchProcedurePlanOnDemandAsync", "FetchPlanBySqlHandleAsync",
        }))
        {
            Assert.Contains(expected, all);
        }

        Assert.Empty(UnseamedPlanReads(sources));

        string plans = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(dir, "McpPlanTools.cs")));
        Assert.Contains("SensitiveStatements.Xml(", plans);
        Assert.Contains("ReadQueryStatsPlanAsync(dataService, resolved.ServerId, query_hash, PlanXmlOutputChars)", plans);
    }

    [Theory]
    [InlineData("GetSnapshotPlanTextAsync")]
    [InlineData("ResolveSnapshotEstimatedPlanAsync")]
    [InlineData("ResolveSnapshotLivePlanAsync")]
    [InlineData("FetchQueryPlanOnDemandAsync")]
    [InlineData("FetchProcedurePlanOnDemandAsync")]
    [InlineData("FetchPlanBySqlHandleAsync")]
    [InlineData("GetCachedQueryPlanAsync")]
    public void ThePlanReadPin_FailsOnAPlantedMcpSourceThatCallsAReadOutsideTheSeam(string read)
    {
        // RED proof: the shipped sources pass, and one more file under Lite/Mcp that calls the read fails the pin
        string dir = Path.Combine(RepoRoot(), "Lite", "Mcp");
        var sources = Directory.GetFiles(dir, "*.cs").Select(f => (Path.GetFileName(f), File.ReadAllText(f))).ToList();
        Assert.Empty(UnseamedPlanReads(sources));

        sources.Add(("McpPlantedTools.cs", "class P { async Task M(LocalDataService d) { var x = await d." + read + "(1); } }"));

        Assert.Contains(UnseamedPlanReads(sources), o => o.StartsWith(read + " <-", StringComparison.Ordinal));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
