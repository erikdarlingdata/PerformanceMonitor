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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348, Lite's live plan display. The plans a Lite window fetches live from the monitored server
/// (<c>FetchQueryPlanOnDemandAsync</c>, <c>FetchProcedurePlanOnDemandAsync</c>, <c>FetchQueryStorePlanAsync</c>) are
/// not collected, so the collection-time filter never sees them: each display site passes the result through
/// <see cref="LivePlanDisplay.Filter"/> at the call. Pinned here: the helper, a scan of every caller in the app, and
/// the state the plan viewer is left in for a plan the filter withholds whole.
/// </summary>
public sealed class StatementScrubLivePlanDisplayTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    [Fact]
    public void ALiveFetchedCanaryPlan_ReachesTheDisplayStringWithTheMarker()
    {
        var shown = LivePlanDisplay.Filter(StatementScrubCanary.CanaryPlan());

        Assert.NotNull(shown);
        Assert.Contains(Marker, shown!, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, shown!, StringComparison.Ordinal);
        }

        foreach (var kept in StatementScrubCanary.KeptNeedles)
        {
            Assert.Contains(kept, shown!, StringComparison.Ordinal);
        }

        /* What the viewer loads: the plan still parses, statement 1 reads as the marker, statement 2 is untouched. */
        var parsed = ShowPlanParser.Parse(shown!);
        Assert.Null(parsed.ParseError);
    }

    [Fact]
    public void APlanTheFilterFindsNothingIn_IsTheSameInstance_AndNullAndEmptyPassThrough()
    {
        var plain = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + StatementScrubCanary.PlainStatement + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

        Assert.Same(plain, LivePlanDisplay.Filter(plain));
        Assert.Null(LivePlanDisplay.Filter(null));
        Assert.Equal(string.Empty, LivePlanDisplay.Filter(string.Empty));
    }

    /// <summary>
    /// The state of the plan viewer for a plan withheld whole (a plan the judging budget could not cover): the
    /// marker is not a plan, so the shared parser refuses it and <c>PlanViewerControl.LoadPlan</c> shows the parse
    /// error in its empty-state title (<c>PlanDisplayText.ParseErrorMessage</c>) instead of a plan.
    /// </summary>
    [Fact]
    public void APlanWithheldWhole_LeavesTheViewerOnItsParseErrorEmptyState()
    {
        var parsed = ShowPlanParser.Parse(Marker);

        var message = PlanDisplayText.ParseErrorMessage(parsed);

        Assert.False(string.IsNullOrEmpty(message));
        Assert.DoesNotContain("S3cret", message!, StringComparison.Ordinal);
        Assert.DoesNotContain(PlanStatements.EnumerateAll(parsed), s => s.RootNode != null);
    }

    private static readonly Regex LiveFetchCall = new(
        @"LocalDataService\s*\.\s*Fetch(?:Query|Procedure|QueryStore)Plan\w*\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex Wrapped = new(
        @"LivePlanDisplay\s*\.\s*Filter\s*\(\s*await\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Every caller of a live plan fetch in the app passes the result through the helper at the call. The MCP plan
    /// tools are the one exception: they answer through <c>AnalyzeFilteredPlan</c> (see
    /// <c>StoredPlanAnalysisFilterTests</c>). A new caller fails here until it is wrapped.
    /// </summary>
    [Fact]
    public void EveryLiveFetchCaller_PassesThePlanThroughTheHelperAtTheCall()
    {
        var liteRoot = Path.Combine(RepoRoot(), "Lite");
        var problems = new List<string>();
        var sites = 0;
        var files = new HashSet<string>(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(liteRoot, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(liteRoot, file).Replace('\\', '/');
            if (relative.StartsWith("obj/", StringComparison.Ordinal)
                || relative.StartsWith("bin/", StringComparison.Ordinal)
                || relative.StartsWith("Services/LocalDataService", StringComparison.Ordinal)
                || relative == "Mcp/McpPlanTools.cs")
            {
                continue;
            }

            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            foreach (Match match in LiveFetchCall.Matches(code))
            {
                sites++;
                files.Add(relative);
                /* The call is `LivePlanDisplay.Filter(await LocalDataService.Fetch...(`, so what precedes `LocalDataService`
                   must end in the wrapper and the `await`. */
                var awaitIndex = code.LastIndexOf("await", match.Index, StringComparison.Ordinal);
                var before = awaitIndex < 0 ? string.Empty : code[..(awaitIndex + "await".Length)];
                var between = awaitIndex < 0 ? "x" : code[(awaitIndex + "await".Length)..match.Index];
                if (awaitIndex < 0 || between.Trim().Length != 0 || !Wrapped.IsMatch(before))
                {
                    var line = code[..match.Index].Count(c => c == '\n') + 1;
                    problems.Add(relative + ":" + line + " calls " + match.Value.TrimEnd('(') + " without LivePlanDisplay.Filter(await ...)");
                }
            }
        }

        /* A scan that found nothing would pass vacuously: the display sites are in these six files. */
        Assert.True(sites >= 16, "expected at least 16 live plan fetch call sites, found " + sites);
        Assert.Equal(6, files.Count);
        Assert.Empty(problems);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
