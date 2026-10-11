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
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5639: every "Get Actual Plan" entry point, in Lite, the shared plan navigation and the Darling viewer, runs the
/// shared data-modification check (<see cref="QueryModificationDetector"/>) before it offers to run a query, and
/// words the result with the shared <see cref="QueryModificationDetector.BuildConsentWarning"/> and
/// <see cref="QueryModificationDetector.BuildConsentTitle"/>. The prompts are WPF message boxes, so the wiring is
/// read from source, the way <see cref="ActualPlanConsentNoticeTests"/> reads it.
///
/// <para>Three gates exist: <c>PlanNavigationController.GetActualPlanAsync</c> (the Lite history windows, the drill-down
/// and the FinOps grids), <c>ViewerActualPlanFlow.RequestActualPlanAsync</c> (every Darling viewer window) and Lite's
/// server tab (<c>ServerTab.Plans.cs</c>). A file that runs a query for its actual plan must go through one of them,
/// and a call into the shared navigation must state its stored plan (<c>estimatedPlanXml:</c>) so the check reads the
/// plan rather than falling back to the text scan.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class ActualPlanModificationCheckGuardTests
{
    private const string Controller = "PerformanceMonitor.Ui/PlanNavigationController.cs";
    private const string ViewerFlow = "Darling/PerformanceMonitor.Darling.Viewer/ViewerActualPlanFlow.cs";
    private const string ServerTabPlans = "Lite/Controls/ServerTab.Plans.cs";

    [Fact]
    public void TheDetector_FlagsAWritingPlan_AndTheTitleSaysSo()
    {
        const string insertPlan = "<ShowPlanXML><StmtSimple StatementType=\"INSERT\" /></ShowPlanXML>";
        const string selectPlan = "<ShowPlanXML><StmtSimple StatementType=\"SELECT\" /></ShowPlanXML>";

        var writes = QueryModificationDetector.Detect(insertPlan, "INSERT dbo.t SELECT 1");
        var reads = QueryModificationDetector.Detect(selectPlan, "SELECT 1");
        var noPlan = QueryModificationDetector.Detect(null, "SELECT 1");

        Assert.True(writes.ModifiesData);
        Assert.False(reads.ModifiesData);
        Assert.True(noPlan.ModifiesData);
        Assert.True(noPlan.IsUncertain);

        Assert.Contains("DATA WILL BE MODIFIED", QueryModificationDetector.BuildConsentTitle(writes), StringComparison.Ordinal);
        Assert.Contains("DATA WILL BE MODIFIED", QueryModificationDetector.BuildConsentTitle(noPlan), StringComparison.Ordinal);
        Assert.DoesNotContain("DATA WILL BE MODIFIED", QueryModificationDetector.BuildConsentTitle(reads), StringComparison.Ordinal);
        Assert.Equal(string.Empty, QueryModificationDetector.BuildConsentWarning(reads, "db"));
    }

    [Theory]
    [InlineData(Controller, "GetActualPlanAsync")]
    [InlineData(ViewerFlow, "RequestActualPlanAsync")]
    [InlineData(ServerTabPlans, "GetActualPlan_Click")]
    public void EveryGate_RunsTheSharedCheck_BeforeItsPrompt(string relative, string method)
    {
        var body = MethodBody(File.ReadAllText(Path.Combine(RepoRoot(), relative)), method);

        var detect = body.IndexOf("QueryModificationDetector.Detect(", StringComparison.Ordinal);
        var warning = body.IndexOf("QueryModificationDetector.BuildConsentWarning(", StringComparison.Ordinal);
        /* The confirmation is the OK/Cancel box; an earlier plain message (no query text) needs no check. */
        var prompt = body.IndexOf("MessageBoxButton.OKCancel", StringComparison.Ordinal);

        Assert.True(detect >= 0, $"{relative} {method} never calls QueryModificationDetector.Detect");
        Assert.True(prompt < 0 || detect < prompt, $"{relative} {method} shows its prompt before the modification check");

        /* The viewer's prompt lives in its Confirm helper; the others build the warning in the gate itself. */
        var flowSource = File.ReadAllText(Path.Combine(RepoRoot(), relative));
        Assert.True(warning >= 0 || flowSource.Contains("QueryModificationDetector.BuildConsentWarning(", StringComparison.Ordinal),
            $"{relative} never builds the shared consent warning");
        Assert.Contains("QueryModificationDetector.BuildConsentTitle(", flowSource, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryCallIntoTheSharedNavigation_StatesItsStoredPlan()
    {
        var calls = new List<string>();
        foreach (var file in SourceFiles("Lite"))
        {
            var text = File.ReadAllText(file);
            foreach (var call in CallsOf(text, ".GetActualPlanAsync("))
            {
                if (!call.Contains("estimatedPlanXml:", StringComparison.Ordinal))
                {
                    calls.Add($"{Path.GetRelativePath(RepoRoot(), file)}: {call.Replace('\n', ' ').Trim()}");
                }
            }
        }

        Assert.True(calls.Count == 0,
            "These calls into PlanNavigationController.GetActualPlanAsync do not pass estimatedPlanXml:\n" + string.Join("\n", calls));
    }

    [Fact]
    public void TheCensus_SeesTheKnownEntryPoints()
    {
        var found = SourceFiles("Lite")
            .Where(f => CallsOf(File.ReadAllText(f), ".GetActualPlanAsync(").Any())
            .Select(f => Path.GetFileName(f))
            .ToHashSet(StringComparer.Ordinal);

        foreach (var expected in new[]
        {
            "QueryStatsHistoryWindow.xaml.cs", "QueryStoreHistoryWindow.xaml.cs", "WaitDrillDownWindow.xaml.cs", "FinOpsTab.xaml.cs",
        })
        {
            Assert.Contains(expected, found);
        }
    }

    [Fact]
    public void EveryFileThatRunsAQueryForItsActualPlan_GoesThroughAGate()
    {
        var offenders = new List<string>();
        var roots = new[] { "Lite", "Darling/PerformanceMonitor.Darling.Viewer", "PerformanceMonitor.Ui" };
        foreach (var file in roots.SelectMany(SourceFiles))
        {
            var text = File.ReadAllText(file);
            var relative = Path.GetRelativePath(RepoRoot(), file).Replace('\\', '/');
            var runsQuery = text.Contains("ExecuteForActualPlanAsync(", StringComparison.Ordinal);
            var viewerRun = text.Contains(".ExecuteActualPlanAsync(", StringComparison.Ordinal)
                && !relative.EndsWith("ViewerDataService.ActualPlan.cs", StringComparison.Ordinal);

            if (viewerRun && relative != ViewerFlow)
            {
                offenders.Add($"{relative}: calls the viewer's execute_actual_plan command outside ViewerActualPlanFlow");
            }

            if (runsQuery)
            {
                var viaController = text.Contains("new PlanNavigationController(", StringComparison.Ordinal);
                var ownCheck = text.Contains("QueryModificationDetector.Detect(", StringComparison.Ordinal);
                if (!viaController && !ownCheck)
                {
                    offenders.Add($"{relative}: runs ExecuteForActualPlanAsync without the shared plan navigation or its own modification check");
                }
            }
        }

        Assert.True(offenders.Count == 0, "Entry points without the data-modification check:\n" + string.Join("\n", offenders));
    }

    /// <summary>The text of <paramref name="method"/>'s body (brace-matched), from its declaration to the closing brace.</summary>
    private static string MethodBody(string source, string method)
    {
        var decl = Regex.Match(source, @"\b" + Regex.Escape(method) + @"\s*\(", RegexOptions.CultureInvariant);
        while (decl.Success)
        {
            var open = source.IndexOf('{', decl.Index);
            var semi = source.IndexOf(';', decl.Index);
            var arrow = source.IndexOf("=>", decl.Index, StringComparison.Ordinal);
            if (open >= 0 && (semi < 0 || open < semi) && (arrow < 0 || open < arrow))
            {
                var depth = 0;
                for (var i = open; i < source.Length; i++)
                {
                    if (source[i] == '{') depth++;
                    else if (source[i] == '}' && --depth == 0)
                    {
                        return source.Substring(decl.Index, i - decl.Index + 1);
                    }
                }
            }

            decl = decl.NextMatch();
        }

        Assert.Fail($"No method body found for {method}");
        return string.Empty;
    }

    /// <summary>Each call's argument text, from the open parenthesis to its matching close.</summary>
    private static IEnumerable<string> CallsOf(string source, string marker)
    {
        var at = 0;
        while ((at = source.IndexOf(marker, at, StringComparison.Ordinal)) >= 0)
        {
            var start = at + marker.Length;
            var depth = 1;
            var i = start;
            for (; i < source.Length && depth > 0; i++)
            {
                if (source[i] == '(') depth++;
                else if (source[i] == ')') depth--;
            }

            yield return source.Substring(start, Math.Max(0, i - start - 1));
            at = start;
        }
    }

    private static IEnumerable<string> SourceFiles(string relativeRoot)
    {
        var sep = Path.DirectorySeparatorChar;
        return Directory.EnumerateFiles(Path.Combine(RepoRoot(), relativeRoot), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{sep}obj{sep}", StringComparison.Ordinal) && !f.Contains($"{sep}bin{sep}", StringComparison.Ordinal));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
