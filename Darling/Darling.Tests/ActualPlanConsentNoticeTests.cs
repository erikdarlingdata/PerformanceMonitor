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
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every confirmation shown before a captured query is re-run for its actual plan says where the text came
/// from: the monitored server, where any user may have written it, so the operator reads it before allowing
/// the run. The sentence lives once in <see cref="QueryModificationDetector.CapturedQueryNotice"/>; these pins
/// hold its wording and that each prompt (the Darling viewer's, Lite's, and the shared plan navigation both
/// apps' history windows use) appends it. The prompts are WPF message boxes, so the prompt side is read from
/// source, anchored on the one-line reference.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class ActualPlanConsentNoticeTests
{
    [Fact]
    public void Notice_SaysTheTextCameFromTheMonitoredServerAndAnyUserMayHaveWrittenIt()
    {
        var notice = QueryModificationDetector.CapturedQueryNotice;
        Assert.Contains("captured from the monitored server", notice, StringComparison.Ordinal);
        Assert.Contains("any user of that server may have written it", notice, StringComparison.Ordinal);
        Assert.Contains("read it before you allow the run", notice, StringComparison.Ordinal);
    }

    private const string InsertPlan =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementType=\"INSERT\" StatementText=\"insert t values (1)\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    private const string SelectPlan =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementType=\"SELECT\" StatementText=\"select 1\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    [Fact]
    public void SharedHistoryWindowPrompt_FlagsAQueryThatModifiesData()
    {
        var modification = QueryModificationDetector.Detect(InsertPlan, "insert t values (1)");
        var text = PlanNavigationController.BuildConfirmationText("example-sql-01", "Sales", modification);

        Assert.Contains("DATA-MODIFICATION WARNING", text, StringComparison.Ordinal);
        Assert.Contains("INSERT", text, StringComparison.Ordinal);
        Assert.Contains("You are about to execute this query against example-sql-01 in database [Sales]", text, StringComparison.Ordinal);
        Assert.EndsWith(QueryModificationDetector.CapturedQueryNotice, text, StringComparison.Ordinal);
        Assert.Contains("DATA WILL BE MODIFIED", PlanNavigationController.ConfirmationTitle(modification), StringComparison.Ordinal);
        /* The warning comes first. */
        Assert.True(text.IndexOf("DATA-MODIFICATION WARNING", StringComparison.Ordinal) < text.IndexOf("You are about to execute", StringComparison.Ordinal));
    }

    [Fact]
    public void SharedHistoryWindowPrompt_ForAReadOnlyPlan_IsTheUsualPromptWithNoWarning()
    {
        var modification = QueryModificationDetector.Detect(SelectPlan, "select 1");
        var text = PlanNavigationController.BuildConfirmationText("example-sql-01", "", modification);

        Assert.DoesNotContain("DATA-MODIFICATION WARNING", text, StringComparison.Ordinal);
        Assert.StartsWith("You are about to execute this query against example-sql-01 in database [default]", text, StringComparison.Ordinal);
        Assert.Equal("Get Actual Plan", PlanNavigationController.ConfirmationTitle(modification));
    }

    [Fact]
    public void SharedHistoryWindowPrompt_WithNoPlan_FailsSafeToTheWarning()
    {
        var modification = QueryModificationDetector.Detect(null, "select 1");
        var text = PlanNavigationController.BuildConfirmationText("example-sql-01", "Sales", modification);

        Assert.Contains("could not be analyzed", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSharedController_RunsTheDetector()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "PerformanceMonitor.Ui/PlanNavigationController.cs"));
        Assert.Contains("QueryModificationDetector.Detect(estimatedPlanXml, queryText)", source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerActualPlanFlow.cs")]
    [InlineData("Lite/Controls/ServerTab.Plans.cs")]
    [InlineData("PerformanceMonitor.Ui/PlanNavigationController.cs")]
    public void EveryActualPlanConfirmation_ShowsTheNotice(string relative)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), relative));
        Assert.Contains("QueryModificationDetector.CapturedQueryNotice", source, StringComparison.Ordinal);
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
