/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows.Automation.Peers;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Release walk V10, D20, D21 and D22 in the server tab of Lite and of the Darling Viewer (a copy of Lite's): the four toolbar
/// combos had no name for a screen reader, the Compare box was greyed on most tabs with a tooltip that WPF never shows on a
/// disabled control, the Running Jobs tab with no running job was an empty grid with no words, the plan viewer's operator tooltips
/// could not be read by a screen reader, and two FinOps column headers had no tooltip while their cells did.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class ServerTabToolbarAndTooltipsTests
{
    private static string Xaml(string app) => app == "Lite"
        ? RepoFile.ReadRepoFile("Lite", "Controls", "ServerTab.xaml")
        : RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");

    private static string FinOpsXaml(string app) => app == "Lite"
        ? RepoFile.ReadRepoFile("Lite", "Controls", "FinOpsTab.xaml")
        : RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");

    public static IEnumerable<object[]> BothTabs() => new[] { new object[] { "Lite" }, new object[] { "Viewer" } };

    /// <summary>The opening tag of the element with the given x:Name, up to its first '&gt;'.</summary>
    private static string OpeningTag(string xaml, string name)
    {
        var at = xaml.IndexOf($"x:Name=\"{name}\"", StringComparison.Ordinal);
        Assert.True(at > 0, $"{name} not found");
        var start = xaml.LastIndexOf('<', at);
        return xaml[start..(xaml.IndexOf('>', at) + 1)];
    }

    [Theory]
    [MemberData(nameof(BothTabs))]
    public void ToolbarCombos_HaveANameForAScreenReader(string app)
    {
        var expected = new Dictionary<string, string>
        {
            ["TimeRangeCombo"] = "Time range",
            ["CompareToCombo"] = "Compare to",
            ["AutoRefreshIntervalCombo"] = "Auto-refresh interval",
            ["TimeDisplayModeBox"] = "Time display",
        };

        foreach (var (control, name) in expected)
        {
            Assert.Contains($"AutomationProperties.Name=\"{name}\"", OpeningTag(Xaml(app), control), StringComparison.Ordinal);
        }
    }

    [Theory]
    [MemberData(nameof(BothTabs))]
    public void CompareBox_ShowsItsToolTipEvenWhenDisabled(string app)
    {
        Assert.Contains("ToolTipService.ShowOnDisabled=\"True\"", OpeningTag(Xaml(app), "CompareToCombo"), StringComparison.Ordinal);
    }

    [Fact]
    public void Viewer_CompareToolTip_NamesTheTabsItWorksOn()
    {
        var tip = ViewerServerTab.CompareUnavailableToolTip;

        Assert.Contains("Top Queries", tip, StringComparison.Ordinal);
        Assert.Contains("Top Procedures", tip, StringComparison.Ordinal);
        Assert.Contains("Query Store", tip, StringComparison.Ordinal);
        Assert.DoesNotContain("not available", tip, StringComparison.Ordinal);
    }

    [Fact]
    public void Lite_CompareToolTip_NamesTheTabsItWorksOn()
    {
        var code = RepoFile.ReadRepoFile("Lite", "Controls", "ServerTab.Comparison.cs");
        var constant = Regex.Match(code, @"CompareUnavailableToolTip\s*=\s*""([^""]+)""");

        Assert.True(constant.Success);
        foreach (var tab in new[] { "Overview", "Top Queries", "Top Procedures", "Query Store" })
        {
            Assert.Contains(tab, constant.Groups[1].Value, StringComparison.Ordinal);
        }

        Assert.Contains("CompareToCombo.ToolTip = CompareUnavailableToolTip;", code, StringComparison.Ordinal);
    }

    /// <summary>The Viewer's Compare box used to stay enabled on every other tab once the Queries sub-tab had been on a supported one.</summary>
    [Theory]
    [InlineData(ViewerServerTab.QueriesInnerTabIndex, 3, true)]
    [InlineData(ViewerServerTab.QueriesInnerTabIndex, 4, true)]
    [InlineData(ViewerServerTab.QueriesInnerTabIndex, 5, true)]
    [InlineData(ViewerServerTab.QueriesInnerTabIndex, 0, false)]
    [InlineData(ViewerServerTab.QueriesInnerTabIndex, 9, false)]
    [InlineData(0, 3, false)]
    [InlineData(1, 3, false)]
    [InlineData(ViewerServerTab.QueriesInnerTabIndex + 1, 4, false)]
    public void Viewer_CompareWorksOnlyOnTheThreeQueriesGrids(int innerTab, int queriesSubTab, bool expected)
    {
        Assert.Equal(expected, ViewerServerTab.IsComparisonSupported(innerTab, queriesSubTab));
    }

    [Fact]
    public void Viewer_InnerTabSwitch_UpdatesTheCompareBox()
    {
        var code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml.cs");
        var handler = code[code.IndexOf("private async void InnerTabs_SelectionChanged", StringComparison.Ordinal)..];

        Assert.Contains("UpdateCompareDropdownState();", handler[..handler.IndexOf("RefreshActiveInnerTabAsync", StringComparison.Ordinal)], StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(BothTabs))]
    public void RunningJobsGrid_SaysNoJobIsRunning_WhenItHasNoRows(string app)
    {
        var xaml = Xaml(app);
        Assert.Contains("Text=\"No SQL Agent jobs are running.\"", OpeningTag(xaml, "RunningJobsNoDataMessage"), StringComparison.Ordinal);

        var code = app == "Lite"
            ? RepoFile.ReadRepoFile("Lite", "Controls", "ServerTab.Refresh.cs")
            : RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.RunningJobs.cs");
        var call = Regex.Match(code, @"ShowEngineGap(?:Async)?\(RunningJobsNoDataMessage[^;]*;", RegexOptions.Singleline);

        Assert.True(call.Success, $"{app}: the Running Jobs load no longer calls the engine-gap step");
        Assert.Contains("keepsOwnEmptyText:", call.Value, StringComparison.Ordinal);
        Assert.DoesNotContain("keepsOwnEmptyText: false", call.Value, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(BothTabs))]
    public void FinOpsFindingsGrid_DetailAndSavingsHeaders_HaveATooltip(string app)
    {
        var xaml = FinOpsXaml(app);

        foreach (var header in new[] { "Detail", "Est. Savings ($/mo)" })
        {
            var at = xaml.IndexOf($"<DataGridTextColumn Header=\"{header}\"", StringComparison.Ordinal);
            Assert.True(at > 0, $"{app}: column {header} not found");
            var column = xaml[at..xaml.IndexOf("</DataGridTextColumn>", at, StringComparison.Ordinal)];

            Assert.Matches(@"<DataGridTextColumn\.HeaderStyle>.*<Setter Property=""ToolTip"" Value=""[^""]{20,}""", column.Replace("\r\n", " ").Replace("\n", " "));
        }
    }

    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        using var staGate = WpfStaGate.Enter();
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (error is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw();
        }

        return result;
    }

    private static PlanNode Scan() => new()
    {
        PhysicalOp = "Clustered Index Scan",
        LogicalOp = "Clustered Index Scan",
        CostPercent = 62,
        EstimatedOperatorCost = 1.5,
        EstimatedTotalSubtreeCost = 2.5,
        EstimateRows = 1200,
        ObjectName = "Orders",
        Warnings = { new PlanWarning { WarningType = "Missing Index", Message = "An index would help." } },
    };

    [Fact]
    public void PlanOperatorBox_ExposesItsTooltipSummaryToAScreenReader()
    {
        var node = Scan();

        var (name, help, isControl) = OnStaThread(() =>
        {
            var box = new PlanNodeBorder();
            System.Windows.Automation.AutomationProperties.SetName(box, PlanNodeBorder.OperatorName(node));
            System.Windows.Automation.AutomationProperties.SetHelpText(box, PlanNodeBorder.Summary(node, null));
            var peer = UIElementAutomationPeer.CreatePeerForElement(box);
            Assert.NotNull(peer);
            return (peer.GetName(), peer.GetHelpText(), peer.IsControlElement());
        });

        Assert.Equal("Clustered Index Scan", name);
        Assert.True(isControl);
        Assert.Contains("62%", help, StringComparison.Ordinal);
        Assert.Contains("Estimated rows", help, StringComparison.Ordinal);
        Assert.Contains("Orders", help, StringComparison.Ordinal);
        Assert.Contains("1 warning: Missing Index", help, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanOperatorSummary_PrefersActualRows_AndListsEachWarningTypeOnce()
    {
        var node = Scan();
        node.HasActualStats = true;
        node.ActualRows = 40;
        node.ActualExecutions = 3;
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Spill" },
            new() { WarningType = "Spill" },
            new() { WarningType = "Implicit Conversion" },
        };

        var text = PlanNodeBorder.Summary(node, warnings);

        Assert.Contains("actual rows 40", text, StringComparison.Ordinal);
        Assert.Contains("actual executions 3", text, StringComparison.Ordinal);
        Assert.Contains("3 warnings: Spill, Implicit Conversion.", text, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanViewer_BuildsItsOperatorBoxesAsPlanNodeBorders_WithHelpText()
    {
        var code = RepoFile.ReadRepoFile("PerformanceMonitor.Ui", "PlanViewerControl.Rendering.cs");

        Assert.Contains("var border = new PlanNodeBorder", code, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(code, @"AutomationProperties\.SetHelpText\(border, PlanNodeBorder\.Summary").Count);
        Assert.Contains("AutomationProperties.SetName(border, PlanNodeBorder.OperatorName(node))", code, StringComparison.Ordinal);
    }
}
