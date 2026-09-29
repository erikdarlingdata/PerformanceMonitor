/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every <c>Button</c>, <c>ToggleButton</c>, <c>MenuItem</c> and <c>TabItem</c> in Lite's XAML (and in the
/// <c>PerformanceMonitor.Ui</c> controls Lite hosts) has a UI Automation name source: an
/// <c>AutomationProperties.Name</c> / <c>LabeledBy</c>, or a <c>Content</c> / <c>Header</c> that is text.
///
/// <para>Found by the source accessibility audit that followed #4684. The bulk of it was one defect: the
/// 700-odd <c>ColumnFilterButtonStyle</c> buttons in DataGrid column headers, whose only content is the Segoe
/// MDL2 funnel glyph (a private-use character), so UIA read each one out as nothing. Content that is an element
/// tree (a sidebar button's glyph-plus-label <c>StackPanel</c>, a <c>StackPanel</c> tab header) fell through to
/// <c>ToString()</c>, the "System.Windows.Controls.TabItem Header:..." name #4684 saw on the plan sub-tabs.
/// The definition of "named" lives in <see cref="XamlAccessibleNames"/>, shared with the Viewer's twin pin.</para>
///
/// <para>The allowlist is the audit's own list of what is left: every entry names the issue that owns it, and
/// the comparison is exact in both directions, so a fixed control forces its entry out.</para>
/// </summary>
public sealed class XamlAccessibleNameTests
{
    /// <summary>Remaining exceptions. Each carries the issue that tracks it; none is fixed by this test.</summary>
    private static readonly (string File, string Type, string Key, string Issue)[] Allowed =
    [
        /* PlanViewerControl.xaml is the plan-viewer lane's file (StandalonePlanViewerController.cs and PlanViewerControl*.cs
           are held open for #4684 follow-ups), so the audit reports these five glyph-only buttons (zoom in, zoom out and
           three close X buttons) instead of naming them. #4692 owns them. */
        ("PerformanceMonitor.Ui/PlanViewerControl.xaml", "Button", "ZoomIn_Click", "#4692"),
        ("PerformanceMonitor.Ui/PlanViewerControl.xaml", "Button", "ZoomOut_Click", "#4692"),
        ("PerformanceMonitor.Ui/PlanViewerControl.xaml", "Button", "CloseStatements_Click", "#4692"),
        ("PerformanceMonitor.Ui/PlanViewerControl.xaml", "Button", "MinimapClose_Click", "#4692"),
        ("PerformanceMonitor.Ui/PlanViewerControl.xaml", "Button", "CloseProperties_Click", "#4692"),
    ];

    [Fact]
    public void EveryButtonToggleMenuItemAndTab_HasAUiAutomationNameSource()
    {
        var result = XamlAccessibleNames.Run(XamlAccessibleNames.RepoRoot(), "Lite", "PerformanceMonitor.Ui");

        /* Floors: an "all of them are named" assertion is vacuously true over a walk that found nothing. */
        Assert.True(result.FileCount >= 40, $"only {result.FileCount} XAML files found; the walk is not reading the tree");
        Assert.True(result.CheckedByType.GetValueOrDefault("Button") >= 600, "Button count fell below the floor; the scan stopped descending");
        Assert.True(result.CheckedByType.GetValueOrDefault("TabItem") >= 50, "TabItem count fell below the floor; the scan stopped descending");
        Assert.True(result.CheckedByType.GetValueOrDefault("MenuItem") >= 60, "MenuItem count fell below the floor; the scan stopped descending");

        var diff = XamlAccessibleNames.Diff(result.Unnamed, Allowed);
        Assert.True(diff is null, "UI Automation name sources are out of step with the allowlist:" + System.Environment.NewLine + diff);
    }
}
