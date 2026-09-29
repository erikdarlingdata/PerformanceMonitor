/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every <c>Button</c>, <c>ToggleButton</c>, <c>MenuItem</c> and <c>TabItem</c> in the Darling Viewer's XAML (and
/// in the <c>PerformanceMonitor.Ui</c> controls it hosts) has a UI Automation name source: an
/// <c>AutomationProperties.Name</c> / <c>LabeledBy</c>, or a <c>Content</c> / <c>Header</c> that is text. The
/// Viewer twin of Lite.Tests' <c>XamlAccessibleNameTests</c>; the definition of "named" is the shared
/// <see cref="XamlAccessibleNames"/>.
///
/// <para>Found by the source accessibility audit that followed #4684: the 600-odd <c>ColumnFilterButtonStyle</c>
/// column-header buttons had only a private-use glyph for content, and every button or tab whose content is an
/// element tree fell back to <c>ToString()</c>. The Viewer's <c>Themes/*.xaml</c> are scanned too; their
/// buttons live inside <c>ControlTemplate</c>s, which are template parts, not instances.</para>
///
/// <para>The allowlist is what the audit left: every entry names the issue that owns it, and the comparison is
/// exact in both directions, so a fixed control forces its entry out.</para>
/// </summary>
public sealed class ViewerXamlAccessibleNameTests
{
    /// <summary>Remaining exceptions. Each carries the issue that tracks it.</summary>
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
        var result = XamlAccessibleNames.Run(
            XamlAccessibleNames.RepoRoot(), "Darling/PerformanceMonitor.Darling.Viewer", "PerformanceMonitor.Ui");

        /* Floors: an "all of them are named" assertion is vacuously true over a walk that found nothing. */
        Assert.True(result.FileCount >= 40, $"only {result.FileCount} XAML files found; the walk is not reading the tree");
        Assert.True(result.CheckedByType.GetValueOrDefault("Button") >= 600, "Button count fell below the floor; the scan stopped descending");
        Assert.True(result.CheckedByType.GetValueOrDefault("TabItem") >= 60, "TabItem count fell below the floor; the scan stopped descending");
        Assert.True(result.CheckedByType.GetValueOrDefault("MenuItem") >= 80, "MenuItem count fell below the floor; the scan stopped descending");

        var diff = XamlAccessibleNames.Diff(result.Unnamed, Allowed);
        Assert.True(diff is null, "UI Automation name sources are out of step with the allowlist:" + System.Environment.NewLine + diff);
    }
}
