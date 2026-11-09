/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8458: an operator's warnings live in the body of its Warnings section, so collapsing the section's header takes them off the
/// screen, as collapsing Plan Warnings already does for the statement's. The control is built on the shared STA thread and its private
/// builder is reached by reflection, as <see cref="PlanViewerEstimatedExecutionsTests"/> does; the collapse is read as laid-out height
/// at a stated width, and nothing reads source text (#5833).
/// </summary>
public sealed class PlanViewerWarningsSectionCollapseTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const double PanelWidth = 420;

    /* A scan with one analyzer warning: the panel's header rows, a Warnings section and nothing else. */
    private static PlanNode WarnedScan() => new()
    {
        NodeId = 4,
        PhysicalOp = "Clustered Index Scan",
        LogicalOp = "Clustered Index Scan",
        Warnings =
        {
            new PlanWarning
            {
                WarningType = "Residual Predicate",
                Message = "The scan reads every row and tests the predicate on each one.",
                Severity = PlanWarningSeverity.Warning,
            },
        },
    };

    [Fact]
    public void TheWarnings_AreInTheWarningsSectionsBody_NotLooseInThePanel()
    {
        var (inSection, looseInPanel) = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            var node = WarnedScan();
            ShowProperties(control, node);
            var header = PlanWarningDisplay.PlanWarningHeader(node.Warnings[0]);
            var content = PropertiesContent(control);
            var body = Assert.IsAssignableFrom<Panel>(WarningsSection(content).Content);
            return (body.Children.OfType<StackPanel>().Any(p => FirstText(p) == header),
                    content.Children.OfType<StackPanel>().Any(p => FirstText(p) == header));
        });

        Assert.True(inSection, "the operator's warning is not in the Warnings section's body");
        Assert.False(looseInPanel, "the operator's warning sits in the panel root after the Warnings section, where collapsing the section cannot reach it");
    }

    [Fact]
    public void CollapsingTheWarningsSection_TakesTheWarningsOffTheScreen()
    {
        var (expandedHeight, warningHeight, collapsedHeight) = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            var node = WarnedScan();
            ShowProperties(control, node);
            var header = PlanWarningDisplay.PlanWarningHeader(node.Warnings[0]);
            var content = PropertiesContent(control);
            var section = WarningsSection(content);

            Layout(content);
            var warning = WarningPanels(content).First(p => FirstText(p) == header);
            var expanded = content.DesiredSize.Height;
            var warningTall = warning.DesiredSize.Height;

            section.IsExpanded = false;
            Layout(content);
            return (expanded, warningTall, content.DesiredSize.Height);
        });

        Assert.True(warningHeight > 0, "the warning laid out at no height, so the collapse cannot be read");
        Assert.True(expandedHeight - collapsedHeight >= warningHeight - 0.5,
            $"collapsing Warnings took the panel from {expandedHeight:F1} to {collapsedHeight:F1} px, but the warning is {warningHeight:F1} px tall, so it stayed on screen");
    }

    private static void ShowProperties(PlanViewerControl control, PlanNode node)
    {
        var show = typeof(PlanViewerControl).GetMethod("ShowPropertiesPanel", Instance);
        Assert.True(show is not null, "PlanViewerControl.ShowPropertiesPanel no longer exists under that name - this test's reflection anchor moved.");
        show!.Invoke(control, new object[] { node });
    }

    private static Panel PropertiesContent(PlanViewerControl control)
    {
        var content = control.FindName("PropertiesContent") as Panel;
        Assert.True(content is not null, "the plan viewer has no PropertiesContent panel - the Properties panel's host moved");
        return content!;
    }

    /// <summary>The one section titled Warnings: an Expander whose header is a TextBlock, as every Properties section is.</summary>
    private static Expander WarningsSection(Panel content)
    {
        var sections = content.Children.OfType<Expander>().Where(e => (e.Header as TextBlock)?.Text == "Warnings").ToList();
        Assert.True(sections.Count == 1, $"expected one Warnings section, found {sections.Count}");
        return sections[0];
    }

    /// <summary>Every prose panel the Properties panel holds, whether in a section's body or loose in the panel root.</summary>
    private static IEnumerable<StackPanel> WarningPanels(Panel content) =>
        content.Children.OfType<StackPanel>()
            .Concat(content.Children.OfType<Expander>().Select(e => e.Content).OfType<Panel>().SelectMany(p => p.Children.OfType<StackPanel>()));

    private static string? FirstText(Panel panel) => panel.Children.OfType<TextBlock>().FirstOrDefault()?.Text;

    /// <summary>Lays the panel out at the stated width with every element re-measured, so a section's open or closed state is what the heights read.</summary>
    private static void Layout(Panel content)
    {
        Invalidate(content);
        content.Measure(new Size(PanelWidth, double.PositiveInfinity));
        content.Arrange(new Rect(0, 0, PanelWidth, content.DesiredSize.Height));
        content.UpdateLayout();
    }

    private static void Invalidate(DependencyObject element)
    {
        if (element is UIElement ui)
        {
            ui.InvalidateMeasure();
            ui.InvalidateArrange();
        }

        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(element); i++)
        {
            Invalidate(VisualTreeHelper.GetChild(element, i));
        }
    }
}
