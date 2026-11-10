/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8540: a plan warning's text is readable in each Viewer theme wherever the plan viewer paints it by severity: the warning lines of an
/// operator's tooltip, the warning types the root operator's tooltip lists, and the warning headers of the Properties panel's Plan Warnings
/// and Warnings sections. The shared control is built on the STA thread with the theme in its own resources, and each line's colour is
/// read from its text block against the background of the element it sits in: the tooltip, or the section.
/// </summary>
public sealed class ViewerPlanWarningTextThemeTests
{
    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush solid ? solid.Color : Colors.Magenta;

    private static double Luminance(Color color)
    {
        static double Channel(byte value)
        {
            var c = value / 255.0;
            return c <= 0.03928 ? c / 12.92 : Math.Pow((c + 0.055) / 1.055, 2.4);
        }

        return (0.2126 * Channel(color.R)) + (0.7152 * Channel(color.G)) + (0.0722 * Channel(color.B));
    }

    private static double Contrast(Color a, Color b)
    {
        var la = Luminance(a);
        var lb = Luminance(b);
        return (Math.Max(la, lb) + 0.05) / (Math.Min(la, lb) + 0.05);
    }

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    [InlineData("Dark")]
    public void AnOperatorTooltipsWarningLines_AreReadableOnTheTooltip(string theme)
    {
        var lines = StaTestThread.Run(() =>
        {
            _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var control = new PlanViewerControl();
            control.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });

            var node = new PlanNode { NodeId = 1, PhysicalOp = "Hash Match", LogicalOp = "Inner Join" };
            node.Warnings.Add(new PlanWarning { WarningType = "No Join Predicate", Message = "The join has no predicate.", Severity = PlanWarningSeverity.Critical });
            node.Warnings.Add(new PlanWarning { WarningType = "Memory Grant", Message = "The grant was far larger than the memory used.", Severity = PlanWarningSeverity.Warning });
            node.Warnings.Add(new PlanWarning { WarningType = "Missing Statistics", Message = "A column has no statistics.", Severity = PlanWarningSeverity.Info });

            var build = typeof(PlanViewerControl).GetMethod("BuildNodeTooltip", Instance);
            Assert.True(build is not null, "PlanViewerControl.BuildNodeTooltip no longer exists under that name - this test's reflection anchor moved.");
            var tip = (ToolTip)build!.Invoke(control, new object?[] { node, null })!;
            var background = ColorOf(tip.Background);
            return ((StackPanel)tip.Content).Children.OfType<TextBlock>()
                .Where(t => t.Text.StartsWith('⚠'))
                .Select(t => (Line: t.Text, Colour: ColorOf(t.Foreground), Background: background))
                .ToList();
        });

        Assert.Equal(3, lines.Count);
        var weak = new List<string>();
        foreach (var line in lines)
        {
            var ratio = Contrast(line.Colour, line.Background);
            if (ratio < 4.5)
            {
                weak.Add($"{theme}: \"{line.Line}\" is {line.Colour} on the tooltip's {line.Background}, {ratio:0.00}:1, under 4.5:1");
            }
        }

        Assert.True(weak.Count == 0, string.Join(Environment.NewLine, weak));
    }

    private const string RootTooltip = "the root operator's tooltip";

    /// <summary>One painted warning line: where it is, its text, its colour and the background of the element it sits in.</summary>
    private readonly record struct Shown(string Where, string Line, Color Colour, Color Background);

    /// <summary>A one-operator statement that carries a warning of each severity on the statement and another of each on its operator.</summary>
    private static PlanStatement WarnedStatement()
    {
        var root = new PlanNode { NodeId = 0, PhysicalOp = "Hash Match", LogicalOp = "Inner Join" };
        root.Warnings.Add(new PlanWarning { WarningType = "No Join Predicate", Message = "The join has no predicate.", Severity = PlanWarningSeverity.Critical });
        root.Warnings.Add(new PlanWarning { WarningType = "Memory Grant", Message = "The grant was far larger than the memory used.", Severity = PlanWarningSeverity.Warning });
        root.Warnings.Add(new PlanWarning { WarningType = "Missing Statistics", Message = "A column has no statistics.", Severity = PlanWarningSeverity.Info });
        return new PlanStatement
        {
            StatementText = "SELECT c.* FROM dbo.Comments AS c JOIN dbo.Users AS u ON u.Id = c.UserId",
            RootNode = root,
            PlanWarnings = new List<PlanWarning>
            {
                new() { WarningType = "Excessive Grant", Message = "The statement was granted far more memory than it used.", Severity = PlanWarningSeverity.Critical },
                new() { WarningType = "Compile Timeout", Message = "The optimizer stopped early.", Severity = PlanWarningSeverity.Warning },
                new() { WarningType = "Query Hint", Message = "The statement carries a hint.", Severity = PlanWarningSeverity.Info },
            },
        };
    }

    /// <summary>
    /// Renders the statement, reads the warning types the root operator's tooltip lists, then clicks the root operator and reads the
    /// warning headers of the Properties panel's Plan Warnings and Warnings sections. Each header's background is its own section's.
    /// </summary>
    private static List<Shown> RenderAndSelectTheRoot(string theme) => StaTestThread.Run(() =>
    {
        _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var control = new PlanViewerControl();
        control.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });

        var render = typeof(PlanViewerControl).GetMethod("RenderStatement", Instance);
        Assert.True(render is not null, "PlanViewerControl.RenderStatement no longer exists under that name - this test's reflection anchor moved.");
        render!.Invoke(control, new object[] { WarnedStatement() });

        var canvas = control.FindName("PlanCanvas") as Canvas;
        Assert.True(canvas is not null, "the plan viewer has no PlanCanvas - the operators' host moved");
        var root = Assert.Single(canvas!.Children.OfType<Border>(), b => b.Tag is PlanNode);
        var shown = new List<Shown>();

        var tip = Assert.IsType<ToolTip>(root.ToolTip);
        var tipBackground = ColorOf(tip.Background);
        foreach (var line in ((StackPanel)tip.Content).Children.OfType<TextBlock>().Where(t => t.Text.StartsWith('⚠')))
        {
            shown.Add(new Shown(RootTooltip, line.Text, ColorOf(line.Foreground), tipBackground));
        }

        // A click on the operator selects it, which builds the Properties panel.
        root.RaiseEvent(new MouseButtonEventArgs(Mouse.PrimaryDevice, 0, MouseButton.Left) { RoutedEvent = UIElement.MouseLeftButtonUpEvent });
        var content = control.FindName("PropertiesContent") as Panel;
        Assert.True(content is not null, "the plan viewer has no PropertiesContent panel - the Properties panel's host moved");
        foreach (var title in new[] { "Plan Warnings", "Warnings" })
        {
            var section = Assert.Single(content!.Children.OfType<Expander>(), e => (e.Header as TextBlock)?.Text == title);
            var sectionBackground = ColorOf(section.Background);
            foreach (var warning in Assert.IsAssignableFrom<Panel>(section.Content).Children.OfType<StackPanel>())
            {
                var header = warning.Children.OfType<TextBlock>().First();
                shown.Add(new Shown(title, header.Text, ColorOf(header.Foreground), sectionBackground));
            }
        }

        return shown;
    });

    private static void AssertReadable(string theme, string where, int expectedLines, List<Shown> shown)
    {
        var lines = shown.Where(s => s.Where == where).ToList();
        Assert.True(lines.Count == expectedLines, $"{theme}: {where} shows {lines.Count} warning lines, expected {expectedLines}");
        var weak = new List<string>();
        foreach (var line in lines)
        {
            var ratio = Contrast(line.Colour, line.Background);
            if (ratio < 4.5)
            {
                weak.Add($"{theme}, {where}: \"{line.Line}\" is {line.Colour} on {line.Background}, {ratio:0.00}:1, under 4.5:1");
            }
        }

        Assert.True(weak.Count == 0, string.Join(Environment.NewLine, weak));
    }

    /// <summary>The root operator's tooltip lists each warning type of the statement and its operators once: three from the statement and three from the operator.</summary>
    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    [InlineData("Dark")]
    public void TheRootOperatorsTooltip_ListsItsWarningTypesReadably(string theme) =>
        AssertReadable(theme, RootTooltip, 6, RenderAndSelectTheRoot(theme));

    /// <summary>The headers sit on their section's background, which is not the tooltip's, so each is held to 4.5:1 against the section it is in.</summary>
    [Theory]
    [InlineData("Light", "Plan Warnings")]
    [InlineData("CoolBreeze", "Plan Warnings")]
    [InlineData("Dark", "Plan Warnings")]
    [InlineData("Light", "Warnings")]
    [InlineData("CoolBreeze", "Warnings")]
    [InlineData("Dark", "Warnings")]
    public void APropertiesWarningHeader_IsReadableOnItsSection(string theme, string section) =>
        AssertReadable(theme, section, 3, RenderAndSelectTheRoot(theme));
}
