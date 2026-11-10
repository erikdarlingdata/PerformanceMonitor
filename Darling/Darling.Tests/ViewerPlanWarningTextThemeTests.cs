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
using System.Windows.Media;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8540: the warning lines of an operator's tooltip in the plan viewer are readable in each Viewer theme. The shared control is built on
/// the STA thread with the theme in its own resources, the tooltip is built for an operator that carries one warning of each severity, and
/// each line's colour is read from its text block against the tooltip's own background.
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
}
