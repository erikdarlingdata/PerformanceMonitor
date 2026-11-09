/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Create Mute Rule dialog's per-metric boxes share a two-column grid. A box whose row partner the metric hides takes the whole row,
/// so the value it opened with reads whole: from a Blocking Detected alert, Database and Query Text span the grid; from a Failed Agent
/// Job alert, Job Name spans it from the left edge. With Metric at (any) every box shows and the two columns stay.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class MuteRuleDialogFieldWidthTests
{
    [Fact]
    public void FromABlockingDetectedAlert_DatabaseAndQueryText_SpanTheWholeRow()
    {
        var (grid, database, queryText) = Measure(new AlertMuteContext { ServerName = "sql01", MetricName = "Blocking Detected", DatabaseName = "Orders", QueryText = "SELECT v FROM dbo.walk_rows WHERE id = @id" },
            "DatabasePatternBox", "QueryTextPatternBox");

        Assert.True(database.Width >= grid - 1, $"Database is {database.Width:F0} px wide in a {grid:F0} px grid");
        Assert.True(queryText.Width >= grid - 1, $"Query Text is {queryText.Width:F0} px wide in a {grid:F0} px grid");
        Assert.True(database.Left <= 1 && queryText.Left <= 1, $"the boxes start at {database.Left:F0} and {queryText.Left:F0}, not at the grid's left edge");
    }

    [Fact]
    public void FromAFailedAgentJobAlert_JobName_SpansTheWholeRowFromTheLeftEdge()
    {
        var (grid, jobName, _) = Measure(new AlertMuteContext { ServerName = "sql01", MetricName = "Failed Agent Job", JobName = "nightly maintenance" },
            "JobNamePatternBox", "JobNamePatternBox");

        Assert.True(jobName.Left <= 1, $"Job Name starts at {jobName.Left:F0} px, in the right column of a {grid:F0} px grid");
        Assert.True(jobName.Width >= grid - 1, $"Job Name is {jobName.Width:F0} px wide in a {grid:F0} px grid");
    }

    [Fact]
    public void WithMetricAtAny_DatabaseAndWaitType_KeepTheirTwoColumns()
    {
        var (grid, database, waitType) = Measure(null, "DatabasePatternBox", "WaitTypePatternBox");

        Assert.True(database.Width < grid / 2 + 1, $"Database is {database.Width:F0} px wide in a {grid:F0} px grid, so it covers Wait Type");
        Assert.True(waitType.Left > grid / 2 - 1, $"Wait Type starts at {waitType.Left:F0} px in a {grid:F0} px grid");
    }

    /// <summary>The pattern grid's width and the two named boxes' widths and left edges within it, in the laid-out dialog.</summary>
    private static (double Grid, (double Left, double Width) First, (double Left, double Width) Second) Measure(AlertMuteContext? context, string first, string second) =>
        StaTestThread.Run(() => WithTheme(() =>
        {
            var dialog = context is null ? new MuteRuleDialog() : new MuteRuleDialog(context);
            dialog.Left = 0;
            dialog.Top = 0;
            StaTestThread.ShowOffScreen(dialog);
            try
            {
                dialog.UpdateLayout();
                var grid = (Grid)dialog.FindName("PatternFieldsGrid");
                Assert.True(grid.ActualWidth > 100, $"the pattern grid laid out {grid.ActualWidth:F0} px wide");
                return (grid.ActualWidth, Place(grid, (FrameworkElement)dialog.FindName(first)), Place(grid, (FrameworkElement)dialog.FindName(second)));
            }
            finally
            {
                dialog.Close();
            }
        }));

    private static (double Left, double Width) Place(Grid grid, FrameworkElement box)
    {
        Assert.True(box.IsVisible, $"{box.Name} is not shown");
        return (box.TransformToAncestor(grid).Transform(new Point(0, 0)).X, box.ActualWidth);
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("AccentButton"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
                app.Resources.MergedDictionaries.Remove(added);
        }
    }
}
