/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4836: the Runtime Summary card nests the early abort reason under Optimization, as
/// erikdarlingdata/PerformanceStudio#614 does. The nested row's LABEL gets a 12px left indent and its VALUE
/// stays in the same column as every other value. <see cref="Viewer4570Tests"/> pins which row is nested (the
/// pure row model, which runs anywhere); these render the card in a live <see cref="PlanViewerControl"/> and
/// read the margins back, so they also prove the control applies the indent to the label and only the label.
/// They need STA and WPF. <c>ShowRuntimeSummary</c> stays <c>private</c>, called through reflection (the shape
/// <c>PlanViewer4632FallbackTests</c> in Lite.Tests uses for <c>CriticalOrangeBrush</c>), so this adds no
/// product surface; <see cref="PlanViewerControl.LoadPlan"/> is not used because it awaits <c>Task.Run</c>
/// and nothing here pumps a dispatcher.
/// </summary>
public sealed class PlanViewerRuntimeSummaryNestingTests
{
    private static PlanStatement StatementWith(string? optimizationLevel, string? earlyAbortReason) => new()
    {
        StatementText = "SELECT 1",
        StatementOptmLevel = optimizationLevel,
        StatementOptmEarlyAbortReason = earlyAbortReason,
        CardinalityEstimationModelVersion = 160
    };

    [Fact]
    public void RuntimeSummary_EarlyAbortUnderOptimization_IndentsItsLabelOnly()
    {
        OnStaThread(() =>
        {
            var control = new PlanViewerControl();
            try
            {
                var grid = RenderRuntimeSummary(control, StatementWith("FULL", "TimeOut"));

                var ceModel = RowOf(grid, "CE model");
                var optimization = RowOf(grid, "Optimization");
                var earlyAbort = RowOf(grid, "Early abort");

                // Optimization, then its reason on the next row, at the end of the card.
                Assert.Equal(Grid.GetRow(ceModel.Label) + 1, Grid.GetRow(optimization.Label));
                Assert.Equal(Grid.GetRow(optimization.Label) + 1, Grid.GetRow(earlyAbort.Label));
                Assert.Equal(grid.RowDefinitions.Count - 1, Grid.GetRow(earlyAbort.Label));

                // The label alone moves: 12px in, every other margin edge as on the un-nested rows.
                Assert.Equal(new Thickness(12, 1, 8, 1), earlyAbort.Label.Margin);
                Assert.Equal(new Thickness(0, 1, 8, 1), optimization.Label.Margin);
                Assert.Equal(new Thickness(0, 1, 8, 1), ceModel.Label.Margin);

                // The value stays where every other value sits: same column, same margin.
                Assert.Equal("TimeOut", earlyAbort.Value.Text);
                Assert.Equal(1, Grid.GetColumn(earlyAbort.Value));
                Assert.Equal(Grid.GetColumn(optimization.Value), Grid.GetColumn(earlyAbort.Value));
                Assert.Equal(optimization.Value.Margin, earlyAbort.Value.Margin);

                // Nothing but the early abort label is indented.
                Assert.All(
                    grid.Children.OfType<TextBlock>().Where(t => Grid.GetColumn(t) == 0 && t.Text != "Early abort"),
                    t => Assert.Equal(0d, t.Margin.Left));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void RuntimeSummary_EarlyAbortWithNoOptimizationRow_IsNotIndented()
    {
        OnStaThread(() =>
        {
            var control = new PlanViewerControl();
            try
            {
                var grid = RenderRuntimeSummary(control, StatementWith(optimizationLevel: null, earlyAbortReason: "TimeOut"));

                var earlyAbort = RowOf(grid, "Early abort");

                Assert.Equal(new Thickness(0, 1, 8, 1), earlyAbort.Label.Margin);
                Assert.Equal(1, Grid.GetColumn(earlyAbort.Value));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    /* ── card lookups ── */

    /// <summary>Runs the control's private <c>ShowRuntimeSummary</c> and returns the card's grid.</summary>
    private static Grid RenderRuntimeSummary(PlanViewerControl control, PlanStatement statement)
    {
        var method = typeof(PlanViewerControl).GetMethod("ShowRuntimeSummary", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.True(method is not null, "PlanViewerControl.ShowRuntimeSummary no longer exists under that name - this pin's reflection anchor moved.");
        method!.Invoke(control, new object[] { statement });

        return Assert.IsType<Grid>(Assert.Single(control.RuntimeSummaryContent.Children));
    }

    /// <summary>The label (column 0) and value (column 1) TextBlocks of the card row with this label.</summary>
    private static (TextBlock Label, TextBlock Value) RowOf(Grid grid, string label)
    {
        var texts = grid.Children.OfType<TextBlock>().ToList();
        var labelText = texts.Single(t => Grid.GetColumn(t) == 0 && t.Text == label);
        var valueText = texts.Single(t => Grid.GetColumn(t) == 1 && Grid.GetRow(t) == Grid.GetRow(labelText));
        return (labelText, valueText);
    }

    /// <summary>WPF objects require STA; same shape as the other WPF tests here.</summary>
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
