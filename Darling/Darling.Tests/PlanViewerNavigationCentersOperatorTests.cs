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
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8449: going to an operator, from a warning's link to where it came from, centres the operator itself in the plan
/// viewport, not its top-left corner, at whatever zoom the plan is shown at. And the zoom percentage is the nearest
/// whole number, so a zoom of 0.29 reads "29%". The control is rendered on the STA thread in a host smaller than the
/// plan in both directions, so the centred offsets are real scroll positions and not clamped at an edge.
/// </summary>
public sealed class PlanViewerNavigationCentersOperatorTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>
    /// The chosen operator sits three levels deep on the third of four branches, so at the host's size its centre lies inside
    /// the scrollable range on both axes and the corner and the centre give different offsets.
    /// </summary>
    private const int TargetNodeId = 22;

    [Fact]
    public void GoingToAnOperator_CentresTheOperator_NotItsCorner()
    {
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control);
                await control.LoadPlan(WidePlan(), "Stored Plan");
                LayOut(host);

                Assert.True(NavigateTo(control, TargetNodeId), "the plan has no operator with NodeId " + TargetNodeId);
                LayOut(host);

                var scroller = (ScrollViewer)control.FindName("PlanScrollViewer");
                var node = OperatorNode(control, TargetNodeId);
                var expected = CentredOffset(scroller, node, zoom: 1.0, out var problems);
                Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));

                Assert.InRange(scroller.HorizontalOffset, expected.X - 1, expected.X + 1);
                Assert.InRange(scroller.VerticalOffset, expected.Y - 1, expected.Y + 1);
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void GoingToAnOperator_AfterZoomingOut_CentresTheOperatorAtThatZoom()
    {
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control);
                await control.LoadPlan(WidePlan(), "Stored Plan");
                LayOut(host);

                Click(ZoomButton(control, "−"));
                LayOut(host);
                var zoom = ((ScaleTransform)control.FindName("ZoomTransform")).ScaleX;
                Assert.InRange(zoom, 0.5, 0.99);

                Assert.True(NavigateTo(control, TargetNodeId), "the plan has no operator with NodeId " + TargetNodeId);
                LayOut(host);

                var scroller = (ScrollViewer)control.FindName("PlanScrollViewer");
                var node = OperatorNode(control, TargetNodeId);
                var expected = CentredOffset(scroller, node, zoom, out var problems);
                Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));

                Assert.InRange(scroller.HorizontalOffset, expected.X - 1, expected.X + 1);
                Assert.InRange(scroller.VerticalOffset, expected.Y - 1, expected.Y + 1);
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Theory]
    [InlineData(0.29, "29%")]
    [InlineData(0.2975, "30%")]
    [InlineData(1.0, "100%")]
    public void TheZoomPercentage_IsTheNearestWholeNumber(double zoom, string text)
    {
        StaTestThread.Run(() =>
        {
            var control = NewControl();
            try
            {
                var setZoom = typeof(PlanViewerControl).GetMethod("SetZoom", Instance);
                Assert.True(setZoom is not null, "PlanViewerControl.SetZoom no longer exists under that name - this test's reflection anchor moved.");
                setZoom!.Invoke(control, new object[] { zoom });

                Assert.Equal(text, ((TextBlock)control.FindName("ZoomLevelText")).Text);
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void TheZoomPercentage_AfterFit_IsTheNearestWholeNumberOfTheScaleShown()
    {
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control);
                await control.LoadPlan(WidePlan(), "Stored Plan");
                LayOut(host);

                Click(ZoomButton(control, "Fit"));
                LayOut(host);

                var scale = ((ScaleTransform)control.FindName("ZoomTransform")).ScaleX;
                Assert.InRange(scale, 0.1, 0.99);
                Assert.Equal($"{Math.Round(scale * 100, MidpointRounding.AwayFromZero)}%", ((TextBlock)control.FindName("ZoomLevelText")).Text);
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    /// <summary>
    /// Where the scroller sits when <paramref name="node"/>'s centre is in the middle of the viewport, clamped to the
    /// scrollable range as the control clamps. Reports a problem when the host leaves the centred offset at an edge on either
    /// axis, because then the corner and the centre would scroll to the same place and the test would not tell them apart.
    /// </summary>
    private static Point CentredOffset(ScrollViewer scroller, PlanNode node, double zoom, out List<string> problems)
    {
        problems = new List<string>();
        var maxX = Math.Max(0, scroller.ExtentWidth - scroller.ViewportWidth);
        var maxY = Math.Max(0, scroller.ExtentHeight - scroller.ViewportHeight);
        var centreX = (node.X + PlanLayoutEngine.NodeWidth / 2) * zoom - scroller.ViewportWidth / 2;
        var centreY = (node.Y + PlanLayoutEngine.GetNodeHeight(node) / 2) * zoom - scroller.ViewportHeight / 2;
        if (centreX <= 0 || centreX >= maxX)
            problems.Add($"the centred horizontal offset {centreX:F1} is not inside (0, {maxX:F1}); viewport {scroller.ViewportWidth:F1}, extent {scroller.ExtentWidth:F1}, node X {node.X:F1}");
        if (centreY <= 0 || centreY >= maxY)
            problems.Add($"the centred vertical offset {centreY:F1} is not inside (0, {maxY:F1}); viewport {scroller.ViewportHeight:F1}, extent {scroller.ExtentHeight:F1}, node Y {node.Y:F1}");
        return new Point(Math.Clamp(centreX, 0, maxX), Math.Clamp(centreY, 0, maxY));
    }

    /// <summary>The warning link's path: select the operator and scroll it into view, deferred to the Loaded pass.</summary>
    private static bool NavigateTo(PlanViewerControl control, int nodeId)
    {
        var navigate = typeof(PlanViewerControl).GetMethod("TryNavigateToNode", Instance);
        Assert.True(navigate is not null, "PlanViewerControl.TryNavigateToNode no longer exists under that name - this test's reflection anchor moved.");
        return (bool)navigate!.Invoke(control, new object[] { nodeId })!;
    }

    private static PlanNode OperatorNode(PlanViewerControl control, int nodeId) =>
        ((Canvas)control.FindName("PlanCanvas")).Children.OfType<Border>().Select(b => b.Tag).OfType<PlanNode>().Single(n => n.NodeId == nodeId);

    private static Button ZoomButton(PlanViewerControl control, string content) =>
        Descendants(control).OfType<Button>().Single(b => b.Content as string == content);

    private static void Click(Button button) => button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var grandchild in Descendants(child))
                yield return grandchild;
        }
    }

    /// <summary>The CI screen is 1024x768, so the size comes from an explicitly sized host, not from a Window.</summary>
    private static Border Host(PlanViewerControl control)
    {
        var host = new Border { Width = 900, Height = 300, Child = control };
        LayOut(host);
        return host;
    }

    private static void LayOut(Border host)
    {
        host.Measure(new Size(host.Width, host.Height));
        host.Arrange(new Rect(0, 0, host.Width, host.Height));
        host.UpdateLayout();
        PumpDispatcher();
        host.Measure(new Size(host.Width, host.Height));
        host.Arrange(new Rect(0, 0, host.Width, host.Height));
        host.UpdateLayout();
    }

    private static void PumpDispatcher()
    {
        var frame = new System.Windows.Threading.DispatcherFrame();
        System.Windows.Threading.Dispatcher.CurrentDispatcher.BeginInvoke(System.Windows.Threading.DispatcherPriority.ContextIdle, new Action(() => frame.Continue = false));
        System.Windows.Threading.Dispatcher.PushFrame(frame);
    }

    /// <summary>
    /// One statement whose root fans out into four branches: a six-deep chain, a leaf, a four-deep chain holding the target,
    /// and a leaf. The chain makes the plan wider than the host and the four leaves make it taller, so the target's centre is
    /// a real scroll position on both axes.
    /// </summary>
    private static string WidePlan()
    {
        var deep = ComputeScalar(10, ComputeScalar(11, ComputeScalar(12, ComputeScalar(13, ComputeScalar(14, Scan(15))))));
        var target = ComputeScalar(20, ComputeScalar(21, ComputeScalar(TargetNodeId, Scan(23))));
        var root = Concat(0, deep, Scan(30), target, Scan(40));
        return $"<ShowPlanXML xmlns=\"{Ns}\" Version=\"1.564\" Build=\"16.0.4000.1\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"SELECT 1 FROM dbo.t\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"0.02\" StatementEstRows=\"4\" QueryHash=\"0x0000000000000001\" QueryPlanHash=\"0x0000000000000001\">"
            + $"<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"100\">{root}</QueryPlan></StmtSimple>"
            + "</Statements></Batch></BatchSequence></ShowPlanXML>";
    }

    private static string Concat(int nodeId, params string[] children) =>
        RelOp(nodeId, "Concatenation", "4", "0.02", $"<Concat><DefinedValues/>{string.Concat(children)}</Concat>");

    private static string ComputeScalar(int nodeId, string child) =>
        RelOp(nodeId, "Compute Scalar", "1", "0.0033", $"<ComputeScalar><DefinedValues/>{child}</ComputeScalar>");

    private static string Scan(int nodeId) =>
        RelOp(nodeId, "Table Scan", "1", "0.0032",
            $"<TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t{nodeId}]\" IndexKind=\"Heap\" Storage=\"RowStore\"/></TableScan>");

    private static string RelOp(int nodeId, string op, string rows, string cost, string operatorElement) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"{op}\" LogicalOp=\"{op}\" EstimateRows=\"{rows}\" EstimateIO=\"0\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"{cost}\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\"><OutputList/>{operatorElement}</RelOp>";

    /// <summary>A plan viewer carrying the Viewer's Dark theme in its own resources, so its brushes resolve without
    /// touching the application-scope theme.</summary>
    private static PlanViewerControl NewControl()
    {
        var control = new PlanViewerControl();
        control.Resources.MergedDictionaries.Add(new ResourceDictionary
        {
            Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
        });
        return control;
    }
}
