/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Threading;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8566: the plan viewer remembers the width its Properties panel is dragged to, through a value-changed hook on the panel's grid
/// column. A <c>DependencyPropertyDescriptor</c> keeps every element it watches, and the handler, in a table that lives as long as
/// the process, so a hook that is never removed keeps a closed viewer, its canvas and its plan reachable from that table. These
/// tests show that a viewer closed with its plan tab, or with its window, is collectable, and that the width is still remembered in
/// a viewer whose plan tab was switched away from and selected again. The control is shared by Lite and the Viewer.
/// </summary>
public sealed class PlanViewerPropertiesWidthWatchLifetimeTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    [Fact]
    public void APlanViewer_ClosedWithItsPlanTab_IsCollectable()
    {
        var alive = false;
        StaTestThread.Run(async () =>
        {
            var tabs = new TabControl();
            tabs.Items.Add(new TabItem { Header = "other", Content = new TextBlock { Text = "other" } });
            var window = ViewerPageLayoutProbe.Open(tabs, 1000, 700);
            try
            {
                var closed = await ShowAPlanThenCloseItsTab(tabs, window);
                Collect();
                alive = closed.TryGetTarget(out _);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.False(alive, "a plan viewer is still reachable after its plan tab was closed and dropped: something that outlives the viewer holds it, and through it the plan it drew");
    }

    [Fact]
    public void APlanViewer_WhoseWindowHasClosed_IsCollectable()
    {
        var alive = StaTestThread.Run(() =>
        {
            var closed = ShowThenCloseTheWindow();
            Collect();
            return closed.TryGetTarget(out _);
        });

        Assert.False(alive, "a plan viewer is still reachable after its window closed and was dropped: something that outlives the viewer holds it");
    }

    [Fact]
    public void APropertiesWidthSetByTheSplitter_IsRemembered_InAViewerWhosePlanTabWasSelectedAgain()
    {
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            var column = (ColumnDefinition)control.FindName("PropertiesColumn");
            var panel = (UIElement)control.FindName("PropertiesPanel");
            var tabs = new TabControl();
            var own = new TabItem { Header = "plan", Content = control };
            var other = new TabItem { Header = "other", Content = new TextBlock { Text = "other" } };
            tabs.Items.Add(own);
            tabs.Items.Add(other);
            tabs.SelectedItem = own;
            var window = ViewerPageLayoutProbe.Open(tabs, 1000, 700);
            double? opened = null;
            try
            {
                await control.LoadPlan(Plan(Statement(1, Scan(0))), "Stored Plan");
                ViewerPageLayoutProbe.Settle(window);
                ClickFirstOperator(control);
                ViewerPageLayoutProbe.Settle(window);
                Assert.Equal(Visibility.Visible, panel.Visibility);
                opened = column.Width.Value;
                /* A width inside the panel's 280 to 800 px range, and not the one it opened at. */
                var dragged = opened.Value >= 500 ? opened.Value - 60 : opened.Value + 60;

                tabs.SelectedItem = other;
                ViewerPageLayoutProbe.Settle(window);
                Assert.False(control.IsLoaded, "switching to the other tab did not unload the plan viewer, so this does not exercise Unloaded");
                tabs.SelectedItem = own;
                ViewerPageLayoutProbe.Settle(window);
                Assert.True(control.IsLoaded, "the plan viewer is not loaded after its tab was selected again");

                /* The splitter writes the width it leaves the column at straight onto the column. */
                column.Width = new GridLength(dragged);
                ViewerPageLayoutProbe.Settle(window);
                CloseProperties(control);
                ViewerPageLayoutProbe.Settle(window);
                Assert.Equal(Visibility.Collapsed, panel.Visibility);

                ClickFirstOperator(control);
                ViewerPageLayoutProbe.Settle(window);
                Assert.Equal(Visibility.Visible, panel.Visibility);
                Assert.True(
                    column.Width.Value == dragged,
                    $"the Properties panel reopened {column.Width.Value} px wide, not the {dragged} px its splitter left it at after the plan tab was selected again (it first opened at {opened} px): the viewer no longer remembers a dragged width");
            }
            finally
            {
                /* The remembered width is shared by every viewer in the process, so the width the panel first opened at is put back. */
                if (opened is { } width && panel.Visibility == Visibility.Visible)
                {
                    column.Width = new GridLength(width);
                }

                control.Cleanup();
                window.Close();
            }
        });
    }

    /// <summary>
    /// Shows a viewer in a plan tab, loads a plan into it, opens the Properties panel on an operator, and closes the tab as the plan
    /// tab's close button does. In its own method, so no local of the test keeps the viewer alive through the collection.
    /// </summary>
    private static async Task<WeakReference<PlanViewerControl>> ShowAPlanThenCloseItsTab(TabControl tabs, Window window)
    {
        var control = NewControl();
        var item = new TabItem { Header = "plan", Content = control };
        tabs.Items.Add(item);
        tabs.SelectedItem = item;
        ViewerPageLayoutProbe.Settle(window);
        if (!control.IsLoaded)
        {
            throw new Xunit.Sdk.XunitException("the selected plan viewer is not loaded, so this does not close a viewer that was shown");
        }

        await control.LoadPlan(Plan(Statement(1, Scan(0))), "Stored Plan");
        ViewerPageLayoutProbe.Settle(window);
        ClickFirstOperator(control);
        ViewerPageLayoutProbe.Settle(window);

        control.Cleanup();
        tabs.Items.Remove(item);
        ViewerPageLayoutProbe.Settle(window);
        return new WeakReference<PlanViewerControl>(control);
    }

    /// <summary>Shows a viewer in a window of its own and closes the window, which cleans the viewer up from its Closed event as a plan window does.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference<PlanViewerControl> ShowThenCloseTheWindow()
    {
        var control = NewControl();
        var window = ViewerPageLayoutProbe.Open(control, 1000, 700);
        if (!control.IsLoaded)
        {
            throw new Xunit.Sdk.XunitException("the plan viewer is not loaded in its window, so this does not close a viewer that was shown");
        }

        window.Closed += (_, _) => control.Cleanup();
        window.Close();
        return new WeakReference<PlanViewerControl>(control);
    }

    private static void Collect()
    {
        Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    /// <summary>The rendered operators: the canvas children that carry a plan node.</summary>
    private static Border[] OperatorBorders(PlanViewerControl control) =>
        ((Canvas)control.FindName("PlanCanvas")).Children.OfType<Border>().Where(b => b.Tag is PlanNode).ToArray();

    /// <summary>Releases the left button over the first rendered operator, the click that selects it.</summary>
    private static void ClickFirstOperator(PlanViewerControl control)
    {
        var border = OperatorBorders(control).First();
        border.RaiseEvent(new MouseButtonEventArgs(Mouse.PrimaryDevice, 0, MouseButton.Left) { RoutedEvent = UIElement.MouseLeftButtonUpEvent });
    }

    /// <summary>Clicks the Properties panel's own close button.</summary>
    private static void CloseProperties(PlanViewerControl control)
    {
        var close = ClippedTextProbe.Descendants((DependencyObject)control.FindName("PropertiesPanel")).OfType<Button>()
            .FirstOrDefault(b => b.ToolTip as string == "Close Properties")
            ?? throw new Xunit.Sdk.XunitException("the Properties panel has no Close Properties button");
        close.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
    }

    private static string Plan(params string[] statements) =>
        $"<ShowPlanXML xmlns=\"{Ns}\" Version=\"1.564\" Build=\"16.0.4000.1\"><BatchSequence><Batch><Statements>{string.Concat(statements)}</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string Statement(int id, string rootOperator) =>
        $"<StmtSimple StatementText=\"SELECT {id} FROM dbo.t{id}\" StatementId=\"{id}\" StatementCompId=\"{id}\" StatementType=\"SELECT\" StatementSubTreeCost=\"0.0032\" StatementEstRows=\"1\" QueryHash=\"0x{id:X16}\" QueryPlanHash=\"0x{id:X16}\">"
        + $"<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"100\">{rootOperator}</QueryPlan></StmtSimple>";

    private static string Scan(int nodeId) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"1\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.0032\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + $"<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t{nodeId}]\" IndexKind=\"Heap\" Storage=\"RowStore\"/></TableScan></RelOp>";

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
