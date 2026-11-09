/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8446: a plan pasted into the plan viewer loads into the control that already shows one. The previous plan's
/// Statements panel and the Properties panel of its selected operator belong to that plan, so they go when the
/// next plan loads, whatever that plan turns out to be: a single statement, several statements, or text that does
/// not parse. The control is rendered on the STA thread and driven through <see cref="PlanViewerControl.LoadPlan"/>
/// and the operator's own click, and the panels are read as laid out.
/// </summary>
public sealed class PlanViewerReloadResetsPanelsTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    [Fact]
    public void ASingleStatementPlan_LoadedOverAMultiStatementPlanWithAnOperatorSelected_ClosesBothPanels()
    {
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control);
                StaTestThread.Step("LoadPlan: two statements, so the Statements panel opens");
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0))), "Stored Plan");
                LayOut(host);
                Assert.Equal(Visibility.Visible, Panel(control, "StatementsPanel").Visibility);
                Assert.Equal(2, ((DataGrid)control.FindName("StatementsGrid")).Items.Count);

                StaTestThread.Step("clicking the rendered operator, so the Properties panel opens");
                ClickFirstOperator(control);
                LayOut(host);
                Assert.Equal(Visibility.Visible, Panel(control, "PropertiesPanel").Visibility);

                StaTestThread.Step("LoadPlan: one statement, over the plan on screen");
                await control.LoadPlan(Plan(Statement(1, Scan(0))), "Pasted Plan");
                LayOut(host);

                Assert.Equal(Visibility.Collapsed, Panel(control, "StatementsPanel").Visibility);
                Assert.Equal(0, ((ColumnDefinition)control.FindName("StatementsColumn")).Width.Value);
                Assert.Equal(Visibility.Collapsed, Panel(control, "PropertiesPanel").Visibility);
                Assert.Equal(0, ((ColumnDefinition)control.FindName("PropertiesColumn")).Width.Value);
                Assert.Equal(Visibility.Visible, Panel(control, "PlanScrollViewer").Visibility);
                Assert.Single(OperatorBorders(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void AMultiStatementPlan_LoadedOverAnotherWithAnOperatorSelected_ListsItsOwnStatements_AndClosesTheProperties()
    {
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control);
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0))), "Stored Plan");
                LayOut(host);
                ClickFirstOperator(control);
                LayOut(host);
                Assert.Equal(Visibility.Visible, Panel(control, "PropertiesPanel").Visibility);

                StaTestThread.Step("LoadPlan: three statements, over the two on screen");
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0)), Statement(3, Scan(0))), "Pasted Plan");
                LayOut(host);

                Assert.Equal(Visibility.Visible, Panel(control, "StatementsPanel").Visibility);
                Assert.Equal(3, ((DataGrid)control.FindName("StatementsGrid")).Items.Count);
                Assert.Equal(Visibility.Collapsed, Panel(control, "PropertiesPanel").Visibility);
                Assert.Equal(0, ((ColumnDefinition)control.FindName("PropertiesColumn")).Width.Value);
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void TextThatIsNotAPlan_LoadedOverAPlanWithAnOperatorSelected_ShowsTheEmptyStateAlone()
    {
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control);
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0))), "Stored Plan");
                LayOut(host);
                ClickFirstOperator(control);
                LayOut(host);
                Assert.Equal(Visibility.Visible, Panel(control, "PropertiesPanel").Visibility);

                StaTestThread.Step("LoadPlan: text that does not parse, over the plan on screen");
                await control.LoadPlan("not a plan", "Pasted Plan");
                LayOut(host);

                Assert.Equal(Visibility.Visible, Panel(control, "EmptyState").Visibility);
                Assert.Equal(Visibility.Collapsed, Panel(control, "PlanScrollViewer").Visibility);
                Assert.Equal(Visibility.Collapsed, Panel(control, "StatementsPanel").Visibility);
                Assert.Equal(Visibility.Collapsed, Panel(control, "PropertiesPanel").Visibility);
                Assert.Empty(OperatorBorders(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    /// <summary>The CI screen is 1024x768, so the size comes from an explicitly sized host, not from a Window.</summary>
    private static Border Host(PlanViewerControl control)
    {
        var host = new Border { Width = 1000, Height = 700, Child = control };
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

    private static UIElement Panel(PlanViewerControl control, string name) => (UIElement)control.FindName(name);

    /// <summary>The rendered operators: the canvas children that carry a plan node.</summary>
    private static Border[] OperatorBorders(PlanViewerControl control) =>
        ((Canvas)control.FindName("PlanCanvas")).Children.OfType<Border>().Where(b => b.Tag is PlanNode).ToArray();

    /// <summary>Releases the left button over the first rendered operator, the click that selects it.</summary>
    private static void ClickFirstOperator(PlanViewerControl control)
    {
        var border = OperatorBorders(control).First();
        border.RaiseEvent(new MouseButtonEventArgs(Mouse.PrimaryDevice, 0, MouseButton.Left) { RoutedEvent = UIElement.MouseLeftButtonUpEvent });
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
