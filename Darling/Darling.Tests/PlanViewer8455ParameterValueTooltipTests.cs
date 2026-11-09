/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8455: the plan viewer's Parameters card lets a value be read in full. A Compiled or Runtime cell the column
/// trims carries its whole value as its tooltip, whatever its length; a sniffed runtime cell's tooltip gives the
/// value first and the sniffing note after it; a cell shown whole, and not sniffed, carries no tooltip.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class PlanViewer8455ParameterValueTooltipTests
{
    private const string SniffedNote = "Runtime value differs from compiled: possible parameter sniffing";
    private const string CompiledDate = "'2024-01-15 00:00:00.000'";
    private const string RuntimeDate = "'2024-01-16 00:00:00.000'";
    private const int CompiledColumn = 2;
    private const int RuntimeColumn = 3;

    [Fact]
    public void TrimmedValueCells_CarryTheirFullValue_AndASniffedCellLeadsWithItsValue()
    {
        StaTestThread.Run(async () =>
        {
            // The node visuals read the theme's brushes, so the Viewer's theme is merged for the body and removed after it.
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("BackgroundBrush"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }

            StaTestThread.Step("new PlanViewerControl()");
            var control = new PlanViewerControl();
            var host = new Border { Width = 1024, Height = 700, Child = control };
            try
            {
                StaTestThread.Step("LoadPlan: four parameters, two of them with 25-character datetime values");
                await control.LoadPlan(DateParameterPlan(), "plan");

                StaTestThread.Step("layout at 1024 x 700");
                Layout(host);

                var grid = ((StackPanel)control.FindName("ParametersContent")).Children.OfType<Grid>().Single();

                StaTestThread.Step("row 1: @StartDate, compiled and runtime differ, both cut by the column");
                var sniffedCompiled = Cell(grid, 1, CompiledColumn);
                var sniffedRuntime = Cell(grid, 1, RuntimeColumn);
                Assert.True(IsTrimmed(sniffedCompiled), $"the compiled date shows whole at {sniffedCompiled.ActualWidth:F0} px, so this layout does not cut it");
                Assert.True(IsTrimmed(sniffedRuntime), $"the runtime date shows whole at {sniffedRuntime.ActualWidth:F0} px, so this layout does not cut it");
                Assert.Equal(CompiledDate, sniffedCompiled.ToolTip as string);
                Assert.Equal(RuntimeDate + Environment.NewLine + SniffedNote, sniffedRuntime.ToolTip as string);

                StaTestThread.Step("row 2: @EndDate, the same 25-character value both ways, cut but not sniffed");
                var sameCompiled = Cell(grid, 2, CompiledColumn);
                var sameRuntime = Cell(grid, 2, RuntimeColumn);
                Assert.True(IsTrimmed(sameCompiled), $"the compiled date shows whole at {sameCompiled.ActualWidth:F0} px, so this layout does not cut it");
                Assert.Equal(CompiledDate, sameCompiled.ToolTip as string);
                Assert.Equal(CompiledDate, sameRuntime.ToolTip as string);

                StaTestThread.Step("row 3: @n, (5) both ways, shown whole and not sniffed");
                var shortCompiled = Cell(grid, 3, CompiledColumn);
                var shortRuntime = Cell(grid, 3, RuntimeColumn);
                Assert.False(IsTrimmed(shortCompiled), $"(5) is cut at {shortCompiled.ActualWidth:F0} px, so this layout is narrower than the test expects");
                Assert.Null(shortCompiled.ToolTip);
                Assert.Null(shortRuntime.ToolTip);

                StaTestThread.Step("row 4: @k, (1) compiled and (2) at runtime, shown whole and sniffed");
                var shortSniffed = Cell(grid, 4, RuntimeColumn);
                Assert.False(IsTrimmed(shortSniffed), $"(2) is cut at {shortSniffed.ActualWidth:F0} px, so this layout is narrower than the test expects");
                Assert.Equal("(2)" + Environment.NewLine + SniffedNote, shortSniffed.ToolTip as string);
            }
            finally
            {
                StaTestThread.Step("Cleanup()");
                control.Cleanup();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });
    }

    /// <summary>
    /// A parameter name wide enough to squeeze the value columns: each keeps 40 px, so each cut cell is a 30 px hover
    /// target (the column less the cell's 10 px right margin) that carries the whole value.
    /// </summary>
    [Fact]
    public void ALongParameterName_LeavesEachValueCellAHoverTarget()
    {
        StaTestThread.Run(async () =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("BackgroundBrush"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }

            StaTestThread.Step("new PlanViewerControl()");
            var control = new PlanViewerControl();
            var host = new Border { Width = 1024, Height = 700, Child = control };
            try
            {
                StaTestThread.Step("LoadPlan: one parameter with a 37-character name and a 25-character value");
                await control.LoadPlan(LongNameParameterPlan(), "plan");

                StaTestThread.Step("layout at 1024 x 700");
                Layout(host);

                var grid = ((StackPanel)control.FindName("ParametersContent")).Children.OfType<Grid>().Single();
                Assert.True(grid.ColumnDefinitions[CompiledColumn].ActualWidth >= 40 - 0.01, $"the compiled column is {grid.ColumnDefinitions[CompiledColumn].ActualWidth:F1} px wide");
                Assert.True(grid.ColumnDefinitions[RuntimeColumn].ActualWidth >= 40 - 0.01, $"the runtime column is {grid.ColumnDefinitions[RuntimeColumn].ActualWidth:F1} px wide");

                foreach (var column in new[] { CompiledColumn, RuntimeColumn })
                {
                    var cell = Cell(grid, 1, column);
                    Assert.True(cell.ActualWidth >= 30 - 0.01, $"the value cell in column {column} is {cell.ActualWidth:F1} px wide, so it has no hover target");
                    Assert.True(IsTrimmed(cell), $"the value shows whole at {cell.ActualWidth:F0} px, so this layout does not cut it");
                    Assert.Equal(CompiledDate, cell.ToolTip as string);
                }
            }
            finally
            {
                StaTestThread.Step("Cleanup()");
                control.Cleanup();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });
    }

    private static void Layout(Border host)
    {
        host.Measure(new Size(host.Width, host.Height));
        host.Arrange(new Rect(0, 0, host.Width, host.Height));
        host.UpdateLayout();
    }

    private static TextBlock Cell(Grid grid, int row, int column) =>
        grid.Children.OfType<TextBlock>().Single(t => Grid.GetRow(t) == row && Grid.GetColumn(t) == column);

    /// <summary>Whether the cell's text is wider than the width its column gave it, so an ellipsis cuts it: the control's
    /// own rule, a floating-point epsilon over the cell's width.</summary>
    private static bool IsTrimmed(TextBlock cell)
    {
        var natural = new FormattedText(
            cell.Text, CultureInfo.CurrentUICulture, cell.FlowDirection,
            new Typeface(cell.FontFamily, cell.FontStyle, cell.FontWeight, cell.FontStretch),
            cell.FontSize, Brushes.Black, VisualTreeHelper.GetDpi(cell).PixelsPerDip).WidthIncludingTrailingWhitespace;
        return natural > cell.ActualWidth + 0.01;
    }

    /// <summary>A single-statement actual plan with one datetime parameter whose name is 37 characters and whose
    /// compiled and runtime values are the same 25 characters.</summary>
    private static string LongNameParameterPlan() =>
        Plan("SELECT * FROM dbo.t WHERE d &gt;= @CustomerSegmentEffectiveStartDateUtc",
            $"<ColumnReference Column=\"@CustomerSegmentEffectiveStartDateUtc\" ParameterDataType=\"datetime\" ParameterCompiledValue=\"{CompiledDate}\" ParameterRuntimeValue=\"{CompiledDate}\" />");

    /// <summary>A single-statement actual plan with four parameters: two datetimes whose values are 25 characters
    /// (one sniffed, one not) and two ints whose values are three characters (one sniffed, one not).</summary>
    private static string DateParameterPlan() =>
        Plan("SELECT * FROM dbo.t WHERE d &gt;= @StartDate AND d &lt; @EndDate AND n = @n AND k = @k",
            $"<ColumnReference Column=\"@StartDate\" ParameterDataType=\"datetime\" ParameterCompiledValue=\"{CompiledDate}\" ParameterRuntimeValue=\"{RuntimeDate}\" />" +
            $"<ColumnReference Column=\"@EndDate\" ParameterDataType=\"datetime\" ParameterCompiledValue=\"{CompiledDate}\" ParameterRuntimeValue=\"{CompiledDate}\" />" +
            "<ColumnReference Column=\"@n\" ParameterDataType=\"int\" ParameterCompiledValue=\"(5)\" ParameterRuntimeValue=\"(5)\" />" +
            "<ColumnReference Column=\"@k\" ParameterDataType=\"int\" ParameterCompiledValue=\"(1)\" ParameterRuntimeValue=\"(2)\" />");

    private static string Plan(string statementText, string parameterList) =>
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        $"<StmtSimple StatementText=\"{statementText}\" StatementId=\"1\" StatementType=\"SELECT\" " +
        "StatementSubTreeCost=\"1\" StatementEstRows=\"10\" StatementOptmLevel=\"FULL\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" DegreeOfParallelism=\"1\">" +
        "<QueryTimeStats CpuTime=\"10\" ElapsedTime=\"10\" />" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Seek\" LogicalOp=\"Clustered Index Seek\" " +
        "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"1\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList />" +
        "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"10\" ActualElapsedms=\"10\" ActualCPUms=\"10\" ActualLogicalReads=\"10\" /></RunTimeInformation>" +
        "<IndexScan Ordered=\"1\" ForcedIndex=\"0\" ForceSeek=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
        "</IndexScan>" +
        "</RelOp>" +
        "<ParameterList>" + parameterList + "</ParameterList>" +
        "</QueryPlan></StmtSimple>" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";
}
