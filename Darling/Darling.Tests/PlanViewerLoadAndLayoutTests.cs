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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8538: what the plan viewer shows as a plan loads and as its panels share a small window. A plan that loads over a zoomed one is
/// drawn at full size. The toolbar buttons that act on a drawn plan are enabled only while one is drawn, so text that does not parse
/// leaves nothing to save, fit or map. With the Statements and Properties panels both open in a 900x600 viewer the canvas keeps room
/// for an operator. In Plan Insights, a sentence longer than its card wraps, and within a width that keeps a card with nothing else
/// to show near its minimum, so it leaves the strip to the cards after it; and the Parameters value columns keep their headers
/// whatever the parameter's name takes.
/// The control is rendered on the STA thread, driven through <see cref="PlanViewerControl.LoadPlan"/> and its own buttons, and read
/// as laid out.
/// </summary>
public sealed class PlanViewerLoadAndLayoutTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    /// <summary>A parameter name of 37 characters, wider than half of a Parameters card.</summary>
    private const string LongParameterName = "@TheThirtySevenCharacterParameterName";

    /// <summary>The narrowest canvas that shows an operator whole: its 150 px box after the layout's 40 px padding, and a scroll bar.</summary>
    private const double CanvasMinimum = 240;

    private const double StatementsMinimum = 200;

    private const double PropertiesMinimum = PropertyRows.MinPropertiesWidth;

    /// <summary>The widest a card is with one wrapped sentence to show: under the cards' 360 px cap by 100 px.</summary>
    private const double SentenceCardMaximum = 260;

    [Fact]
    public void APlanLoadedOverAZoomedPlan_IsDrawnAtFullSize()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 1000, 700);
                StaTestThread.Step("LoadPlan: a chain of operators wider than the viewer");
                await control.LoadPlan(Plan(Statement(1, Chain(12))), "Stored Plan");
                LayOut(host);
                Assert.True(OperatorBorders(control).Length >= 13, $"the chain drew {OperatorBorders(control).Length} operators");

                StaTestThread.Step("Fit, so the wide plan is zoomed out");
                Click(ToolbarButton(control, "Fit"));
                LayOut(host);
                var fitted = ZoomText(control);
                Assert.True(Zoom(control).ScaleX < 0.99, $"Fit left the operator chain at {fitted} in a 1000 px viewer; this plan is wider than the viewer, so Fit zooms it out");

                StaTestThread.Step("LoadPlan: one operator, over the zoomed plan");
                await control.LoadPlan(Plan(Statement(1, Scan(7))), "Pasted Plan");
                LayOut(host);

                var found = new List<string>();
                if (ZoomText(control) != "100%")
                    found.Add($"the zoom reads {ZoomText(control)} after a plan loaded over one at {fitted}: a plan that loads is drawn at 100%");
                var zoom = Zoom(control);
                if (zoom.ScaleX != 1 || zoom.ScaleY != 1)
                    found.Add($"the canvas is scaled {zoom.ScaleX:F2} x {zoom.ScaleY:F2} after a plan loaded over one at {fitted}");
                var scroller = (ScrollViewer)control.FindName("PlanScrollViewer");
                var scan = OperatorBorders(control).First(b => ((PlanNode)b.Tag).NodeId == 7);
                var drawn = scan.TransformToAncestor(scroller).TransformBounds(new Rect(scan.RenderSize));
                if (Math.Abs(drawn.Width - PlanLayoutEngine.NodeWidth) > 0.5)
                    found.Add($"the pasted plan's operator is drawn {drawn.Width:F0} px wide; at 100% an operator is {PlanLayoutEngine.NodeWidth} px wide");
                problems.AddRange(found);
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheButtonsThatActOnAPlan_AreEnabledOnlyWhileOneIsDrawn()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 1000, 700);
                var found = new List<string>();

                found.AddRange(ButtonsNotIn(control, enabled: false, "before any plan loads"));

                StaTestThread.Step("LoadPlan: a plan that parses");
                await control.LoadPlan(Plan(Statement(1, Scan(0))), "Stored Plan");
                LayOut(host);
                Assert.NotEmpty(OperatorBorders(control));
                found.AddRange(ButtonsNotIn(control, enabled: true, "with a plan drawn"));

                StaTestThread.Step("LoadPlan: text that does not parse, over the plan on screen");
                await control.LoadPlan("not a plan", "Pasted Plan");
                LayOut(host);
                Assert.Equal(Visibility.Visible, ((UIElement)control.FindName("EmptyState")).Visibility);
                Assert.StartsWith("This plan couldn't be parsed", ((TextBlock)control.FindName("EmptyStateTitle")).Text, StringComparison.Ordinal);
                found.AddRange(ButtonsNotIn(control, enabled: false, "after text that does not parse"));

                StaTestThread.Step("LoadPlan: a plan that parses, over the failed one");
                await control.LoadPlan(Plan(Statement(1, Scan(3))), "Pasted Plan");
                LayOut(host);
                found.AddRange(ButtonsNotIn(control, enabled: true, "with a plan drawn after a failed one"));
                problems.AddRange(found);
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void WithTheStatementsAndPropertiesPanelsOpen_A900By600Viewer_KeepsRoomForAnOperator()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 900, 600);
                StaTestThread.Step("LoadPlan: two statements, so the Statements panel opens");
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0))), "Stored Plan");
                LayOut(host);
                StaTestThread.Step("clicking the rendered operator, so the Properties panel opens");
                ClickFirstOperator(control);
                LayOut(host);

                var statements = (FrameworkElement)control.FindName("StatementsPanel");
                var properties = (FrameworkElement)control.FindName("PropertiesPanel");
                var scroller = (ScrollViewer)control.FindName("PlanScrollViewer");
                Assert.Equal(Visibility.Visible, statements.Visibility);
                Assert.Equal(Visibility.Visible, properties.Visibility);

                var found = new List<string>();
                if (scroller.ActualWidth < CanvasMinimum - 0.5)
                    found.Add($"the plan canvas is {scroller.ActualWidth:F0} px wide in a 900 px viewer with the Statements panel ({statements.ActualWidth:F0} px) and the Properties panel ({properties.ActualWidth:F0} px) open: it keeps {CanvasMinimum} px");
                var first = OperatorBorders(control).First();
                var drawn = first.TransformToAncestor(scroller).TransformBounds(new Rect(first.RenderSize));
                if (drawn.Left < -0.5 || drawn.Right > scroller.ActualWidth + 0.5)
                    found.Add($"the first operator spans {drawn.Left:F0} to {drawn.Right:F0} px in a canvas {scroller.ActualWidth:F0} px wide: no operator shows whole");
                if (statements.ActualWidth < StatementsMinimum - 0.5)
                    found.Add($"the Statements panel is {statements.ActualWidth:F0} px wide; it narrows no further than {StatementsMinimum} px");
                if (properties.ActualWidth < PropertiesMinimum - 0.5)
                    found.Add($"the Properties panel is {properties.ActualWidth:F0} px wide; it narrows no further than {PropertiesMinimum} px");
                var propertiesRight = properties.TransformToAncestor(control).TransformBounds(new Rect(properties.RenderSize)).Right;
                if (propertiesRight > 900 + 0.5)
                    found.Add($"the Properties panel ends at {propertiesRight:F0} px in a 900 px viewer");

                /* The narrowing is the small viewer's alone: with room for all three, the Statements panel is back at its 450 px. */
                StaTestThread.Step("widening the viewer to 1600 px");
                host.Width = 1600;
                LayOut(host);
                if (Math.Abs(statements.ActualWidth - 450) > 0.5)
                    found.Add($"the Statements panel is {statements.ActualWidth:F0} px wide in a 1600 px viewer after the viewer was 900 px wide; with room for the canvas it takes its 450 px");
                if (scroller.ActualWidth < CanvasMinimum - 0.5)
                    found.Add($"the plan canvas is {scroller.ActualWidth:F0} px wide in a 1600 px viewer");
                problems.AddRange(found);
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheServerContextSentence_WrapsInsideItsCard_AndTheCardStaysNarrow()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 1280, 720);
                await control.LoadPlan(Plan(Statement(1, Scan(0))), "Stored Plan");
                LayOut(host);
                Assert.True(((Expander)control.FindName("InsightsExpander")).IsExpanded, "Plan Insights is collapsed in a 720 px tall viewer; the cards are read open");

                var sentence = (TextBlock)control.FindName("ServerContextEmpty");
                Assert.Equal(Visibility.Visible, sentence.Visibility);
                Assert.Equal(ServerContextCard.EmptyText, sentence.Text);
                var card = Ancestors(sentence).OfType<ScrollViewer>().First();

                var found = new List<string>();

                /* The sentence is longer than the card shows. It reads whole when it takes more than one line or ends in an ellipsis;
                   on one line with neither, the card's edge cuts it. */
                var oneLine = OneLine(sentence);
                if (oneLine.Width > card.ViewportWidth + 0.5
                    && sentence.ActualHeight < oneLine.Height * 1.5
                    && sentence.TextTrimming == TextTrimming.None)
                {
                    found.Add($"\"{sentence.Text}\" is {oneLine.Width:F0} px long and is drawn on one line in a card that shows {card.ViewportWidth:F0} px, with no ellipsis: the card's edge cuts it mid-word");
                }

                /* A card with one sentence to show does not take the cards' 360 px cap: the cards after it sit that much further
                   along the strip, and a strip narrower than its cards shows the last of them cut at its own edge. */
                var column = (ColumnDefinition)control.FindName("ServerContextColumn");
                if (column.ActualWidth > SentenceCardMaximum + 0.5)
                    found.Add($"the Server Context card is {column.ActualWidth:F0} px wide for one sentence; it wraps the sentence and takes no more than {SentenceCardMaximum} px, so the cards after it are not pushed {column.ActualWidth - SentenceCardMaximum:F0} px further along the strip");
                problems.AddRange(found);
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void ALongParameterName_LeavesTheValueColumnsTheirHeaders()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 1280, 720);
                await control.LoadPlan(Plan(Statement(1, Scan(0), LongNameParameter())), "Stored Plan");
                LayOut(host);
                Assert.True(((Expander)control.FindName("InsightsExpander")).IsExpanded, "Plan Insights is collapsed in a 720 px tall viewer; the cards are read open");

                var content = (Panel)control.FindName("ParametersContent");
                var grid = content.Children.OfType<Grid>().Single();
                var cells = grid.Children.OfType<TextBlock>().ToList();
                var card = Ancestors(content).OfType<ScrollViewer>().First();
                var found = new List<string>();

                /* The grid is as wide as the card shows: a column past the card's edge is cut there with no ellipsis. */
                if (grid.ActualWidth > card.ViewportWidth + 0.5)
                    found.Add($"the Parameters grid is {grid.ActualWidth:F0} px wide in a card that shows {card.ViewportWidth:F0} px: its last column is cut at the card's edge");

                foreach (var header in new[] { "Compiled", "Runtime" })
                {
                    var cell = cells.SingleOrDefault(c => Grid.GetRow(c) == 0 && c.Text == header);
                    if (cell is null)
                    {
                        found.Add($"the Parameters card has no \"{header}\" column for a parameter with a compiled and a runtime value");
                        continue;
                    }

                    var natural = OneLine(cell).Width;
                    if (cell.ActualWidth < natural - 0.5)
                        found.Add($"the \"{header}\" header is drawn {cell.ActualWidth:F0} px wide and needs {natural:F0} px: its column ({grid.ColumnDefinitions[Grid.GetColumn(cell)].ActualWidth:F0} px) is too narrow for its own header");
                    var right = cell.TransformToAncestor(card).TransformBounds(new Rect(cell.RenderSize)).Right;
                    if (right > card.ViewportWidth + 0.5)
                        found.Add($"the \"{header}\" header ends at {right:F0} px in a card that shows {card.ViewportWidth:F0} px");
                }

                /* The name is what gives: it is cut with an ellipsis, and its tooltip carries it whole. */
                var name = cells.Single(c => c.Text == LongParameterName);
                if (name.ActualWidth > OneLine(name).Width - 0.5)
                    found.Add($"the {LongParameterName.Length}-character parameter name is drawn whole ({name.ActualWidth:F0} px) in a {card.ViewportWidth:F0} px card with two value columns");
                if (name.TextTrimming == TextTrimming.None)
                    found.Add("the parameter name is cut with no ellipsis");
                if (name.ToolTip as string != LongParameterName)
                    found.Add($"the cut parameter name's tooltip is \"{name.ToolTip}\"");
                problems.AddRange(found);
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void NarrowingAWideViewerTo900_WithBothPanelsOpen_FitsThePanelsAgain()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 1600, 600);
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0))), "Stored Plan");
                LayOut(host);
                ClickFirstOperator(control);
                LayOut(host);

                var statements = (FrameworkElement)control.FindName("StatementsPanel");
                var properties = (FrameworkElement)control.FindName("PropertiesPanel");
                var scroller = (ScrollViewer)control.FindName("PlanScrollViewer");
                Assert.Equal(Visibility.Visible, statements.Visibility);
                Assert.Equal(Visibility.Visible, properties.Visibility);
                Assert.True(Math.Abs(statements.ActualWidth - 450) < 0.5, $"the Statements panel is {statements.ActualWidth:F0} px wide in a 1600 px viewer");

                StaTestThread.Step("narrowing the viewer to 900 px");
                host.Width = 900;
                LayOut(host);

                var found = new List<string>();
                var propertiesRight = properties.TransformToAncestor(control).TransformBounds(new Rect(properties.RenderSize)).Right;
                if (propertiesRight > 900 + 0.5)
                    found.Add($"the Properties panel ends at {propertiesRight:F0} px in a viewer narrowed from 1600 px to 900 px: {propertiesRight - 900:F0} px of it is past the viewer's edge");
                if (statements.ActualWidth > 450 - 0.5)
                    found.Add($"the Statements panel is still {statements.ActualWidth:F0} px wide in a viewer narrowed from 1600 px to 900 px: it narrows to make room for the canvas");
                if (statements.ActualWidth < StatementsMinimum - 0.5)
                    found.Add($"the Statements panel is {statements.ActualWidth:F0} px wide; it narrows no further than {StatementsMinimum} px");
                if (properties.ActualWidth < PropertiesMinimum - 0.5)
                    found.Add($"the Properties panel is {properties.ActualWidth:F0} px wide; it narrows no further than {PropertiesMinimum} px");
                if (scroller.ActualWidth < CanvasMinimum - 0.5)
                    found.Add($"the plan canvas is {scroller.ActualWidth:F0} px wide in a viewer narrowed from 1600 px to 900 px: it keeps {CanvasMinimum} px");
                problems.AddRange(found);
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A 600 px viewer is the plan viewer in either app's 900 px minimum window with the sidebar open. It cannot hold
    /// both panels at their minimums and a 240 px canvas, so one panel shows at a time, the one opened last, and the
    /// Statements button brings the list back.</summary>
    [Fact]
    public void InAViewerTooNarrowForBothPanels_ThePanelOpenedLastShows_AndTheCanvasKeepsItsRoom()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 600, 600);
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0))), "Stored Plan");
                LayOut(host);
                var statements = (FrameworkElement)control.FindName("StatementsPanel");
                var properties = (FrameworkElement)control.FindName("PropertiesPanel");
                var scroller = (ScrollViewer)control.FindName("PlanScrollViewer");
                Assert.Equal(Visibility.Visible, statements.Visibility);

                StaTestThread.Step("clicking the rendered operator, so the Properties panel opens");
                ClickFirstOperator(control);
                LayOut(host);
                Assert.Equal(Visibility.Visible, properties.Visibility);

                var found = new List<string>();
                if (statements.Visibility == Visibility.Visible)
                    found.Add($"the Statements panel ({statements.ActualWidth:F0} px) stays open beside the Properties panel ({properties.ActualWidth:F0} px) in a 600 px viewer, which cannot hold both and the canvas");
                if (((UIElement)control.FindName("StatementsButton")).Visibility != Visibility.Visible)
                    found.Add("the Statements button is gone with its panel: nothing reopens the statement list");
                if (scroller.ActualWidth < CanvasMinimum - 0.5)
                    found.Add($"the plan canvas is {scroller.ActualWidth:F0} px wide in a 600 px viewer with an operator selected: it keeps {CanvasMinimum} px");
                var first = OperatorBorders(control).First();
                var drawn = first.TransformToAncestor(scroller).TransformBounds(new Rect(first.RenderSize));
                if (drawn.Left < -0.5 || drawn.Right > scroller.ActualWidth + 0.5)
                    found.Add($"the first operator spans {drawn.Left:F0} to {drawn.Right:F0} px in a canvas {scroller.ActualWidth:F0} px wide: no operator shows whole");
                var propertiesRight = properties.TransformToAncestor(control).TransformBounds(new Rect(properties.RenderSize)).Right;
                if (propertiesRight > 600 + 0.5)
                    found.Add($"the Properties panel ends at {propertiesRight:F0} px in a 600 px viewer");

                StaTestThread.Step("the Statements button, with the Properties panel open");
                Click((Button)control.FindName("StatementsButton"));
                LayOut(host);
                if (statements.Visibility != Visibility.Visible)
                    found.Add("the Statements button did not reopen the statement list");
                if (statements.Visibility == Visibility.Visible && properties.Visibility == Visibility.Visible)
                    found.Add($"the Properties panel ({properties.ActualWidth:F0} px) stays open beside the reopened Statements panel ({statements.ActualWidth:F0} px) in a 600 px viewer");
                if (scroller.ActualWidth < CanvasMinimum - 0.5)
                    found.Add($"the plan canvas is {scroller.ActualWidth:F0} px wide in a 600 px viewer with the statement list reopened: it keeps {CanvasMinimum} px");
                problems.AddRange(found);
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void AClickOnTheStatementsSplitter_IsNotADraggedWidth()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 900, 600);
                await control.LoadPlan(Plan(Statement(1, Scan(0)), Statement(2, Scan(0))), "Stored Plan");
                LayOut(host);
                ClickFirstOperator(control);
                LayOut(host);
                var statements = (FrameworkElement)control.FindName("StatementsPanel");
                Assert.True(statements.ActualWidth < 450 - 0.5, $"the Statements panel is {statements.ActualWidth:F0} px wide beside the Properties panel in a 900 px viewer; this test starts from a narrowed panel");

                StaTestThread.Step("a mouse-down and mouse-up on the Statements splitter with no movement");
                ((GridSplitter)control.FindName("StatementsSplitter")).RaiseEvent(new DragCompletedEventArgs(0, 0, false));
                host.Width = 1600;
                LayOut(host);

                if (Math.Abs(statements.ActualWidth - 450) > 0.5)
                    problems.Add($"the Statements panel is {statements.ActualWidth:F0} px wide in a 1600 px viewer after a click on its splitter at 900 px: a click that moves nothing is not a dragged width, so the panel takes its 450 px");
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A plan over the parser's size limit and a plan with no statement to draw are plans all the same: the viewer
    /// cannot draw them, and Save .sqlplan is what takes them to a tool that can.</summary>
    [Fact]
    public void SaveStaysEnabled_ForAPlanTheViewerDoesNotDraw()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 1000, 700);

                StaTestThread.Step("LoadPlan: a plan over the parser's size limit");
                await control.LoadPlan(Plan(Statement(1, Scan(0))) + new string(' ', 16_777_217), "Stored Plan");
                LayOut(host);
                Assert.Contains("size limit", ((TextBlock)control.FindName("EmptyStateTitle")).Text, StringComparison.Ordinal);
                problems.AddRange(SaveAloneIsEnabled(control, "for a plan over the parser's size limit"));

                StaTestThread.Step("LoadPlan: a plan with no statement");
                await control.LoadPlan(Plan(), "Stored Plan");
                LayOut(host);
                Assert.Empty(OperatorBorders(control));
                problems.AddRange(SaveAloneIsEnabled(control, "for a plan with no statement to draw"));

                StaTestThread.Step("LoadPlan: text that does not parse");
                await control.LoadPlan("not a plan", "Pasted Plan");
                LayOut(host);
                if (ToolbarButton(control, "Save .sqlplan").IsEnabled)
                    problems.Add("after text that does not parse, the \"Save .sqlplan\" button is enabled");
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A withheld plan is not drawn, and Save stays enabled for it: its click is what says why that plan is not
    /// saved.</summary>
    [Fact]
    public void AWithheldPlan_KeepsSaveEnabled_AndDisablesTheRest()
    {
        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var control = NewControl();
            try
            {
                var host = Host(control, 1000, 700);
                Assert.True(WithheldPlanGuard.WithheldSentence(SensitiveStatements.PlaceholderText) is not null, "the placeholder text is not the withheld-plan marker");
                await control.LoadPlan(Plan(Statement(1, Scan(0))), "Stored Plan");
                LayOut(host);
                problems.AddRange(ButtonsNotIn(control, enabled: true, "with a plan drawn"));

                StaTestThread.Step("LoadPlan: the withheld-plan marker, over the plan on screen");
                await control.LoadPlan(SensitiveStatements.PlaceholderText, "Stored Plan");
                LayOut(host);
                Assert.Empty(OperatorBorders(control));
                problems.AddRange(SaveAloneIsEnabled(control, "for a withheld plan"));

                StaTestThread.Step("LoadPlan: a plan that parses, over the withheld one");
                await control.LoadPlan(Plan(Statement(1, Scan(3))), "Pasted Plan");
                LayOut(host);
                problems.AddRange(ButtonsNotIn(control, enabled: true, "with a plan drawn after a withheld one"));
            }
            finally
            {
                control.Cleanup();
            }
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>What is wrong when Save .sqlplan is not the one enabled button of the five.</summary>
    private static IEnumerable<string> SaveAloneIsEnabled(PlanViewerControl control, string when)
    {
        foreach (var content in new[] { "+", "\u2212", "Fit", "Save .sqlplan", "Minimap" })
        {
            var expected = content == "Save .sqlplan";
            if (ToolbarButton(control, content).IsEnabled != expected)
                yield return $"{when}, the \"{content}\" button is {(expected ? "disabled" : "enabled")}";
        }
    }

    /// <summary>The toolbar buttons that act on a drawn plan and are not in the <paramref name="enabled"/> state.</summary>
    private static IEnumerable<string> ButtonsNotIn(PlanViewerControl control, bool enabled, string when)
    {
        foreach (var content in new[] { "+", "\u2212", "Fit", "Save .sqlplan", "Minimap" })
        {
            if (ToolbarButton(control, content).IsEnabled != enabled)
                yield return $"{when}, the \"{content}\" button is {(enabled ? "disabled" : "enabled")}";
        }
    }

    private static Button ToolbarButton(PlanViewerControl control, string content)
    {
        var button = Descendants(control).OfType<Button>().FirstOrDefault(b => b.Content as string == content);
        Assert.True(button is not null, $"the plan viewer's toolbar has no \"{content}\" button");
        return button!;
    }

    private static void Click(ButtonBase button) => button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

    private static string ZoomText(PlanViewerControl control) => ((TextBlock)control.FindName("ZoomLevelText")).Text;

    private static ScaleTransform Zoom(PlanViewerControl control) => (ScaleTransform)((Canvas)control.FindName("PlanCanvas")).LayoutTransform;

    /// <summary>The size <paramref name="text"/> takes on one line with nothing to cut it.</summary>
    private static Size OneLine(TextBlock text)
    {
        var oneLine = new TextBlock { Text = text.Text, FontFamily = text.FontFamily, FontSize = text.FontSize, FontWeight = text.FontWeight, FontStyle = text.FontStyle };
        oneLine.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        return oneLine.DesiredSize;
    }

    /// <summary>The CI screen is 1024x768, so the size comes from an explicitly sized host, not from a Window.</summary>
    private static Border Host(PlanViewerControl control, double width, double height)
    {
        var host = new Border { Width = width, Height = height, Child = control };
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

    /// <summary>The rendered operators: the canvas children that carry a plan node.</summary>
    private static Border[] OperatorBorders(PlanViewerControl control) =>
        ((Canvas)control.FindName("PlanCanvas")).Children.OfType<Border>().Where(b => b.Tag is PlanNode).ToArray();

    /// <summary>Releases the left button over the first rendered operator, the click that selects it.</summary>
    private static void ClickFirstOperator(PlanViewerControl control)
    {
        var border = OperatorBorders(control).First();
        border.RaiseEvent(new MouseButtonEventArgs(Mouse.PrimaryDevice, 0, MouseButton.Left) { RoutedEvent = UIElement.MouseLeftButtonUpEvent });
    }

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            yield return child;
            foreach (var grandchild in Descendants(child))
                yield return grandchild;
        }
    }

    private static IEnumerable<DependencyObject> Ancestors(DependencyObject element)
    {
        for (var parent = LogicalTreeHelper.GetParent(element); parent is not null; parent = LogicalTreeHelper.GetParent(parent))
            yield return parent;
    }

    private static string Plan(params string[] statements) =>
        $"<ShowPlanXML xmlns=\"{Ns}\" Version=\"1.564\" Build=\"16.0.4000.1\"><BatchSequence><Batch><Statements>{string.Concat(statements)}</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string Statement(int id, string rootOperator, string parameterList = "") =>
        $"<StmtSimple StatementText=\"SELECT {id} FROM dbo.t{id}\" StatementId=\"{id}\" StatementCompId=\"{id}\" StatementType=\"SELECT\" StatementSubTreeCost=\"0.0032\" StatementEstRows=\"1\" QueryHash=\"0x{id:X16}\" QueryPlanHash=\"0x{id:X16}\">"
        + $"<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"100\">{rootOperator}{parameterList}</QueryPlan></StmtSimple>";

    /// <summary>One datetime parameter with a compiled and a runtime value, under <see cref="LongParameterName"/>.</summary>
    private static string LongNameParameter() =>
        $"<ParameterList><ColumnReference Column=\"{LongParameterName}\" ParameterDataType=\"datetime\" ParameterCompiledValue=\"'2026-01-01 00:00:00.000'\" ParameterRuntimeValue=\"'2026-02-01 00:00:00.000'\"/></ParameterList>";

    private static string Scan(int nodeId) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"1\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.0032\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + $"<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t{nodeId}]\" IndexKind=\"Heap\" Storage=\"RowStore\"/></TableScan></RelOp>";

    /// <summary>A scan under <paramref name="tops"/> Top operators, one beneath the other: a plan that many operators wide.</summary>
    private static string Chain(int tops)
    {
        var plan = Scan(tops);
        for (var nodeId = tops - 1; nodeId >= 0; nodeId--)
        {
            plan = $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"Top\" LogicalOp=\"Top\" EstimateRows=\"1\" EstimateIO=\"0\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.0032\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
                + $"<OutputList/><Top RowCount=\"0\" IsPercent=\"0\" WithTies=\"0\">{plan}</Top></RelOp>";
        }

        return plan;
    }

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
