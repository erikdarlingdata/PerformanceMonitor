/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8460: the Properties panel's Parameterization Type row names the statement's parameterization instead of printing the
/// showplan attribute's code. The codes are the ones <c>sys.query_store_query.query_parameterization_type</c> documents:
/// 1 User, 2 Simple, 3 Forced. A code outside that list stays visible as its number. A 0 reads "Not recorded", since
/// the engine writes 0 for a parameterized statement whenever Query Store is not READ_WRITE; when the statement's
/// parameters are all numbered, that shape follows as a hint toward simple or forced parameterization or a prepared
/// statement, which the plan XML cannot tell apart; and a plan with no attribute at all has no row, as a plan with no
/// optimization level has no Optimization Level row. The row is read from the
/// rendered panel: a plan carrying the code is loaded on the shared STA thread, the panel is opened for the root node
/// through the control's private builder, as <see cref="PlanViewerEstimatedExecutionsTests"/> does, and the row's value
/// text is read back; nothing reads source text (#5833). The plan canvas reads the theme's brushes, so the Viewer's theme
/// is merged at application scope for the body and removed after it.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class PlanViewerParameterizationTypeRowTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Theory]
    [InlineData(0, "Not recorded")]
    [InlineData(1, "User")]
    [InlineData(2, "Simple")]
    [InlineData(3, "Forced")]
    public void ThePropertiesPanel_NamesTheStatementsParameterizationType(int code, string expected)
    {
        Assert.Equal(expected, ParameterizationTypeRow(Plan(code)));
    }

    [Theory]
    [InlineData(4)]
    [InlineData(17)]
    public void ThePropertiesPanel_LeavesAnUnknownParameterizationCodeAsItsNumber(int code)
    {
        Assert.Equal(code.ToString(CultureInfo.InvariantCulture), ParameterizationTypeRow(Plan(code)));
    }

    /// <summary>An actual plan whose statement is <c>WHERE [u].[Reputation]=@1 AND [u].[DisplayName]=@2</c> with a
    /// parameter list of <c>@1</c> and <c>@2</c> and <c>StatementParameterizationType="0"</c>: simple parameterization
    /// captured while Query Store was not READ_WRITE, or a prepared statement with numbered names; the plan XML cannot
    /// tell which, so the row says so.</summary>
    [Fact]
    public void ThePropertiesPanel_HintsAtTheNumberedParameters_WhenTheCodeIsZero()
    {
        var xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", "key_lookup_plan.sqlplan"));
        Assert.Equal("Not recorded (parameters numbered from @1: simple parameterization or a prepared statement)", ParameterizationTypeRow(xml));
    }

    /// <summary>A SQL Server 2019 capture of a prepared auto-parameterized plan with no
    /// <c>StatementParameterizationType</c> attribute at all: the row is left out rather than printed as a 0, while
    /// the section's other rows, Optimization Level among them, are still there.</summary>
    [Fact]
    public void ThePropertiesPanel_HasNoParameterizationTypeRow_WhenThePlanCarriesNoAttribute()
    {
        var rows = PropertiesRows(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "StatementScrub", "autoparam_select_prepared.xml")));
        Assert.True(rows.ContainsKey("Optimization Level"), "the Statement Info section rendered without its Optimization Level row");
        Assert.False(rows.ContainsKey("Parameterization Type"), "a plan with no StatementParameterizationType attribute has a Parameterization Type row");
    }

    /// <summary>Loads <paramref name="planXml"/>, opens the Properties panel for its root node and returns the
    /// Parameterization Type row's value text, or null when the Statement Info section has no such row.</summary>
    private static string? ParameterizationTypeRow(string planXml) =>
        PropertiesRows(planXml).TryGetValue("Parameterization Type", out var text) ? text : null;

    /// <summary>Loads <paramref name="planXml"/>, opens the Properties panel for its root node and returns the panel's
    /// rows by label.</summary>
    private static Dictionary<string, string> PropertiesRows(string planXml)
    {
        Dictionary<string, string>? value = null;
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
            try
            {
                StaTestThread.Step("LoadPlan");
                await control.LoadPlan(planXml, "plan");

                StaTestThread.Step("opening the Properties panel for the root node");
                ShowProperties(control, new PlanNode { NodeId = 0, PhysicalOp = "Clustered Index Seek", LogicalOp = "Clustered Index Seek" });

                value = PanelRows(control);
                Assert.True(value.Count > 0, "the Properties panel rendered no rows");
            }
            finally
            {
                StaTestThread.Step("Cleanup()");
                control.Cleanup();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });
        return value!;
    }

    private static void ShowProperties(PlanViewerControl control, PlanNode node)
    {
        var show = typeof(PlanViewerControl).GetMethod("ShowPropertiesPanel", Instance);
        Assert.True(show is not null, "PlanViewerControl.ShowPropertiesPanel no longer exists under that name - this test's reflection anchor moved.");
        show!.Invoke(control, new object[] { node });
    }

    /// <summary>The Properties panel's rows by label: each is a Grid of a label TextBlock and a value TextBox inside a section's Expander.</summary>
    private static Dictionary<string, string> PanelRows(PlanViewerControl control)
    {
        var content = control.FindName("PropertiesContent") as Panel;
        Assert.True(content is not null, "the plan viewer has no PropertiesContent panel - the Properties panel's host moved");

        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var grid in content!.Children.OfType<Expander>().Select(e => e.Content).OfType<Panel>().SelectMany(p => p.Children.OfType<Grid>()))
        {
            var label = grid.Children.OfType<TextBlock>().FirstOrDefault()?.Text;
            var text = grid.Children.OfType<TextBox>().FirstOrDefault()?.Text;
            if (label is not null && text is not null)
            {
                rows[label] = text;
            }
        }

        return rows;
    }

    /// <summary>A single-statement plan with one seek whose statement carries the given parameterization code.</summary>
    private static string Plan(int code) =>
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT * FROM dbo.t WHERE id = @1\" StatementId=\"1\" StatementType=\"SELECT\" " +
        $"StatementParameterizationType=\"{code.ToString(CultureInfo.InvariantCulture)}\" " +
        "StatementSubTreeCost=\"1\" StatementEstRows=\"10\" StatementOptmLevel=\"FULL\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" DegreeOfParallelism=\"1\">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Seek\" LogicalOp=\"Clustered Index Seek\" " +
        "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"1\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList />" +
        "<IndexScan Ordered=\"1\" ForcedIndex=\"0\" ForceSeek=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
        "</IndexScan>" +
        "</RelOp>" +
        "</QueryPlan></StmtSimple>" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";
}
