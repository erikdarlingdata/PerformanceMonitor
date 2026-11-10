/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8459: a plan loaded into a sub-tab that already holds a plan replaces it, so the tab keeps the plain label. The
/// label is made unique among the OTHER sub-tabs; the tab being labelled is not a sibling of itself. Lite and the
/// Darling Viewer both build their sub-tabs through <see cref="StandalonePlanViewerController"/>, so one class covers
/// both apps. The plan canvas reads the theme's brushes, so the Viewer's theme is merged at application scope for each
/// body and removed after it.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class StandalonePlanViewerReloadLabelTests
{
    [Fact]
    public void ReloadingAPlanIntoItsOwnTab_KeepsThePlainLabel()
    {
        OnController(async controller =>
        {
            var tab = controller.AddNewEmptyPlanSubTab();

            await controller.LoadPlanIntoSubTab(tab, Plan, "Pasted Plan");
            Assert.Equal("Pasted Plan", Label(tab));

            await controller.LoadPlanIntoSubTab(tab, Plan, "Pasted Plan");
            Assert.Equal("Pasted Plan", Label(tab));
            Assert.Equal("Pasted Plan", HeaderText(tab));

            await controller.LoadPlanIntoSubTab(tab, Plan, "Pasted Plan");
            Assert.Equal("Pasted Plan", Label(tab));
        });
    }

    /// <summary>A sibling holding the plain label still earns the new tab its suffix, and a reload of the suffixed tab
    /// keeps that suffix rather than counting up.</summary>
    [Fact]
    public void ReloadingASuffixedTab_KeepsItsSuffix_WhileASiblingHoldsThePlainLabel()
    {
        OnController(async controller =>
        {
            var first = controller.AddNewEmptyPlanSubTab();
            var second = controller.AddNewEmptyPlanSubTab();

            await controller.LoadPlanIntoSubTab(first, Plan, "Pasted Plan");
            await controller.LoadPlanIntoSubTab(second, Plan, "Pasted Plan");
            Assert.Equal("Pasted Plan", Label(first));
            Assert.Equal("Pasted Plan (1)", Label(second));

            await controller.LoadPlanIntoSubTab(second, Plan, "Pasted Plan");
            Assert.Equal("Pasted Plan (1)", Label(second));
            Assert.Equal("Pasted Plan", Label(first));
        });
    }

    /// <summary>The full label, which the header keeps in its tooltip (the visible text is cut at 30 characters).</summary>
    private static string? Label(TabItem tab) => HeaderBlock(tab).ToolTip as string;

    private static string HeaderText(TabItem tab) => HeaderBlock(tab).Text;

    private static TextBlock HeaderBlock(TabItem tab) => (TextBlock)((StackPanel)tab.Header).Children[0];

    /// <summary>WPF objects need STA and a dispatcher: the body runs on the shared STA test thread (#5602). The
    /// controller owns a bare <see cref="TabControl"/>, initialized with its "+" tab, whose resources resolve from the
    /// merged theme; every sub-tab's viewer is released through <see cref="StandalonePlanViewerController.Reset"/>.</summary>
    private static void OnController(Func<StandalonePlanViewerController, Task> body) => StaTestThread.Run(async () =>
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("ForegroundMutedBrush"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        var controller = new StandalonePlanViewerController(new TabControl());
        controller.EnsureInitialized();
        try
        {
            await body(controller);
        }
        finally
        {
            controller.Reset();
            if (added is not null) app.Resources.MergedDictionaries.Remove(added);
        }
    });

    /// <summary>A single-statement plan with one seek, enough for the viewer to render and the tab to be labelled.</summary>
    private const string Plan =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT * FROM dbo.t WHERE id = 1\" StatementId=\"1\" StatementType=\"SELECT\" " +
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
