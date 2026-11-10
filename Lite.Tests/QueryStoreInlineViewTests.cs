/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using Darling.Tests;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Query Store grid's inline View button, clicked on the rendered grid with the app's theme. The plan is fetched by the
/// row's plan id; here the fetch is answered by the test in place of a monitored server. A plan that comes back opens on the
/// Plan Viewer tab and that tab is shown. A fetch that comes back empty says the plan is not in Query Store, and a fetch that
/// fails says why it failed; both leave the grid showing.
/// </summary>
[Collection("credential-manager")]
public sealed class QueryStoreInlineViewTests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private const string OneScanPlan =
        $"<ShowPlanXML xmlns=\"{Ns}\" Version=\"1.564\" Build=\"16.0.4000.1\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"SELECT 1 FROM dbo.t1\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"0.0032\" StatementEstRows=\"1\" QueryHash=\"0x0000000000000001\" QueryPlanHash=\"0x0000000000000001\">"
        + "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"100\">"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"1\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.0032\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t1]\" IndexKind=\"Heap\" Storage=\"RowStore\"/></TableScan></RelOp>"
        + "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    [Fact]
    public void TheViewButton_IsEnabledOnARowThatNamesAPlan_AndDisabledOnOneThatDoesNot()
    {
        var after = ClickView(_ => Task.FromResult<string?>(OneScanPlan));

        Assert.True(after.ViewWasEnabled, "the View button is disabled on a Query Store row with plan id 7");
        Assert.False(after.ViewWithoutAPlanWasEnabled, "the View button is enabled on a Query Store row with no plan id");
    }

    [Fact]
    public void ClickingView_OpensThePlanOnThePlanViewerTab_AndShowsThatTab()
    {
        var after = ClickView(_ => Task.FromResult<string?>(OneScanPlan));

        Assert.Equal(7, after.FetchedPlanId);
        Assert.True(after.PlanViewerTabSelected, $"after a click on View the page showing is \"{after.PageShowing}\", not the Plan Viewer: the plan opened on a tab that is not shown, so the click looks like it did nothing");
        Assert.Equal(1, after.PlanTabs);
        Assert.True(after.SelectedPlanTabOperators == 1, $"the plan tab shows {after.SelectedPlanTabOperators} operators; the fetched plan has one");
        Assert.Equal("View", after.ButtonText);
        Assert.Empty(after.Messages);
    }

    [Fact]
    public void ClickingView_ForAPlanQueryStoreDoesNotHold_SaysSo_AndLeavesTheGridShowing()
    {
        var after = ClickView(_ => Task.FromResult<string?>(null));

        var message = Assert.Single(after.Messages);
        Assert.Equal("Plan Not Found", message.Title);
        Assert.False(after.PlanViewerTabSelected, "a fetch that returned no plan showed the Plan Viewer tab");
        Assert.Equal("Queries", after.PageShowing);
        Assert.Equal(0, after.PlanTabs);
        Assert.Equal("View", after.ButtonText);
    }

    [Fact]
    public void ClickingView_WhenTheFetchFails_SaysWhyItFailed_NotThatThePlanIsMissing()
    {
        var after = ClickView(_ => Task.FromException<string?>(new InvalidOperationException("the connection to the monitored server timed out")));

        var message = Assert.Single(after.Messages);
        Assert.True(message.Title != "Plan Not Found", $"a fetch that failed is reported as a plan Query Store does not hold: \"{message.Text}\"");
        Assert.Contains("the connection to the monitored server timed out", message.Text, StringComparison.Ordinal);
        Assert.Equal(MessageBoxImage.Error, message.Image);
        Assert.False(after.PlanViewerTabSelected, "a fetch that failed showed the Plan Viewer tab");
        Assert.Equal("Queries", after.PageShowing);
        Assert.Equal(0, after.PlanTabs);
        Assert.Equal("View", after.ButtonText);
    }

    private sealed record Shown(string Title, string Text, MessageBoxImage Image);

    private sealed record AfterClick(
        bool ViewWasEnabled,
        bool ViewWithoutAPlanWasEnabled,
        long FetchedPlanId,
        bool PlanViewerTabSelected,
        string PageShowing,
        int PlanTabs,
        int SelectedPlanTabOperators,
        string ButtonText,
        List<Shown> Messages);

    /// <summary>Renders the tab on its Query Store grid with two rows, clicks View on the row that names plan 7, and waits for that click to finish.</summary>
    private static AfterClick ClickView(Func<QueryStoreRow, Task<string?>> fetch)
    {
        var dir = DialogWalkTestHost.ScratchDir("QsInlineView");
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            AfterClick? after = null;
            StaTestThread.Run(() => WithThemeAsync(async () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "qs-inline-view", DisplayName = "qs-inline-view", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                var messages = new List<Shown>();
                long fetchedPlanId = 0;
                tab.QueryStorePlanFetch = row => { fetchedPlanId = row.PlanId; return fetch(row); };
                tab.PlanMessage = (text, title, image) => messages.Add(new Shown(title, text, image));
                var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = 900, Height = 600, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var grid = (DataGrid)tab.FindName("QueryStoreGrid");
                    Reveal(grid);
                    ClippedTextProbe.Settle(window);
                    grid.ItemsSource = new List<QueryStoreRow>
                    {
                        new() { DatabaseName = "db", QueryId = 3, PlanId = 7, QueryText = "SELECT 1 FROM dbo.t1" },
                        new() { DatabaseName = "db", QueryId = 4, PlanId = 0, QueryText = "SELECT 2 FROM dbo.t2" },
                    };
                    window.UpdateLayout();

                    var buttons = ClippedTextProbe.Descendants(grid).OfType<Button>()
                        .Where(b => b.DataContext is QueryStoreRow && Equals(b.Content, "View")).ToList();
                    Assert.True(buttons.Count == 2, $"the Query Store grid rendered {buttons.Count} View buttons for two rows");
                    var view = buttons.Single(b => ((QueryStoreRow)b.DataContext).PlanId == 7);
                    var viewWithoutAPlan = buttons.Single(b => ((QueryStoreRow)b.DataContext).PlanId == 0);
                    var viewWasEnabled = view.IsEnabled;
                    var viewWithoutAPlanWasEnabled = viewWithoutAPlan.IsEnabled;

                    StaTestThread.Step("a click on the row's View button");
                    view.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                    await tab.InlineQueryStorePlanView;
                    window.UpdateLayout();

                    var main = (TabControl)tab.FindName("MainTabControl");
                    var plans = (TabControl)tab.FindName("PlanTabControl");
                    var operators = (plans.SelectedItem as TabItem)?.Content is PlanViewerControl viewer
                        ? ((Canvas)viewer.FindName("PlanCanvas")).Children.OfType<Border>().Count(b => b.Tag is PlanNode)
                        : 0;
                    after = new AfterClick(
                        viewWasEnabled,
                        viewWithoutAPlanWasEnabled,
                        fetchedPlanId,
                        ((TabItem)tab.FindName("PlanViewerTabItem")).IsSelected,
                        (main.SelectedItem as TabItem)?.Header as string ?? "",
                        plans.Items.Count,
                        operators,
                        view.Content as string ?? "",
                        messages);
                }
                finally
                {
                    foreach (var planTab in ((TabControl)tab.FindName("PlanTabControl")).Items.OfType<TabItem>())
                        (planTab.Content as PlanViewerControl)?.Cleanup();
                    window.Close();
                    tab.StopRefresh();
                }
            }));
            return after!;
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }

    /// <summary>Selects the tab that holds <paramref name="element"/> at every level: a TabItem's content is laid out only while it is selected.</summary>
    private static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
                owner.SelectedItem = item;
        }
    }

    /// <summary>The tab's markup and the plan viewer it opens name the theme's resources, so the theme stays merged until the click has finished.</summary>
    private static async Task WithThemeAsync(Func<Task> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            await body();
        }
        finally
        {
            if (added is not null)
                app.Resources.MergedDictionaries.Remove(added);
        }
    }
}
