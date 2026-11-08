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
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;

using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8310: the FinOps pages, rendered on the real tab with the app's themes, show what they hold. The 7-Day Provisioning Trend's
/// Status reads whole; Expensive Queries and Cost Savings keep every column header named and inside the grid when the window is
/// maximized beside the sidebar; the Cost Savings Detail tooltip stays inside a 900 px window; the Index Analysis "Database:" box is a
/// picker with an arrow in every theme; its two grids show no horizontal scroll bar while they are empty; High Impact shows one empty
/// sentence, not two; and the 7-Day Provisioning Trend shows the stolen memory that decides a low-CPU day, gives the day's reason on
/// its Status, and says which window the badge covers. The Darling Viewer's twin is ViewerFinOpsPagesReadabilityTests in Darling.Tests.
/// </summary>
[Collection("credential-manager")]
public sealed class FinOpsPagesReadabilityTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /* A maximized window on the walk's display is about 1550 wide; the tab sits beside the 280 px sidebar. At Lite's 900x600
       minimum the sidebar is its 40 px rail (#8219). */
    private const int MaximizedTabWidth = 1270;
    private const int MinimumTabWidth = 860;

    [Theory]
    [InlineData(MinimumTabWidth, 600)]
    [InlineData(MaximizedTabWidth, 925)]
    public void TheProvisioningTrend_ShowsEveryStatusWhole(int width, int height)
    {
        var problems = StaTestThread.Run(() => WithTheme("Dark", () => WithTab(width, height, (tab, window) =>
        {
            ShowUtilization(tab);
            var grid = (DataGrid)tab.FindName("ProvisioningTrendGrid");
            grid.ItemsSource = new[] { "UNDER_PROVISIONED", "OVER_PROVISIONED", "RIGHT_SIZED" }
                .Select((status, i) => new ProvisioningTrendRow { Day = new DateTime(2026, 11, 1 + i), Status = status, AvgCpuPct = 40 + i, P95CpuPct = 70 + i, MaxCpuPct = 90, MemoryRatio = 1.5m })
                .ToList();
            Reveal(grid);
            Settle(window);
            grid.BringIntoView();
            Settle(window);

            var found = new List<string>();
            var statuses = Cells(grid, "StatusDisplay").ToList();
            if (statuses.Count == 0)
            {
                found.Add($"{width}x{height}: no Status cell was realized");
            }
            found.AddRange(statuses.Where(c => !Whole(c)).Select(c => $"{width}x{height}: '{c.Text}' is cut in a {c.ActualWidth:0.#} px Status cell"));
            return found;
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void ExpensiveQueriesAndCostSavings_KeepEveryColumnHeaderNamedAndInsideTheGrid_WhenMaximized()
    {
        var problems = StaTestThread.Run(() => WithTheme("Dark", () => WithTab(MaximizedTabWidth, 925, (tab, window) =>
        {
            var found = new List<string>();

            var recommendations = (DataGrid)tab.FindName("RecommendationsDataGrid");
            recommendations.ItemsSource = Recommendations();
            Reveal(recommendations);
            Settle(window);
            found.AddRange(HeadersOutside("Cost Savings", recommendations));

            var expensive = (DataGrid)tab.FindName("ExpensiveQueriesDataGrid");
            expensive.ItemsSource = ExpensiveQueries();
            Reveal(expensive);
            Settle(window);
            expensive.BringIntoView();
            Settle(window);
            found.AddRange(HeadersOutside("Expensive Queries", expensive));
            return found;
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheCostSavingsDetailTooltip_StaysInsideA900PxWindow()
    {
        var problems = StaTestThread.Run(() => WithTheme("Dark", () => WithTab(MinimumTabWidth, 600, (tab, window) =>
        {
            var grid = (DataGrid)tab.FindName("RecommendationsDataGrid");
            grid.ItemsSource = Recommendations();
            Reveal(grid);
            Settle(window);
            var detail = Cells(grid, "Detail").FirstOrDefault() ?? throw new Xunit.Sdk.XunitException("no Detail cell was realized");

            var tip = detail.ToolTip as ToolTip ?? new ToolTip { Content = detail.ToolTip };
            tip.DataContext = detail.DataContext;
            tip.PlacementTarget = detail;
            tip.IsOpen = true;
            try
            {
                Settle(window);
                var found = new List<string>();
                if (tip.ActualWidth > window.ActualWidth + 0.5)
                {
                    found.Add($"the Detail tooltip is {tip.ActualWidth:0} px wide in a {window.ActualWidth:0} px window");
                }
                if (tip.ActualWidth <= 0)
                {
                    found.Add("the Detail tooltip did not open");
                }
                return found;
            }
            finally
            {
                tip.IsOpen = false;
            }
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheIndexAnalysisDatabaseBox_IsAPickerWithAnArrow(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, () => WithTab(MaximizedTabWidth, 925, (tab, window) =>
        {
            var box = (FrameworkElement)tab.FindName("IndexAnalysisDatabaseInput");
            Reveal(box);
            Settle(window);

            var found = new List<string>();
            if (box is not ComboBox)
            {
                found.Add($"{theme}: the Database box is a {box.GetType().Name}, not a picker");
                return found;
            }

            var arrow = Descendants(box).OfType<ToggleButton>().SelectMany(Descendants).OfType<System.Windows.Shapes.Path>().FirstOrDefault(p => p.ActualWidth > 0 && p.ActualHeight > 0 && p.IsVisible);
            if (arrow is null)
            {
                found.Add($"{theme}: the Database picker shows no drop-down arrow");
            }
            return found;
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheIndexAnalysisGrids_ShowNoHorizontalScrollBar_WhileTheyAreEmpty()
    {
        var problems = StaTestThread.Run(() => WithTheme("Dark", () => WithTab(MinimumTabWidth, 600, (tab, window) =>
        {
            var summary = (DataGrid)tab.FindName("IndexAnalysisSummaryGrid");
            var detail = (DataGrid)tab.FindName("IndexAnalysisDetailGrid");
            Reveal(summary);
            Settle(window);

            var found = new List<string>();
            foreach (var (name, grid) in new[] { ("summary", summary), ("detail", detail) })
            {
                if (grid.Items.Count != 0)
                {
                    found.Add($"the {name} grid holds {grid.Items.Count} rows before a run");
                }
                var scroller = Descendants(grid).OfType<ScrollViewer>().FirstOrDefault();
                if (scroller is null)
                {
                    found.Add($"the {name} grid has no scroll viewer");
                }
                else if (scroller.ComputedHorizontalScrollBarVisibility == Visibility.Visible)
                {
                    found.Add($"the empty {name} grid shows a horizontal scroll bar");
                }
            }
            return found;
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public async Task HighImpact_ShowsOneEmptySentence_WhenTheWindowHasNoRows()
    {
        var dir = Path.Combine(Path.GetTempPath(), "FinOpsHighImpactEmpty_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "empty.duckdb"));
            await duckDb.InitializeFromTemplateAsync();
            var dataService = new LocalDataService(duckDb);

            var problems = new List<string>();
            StaTestThread.Run(async () =>
            {
                var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                var added = AddTheme(app, "Dark");
                var tab = new FinOpsTab();
                typeof(FinOpsTab).GetField("_dataService", Instance)!.SetValue(tab, dataService);
                var window = new Window { Content = tab, Width = MaximizedTabWidth, Height = 925, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var grid = (DataGrid)tab.FindName("HighImpactDataGrid");
                    Reveal(grid);
                    Settle(window);
                    await (Task)typeof(FinOpsTab).GetMethod("LoadHighImpactQueriesAsync", Instance)!.Invoke(tab, [12345])!;
                    Settle(window);
                    problems.AddRange(EmptySentences(grid, "High Impact"));
                }
                finally
                {
                    window.Close();
                    RemoveTheme(app, added);
                }
            });

            Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void TheProvisioningTrend_ShowsTheStolenMemoryThatDecidesADay_AndSaysWhichWindowTheBadgeCovers()
    {
        var problems = StaTestThread.Run(() => WithTheme("Dark", () => WithTab(MaximizedTabWidth, 925, (tab, window) =>
        {
            ShowUtilization(tab);
            var grid = (DataGrid)tab.FindName("ProvisioningTrendGrid");
            grid.ItemsSource = TrendRows();
            Reveal(grid);
            Settle(window);
            grid.BringIntoView();
            Settle(window);
            return TrendProblems(grid, (FrameworkElement)tab.FindName("ProvisioningTrendCard"));
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The walk's first three days: 0.6% to 1.6% average CPU, read UNDER PROVISIONED because 82%, 69% and 63% of memory was stolen. The
    /// stolen share and the reason are set by name, so the rows build whether or not the row type carries them yet; the asserts then
    /// say whether the grid shows them.
    /// </summary>
    private static List<ProvisioningTrendRow> TrendRows()
    {
        var rows = new List<ProvisioningTrendRow>();
        foreach (var (day, cpu, stolen, status) in new[] { (1, 0.6m, 82m, "UNDER_PROVISIONED"), (2, 1.1m, 69m, "UNDER_PROVISIONED"), (3, 1.6m, 63m, "UNDER_PROVISIONED"), (4, 12m, 37m, "OVER_PROVISIONED") })
        {
            var row = new ProvisioningTrendRow { Day = new DateTime(2026, 11, day), AvgCpuPct = cpu, P95CpuPct = cpu + 1, MaxCpuPct = 5, MemoryRatio = 0.3m, Status = status };
            typeof(ProvisioningTrendRow).GetProperty("StolenMemoryPct")?.SetValue(row, stolen);
            typeof(ProvisioningTrendRow).GetProperty("StatusReason")?.SetValue(row, status == "UNDER_PROVISIONED" ? $"Memory is under pressure: stolen memory is {stolen:N0}% of total server memory (threshold: 60%)." : null);
            rows.Add(row);
        }
        return rows;
    }

    /// <summary>The Stolen Mem % column with the first day's 82.0, a reason tooltip on an UNDER PROVISIONED day, none on the other, and the card's window note.</summary>
    private static List<string> TrendProblems(DataGrid grid, FrameworkElement card)
    {
        var found = new List<string>();
        var stolenHeader = Descendants(grid).OfType<DataGridColumnHeader>().FirstOrDefault(h => h.Column is not null && Descendants(h).OfType<TextBlock>().Any(t => t.Text == "Stolen Mem %"));
        if (stolenHeader is null)
        {
            found.Add("the trend has no Stolen Mem % column");
        }
        else
        {
            var cells = Descendants(grid).OfType<DataGridCell>().Where(c => c.Column == stolenHeader.Column).Select(c => Descendants(c).OfType<TextBlock>().FirstOrDefault()?.Text).ToList();
            if (!cells.Contains("82.0"))
            {
                found.Add($"no Stolen Mem % cell reads 82.0 (cells: {string.Join(", ", cells)})");
            }
        }

        var statuses = Descendants(grid).OfType<DataGridCell>().Where(c => PathOf(c.Column) == "StatusDisplay").Select(c => Descendants(c).OfType<TextBlock>().First()).ToList();
        var under = statuses.FirstOrDefault(t => t.Text.StartsWith("UNDER", StringComparison.Ordinal));
        var over = statuses.FirstOrDefault(t => t.Text.StartsWith("OVER", StringComparison.Ordinal));
        if (under is null || over is null)
        {
            found.Add($"the Status cells were not realized ({statuses.Count} seen)");
        }
        else
        {
            var reason = under.ToolTip is ToolTip tip ? WithContext(tip, under).Content as string : under.ToolTip as string;
            if (reason is null || !reason.Contains("stolen", StringComparison.OrdinalIgnoreCase))
            {
                found.Add($"the UNDER PROVISIONED day's Status gives no reason naming stolen memory (tooltip: '{reason}')");
            }
            if (over.ToolTip is ToolTip overTip ? WithContext(overTip, over).Content is not null : over.ToolTip is not null)
            {
                found.Add("the OVER PROVISIONED day carries a reason tooltip it has no text for");
            }
        }

        if (!Descendants(card).OfType<TextBlock>().Any(t => t.Text.Contains("last 24 hours", StringComparison.Ordinal) && t.Text.Contains("whole day", StringComparison.Ordinal)))
        {
            found.Add("the card does not say that the badge covers the last 24 hours and each row a whole day");
        }
        return found;
    }

    private static ToolTip WithContext(ToolTip tip, FrameworkElement owner)
    {
        tip.DataContext = owner.DataContext;
        return tip;
    }

    /// <summary>Every visible "No ..." sentence in the Grid that holds <paramref name="grid"/>, when there is not exactly one.</summary>
    private static IEnumerable<string> EmptySentences(DataGrid grid, string page)
    {
        var host = (Panel)VisualTreeHelper.GetParent(grid);
        var shown = Descendants(host).OfType<TextBlock>()
            .Where(t => t.IsVisible && t.ActualWidth > 0 && t.Text.StartsWith("No ", StringComparison.Ordinal) && !Descendants(grid).Contains(t))
            .Select(t => t.Text).ToList();
        if (shown.Count != 1)
        {
            yield return $"{page} shows {shown.Count} empty sentences: {string.Join(" | ", shown)}";
        }
    }

    /// <summary>Findings long enough that an auto-sized Finding column would take the width Est. Savings needs; details long enough to wrap.</summary>
    private static List<RecommendationRow> Recommendations() =>
    [
        new() { Category = "Licensing", Severity = "High", Confidence = "High", Finding = "Enterprise Edition licensed on 32 cores while no Enterprise-only feature has been used in the last 30 days of collection", Detail = "The server reports Enterprise Edition: Core-based Licensing and sys.dm_db_persisted_sku_features lists no feature in any database. Standard Edition covers this workload at its current size, with the 128 GB buffer pool cap and 24 core limit both above what the server uses today, and no partitioning, compression, online index operations or Resource Governor pools were seen in the collected window.", EstMonthlySavings = 4250m },
        new() { Category = "CPU", Severity = "Medium", Confidence = "Medium", Finding = "P95 CPU below 20% for 30 days on a 16 vCore server with 64 GB of memory and no waits on schedulers", Detail = "The 7-day P95 is 14% and the 30-day P95 is 17%. Halving the core count keeps the P95 under 40%, which leaves headroom for month-end batches twice the size of the largest one collected.", EstMonthlySavings = 1100m },
        new() { Category = "Storage", Severity = "Low", Confidence = "High", Finding = "Unused indexes hold 42 GB across three databases", Detail = "Twenty-one indexes with no seeks, scans or lookups since the last restart 61 days ago, each over 500 MB.", EstMonthlySavings = null },
    ];

    private static List<ExpensiveQueryRow> ExpensiveQueries() =>
    [
        new() { DatabaseName = "tenant_a", QueryPreview = "SELECT o.OrderId, o.Total FROM dbo.Orders AS o WHERE o.CustomerId = @c ORDER BY o.Placed DESC", FullQueryText = "SELECT ...", TotalCpuMs = 1_234_567, AvgCpuMsPerExec = 12.34m, TotalReads = 9_876_543, AvgReadsPerExec = 98.7m, Executions = 100_000, MonthlyCostShare = 123.45m },
        new() { DatabaseName = "tenant_b", QueryPreview = "UPDATE dbo.Inventory SET OnHand = OnHand - @q WHERE Sku = @s", FullQueryText = "UPDATE ...", TotalCpuMs = 234_567, AvgCpuMsPerExec = 2.3m, TotalReads = 876_543, AvgReadsPerExec = 8.7m, Executions = 100_000, MonthlyCostShare = 23.45m },
    ];

    /// <summary>Every header of a bound column that has no name, or whose name is cut or ends past the grid's right edge.</summary>
    private static IEnumerable<string> HeadersOutside(string gridName, DataGrid grid)
    {
        var headers = Descendants(grid).OfType<DataGridColumnHeader>().Where(h => h.Column is not null).ToList();
        if (headers.Count == 0)
        {
            yield return $"{gridName}: no column header was realized";
        }

        foreach (var header in headers)
        {
            var name = Descendants(header).OfType<TextBlock>().FirstOrDefault(t => !string.IsNullOrWhiteSpace(t.Text));
            if (name is null)
            {
                yield return $"{gridName}: the column bound to {PathOf(header.Column)} has a header with no name";
            }
            else if (!Whole(name) || Bounds(name, grid).Right > grid.ActualWidth + 0.5)
            {
                yield return $"{gridName}: the header '{name.Text}' ends at {Bounds(name, grid).Right:0.#} px in a {grid.ActualWidth:0.#} px wide grid";
            }
        }
    }

    private static string PathOf(DataGridColumn column) =>
        column is DataGridBoundColumn { Binding: Binding binding } ? binding.Path.Path : column.Header?.ToString() ?? column.DisplayIndex.ToString();

    /// <summary>The realized TextBlocks of the column bound to <paramref name="path"/>.</summary>
    private static IEnumerable<TextBlock> Cells(DataGrid grid, string path) =>
        Descendants(grid).OfType<DataGridCell>()
            .Where(c => PathOf(c.Column) == path)
            .Select(c => Descendants(c).OfType<TextBlock>().FirstOrDefault())
            .Where(t => t is not null && !string.IsNullOrWhiteSpace(t.Text))!;

    private static bool Whole(TextBlock block) => ClippedTextProbe.NaturalWidth(block) <= block.ActualWidth + 1;

    private static Rect Bounds(FrameworkElement element, Visual ancestor) =>
        element.TransformToAncestor(ancestor).TransformBounds(new Rect(element.RenderSize));

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root) => ClippedTextProbe.Descendants(root);

    private static void Settle(Window window) => ClippedTextProbe.Settle(window);

    private static void ShowUtilization(FinOpsTab tab)
    {
        ((FrameworkElement)tab.FindName("UtilizationContent")).Visibility = Visibility.Visible;
        ((FrameworkElement)tab.FindName("SummaryContent")).Visibility = Visibility.Visible;
        ((FrameworkElement)tab.FindName("NoUtilizationMessage")).Visibility = Visibility.Collapsed;
        ((FrameworkElement)tab.FindName("ProvisioningTrendCard")).Visibility = Visibility.Visible;
    }

    /// <summary>Selects the sub-tab that holds <paramref name="element"/>: a TabItem's content is laid out only while it is selected.</summary>
    private static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
            {
                owner.SelectedItem = item;
            }
        }
    }

    private static List<string> WithTab(int width, int height, Func<FinOpsTab, Window, List<string>> body)
    {
        var tab = new FinOpsTab();
        var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
        StaTestThread.ShowOffScreen(window);
        try
        {
            Settle(window);
            return body(tab, window);
        }
        finally
        {
            window.Close();
        }
    }

    /// <summary>Runs <paramref name="body"/> with the named theme alone in the application scope, where a control's StaticResource lookups resolve at load.</summary>
    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = AddTheme(app, theme);
        try
        {
            return body();
        }
        finally
        {
            RemoveTheme(app, added);
        }
    }

    private static (List<ResourceDictionary> Removed, ResourceDictionary Added) AddTheme(Application app, string theme)
    {
        var removed = app.Resources.MergedDictionaries.Where(d => d.Source?.OriginalString.Contains("/Themes/", StringComparison.Ordinal) == true).ToList();
        foreach (var dictionary in removed)
        {
            app.Resources.MergedDictionaries.Remove(dictionary);
        }
        var added = new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml") };
        app.Resources.MergedDictionaries.Add(added);
        return (removed, added);
    }

    private static void RemoveTheme(Application app, (List<ResourceDictionary> Removed, ResourceDictionary Added) added)
    {
        app.Resources.MergedDictionaries.Remove(added.Added);
        foreach (var dictionary in added.Removed)
        {
            app.Resources.MergedDictionaries.Add(dictionary);
        }
    }
}
