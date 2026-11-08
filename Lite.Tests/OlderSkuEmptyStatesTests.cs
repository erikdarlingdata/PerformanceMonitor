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
using System.Windows.Media;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A Lite page on a SQL Server version that cannot have its data says so, a near-flat size series reads flat, and an empty FinOps card says
/// why. Each fact builds the real control on the shared STA thread with the theme and the shared styles loaded, shows it, and reads what was
/// realized: the empty-state sentence inside a grid, a chart's Y axis and its tick labels, the message in a card, an expander's header and
/// its distance from the grid below it.
/// </summary>
[Collection("credential-manager")]
public sealed class OlderSkuEmptyStatesTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly DateTime At = new(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The Plan Corrections grid's own sentence for a quiet window on a server whose version has automatic tuning.</summary>
    private const string QuietWindow = "No plan corrections in the selected time window.";

    /// <summary>The sentence the FinOps "Top Databases by Avg CPU / Execution" card shows when the Total card has rows and it has none.</summary>
    private const string NoExecutions = "No database recorded an execution in the last 24 hours, so there is no CPU per execution to rank.";

    /// <summary>
    /// On a server whose newest stored properties say SQL Server 2016, the Automatic Tuning grid and the Plan Corrections grid both say the
    /// version has no automatic tuning, in the collector's own sentence, instead of a bare grid and a "quiet window" sentence.
    /// </summary>
    [Fact]
    public async Task OnA2016Server_AutomaticTuningAndPlanCorrections_SayTheVersionHasNoAutomaticTuning()
    {
        var expected = PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 });
        var texts = await EmptyTextsAfterRefresh("13.0.5026.0");

        Assert.Equal(expected, texts.AutomaticTuning);
        Assert.Equal(expected, texts.PlanCorrections);
    }

    /// <summary>
    /// On a server whose version has automatic tuning, an empty Automatic Tuning grid says the collector has not stored a cycle, in the
    /// collector's own sentence, and the Plan Corrections grid keeps its quiet-window sentence.
    /// </summary>
    [Fact]
    public async Task OnA2019Server_AutomaticTuning_SaysNoCycleIsStored_AndPlanCorrectionsKeepsItsOwnSentence()
    {
        var expected = PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 15 });
        var texts = await EmptyTextsAfterRefresh("15.0.4335.1");

        Assert.Equal(expected, texts.AutomaticTuning);
        Assert.Equal(QuietWindow, texts.PlanCorrections);
    }

    /// <summary>
    /// A tempdb Size series that stays within 0.01 MB of 8,190 MB draws on an axis at least 1 MB tall, and its tick labels are not all the
    /// same number.
    /// </summary>
    [Fact]
    public void TempDbSize_ANearFlatSeries_GetsAnAxisWithAMinimumSpan_AndDistinctTickLabels()
    {
        var rows = Enumerable.Range(0, 24)
            .Select(i => new TempDbRow { CollectionTime = At.AddMinutes(-5 * (24 - i)), TotalReservedMb = 8000.0, UnallocatedMb = 190.25 + (i % 2) * 0.01 })
            .ToList();

        var (span, labels) = WithServerTab("DarkTheme", tab =>
        {
            typeof(ServerTab).GetMethod("UpdateTempDbSizeChart", Instance)!.Invoke(tab, new object?[] { rows, 24, null, null });
            var chart = Find<ScottPlot.WPF.WpfPlot>(tab, "TempDbSizeChart");
            var limits = chart.Plot.Axes.GetLimits();
            using (chart.Plot.GetImage(900, 320)) { }
            var ticks = chart.Plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor).Select(t => t.Label).ToList();
            return (limits.Top - limits.Bottom, ticks);
        });

        Assert.True(span >= 1.0, $"the Y axis spans {span:0.####} MB for a series that moves 0.01 MB");
        Assert.True(labels.Distinct(StringComparer.Ordinal).Count() >= 2, "the tick labels all read the same: " + string.Join(", ", labels));
    }

    /// <summary>
    /// A series that does move keeps the axis the autoscale gave it, so a real growth trend is not flattened by the floor.
    /// </summary>
    [Fact]
    public void TempDbSize_AGrowingSeries_KeepsTheAutoscaledAxis()
    {
        var rows = Enumerable.Range(0, 24)
            .Select(i => new TempDbRow { CollectionTime = At.AddMinutes(-5 * (24 - i)), TotalReservedMb = 8000.0 + i * 50, UnallocatedMb = 190.0 })
            .ToList();

        var (bottom, top) = WithServerTab("DarkTheme", tab =>
        {
            typeof(ServerTab).GetMethod("UpdateTempDbSizeChart", Instance)!.Invoke(tab, new object?[] { rows, 24, null, null });
            var limits = Find<ScottPlot.WPF.WpfPlot>(tab, "TempDbSizeChart").Plot.Axes.GetLimits();
            return (limits.Bottom, limits.Top);
        });

        Assert.True(bottom <= 8190.0 && top >= 9340.0, $"the axis {bottom:0.#}..{top:0.#} does not hold the series 8,190..9,340");
        Assert.True(top - bottom < 1500, $"the axis {bottom:0.#}..{top:0.#} is far wider than the series");
    }

    /// <summary>
    /// When the "Top Databases by Total CPU" card has rows and the "Top Databases by Avg CPU / Execution" card has none, the Avg card says
    /// no execution was recorded in the last 24 hours, in a muted sentence inside the card.
    /// </summary>
    [Fact]
    public void FinOpsTopAvg_WithTotalRowsAndNoAvgRows_SaysNoExecutionWasRecorded()
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", muted =>
        {
            var found = new List<string>();
            var tab = new FinOpsTab();
            var window = Open(tab, 1000, 760);
            try
            {
                var total = Find<DataGrid>(tab, "TopTotalGrid");
                var avg = Find<DataGrid>(tab, "TopAvgGrid");
                total.ItemsSource = new List<TopResourceConsumerRow> { new() { DatabaseName = "Sales", CpuTimeMs = 0, ExecutionCount = 0, IoTotalMb = 12m } };
                avg.ItemsSource = new List<TopResourceConsumerRow>();
                var card = Find<FrameworkElement>(tab, "TopAvgCard");
                StaTestThread.Reveal(card);
                StaTestThread.Settle(window);

                var message = Descendants(card).OfType<TextBlock>().FirstOrDefault(t => t.Text == NoExecutions);
                if (message is null)
                {
                    found.Add("the Avg card shows no sentence saying why it is empty; its visible text is: " + string.Join(" | ", Descendants(card).OfType<TextBlock>().Where(t => t.IsVisible && t.Text.Length > 0).Select(t => t.Text)));
                    return found;
                }

                if (!message.IsVisible) found.Add("the sentence is on the card but not visible");
                if (message.Foreground is not SolidColorBrush brush || brush.Color != muted) found.Add($"the sentence is painted {message.Foreground}, not the theme's muted colour");

                /* Rows in both cards: the sentence goes away. */
                avg.ItemsSource = new List<TopResourceConsumerRow> { new() { DatabaseName = "Sales", AvgCpuMs = 1.5m, ExecutionCount = 10 } };
                StaTestThread.Settle(window);
                if (message.IsVisible) found.Add("the sentence stays visible once the Avg card has rows");
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The collapsed "CPU by database" expander above the Top Queries grid reads as a control, not as the grid's title: its header says what
    /// it does, and a gap separates it from the grid's column headers.
    /// </summary>
    [Fact]
    public void TheCpuByDatabaseExpander_ReadsAsAControl_NotAsTheGridTitle()
    {
        var problems = WithServerTab("DarkTheme", tab =>
        {
            var found = new List<string>();
            var cards = Find<FrameworkElement>(tab, "QueryStatsByDbCards");
            var expander = Ancestors(cards).OfType<Expander>().FirstOrDefault();
            if (expander is null)
            {
                found.Add("QueryStatsByDbCards is not inside an Expander");
                return found;
            }

            var grid = Find<DataGrid>(tab, "QueryStatsGrid");
            StaTestThread.Reveal(grid);
            StaTestThread.Settle((Window)Window.GetWindow(tab));

            if (expander.Header is not string header || !header.StartsWith("Show ", StringComparison.Ordinal))
            {
                found.Add($"the expander header is \"{expander.Header}\", which reads as a title, not an action");
            }

            var expanderBottom = expander.TransformToAncestor(tab).Transform(new Point(0, expander.ActualHeight)).Y;
            var gridTop = grid.TransformToAncestor(tab).Transform(new Point(0, 0)).Y;
            if (gridTop - expanderBottom < 6)
            {
                found.Add($"the grid's headers start {gridTop - expanderBottom:0.#} px under the expander, so the two read as one block");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Seeds one server_properties row for the tab's server with <paramref name="productVersion"/>, lets the tab learn the server's engine
    /// facts the way a tab load does, binds an empty list to both grids, and reads the sentence each shows.
    /// </summary>
    private static async Task<(string? AutomaticTuning, string? PlanCorrections)> EmptyTextsAfterRefresh(string productVersion)
    {
        var dir = Path.Combine(Path.GetTempPath(), "OlderSku_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            await duckDb.InitializeAsync();
            var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "older-sku-test", DisplayName = "older-sku-test", IsEnabled = true };
            var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
            await SeedServerPropertiesAsync(duckDb, serverId, productVersion);

            (string?, string?) texts = default;
            StaTestThread.Run(async () =>
            {
                await Task.Yield();
                texts = await WithThemeAsync("DarkTheme", async () =>
                {
                    var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                    typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                    var window = Open(tab, 1000, 800);
                    try
                    {
                        await (Task)typeof(ServerTab).GetMethod("RefreshEngineEditionAsync", Instance)!.Invoke(tab, null)!;
                        BindEmpty(tab, "_automaticTuningFilterMgr");
                        BindEmpty(tab, "_planCorrectionFilterMgr");
                        var tuning = Find<DataGrid>(tab, "AutomaticTuningGrid");
                        var corrections = Find<DataGrid>(tab, "PlanCorrectionGrid");
                        StaTestThread.Reveal(tuning);
                        StaTestThread.Settle(window);
                        var tuningText = ShownEmptyText(tuning);
                        StaTestThread.Reveal(corrections);
                        StaTestThread.Settle(window);
                        return (tuningText, ShownEmptyText(corrections));
                    }
                    finally
                    {
                        window.Close();
                    }
                });
            });

            return texts;
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static async Task SeedServerPropertiesAsync(DuckDbInitializer duckDb, int serverId, string productVersion)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES (-1, $1, $2, 'older-sku-test', 'Enterprise Edition', $3, 'RTM', 3, 8, 1, 16384, 0, 'UTC')";
        command.Parameters.Add(new DuckDBParameter { Value = At });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = productVersion });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>Binds an empty list through the grid's filter manager, the way a load with no rows does.</summary>
    private static void BindEmpty(ServerTab tab, string managerField)
    {
        var manager = typeof(ServerTab).GetField(managerField, Instance)!.GetValue(tab) ?? throw new Xunit.Sdk.XunitException(managerField + " is null");
        var update = manager.GetType().GetMethod("UpdateData") ?? throw new Xunit.Sdk.XunitException(managerField + " has no UpdateData");
        var rowType = update.GetParameters()[0].ParameterType.GetGenericArguments()[0];
        update.Invoke(manager, new[] { Activator.CreateInstance(typeof(List<>).MakeGenericType(rowType)) });
    }

    /// <summary>The visible empty-state sentence inside <paramref name="grid"/>'s cell, or null when the grid shows none.</summary>
    private static string? ShownEmptyText(DataGrid grid)
    {
        if (grid.Parent is not Grid parent) return null;
        var text = parent.Children.OfType<TextBlock>().FirstOrDefault(t => t.Name == EmptyState.ElementName + "_" + grid.Name);
        return text is { Visibility: Visibility.Visible } ? text.Text : null;
    }

    private static T WithServerTab<T>(string theme, Func<ServerTab, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "OlderSku_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(theme, _ =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "older-sku-test", DisplayName = "older-sku-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = Open(tab, 1000, 800);
                try
                {
                    return body(tab);
                }
                finally
                {
                    window.Close();
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static Window Open(FrameworkElement content, int width, int height)
    {
        var window = new Window { Content = content, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
        StaTestThread.ShowOffScreen(window);
        StaTestThread.Settle(window);
        return window;
    }

    private static T Find<T>(FrameworkElement root, string name) where T : FrameworkElement =>
        root.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the control");

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var grandChild in Descendants(child)) yield return grandChild;
        }
    }

    private static IEnumerable<DependencyObject> Ancestors(DependencyObject element)
    {
        for (var at = LogicalTreeHelper.GetParent(element); at is not null; at = LogicalTreeHelper.GetParent(at)) yield return at;
    }

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> and the shared styles as the app's resources, and gives it the theme's muted colour.</summary>
    private static T WithTheme<T>(string theme, Func<Color, T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
        try
        {
            return body(((SolidColorBrush)app.FindResource("ForegroundMutedBrush")).Color);
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var dictionary in saved)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }
    }

    private static async Task<T> WithThemeAsync<T>(string theme, Func<Task<T>> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
        try
        {
            return await body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var dictionary in saved)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }
    }
}
