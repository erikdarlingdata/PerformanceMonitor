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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Runs a saved Custom View (#6390 S6). Each composed panel goes through <see cref="ViewerDataService.RunComposedPanelAsync"/>, which is
/// <c>ComposePanelRunner.RunComposedPanelCoreAsync</c> (Storage), the call the web makes. A table panel is a grid, a line panel a
/// chart in the viewer's chart theme, a stat panel a tile, a bandlist a list; a notebook shows its markdown cells as text between its
/// composed cells. A v1 read panel is not run (the 82-read dispatch is in the service only): it says to open the web dashboard. A
/// refusal or error shows the runner's own sentence on that panel.
/// </summary>
public partial class CustomViewRunWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly CustomView _view;
    private CancellationTokenSource? _cts;

    private sealed record ServerChoice(string? Name, string Display);

    public CustomViewRunWindow(ViewerDataService dataService, CustomView view)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _view = view ?? throw new ArgumentNullException(nameof(view));
        ViewTitle.Text = view.Name;
        Title = $"Run Custom View - {view.Name}";
        Closing += (_, _) => _cts?.Cancel();
        Loaded += async (_, _) => await StartAsync();
    }

    private async Task StartAsync()
    {
        var choices = new List<ServerChoice> { new(null, "All servers") };
        try
        {
            choices.AddRange((await _dataService.GetPickerServersAsync()).Select(s => new ServerChoice(s.ServerName, s.DisplayName)));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            RunStatus.Text = $"The server list could not be read: {ex.Message}";
        }

        ServerBox.ItemsSource = choices;
        ServerBox.SelectedIndex = 0;
        await RunAsync();
    }

    private async void Run_Click(object sender, RoutedEventArgs e) => await RunAsync();

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private async Task RunAsync()
    {
        _cts?.Cancel();
        var cts = _cts = new CancellationTokenSource();
        var server = (ServerBox.SelectedItem as ServerChoice)?.Name;
        var (items, error) = ViewerCustomViewRun.Plan(_view.DefinitionJson, server);
        Panels.Children.Clear();
        if (error is not null)
        {
            RunStatus.Text = error;
            return;
        }

        RunButton.IsEnabled = false;
        RunStatus.Text = "Running...";
        try
        {
            foreach (var item in items)
            {
                var host = new StackPanel();
                Panels.Children.Add(Card(item.Title, host));
                if (item.Kind == ViewerRunItemKind.Markdown)
                {
                    host.Children.Add(new TextBlock { Text = item.Text, TextWrapping = TextWrapping.Wrap });
                }
                else if (item.Kind == ViewerRunItemKind.ReadPanel)
                {
                    host.Children.Add(Muted(item.Text!));
                }
                else
                {
                    host.Children.Add(Muted("Running..."));
                    var run = await _dataService.RunComposedPanelAsync(item.Body!, cts.Token);
                    host.Children.Clear();
                    Show(host, item, run);
                }
            }

            RunStatus.Text = $"{items.Count(i => i.Kind == ViewerRunItemKind.Composed)} panel(s) run.";
        }
        catch (OperationCanceledException)
        {
            /* The window closed, or Run was pressed again. */
        }
        finally
        {
            if (ReferenceEquals(_cts, cts))
            {
                RunButton.IsEnabled = true;
            }
        }
    }

    private void Show(StackPanel host, ViewerRunItem item, ViewerPanelRun run)
    {
        if (!run.Ok)
        {
            host.Children.Add(new TextBlock { Text = run.Error, TextWrapping = TextWrapping.Wrap, Foreground = (Brush)FindResource("WarningTextBrush") });
            return;
        }

        var rows = run.Payload!["rows"] as JsonArray ?? new JsonArray();
        if (rows.Count == 0)
        {
            host.Children.Add(Muted("No data in this window."));
        }
        else if (item.Viz == "stat")
        {
            host.Children.Add(new TextBlock { Text = ViewerCustomViewRun.StatText(rows), FontSize = 32, FontWeight = FontWeights.Bold });
        }
        else if (item.Viz == "bandlist")
        {
            var table = ViewerCustomViewRun.ToTable(rows);
            foreach (System.Data.DataRow row in table.Rows)
            {
                host.Children.Add(new TextBlock { Text = "● " + string.Join("  ", row.ItemArray.Select(c => c is DBNull ? "" : Convert.ToString(c))), Margin = new Thickness(0, 2, 0, 2) });
            }
        }
        else if (ViewerCustomViewRun.IsLineFamily(item.Viz) && ViewerCustomViewRun.Lines(rows) is { Count: > 0 } lines)
        {
            host.Children.Add(Chart(lines));
            if (ViewerCustomViewRun.SeriesCapNotice(lines.Count) is { } capNotice)
            {
                host.Children.Add(Muted(capNotice));
            }
        }
        else
        {
            host.Children.Add(Grid(rows));
        }

        if (run.Payload["notice"] is JsonValue notice && notice.TryGetValue<string>(out var text) && text.Length > 0)
        {
            host.Children.Add(Muted(text));
        }
    }

    private DataGrid Grid(JsonArray rows) => new()
    {
        Style = (Style)FindResource("DarkGrid"),
        ItemsSource = ViewerCustomViewRun.ToTable(rows).DefaultView,
        IsReadOnly = true,
        MaxHeight = 360,
        AutoGenerateColumns = true,
    };

    private static ScottPlot.WPF.WpfPlot Chart(IReadOnlyList<(string Name, IReadOnlyList<(DateTime Bucket, double Value)> Points)> lines)
    {
        var chart = new ScottPlot.WPF.WpfPlot { Height = 280 };
        var palette = ChartPalette.CyclingPalette.ToArray();
        var index = 0;
        foreach (var (name, points) in lines.Take(ViewerCustomViewRun.MaxSeries))
        {
            var scatter = chart.Plot.Add.TimeSeries(points.Select(p => p.Bucket.ToOADate()).ToArray(), points.Select(p => p.Value).ToArray());
            scatter.Color = ScottPlot.Color.FromHex(palette[index++ % palette.Length]);
            scatter.LegendText = name;
            ChartStyle.StyleScatter(scatter);
        }

        chart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.FleetWideZone);
        ChartStyle.ApplyThemeToChart(chart);
        chart.Plot.ShowLegend();
        chart.Refresh();
        return chart;
    }

    private Border Card(string title, UIElement body)
    {
        var stack = new StackPanel();
        if (!string.IsNullOrWhiteSpace(title))
        {
            stack.Children.Add(new TextBlock { Text = title, FontWeight = FontWeights.SemiBold, Margin = new Thickness(0, 0, 0, 6) });
        }

        stack.Children.Add(body);
        return new Border
        {
            Child = stack,
            Padding = new Thickness(10),
            Margin = new Thickness(0, 0, 0, 10),
            BorderThickness = new Thickness(1),
            BorderBrush = (Brush)FindResource("BorderBrush"),
        };
    }

    private TextBlock Muted(string text) => new()
    {
        Text = text,
        TextWrapping = TextWrapping.Wrap,
        Foreground = (Brush)FindResource("ForegroundMutedBrush"),
    };
}
