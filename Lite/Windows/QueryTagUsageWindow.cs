/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// #5993 part B: "Resource use by tag" for one server or all servers. One table per section (plan cache, Query Store, procedures,
/// blocking reports, blocking snapshots, deadlocks), never added together. A read-only view over <see cref="LocalDataService.GetQueryTagRollupDataAsync"/>
/// and <see cref="QueryTagRollup"/> (what <c>get_resource_use_by_tag</c> answers from), shaped by <see cref="QueryTagUsageView"/>.
/// </summary>
public sealed class QueryTagUsageWindow : Window
{
    private readonly LocalDataService _data;
    private readonly (int Id, string Name) _here;
    private readonly List<(int Id, string Name)> _fleet;
    private readonly ComboBox _scope = new() { Width = 130, Margin = new Thickness(0, 0, 8, 0) };
    private readonly ComboBox _hours = new() { Width = 110, Margin = new Thickness(0, 0, 8, 0) };
    private readonly ComboBox _tag = new() { Width = 220, Margin = new Thickness(0, 0, 8, 0) };
    private readonly TextBlock _message = new() { TextWrapping = TextWrapping.Wrap, Margin = new Thickness(0, 8, 0, 4) };
    private readonly StackPanel _tables = new();
    private List<QueryTagRow> _tags = [];
    private bool _loading;

    public QueryTagUsageWindow(LocalDataService data, (int Id, string Name) here, List<(int Id, string Name)> fleet)
    {
        _data = data;
        _here = here;
        _fleet = fleet;
        Title = "Resource use by query tag";
        Width = 900;
        Height = 650;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;
        SetResourceReference(BackgroundProperty, "BackgroundDarkBrush");
        _message.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundBrush");

        _scope.ItemsSource = new[] { "This server", "All servers" };
        _scope.SelectedIndex = 0;
        _hours.ItemsSource = new[] { "Last 1 hour", "Last 24 hours", "Last 7 days" };
        _hours.SelectedIndex = 1;
        _tag.ItemsSource = new[] { QueryTagGrid.AllChoice };
        _tag.SelectedIndex = 0;
        var refresh = new Button { Content = "Refresh", Padding = new Thickness(10, 3, 10, 3) };
        refresh.Click += async (_, _) => await LoadAsync();

        var bar = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 0, 0, 4) };
        foreach (var element in new UIElement[] { _scope, _hours, _tag, refresh })
        {
            bar.Children.Add(element);
        }

        var root = new DockPanel { Margin = new Thickness(12) };
        DockPanel.SetDock(bar, Dock.Top);
        DockPanel.SetDock(_message, Dock.Top);
        root.Children.Add(bar);
        root.Children.Add(_message);
        root.Children.Add(new ScrollViewer { Content = _tables, VerticalScrollBarVisibility = ScrollBarVisibility.Auto });
        Content = root;

        _scope.SelectionChanged += async (_, _) => await LoadAsync();
        _hours.SelectionChanged += async (_, _) => await LoadAsync();
        _tag.SelectionChanged += async (_, _) => await LoadAsync();
        Loaded += async (_, _) => await LoadAsync();
    }

    private async Task LoadAsync()
    {
        if (_loading)
        {
            return;
        }

        _loading = true;
        try
        {
            var allServers = _scope.SelectedIndex == 1;
            var hours = _hours.SelectedIndex switch { 0 => 1, 2 => 168, _ => 24 };
            var scope = allServers ? _fleet : [_here];
            var end = DateTime.UtcNow;
            var data = await Task.Run(() => _data.GetQueryTagRollupDataAsync(scope.Select(s => s.Id).ToList(), end.AddHours(-hours), end));

            if (_tag.Items.Count <= 1)
            {
                _tags = data.Tags.ToList();
                var choices = QueryTagGrid.FilterChoices(_tags).Where(c => c != QueryTagGrid.UntaggedChoice).ToList();
                var keep = _tag.SelectedItem as string;
                _tag.ItemsSource = choices;
                _tag.SelectedItem = keep is not null && choices.Contains(keep) ? keep : QueryTagGrid.AllChoice;
            }

            int? filter = null;
            if (_tag.SelectedItem is string picked && picked != QueryTagGrid.AllChoice)
            {
                var paths = QueryTagGrid.PathsOf(data.Tags);
                filter = paths.Where(p => p.Value == picked).Select(p => (int?)p.Key).FirstOrDefault();
            }

            var result = QueryTagUsageView.Build(data, scope, allServers, filter);
            Render(result);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _message.Text = $"Could not read resource use by tag: {ex.Message}";
        }
        finally
        {
            _loading = false;
        }
    }

    private void Render(QueryTagUsageResult result)
    {
        _message.Text = result.Message ?? "Each table is its own source and is never added to another: the plan cache and Query Store count the same executions.";
        _tables.Children.Clear();
        foreach (var table in result.Tables)
        {
            var title = new TextBlock { Text = table.Title + (table.Status == "ok" ? "" : $"  ({table.Status})"), FontWeight = FontWeights.SemiBold, Margin = new Thickness(0, 10, 0, 2) };
            title.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundBrush");
            _tables.Children.Add(title);
            if (table.Message is not null && table.Status != "ok")
            {
                var why = new TextBlock { Text = table.Message, TextWrapping = TextWrapping.Wrap, Margin = new Thickness(0, 0, 0, 2) };
                why.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundDimBrush");
                _tables.Children.Add(why);
            }

            var grid = new DataGrid
            {
                AutoGenerateColumns = false, IsReadOnly = true, HeadersVisibility = DataGridHeadersVisibility.Column,
                CanUserSortColumns = false, ItemsSource = table.Rows, MaxHeight = 260,
            };
            grid.Columns.Add(new DataGridTextColumn { Header = table.Columns[0], Binding = new System.Windows.Data.Binding("Label"), Width = 240 });
            for (var i = 1; i < table.Columns.Count; i++)
            {
                grid.Columns.Add(new DataGridTextColumn
                {
                    Header = table.Columns[i], Binding = new System.Windows.Data.Binding($"Cells[{i - 1}]"), Width = DataGridLength.Auto,
                });
            }

            _tables.Children.Add(grid);
            if (table.Note is not null)
            {
                var note = new TextBlock { Text = table.Note, TextWrapping = TextWrapping.Wrap, FontSize = 11 };
                note.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundDimBrush");
                _tables.Children.Add(note);
            }
        }
    }
}
