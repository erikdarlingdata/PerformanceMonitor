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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #5988 part B: "Resource use by tag" for one server or all servers, the twin of Lite's <c>QueryTagUsageWindow</c>. One table per section (plan cache,
/// Query Store, procedures, blocking reports, blocking snapshots, deadlocks), never added together. Read-only: the figures come from
/// <see cref="ViewerDataService.GetQueryTagRollupDataAsync"/>, shaped by <see cref="QueryTagUsageView"/>.
/// </summary>
public sealed class QueryTagUsageWindow : Window
{
    private readonly ViewerDataService _data;
    private readonly (int Id, string Name) _here;
    private readonly Func<Task<List<(int Id, string Name)>>> _fleet;
    private readonly ComboBox _scope = new() { Width = 130, Margin = new Thickness(0, 0, 8, 0) };
    private readonly ComboBox _hours = new() { Width = 110, Margin = new Thickness(0, 0, 8, 0) };
    private readonly ComboBox _tag = new() { Width = 220, Margin = new Thickness(0, 0, 8, 0) };
    private readonly TextBlock _message = new() { TextWrapping = TextWrapping.Wrap, Margin = new Thickness(0, 8, 0, 4) };
    private readonly StackPanel _tables = new();
    private IReadOnlyList<Storage.QueryTagNode> _tags = [];
    private bool _loading;

    public QueryTagUsageWindow(ViewerDataService data, (int Id, string Name) here, Func<Task<List<(int Id, string Name)>>> fleet)
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
        _tag.ItemsSource = new[] { QueryTagUsageView.AllChoice };
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
            var scope = allServers ? await _fleet() : [_here];
            var end = DateTime.UtcNow;
            var data = await _data.GetQueryTagRollupDataAsync(scope.Select(s => s.Id).ToList(), end.AddHours(-hours), end);

            if (_tag.Items.Count <= 1)
            {
                _tags = data.Tags;
                var choices = QueryTagUsageView.Choices(_tags);
                _tag.ItemsSource = choices;
                _tag.SelectedItem = QueryTagUsageView.AllChoice;
            }

            var filter = QueryTagUsageView.FilterFor(data.Tags, _tag.SelectedItem as string);
            Render(QueryTagUsageView.Build(data, scope, allServers, filter));
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
                why.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundMutedBrush");
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
                note.SetResourceReference(TextBlock.ForegroundProperty, "ForegroundMutedBrush");
                _tables.Children.Add(note);
            }
        }
    }
}
