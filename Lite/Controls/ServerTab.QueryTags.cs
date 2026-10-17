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
using PerformanceMonitorLite.Windows;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #5993 part B: the query-tag filter in the header, the "Assign query tag..." menu item on the Queries grids and the
/// "Resource use by tag" window. The tag column and the filter come from the #5992 readers (see <see cref="QueryTagGrid"/>);
/// this file only wires them to the tab.
/// </summary>
public partial class ServerTab
{
    /// <summary>The tag filter choice the Queries grids read with: null (all), "Untagged" or a tag path.</summary>
    private string? _queryTagChoice;
    private bool _queryTagChoicesFilling;

    private async void QueryTagFilterCombo_DropDownOpened(object sender, EventArgs e)
    {
        try
        {
            var tags = await Task.Run(() => _dataService.GetQueryTagsAsync());
            var choices = QueryTagGrid.FilterChoices(tags);
            _queryTagChoicesFilling = true;
            QueryTagFilterCombo.ItemsSource = choices;
            QueryTagFilterCombo.SelectedItem = choices.Contains(_queryTagChoice ?? QueryTagGrid.AllChoice) ? _queryTagChoice ?? QueryTagGrid.AllChoice : QueryTagGrid.AllChoice;
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] query tag list failed: {ex.Message}");
        }
        finally
        {
            _queryTagChoicesFilling = false;
        }
    }

    private async void QueryTagFilterCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_queryTagChoicesFilling || QueryTagFilterCombo.SelectedItem is not string choice)
        {
            return;
        }

        var next = choice == QueryTagGrid.AllChoice ? null : choice;
        if (next == _queryTagChoice)
        {
            return;
        }

        _queryTagChoice = next;
        await RefreshAllDataAsync();
    }

    private async void AssignQueryTag_Click(object sender, RoutedEventArgs e)
    {
        var grid = FindParentDataGrid(sender);
        var target = QueryTagGrid.TargetOf(grid?.CurrentItem);
        var owner = Window.GetWindow(this);
        if (target is null)
        {
            return;
        }

        try
        {
            var tags = await Task.Run(() => _dataService.GetQueryTagsAsync());
            if (tags.Count == 0)
            {
                MessageBox.Show(owner, "There are no query tags yet. Create one in Manage Query Tags first.", "Assign query tag",
                    MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var paths = QueryTagGrid.PathsOf(tags);
            var dialog = new QueryTagPickDialog($"Assign {target.Display} to a query tag:", paths.OrderBy(p => p.Value, StringComparer.OrdinalIgnoreCase).ToList()) { Owner = owner };
            if (dialog.ShowDialog() != true || dialog.SelectedTagId is not int tagId)
            {
                return;
            }

            await Task.Run(() => QueryTagGrid.AssignAsync(_dataService, _serverId, tagId, target));
            MessageBox.Show(owner, QueryTagGrid.AssignedMessage(target, paths[tagId]), "Assign query tag",
                MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (TimeoutException ex)
        {
            MessageBox.Show(owner, StoreBusyReport.Report("ServerTab", "assign query tag", ex, "assigning the query tag"), "Assign query tag",
                MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MessageBox.Show(owner, $"Could not assign the query tag: {ex.Message}", "Assign query tag",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void ResourceUseByTagButton_Click(object sender, RoutedEventArgs e)
    {
        var servers = new ServerManager(App.SharedConfigDirectory).GetEnabledServers()
            .Select(s => (RemoteCollectorService.GetServerId(s), RemoteCollectorService.GetServerNameForStorage(s))).ToList();
        var here = (_serverId, RemoteCollectorService.GetServerNameForStorage(_server));
        new QueryTagUsageWindow(_dataService, here, servers) { Owner = Window.GetWindow(this) }.Show();
    }
}
