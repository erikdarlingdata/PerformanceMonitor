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
using PerformanceMonitorLite.Services;

using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// #1319: the per-server global database filter (display-only). A toolbar button opens a popup of
/// collected-database checkboxes (search + Select/Clear All + count), mirroring the wait-type picker.
/// Selection is applied when the popup closes — persisted per-server (servers.json) and threaded into
/// every database-scoped reader. Empty selection = "All" (today's unfiltered behavior).
/// </summary>
public partial class ServerTab : UserControl
{
    private int _databaseFilterTotalCount;
    private bool _databaseFilterDirty;

    private async void DatabaseFilterButton_Click(object sender, RoutedEventArgs e)
    {
        if (DatabaseFilterPopup.IsOpen)
        {
            DatabaseFilterPopup.IsOpen = false;
            return;
        }

        await PopulateDatabaseFilterPickerAsync();
        DatabaseFilterPopup.IsOpen = true;
    }

    /// <summary>
    /// (Re)loads the collected user-database list on each open so newly-collected databases appear.
    /// Any currently-selected database missing from the collected list is kept (still checked) so a
    /// sticky filter for a not-yet-collected database survives.
    /// </summary>
    private async Task PopulateDatabaseFilterPickerAsync()
    {
        List<string> names;
        try
        {
            names = await Task.Run(() => _dataService.GetCollectedDatabaseNamesAsync(_serverId));
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] GetCollectedDatabaseNamesAsync failed: {ex.Message}");
            names = new List<string>();
        }

        _databaseFilterTotalCount = names.Count;

        var union = new List<string>(names);
        foreach (var sel in _selectedDatabases)
        {
            if (!union.Contains(sel, StringComparer.OrdinalIgnoreCase))
            {
                union.Add(sel);
            }
        }
        union.Sort(StringComparer.OrdinalIgnoreCase);

        _databaseFilterItems = union
            .Select(n => new SelectableItem { DisplayName = n, IsSelected = _selectedDatabases.Contains(n) })
            .ToList();

        if (DatabaseFilterSearchBox != null)
        {
            DatabaseFilterSearchBox.Text = "";
        }
        ApplyDatabaseFilterSearch();
        UpdateDatabaseFilterLabel();
    }

    private void ApplyDatabaseFilterSearch()
    {
        var search = DatabaseFilterSearchBox?.Text?.Trim() ?? "";
        DatabaseFilterList.ItemsSource = null;
        DatabaseFilterList.ItemsSource = string.IsNullOrEmpty(search)
            ? _databaseFilterItems
            : _databaseFilterItems.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
    }

    private void DatabaseFilterSearch_TextChanged(object sender, TextChangedEventArgs e) => ApplyDatabaseFilterSearch();

    private void DatabaseFilter_CheckChanged(object sender, RoutedEventArgs e)
    {
        if (_isUpdatingDatabaseFilterSelection)
        {
            return;
        }
        SyncSelectedDatabasesFromItems();
    }

    private void DatabaseFilterSelectAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingDatabaseFilterSelection = true;
        foreach (var item in _databaseFilterItems)
        {
            item.IsSelected = true;
        }
        _isUpdatingDatabaseFilterSelection = false;
        ApplyDatabaseFilterSearch();
        SyncSelectedDatabasesFromItems();
    }

    private void DatabaseFilterClearAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingDatabaseFilterSelection = true;
        foreach (var item in _databaseFilterItems)
        {
            item.IsSelected = false;
        }
        _isUpdatingDatabaseFilterSelection = false;
        ApplyDatabaseFilterSearch();
        SyncSelectedDatabasesFromItems();
    }

    private void SyncSelectedDatabasesFromItems()
    {
        /* Release walk V12c: every box ticked (Select All, or the last box ticked by hand) is "All", not a real filter that
           names every database collected so far and leaves out the next one. */
        _selectedDatabases.Clear();
        foreach (var name in DatabaseFilterSelection.Stored(_databaseFilterItems.Select(i => (i.DisplayName, i.IsSelected)).ToList()))
        {
            _selectedDatabases.Add(name);
        }
        _databaseFilterDirty = true;
        UpdateDatabaseFilterLabel();
    }

    private void UpdateDatabaseFilterLabel()
    {
        if (DatabaseFilterButton == null)
        {
            return;
        }

        int selected = _selectedDatabases.Count;
        DatabaseFilterButton.Content = selected == 0
            ? "Databases: All"
            : $"Databases: {selected} of {Math.Max(selected, _databaseFilterTotalCount)}";

        if (DatabaseFilterCountText != null)
        {
            DatabaseFilterCountText.Text = selected == 0 ? "All databases" : $"{selected} selected";
        }
    }

    /// <summary>
    /// Applies the selection when the popup closes (rather than on every checkbox toggle) so a
    /// multi-database change triggers a single reload. Persists per-server, then reloads the visible tab.
    /// </summary>
    private async void DatabaseFilterPopup_Closed(object sender, EventArgs e)
    {
        if (!_databaseFilterDirty)
        {
            return;
        }
        _databaseFilterDirty = false;

        _server.ViewFilterDatabases = _selectedDatabases.ToList();
        PersistServerRequested?.Invoke(_server);

        await RefreshAllDataAsync();
    }
}
