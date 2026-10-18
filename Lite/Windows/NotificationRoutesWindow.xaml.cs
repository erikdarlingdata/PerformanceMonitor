/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// The scoped notification routes manager (#5750): the list, plus Add, Edit, Toggle and Delete over
/// <see cref="DuckDbNotificationRouteStore"/>. Destinations live in Windows Credential Manager and are never shown here,
/// only a set or not-set marker per channel. Every write first asks <see cref="PendingRestoreNotice.Refuse"/>, because a
/// pending archive restore of the <c>notification_routes</c> table would otherwise be overwritten by the restore.
/// </summary>
public partial class NotificationRoutesWindow : Window
{
    /// <summary>The table name a pending restore is checked against (the store's own).</summary>
    private const string RestoreTable = "notification_routes";

    private readonly DuckDbNotificationRouteStore _store;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly ObservableCollection<NotificationRouteRow> _rows = new();
    private IReadOnlyList<RouteScopeOption> _scopeOptions = Array.Empty<RouteScopeOption>();

    internal NotificationRoutesWindow(DuckDbNotificationRouteStore store, LocalDataService dataService, ServerManager serverManager)
    {
        InitializeComponent();
        _store = store;
        _dataService = dataService;
        _serverManager = serverManager;

        /* Newest route first: the grid sorts descending by route id until the user clicks another header. */
        var view = (ListCollectionView)CollectionViewSource.GetDefaultView(_rows);
        view.SortDescriptions.Add(new SortDescription(nameof(NotificationRouteRow.RouteId), ListSortDirection.Descending));
        RoutesGrid.ItemsSource = view;

        Loaded += async (_, _) => await LoadRoutesAsync();
    }

    private async Task LoadRoutesAsync()
    {
        try
        {
            var routes = await _store.ListAsync();
            var tags = await Task.Run(() => _dataService.GetServerTagsAsync());
            var servers = _serverManager.GetAllServers()
                .Select(s => (Id: RemoteCollectorService.GetServerId(s), Name: ServerManager.NameForMessage(s)))
                .GroupBy(s => s.Id)
                .Select(g => g.First())
                .ToList();

            _scopeOptions = NotificationRouteUi.ScopeOptions(servers, tags);
            var serverNames = servers.ToDictionary(s => s.Id, s => s.Name);
            var tagPaths = NotificationRouteUi.TagPaths(tags);

            _rows.Clear();
            foreach (var route in routes)
            {
                _rows.Add(new NotificationRouteRow(route, serverNames, tagPaths));
            }

            NoRoutesMessage.Visibility = _rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            ShowError("Could not read the notification routes", ex);
        }
    }

    private bool RefuseWhileRestorePending() => PendingRestoreNotice.Refuse(RestoreTable, this);

    private async void AddRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseWhileRestorePending()) return;
        var dialog = new NotificationRouteEditDialog(
            _scopeOptions, async route => { await _store.InsertAsync(route); return true; }) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            await LoadRoutesAsync();
        }
    }

    private async void EditRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseWhileRestorePending()) return;
        if (RoutesGrid.SelectedItem is not NotificationRouteRow selected) return;
        var dialog = new NotificationRouteEditDialog(
            _scopeOptions, route => _store.UpdateAsync(route), selected.Route,
            generateKey: id => _store.GenerateSigningKeyAsync(id), removeKey: id => _store.RemoveSigningKeyAsync(id)) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            await LoadRoutesAsync();
        }
    }

    private async void ToggleRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseWhileRestorePending()) return;
        if (RoutesGrid.SelectedItem is not NotificationRouteRow selected) return;
        try
        {
            await _store.SetEnabledAsync(selected.RouteId, !selected.Enabled);
        }
        catch (Exception ex)
        {
            ShowError("Could not change the route", ex);
        }

        await LoadRoutesAsync();
    }

    private async void DeleteRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseWhileRestorePending()) return;
        if (RoutesGrid.SelectedItem is not NotificationRouteRow selected) return;

        var result = MessageBox.Show(this,
            $"Delete route {selected.RouteId}?\n\n{selected.MetricMatch} for {selected.ScopeDisplay}\n\n" +
            "Its saved destinations are removed from Windows Credential Manager. The alerts it matched fall back to a less specific route, if any, then to the app-wide destinations.",
            "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (result != MessageBoxResult.Yes) return;

        /* A restore can start while the confirmation is open; check again so the message shows now. */
        if (RefuseWhileRestorePending()) return;
        try
        {
            await _store.DeleteAsync(selected.RouteId);
        }
        catch (Exception ex)
        {
            ShowError("Could not delete the route", ex);
        }

        await LoadRoutesAsync();
    }

    private async void EnabledCheckBox_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not CheckBox cb || cb.DataContext is not NotificationRouteRow row) return;

        /* The two-way binding already flipped row.Enabled; put it and the box back when the write is refused. */
        var wanted = cb.IsChecked == true;
        if (RefuseWhileRestorePending())
        {
            row.Enabled = !wanted;
            cb.IsChecked = !wanted;
            return;
        }

        try
        {
            if (!await _store.SetEnabledAsync(row.RouteId, wanted))
            {
                ShowMessage("This route no longer exists.");
            }
        }
        catch (Exception ex)
        {
            ShowError("Could not change the route", ex);
        }

        await LoadRoutesAsync();
    }

    private void RoutesGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        /* Only a double-click on a row edits; one on the header or the empty area does nothing. */
        if (RoutesGrid.SelectedItem is NotificationRouteRow
            && e.OriginalSource is DependencyObject source
            && FindAncestor<DataGridRow>(source) is not null)
        {
            EditRoute_Click(sender, e);
        }
    }

    private static T? FindAncestor<T>(DependencyObject from) where T : DependencyObject
    {
        for (var current = from; current != null; current = System.Windows.Media.VisualTreeHelper.GetParent(current))
        {
            if (current is T match) return match;
        }

        return null;
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowMessage(string text) =>
        MessageBox.Show(this, text, "Notification Routes", MessageBoxButton.OK, MessageBoxImage.Warning);

    private void ShowError(string what, Exception ex) => ShowMessage($"{what}: {ex.Message}");
}
