/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's notification-routes manager (#3598) — <see cref="MuteRulesWindow"/>'s shape over
/// <c>config.config_notification_routes</c>: Add/Edit/Toggle/Delete, each write awaited and the list reloaded
/// from the store so the grid always shows what was persisted. Opened from the Settings window's
/// Notifications section, beside the parent channel fields the routes layer over. The store is the
/// coordination point: the V131 trigger bumps <c>config_version</c> on every write here and the running
/// service re-reads the routes on its next sweep.
/// </summary>
public partial class NotificationRoutesWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly ObservableCollection<NotificationRouteRow> _routes = new();

    /// <summary>False on the read-only <c>viewer</c> role — bound by the Enabled checkbox's <c>IsEnabled</c>.
    /// Set once at construction (the connection's role does not change while the window is open).</summary>
    public bool RoutesAreEditable { get; }

    public NotificationRoutesWindow(ViewerDataService dataService)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        RoutesAreEditable = !dataService.IsReadOnly;

        if (dataService.IsReadOnly)
        {
            ActionButtonsPanel.Visibility = Visibility.Collapsed;
            ReadOnlyBanner.Visibility = Visibility.Visible;
        }

        RoutesGrid.ItemsSource = _routes;
        Loaded += async (_, _) => await LoadRoutesAsync();
    }

    private async System.Threading.Tasks.Task LoadRoutesAsync()
    {
        try
        {
            var routes = await _dataService.GetNotificationRoutesAsync();
            _routes.Clear();
            foreach (var route in routes)
            {
                _routes.Add(route);
            }

            NoRoutesMessage.Visibility = _routes.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            ShowError("Could not read notification routes", ex);
        }
    }

    private async void AddRoute_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new NotificationRouteEditDialog { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            await _dataService.InsertNotificationRouteAsync(dialog.Route);
            await LoadRoutesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not save the route", ex);
        }
    }

    private async void EditRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RoutesGrid.SelectedItem is not NotificationRouteRow selected)
        {
            return;
        }

        var dialog = new NotificationRouteEditDialog(selected) { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            await _dataService.UpdateNotificationRouteAsync(dialog.Route);
            await LoadRoutesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not update the route", ex);
        }
    }

    private async void ToggleRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RoutesGrid.SelectedItem is not NotificationRouteRow selected)
        {
            return;
        }

        try
        {
            await _dataService.SetNotificationRouteEnabledAsync(selected.RouteId, !selected.Enabled);
            await LoadRoutesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not toggle the route", ex);
        }
    }

    private async void DeleteRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RoutesGrid.SelectedItem is not NotificationRouteRow selected)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Delete this route?\n\n{selected.MetricMatch} → {selected.ChannelsDisplay}\n\nAlerts it matched will fall back to the family route, if any, then to the Notifications channels.",
            "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.DeleteNotificationRouteAsync(selected.RouteId);
            await LoadRoutesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not delete the route", ex);
        }
    }

    private async void EnabledCheckBox_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not CheckBox cb || cb.DataContext is not NotificationRouteRow route)
        {
            return;
        }

        try
        {
            await _dataService.SetNotificationRouteEnabledAsync(route.RouteId, cb.IsChecked == true);
        }
        catch (Exception ex)
        {
            /* The two-way binding already wrote the attempted value; reload from the store (unchanged by
               the failed write) to restore the grid to the truth. */
            ShowError("Could not change the enabled state", ex);
            await LoadRoutesAsync();
        }
    }

    private void RoutesGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e) => EditRoute_Click(sender, e);

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowError(string what, Exception ex) =>
        MessageBox.Show($"{what}: {ex.Message}", "Notification Routes", MessageBoxButton.OK, MessageBoxImage.Warning);
}
