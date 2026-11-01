/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
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

    /// <summary>The servers and tags the scope picker offers and the Status column checks against (#5750); null when they
    /// could not be read, which the Status column says and the picker survives (All servers, plus the route's own scope).</summary>
    private ViewerRouteScopeInputs? _scopeInputs;

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
            ReadOnlyBanner.Text = "Read-only connection: routes can be viewed but not changed here, and their destination URLs are not shown. " + ViewerReadOnlyException.HowToConnectAsAdmin;
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

            /* Orphan status uses the shared expander on the same three inputs dispatch uses, read through the viewer's own
               connection (the viewer cannot call the service's loader). A failed read leaves them null: scoped routes then read
               "Not active", never "OK". */
            string? scopeReadError = null;
            try
            {
                _scopeInputs = await _dataService.GetNotificationRouteScopeInputsAsync();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _scopeInputs = null;
                scopeReadError = ex.Message;
            }

            ViewerNotificationRouteScope.Annotate(routes, _scopeInputs);
            _routes.Clear();
            foreach (var route in routes.OrderByDescending(r => r.RouteId))
            {
                _routes.Add(route);
            }

            NoRoutesMessage.Visibility = _routes.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            if (scopeReadError is not null)
            {
                ShowError("Could not read the servers and tags", new InvalidOperationException(scopeReadError));
            }
        }
        catch (Exception ex)
        {
            ShowError("Could not read notification routes", ex);
        }
    }

    private async void AddRoute_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new NotificationRouteEditDialog(_scopeInputs, null, null, _routes) { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            await SaveRouteAsync(dialog.Route, null);
            await LoadRoutesAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not save the route", ex);
        }
    }

    /// <summary>
    /// Saves a route with its webhook destinations sealed (#5366). The key is asked for only when a destination was typed; a
    /// route that keeps its saved values needs none. A key notice is shown once.
    /// </summary>
    private async Task SaveRouteAsync(NotificationRouteRow route, NotificationRouteRow? stored)
    {
        var parent = await _dataService.GetNotificationAsync() ?? NotificationRow.Defaults();
        ViewerPasswordSealer? sealer = null;
        string? refusal = null;
        if (ViewerWebhookSealing.NeedsKey(route, stored))
        {
            var key = await ViewerPasswordKey.GetSealKeyAsync(_dataService, this);
            sealer = key.Sealer;
            refusal = key.Refusal;
            if (key.Notice is { } notice)
            {
                MessageBox.Show(notice, "Notification Routes", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        try
        {
            await _dataService.SaveNotificationRouteSealedAsync(route, stored, parent, sealer, refusal, Environment.UserName, default);
        }
        catch (ViewerPasswordRefusedException ex)
        {
            throw new InvalidOperationException(ex.Message, ex);
        }
    }

    private async void EditRoute_Click(object sender, RoutedEventArgs e)
    {
        if (RoutesGrid.SelectedItem is not NotificationRouteRow selected)
        {
            return;
        }

        var dialog = new NotificationRouteEditDialog(_scopeInputs, selected, _dataService, _routes) { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            await SaveRouteAsync(dialog.Route, selected);
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
            await _dataService.SetNotificationRouteEnabledAsync(selected.RouteId, !selected.Enabled, Environment.UserName);
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
            $"Delete this route?\n\n{selected.MetricMatch} ({selected.ScopeDisplay}) → {selected.ChannelsDisplay}\n\nAlerts it matched will fall back to the next most specific route, if any, then to the Notifications channels.",
            "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.DeleteNotificationRouteAsync(selected.RouteId, Environment.UserName);
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
            await _dataService.SetNotificationRouteEnabledAsync(route.RouteId, cb.IsChecked == true, Environment.UserName);
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
