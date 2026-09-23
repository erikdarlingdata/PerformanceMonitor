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
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's Manage Servers window — the control-plane server list. It reads the DESIRED-state
/// <c>config.config_monitored_servers</c> through <see cref="ViewerDataService"/> (the source of truth for
/// what the Darling service collects, Stage 3) and Add / Edit / Delete / Enable-Disable all WRITE that store,
/// so a change here starts or stops collection on the service's next reload. Excluded-databases editing and
/// shared credential-profile management ride along. Favorites stay viewer-local (<see cref="ViewerServerStore"/>).
/// </summary>
public partial class ManageServersWindow : Window
{
    private readonly ViewerServerStore _serverStore;
    private readonly ViewerProfileStore _profileStore;
    private readonly ViewerDataService? _dataService;

    /// <summary>True when servers were modified, so the caller refreshes the sidebar.</summary>
    public bool ServersChanged { get; private set; }

    /// <param name="dataService">The store the server list reads + writes; null (viewer not connected) shows an
    /// empty list with a note — server management needs the store.</param>
    public ManageServersWindow(ViewerServerStore serverStore, ViewerProfileStore profileStore, ViewerDataService? dataService)
    {
        InitializeComponent();
        _serverStore = serverStore;
        _profileStore = profileStore;
        _dataService = dataService;
        Loaded += async (_, _) => await RefreshGridAsync();
    }

    private async Task RefreshGridAsync()
    {
        if (_dataService is null)
        {
            ServersGrid.ItemsSource = null;
            return;
        }

        try
        {
            /* Preserve the selected row across the reload (after Edit/Delete/Enable), matching the sidebar. */
            var previouslySelected = (ServersGrid.SelectedItem as ManagedServerListItem)?.Row.ServerId;

            var rows = await _dataService.GetMonitoredServersAsync();
            var appVersion = GetAppVersion();

            /* One collection-freshness read for the whole list — the same MAX(collection_time) per server the
               sidebar dots + Overview cards derive freshness from — so the grid shows when the Darling SERVICE
               last collected each server (the viewer never connects to a monitored server). Defensive: a
               read-only seat or a transient failure just leaves the column showing "Never". */
            Dictionary<int, DateTime> freshness;
            try
            {
                freshness = await _dataService.GetServerFreshnessAsync();
            }
            catch (Exception ex)
            {
                ViewerLogger.Warn("ManageServersWindow", $"collection-freshness read failed: {ex.Message}");
                freshness = new Dictionary<int, DateTime>();
            }

            /* #3967: the registry's first-connect instants, so a server whose whole history retention has
               dropped reads "None retained" rather than "Never" — the registration rule the sidebar dot and the
               Overview card apply. Defensive like the freshness read: a failure leaves every row on "Never". */
            Dictionary<int, DateTime?> registered;
            try
            {
                registered = (await _dataService.GetServersAsync()).ToDictionary(s => s.ServerId, s => s.RegisteredAt);
            }
            catch (Exception ex)
            {
                ViewerLogger.Warn("ManageServersWindow", $"registry read failed: {ex.Message}");
                registered = new Dictionary<int, DateTime?>();
            }

            var nowUtc = DateTime.UtcNow;
            var items = new List<ManagedServerListItem>(rows.Count);
            foreach (var row in rows)
            {
                var lastCollected = freshness.TryGetValue(row.ServerId, out var t) ? t : (DateTime?)null;
                var registeredAt = registered.TryGetValue(row.ServerId, out var r) ? r : null;
                items.Add(new ManagedServerListItem(row, _serverStore.IsFavorite(row.Host))
                {
                    InstalledVersion = appVersion,
                    LastCollectedUtc = lastCollected,
                    HistoryAgedOut = lastCollected is null
                        && ServerSummaryItem.ClassifyFreshness(null, registeredAt, nowUtc) == ServerFreshness.Offline,
                });
            }

            ServersGrid.ItemsSource = items;
            if (previouslySelected is int prev)
            {
                ServersGrid.SelectedItem = items.Find(i => i.Row.ServerId == prev);
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageServersWindow", "Failed to read the configured servers", ex);
            MessageBox.Show($"Could not read the configured servers: {ex.Message}",
                "Manage Servers", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private static string GetAppVersion()
    {
        var raw = Assembly.GetExecutingAssembly()
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString()
            ?? "0.0.0";

        var plusIndex = raw.IndexOf('+');
        var trimmed = plusIndex >= 0 ? raw[..plusIndex] : raw;
        return Version.TryParse(trimmed, out var v)
            ? new Version(v.Major, v.Minor, v.Build).ToString()
            : trimmed;
    }

    private async void AddButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        var dialog = new AddServerDialog(_dataService, _serverStore, _profileStore) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ServersChanged = true;
            await RefreshGridAsync();
        }
    }

    private void EditButton_Click(object sender, RoutedEventArgs e) => EditSelected();

    private void ServersGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e) => EditSelected();

    private async void EditSelected()
    {
        if (_dataService is null || ServersGrid.SelectedItem is not ManagedServerListItem selected)
        {
            return;
        }

        /* The Manage Servers LIST read no longer carries encrypted_password (#1416: a read-only viewer seat
           lost SELECT on it, so MonitoredServersSelectSql omits the column). Editing needs the DPAPI blob to
           keep the password when the user doesn't retype it, so reload the full row by id here — the edit-load
           read (MonitoredServerByIdSql) DOES select the secret, which an admin-role seat can read. On a
           read-only seat this read 42501s and the edit is aborted with a message (a read-only seat can't save
           anyway). */
        MonitoredServerRow row;
        try
        {
            row = await _dataService.GetMonitoredServerAsync(selected.Row.ServerId) ?? selected.Row;
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageServersWindow", "Failed to load the server for editing", ex);
            MessageBox.Show($"Could not load the server for editing: {ex.Message}",
                "Edit Server", MessageBoxButton.OK, MessageBoxImage.Error);
            return;
        }

        var dialog = new AddServerDialog(_dataService, _serverStore, _profileStore, row, selected.IsFavorite) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ServersChanged = true;
            await RefreshGridAsync();
        }
    }

    private void EditMenuItem_Click(object sender, RoutedEventArgs e) => EditSelected();

    private void DeleteMenuItem_Click(object sender, RoutedEventArgs e) => DeleteButton_Click(sender, e);

    private async void ToggleEnabledMenuItem_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || ServersGrid.SelectedItem is not ManagedServerListItem selected)
        {
            return;
        }

        var newState = !selected.Row.IsEnabled;
        try
        {
            await _dataService.SetMonitoredServerEnabledAsync(selected.Row.ServerId, newState);
            ServersChanged = true;
            await RefreshGridAsync();
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Manage Servers", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (ViewerSchemaSkewException ex)
        {
            MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageServersWindow", "Failed to toggle collection", ex);
            MessageBox.Show($"Could not change collection state: {ex.Message}",
                "Manage Servers", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void CredentialProfiles_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new ManageCredentialProfilesDialog(_profileStore) { Owner = this };
        dialog.ShowDialog();
        if (dialog.ProfilesChanged)
        {
            /* Server rows may reference a profile; refresh to reflect reassignments. */
            _ = RefreshGridAsync();
        }
    }

    private async void ExcludedDatabases_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || ServersGrid.SelectedItem is not ManagedServerListItem selected)
        {
            return;
        }

        var dialog = new ExcludedDatabasesDialog(_dataService, selected.Row, selected.DisplayNameWithIntent) { Owner = this };
        if (dialog.ShowDialog() == true && dialog.ExclusionsModified)
        {
            ServersChanged = true;
            await RefreshGridAsync();
        }
    }

    private async void DeleteButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || ServersGrid.SelectedItem is not ManagedServerListItem selected)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Delete server '{selected.DisplayNameWithIntent}'?\n\n" +
            "This removes the server definition from the store, so the Darling service stops collecting it on " +
            "its next reload. Already-collected history is kept.",
            "Delete Server",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.DeleteMonitoredServerAsync(selected.Row.ServerId);
            /* Drop the viewer-local favorite pin too, so a removed server doesn't linger starred. */
            _serverStore.SetFavorite(selected.Row.Host, false);
            ServersChanged = true;
            await RefreshGridAsync();
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Delete Server", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (ViewerSchemaSkewException ex)
        {
            MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageServersWindow", "Failed to delete server", ex);
            MessageBox.Show($"Could not delete the server: {ex.Message}",
                "Delete Server", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "servers", ViewerExportSettings.CsvSeparator);

    private void CloseButton_Click(object sender, RoutedEventArgs e) => Close();
}

/// <summary>
/// One Manage-Servers grid row — a display projection of a <see cref="MonitoredServerRow"/> from the store.
/// The bound property names match the grid's columns (<c>DisplayNameWithIntent</c> / <c>ServerNameDisplay</c>
/// / <c>AuthenticationDisplay</c> / <c>StatusDisplay</c> / <c>MonthlyCostUsd</c> / <c>CreatedDate</c>), so the
/// XAML is unchanged from the former <c>ViewerServerEntry</c> binding; the underlying <see cref="Row"/> drives
/// the Edit / Delete / Enable / Excluded-Databases actions.
/// </summary>
public sealed class ManagedServerListItem
{
    public ManagedServerListItem(MonitoredServerRow row, bool isFavorite)
    {
        Row = row ?? throw new ArgumentNullException(nameof(row));
        IsFavorite = isFavorite;
    }

    public MonitoredServerRow Row { get; }

    public bool IsFavorite { get; }

    /// <summary>Set once before binding; one viewer shows the same version for every row.</summary>
    public string? InstalledVersion { get; set; }

    public string DisplayNameWithIntent => Row.ReadOnlyIntent ? $"{Row.Name} (Read-Only)" : Row.Name;

    public string ServerNameDisplay => Row.ReadOnlyIntent ? $"{Row.Host} (Read-Only)" : Row.Host;

    public string AuthenticationDisplay =>
        string.Equals(Row.Auth, ServerStoreCredential.Sql, StringComparison.OrdinalIgnoreCase) ? "SQL Server" : "Windows";

    public string StatusDisplay => Row.IsEnabled ? "Enabled" : "Disabled";

    public decimal MonthlyCostUsd => Row.MonthlyCostUsd;

    /// <summary>The store's creation time (local, for the grid's "Added" column); falls back to now for a row read without it.</summary>
    public DateTime CreatedDate => (Row.CreatedAt ?? DateTime.UtcNow).ToLocalTime();

    /// <summary>The store's naive-UTC newest collection time for this server (MAX(collection_time) across all
    /// collectors), set once before binding; null when the Darling service has not collected this server yet.</summary>
    public DateTime? LastCollectedUtc { get; set; }

    /// <summary>True when there is no collection to show because retention has dropped all of it (#3967): the
    /// server registered before the collection log's horizon and nothing newer is left. Set once before
    /// binding.</summary>
    public bool HistoryAgedOut { get; set; }

    /// <summary>The "Last Collected" cell: the newest collection time rendered in the viewer's timestamp-display
    /// mode (Server/Local/UTC via <see cref="ViewerTimeHelper.ForDisplay"/>), "None retained" when retention
    /// has dropped all of it, or "Never" when the service has not collected this server yet. Labeled "Last
    /// Collected" — the SERVICE connects and collects, the viewer never does — so this reflects service
    /// activity, not a viewer connection.</summary>
    public string LastCollectedDisplay =>
        LastCollectedUtc is { } utc ? ViewerTimeHelper.ForDisplay(utc).ToString("yyyy-MM-dd HH:mm")
        : HistoryAgedOut ? "None retained"
        : "Never";
}
