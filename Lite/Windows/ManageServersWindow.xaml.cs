/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Reflection;
using System.Windows;
using System.Windows.Input;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Windows;

public partial class ManageServersWindow : Window
{
    private readonly ServerManager _serverManager;
    private readonly ProfileManager _profileManager;

    /// <summary>
    /// Set to true if servers were modified so the caller knows to refresh.
    /// </summary>
    public bool ServersChanged { get; private set; }

    /// <summary>
    /// The caller's removal of a server, invoked by a Delete (#2033, #4795): MainWindow passes its
    /// <c>RemoveServerAsync</c>, which clears the same hash-keyed state (collection health, AG edge state, tag
    /// assignments) the sidebar Remove does AND deletes the registry entry, with nothing awaited between the
    /// first drop and the delete — this window can't reach those services itself, and before this it silently
    /// left all three behind for a re-added server to resurrect. Required: the window never deletes a registry
    /// entry on its own.
    /// </summary>
    private readonly Func<ServerConnection, Task> _removeServer;

    public ManageServersWindow(ServerManager serverManager, ProfileManager profileManager, Func<ServerConnection, Task> removeServer)
    {
        InitializeComponent();
        _serverManager = serverManager;
        _profileManager = profileManager;
        _removeServer = removeServer;
        RefreshGrid();
    }

    private void RefreshGrid()
    {
        ServersGrid.ItemsSource = null;
        var servers = _serverManager.GetAllServers();
        string appVersion = GetAppVersion();
        foreach (var s in servers)
        {
            s.InstalledVersion = appVersion;
        }
        ServersGrid.ItemsSource = servers;
    }

    private static string GetAppVersion()
    {
        string raw = Assembly.GetExecutingAssembly()
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString()
            ?? "0.0.0";

        return VersionText.Normalize(raw);
    }

    private void AddButton_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new AddServerDialog(_serverManager, _profileManager) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ServersChanged = true;
            RefreshGrid();
        }
    }

    private void EditButton_Click(object sender, RoutedEventArgs e)
    {
        EditSelected();
    }

    private void ServersGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        EditSelected();
    }

    private void EditSelected()
    {
        if (ServersGrid.SelectedItem is not ServerConnection selected)
        {
            return;
        }

        var dialog = new AddServerDialog(_serverManager, _profileManager, selected) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ServersChanged = true;
            RefreshGrid();
        }
    }

    private void CredentialProfiles_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new ManageCredentialProfilesDialog(_profileManager) { Owner = this };
        dialog.ShowDialog();
        if (dialog.ProfilesChanged)
        {
            // Server rows may display which profile they use; refresh to reflect reassignments.
            RefreshGrid();
        }
    }

    private void EditMenuItem_Click(object sender, RoutedEventArgs e) => EditSelected();

    private void DeleteMenuItem_Click(object sender, RoutedEventArgs e) => DeleteButton_Click(sender, e);

    private void ExcludedDatabases_Click(object sender, RoutedEventArgs e)
    {
        if (ServersGrid.SelectedItem is not ServerConnection selected)
        {
            return;
        }

        var dialog = new ExcludedDatabasesDialog(_serverManager, selected) { Owner = this };
        if (dialog.ShowDialog() == true && dialog.ExclusionsModified)
        {
            ServersChanged = true;
            RefreshGrid();
        }
    }

    private async void DeleteButton_Click(object sender, RoutedEventArgs e)
    {
        if (ServersGrid.SelectedItem is not ServerConnection selected)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Delete server '{selected.DisplayNameWithIntent}'?\n\nThis will remove the server and its stored credentials.",
            "Delete Server",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (result == MessageBoxResult.Yes)
        {
            /* #2033: the caller's removal derives the storage-name hash from the intact connection, drops the
               runtime state and deletes the registry entry, the same removal the sidebar Remove uses. #4795: it
               deletes the entry itself so that nothing is awaited between its first drop and the delete; a tag
               clear failure logs inside it and never blocks the delete. */
            await _removeServer(selected);

            ServersChanged = true;
            RefreshGrid();
        }
    }

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "servers", App.CsvSeparator);

    private void CloseButton_Click(object sender, RoutedEventArgs e)
    {
        Close();
    }
}
