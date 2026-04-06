/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Installer.Core;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class ManageServersWindow : Window
    {
        private readonly ServerManager _serverManager;
        public bool ServersModified { get; private set; }

        public ManageServersWindow(ServerManager serverManager)
        {
            InitializeComponent();
            _serverManager = serverManager;
            ServersModified = false;
            LoadServers();
        }

        private void LoadServers()
        {
            ServersDataGrid.ItemsSource = _serverManager.GetAllServers();
        }

        private void ServersDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!Helpers.TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            EditSelectedServer();
        }

        private void AddServer_Click(object sender, RoutedEventArgs e)
        {
            var dialog = new AddServerDialog();
            dialog.Owner = this;

            if (dialog.ShowDialog() == true)
            {
                try
                {
                    _serverManager.AddServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                    LoadServers();
                    ServersModified = true;

                    MessageBox.Show(
                        $"Server '{dialog.ServerConnection.DisplayNameWithIntent}' added successfully!",
                        "Server Added",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                }
                catch (Exception ex)
                {
                    MessageBox.Show(
                        $"Failed to add server:\n\n{ex.Message}",
                        "Error Adding Server",
                        MessageBoxButton.OK,
                        MessageBoxImage.Error
                    );
                }
            }
        }

        private void EditServer_Click(object sender, RoutedEventArgs e)
        {
            EditSelectedServer();
        }

        private void EditSelectedServer()
        {
            if (ServersDataGrid.SelectedItem is ServerConnection server)
            {
                var dialog = new AddServerDialog(server);
                dialog.Owner = this;

                if (dialog.ShowDialog() == true)
                {
                    try
                    {
                        _serverManager.UpdateServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                        LoadServers();
                        ServersModified = true;

                        MessageBox.Show(
                            $"Server '{dialog.ServerConnection.DisplayNameWithIntent}' updated successfully!",
                            "Server Updated",
                            MessageBoxButton.OK,
                            MessageBoxImage.Information
                        );
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(
                            $"Failed to update server:\n\n{ex.Message}",
                            "Error Updating Server",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error
                        );
                    }
                }
            }
            else
            {
                MessageBox.Show(
                    "Please select a server to edit.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );
            }
        }

        private void ToggleFavorite_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is ServerConnection server)
            {
                server.IsFavorite = !server.IsFavorite;
                _serverManager.UpdateServer(server, null, null);
                LoadServers();
                ServersModified = true;
            }
            else
            {
                MessageBox.Show(
                    "Please select a server to toggle favorite status.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );
            }
        }

        private async void RemoveServer_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is ServerConnection server)
            {
                var dialog = new RemoveServerDialog(server.DisplayNameWithIntent);
                dialog.Owner = this;

                if (dialog.ShowDialog() == true)
                {
                    if (dialog.DropDatabase)
                    {
                        try
                        {
                            await _serverManager.DropMonitorDatabaseAsync(server);
                        }
                        catch (Exception ex)
                        {
                            MessageBox.Show(
                                $"Could not drop the PerformanceMonitor database on '{server.DisplayNameWithIntent}':\n\n{ex.Message}\n\nThe server will still be removed from the Dashboard.",
                                "Database Drop Failed",
                                MessageBoxButton.OK,
                                MessageBoxImage.Warning
                            );
                        }
                    }

                    _serverManager.DeleteServer(server.Id);
                    LoadServers();
                    ServersModified = true;

                    MessageBox.Show(
                        $"Server '{server.DisplayNameWithIntent}' removed successfully!",
                        "Server Removed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                }
            }
            else
            {
                MessageBox.Show(
                    "Please select a server to remove.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information
                );
            }
        }

        private async void CheckForUpdates_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is not ServerConnection server)
            {
                MessageBox.Show(
                    "Please select a server to check for updates.",
                    "No Server Selected",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            CheckUpdatesButton.IsEnabled = false;
            CheckUpdatesButton.Content = "Checking...";

            try
            {
                string? installedVersion = await _serverManager.GetInstalledVersionAsync(server);

                if (installedVersion == null)
                {
                    MessageBox.Show(
                        $"No PerformanceMonitor installation found on '{server.DisplayNameWithIntent}'.\n\nUse Edit to install.",
                        "Not Installed",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                    return;
                }

                string appVersion = Assembly.GetExecutingAssembly()
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                    ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "0.0.0";
                /* Strip git hash suffix if present (e.g., "2.5.0+abc123" → "2.5.0") */
                int plusIndex = appVersion.IndexOf('+');
                if (plusIndex >= 0) appVersion = appVersion[..plusIndex];

                /* Normalize both to 3-part for comparison */
                string Normalize(string v)
                {
                    if (Version.TryParse(v, out var parsed))
                        return new Version(parsed.Major, parsed.Minor, parsed.Build).ToString();
                    return v;
                }

                string normalizedInstalled = Normalize(installedVersion);
                string normalizedApp = Normalize(appVersion);

                if (Version.TryParse(normalizedInstalled, out var installed) &&
                    Version.TryParse(normalizedApp, out var app) &&
                    installed < app)
                {
                    var result = MessageBox.Show(
                        $"'{server.DisplayNameWithIntent}' has v{normalizedInstalled} installed.\n\nv{normalizedApp} is available. Open the server editor to upgrade?",
                        "Update Available",
                        MessageBoxButton.YesNo,
                        MessageBoxImage.Information);

                    if (result == MessageBoxResult.Yes)
                    {
                        var dialog = new AddServerDialog(server);
                        dialog.Owner = this;

                        if (dialog.ShowDialog() == true)
                        {
                            try
                            {
                                _serverManager.UpdateServer(dialog.ServerConnection, dialog.Username, dialog.Password);
                                LoadServers();
                                ServersModified = true;
                            }
                            catch (Exception ex)
                            {
                                MessageBox.Show(
                                    $"Failed to update server:\n\n{ex.Message}",
                                    "Error",
                                    MessageBoxButton.OK,
                                    MessageBoxImage.Error);
                            }
                        }
                    }
                }
                else
                {
                    MessageBox.Show(
                        $"'{server.DisplayNameWithIntent}' is up to date (v{normalizedInstalled}).",
                        "No Updates",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to check for updates:\n\n{ex.Message}",
                    "Connection Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error);
            }
            finally
            {
                CheckUpdatesButton.IsEnabled = true;
                CheckUpdatesButton.Content = "Check Server Version";
            }
        }

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = Helpers.TabHelpers.GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                        Clipboard.SetDataObject(cellContent, false);
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid?.SelectedItem != null)
                    Clipboard.SetDataObject(Helpers.TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem), false);
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new System.Text.StringBuilder();
                    var headers = new System.Collections.Generic.List<string>();
                    foreach (var column in dataGrid.Columns)
                        headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                    sb.AppendLine(string.Join("\t", headers));
                    foreach (var item in dataGrid.Items)
                        sb.AppendLine(Helpers.TabHelpers.GetRowAsText(dataGrid, item));
                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var dialog = new Microsoft.Win32.SaveFileDialog
                    {
                        FileName = $"servers_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };
                    if (dialog.ShowDialog() == true)
                    {
                        var sb = new System.Text.StringBuilder();
                        var headers = new System.Collections.Generic.List<string>();
                        foreach (var column in dataGrid.Columns)
                            headers.Add(Helpers.TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column)));
                        sb.AppendLine(string.Join(",", headers));
                        foreach (var item in dataGrid.Items)
                        {
                            var values = Helpers.TabHelpers.GetRowValues(dataGrid, item);
                            sb.AppendLine(string.Join(",", values.Select(v => Helpers.TabHelpers.EscapeCsvField(v))));
                        }
                        System.IO.File.WriteAllText(dialog.FileName, sb.ToString());
                    }
                }
            }
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = ServersModified;
            Close();
        }
    }
}
