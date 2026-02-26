/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
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
                        $"Server '{dialog.ServerConnection.DisplayName}' added successfully!",
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
                            $"Server '{dialog.ServerConnection.DisplayName}' updated successfully!",
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
                var dialog = new RemoveServerDialog(server.DisplayName);
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
                                $"Could not drop the PerformanceMonitor database on '{server.DisplayName}':\n\n{ex.Message}\n\nThe server will still be removed from the Dashboard.",
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
                        $"Server '{server.DisplayName}' removed successfully!",
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
