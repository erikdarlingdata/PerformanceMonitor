/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
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

        private void RemoveServer_Click(object sender, RoutedEventArgs e)
        {
            if (ServersDataGrid.SelectedItem is ServerConnection server)
            {
                var result = MessageBox.Show(
                    $"Are you sure you want to remove server '{server.DisplayName}'?\n\nThis action cannot be undone.",
                    "Confirm Remove Server",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Warning
                );

                if (result == MessageBoxResult.Yes)
                {
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

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            DialogResult = ServersModified;
            Close();
        }
    }
}
