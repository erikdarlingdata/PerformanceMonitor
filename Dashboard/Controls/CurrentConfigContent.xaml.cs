/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class CurrentConfigContent : UserControl
    {
        private DatabaseService? _databaseService;

        public CurrentConfigContent()
        {
            InitializeComponent();
        }

        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService;
        }

        public async Task RefreshAllDataAsync()
        {
            if (_databaseService == null) return;

            await Task.WhenAll(
                RefreshServerConfigAsync(),
                RefreshDatabaseConfigAsync(),
                RefreshTraceFlagsAsync()
            );
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(ServerConfigDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(DatabaseConfigDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(TraceFlagsDataGrid);
            TabHelpers.FreezeColumns(ServerConfigDataGrid, 1);
            TabHelpers.FreezeColumns(DatabaseConfigDataGrid, 1);
        }

        #region Server Configuration

        private async Task RefreshServerConfigAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetCurrentServerConfigAsync();
                ServerConfigDataGrid.ItemsSource = data;
                ServerConfigNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading server configuration: {ex.Message}");
            }
        }

        private void ServerConfigFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(ServerConfigDataGrid, sender as TextBox);
        }

        #endregion

        #region Database Configuration

        private async Task RefreshDatabaseConfigAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetCurrentDatabaseConfigAsync();
                DatabaseConfigDataGrid.ItemsSource = data;
                DatabaseConfigNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database configuration: {ex.Message}");
            }
        }

        private void DatabaseConfigFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(DatabaseConfigDataGrid, sender as TextBox);
        }

        #endregion

        #region Trace Flags

        private async Task RefreshTraceFlagsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetCurrentTraceFlagsAsync();
                TraceFlagsDataGrid.ItemsSource = data;
                TraceFlagsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading trace flags: {ex.Message}");
            }
        }

        #endregion
    }
}
