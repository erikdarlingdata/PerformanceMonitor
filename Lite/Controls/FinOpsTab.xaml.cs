/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class FinOpsTab : UserControl
{
    private LocalDataService? _dataService;
    private ServerManager? _serverManager;

    public FinOpsTab()
    {
        InitializeComponent();
    }

    /// <summary>
    /// Initializes the control with required dependencies.
    /// </summary>
    public void Initialize(LocalDataService dataService, ServerManager serverManager)
    {
        _dataService = dataService;
        _serverManager = serverManager;

        PopulateServerSelector();
        RefreshData();
    }

    private void PopulateServerSelector()
    {
        if (_serverManager == null) return;

        var servers = _serverManager.GetAllServers();
        ServerSelector.ItemsSource = servers;
        if (servers.Count > 0)
            ServerSelector.SelectedIndex = 0;
    }

    private int GetSelectedServerId()
    {
        if (ServerSelector.SelectedItem is ServerConnection server)
            return RemoteCollectorService.GetDeterministicHashCode(server.ServerName);
        return 0;
    }

    /// <summary>
    /// Refreshes all FinOps data.
    /// </summary>
    public async void RefreshData()
    {
        await LoadDatabaseSizesAsync();
        await LoadServerInventoryAsync();
        await LoadPerServerDataAsync();
    }

    #region Data Loading

    private async System.Threading.Tasks.Task LoadPerServerDataAsync()
    {
        var serverId = GetSelectedServerId();
        if (serverId == 0 || _dataService == null) return;

        await LoadUtilizationAsync(serverId);
        await LoadDatabaseResourcesAsync(serverId);
        await LoadApplicationConnectionsAsync(serverId);
    }

    private async System.Threading.Tasks.Task LoadUtilizationAsync(int serverId)
    {
        if (_dataService == null) return;

        try
        {
            var data = await _dataService.GetUtilizationEfficiencyAsync(serverId);
            UpdateUtilizationSummary(data);
            NoUtilizationMessage.Visibility = data == null ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load utilization: {ex.Message}");
        }
    }

    private void UpdateUtilizationSummary(UtilizationEfficiencyRow? data)
    {
        if (data == null)
        {
            ProvisioningStatusText.Text = "No Data";
            ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
            AvgCpuText.Text = P95CpuText.Text = MaxCpuText.Text = CpuSamplesText.Text = "-";
            PhysicalMemoryText.Text = TargetMemoryText.Text = TotalMemoryText.Text = BufferPoolText.Text = MemoryRatioText.Text = "-";
            return;
        }

        ProvisioningStatusText.Text = data.ProvisioningStatus.Replace("_", " ");
        switch (data.ProvisioningStatus)
        {
            case "RIGHT_SIZED":
                ProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#27AE60"));
                ProvisioningStatusText.Foreground = Brushes.White;
                break;
            case "OVER_PROVISIONED":
                ProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#F39C12"));
                ProvisioningStatusText.Foreground = Brushes.Black;
                break;
            case "UNDER_PROVISIONED":
                ProvisioningStatusBorder.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E74C3C"));
                ProvisioningStatusText.Foreground = Brushes.White;
                break;
            default:
                ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
                ProvisioningStatusText.Foreground = Brushes.White;
                break;
        }

        AvgCpuText.Text = $"{data.AvgCpuPct:N2}%";
        P95CpuText.Text = $"{data.P95CpuPct:N2}%";
        MaxCpuText.Text = $"{data.MaxCpuPct}%";
        CpuSamplesText.Text = data.CpuSamples.ToString("N0");

        PhysicalMemoryText.Text = $"{data.PhysicalMemoryMb:N0} MB";
        TargetMemoryText.Text = $"{data.TargetMemoryMb:N0} MB";
        TotalMemoryText.Text = $"{data.TotalMemoryMb:N0} MB";
        BufferPoolText.Text = $"{data.BufferPoolMb:N0} MB";
        MemoryRatioText.Text = $"{data.MemoryRatio:N2}";
    }

    private async System.Threading.Tasks.Task LoadDatabaseResourcesAsync(int serverId)
    {
        if (_dataService == null) return;

        try
        {
            var data = await _dataService.GetDatabaseResourceUsageAsync(serverId);
            DatabaseResourcesDataGrid.ItemsSource = data;
            NoDatabaseResourcesMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            DbResourcesCountIndicator.Text = data.Count > 0 ? $"{data.Count} database(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load database resources: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadApplicationConnectionsAsync(int serverId)
    {
        if (_dataService == null) return;

        try
        {
            var data = await _dataService.GetApplicationConnectionsAsync(serverId);
            ApplicationConnectionsDataGrid.ItemsSource = data;
            NoAppConnectionsMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            AppConnectionsCountIndicator.Text = data.Count > 0 ? $"{data.Count} application(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load application connections: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadDatabaseSizesAsync()
    {
        if (_dataService == null) return;

        try
        {
            var data = await _dataService.GetDatabaseSizeLatestAsync();
            DatabaseSizesDataGrid.ItemsSource = data;

            NoDbSizesMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            DbSizeCountIndicator.Text = data.Count > 0 ? $"{data.Count} file(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load database sizes: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadServerInventoryAsync()
    {
        if (_dataService == null) return;

        try
        {
            var data = await _dataService.GetServerPropertiesLatestAsync();
            ServerInventoryDataGrid.ItemsSource = data;

            NoServerInventoryMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            ServerInventoryCountIndicator.Text = data.Count > 0 ? $"{data.Count} server(s)" : "";
        }
        catch (Exception ex)
        {
            AppLogger.Error("FinOps", $"Failed to load server inventory: {ex.Message}");
        }
    }

    #endregion

    #region Event Handlers

    private async void ServerSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        await LoadPerServerDataAsync();
    }

    private async void RefreshUtilization_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadUtilizationAsync(serverId);
    }

    private async void RefreshDatabaseResources_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadDatabaseResourcesAsync(serverId);
    }

    private async void RefreshApplicationConnections_Click(object sender, RoutedEventArgs e)
    {
        var serverId = GetSelectedServerId();
        if (serverId != 0) await LoadApplicationConnectionsAsync(serverId);
    }

    private async void RefreshDatabaseSizes_Click(object sender, RoutedEventArgs e)
    {
        await LoadDatabaseSizesAsync();
    }

    private async void RefreshServerInventory_Click(object sender, RoutedEventArgs e)
    {
        await LoadServerInventoryAsync();
    }

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentCell.Column == null || grid.CurrentItem == null) return;

        var value = GetCellValue(grid.CurrentCell.Column, grid.CurrentItem);
        if (value.Length > 0) Clipboard.SetDataObject(value, false);
    }

    private void CopyRow_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        var sb = new StringBuilder();
        foreach (var col in grid.Columns)
        {
            sb.Append(GetCellValue(col, grid.CurrentItem));
            sb.Append('\t');
        }
        Clipboard.SetDataObject(sb.ToString().TrimEnd('\t'), false);
    }

    private void CopyAllRows_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null) return;

        var sb = new StringBuilder();

        foreach (var col in grid.Columns)
        {
            sb.Append(col.Header?.ToString() ?? "");
            sb.Append('\t');
        }
        sb.AppendLine();

        foreach (var item in grid.Items)
        {
            foreach (var col in grid.Columns)
            {
                sb.Append(GetCellValue(col, item));
                sb.Append('\t');
            }
            sb.AppendLine();
        }

        Clipboard.SetDataObject(sb.ToString(), false);
    }

    private void ExportToCsv_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null || grid.Items.Count == 0) return;

        var prefix = grid.Name switch
        {
            nameof(DatabaseSizesDataGrid) => "database_sizes",
            nameof(ServerInventoryDataGrid) => "server_inventory",
            nameof(DatabaseResourcesDataGrid) => "database_resources",
            nameof(ApplicationConnectionsDataGrid) => "application_connections",
            _ => "finops_export"
        };

        var dialog = new SaveFileDialog
        {
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
            DefaultExt = ".csv",
            FileName = $"{prefix}_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };

        if (dialog.ShowDialog() != true) return;

        var sb = new StringBuilder();

        var headers = new List<string>();
        foreach (var col in grid.Columns)
            headers.Add(CsvEscape(col.Header?.ToString() ?? ""));
        sb.AppendLine(string.Join(",", headers));

        foreach (var item in grid.Items)
        {
            var values = new List<string>();
            foreach (var col in grid.Columns)
                values.Add(CsvEscape(GetCellValue(col, item)));
            sb.AppendLine(string.Join(",", values));
        }

        try
        {
            File.WriteAllText(dialog.FileName, sb.ToString(), Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to export: {ex.Message}", "Export Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    #endregion

    #region Helpers

    private static DataGrid? FindParentDataGrid(MenuItem menuItem)
    {
        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        while (target != null && target is not DataGrid)
            target = VisualTreeHelper.GetParent(target) as FrameworkElement;
        return target as DataGrid;
    }

    private static string GetCellValue(DataGridColumn col, object item)
    {
        if (col is DataGridBoundColumn boundCol && boundCol.Binding is Binding binding)
        {
            var prop = item.GetType().GetProperty(binding.Path.Path);
            return prop?.GetValue(item)?.ToString() ?? "";
        }
        return "";
    }

    private static string CsvEscape(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    #endregion
}
