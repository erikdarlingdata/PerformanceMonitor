/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    /// <summary>
    /// UserControl for the FinOps tab content.
    /// Displays utilization efficiency, database resource usage,
    /// database sizes, and application connection metrics.
    /// </summary>
    public partial class FinOpsContent : UserControl
    {
        private DatabaseService? _databaseService;
        private ServerManager? _serverManager;
        private CredentialService? _credentialService;

        public FinOpsContent()
        {
            InitializeComponent();
            Loaded += OnLoaded;
            Unloaded += OnUnloaded;
            Helpers.ThemeManager.ThemeChanged += OnThemeChanged;

            // Apply dark theme immediately so charts don't flash white
            TabHelpers.ApplyThemeToChart(PeakUtilizationChart);
        }

        private void OnUnloaded(object sender, RoutedEventArgs e)
        {
            Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
        }

        private void OnThemeChanged(string _)
        {
            TabHelpers.ApplyThemeToChart(PeakUtilizationChart);
            PeakUtilizationChart.Refresh();
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(DatabaseResourcesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(DatabaseSizesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(ApplicationConnectionsDataGrid);

            TabHelpers.FreezeColumns(DatabaseResourcesDataGrid, 1);
            TabHelpers.FreezeColumns(DatabaseSizesDataGrid, 1);
            TabHelpers.FreezeColumns(ApplicationConnectionsDataGrid, 1);
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        public void Initialize(ServerManager serverManager, CredentialService credentialService)
        {
            _serverManager = serverManager ?? throw new ArgumentNullException(nameof(serverManager));
            _credentialService = credentialService ?? throw new ArgumentNullException(nameof(credentialService));

            var servers = _serverManager.GetAllServers();
            ServerSelector.ItemsSource = servers;
            if (servers.Count > 0)
                ServerSelector.SelectedIndex = 0;
        }

        private async void ServerSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (ServerSelector.SelectedItem is ServerConnection server && _credentialService != null)
            {
                var connectionString = server.GetConnectionString(_credentialService);
                _databaseService = new DatabaseService(connectionString);
                await RefreshDataAsync();
            }
        }

        /// <summary>
        /// Refreshes all FinOps data. Can be called from parent control.
        /// </summary>
        public async Task RefreshDataAsync()
        {
            try
            {
                await Task.WhenAll(
                    LoadUtilizationAsync(),
                    LoadDatabaseResourcesAsync(),
                    LoadDatabaseSizesAsync(),
                    LoadApplicationConnectionsAsync()
                );
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing FinOps data: {ex.Message}", ex);
            }
        }

        // ============================================
        // Utilization Tab
        // ============================================

        private async Task LoadUtilizationAsync()
        {
            if (_databaseService == null) return;

            try
            {
                UtilizationLoading.IsLoading = true;

                var efficiencyTask = _databaseService.GetFinOpsUtilizationEfficiencyAsync();
                var peakTask = _databaseService.GetFinOpsPeakUtilizationAsync();

                await Task.WhenAll(efficiencyTask, peakTask);

                var efficiency = await efficiencyTask;
                var peakData = await peakTask;

                UpdateUtilizationSummary(efficiency);
                RenderPeakUtilizationChart(peakData);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading utilization data: {ex.Message}", ex);
            }
            finally
            {
                UtilizationLoading.IsLoading = false;
            }
        }

        private void UpdateUtilizationSummary(FinOpsUtilizationEfficiency? efficiency)
        {
            if (efficiency == null)
            {
                ProvisioningStatusText.Text = "No Data";
                ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
                return;
            }

            // Provisioning status with color coding
            ProvisioningStatusText.Text = efficiency.ProvisioningStatus.Replace("_", " ");

            switch (efficiency.ProvisioningStatus)
            {
                case "RIGHT_SIZED":
                    ProvisioningStatusBorder.Background = (Brush)FindResource("SuccessBrush");
                    ProvisioningStatusText.Foreground = Brushes.White;
                    break;
                case "OVER_PROVISIONED":
                    ProvisioningStatusBorder.Background = (Brush)FindResource("WarningBrush");
                    ProvisioningStatusText.Foreground = Brushes.Black;
                    break;
                case "UNDER_PROVISIONED":
                    ProvisioningStatusBorder.Background = (Brush)FindResource("ErrorBrush");
                    ProvisioningStatusText.Foreground = Brushes.White;
                    break;
                default:
                    ProvisioningStatusBorder.Background = new SolidColorBrush(Colors.Gray);
                    ProvisioningStatusText.Foreground = Brushes.White;
                    break;
            }

            // CPU metrics
            CpuCountText.Text = efficiency.CpuCount.ToString("N0");
            AvgCpuText.Text = $"{efficiency.AvgCpuPct:N2}%";
            P95CpuText.Text = $"{efficiency.P95CpuPct:N2}%";
            MaxCpuText.Text = $"{efficiency.MaxCpuPct}%";
            CpuSamplesText.Text = efficiency.CpuSamples.ToString("N0");

            // Memory metrics
            PhysicalMemoryText.Text = $"{efficiency.PhysicalMemoryMb:N0} MB";
            TargetMemoryText.Text = $"{efficiency.TargetMemoryMb:N0} MB";
            TotalMemoryText.Text = $"{efficiency.TotalMemoryMb:N0} MB";
            MemoryUtilText.Text = $"{efficiency.MemoryUtilizationPct}%";

            // Thread metrics
            WorkerThreadsText.Text = $"{efficiency.WorkerThreadsCurrent:N0} / {efficiency.WorkerThreadsMax:N0}";
            ThreadRatioText.Text = $"{efficiency.WorkerThreadRatio:N2}";
        }

        private void RenderPeakUtilizationChart(List<FinOpsPeakUtilization> data)
        {
            PeakUtilizationChart.Plot.Clear();

            if (data.Count == 0)
            {
                var noDataText = PeakUtilizationChart.Plot.Add.Text("No peak utilization data available", 12, 50);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Color.FromHex("#888888");
                PeakUtilizationChart.Refresh();
                return;
            }

            // Build bars for avg CPU by hour, colored by classification
            var bars = new List<ScottPlot.Bar>();

            foreach (var item in data)
            {
                var color = item.HourClassification switch
                {
                    "PEAK" => ScottPlot.Color.FromHex("#E74C3C"),   // Red
                    "IDLE" => ScottPlot.Color.FromHex("#27AE60"),   // Green
                    "NORMAL" => ScottPlot.Color.FromHex("#3498DB"), // Blue
                    _ => ScottPlot.Color.FromHex("#95A5A6")         // Gray
                };

                bars.Add(new ScottPlot.Bar
                {
                    Position = item.HourOfDay,
                    Value = (double)item.AvgCpuPct,
                    FillColor = color,
                    Size = 0.8
                });
            }

            var barPlot = PeakUtilizationChart.Plot.Add.Bars(bars.ToArray());
            barPlot.Horizontal = false;

            PeakUtilizationChart.Plot.Axes.Bottom.Label.Text = "Hour of Day";
            PeakUtilizationChart.Plot.Axes.Left.Label.Text = "Avg CPU %";

            // Set x-axis ticks to show each hour
            var ticks = data.Select(d => new ScottPlot.Tick(d.HourOfDay, $"{d.HourOfDay:D2}:00")).ToArray();
            PeakUtilizationChart.Plot.Axes.Bottom.TickGenerator = new ScottPlot.TickGenerators.NumericManual(ticks);
            PeakUtilizationChart.Plot.Axes.Bottom.TickLabelStyle.Rotation = 45;

            // Add a legend for classifications
            PeakUtilizationChart.Plot.Legend.IsVisible = true;
            PeakUtilizationChart.Plot.Legend.Alignment = ScottPlot.Alignment.UpperRight;

            PeakUtilizationChart.Refresh();
        }

        // ============================================
        // Database Resources Tab
        // ============================================

        private async Task LoadDatabaseResourcesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                if (DatabaseResourcesDataGrid.ItemsSource == null)
                {
                    DatabaseResourcesLoading.IsLoading = true;
                    DatabaseResourcesNoDataMessage.Visibility = Visibility.Collapsed;
                }

                var data = await _databaseService.GetFinOpsDatabaseResourceUsageAsync();
                DatabaseResourcesDataGrid.ItemsSource = data;
                DatabaseResourcesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database resources: {ex.Message}", ex);
            }
            finally
            {
                DatabaseResourcesLoading.IsLoading = false;
            }
        }

        // ============================================
        // Database Sizes Tab
        // ============================================

        private async Task LoadDatabaseSizesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                if (DatabaseSizesDataGrid.ItemsSource == null)
                {
                    DatabaseSizesLoading.IsLoading = true;
                    DatabaseSizesNoDataMessage.Visibility = Visibility.Collapsed;
                }

                var data = await _databaseService.GetFinOpsDatabaseSizeStatsAsync();
                DatabaseSizesDataGrid.ItemsSource = data;
                DatabaseSizesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database sizes: {ex.Message}", ex);
            }
            finally
            {
                DatabaseSizesLoading.IsLoading = false;
            }
        }

        // ============================================
        // Application Connections Tab
        // ============================================

        private async Task LoadApplicationConnectionsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                if (ApplicationConnectionsDataGrid.ItemsSource == null)
                {
                    ApplicationConnectionsLoading.IsLoading = true;
                    ApplicationConnectionsNoDataMessage.Visibility = Visibility.Collapsed;
                }

                var data = await _databaseService.GetFinOpsApplicationResourceUsageAsync();
                ApplicationConnectionsDataGrid.ItemsSource = data;
                ApplicationConnectionsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading application connections: {ex.Message}", ex);
            }
            finally
            {
                ApplicationConnectionsLoading.IsLoading = false;
            }
        }

        // ============================================
        // Refresh Button Handlers
        // ============================================

        private async void UtilizationRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadUtilizationAsync();
        }

        private async void DatabaseResourcesRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadDatabaseResourcesAsync();
        }

        private async void DatabaseSizesRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadDatabaseSizesAsync();
        }

        private async void ApplicationConnectionsRefresh_Click(object sender, RoutedEventArgs e)
        {
            await LoadApplicationConnectionsAsync();
        }

        // ============================================
        // Copy / Export Context Menu Handlers
        // ============================================

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid && grid.CurrentCell.Column != null)
                {
                    var cellContent = TabHelpers.GetCellContent(grid, grid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(cellContent, false);
                    }
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid && grid.SelectedItem != null)
                {
                    var rowText = TabHelpers.GetRowAsText(grid, grid.SelectedItem);
                    if (!string.IsNullOrEmpty(rowText))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(rowText, false);
                    }
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var sb = new StringBuilder();

                    // Header row
                    var headers = grid.Columns.Select(c => DataGridClipboardBehavior.GetHeaderText(c));
                    sb.AppendLine(string.Join("\t", headers));

                    // Data rows
                    foreach (var item in grid.Items)
                    {
                        var values = new List<string>();
                        foreach (var column in grid.Columns)
                        {
                            var binding = (column as DataGridBoundColumn)?.Binding as Binding;
                            if (binding != null)
                            {
                                var prop = item.GetType().GetProperty(binding.Path.Path);
                                var value = prop?.GetValue(item)?.ToString() ?? string.Empty;
                                values.Add(value);
                            }
                        }
                        sb.AppendLine(string.Join("\t", values));
                    }

                    if (sb.Length > 0)
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(sb.ToString(), false);
                    }
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var dialog = new SaveFileDialog
                    {
                        Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                        DefaultExt = ".csv",
                        FileName = $"FinOps_Export_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
                    };

                    if (dialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Header row
                            var sep = TabHelpers.CsvSeparator;
                            var headers = grid.Columns.Select(c => TabHelpers.EscapeCsvField(DataGridClipboardBehavior.GetHeaderText(c), sep));
                            sb.AppendLine(string.Join(sep, headers));

                            // Data rows
                            foreach (var item in grid.Items)
                            {
                                var values = new List<string>();
                                foreach (var column in grid.Columns)
                                {
                                    var binding = (column as DataGridBoundColumn)?.Binding as Binding;
                                    if (binding != null)
                                    {
                                        var prop = item.GetType().GetProperty(binding.Path.Path);
                                        values.Add(TabHelpers.EscapeCsvField(TabHelpers.FormatForExport(prop?.GetValue(item)), sep));
                                    }
                                }
                                sb.AppendLine(string.Join(sep, values));
                            }

                            File.WriteAllText(dialog.FileName, sb.ToString());
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Error exporting to CSV: {ex.Message}", ex);
                            MessageBox.Show($"Error exporting to CSV: {ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }
    }
}
