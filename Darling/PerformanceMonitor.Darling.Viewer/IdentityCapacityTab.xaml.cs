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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Identity Capacity (#6875): one server's identity columns above 10 percent used, how long until each runs out, and each database's scan
/// state, read only, the Viewer twin of the web's Identity Capacity panels. The read is
/// <see cref="ViewerDataService.ReadIdentityCapacityAsync"/> and the words are <see cref="ViewerIdentityCapacityText"/>. A server with no
/// scan shows the read's own sentence in place of the lists. Load failures surface on the host status bar via <see cref="StatusChanged"/>.
/// </summary>
public partial class IdentityCapacityTab : UserControl
{
    private ViewerDataService? _dataService;
    private readonly ScopedLoadGenerations _loads = new();
    private bool _binding;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public IdentityCapacityTab()
    {
        InitializeComponent();
    }

    /// <summary>Wires the data service.</summary>
    public void Initialize(ViewerDataService dataService) => _dataService = dataService;

    /// <summary>Reloads the server list and the picked server's capacity (the shell calls this when the tab becomes visible).</summary>
    public async Task RefreshAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        var gen = _loads.Claim(nameof(RefreshAsync));
        try
        {
            var servers = await _dataService.GetServersAsync();
            if (_loads.Superseded(nameof(RefreshAsync), gen)) return;

            var pickedId = (ServerComboBox.SelectedItem as DarlingServer)?.ServerId;
            _binding = true;
            try
            {
                ServerComboBox.ItemsSource = servers;
                ServerComboBox.SelectedItem = servers.FirstOrDefault(s => s.ServerId == pickedId) ?? servers.FirstOrDefault();
            }
            finally
            {
                _binding = false;
            }

            await LoadCapacityAsync();
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(RefreshAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load the server list for identity capacity: {ex.Message}");
        }
    }

    private async Task LoadCapacityAsync()
    {
        if (_dataService == null || ServerComboBox.SelectedItem is not DarlingServer server)
        {
            Show(null);
            return;
        }

        var gen = _loads.Claim(nameof(LoadCapacityAsync));
        try
        {
            var read = await _dataService.ReadIdentityCapacityAsync(server, DateTime.UtcNow);
            if (_loads.Superseded(nameof(LoadCapacityAsync), gen)) return;

            Show(read);
            LastRefreshedIndicator.Text = "Last refreshed " + ViewerTimeHelper.FormatFleetWide(DateTime.UtcNow, "HH:mm:ss");
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadCapacityAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load identity capacity for {server.DisplayName}: {ex.Message}");
        }
    }

    private void Show(ViewerIdentityCapacityRead? read)
    {
        if (read is null)
        {
            StatusLine.Text = "";
            ColumnsDataGrid.ItemsSource = new List<ViewerIdentityColumnRow>();
            ScanDataGrid.ItemsSource = new List<ViewerIdentityScanRow>();
            ColumnsEmptyMessage.Visibility = ScanEmptyMessage.Visibility = Visibility.Collapsed;
            return;
        }

        StatusLine.Text = ViewerIdentityCapacityText.StatusLine(read);
        var columns = ViewerIdentityCapacityText.ColumnRows(read);
        var scans = ViewerIdentityCapacityText.ScanRows(read);
        ColumnsDataGrid.ItemsSource = columns;
        ScanDataGrid.ItemsSource = scans;
        ColumnsEmptyMessage.Text = ViewerIdentityCapacityText.ColumnsEmptyText(read);
        ScanEmptyMessage.Text = ViewerIdentityCapacityText.ScanEmptyText(read);
        ColumnsEmptyMessage.Visibility = columns.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        ScanEmptyMessage.Visibility = scans.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    private async void ServerComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_binding) return;

        await LoadCapacityAsync();
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await RefreshAsync();
}
