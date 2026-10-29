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
/// Agent Alerts (#6874): one server's SQL Server Agent alerts and the gaps in them, read only, the Viewer twin of the web's
/// Configuration tab panels. The read is <see cref="ViewerDataService.ReadAgentAlertsAsync"/> and the words are
/// <see cref="ViewerAgentAlertsText"/>. A server with no Agent alerts (Azure SQL Database, Managed Instance), or whose collector is
/// refused, shows the read's own sentence in place of the lists. Load failures surface on the host status bar via <see cref="StatusChanged"/>.
/// </summary>
public partial class AgentAlertsTab : UserControl
{
    private ViewerDataService? _dataService;
    private readonly ScopedLoadGenerations _loads = new();
    private bool _binding;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public AgentAlertsTab()
    {
        InitializeComponent();
    }

    /// <summary>Wires the data service.</summary>
    public void Initialize(ViewerDataService dataService) => _dataService = dataService;

    /// <summary>Reloads the server list and the picked server's alerts (the shell calls this when the tab becomes visible).</summary>
    public async Task RefreshAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        var gen = _loads.Claim(nameof(RefreshAsync));
        try
        {
            var servers = await _dataService.GetPickerServersAsync();
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

            await LoadAlertsAsync();
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(RefreshAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load the server list for agent alerts: {ex.Message}");
        }
    }

    private async Task LoadAlertsAsync()
    {
        if (_dataService == null || ServerComboBox.SelectedItem is not DarlingServer server)
        {
            Show(null);
            return;
        }

        var gen = _loads.Claim(nameof(LoadAlertsAsync));
        try
        {
            var read = await _dataService.ReadAgentAlertsAsync(server, DateTime.UtcNow);
            if (_loads.Superseded(nameof(LoadAlertsAsync), gen)) return;

            Show(read);
            LastRefreshedIndicator.Text = "Last refreshed " + ViewerTimeHelper.FormatFleetWide(DateTime.UtcNow, "HH:mm:ss");
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadAlertsAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load agent alerts for {server.DisplayName}: {ex.Message}");
        }
    }

    private void Show(ViewerAgentAlertsRead? read)
    {
        if (read is null)
        {
            StatusLine.Text = "";
            GapsDataGrid.ItemsSource = new List<ViewerAgentAlertGapRow>();
            AlertsDataGrid.ItemsSource = new List<ViewerAgentAlertRow>();
            GapsEmptyMessage.Visibility = AlertsEmptyMessage.Visibility = Visibility.Collapsed;
            return;
        }

        StatusLine.Text = ViewerAgentAlertsText.StatusLine(read);
        var gaps = ViewerAgentAlertsText.GapRows(read);
        var alerts = ViewerAgentAlertsText.AlertRows(read);
        GapsDataGrid.ItemsSource = gaps;
        AlertsDataGrid.ItemsSource = alerts;
        GapsEmptyMessage.Text = ViewerAgentAlertsText.GapsEmptyText(read);
        AlertsEmptyMessage.Text = ViewerAgentAlertsText.AlertsEmptyText(read);
        GapsEmptyMessage.Visibility = gaps.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        AlertsEmptyMessage.Visibility = alerts.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    private async void ServerComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_binding) return;

        await LoadAlertsAsync();
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await RefreshAsync();
}
