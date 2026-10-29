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
/// Database Protection (#6873): one server's last full backup, log backup and CHECKDB per database, read only, the Viewer twin of the web's
/// Health Checks panels. The read is <see cref="ViewerDataService.ReadDatabaseProtectionAsync"/> and the words are
/// <see cref="ViewerDatabaseProtectionText"/>. A server whose engine is not collected, whose collector is refused or that has no capture
/// shows the read's own sentence in place of the list. Load failures surface on the host status bar via <see cref="StatusChanged"/>.
/// </summary>
public partial class DatabaseProtectionTab : UserControl
{
    private ViewerDataService? _dataService;
    private readonly ScopedLoadGenerations _loads = new();
    private bool _binding;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public DatabaseProtectionTab()
    {
        InitializeComponent();
    }

    /// <summary>Wires the data service.</summary>
    public void Initialize(ViewerDataService dataService) => _dataService = dataService;

    /// <summary>Reloads the server list and the picked server's databases (the shell calls this when the tab becomes visible).</summary>
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

            await LoadAsync();
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(RefreshAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load the server list for database protection: {ex.Message}");
        }
    }

    private async Task LoadAsync()
    {
        if (_dataService == null || ServerComboBox.SelectedItem is not DarlingServer server)
        {
            Show(null);
            return;
        }

        var gen = _loads.Claim(nameof(LoadAsync));
        try
        {
            var read = await _dataService.ReadDatabaseProtectionAsync(server, DateTime.UtcNow);
            if (_loads.Superseded(nameof(LoadAsync), gen)) return;

            Show(read);
            LastRefreshedIndicator.Text = "Last refreshed " + ViewerTimeHelper.FormatFleetWide(DateTime.UtcNow, "HH:mm:ss");
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load database protection for {server.DisplayName}: {ex.Message}");
        }
    }

    private void Show(ViewerDatabaseProtectionRead? read)
    {
        if (read is null)
        {
            StatusLine.Text = "";
            DatabasesDataGrid.ItemsSource = new List<ViewerDatabaseProtectionRow>();
            EmptyMessage.Visibility = Visibility.Collapsed;
            return;
        }

        StatusLine.Text = ViewerDatabaseProtectionText.StatusLine(read);
        var rows = ViewerDatabaseProtectionText.Rows(read);
        DatabasesDataGrid.ItemsSource = rows;
        EmptyMessage.Text = ViewerDatabaseProtectionText.EmptyText(read);
        EmptyMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    private async void ServerComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_binding) return;

        await LoadAsync();
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await RefreshAsync();
}
