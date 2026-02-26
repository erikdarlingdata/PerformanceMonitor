/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Input;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class ManageServersWindow : Window
{
    private readonly ServerManager _serverManager;

    /// <summary>
    /// Set to true if servers were modified so the caller knows to refresh.
    /// </summary>
    public bool ServersChanged { get; private set; }

    public ManageServersWindow(ServerManager serverManager)
    {
        InitializeComponent();
        _serverManager = serverManager;
        RefreshGrid();
    }

    private void RefreshGrid()
    {
        ServersGrid.ItemsSource = null;
        ServersGrid.ItemsSource = _serverManager.GetAllServers();
    }

    private void AddButton_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new AddServerDialog(_serverManager) { Owner = this };
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

        var dialog = new AddServerDialog(_serverManager, selected) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ServersChanged = true;
            RefreshGrid();
        }
    }

    private void DeleteButton_Click(object sender, RoutedEventArgs e)
    {
        if (ServersGrid.SelectedItem is not ServerConnection selected)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Delete server '{selected.DisplayName}'?\n\nThis will remove the server and its stored credentials.",
            "Delete Server",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (result == MessageBoxResult.Yes)
        {
            _serverManager.DeleteServer(selected.Id);
            ServersChanged = true;
            RefreshGrid();
        }
    }

    private void CopyCell_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) => Helpers.ContextMenuHelper.ExportToCsv(sender, "servers");

    private void CloseButton_Click(object sender, RoutedEventArgs e)
    {
        Close();
    }
}
