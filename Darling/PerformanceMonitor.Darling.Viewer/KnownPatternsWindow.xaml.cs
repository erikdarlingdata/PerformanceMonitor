/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's known-pattern manager (#6334): list, add, edit and delete over the entries, <see cref="MuteRulesWindow"/>'s shape.
/// Writes go straight to Postgres through <see cref="ViewerDataService"/>; each write is awaited and the list reloaded from the
/// store, so the grid shows what was persisted. A read-only seat can view but not change them, and a store older than V214 has no
/// table: the window then says so and offers nothing to change.
/// </summary>
public partial class KnownPatternsWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly ObservableCollection<KnownPatternRow> _rows = new();
    private IReadOnlyList<(int ServerId, string Name)> _servers = Array.Empty<(int, string)>();

    public KnownPatternsWindow(ViewerDataService dataService)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        PatternsGrid.ItemsSource = _rows;

        if (dataService.IsReadOnly)
        {
            ShowOnly("Read-only connection: known patterns can be viewed but not changed here. " + ViewerReadOnlyException.HowToConnectAsAdmin);
        }

        Loaded += async (_, _) => await LoadAsync();
    }

    private void ShowOnly(string banner)
    {
        ActionButtonsPanel.Visibility = Visibility.Collapsed;
        StatusBanner.Text = banner;
        StatusBanner.Visibility = Visibility.Visible;
    }

    private async Task LoadAsync()
    {
        try
        {
            if (!await _dataService.KnownPatternsSupportedAsync())
            {
                ShowOnly(ViewerDataService.KnownPatternsNeedNewerStoreMessage);
                NoPatternsMessage.Visibility = Visibility.Visible;
                return;
            }

            var servers = await _dataService.GetMonitoredServersAsync();
            _servers = servers.Select(s => (s.ServerId, s.Name)).ToList();
            var names = _servers.ToDictionary(s => s.ServerId, s => s.Name);

            var entries = await _dataService.GetKnownPatternsAsync();
            _rows.Clear();
            foreach (var entry in entries)
            {
                _rows.Add(new KnownPatternRow(entry, names));
            }

            NoPatternsMessage.Visibility = _rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            ShowError("Could not read known patterns", ex);
        }
    }

    private async void Add_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new KnownPatternEditDialog(_servers) { Owner = this };
        if (dialog.ShowDialog() != true || dialog.Input is null)
        {
            return;
        }

        try
        {
            await _dataService.InsertKnownPatternAsync(dialog.Input);
            await LoadAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not save the known pattern", ex);
        }
    }

    private async void Edit_Click(object sender, RoutedEventArgs e)
    {
        if (PatternsGrid.SelectedItem is not KnownPatternRow selected || _dataService.IsReadOnly || ActionButtonsPanel.Visibility != Visibility.Visible)
        {
            return;
        }

        var dialog = new KnownPatternEditDialog(_servers, selected.Entry) { Owner = this };
        if (dialog.ShowDialog() != true || dialog.Input is null)
        {
            return;
        }

        try
        {
            if (!await _dataService.UpdateKnownPatternAsync(selected.Id, dialog.Input))
            {
                MessageBox.Show("That known pattern no longer exists. It was probably deleted by another operator.", "Known Patterns",
                    MessageBoxButton.OK, MessageBoxImage.Information);
            }

            await LoadAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not update the known pattern", ex);
        }
    }

    private async void Delete_Click(object sender, RoutedEventArgs e)
    {
        if (PatternsGrid.SelectedItem is not KnownPatternRow selected)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Delete this known pattern?\n\n{selected.Note}",
            "Confirm Delete", MessageBoxButton.YesNo, MessageBoxImage.Question);
        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.DeleteKnownPatternAsync(selected.Id);
            await LoadAsync();
        }
        catch (Exception ex)
        {
            ShowError("Could not delete the known pattern", ex);
        }
    }

    private void PatternsGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e) => Edit_Click(sender, e);

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowError(string what, Exception ex) =>
        MessageBox.Show($"{what}: {ex.Message}", "Known Patterns", MessageBoxButton.OK, MessageBoxImage.Warning);
}
