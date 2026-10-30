/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's threshold-overrides manager (#5916 part 4): the overrides, each with the global value it replaces, and a form to
/// add, change or remove one. Opened from the Settings window's Alert rules tab, beside the global thresholds. All checks are
/// <see cref="ThresholdOverridesViewModel"/>'s, so a refused key or bound never reaches the store. Every write is awaited and the
/// list is read again from the store, so the grid shows what was persisted. A read-only seat (<c>postgres.connectAs = "viewer"</c>)
/// sees the grid and none of the form.
/// </summary>
public partial class ThresholdOverridesWindow : Window
{
    internal const string ReadOnlyMessage =
        "Read-only connection (postgres.connectAs = \"viewer\"): overrides can be viewed but not changed here. Set connectAs to \"admin\" and restart the viewer to change them.";

    private readonly ViewerDataService _dataService;
    private readonly ThresholdOverridesViewModel _model;
    private readonly ObservableCollection<ThresholdOverrideViewRow> _rows = new();

    public ThresholdOverridesWindow(ViewerDataService dataService)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _model = new ThresholdOverridesViewModel(dataService, Environment.UserName);
        OverridesGrid.ItemsSource = _rows;
        KeyCombo.ItemsSource = ThresholdOverridesViewModel.Keys;

        if (dataService.IsReadOnly)
        {
            EditPanel.Visibility = Visibility.Collapsed;
            StatusText.Text = ReadOnlyMessage;
        }

        Loaded += async (_, _) => await LoadAsync();
    }

    private void SetStatus(string message, bool isError)
    {
        StatusText.Text = message;
        StatusText.Foreground = (System.Windows.Media.Brush)FindResource(isError ? "WarningTextBrush" : "ForegroundMutedBrush");
    }

    private async Task LoadAsync()
    {
        try
        {
            var servers = await _dataService.GetMonitoredServersAsync();
            var tags = await _dataService.GetServerTagsAsync();
            ServerCombo.ItemsSource = servers
                .OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
                .Select(s => new ThresholdOverrideServerChoice(s.ServerId, s.Name))
                .Concat(tags
                    .OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
                    .Select(t => new ThresholdOverrideServerChoice(0, "Tag: " + t.Name, t.Id)))
                .ToList();
            _rows.Clear();
            foreach (var row in await _model.LoadAsync())
            {
                _rows.Add(row);
            }

            NoOverridesMessage.Visibility = _rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            SetStatus("Could not read the overrides: " + ex.Message, true);
        }
    }

    private void OverridesGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (OverridesGrid.SelectedItem is not ThresholdOverrideViewRow row || _dataService.IsReadOnly)
        {
            return;
        }

        /* Selecting a row loads it into the form, so Save changes it and Remove removes it. */
        ServerCombo.SelectedItem = ServerCombo.Items.OfType<ThresholdOverrideServerChoice>()
            .FirstOrDefault(s => row.IsTag ? s.TagId == row.TagId : s.TagId is null && s.ServerId == row.ServerId);
        KeyCombo.SelectedItem = row.Key;
        DatabaseBox.Text = row.Database ?? "";
        ValueBox.Text = row.Row.Value.ToString("0.##", System.Globalization.CultureInfo.CurrentCulture);
    }

    private async void Save_Click(object sender, RoutedEventArgs e)
    {
        /* An async void handler's exception reaches the dispatcher and ends the process, so every failure the store raises
           that is not the view-model's own refusal (a dropped connection, a timeout, a lock) becomes a status line. */
        try
        {
            var server = ServerCombo.SelectedItem as ThresholdOverrideServerChoice;
            var outcome = server?.TagId is { } tagId
                ? await _model.SaveForTagAsync(tagId, KeyCombo.SelectedItem as string, ValueBox.Text, DatabaseBox.Text)
                : await _model.SaveAsync(server?.ServerId, KeyCombo.SelectedItem as string, ValueBox.Text, DatabaseBox.Text);
            SetStatus(outcome.Message, !outcome.Ok);
            if (outcome.Ok)
            {
                await LoadAsync();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            SetStatus("Could not save the override: " + ex.Message, true);
        }
    }

    private async void Remove_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            var outcome = await _model.RemoveAsync(OverridesGrid.SelectedItem as ThresholdOverrideViewRow);
            SetStatus(outcome.Message, !outcome.Ok);
            if (outcome.Ok)
            {
                await LoadAsync();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            SetStatus("Could not remove the override: " + ex.Message, true);
        }
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
