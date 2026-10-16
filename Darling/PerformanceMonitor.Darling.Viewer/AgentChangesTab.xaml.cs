/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Agent changes tab of the Investigations window (#5917): the calls and refusals from the journal, Undo for the selected
/// call, and the switch. A read-only seat sees the list and none of the write controls. Every write is awaited and the list is
/// reloaded from the store. The result of every command goes to <see cref="Status"/>, which the window shows in its status line.
/// </summary>
public partial class AgentChangesTab : UserControl
{
    private ViewerDataService? _dataService;
    private string _login = Environment.UserName;
    private bool _enabled = true;
    private bool _readOnly;

    public AgentChangesTab()
    {
        InitializeComponent();
    }

    /// <summary>The sentence of the last command, for the host's status line.</summary>
    public event Action<string>? Status;

    /// <summary>Gives the tab its store, the seat's role and the login the person's writes are journaled as.</summary>
    internal void Initialize(ViewerDataService dataService, bool readOnly, string login)
    {
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _readOnly = readOnly;
        _login = login;
        UndoPanel.Visibility = readOnly ? Visibility.Collapsed : Visibility.Visible;
        SwitchButton.Visibility = readOnly ? Visibility.Collapsed : Visibility.Visible;
    }

    /// <summary>Reads the list and the switch from the store.</summary>
    internal async Task LoadAsync()
    {
        if (_dataService is null)
            return;
        try
        {
            var read = await _dataService.ReadAgentChangesAsync();
            _enabled = read.Enabled;
            ChangesGrid.ItemsSource = read.Changes.Select(c => new AgentChangeViewRow(c)).ToList();
            RefusalsGrid.ItemsSource = AgentRefusalViewRow.From(read.Refusals);
            ShowSwitch();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not read agent changes: " + ex.Message);
        }
    }

    private void ShowSwitch()
    {
        SwitchText.Text = _enabled
            ? "Agent changes are ON."
            : "Agent changes are OFF: every agent change is refused (actions_off).";
        SwitchButton.Content = _enabled ? "Turn off agent changes" : "Turn on agent changes";
    }

    /// <summary>The check before Undo: what is missing, or null.</summary>
    internal static string? UndoError(AgentChangeViewRow? selected) =>
        selected is null ? "Select a change first."
        : selected.Row.Result == PerformanceMonitor.Darling.Storage.AgentChangeResult.Unknown ? "The result of this call is unknown, so what it changed is unknown and it cannot be undone from here."
        : selected.Row.UndoneAtUtc is not null ? "This change was already undone."
        : !selected.CanUndo ? "This change cannot be undone (it failed, is not a kind that has an undo, or is older than 365 days)."
        : null;

    internal async Task UndoCommandAsync()
    {
        if (_dataService is null)
            return;
        var selected = ChangesGrid.SelectedItem as AgentChangeViewRow;
        if (UndoError(selected) is { } error)
        {
            Status?.Invoke(error);
            return;
        }

        try
        {
            var result = await _dataService.UndoAgentChangeAsync(selected!.CalledId, _login);
            Status?.Invoke(result.Words);
            await LoadAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not undo the change: " + ex.Message);
        }
    }

    internal async Task SwitchCommandAsync()
    {
        if (_dataService is null || _readOnly)
            return;
        var turnOn = !_enabled;
        try
        {
            await _dataService.SetAgentActionsEnabledAsync(turnOn, _login);
            Status?.Invoke(turnOn ? "Agent changes are on." : "Agent changes are off: the gate refuses every agent change as actions_off.");
            await LoadAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not change the switch: " + ex.Message);
        }
    }

    private async void Undo_Click(object sender, RoutedEventArgs e) => await UndoCommandAsync();

    private async void Switch_Click(object sender, RoutedEventArgs e) => await SwitchCommandAsync();
}
