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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Server actions tab of the Investigations window (#6269): each server's agent action switches, with who changed each and when.
/// A read-only seat sees the list and none of the write controls. Turning a switch ON asks first, in plain words, what it lets an agent
/// do; the change goes through the service and the list is read again from the store. The sentence of every command goes to
/// <see cref="Status"/>, which the window shows in its status line.
/// </summary>
public partial class ServerActionsTab : UserControl
{
    private ViewerDataService? _dataService;
    private string _login = Environment.UserName;
    private bool _readOnly;

    public ServerActionsTab()
    {
        InitializeComponent();
    }

    /// <summary>The sentence of the last command, for the host's status line.</summary>
    public event Action<string>? Status;

    /// <summary>Gives the tab its store, the seat's role and the login the person's changes are journaled as.</summary>
    internal void Initialize(ViewerDataService dataService, bool readOnly, string login)
    {
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _readOnly = readOnly;
        _login = login;
        SwitchPanel.Visibility = readOnly ? Visibility.Collapsed : Visibility.Visible;
    }

    /// <summary>Reads every server's switches from the store.</summary>
    internal async Task LoadAsync()
    {
        if (_dataService is null)
            return;
        try
        {
            var selected = ServersGrid.SelectedItem as ServerActionViewRow;
            var rows = ServerActionViewRow.From(await _dataService.ReadServerActionsAsync());
            ServersGrid.ItemsSource = rows;
            if (selected is not null)
            {
                foreach (var row in rows)
                {
                    if (row.ServerId == selected.ServerId && row.Action == selected.Action)
                    {
                        ServersGrid.SelectedItem = row;
                        break;
                    }
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not read the server action switches: " + ex.Message);
        }
    }

    internal async Task ChangeCommandAsync(bool turnOn)
    {
        if (_dataService is null || _readOnly)
            return;
        var selected = ServersGrid.SelectedItem as ServerActionViewRow;
        if (ServerActionViewRow.ChangeError(selected, turnOn) is { } error)
        {
            Status?.Invoke(error);
            return;
        }

        if (turnOn)
        {
            var ask = MessageBox.Show(Window.GetWindow(this), ServerActionViewRow.WarningText(selected!.ServerName, selected.Action),
                "Turn on: " + selected.ActionDisplay + " on " + selected.ServerName, MessageBoxButton.OKCancel, MessageBoxImage.Warning);
            if (ask != MessageBoxResult.OK)
            {
                Status?.Invoke("The switch was not changed.");
                return;
            }
        }

        try
        {
            Status?.Invoke("Asking the service to change the switch...");
            var answer = await _dataService.SetServerActionAsync(selected!.ServerId, selected.Action, turnOn, _login);
            Status?.Invoke(answer.Message);
            await LoadAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not change the switch: " + ex.Message);
        }
    }

    private async void TurnOn_Click(object sender, RoutedEventArgs e) => await ChangeCommandAsync(true);

    private async void TurnOff_Click(object sender, RoutedEventArgs e) => await ChangeCommandAsync(false);
}
