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
using PerformanceMonitor.Darling.Storage;

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
        /* #7109: a read-only seat cannot ask the service for anything, so it sees no proposals. */
        var proposals = readOnly ? Visibility.Collapsed : Visibility.Visible;
        ProposalsLabel.Visibility = proposals;
        ProposalsGrid.Visibility = proposals;
        ProposalsPanel.Visibility = proposals;
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

        await LoadProposalsAsync();
    }

    /// <summary>Reads the proposals through the service (#7109). A service that does not answer leaves the list as it was and says so.</summary>
    internal async Task LoadProposalsAsync()
    {
        if (_dataService is null || _readOnly)
            return;
        try
        {
            var rows = await _dataService.ReadProposalsAsync(null, _login);
            if (rows is null)
                Status?.Invoke("The service has not listed the proposals yet. Reload the tab to read them again.");
            else
                ProposalsGrid.ItemsSource = rows;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not read the proposals: " + ex.Message);
        }
    }

    /// <summary>The words of the confirm question: what the first press read, and what the confirm does.</summary>
    internal static string ApproveConfirmText(ProposalViewRow row, ProposalPressAnswer first) =>
        first.Message + Environment.NewLine + Environment.NewLine + row.Kind + " on " + row.Server + Environment.NewLine
        + (first.FactsText.Length > 0 ? first.FactsText + Environment.NewLine : "") + Environment.NewLine
        + "The agent's reason (the agent's own words): " + (row.Reason.Length > 0 ? row.Reason : "none given") + Environment.NewLine + Environment.NewLine
        + "Run this action on the monitored server?";

    /// <summary>The first press shows the fresh facts; the confirm runs only after the person says yes to them.</summary>
    internal async Task ApproveCommandAsync()
    {
        if (_dataService is null || _readOnly)
            return;
        var selected = ProposalsGrid.SelectedItem as ProposalViewRow;
        if (ProposalViewRow.ApproveError(selected) is { } error)
        {
            Status?.Invoke(error);
            return;
        }

        try
        {
            Status?.Invoke("Asking the service to read the facts again...");
            var first = await _dataService.PressProposalAsync(selected!.ProposalId, confirm: false, _login);
            if (!first.FirstPressed)
            {
                Status?.Invoke(first.Words);
                await LoadProposalsAsync();
                return;
            }

            var ask = MessageBox.Show(Window.GetWindow(this), ApproveConfirmText(selected, first), "Approve agent action", MessageBoxButton.OKCancel, MessageBoxImage.Question);
            if (ask != MessageBoxResult.OK)
            {
                Status?.Invoke("Not approved. The proposal is still waiting.");
                return;
            }

            Status?.Invoke("Confirming...");
            var done = await _dataService.PressProposalAsync(selected.ProposalId, confirm: true, _login);
            Status?.Invoke(done.Words);
            await LoadAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not approve the proposal: " + ex.Message);
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
        : selected.Row.UndoneAtUtc is not null ? "This change was already undone."
        /* A plan force or unforce is offered whatever its result: the undo reads the server first and decides from what it finds (#5632, step 9). */
        : selected.Row.Result == PerformanceMonitor.Darling.Storage.AgentChangeResult.Unknown && !selected.Row.IsPlanChange ? "The result of this call is unknown, so what it changed is unknown and it cannot be undone from here."
        : !selected.CanUndo ? "This change cannot be undone (it failed, is not a kind that has an undo, or is older than 365 days)."
        : null;

    /// <summary>The words of a plan undo's confirm step: what the service read, the exact statement, and the question.</summary>
    internal static string ConfirmText(PerformanceMonitor.Darling.Storage.PlanUndoAnswer answer) =>
        answer.Message + Environment.NewLine + Environment.NewLine + StateText(answer.State) + Environment.NewLine + Environment.NewLine
        + (answer.Statement ?? "") + Environment.NewLine + Environment.NewLine + "Run this statement on the monitored server?";

    /// <summary>The state the service read, in a line: the plan forced now, or that none is.</summary>
    internal static string StateText(System.Text.Json.Nodes.JsonNode? state)
    {
        var forced = state?["forced_plan"];
        return forced is null
            ? "On the server now: no plan of the query is forced."
            : "On the server now: plan " + forced["plan_id"] + " is forced (" + forced["forcing_type"] + ").";
    }

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
            if (selected!.Row.IsPlanChange)
            {
                await UndoPlanChangeAsync(selected.CalledId);
                return;
            }

            var result = await _dataService.UndoAgentChangeAsync(selected.CalledId, _login);
            Status?.Invoke(result.Words);
            await LoadAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not undo the change: " + ex.Message);
        }
    }

    /// <summary>The first command reads the server and answers the statement; the second runs only after the person confirms it.</summary>
    private async Task UndoPlanChangeAsync(long calledId)
    {
        Status?.Invoke("Asking the service to read the server...");
        var shown = await _dataService!.UndoPlanChangeAsync(calledId, _login, confirm: false, statement: null);
        if (shown.Status != PerformanceMonitor.Darling.Storage.PlanUndoAnswer.Confirm)
        {
            Status?.Invoke(shown.Message);
            await LoadAsync();
            return;
        }

        var ask = MessageBox.Show(Window.GetWindow(this), ConfirmText(shown), "Undo plan change", MessageBoxButton.OKCancel, MessageBoxImage.Question);
        if (ask != MessageBoxResult.OK)
        {
            Status?.Invoke("Not undone.");
            return;
        }

        var done = await _dataService.UndoPlanChangeAsync(calledId, _login, confirm: true, statement: shown.Statement);
        Status?.Invoke(done.Message);
        await LoadAsync();
    }

    internal async Task SwitchCommandAsync()
    {
        if (_dataService is null || _readOnly)
            return;
        var turnOn = !_enabled;
        try
        {
            /* The write waits for an agent change in progress to finish, up to a minute (#5632), so say so while it does. */
            Status?.Invoke("Waiting for an agent change in progress to finish, if there is one...");
            if (!await _dataService.SetAgentActionsEnabledAsync(turnOn, _login))
            {
                Status?.Invoke(AgentChangeUndo.SwitchStoreNotSetUpMessage);
                return;
            }

            Status?.Invoke(turnOn ? "Agent changes are on." : "Agent changes are off: the gate refuses every agent change as actions_off.");
            await LoadAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Status?.Invoke("Could not change the switch: " + ex.Message);
        }
    }

    private async void Undo_Click(object sender, RoutedEventArgs e) => await UndoCommandAsync();

    private async void Approve_Click(object sender, RoutedEventArgs e) => await ApproveCommandAsync();

    private async void Switch_Click(object sender, RoutedEventArgs e) => await SwitchCommandAsync();
}
