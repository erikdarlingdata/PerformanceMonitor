/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// Per-database expected-state editor for the baseline-deviation database-state alert. Lists every
/// database in the selected server's latest snapshot with its current state and its expected state
/// (auto-seeded baseline or user override), lets the operator change the expected state — including
/// the <c>(ignore)</c> opt-out — and re-baseline a database to its current state. Writes go straight
/// to <c>config_database_state_expected</c> via <see cref="LocalDataService"/>.
/// </summary>
public partial class DatabaseStateOverridesWindow : Window
{
    private readonly ServerManager _serverManager;
    private readonly LocalDataService _dataService;

    private sealed class ServerPick
    {
        public string DisplayName { get; set; } = "";
        public int ServerId { get; set; }
    }

    private sealed class EditRow
    {
        public string DatabaseName { get; set; } = "";
        public string CurrentState { get; set; } = "";
        public string ExpectedState { get; set; } = "";
        public string OriginalExpected { get; set; } = "";
        public bool IsUserOverride { get; set; }

        public string SourceLabel =>
            string.IsNullOrEmpty(ExpectedState) ? "Pending"
            : string.Equals(ExpectedState, DatabaseStateTokens.Ignore, StringComparison.Ordinal) ? "Ignored"
            : IsUserOverride ? "Override"
            : "Baseline";
    }

    private List<EditRow> _rows = new();

    public DatabaseStateOverridesWindow(ServerManager serverManager)
    {
        InitializeComponent();
        _serverManager = serverManager;
        _dataService = new LocalDataService(new DuckDbInitializer(App.DatabasePath));

        var picks = _serverManager.GetAllServers()
            .Select(s => new ServerPick
            {
                DisplayName = s.DisplayName,
                ServerId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s))
            })
            .OrderBy(p => p.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        ServerCombo.ItemsSource = picks;
        if (picks.Count > 0)
        {
            ServerCombo.SelectedIndex = 0; // triggers the initial load
        }
        else
        {
            StatusText.Text = "No servers are configured.";
        }
    }

    private int? SelectedServerId => (ServerCombo.SelectedItem as ServerPick)?.ServerId;

    private readonly PerformanceMonitor.Ui.ScopedLoadGenerations _loads = new();

    private async void ServerCombo_SelectionChanged(object sender, SelectionChangedEventArgs e) => await LoadAsync();

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await LoadAsync();

    /// <summary>
    /// Loads the selected server's database states into the grid.
    ///
    /// <para><b>The scope is captured before the read and verified after it</b> (#2924). One grid, one
    /// dropdown and one <see cref="_rows"/> model carry every server's answer, and
    /// <see cref="ServerCombo_SelectionChanged"/> starts a load without a single-flight guard — so two
    /// combo changes inside one store read leave two loads in flight and the last to COMPLETE painting,
    /// which need not be the last one requested.</para>
    ///
    /// <para>Verifying rather than replaying, because the combo change is itself the re-read: the handler
    /// has already started a load for the new server, so a mismatching answer is stale by definition and
    /// its replacement is in flight. Dropping leaves the newest load — the only one whose scope can still
    /// match when it lands — to paint.</para>
    ///
    /// <para><see cref="_rows"/> is the reason this is not only cosmetic. It is the model
    /// <see cref="Save_Click"/> writes from, while that method resolves <see cref="SelectedServerId"/>
    /// fresh: rows belonging to one server, edited under another server's selection, would be saved
    /// against the server the combo names.</para>
    ///
    /// <para><b>And the read is ordered by generation</b> (#2933), because the scope verify above cannot
    /// separate two reads that carry the SAME scope: after server A to B and back to A, the third read's
    /// scope equals the first's, so an answer still in flight for the first compares current and lands
    /// last. The two tests are not redundant — a load can be the newest and still answer for a departed
    /// scope, and a load can carry a matching scope and still be superseded. This one is the same
    /// generation idiom Lite's picker charts use (<c>ServerTab.Pickers.cs</c>).</para>
    /// </summary>
    private async System.Threading.Tasks.Task LoadAsync()
    {
        if (SelectedServerId is not int serverId)
        {
            return;
        }

        var gen = _loads.Claim(nameof(LoadAsync));

        try
        {
            var rows = await _dataService.GetDatabaseStateExpectationsAsync(serverId);

            /* A newer load for this grid has started, so this answer is not the one the operator is
               waiting for even if the combo came back to the same server. */
            if (_loads.Superseded(nameof(LoadAsync), gen))
            {
                return;
            }

            /* The combo moved while the read was in flight, so this answer is for a server the operator has
               left. Drop it ahead of every paint below, _rows included. */
            if (SelectedServerId != serverId)
            {
                return;
            }

            _rows = rows.Select(r => new EditRow
            {
                DatabaseName = r.DatabaseName,
                CurrentState = r.CurrentState,
                ExpectedState = string.IsNullOrEmpty(r.ExpectedState) ? r.CurrentState : r.ExpectedState,
                OriginalExpected = string.IsNullOrEmpty(r.ExpectedState) ? r.CurrentState : r.ExpectedState,
                IsUserOverride = r.IsUserOverride
            }).ToList();

            /* The dropdown offers the standard state tokens plus (ignore), unioned with any state value
               actually present (a baseline or current state the token list doesn't enumerate, e.g. COPYING). */
            var options = new List<string>
            {
                DatabaseStateTokens.Online, DatabaseStateTokens.Offline, DatabaseStateTokens.Restoring,
                DatabaseStateTokens.Recovering, DatabaseStateTokens.RecoveryPending, DatabaseStateTokens.Suspect,
                DatabaseStateTokens.Emergency, DatabaseStateTokens.Standby, DatabaseStateTokens.Ignore
            };
            foreach (var extra in _rows.SelectMany(r => new[] { r.ExpectedState, r.CurrentState })
                         .Where(s => !string.IsNullOrEmpty(s) && !options.Contains(s)))
            {
                options.Add(extra);
            }
            ExpectedColumn.ItemsSource = options;

            StatesGrid.ItemsSource = _rows;
            var deviating = _rows.Count(r => !string.Equals(r.ExpectedState, DatabaseStateTokens.Ignore, StringComparison.Ordinal)
                && !string.Equals(r.CurrentState, r.ExpectedState, StringComparison.Ordinal));
            StatusText.Text = $"{_rows.Count} database(s); {deviating} currently deviating from expected.";
        }
        catch (Exception ex)
        {
            /* Both of the success path's tests, for the same reasons: a failure reading the departed
               server's states must not blank the grid the operator is now looking at, and a superseded
               load's failure must not overwrite the newest load's answer. */
            if (_loads.Superseded(nameof(LoadAsync), gen))
            {
                return;
            }

            if (SelectedServerId != serverId)
            {
                return;
            }

            StatesGrid.ItemsSource = null;
            StatusText.Text = $"Could not load database states: {ex.Message}";
        }
    }

    private async void ResetSelected_Click(object sender, RoutedEventArgs e)
    {
        if (SelectedServerId is not int serverId || StatesGrid.SelectedItem is not EditRow row)
        {
            return;
        }

        try
        {
            await _dataService.ResetDatabaseStateExpectedToCurrentAsync(serverId, row.DatabaseName);
            await LoadAsync();
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Could not reset {row.DatabaseName}: {ex.Message}";
        }
    }

    private async void Save_Click(object sender, RoutedEventArgs e)
    {
        StatesGrid.CommitEdit(DataGridEditingUnit.Cell, true);
        StatesGrid.CommitEdit(DataGridEditingUnit.Row, true);

        if (SelectedServerId is not int serverId)
        {
            DialogResult = true;
            return;
        }

        try
        {
            var changed = _rows.Where(r => !string.Equals(r.ExpectedState, r.OriginalExpected, StringComparison.Ordinal)).ToList();
            foreach (var row in changed)
            {
                await _dataService.SetDatabaseStateExpectedAsync(serverId, row.DatabaseName, row.ExpectedState);
            }

            DialogResult = true;
        }
        catch (Exception ex)
        {
            StatusText.Text = $"Could not save: {ex.Message}";
        }
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;
}
