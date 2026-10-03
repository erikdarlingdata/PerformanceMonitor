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
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's collector-schedule editor — a faithful port of Lite's <c>CollectorScheduleEditorWindow</c>,
/// rewired onto the control-plane store: it edits the fleet-wide default schedule (<c>server_id</c> NULL) or a
/// single server's override, overlays the store's sparse <c>config.config_collector_schedules</c> rows on the
/// shared <see cref="CollectorSchedulePresets.BuildDefaultSchedule"/> baseline, and writes the result back through
/// <see cref="ViewerDataService.SaveCollectorScheduleAsync"/>.
/// Presets change frequencies only (enabled + retention untouched), exactly as Lite. A read-only seat shows a
/// banner and disables the writes.
///
/// <para>The Run at column (#4938) is read from, and written to, <c>config.config_collector_run_times</c>, a table of its
/// own: the schedule Save deletes a scope's schedule rows and inserts them again, and a run time kept on those rows
/// would be cleared by it. So Save writes the run-time changes as statements of their own, then the schedule rows exactly as
/// <see cref="ViewerDataService.ReplaceFleetSchedulesAsync"/> / <see cref="ViewerDataService.ReplaceServerSchedulesAsync"/>
/// always have, all in ONE call and ONE transaction (<see cref="ViewerDataService.SaveCollectorScheduleAsync"/>), so a Save
/// that fails leaves neither the run times nor the schedule rows written.</para>
///
/// <para>A schedule Reset also deletes that scope's run times, in the same transaction as the schedule rows: the shipped defaults
/// have no fixed time. "Reset to Defaults" and a server's "Use default schedule" are saved with the scope's run-time rows cleared
/// first, and "Apply Default to All Servers" deletes every server's schedule rows and run times in one call.</para>
/// </summary>
public partial class CollectorScheduleEditorWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly IReadOnlyList<DarlingServer> _servers;

    private List<CollectorScheduleRow> _allOverrides = new();
    private List<CollectorRunTimeRow> _runTimes = new();
    private List<CollectorScheduleEditItem> _editing = new();
    private int? _scopeServerId;           // null = fleet-wide default scope
    private bool _resetToDefaults;         // Reset to Defaults was confirmed for this scope and not saved yet (#4938)
    private bool _suppressPresetChange;
    private bool _suppressScopeReload;

    /* What the Run at line needs to know about the scope's server (#4938): its clock from the newest
       server_properties row (null until it has one, which reads the time as UTC) and whether it is an Azure SQL
       Database, which always reports UTC. Both are read once per scope and stay null/false on the fleet scope. */
    private ServerClock? _scopeClock;
    private bool _scopeIsAzureSqlDatabase;

    /// <summary>True when the user saved changes (the caller then re-reads if it cares).</summary>
    public bool Saved { get; private set; }

    public CollectorScheduleEditorWindow(ViewerDataService dataService, IReadOnlyList<DarlingServer> servers)
    {
        ArgumentNullException.ThrowIfNull(dataService);
        ArgumentNullException.ThrowIfNull(servers);

        InitializeComponent();
        _dataService = dataService;
        _servers = servers;

        Loaded += OnLoaded;
    }

    private async void OnLoaded(object sender, RoutedEventArgs e)
    {
        Loaded -= OnLoaded;

        if (_dataService.IsReadOnly)
        {
            ReadOnlyBanner.Visibility = Visibility.Visible;
            SaveButton.IsEnabled = false;
            ApplyDefaultToAllButton.IsEnabled = false;
        }

        PopulateScopeCombos();

        /* The delta cadence cap (#3532), from the shared constants so the shown numbers and collector
           list can never drift from what ValidateSchedule enforces. */
        var deltaNames = CollectorSchedulePresets.BuildDefaultSchedule()
            .Where(s => CollectorDeltaCalculator.IsDeltaFamily(s.Name))
            .Select(s => s.Name);
        FooterHintText.Text +=
            $" Delta collectors ({string.Join(", ", deltaNames)}) accept at most {CollectorDeltaCalculator.MaxDeltaFrequencyMinutes} minutes: " +
            $"past the {CollectorDeltaCalculator.DefaultMaxGapSeconds / 60}-minute delta gap policy every reading would be discarded as stale and recorded as zero.";

        try
        {
            _allOverrides = await _dataService.GetCollectorSchedulesAsync();
        }
        catch (Exception ex)
        {
            _allOverrides = new List<CollectorScheduleRow>();
            StatusText.Text = $"Could not read the current schedules: {ex.Message}";
        }

        await ReloadRunTimesAsync();

        /* Default to the fleet scope (index 0). */
        _suppressScopeReload = true;
        ScopeCombo.SelectedIndex = 0;
        _suppressScopeReload = false;
        LoadScopeSchedule();
    }

    /// <summary>
    /// Reads the run times (#4938) from their own table. A store below V160 has no such table, and
    /// <see cref="ViewerDataService.GetCollectorRunTimesAsync"/> answers that with no run times, so the editor opens with every
    /// Run at cell on "Use default". Any other failure is said in the status line (appended, so it does not replace the
    /// schedule read's) and leaves no run times shown, which a Save then treats as nothing stored for them.
    /// </summary>
    private async Task ReloadRunTimesAsync()
    {
        try
        {
            _runTimes = await _dataService.GetCollectorRunTimesAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _runTimes = new List<CollectorRunTimeRow>();
            StatusText.Text += $" Could not read the run times: {ex.Message}";
        }
    }

    private void PopulateScopeCombos()
    {
        ScopeCombo.Items.Clear();
        ScopeCombo.Items.Add(new ComboBoxItem { Content = "All servers (default schedule)", Tag = null });
        foreach (var server in _servers)
        {
            ScopeCombo.Items.Add(new ComboBoxItem { Content = server.DisplayName, Tag = server.ServerId });
        }

        CopyFromServerCombo.ItemsSource = _servers;
        CopyFromServerCombo.DisplayMemberPath = nameof(DarlingServer.DisplayName);
        if (_servers.Count > 0)
        {
            CopyFromServerCombo.SelectedIndex = 0;
        }
        else
        {
            CopyFromServerCombo.IsEnabled = false;
            CopyFromServerButton.IsEnabled = false;
        }
    }

    private void ScopeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressScopeReload)
        {
            return;
        }

        LoadScopeSchedule();
    }

    private void LoadScopeSchedule()
    {
        _scopeServerId = (ScopeCombo.SelectedItem as ComboBoxItem)?.Tag as int?;
        _resetToDefaults = false;   /* a Reset belongs to the scope it was made in, and a reload shows what is stored */

        var isServerScope = _scopeServerId is not null;
        UseDefaultCheckBox.Visibility = isServerScope ? Visibility.Visible : Visibility.Collapsed;

        if (isServerScope)
        {
            HeaderText.Text = $"Collector Schedules — {(ScopeCombo.SelectedItem as ComboBoxItem)?.Content}";
            SubHeaderText.Text = "This server's custom schedule. Uncheck 'Use default schedule' to override the fleet-wide defaults for it.";
            var hasOverride = CollectorScheduleOverlay.ServerHasOverride(_allOverrides, _runTimes, _scopeServerId!.Value);

            _suppressPresetChange = true;
            UseDefaultCheckBox.IsChecked = !hasOverride;
            _suppressPresetChange = false;
        }
        else
        {
            HeaderText.Text = "Collector Schedules — All servers";
            SubHeaderText.Text = "The fleet-wide default schedule. Every server without its own override collects on this schedule.";
        }

        /* The run time is read on the scope's own server clock, so a server scope reads that server's clock and engine
           (the fleet scope has no one clock: its line shows the spread instead). */
        _scopeClock = null;
        _scopeIsAzureSqlDatabase = false;

        RebuildEditingForScope();

        if (_scopeServerId is int scopeServer)
        {
            _ = LoadScopeContextAsync(scopeServer);
        }
    }

    /// <summary>
    /// Reads the scope server's clock (its newest <c>server_properties</c> row) and whether it is an Azure SQL Database, then
    /// refreshes the Run at line. Best effort: a read that fails leaves the clock unknown, which the line says (the time reads
    /// as UTC), and nothing here blocks the grid. A scope the user has moved away from meanwhile is left alone.
    /// </summary>
    private async Task LoadScopeContextAsync(int serverId)
    {
        ServerClock? clock = null;
        var azureSqlDatabase = false;
        try
        {
            var clocks = await _dataService.GetServerClocksAsync(serverId, CancellationToken.None);
            clock = clocks.TryGetValue(serverId, out var found) ? found : null;
            azureSqlDatabase = await _dataService.GetRecommendationEngineEditionAsync(serverId)
                == CollectorEngineCapability.AzureSqlDatabaseEngineEdition;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            StatusText.Text += $" Could not read this server's clock: {ex.Message}";
        }

        if (_scopeServerId != serverId)
        {
            return;
        }

        _scopeClock = clock;
        _scopeIsAzureSqlDatabase = azureSqlDatabase;
        RefreshRunAtDetail();
    }

    private void ScheduleGrid_SelectionChanged(object sender, SelectionChangedEventArgs e) => RefreshRunAtDetail();

    /// <summary>A cell edit is committed after this event, so the line is refreshed once it has been.</summary>
    private void ScheduleGrid_CellEditEnding(object sender, DataGridCellEditEndingEventArgs e) =>
        Dispatcher.BeginInvoke(new Action(RefreshRunAtDetail), DispatcherPriority.Background);

    /// <summary>
    /// The line under the grid for the selected row (#4938): what its Run at time means on the server's clock and in UTC,
    /// the spread on a fleet row, the next run on a server row, and the refusal text when the cell holds something the save
    /// would refuse. All of it comes from <see cref="CollectorScheduleRunAtText.Describe"/>, which is built on the shared
    /// run-time rules.
    /// </summary>
    private void RefreshRunAtDetail()
    {
        if (ScheduleGrid.SelectedItem is not CollectorScheduleEditItem item)
        {
            RunAtDetailText.Text = "Select a collector to see what its Run at time means on its server's clock.";
            return;
        }

        int? fleetRunAt = _runTimes
            .FirstOrDefault(r => r.ServerId is null && string.Equals(r.CollectorName, item.Name, StringComparison.OrdinalIgnoreCase))
            ?.RunAtMinute;

        /* #4938: a server on "Use default schedule" shows the FLEET's schedule in the grid, so the cell holds the fleet's own time.
           The line reads it as the server falling through to that time and says it uses the fleet-wide run time. */
        var usesDefaultSchedule = _scopeServerId is not null && UseDefaultCheckBox.IsChecked == true;

        try
        {
            RunAtDetailText.Text = $"{item.Name}: " + CollectorScheduleRunAtText.Describe(
                item.Name, item.RunAtText, item.FrequencyMinutes, _scopeServerId, fleetRunAt,
                _scopeClock, _scopeIsAzureSqlDatabase, DateTime.UtcNow, usesDefaultSchedule: usesDefaultSchedule);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or FormatException)
        {
            RunAtDetailText.Text = $"{item.Name}: could not work out this run time ({ex.Message}).";
        }
    }

    /// <summary>Rebuilds the editable list for the current scope + use-default state and re-binds the grid.</summary>
    private void RebuildEditingForScope()
    {
        var usesDefault = _scopeServerId is not null && UseDefaultCheckBox.IsChecked == true;

        /* Fleet scope, or a server "using default", shows the fleet-over-default effective (server rows
           excluded); a customizing server shows its own effective schedule. */
        var overlayScope = usesDefault ? (int?)null : _scopeServerId;
        _editing = CollectorScheduleOverlay.BuildEffectiveSchedule(_allOverrides, _runTimes, overlayScope);

        BindGrid();
        UpdateEditableState(!usesDefault);
        DetectActivePreset();
    }

    private void BindGrid()
    {
        ScheduleGrid.ItemsSource = null;
        ScheduleGrid.ItemsSource = _editing;
        RefreshRunAtDetail();
    }

    private void UpdateEditableState(bool editable)
    {
        var writable = editable && !_dataService.IsReadOnly;
        ScheduleGrid.IsReadOnly = !writable;
        ScheduleGrid.Opacity = writable ? 1.0 : 0.6;
        PresetCombo.IsEnabled = writable;
        ResetDefaultsButton.IsEnabled = writable;
        CopyFromServerCombo.IsEnabled = writable && _servers.Count > 0;
        CopyFromServerButton.IsEnabled = writable && _servers.Count > 0;

        if (_dataService.IsReadOnly)
        {
            StatusText.Text = "Read-only connection — schedules can't be changed.";
        }
        else if (_scopeServerId is null)
        {
            StatusText.Text = "Fleet-wide default schedule. Applies to every server without its own override.";
        }
        else
        {
            StatusText.Text = writable
                ? "Custom schedule. Changes apply only to this server."
                : "Using the fleet-wide default (read-only). Uncheck 'Use default schedule' to customize this server.";
        }
    }

    private void UseDefaultCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        if (_suppressPresetChange || _scopeServerId is null)
        {
            return;
        }

        RebuildEditingForScope();
    }

    private void DetectActivePreset()
    {
        _suppressPresetChange = true;
        try
        {
            var active = CollectorSchedulePresets.DetectPreset(_editing);
            for (var i = 0; i < PresetCombo.Items.Count; i++)
            {
                if (PresetCombo.Items[i] is ComboBoxItem item &&
                    string.Equals(item.Content?.ToString(), active, StringComparison.OrdinalIgnoreCase))
                {
                    PresetCombo.SelectedIndex = i;
                    return;
                }
            }

            PresetCombo.SelectedIndex = 0; /* Custom */
        }
        finally
        {
            _suppressPresetChange = false;
        }
    }

    private void PresetCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressPresetChange || PresetCombo.SelectedItem is not ComboBoxItem selected)
        {
            return;
        }

        var presetName = selected.Content?.ToString() ?? "";
        if (presetName == CollectorSchedulePresets.Custom)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Apply the \"{presetName}\" preset?\n\nThis changes all collection frequencies. Enabled/disabled state and retention are not affected.",
            "Apply Collection Preset", MessageBoxButton.YesNo, MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
        {
            DetectActivePreset();
            return;
        }

        CollectorSchedulePresets.ApplyPreset(_editing, presetName);
        BindGrid();
        DetectActivePreset();
    }

    private void ResetDefaults_Click(object sender, RoutedEventArgs e)
    {
        var result = MessageBox.Show(
            "Replace the current values with the built-in default frequencies and retention, and clear every run time?",
            "Reset to Defaults", MessageBoxButton.YesNo, MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        _editing = CollectorSchedulePresets.BuildDefaultSchedule();
        /* #4938: back to the shipped defaults means no fixed time, so the Save deletes this scope's run-time rows by scope, in
           its own transaction with the schedule rows, and not only the ones this window loaded. */
        _resetToDefaults = true;
        BindGrid();
        DetectActivePreset();
    }

    private void CopyFromServer_Click(object sender, RoutedEventArgs e)
    {
        if (CopyFromServerCombo.SelectedItem is not DarlingServer source)
        {
            return;
        }

        var result = MessageBox.Show(
            $"Replace the current values with a copy of {source.DisplayName}'s effective schedule?",
            "Copy from Server", MessageBoxButton.YesNo, MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        _editing = CollectorScheduleOverlay.BuildEffectiveSchedule(_allOverrides, _runTimes, source.ServerId);
        BindGrid();
        DetectActivePreset();
    }

    private async void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        /* Flush any in-progress cell edit into the bound items before we read them. */
        ScheduleGrid.CommitEdit(DataGridEditingUnit.Row, true);

        var usesDefault = _scopeServerId is not null && UseDefaultCheckBox.IsChecked == true;

        if (!usesDefault && !CollectorScheduleOverlay.ValidateSchedule(_editing, out var error))
        {
            MessageBox.Show(error, "Collector Schedules", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        SaveButton.IsEnabled = false;
        try
        {
            /* #4938: the run times go to their own table, each change a statement of its own and never through the
               schedule rows' delete-and-reinsert. ONE call writes both on one connection in one transaction, so a Save
               whose schedule write fails does not leave the run time saved, and a store that cannot take the run times
               (below V160, which the viewer says in its own words) is refused with nothing written. A Save that did not
               change a run time sends none, so it still saves its schedules on such a store. */
            /* A schedule Reset (Reset to Defaults, or a server on "Use default schedule") also deletes this scope's run-time
               rows, by scope and in the same transaction, because the shipped defaults have no fixed time. The changes are then
               worked out against an empty scope: no delete for a "Use default" cell, and an insert for every time the grid holds. */
            var clearRunTimes = _resetToDefaults || usesDefault;
            var runTimeChanges = CollectorScheduleOverlay.ToRunTimeChanges(_editing, _runTimes, _scopeServerId, usesDefault, clearRunTimes);
            var rows = _scopeServerId is int serverId
                ? (usesDefault
                    ? new List<CollectorScheduleRow>()
                    : CollectorScheduleOverlay.ToServerOverrideRows(_editing, serverId))
                : CollectorScheduleOverlay.ToFleetOverrideRows(_editing);
            await _dataService.SaveCollectorScheduleAsync(_scopeServerId, rows, runTimeChanges, clearRunTimes);

            Saved = true;
            Close();
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (ViewerSchemaSkewException ex)
        {
            MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not save the collector schedules:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            SaveButton.IsEnabled = !_dataService.IsReadOnly;
        }
    }

    /// <summary>
    /// "Apply Default to All Servers" — the fleet-scale bulk reset: removes EVERY server's per-server schedule
    /// override in one write (<see cref="ViewerDataService.ResetAllServerSchedulesAsync"/>) so they all fall back
    /// to the fleet-wide default, the shortcut over reverting each server one at a time. The fleet-wide default
    /// itself is untouched. Confirmed first (it clears every server's customization), then the editor re-reads so
    /// the current scope reflects the reset.
    /// </summary>
    private async void ApplyDefaultToAll_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService.IsReadOnly)
        {
            return;
        }

        var result = MessageBox.Show(
            "Reset EVERY server to the fleet-wide default schedule?\n\n" +
            "This removes all per-server schedule overrides and run times; the fleet-wide default schedule and run times are not changed. " +
            "This can't be undone.",
            "Apply Default to All Servers", MessageBoxButton.YesNo, MessageBoxImage.Warning);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        ApplyDefaultToAllButton.IsEnabled = false;
        try
        {
            /* #4938: a server's run time is part of its override, so this one call deletes every server's schedule rows and
               run times together, in one transaction. */
            var removed = await _dataService.ResetAllServerSchedulesAsync();
            Saved = true;

            /* Re-read the overrides so the editor reflects the reset (every per-server row is now gone) and
               reload the current scope's grid + preset detection. */
            _allOverrides = await _dataService.GetCollectorSchedulesAsync();
            await ReloadRunTimesAsync();
            LoadScopeSchedule();

            StatusText.Text = removed > 0
                ? $"Reset {removed} per-server schedule override(s) — every server now follows the fleet default."
                : "No per-server overrides to reset — every server already follows the fleet default.";
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (ViewerSchemaSkewException ex)
        {
            MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not reset the schedules:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            ApplyDefaultToAllButton.IsEnabled = !_dataService.IsReadOnly;
        }
    }

    private void CancelButton_Click(object sender, RoutedEventArgs e) => Close();
}
