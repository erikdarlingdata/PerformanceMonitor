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
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's collector-schedule editor — a faithful port of Lite's <c>CollectorScheduleEditorWindow</c>,
/// rewired onto the control-plane store: it edits the fleet-wide default schedule (<c>server_id</c> NULL) or a
/// single server's override, overlays the store's sparse <c>config.config_collector_schedules</c> rows on the
/// shared <see cref="CollectorSchedulePresets.BuildDefaultSchedule"/> baseline, and writes the result back via
/// <see cref="ViewerDataService.ReplaceFleetSchedulesAsync"/> / <see cref="ViewerDataService.ReplaceServerSchedulesAsync"/>.
/// Presets change frequencies only (enabled + retention untouched), exactly as Lite. A read-only seat shows a
/// banner and disables the writes.
/// </summary>
public partial class CollectorScheduleEditorWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly IReadOnlyList<DarlingServer> _servers;

    private List<CollectorScheduleRow> _allOverrides = new();
    private List<CollectorScheduleEditItem> _editing = new();
    private int? _scopeServerId;           // null = fleet-wide default scope
    private bool _suppressPresetChange;
    private bool _suppressScopeReload;

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

        /* Default to the fleet scope (index 0). */
        _suppressScopeReload = true;
        ScopeCombo.SelectedIndex = 0;
        _suppressScopeReload = false;
        LoadScopeSchedule();
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

        var isServerScope = _scopeServerId is not null;
        UseDefaultCheckBox.Visibility = isServerScope ? Visibility.Visible : Visibility.Collapsed;

        if (isServerScope)
        {
            HeaderText.Text = $"Collector Schedules — {(ScopeCombo.SelectedItem as ComboBoxItem)?.Content}";
            SubHeaderText.Text = "This server's custom schedule. Uncheck 'Use default schedule' to override the fleet-wide defaults for it.";
            var hasOverride = CollectorScheduleOverlay.ServerHasOverride(_allOverrides, _scopeServerId!.Value);

            _suppressPresetChange = true;
            UseDefaultCheckBox.IsChecked = !hasOverride;
            _suppressPresetChange = false;
        }
        else
        {
            HeaderText.Text = "Collector Schedules — All servers";
            SubHeaderText.Text = "The fleet-wide default schedule. Every server without its own override collects on this schedule.";
        }

        RebuildEditingForScope();
    }

    /// <summary>Rebuilds the editable list for the current scope + use-default state and re-binds the grid.</summary>
    private void RebuildEditingForScope()
    {
        var usesDefault = _scopeServerId is not null && UseDefaultCheckBox.IsChecked == true;

        /* Fleet scope, or a server "using default", shows the fleet-over-default effective (server rows
           excluded); a customizing server shows its own effective schedule. */
        var overlayScope = usesDefault ? (int?)null : _scopeServerId;
        _editing = CollectorScheduleOverlay.BuildEffectiveSchedule(_allOverrides, overlayScope);

        BindGrid();
        UpdateEditableState(!usesDefault);
        DetectActivePreset();
    }

    private void BindGrid()
    {
        ScheduleGrid.ItemsSource = null;
        ScheduleGrid.ItemsSource = _editing;
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
            "Replace the current values with the built-in default frequencies and retention?",
            "Reset to Defaults", MessageBoxButton.YesNo, MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        _editing = CollectorSchedulePresets.BuildDefaultSchedule();
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

        _editing = CollectorScheduleOverlay.BuildEffectiveSchedule(_allOverrides, source.ServerId);
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
            if (_scopeServerId is int serverId)
            {
                var rows = usesDefault
                    ? new List<CollectorScheduleRow>()
                    : CollectorScheduleOverlay.ToServerOverrideRows(_editing, serverId);
                await _dataService.ReplaceServerSchedulesAsync(serverId, rows);
            }
            else
            {
                await _dataService.ReplaceFleetSchedulesAsync(CollectorScheduleOverlay.ToFleetOverrideRows(_editing));
            }

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
            "This removes all per-server schedule overrides; the fleet-wide default schedule is not changed. " +
            "This can't be undone.",
            "Apply Default to All Servers", MessageBoxButton.YesNo, MessageBoxImage.Warning);

        if (result != MessageBoxResult.Yes)
        {
            return;
        }

        ApplyDefaultToAllButton.IsEnabled = false;
        try
        {
            var removed = await _dataService.ResetAllServerSchedulesAsync();
            Saved = true;

            /* Re-read the overrides so the editor reflects the reset (every per-server row is now gone) and
               reload the current scope's grid + preset detection. */
            _allOverrides = await _dataService.GetCollectorSchedulesAsync();
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
