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
using System.Windows.Controls.Primitives;
using System.Windows.Threading;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// Fleet-wide retained SQL Agent job-run history (issue #1433) — a structural sibling of
/// <see cref="AlertsHistoryTab"/> reading <c>v_job_history</c> via
/// <see cref="LocalDataService.GetJobHistoryAsync"/>. Time-range + Server + Status + Category filters,
/// per-column filter popups, failure / long-runtime / retry row color-coding, and CSV export. Job history
/// is a durable record, so there is no dismiss/mute surface (unlike alerts).
/// </summary>
public partial class JobHistoryTab : UserControl
{
    private LocalDataService? _dataService;
    private Func<IReadOnlyDictionary<int, string>>? _displayNames;
    private DataGridFilterManager<JobHistoryRow>? _filterManager;
    private readonly ScopedLoadGenerations _loads = new();
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private DateTime? _lastRefreshed;
    private readonly DispatcherTimer _staleDataTimer;

    public JobHistoryTab()
    {
        InitializeComponent();
        _staleDataTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(10) };
        _staleDataTimer.Tick += StaleDataTimer_Tick;
    }

    /// <summary>
    /// Initializes the control with required dependencies. <paramref name="displayNames"/> snapshots
    /// server_id → operator display name from the CONFIG layer (#2126's Lite half): Lite's display-name
    /// concept lives on <c>ServerConnection</c>, not in DuckDB (the stored <c>servers.display_name</c>
    /// column is unpopulated), so the shell supplies the mapping the same way the Overview tab passes
    /// <c>DisplayNameWithIntent</c> into <c>GetServerSummaryAsync</c>.
    /// </summary>
    public void Initialize(LocalDataService dataService, Func<IReadOnlyDictionary<int, string>>? displayNames = null)
    {
        _dataService = dataService;
        _displayNames = displayNames;
        _filterManager = new DataGridFilterManager<JobHistoryRow>(JobHistoryDataGrid);
        _staleDataTimer.Start();
    }

    /// <summary>Refreshes the job history data.</summary>
    public async void RefreshJobs()
    {
        await LoadJobsAsync();
    }

    private async System.Threading.Tasks.Task LoadJobsAsync()
    {
        if (_dataService == null) return;

        /* #2933: no load guard, and the server/hours combos scope the grid below them — the later-
           starting of two overlapping reads can land first. Same idiom as FinOpsTab's. */
        var gen = _loads.Claim(nameof(LoadJobsAsync));

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();

            var all = await System.Threading.Tasks.Task.Run(() => _dataService.GetJobHistoryAsync(hoursBack, 2000, serverId));
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            /* #2126: rows carry the raw collected server name; swap in the operator's alias where the
               config layer knows one, so the Server column and filter speak the same names as every
               other tab. A server no longer in config keeps its raw name (the durable-record case). */
            if (_displayNames?.Invoke() is { Count: > 0 } names)
            {
                foreach (var row in all)
                {
                    if (names.TryGetValue(row.ServerId, out var alias) && !string.IsNullOrEmpty(alias))
                    {
                        row.ServerName = alias;
                    }
                }
            }

            /* Populate the Server / Category combos from the full (pre status/category) result, then apply
               Status + Category client-side — those must NOT go into the reader's window (they'd skew the
               per-job long-running / last-success baselines, which are computed over every run in the
               window). The per-column filter popups still layer on top. */
            PopulateServerFilter(all);
            PopulateCategoryFilter(all);

            var statusFilter = GetSelectedStatus();
            var categoryFilter = GetSelectedCategory();

            var filtered = all.Where(r =>
                (statusFilter is null || r.RunStatus == statusFilter.Value) &&
                (categoryFilter is null || string.Equals(r.CategoryName, categoryFilter, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            if (_filterManager != null)
                _filterManager.UpdateData(filtered);
            else
                JobHistoryDataGrid.ItemsSource = filtered;

            var displayCount = JobHistoryDataGrid.ItemsSource is ICollection<JobHistoryRow> coll ? coll.Count : filtered.Count;
            NoJobsMessage.Visibility = displayCount == 0 ? Visibility.Visible : Visibility.Collapsed;
            JobCountIndicator.Text = displayCount > 0 ? $"{displayCount} run(s)" : "";
            AppLogger.Debug("JobHistory", $"Loaded {displayCount} job run(s) (query returned {all.Count}, hoursBack={hoursBack}, serverId={serverId?.ToString() ?? "all"})");

            _lastRefreshed = DateTime.UtcNow;
            UpdateStaleDataIndicator();

            await UpdateAgentStatusAsync(serverId);
        }
        catch (Exception ex)
        {
            AppLogger.Error("JobHistory", $"Failed to load job history: {ex.Message}");
        }
    }

    /// <summary>
    /// Populates the header Agent indicator (issue #1433 Phase 2). With a server selected it shows that
    /// server's Agent status + next scheduled run; across all servers it shows a running/stopped roll-up.
    /// A stopped Agent is drawn in red. Best-effort — an agent_status read failure just clears the indicator.
    /// </summary>
    private async System.Threading.Tasks.Task UpdateAgentStatusAsync(int? serverId)
    {
        if (_dataService == null) return;

        var gen = _loads.Claim(nameof(UpdateAgentStatusAsync));

        try
        {
            var statuses = await System.Threading.Tasks.Task.Run(() => _dataService.GetAgentStatusAsync(serverId));
            if (_loads.Superseded(nameof(UpdateAgentStatusAsync), gen)) return;

            var okBrush = TryFindResource("ForegroundMutedBrush") as System.Windows.Media.Brush
                ?? System.Windows.Media.Brushes.Gray;
            var alertBrush = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromRgb(0xDC, 0x26, 0x26));

            if (statuses.Count == 0)
            {
                AgentStatusIndicator.Text = "";
                return;
            }

            /* Red is reserved for a fresh reading of a genuinely stopped Agent on a server that RUNS one.
               A stale snapshot and a target that never ran Agent (a container built without it, Express, a
               Linux-minimal image) are absence of signal, not a problem, and painting them red trained the
               operator to ignore the indicator. */
            if (serverId.HasValue)
            {
                var s = statuses[0];
                AgentStatusIndicator.Text = s.IsAgentProblem || s.AgentRunning
                    ? $"Agent: {s.StatusDisplay} · Next run: {s.NextScheduledRunLocal}"
                    : $"Agent: {s.StatusDisplay}";
                AgentStatusIndicator.Foreground = s.IsAgentProblem ? alertBrush : okBrush;
            }
            else
            {
                /* Fleet roll-up counts only the servers the question applies to: a server that never ran Agent
                   is not "stopped", and a stale one is not evidence either way. */
                var known = statuses.Where(x => !x.IsStale && x.EverSeenRunning).ToList();
                var running = known.Count(x => x.AgentRunning);
                var stopped = known.Count - running;

                AgentStatusIndicator.Text = known.Count == 0
                    ? "Agents: none observed"
                    : stopped > 0
                        ? $"Agents: {running}/{known.Count} running, {stopped} stopped"
                        : $"Agents: {running}/{known.Count} running";
                AgentStatusIndicator.Foreground = stopped > 0 ? alertBrush : okBrush;
            }
        }
        catch (Exception ex)
        {
            AppLogger.Debug("JobHistory", $"Failed to load agent status: {ex.Message}");

            /* The error path paints too: a superseded read's failure must not blank the indicator the
               newest read has already filled in. */
            if (_loads.Superseded(nameof(UpdateAgentStatusAsync), gen)) return;

            AgentStatusIndicator.Text = "";
        }
    }

    private void PopulateServerFilter(List<JobHistoryRow> rows)
    {
        var servers = rows
            .Select(r => (r.ServerId, r.ServerName))
            .Where(s => !string.IsNullOrEmpty(s.ServerName))
            .Distinct()
            .OrderBy(s => s.ServerName)
            .ToList();

        var currentSelection = ServerFilterComboBox.SelectedIndex > 0
            ? (ServerFilterComboBox.SelectedItem as ComboBoxItem)?.Tag?.ToString()
            : null;

        var existingIds = ServerFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Select(i => i.Tag?.ToString())
            .ToList();

        var newIds = servers.Select(s => s.ServerId.ToString()).ToList();
        if (newIds.SequenceEqual(existingIds)) return;

        ServerFilterComboBox.SelectionChanged -= Filter_SelectionChanged;

        while (ServerFilterComboBox.Items.Count > 1)
            ServerFilterComboBox.Items.RemoveAt(1);

        foreach (var (serverId, serverName) in servers)
        {
            ServerFilterComboBox.Items.Add(new ComboBoxItem
            {
                Content = serverName,
                Tag = serverId.ToString()
            });
        }

        if (currentSelection != null)
        {
            for (int i = 1; i < ServerFilterComboBox.Items.Count; i++)
            {
                if ((ServerFilterComboBox.Items[i] as ComboBoxItem)?.Tag?.ToString() == currentSelection)
                {
                    ServerFilterComboBox.SelectedIndex = i;
                    break;
                }
            }
        }

        ServerFilterComboBox.SelectionChanged += Filter_SelectionChanged;
    }

    private void PopulateCategoryFilter(List<JobHistoryRow> rows)
    {
        var categories = rows
            .Select(r => r.CategoryName)
            .Where(c => !string.IsNullOrEmpty(c))
            .Select(c => c!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(c => c, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var currentSelection = CategoryFilterComboBox.SelectedIndex > 0
            ? (CategoryFilterComboBox.SelectedItem as ComboBoxItem)?.Tag?.ToString()
            : null;

        var existing = CategoryFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Select(i => i.Tag?.ToString())
            .ToList();

        if (categories.SequenceEqual(existing, StringComparer.OrdinalIgnoreCase)) return;

        CategoryFilterComboBox.SelectionChanged -= Filter_SelectionChanged;

        while (CategoryFilterComboBox.Items.Count > 1)
            CategoryFilterComboBox.Items.RemoveAt(1);

        foreach (var category in categories)
        {
            CategoryFilterComboBox.Items.Add(new ComboBoxItem { Content = category, Tag = category });
        }

        if (currentSelection != null)
        {
            for (int i = 1; i < CategoryFilterComboBox.Items.Count; i++)
            {
                if ((CategoryFilterComboBox.Items[i] as ComboBoxItem)?.Tag?.ToString() == currentSelection)
                {
                    CategoryFilterComboBox.SelectedIndex = i;
                    break;
                }
            }
        }

        CategoryFilterComboBox.SelectionChanged += Filter_SelectionChanged;
    }

    private int GetSelectedHoursBack()
    {
        if (TimeRangeComboBox.SelectedItem is ComboBoxItem item && item.Tag is string tagStr)
            return int.TryParse(tagStr, out var hours) ? hours : 24;
        return 24;
    }

    private int? GetSelectedServerId()
    {
        if (ServerFilterComboBox.SelectedIndex > 0 &&
            ServerFilterComboBox.SelectedItem is ComboBoxItem item &&
            item.Tag is string tagStr &&
            int.TryParse(tagStr, out var serverId))
        {
            return serverId;
        }
        return null;
    }

    private int? GetSelectedStatus()
    {
        if (StatusFilterComboBox.SelectedIndex > 0 &&
            StatusFilterComboBox.SelectedItem is ComboBoxItem item &&
            item.Tag is string tagStr &&
            int.TryParse(tagStr, out var status))
        {
            return status;
        }
        return null;
    }

    private string? GetSelectedCategory()
    {
        if (CategoryFilterComboBox.SelectedIndex > 0 &&
            CategoryFilterComboBox.SelectedItem is ComboBoxItem item &&
            item.Tag is string tagStr &&
            !string.IsNullOrEmpty(tagStr))
        {
            return tagStr;
        }
        return null;
    }

    #region Column Filter Handlers

    private void FilterButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button button || button.Tag is not string columnName) return;

        if (_filterPopup == null)
        {
            _filterPopupContent = new ColumnFilterPopup();
            _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

            _filterPopup = new Popup
            {
                Child = _filterPopupContent,
                StaysOpen = false,
                Placement = PlacementMode.Bottom,
                AllowsTransparency = true
            };
        }

        ColumnFilterState? existingFilter = null;
        _filterManager?.Filters.TryGetValue(columnName, out existingFilter);
        _filterPopupContent!.Initialize(columnName, existingFilter);

        _filterPopup.PlacementTarget = button;
        _filterPopup.IsOpen = true;
    }

    private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;

        _filterManager?.SetFilter(e.FilterState);
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null)
            _filterPopup.IsOpen = false;
    }

    #endregion

    #region Stale Data Indicator

    private void StaleDataTimer_Tick(object? sender, EventArgs e)
    {
        UpdateStaleDataIndicator();
    }

    private void UpdateStaleDataIndicator()
    {
        if (_lastRefreshed.HasValue)
        {
            var elapsed = DateTime.UtcNow - _lastRefreshed.Value;
            LastRefreshedIndicator.Text = elapsed.TotalSeconds < 5
                ? "Refreshed just now"
                : elapsed.TotalMinutes < 1
                    ? $"Refreshed {(int)elapsed.TotalSeconds}s ago"
                    : $"Refreshed {(int)elapsed.TotalMinutes}m ago";
        }

        if (ArchiveService.IsArchiving)
        {
            ArchivalWarning.Text = "⚠ Archival in progress";
            ArchivalWarning.Visibility = Visibility.Visible;
        }
        else
        {
            ArchivalWarning.Visibility = Visibility.Collapsed;
        }
    }

    #endregion

    #region Event Handlers

    private async void Filter_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
            await LoadJobsAsync();
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e)
    {
        await LoadJobsAsync();
    }

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);

    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);

    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);

    private void ExportToCsv_Click(object sender, RoutedEventArgs e) =>
        DataGridExport.ExportToCsv(sender, "job_history", App.CsvSeparator);

    #endregion
}
