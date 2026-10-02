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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Fleet-wide retained SQL Agent job-run history (issue #1433) — the Darling viewer twin of Lite's
/// <c>JobHistoryTab</c>, a structural sibling of <see cref="AlertsHistoryTab"/> reading the Postgres store
/// via <see cref="ViewerDataService.GetJobHistoryAsync"/>. Time-range + Server + Status + Category filters,
/// per-column filter popups, failure / long-runtime / retry row color-coding, and CSV export. Job history
/// is a durable record, so there is no dismiss/mute surface (unlike alerts). Load failures surface on the
/// host status bar via <see cref="StatusChanged"/>.
/// </summary>
public partial class JobHistoryTab : UserControl
{
    private ViewerDataService? _dataService;
    private DataGridFilterManager<ViewerJobHistoryRow>? _filterManager;
    private readonly PerformanceMonitor.Ui.ScopedLoadGenerations _loads = new();
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private DateTime? _lastRefreshed;
    private readonly DispatcherTimer _staleDataTimer;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public JobHistoryTab()
    {
        InitializeComponent();
        _staleDataTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(10) };
        _staleDataTimer.Tick += StaleDataTimer_Tick;
    }

    /// <summary>Wires the data service and the column-filter manager, and starts the stale-data ticker.</summary>
    public void Initialize(ViewerDataService dataService)
    {
        _dataService = dataService;
        _filterManager = new DataGridFilterManager<ViewerJobHistoryRow>(JobHistoryDataGrid);
        _staleDataTimer.Start();
    }

    /// <summary>Reloads the job history (the shell calls this when the tab becomes visible / on the timer).</summary>
    public Task RefreshJobsAsync() => LoadJobsAsync();

    /// <summary>The read's row cap (#4478) — the 2,000 <see cref="LoadJobsAsync"/> passes to
    /// <see cref="ViewerDataService.GetJobHistoryAsync"/>.</summary>
    private const int RowCap = 2000;

    private async Task LoadJobsAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        var gen = _loads.Claim(nameof(LoadJobsAsync));

        NoJobsMessage.Visibility = Visibility.Collapsed;
        LoadingMessage.Visibility = Visibility.Visible;

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();
            var sinceUtc = DateTime.UtcNow.AddHours(-hoursBack);

            var all = await _dataService.GetJobHistoryAsync(sinceUtc, serverId, RowCap);
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            /* Populate the Server / Category combos from the full (pre status/category) result, then apply
               Status + Category client-side — those must NOT go into the reader's window (they'd skew the
               per-job long-running / last-success baselines, computed over every run in the window). The
               per-column filter popups still layer on top. */
            PopulateServerFilter(all);
            PopulateCategoryFilter(all);

            var statusFilter = GetSelectedStatus();
            var categoryFilter = GetSelectedCategory();

            var filtered = all.Where(r =>
                (statusFilter is null || r.RunStatus == statusFilter.Value) &&
                (categoryFilter is null || string.Equals(r.CategoryName, categoryFilter, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            if (_filterManager != null)
            {
                _filterManager.UpdateData(filtered);
            }
            else
            {
                JobHistoryDataGrid.ItemsSource = filtered;
            }

            var displayCount = JobHistoryDataGrid.Items.Count;
            NoJobsMessage.Visibility = displayCount == 0 ? Visibility.Visible : Visibility.Collapsed;

            /* The cap applies to the UNFILTERED read (all.Count), not the client-side-filtered display count:
               a Status/Category filter narrowing the grid must not make the "newest 2,000" label disappear when
               the underlying read still hit the cap. */
            var capLabel = JobHistoryCap.Label(all.Count, RowCap);
            JobCountIndicator.Text = displayCount == 0
                ? ""
                : capLabel.Length > 0 ? $"{displayCount} run(s) ({capLabel})" : $"{displayCount} run(s)";

            _lastRefreshed = DateTime.UtcNow;
            UpdateStaleDataIndicator();

            await UpdateAgentStatusAsync(serverId);
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            LoadingMessage.Visibility = Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"failed to load job history: {ex.Message}");
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            LoadingMessage.Visibility = Visibility.Collapsed;
        }
    }

    /// <summary>
    /// Populates the header Agent indicator (issue #1433 Phase 2). With a server selected it shows that
    /// server's Agent status + next scheduled run; across all servers it shows a running/stopped roll-up.
    /// A stopped Agent is drawn in red. Best-effort — an agent_status read failure just clears the indicator.
    /// </summary>
    private async Task UpdateAgentStatusAsync(int? serverId)
    {
        if (_dataService == null) return;

        try
        {
            var statuses = await _dataService.GetAgentStatusAsync(serverId);

            var okBrush = TryFindResource("ForegroundMutedBrush") as System.Windows.Media.Brush
                ?? System.Windows.Media.Brushes.Gray;
            var alertBrush = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromRgb(0xDC, 0x26, 0x26));

            if (statuses.Count == 0)
            {
                AgentStatusIndicator.Text = "";
                return;
            }

            if (serverId.HasValue)
            {
                var s = statuses[0];
                AgentStatusIndicator.Text = $"Agent: {s.StatusDisplay} · Next run: {s.NextScheduledRunLocal}";
                AgentStatusIndicator.Foreground = s.AgentRunning ? okBrush : alertBrush;
            }
            else
            {
                var running = statuses.Count(x => x.AgentRunning);
                var stopped = statuses.Count - running;
                AgentStatusIndicator.Text = stopped > 0
                    ? $"Agents: {running}/{statuses.Count} running, {stopped} stopped"
                    : $"Agents: {running}/{statuses.Count} running";
                AgentStatusIndicator.Foreground = stopped > 0 ? alertBrush : okBrush;
            }
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"failed to load agent status: {ex.Message}");
            AgentStatusIndicator.Text = "";
        }
    }

    private void PopulateServerFilter(List<ViewerJobHistoryRow> rows)
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
        if (newIds.SequenceEqual(existingIds))
        {
            return;
        }

        ServerFilterComboBox.SelectionChanged -= Filter_SelectionChanged;

        while (ServerFilterComboBox.Items.Count > 1)
        {
            ServerFilterComboBox.Items.RemoveAt(1);
        }

        foreach (var (serverId, serverName) in servers)
        {
            ServerFilterComboBox.Items.Add(new ComboBoxItem { Content = serverName, Tag = serverId.ToString() });
        }

        if (currentSelection != null)
        {
            for (var i = 1; i < ServerFilterComboBox.Items.Count; i++)
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

    private void PopulateCategoryFilter(List<ViewerJobHistoryRow> rows)
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

        if (categories.SequenceEqual(existing, StringComparer.OrdinalIgnoreCase))
        {
            return;
        }

        CategoryFilterComboBox.SelectionChanged -= Filter_SelectionChanged;

        while (CategoryFilterComboBox.Items.Count > 1)
        {
            CategoryFilterComboBox.Items.RemoveAt(1);
        }

        foreach (var category in categories)
        {
            CategoryFilterComboBox.Items.Add(new ComboBoxItem { Content = category, Tag = category });
        }

        if (currentSelection != null)
        {
            for (var i = 1; i < CategoryFilterComboBox.Items.Count; i++)
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
        {
            return int.TryParse(tagStr, out var hours) ? hours : 24;
        }
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
        if (sender is not Button button || button.Tag is not string columnName)
        {
            return;
        }

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
        {
            _filterPopup.IsOpen = false;
        }

        _filterManager?.SetFilter(e.FilterState);
        JobCountIndicator.Text = JobHistoryDataGrid.Items.Count > 0 ? $"{JobHistoryDataGrid.Items.Count} run(s)" : "";
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null)
        {
            _filterPopup.IsOpen = false;
        }
    }

    #endregion

    #region Stale Data Indicator

    private void StaleDataTimer_Tick(object? sender, EventArgs e) => UpdateStaleDataIndicator();

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
    }

    #endregion

    #region Event Handlers

    private async void Filter_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadJobsAsync();
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await LoadJobsAsync();

    #endregion

    #region Context Menu Handlers

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);

    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);

    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);

    private void ExportToCsv_Click(object sender, RoutedEventArgs e) =>
        DataGridExport.ExportToCsv(sender, "job_history", ViewerExportSettings.CsvSeparator);

    #endregion
}
