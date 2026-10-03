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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// Fleet-wide retained SQL Agent job-run history (issue #1433) — a structural sibling of
/// <see cref="AlertsHistoryTab"/> reading <c>v_job_history</c> via
/// <see cref="LocalDataService.GetJobHistoryAsync(DateTime, int, int?, IReadOnlyDictionary{int, ServerClock}?)"/>. Time-range + Server + Status + Category filters,
/// per-column filter popups, failure / long-runtime / retry row color-coding, and CSV export. Job history
/// is a durable record, so there is no dismiss/mute surface (unlike alerts).
/// </summary>
public partial class JobHistoryTab : UserControl
{
    private LocalDataService? _dataService;
    private Func<IReadOnlyDictionary<int, string>>? _displayNames;
    private Func<IReadOnlyDictionary<int, ServerClock>>? _openTabClocks;
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
    /// <para><paramref name="openTabClocks"/> (#4966) snapshots the clock of every open server tab by server id, the second place
    /// a server's clock comes from after its own collected one, as <see cref="AlertsHistoryTab"/>'s open-tab lookup does. It is
    /// called on the UI thread, at the start of each load, because the open tabs are UI objects, and the read gets the snapshot.</para>
    /// </summary>
    public void Initialize(
        LocalDataService dataService, Func<IReadOnlyDictionary<int, string>>? displayNames = null,
        Func<IReadOnlyDictionary<int, ServerClock>>? openTabClocks = null)
    {
        _dataService = dataService;
        _displayNames = displayNames;
        _openTabClocks = openTabClocks;
        _filterManager = new DataGridFilterManager<JobHistoryRow>(JobHistoryDataGrid);
        _staleDataTimer.Start();
    }

    /// <summary>Refreshes the job history data.</summary>
    public async void RefreshJobs()
    {
        await LoadJobsAsync();
    }

    /// <summary>The read's row cap (#4478) — the 2,000 <see cref="LoadJobsAsync"/> passes to
    /// <see cref="LocalDataService.GetJobHistoryAsync(DateTime, int, int?, IReadOnlyDictionary{int, ServerClock}?)"/>. The
    /// "Showing since" note names the oldest run of a page this full (#4966).</summary>
    internal const int RowCap = 2000;

    private async System.Threading.Tasks.Task LoadJobsAsync()
    {
        if (_dataService == null) return;

        /* #2933: no load guard, and the server/hours combos scope the grid below them — the later-
           starting of two overlapping reads can land first. Same idiom as FinOpsTab's. */
        var gen = _loads.Claim(nameof(LoadJobsAsync));

        NoJobsMessage.Visibility = Visibility.Collapsed;
        LoadingMessage.Visibility = Visibility.Visible;

        try
        {
            var hoursBack = GetSelectedHoursBack();
            int? serverId = GetSelectedServerId();

            /* #4966: the window's start is worked out ONCE, and the read and the data-start probe both take that instant: the note is
               worded against the window the rows were read over. The open tabs' clocks are taken here, on the UI thread (they are
               UI objects), and the read gets them as a plain snapshot. */
            var nowUtc = DateTime.UtcNow;
            var startUtc = nowUtc.AddHours(-hoursBack);
            var openTabClocks = _openTabClocks?.Invoke();

            var all = await System.Threading.Tasks.Task.Run(() => _dataService.GetJobHistoryAsync(startUtc, RowCap, serverId, openTabClocks));
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

            /* The cap applies to the UNFILTERED read (all.Count), not the client-side-filtered display count:
               a Status/Category filter narrowing the grid must not make the "newest 2,000" label disappear when
               the underlying read still hit the cap. */
            JobCountIndicator.Text = JobHistoryCap.CountText(displayCount, all.Count, RowCap);
            AppLogger.Debug("JobHistory", $"Loaded {displayCount} job run(s) (query returned {all.Count}, hoursBack={hoursBack}, serverId={serverId?.ToString() ?? "all"})");

            _lastRefreshed = DateTime.UtcNow;
            UpdateStaleDataIndicator();

            await UpdateAgentStatusAsync(serverId);
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            LoadingMessage.Visibility = Visibility.Collapsed;

            /* #4966: the rows are bound and the loading note is down; the note comes last, from the UNFILTERED read (all): the Status
               and Category filters narrow the grid on the client and say nothing about where the data starts. A probe that fails
               costs the note and never the grid. */
            await ShowDataStartNoteAsync(serverId, openTabClocks, startUtc, nowUtc, all, gen);
        }
        catch (Exception ex)
        {
            AppLogger.Error("JobHistory", $"Failed to load job history: {ex.Message}");
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            LoadingMessage.Visibility = Visibility.Collapsed;
        }
    }

    /// <summary>
    /// The "Showing since" note of the grid (#4966), worked out for one load: the frame it is worded in, the servers the probe asks,
    /// then the step below. For one server the note is worded on that server's wall clock, the clock the read windowed on (the
    /// server's collected clock, else its open tab's, else the machine's, <see cref="LocalDataService.ReadJobHistoryClockAsync"/>),
    /// which is the clock the Run Time column prints in. For the All Servers view the rows sit on different servers' clocks, so
    /// the note is worded in UTC and says so; the probe has no all-servers form, so it asks the servers the Server combo lists
    /// (<see cref="LocalDataService.GetJobHistoryDataStartAsync"/> takes the earliest coverage among them and bounds its own work).
    /// A load that a newer one has superseded writes nothing.
    /// </summary>
    private async System.Threading.Tasks.Task ShowDataStartNoteAsync(
        int? serverId, IReadOnlyDictionary<int, ServerClock>? openTabClocks, DateTime startUtc, DateTime endUtc,
        IReadOnlyCollection<JobHistoryRow> read, int gen)
    {
        if (_dataService == null) return;
        var service = _dataService;

        TimeZoneInfo zone;
        IReadOnlyCollection<int> asked;
        if (serverId is int one)
        {
            var tabClock = openTabClocks is not null && openTabClocks.TryGetValue(one, out var found) ? found : null;
            var clock = await System.Threading.Tasks.Task.Run(() => service.ReadJobHistoryClockAsync(one, tabClock));
            zone = clock.AsTimeZone();
            asked = [one];
        }
        else
        {
            zone = TimeZoneInfo.Utc;
            asked = ListedServerIds();
        }

        if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

        await ShowJobHistoryDataStartAsync(
            JobHistoryWindowTruncatedBanner,
            () => System.Threading.Tasks.Task.Run(() => service.GetJobHistoryDataStartAsync(asked, startUtc, endUtc)),
            startUtc, endUtc, read, zone, inUtc: serverId is null,
            superseded: () => _loads.Superseded(nameof(LoadJobsAsync), gen));
    }

    /// <summary>The server ids the Server combo lists (every item after "All Servers"): the servers with a run in the read just
    /// bound, which is the set the All Servers note asks (#4966).</summary>
    private List<int> ListedServerIds() =>
        ServerFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Select(i => int.TryParse(i.Tag?.ToString(), out var id) ? id : (int?)null)
            .Where(id => id.HasValue)
            .Select(id => id!.Value)
            .ToList();

    /// <summary>
    /// The "Showing since" banner of the grid (#4966), through the steps the server tab's grids share. Job history is an event
    /// surface: the read windows on the run's own time, and a server's first collection copies the history msdb already holds, so a
    /// run can sit long before the coverage the probe found. Below its cap the note names the earlier of the coverage start and the
    /// earliest run in <paramref name="read"/> (<see cref="ServerTab.EarlierOfFloorAndRowShown"/>). The read lists the newest
    /// <see cref="RowCap"/> runs, so a full page names its oldest run whatever the store covers, with no slack and no probe
    /// (<see cref="ServerTab.CappedGridBannerAsync{T}"/>). A window of 90 minutes or less makes no probe call, and a probe that
    /// throws shows no note (<see cref="ServerTab.ProbeWindowFloorOrNullAsync"/>). <paramref name="read"/> is the read's own result,
    /// before the Status and Category filters. The time is worded to the second in <paramref name="zone"/>; with
    /// <paramref name="inUtc"/> (the All Servers view, whose rows sit on different servers' clocks) the text ends in "UTC". The cap
    /// label beside the count (<see cref="JobHistoryCap"/>) stays: it says how many runs the page is cut to, and this note says the
    /// time that page reaches back to. A step of its own so the tests run the tab's banner call without building the control.
    /// </summary>
    /// <param name="banner">The grid's banner.</param>
    /// <param name="probe">The data-start probe, called at most once and not at all for a window of 90 minutes or less or a full page.</param>
    /// <param name="startUtc">The window's start: the one the read and the probe took.</param>
    /// <param name="endUtc">The window's end.</param>
    /// <param name="read">The runs the read returned.</param>
    /// <param name="zone">The zone the time is worded in.</param>
    /// <param name="inUtc">Whether the text names UTC.</param>
    /// <param name="superseded">True when a newer load has started: checked after the probe answers and before the banner is written.</param>
    internal static System.Threading.Tasks.Task ShowJobHistoryDataStartAsync(
        TextBlock banner, Func<System.Threading.Tasks.Task<DateTime?>> probe, DateTime startUtc, DateTime endUtc,
        IReadOnlyCollection<JobHistoryRow> read, TimeZoneInfo zone, bool inUtc, Func<bool>? superseded = null)
    {
        var runTimes = read.Where(r => r.RunDateTimeUtc.HasValue).Select(r => r.RunDateTimeUtc!.Value).ToList();

        void Word()
        {
            if (inUtc && banner.Visibility == Visibility.Visible)
            {
                banner.Text += " UTC";
            }
        }

        return ServerTab.CappedGridBannerAsync(runTimes, RowCap, t => t,
            oldestRowShown =>
            {
                ServerTab.ApplyCappedWindowFloorToBanner(banner, oldestRowShown, startUtc, zone);
                Word();
            },
            async () =>
            {
                var floor = await ServerTab.ProbeWindowFloorOrNullAsync(probe, "Job History", startUtc, endUtc);
                if (superseded?.Invoke() == true) return;

                ServerTab.ApplyWindowFloorToBanner(banner, ServerTab.EarlierOfFloorAndRowShown(floor, ServerTab.EarliestRowShown(runTimes, t => t)), startUtc, zone);
                Word();
            });
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
