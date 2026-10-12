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

    /// <summary>The configured servers the Server drop-down offers before any run names them (#5703): those the job history collector
    /// runs on, by the name the grid speaks (<see cref="LocalDataService.BuildJobHistoryChoiceMap"/>). Set by the shell in
    /// <c>MainWindow.AlertEngine.cs</c>, beside the engine editions it resolves a server's edition from. Null offers none, so the rows
    /// alone name the servers.</summary>
    internal Func<IReadOnlyDictionary<int, string>>? OfferedServers { get; set; }
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
        RangePicker.Value = TimeRangePresets.FromLegacyHours(24)!; /* 24 hours, as the list opened; '1mo' is the old 30 days, '90d' can be typed */
        /* R8: the old "Last Year" (365 days). The parser has no year unit, so "1y" cannot be typed; this choice is the way to a year. */
        RangePicker.SetLongestChoice(LiteTimeRange.JobHistoryLongestChoice, "Last Year");
        /* #5562 M1: typed times and calendar periods are read in the zone the grid words its rows in (see PickerZone). */
        RangePicker.ZoneProvider = PickerZone;
        /* #5565: a cross-server list has one fixed filter scope, so its filters survive a restart. */
        ColumnFilterScope.SetServer(this, ColumnFilterScope.AllServers);
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

    /// <summary>The clocks the last read worked each server's window on (<see cref="LocalDataService.GetJobHistoryWithClocksAsync"/>), kept so the picker can word a single server's range in the same clock.</summary>
    private readonly Dictionary<int, ServerClock> _readClocks = new();

    /// <summary>
    /// The zone the picker reads typed times and calendar periods in (#5562 M1): the zone the grid words its rows in. For one
    /// server that is its wall clock (the clock its last read used, else its open tab's, else this machine's, the chain
    /// <see cref="ServerTimeHelper.ClockForServer(ServerClock?, ServerClock?)"/> follows); for All Servers it is UTC, since the rows
    /// sit on different servers' clocks (<see cref="ShowDataStartNoteAsync"/>).
    /// </summary>
    internal static TimeZoneInfo JobHistoryZone(int? serverId, IReadOnlyDictionary<int, ServerClock>? readClocks, IReadOnlyDictionary<int, ServerClock>? openTabClocks)
    {
        if (serverId is not int one)
        {
            return TimeZoneInfo.Utc;
        }

        if (readClocks is not null && readClocks.TryGetValue(one, out var read))
        {
            return read.AsTimeZone();
        }

        if (openTabClocks is not null && openTabClocks.TryGetValue(one, out var tab))
        {
            return tab.AsTimeZone();
        }

        return TimeZoneInfo.Local;
    }

    private TimeZoneInfo PickerZone()
    {
        /* The open tabs' clocks are only asked for when the last read has none for this server (they are UI objects). */
        var serverId = GetSelectedServerId();
        return JobHistoryZone(serverId, _readClocks, serverId is int id && !_readClocks.ContainsKey(id) ? _openTabClocks?.Invoke() : null);
    }

    /// <summary>
    /// Puts one load's rows on the grid. Synchronous on purpose: <see cref="JobHistoryLoadSteps.BindWhenCurrentAsync{T}"/> runs it
    /// straight after the last supersede check, with nothing awaited between (#5703).
    /// </summary>
    private void BindRows(List<JobHistoryRow> all, IReadOnlyDictionary<int, ServerClock> readClocks)
    {
        foreach (var (id, readClock) in readClocks)
        {
            _readClocks[id] = readClock;
        }

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

        /* The Server / Category combos grow and never shrink: they hold what is already on offer, every configured server the collector
           runs on and every category with a run in the window (read beside the grid, #5703, and added by ApplyKnownCategoriesAsync when
           that read arrives), and what the rows add. The per-column filter popups still layer on top. */
        PopulateServerFilter(all);
        PopulateCategoryFilter(all);

        if (_filterManager != null)
            _filterManager.UpdateData(all);
        else
            JobHistoryDataGrid.ItemsSource = all;
    }

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
            int? serverId = GetSelectedServerId();

            /* #4966: the window's start is worked out ONCE, and the read and the data-start probe both take that instant: the note is
               worded against the window the rows were read over. The open tabs' clocks are taken here, on the UI thread (they are
               UI objects), and the read gets them as a plain snapshot. */
            var nowUtc = DateTime.UtcNow;
            var (startUtc, rangeEndUtc) = LiteTimeRange.BoundsOf(RangePicker, 24, nowUtc);
            var openTabClocks = _openTabClocks?.Invoke();

            /* #5703: Status and Category go into the read. The cap keeps the newest runs of the whole fleet, so a filter applied to those
               rows afterwards (as this tab did) never saw a failure that a busier server's newer runs had pushed past it: All Servers lost
               runs the single-server grid showed. The read applies them ahead of the cap. */
            var statusFilter = GetSelectedStatus();
            var categoryFilter = GetSelectedCategory();
            /* #5703: the Category choices that do not depend on the runs read: read once per window and every few minutes after, beside
               the grid's read; a failure costs the extra choices and never the grid. */
            /* Keyed on what identifies the window and the server (JobHistoryChoicesKey), not on its span: a live window with a fixed
               start (Today, Month to Date, "since X") grows its span on every 30 s refresh, which would read them every time. The read
               is the picked server's (all servers at All Servers), so the drop-down offers no category the pick would leave empty. */
            var choicesKey = LiteTimeRange.ChoicesKeyOf(RangePicker, startUtc, rangeEndUtc, nowUtc, serverId);
            var categoriesTask = _choicesFreshness.NeedsRead(choicesKey, DateTime.UtcNow)
                ? ReadKnownCategoriesAsync(startUtc, rangeEndUtc, serverId, choicesKey)
                : null;
            var readTask = System.Threading.Tasks.Task.Run(() => _dataService.GetJobHistoryWithClocksAsync(startUtc, RowCap, serverId, openTabClocks, rangeEndUtc, statusFilter, categoryFilter));

            var (all, readClocks) = await readTask;

            /* The last supersede check and the bind sit in one synchronous run (JobHistoryLoadSteps.BindWhenCurrentAsync): a load that
               was current at the check cannot be superseded before it binds, so an older load never binds over a newer one. The
               Category choices are applied after the bind, by a load that is still current; the grid never waits on their read. */
            if (!await JobHistoryLoadSteps.BindWhenCurrentAsync(readTask, () => _loads.Superseded(nameof(LoadJobsAsync), gen), read => BindRows(read.Rows, read.Clocks)))
            {
                return;
            }

            var choicesApplied = ApplyKnownCategoriesAsync(categoriesTask, choicesKey, gen, all);

            var displayCount = JobHistoryDataGrid.ItemsSource is ICollection<JobHistoryRow> coll ? coll.Count : all.Count;
            NoJobsMessage.Visibility = displayCount == 0 ? Visibility.Visible : Visibility.Collapsed;
            RunTimeHeaderText.Text = TimeColumnTitle.For("Run Time", ServerTimeHelper.CurrentDisplayMode); // D5: the column names its clock

            /* The cap applies to the read (all.Count), not the column-filtered display count: a column filter narrowing the
               grid must not make the "newest 2,000" label disappear when the underlying read still hit the cap. */
            JobCountIndicator.Text = JobHistoryCap.CountText(displayCount, all.Count, RowCap);
            AppLogger.Debug("JobHistory", $"Loaded {displayCount} job run(s) (query returned {all.Count}, since={startUtc:O}, until={(rangeEndUtc.HasValue ? rangeEndUtc.Value.ToString("O") : "now")}, serverId={serverId?.ToString() ?? "all"})");

            _lastRefreshed = DateTime.UtcNow;
            UpdateStaleDataIndicator();

            await UpdateAgentStatusAsync(serverId);
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            LoadingMessage.Visibility = Visibility.Collapsed;

            /* #4966: the rows are bound and the loading note is down; the note comes last, from the read (all). Status and Category are
               applied in the read, ahead of the row cap (#5703), so a full page of matching runs names its oldest run and the cap label
               counts the same rows. A probe that fails costs the note and never the grid. */
            await ShowDataStartNoteAsync(serverId, readClocks, startUtc, rangeEndUtc ?? nowUtc, all, gen);
            await choicesApplied;
        }
        catch (Exception ex)
        {
            AppLogger.Error("JobHistory", $"Failed to load job history: {ex.Message}");
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            LoadingMessage.Visibility = Visibility.Collapsed;
        }
    }

    /// <summary>
    /// The "Showing since" note of the grid (#4966), worked out for one load: the frame it is worded in,
    /// then the step below. For one server the note is worded on that server's wall clock, the clock the read windowed on (the
    /// server's collected clock, else its open tab's, else the machine's, <see cref="LocalDataService.ReadJobHistoryClockAsync"/>, handed back by the read),
    /// which is the clock the Run Time column prints in. For the All Servers view the rows sit on different servers' clocks, so
    /// the note is worded in UTC and says so; the probe reads the All Servers set itself
    /// (<see cref="LocalDataService.GetJobHistoryDataStartAsync"/> takes the earliest coverage among the servers with a run or a logged collector run in the window and bounds its own work).
    /// A load that a newer one has superseded writes nothing.
    /// </summary>
    private async System.Threading.Tasks.Task ShowDataStartNoteAsync(
        int? serverId, IReadOnlyDictionary<int, ServerClock> readClocks, DateTime startUtc, DateTime endUtc,
        IReadOnlyCollection<JobHistoryRow> read, int gen)
    {
        if (_dataService == null) return;
        var service = _dataService;

        /* One clock read: for one server the note is worded on the clock the read windowed on, handed back by the read. */
        var zone = serverId is int one && readClocks.TryGetValue(one, out var clock) ? clock.AsTimeZone() : TimeZoneInfo.Utc;

        if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

        await ShowJobHistoryDataStartAsync(
            JobHistoryWindowTruncatedBanner,
            () => System.Threading.Tasks.Task.Run(() => service.GetJobHistoryDataStartAsync(serverId, startUtc, endUtc)),
            startUtc, endUtc, read, zone, inUtc: serverId is null,
            superseded: () => _loads.Superseded(nameof(LoadJobsAsync), gen),
            onFloor: floor => RangePicker.DataStartUtc = LiteTimeRange.DataStartFor(floor, DateTime.UtcNow));
    }

    /// <summary>
    /// The "Showing since" banner of the grid (#4966), through the steps the server tab's grids share. Job history is an event
    /// surface: the read windows on the run's own time, and a server's first collection copies the history msdb already holds, so a
    /// run can sit long before the coverage the probe found. Below its cap the note names the earlier of the coverage start and the
    /// earliest run in <paramref name="read"/> (<see cref="ServerTab.EarlierOfFloorAndRowShown"/>). The read lists the newest
    /// <see cref="RowCap"/> runs, so a full page names its oldest run whatever the store covers, with no slack and no probe
    /// (<see cref="ServerTab.CappedGridBannerAsync{T}"/>). A window of 90 minutes or less makes no probe call, and a probe that
    /// throws shows no note (<see cref="ServerTab.ProbeWindowFloorOrNullAsync"/>). <paramref name="read"/> is the read's own result,
    /// the runs the grid shows (Status and Category are applied in the read, ahead of the row cap, #5703). The time is worded to the second in <paramref name="zone"/>; with
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
        IReadOnlyCollection<JobHistoryRow> read, TimeZoneInfo zone, bool inUtc, Func<bool>? superseded = null,
        Action<DateTime?>? onFloor = null)
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
                onFloor?.Invoke(floor); /* #5562 R7: the picker's data-start note takes the floor this probe found */
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

    /// <summary>How long the category choices read beside the grid stay current before they are read again.</summary>
    private static readonly TimeSpan ChoicesLifetime = TimeSpan.FromMinutes(5);

    /// <summary>Every category with a run in the window (the picked server's when one is picked), as the last read found them (<see cref="LocalDataService.GetJobHistoryCategoriesAsync"/>).</summary>
    private List<string> _knownCategories = [];

    /// <summary>When the choices are read again: for a different window or server, or after <see cref="ChoicesLifetime"/>.</summary>
    private readonly JobHistoryChoicesFreshness _choicesFreshness = new(ChoicesLifetime);

    /// <summary>The configured servers the job history collector runs on, by the name the grid's Server column speaks (the shell's
    /// snapshot, <see cref="LocalDataService.BuildJobHistoryChoiceMap"/>). A disabled server or an Azure SQL Database cannot have job
    /// history, so it is not offered unless it has rows in the window (the rows add it).</summary>
    private List<JobHistoryFilterChoices.ServerChoice> KnownServers() =>
        OfferedServers?.Invoke()
            .Where(p => !string.IsNullOrEmpty(p.Value))
            .Select(p => new JobHistoryFilterChoices.ServerChoice(p.Key, p.Value))
            .ToList()
        ?? [];

    /// <summary>Reads the window's categories. Never throws: a failed read returns null and is logged.</summary>
    private async System.Threading.Tasks.Task<List<string>?> ReadKnownCategoriesAsync(
        DateTime startUtc, DateTime? endUtc, int? serverId, JobHistoryChoicesKey key)
    {
        var service = _dataService!;
        try
        {
            var categories = await System.Threading.Tasks.Task.Run(() => service.GetJobHistoryCategoriesAsync(startUtc, endUtc, serverId));
            _choicesFailures.Succeeded("categories");
            return categories;
        }
        catch (Exception ex)
        {
            /* Logged once for a window; the next refresh still reads again. */
            if (_choicesFailures.ShouldLog("categories", key))
            {
                AppLogger.Error("JobHistory", $"Failed to load the job history category choices: {ex.Message}");
            }

            return null;
        }
    }

    private readonly ChoicesFailureLogGate<JobHistoryChoicesKey> _choicesFailures = new();

    /// <summary>Takes the categories read beside the grid once they arrive, and only if this load is still the current one (null when
    /// this load did not read them, or the read failed, which keeps the last ones and reads again on the next load). The grid is
    /// already bound: it never waits on this read (<see cref="JobHistoryLoadSteps.ApplyWhenCurrentAsync{T}"/>).</summary>
    private System.Threading.Tasks.Task<bool> ApplyKnownCategoriesAsync(
        System.Threading.Tasks.Task<List<string>?>? categoriesTask, JobHistoryChoicesKey key, int gen, List<JobHistoryRow> rows) =>
        JobHistoryLoadSteps.ApplyWhenCurrentAsync(categoriesTask, () => _loads.Superseded(nameof(LoadJobsAsync), gen), categories =>
        {
            if (categories is null)
            {
                return;
            }

            _knownCategories = categories;
            _choicesFreshness.Record(key, DateTime.UtcNow);
            PopulateServerFilter(rows);
            PopulateCategoryFilter(rows);
        });

    private void PopulateServerFilter(List<JobHistoryRow> rows)
    {
        /* #5703: the choices only grow (JobHistoryFilterChoices.MergeServers): those already on offer, every configured server (a read that
           Status or Category narrowed, or that a busier server's newest runs filled, holds only some of them), and the rows just read. */
        var offered = ServerFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Where(i => int.TryParse(i.Tag?.ToString(), out _) && i.Content is string)
            .Select(i => new JobHistoryFilterChoices.ServerChoice(int.Parse(i.Tag!.ToString()!, System.Globalization.CultureInfo.InvariantCulture), (string)i.Content));
        var currentSelection = ServerFilterComboBox.SelectedIndex > 0
            ? (ServerFilterComboBox.SelectedItem as ComboBoxItem)?.Tag?.ToString()
            : null;
        JobHistoryFilterChoices.ServerChoice? selected = GetSelectedServerId() is int selectedId && ServerFilterComboBox.SelectedItem is ComboBoxItem { Content: string selectedName }
            ? new JobHistoryFilterChoices.ServerChoice(selectedId, selectedName)
            : null;
        var servers = JobHistoryFilterChoices.MergeServers(
            offered, KnownServers(), rows.Select(r => new JobHistoryFilterChoices.ServerChoice(r.ServerId, r.ServerName)), selected);

        var existingIds = ServerFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Select(i => i.Tag?.ToString())
            .ToList();

        var newIds = servers.Select(s => s.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture)).ToList();
        if (newIds.SequenceEqual(existingIds)) return;

        ServerFilterComboBox.SelectionChanged -= Filter_SelectionChanged;

        while (ServerFilterComboBox.Items.Count > 1)
            ServerFilterComboBox.Items.RemoveAt(1);

        foreach (var (serverId, serverName) in servers)
        {
            ServerFilterComboBox.Items.Add(new ComboBoxItem
            {
                Content = serverName,
                Tag = serverId.ToString(System.Globalization.CultureInfo.InvariantCulture)
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
        /* #5703: the choices only grow (JobHistoryFilterChoices.MergeCategories): those already on offer, every category with a run in the
           window (a read the row cap does not touch), and the rows just read. */
        var offered = CategoryFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Select(i => i.Tag?.ToString());
        var currentSelection = CategoryFilterComboBox.SelectedIndex > 0
            ? (CategoryFilterComboBox.SelectedItem as ComboBoxItem)?.Tag?.ToString()
            : null;
        var categories = JobHistoryFilterChoices.MergeCategories(offered, _knownCategories, rows.Select(r => r.CategoryName), GetSelectedCategory());

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

    private int? GetSelectedServerId()
    {
        return JobHistoryFilterChoices.ServerFilter(ServerFilterComboBox.SelectedIndex, (ServerFilterComboBox.SelectedItem as ComboBoxItem)?.Tag as string);
    }

    private int? GetSelectedStatus()
    {
        return JobHistoryFilterChoices.StatusFilter(StatusFilterComboBox.SelectedIndex, (StatusFilterComboBox.SelectedItem as ComboBoxItem)?.Tag as string);
    }

    private string? GetSelectedCategory()
    {
        return JobHistoryFilterChoices.CategoryFilter(CategoryFilterComboBox.SelectedIndex, (CategoryFilterComboBox.SelectedItem as ComboBoxItem)?.Tag as string);
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

    private async void RangePicker_RangeChanged(object? sender, TimeRangeChangedEventArgs e)
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
