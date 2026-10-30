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

    /// <summary>The rows the last read returned, before the per-column filter popups (#4966); Status and Category are in the read itself (#5703). The cap label
    /// belongs to the read, so the column-filter handler, which only sees the grid, needs this to say whether the read was
    /// cut at <see cref="RowCap"/>.</summary>
    private int _lastReadRowCount;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public JobHistoryTab()
    {
        InitializeComponent();
        /* #5562: the shared picker; the old combo defaulted to the last 24 hours and offered 7, 30 and 90 days, which are
           one click away in the picker's presets (1w, 1mo) and a typed "90d". */
        TimeRangePickerControl.ZoneProvider = ViewerTimeHelper.CurrentDisplayZone;
        TimeRangePickerControl.Value = TimeRangePresets.Find("1d")!;
        /* R8: the old "Last Year" (365 days). The parser has no year unit, so "1y" cannot be typed; this choice is the way to a year. */
        TimeRangePickerControl.SetLongestChoice(ViewerTimeRangeWindow.JobHistoryLongestChoice, "Last Year");
        /* #5565: a cross-server list has one fixed filter scope, so its filters survive a restart. */
        ColumnFilterScope.SetServer(this, ColumnFilterScope.AllServers);
        _staleDataTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(10) };
        _staleDataTimer.Tick += StaleDataTimer_Tick;
        /* #6136: each column is at least as wide as its own header, so "Duration" and "Retries" are not cut at the fixed widths. */
        Loaded += (_, _) => DataGridHeaderFit.Apply(this);
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
    internal const int RowCap = 2000;

    /// <summary>
    /// The "Showing since" note of the grid (#4966), through the event-surface step the server tab's grids share
    /// (<see cref="ViewerServerTab.ShowEventDataStartAsync"/>). Job history is an event surface: the read windows on the run's
    /// own time, and a server's first collection copies the history msdb already holds, so a run can sit long before the
    /// coverage the probe found. The note names the earlier of the coverage start and the earliest run in <paramref name="read"/>.
    /// The read lists the newest <see cref="RowCap"/> runs, so a full page names its oldest run whatever the store covers, with
    /// no slack. <paramref name="read"/> is the read's own result, the runs the grid shows: the Status and Category filters are applied in the
    /// read, ahead of the row cap (#5703), so a full page of matching runs names its oldest run and the cap label counts the same rows. The time prints in the zone the Run Time column does (the
    /// display zone, see <see cref="ViewerJobHistoryRow.RunTimeLocal"/>). A step of its own so the tests run the tab's banner call.
    /// The cap label beside the count (<see cref="PerformanceMonitor.Common.JobHistoryCap"/>) stays: it says how many runs the
    /// page is cut to, and this note says the time that page reaches back to, so the tab says one thing about the cap.
    /// </summary>
    /// <param name="banner">The grid's banner.</param>
    /// <param name="probe">The data-start probe the tab started beside its read.</param>
    /// <param name="startUtc">The window's start: the one the read and the probe took.</param>
    /// <param name="read">The runs the read returned.</param>
    internal static Task ShowJobHistoryDataStartAsync(TextBlock banner, Task<DateTime?> probe, DateTime startUtc, IEnumerable<ViewerJobHistoryRow> read) =>
        ViewerServerTab.ShowEventDataStartAsync(banner, probe, "Job History", startUtc, read.Select(r => r.RunDateTimeUtc), RowCap);

    /// <summary>Sets the picker's "Data starts ..." note from the data-start probe the tab already awaited (#5562 R7): the floor its
    /// "Showing since" banner reads, fed by <see cref="ViewerServerTab.UpdateTruncationBanner"/>. No query of its own.</summary>
    internal void RecordDataStart(DateTime? floor)
    {
        if (TimeRangePickerControl.DataStartUtc != floor)
        {
            TimeRangePickerControl.DataStartUtc = floor;
        }
    }

    /// <summary>
    /// Puts one load's rows on the grid. Synchronous on purpose: <see cref="JobHistoryLoadSteps.BindWhenCurrentAsync{T}"/> runs it
    /// straight after the last supersede check, with nothing awaited between (#5703).
    /// </summary>
    private void BindRows(List<ViewerJobHistoryRow> all)
    {
        /* The Server / Category combos grow and never shrink: they hold what is already on offer, every monitored server the collector
           runs on and every category with a run in the window (read beside the grid, #5703, and added by ApplyKnownChoicesAsync when
           that read arrives), and what the rows add. The per-column filter popups still layer on top. */
        PopulateServerFilter(all);
        PopulateCategoryFilter(all);

        if (_filterManager != null)
        {
            _filterManager.UpdateData(all);
        }
        else
        {
            JobHistoryDataGrid.ItemsSource = all;
        }
    }

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
            var (windowStartUtc, windowEndUtc, windowIsLive) = ViewerTimeRangeWindow.Window(
                TimeRangePickerControl.Value, DateTime.UtcNow, ViewerTimeHelper.CurrentDisplayZone());
            int? serverId = GetSelectedServerId();
            /* #4966: the window's start is worked out once, and the read and the data-start probe both take it. The probe starts
               beside the read; its answer is awaited only after the rows are on screen, so a probe that fails costs the note and
               never the grid. */
            var nowUtc = windowEndUtc;
            var sinceUtc = windowStartUtc;
            /* The read, the data-start probe and (on a load that refreshes them) the two choices reads (servers, categories) run together:
               priced as four. */
            /* The choices are keyed on what identifies the window and the server (JobHistoryChoicesKey), not on its span: a live window
               with a fixed start (Today, Month to Date, "since X") grows its span on every refresh, which would read them every time. */
            var choicesKey = JobHistoryChoicesKey.For(
                ViewerTimeRangeWindow.StartSlides(TimeRangePickerControl.Value, DateTime.UtcNow, ViewerTimeHelper.CurrentDisplayZone()),
                windowStartUtc, windowEndUtc, windowIsLive, serverId);
            var refreshChoices = _choicesFreshness.NeedsRead(choicesKey, DateTime.UtcNow);
            using var readFanOut = ViewerReadFanOut.Of(4);
            var dataStartTask = _dataService.GetJobHistoryDataStartAsync(serverId, sinceUtc, nowUtc);

            /* #5562 R6: a range that ended in the past sends its end to the read, so the end bound is applied before the row cap.
               #5703: Status and Category go into the read too. The cap keeps the newest runs of the whole fleet, so a filter applied to
               those rows afterwards (as this tab did) never saw a failure that a busier server's newer runs had pushed past the cap: All
               Servers lost runs the single-server grid showed. The reader applies them inside each server's newest rows, ahead of the cap. */
            var statusFilter = GetSelectedStatus();
            var categoryFilter = GetSelectedCategory();
            /* #5703: the Server and Category choices that do not depend on the runs read: read once per window and every few minutes after,
               beside the grid's read; a failure costs the extra choices and never the grid. */
            var service = _dataService;
            /* The two reads start side by side, here, where the width is declared; JoinChoicesAsync only waits for both. */
            var serversTask = refreshChoices ? ReadHalfAsync("servers", "server", () => service.GetJobHistoryServersAsync(), choicesKey) : null;
            var categoriesTask = refreshChoices
                ? ReadHalfAsync("categories", "category", () => service.GetJobHistoryCategoriesAsync(windowStartUtc, windowIsLive ? null : windowEndUtc, serverId: serverId), choicesKey)
                : null;
            var choicesTask = serversTask is null || categoriesTask is null ? null : JoinChoicesAsync(serversTask, categoriesTask);
            var readTask = _dataService.GetJobHistoryAsync(sinceUtc, serverId, RowCap, untilUtc: windowIsLive ? null : windowEndUtc, runStatus: statusFilter, category: categoryFilter);
            await ViewerServerTab.AwaitReadWatchingProbeAsync(readTask, dataStartTask, "Job History");
            var all = await readTask;
            readFanOut.Release();

            /* The last supersede check and the bind sit in one synchronous run (JobHistoryLoadSteps.BindWhenCurrentAsync): a load that
               was current at the check cannot be superseded before it binds, so an older load never binds over a newer one. The
               choices are applied after the bind, by a load that is still current; the grid never waits on their read. */
            if (!await JobHistoryLoadSteps.BindWhenCurrentAsync(readTask, () => _loads.Superseded(nameof(LoadJobsAsync), gen), BindRows))
            {
                return;
            }

            var choicesApplied = ApplyKnownChoicesAsync(choicesTask, choicesKey, gen, all);

            var displayCount = JobHistoryDataGrid.Items.Count;
            NoJobsMessage.Text = EmptyNote(IsAzureSqlDatabaseSelected());
            NoJobsMessage.Visibility = displayCount == 0 ? Visibility.Visible : Visibility.Collapsed;
            RunTimeHeaderText.Text = TimeColumnTitle.For("Run Time", ViewerTimeHelper.CurrentDisplayMode, all.Any(r => r.MixedServers)); // D5: the column names its clock
            DataGridHeaderFit.Apply(JobHistoryDataGrid); // #6136: the longer title ("Run Time (server)") widens its column

            /* The cap applies to the read (all.Count), not the display count after the per-column filter popups: a column filter
               narrowing the grid must not make the "newest 2,000" label disappear when the underlying read still hit the cap.
               The Status and Category filters are in the read itself (#5703), so the count is of the runs they matched. The
               column-filter handler builds the same text from the same row count (#4966). */
            _lastReadRowCount = all.Count;
            JobCountIndicator.Text = JobHistoryCap.CountText(displayCount, _lastReadRowCount, RowCap);

            _lastRefreshed = DateTime.UtcNow;
            UpdateStaleDataIndicator();

            await UpdateAgentStatusAsync(serverId);
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            LoadingMessage.Visibility = Visibility.Collapsed;

            /* #4966: the rows are bound and the loading note is down; the probe is awaited last. This await settles it whether it
               succeeded or faulted, without throwing, so the supersede check lands before the note is written and the step below awaits a finished
               task. A probe that threw is logged by the step and hides the note. */
            await ((Task)dataStartTask).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            await ShowJobHistoryDataStartAsync(JobHistoryTruncationBanner, dataStartTask, sinceUtc, all);
            await choicesApplied;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadJobsAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load job history: {ex.Message}");

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

    /// <summary>How long the monitored-server and category choices read beside the grid stay current before they are read again.</summary>
    private static readonly TimeSpan ChoicesLifetime = TimeSpan.FromMinutes(5);

    /// <summary>Every monitored SQL Server, as the last read found them (<see cref="ViewerDataService.GetJobHistoryServerChoicesAsync"/>).</summary>
    private List<JobHistoryFilterChoices.ServerChoice> _knownServers = [];

    /// <summary>Every category with a run in the window (the picked server's when one is picked), as the last read found them (<see cref="ViewerDataService.GetJobHistoryCategoriesAsync"/>).</summary>
    private List<string> _knownCategories = [];

    /// <summary>When the choices are read again: for a different window or server, or after <see cref="ChoicesLifetime"/>.</summary>
    private readonly JobHistoryChoicesFreshness _choicesFreshness = new(ChoicesLifetime);

    /// <summary>Failed choices reads, logged once per window (the next refresh still reads again).</summary>
    private readonly ChoicesFailureLogGate<JobHistoryChoicesKey> _choicesFailures = new();

    /// <summary>Waits for the two choices reads the load started (null when this load did not start them). Neither throws: a failed
    /// read is null for its half (see <see cref="ReadHalfAsync{T}"/>).</summary>
    private static async Task<(ViewerDataService.JobHistoryServers? Servers, List<string>? Categories)> JoinChoicesAsync(
        Task<ViewerDataService.JobHistoryServers?> serversTask, Task<List<string>?> categoriesTask)
    {
        var servers = await serversTask;
        var categories = await categoriesTask;
        return (servers, categories);
    }

    /// <summary>One half of the choices read. Never throws: a failed read returns null, says so on the status line, and is written to the
    /// viewer's log once per window (the next refresh still reads again).</summary>
    private async Task<T?> ReadHalfAsync<T>(string source, string noun, Func<Task<T>> read, JobHistoryChoicesKey key) where T : class
    {
        try
        {
            var result = await read();
            _choicesFailures.Succeeded(source);
            return result;
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"failed to load the job history {noun} choices: {ex.Message}");
            if (_choicesFailures.ShouldLog(source, key))
            {
                ViewerLogger.Error("JobHistory", $"failed to load the job history {noun} choices; the drop-down keeps what it had and the read is tried again on the next refresh", ex);
            }

            return null;
        }
    }

    /// <summary>Takes the choices read beside the grid once they arrive, and only if this load is still the current one (null when this
    /// load did not read them). A half that failed keeps the last one, and the choices are read again on the next load until both halves
    /// have succeeded for this window. The grid is already bound: it never waits on this read.</summary>
    private Task<bool> ApplyKnownChoicesAsync(
        Task<(ViewerDataService.JobHistoryServers? Servers, List<string>? Categories)>? choicesTask,
        JobHistoryChoicesKey key, int gen, List<ViewerJobHistoryRow> rows) =>
        JobHistoryLoadSteps.ApplyWhenCurrentAsync(choicesTask, () => _loads.Superseded(nameof(LoadJobsAsync), gen), choices =>
        {
            ApplyChoices(choices.Servers, choices.Categories, rows);
            if (choices.Servers is not null && choices.Categories is not null)
            {
                _choicesFreshness.Record(key, DateTime.UtcNow);
            }
        });

    /// <summary>
    /// Takes the choices read beside the grid and redraws the Server and Category drop-downs from them and from <paramref name="rows"/>.
    /// A half that is null (its read failed) keeps the last one. Internal so the tests drive the tab's own redraw.
    /// </summary>
    internal void ApplyChoices(ViewerDataService.JobHistoryServers? servers, List<string>? categories, List<ViewerJobHistoryRow> rows)
    {
        if (servers is not null)
        {
            _knownServers = servers.Choices;
            _azureServerIds = servers.AzureSqlDatabaseIds;
        }

        StartCategoriesOverWhenServerChanged();
        if (categories is not null)
        {
            _knownCategories = categories;
        }

        PopulateServerFilter(rows);
        PopulateCategoryFilter(rows);

        /* #6391: the empty-grid sentence follows the picked server, and the engine list may have arrived after the grid bound. */
        NoJobsMessage.Text = EmptyNote(IsAzureSqlDatabaseSelected());
    }

    /// <summary>The Azure SQL Database servers among the Server choices, as the last read found them (they have no SQL Agent, so they never have rows).</summary>
    private IReadOnlySet<int> _azureServerIds = new HashSet<int>();

    /// <summary>The server the Category drop-down was last built for (null is All Servers), so a different pick starts the list over.</summary>
    private int? _categoryListServerId;

    /// <summary>What the empty grid says (#6391): the plain no-SQL-Agent sentence the web's Job History uses for an Azure SQL Database, else the usual note.</summary>
    internal static string EmptyNote(bool azureSqlDatabase) =>
        azureSqlDatabase ? AzureNoAgentLine : "No job runs recorded in the selected time range";

    /// <summary>The web Job History's sentence for an Azure SQL Database (job-history.js AZURE_NO_AGENT_LINE).</summary>
    internal const string AzureNoAgentLine = "Azure SQL Database has no SQL Agent, so it has no job history.";

    private bool IsAzureSqlDatabaseSelected() => GetSelectedServerId() is int id && _azureServerIds.Contains(id);

    private void PopulateServerFilter(List<ViewerJobHistoryRow> rows)
    {
        /* #5703: the choices only grow (JobHistoryFilterChoices.MergeServers): those already on offer, every monitored server (a read that
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
            offered, _knownServers, rows.Select(r => new JobHistoryFilterChoices.ServerChoice(r.ServerId, r.ServerName)), selected);

        var existingIds = ServerFilterComboBox.Items
            .OfType<ComboBoxItem>()
            .Skip(1)
            .Select(i => i.Tag?.ToString())
            .ToList();

        var newIds = servers.Select(s => s.ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture)).ToList();
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
            ServerFilterComboBox.Items.Add(new ComboBoxItem { Content = serverName, Tag = serverId.ToString(System.Globalization.CultureInfo.InvariantCulture) });
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

    /// <summary>
    /// #6391: the Category list is the picked server's categories (or the fleet's at All Servers). A different pick starts it over: what was
    /// on offer, and the categories read for the earlier pick, belong to the earlier server. Run before a choices read is taken in, so the
    /// new server's categories are not thrown away with the old ones.
    /// </summary>
    private void StartCategoriesOverWhenServerChanged()
    {
        var pickedServer = GetSelectedServerId();
        if (pickedServer == _categoryListServerId)
        {
            return;
        }

        _categoryListServerId = pickedServer;
        _knownCategories = [];
        CategoryFilterComboBox.SelectionChanged -= Filter_SelectionChanged;
        while (CategoryFilterComboBox.Items.Count > 1)
        {
            CategoryFilterComboBox.Items.RemoveAt(1);
        }

        CategoryFilterComboBox.SelectedIndex = 0;
        CategoryFilterComboBox.SelectionChanged += Filter_SelectionChanged;
    }

    private void PopulateCategoryFilter(List<ViewerJobHistoryRow> rows)
    {
        /* #5703: the choices only grow (JobHistoryFilterChoices.MergeCategories): those already on offer, every category with a run in the
           window (a read the row cap does not touch), and the rows just read. */
        StartCategoriesOverWhenServerChanged();

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

        /* The count follows the column filter, and keeps the cap label whenever the read hit the cap (#4966): this used
           to rewrite it as a bare "N run(s)", which took the label off a count whose read was still cut. */
        JobCountIndicator.Text = JobHistoryCap.CountText(JobHistoryDataGrid.Items.Count, _lastReadRowCount, RowCap);
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

    private async void TimeRangePicker_RangeChanged(object? sender, TimeRangeChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadJobsAsync();
        }
    }

    private async void Filter_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (CategoryFilterComboBox is null || NoJobsMessage is null)
        {
            return;
        }

        /* #6391: a category belongs to the server it was picked for. A different server starts the Category pick over, before the read,
           so the read does not carry the earlier server's category into a server that has no such category. */
        if (ReferenceEquals(sender, ServerFilterComboBox) && CategoryFilterComboBox.SelectedIndex > 0)
        {
            CategoryFilterComboBox.SelectionChanged -= Filter_SelectionChanged;
            CategoryFilterComboBox.SelectedIndex = 0;
            CategoryFilterComboBox.SelectionChanged += Filter_SelectionChanged;
        }

        NoJobsMessage.Text = EmptyNote(IsAzureSqlDatabaseSelected());

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
