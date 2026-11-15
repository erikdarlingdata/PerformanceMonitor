/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Store Health (#8787): the health of the monitoring store's own tables and indexes, read only. The Viewer twin of the web's Store
/// health page. Every read goes through <see cref="ViewerDataService"/>'s store health reads, which call the same Storage readers the web
/// page and the MCP tools call. The tables, the indexes and the maintenance check are three separate reads started together: the tables
/// grid and the header render as soon as the tables read returns (the indexes read takes about as long again), and the index sections
/// say they are loading until theirs does. Load failures surface on the host status bar via <see cref="StatusChanged"/> and in the
/// section that failed.
/// <para>A refresh of a page that already shows rows repaints in place (#8819): the grids and the summary lines stay readable and the
/// strip beside Refresh says "Refreshing…". The "Loading…" text is for a section with nothing on screen. A refresh that comes right
/// after a successful one is skipped, so a trigger that queued while a load ran (the refresh timer, a tab pick) does not run the same
/// slow reads again the moment the first ends.</para>
/// </summary>
public partial class StoreHealthTab : UserControl
{
    private ViewerDataService? _dataService;
    private readonly ScopedLoadGenerations _loads = new();
    private bool _measuring;

    /// <summary>Each read's last outcome: true once its content is on screen, false once it failed. A read that a newer one superseded
    /// leaves its outcome to that newer read.</summary>
    private bool _tablesLoaded;
    private bool _indexesLoaded;
    private bool _readinessLoaded;

    /// <summary>True while a refresh runs. A second trigger that arrives meanwhile returns at once: the load on its way answers it.</summary>
    private bool _refreshing;

    /// <summary>The stamp text ("15:14:07 UTC") of the last refresh that loaded all three reads; null before the first.</summary>
    private string? _lastStamp;

    /// <summary>When that refresh ended, on the monotonic clock; null before the first.</summary>
    private long? _lastLoadedAt;

    /// <summary>How long after a successful refresh another one that nothing forced is skipped (#8819). The tables read takes seconds on a
    /// large store, and a trigger queued behind a running load starts the moment it ends. The Refresh button is never skipped.</summary>
    internal TimeSpan FreshFor { get; set; } = TimeSpan.FromSeconds(5);

    /* #8549, #8819: the first open's clock (see ViewerFirstOpen): the shell starts it when the tab becomes visible and the tab's first load
       runs on it, so a wait behind another tab's refresh counts. Every later load runs on its own clock. A slow one writes a SlowLoad line. */
    private readonly ViewerFirstOpen _firstOpen = new();

    /// <summary>Where a slow load's line goes; null is the Viewer log's "SlowLoad" source. A test swaps it to read the line.</summary>
    internal Action<string>? SlowLoadSink { get; set; }

    /// <summary>The slow-load threshold of every load of this tab. A test lowers it to read a line without waiting three seconds.</summary>
    internal int SlowLoadThresholdMs { get => _firstOpen.SlowThresholdMs; set => _firstOpen.SlowThresholdMs = value; }

    /// <summary>Test seams for the three reads. Null reads the store through the data service.</summary>
    internal Func<Task<List<DarlingStoreTableHealthReader.StoreTableHealthRow>>>? TablesRead { get; set; }
    internal Func<Task<StoreIndexHealth>>? IndexesRead { get; set; }
    internal Func<Task<DarlingStoreMaintenanceReadinessReader.StoreMaintenanceReadiness>>? ReadinessRead { get; set; }

    /// <summary>The shell calls this when it makes the tab visible, so a wait behind another tab's refresh counts in a slow first open's line.</summary>
    internal void BeginFirstOpen() => _firstOpen.Begin();

    /// <summary>The shell calls this when the fleet lane starts the tab's refresh.</summary>
    internal void FirstOpenRefreshStarted() => _firstOpen.RefreshStarted();

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public StoreHealthTab()
    {
        InitializeComponent();
    }

    /// <summary>Wires the data service.</summary>
    public void Initialize(ViewerDataService dataService) => _dataService = dataService;

    /// <summary>Reruns the tables, indexes and maintenance reads together (the shell calls this when the tab becomes visible and on its timer).
    /// Skipped when a refresh is running, or when one succeeded less than <see cref="FreshFor"/> ago.</summary>
    public Task RefreshAsync() => RefreshAsync(force: false);

    /// <summary>The refresh. <paramref name="force"/> is the Refresh button: it skips the freshness window, but not a refresh already running.</summary>
    internal async Task RefreshAsync(bool force)
    {
        if (_dataService == null && (TablesRead is null || IndexesRead is null || ReadinessRead is null))
        {
            return;
        }

        if (_refreshing)
        {
            return;
        }

        if (!force && LastLoadIsRecent())
        {
            ShowStamp();
            return;
        }

        _refreshing = true;
        var timer = _firstOpen.Take() ?? new ViewerLoadTimer(_firstOpen.SlowThresholdMs);
        try
        {
            ShowRefreshing();
            await Task.WhenAll(LoadTablesAsync(timer), LoadIndexesAsync(timer), LoadReadinessAsync(timer));
            if (!StampIfLoaded())
            {
                ShowStamp();
            }
        }
        finally
        {
            _refreshing = false;
            timer.FinishAndLog("Store Health", SlowLoadSink);
        }
    }

    /// <summary>True when the last refresh loaded all three reads less than <see cref="FreshFor"/> ago.</summary>
    private bool LastLoadIsRecent() =>
        _lastLoadedAt is { } at && _tablesLoaded && _indexesLoaded && _readinessLoaded && Stopwatch.GetElapsedTime(at) < FreshFor;

    /// <summary>The shell calls this the moment the tab is picked, before the fleet lane may have started its load (another tab's load can
    /// hold the lane for seconds), so the tab says it is refreshing at once. A tab with nothing on screen already says "Loading…".</summary>
    internal void ShowRefreshPending()
    {
        if (!_refreshing && !LastLoadIsRecent())
        {
            ShowRefreshing();
        }
    }

    /// <summary>The strip beside Refresh while a refresh runs: it keeps the last stamp, so the page is still read against the time it was true.</summary>
    private void ShowRefreshing() =>
        LastRefreshedIndicator.Text = _lastStamp is null ? "Loading…" : $"Refreshing… (last refreshed {_lastStamp})";

    /// <summary>The strip beside Refresh when no refresh runs: the last stamp, or nothing before the first.</summary>
    private void ShowStamp() =>
        LastRefreshedIndicator.Text = _lastStamp is null ? "" : "Last refreshed " + _lastStamp;

    /// <summary>Raises "store health: refreshed …" when all three reads last finished. Each read reports its own failure, so the stamp never
    /// writes over one. Returns whether it stamped.</summary>
    private bool StampIfLoaded()
    {
        if (!(_tablesLoaded && _indexesLoaded && _readinessLoaded))
        {
            return false;
        }

        _lastStamp = ViewerTimeHelper.RefreshedStamp();
        _lastLoadedAt = Stopwatch.GetTimestamp();
        ShowStamp();
        StatusChanged?.Invoke($"store health: refreshed {_lastStamp}");
        return true;
    }

    private async Task LoadTablesAsync(ViewerLoadTimer timer)
    {
        var gen = _loads.Claim(nameof(LoadTablesAsync));
        /* #8819: a refresh over rows already on screen keeps them and the summary line; the loading text is for a grid with nothing in it. */
        if (TablesDataGrid.Items.Count == 0)
        {
            TablesMessage.Text = "Loading store tables…";
            TablesMessage.Visibility = Visibility.Visible;
            HeaderText.Text = "Loading store tables…";
        }

        try
        {
            var read = await ViewerServerTab.Timed(timer, "store tables read", TablesRead?.Invoke() ?? _dataService!.GetStoreTableHealthAsync());
            if (_loads.Superseded(nameof(LoadTablesAsync), gen)) return;

            var selected = (TablesDataGrid.SelectedItem as StoreTableRow)?.Table;
            var rows = StoreHealthRows.Tables(read);
            TablesDataGrid.ItemsSource = rows;
            TablesDataGrid.SelectedItem = selected is null ? null : rows.Find(r => r.Table == selected);
            HeaderText.Text = StoreHealthRows.HeaderText(read);
            TablesMessage.Text = "The store has no tables to show.";
            TablesMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            _tablesLoaded = true;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadTablesAsync), gen)) return;

            _tablesLoaded = false;
            TablesDataGrid.ItemsSource = null;
            HeaderText.Text = "The store tables failed to load.";
            TablesMessage.Text = $"The store tables failed to load: {ex.Message}";
            TablesMessage.Visibility = Visibility.Visible;
            StatusChanged?.Invoke($"failed to load the store tables: {ex.Message}");
        }

        UpdateMeasureButton();
    }

    private async Task LoadIndexesAsync(ViewerLoadTimer timer)
    {
        var gen = _loads.Claim(nameof(LoadIndexesAsync));
        if (IndexesDataGrid.Items.Count == 0)
        {
            IndexesMessage.Text = "Loading store indexes…";
            IndexesMessage.Visibility = Visibility.Visible;
            IndexHeaderText.Text = "Loading store indexes…";
        }

        if (CoverageDataGrid.Items.Count == 0)
        {
            CoverageMessage.Text = "Loading partition index coverage…";
            CoverageMessage.Visibility = Visibility.Visible;
        }

        try
        {
            var read = await ViewerServerTab.Timed(timer, "store indexes read", IndexesRead?.Invoke() ?? _dataService!.GetStoreIndexHealthAsync());
            if (_loads.Superseded(nameof(LoadIndexesAsync), gen)) return;

            var indexRows = StoreHealthRows.Indexes(read.Indexes);
            IndexesDataGrid.ItemsSource = indexRows;
            IndexHeaderText.Text = StoreHealthRows.IndexHeaderText(read.Indexes);
            IndexesMessage.Text = "The store has no indexes to show.";
            IndexesMessage.Visibility = indexRows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

            var coverageRows = StoreHealthRows.CoverageRows(read.Coverage);
            CoverageDataGrid.ItemsSource = coverageRows;
            CoverageDataGrid.Visibility = coverageRows.Count == 0 ? Visibility.Collapsed : Visibility.Visible;
            CoverageMessage.Text = "No partitioned tables in the store have indexes to check.";
            CoverageMessage.Visibility = coverageRows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            _indexesLoaded = true;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadIndexesAsync), gen)) return;

            _indexesLoaded = false;
            IndexesDataGrid.ItemsSource = null;
            CoverageDataGrid.ItemsSource = null;
            CoverageDataGrid.Visibility = Visibility.Collapsed;
            IndexHeaderText.Text = "The store indexes failed to load.";
            IndexesMessage.Text = $"The store indexes failed to load: {ex.Message}";
            IndexesMessage.Visibility = Visibility.Visible;
            CoverageMessage.Text = "Partition index coverage failed to load.";
            CoverageMessage.Visibility = Visibility.Visible;
            StatusChanged?.Invoke($"failed to load the store indexes: {ex.Message}");
        }
    }

    private async Task LoadReadinessAsync(ViewerLoadTimer timer)
    {
        var gen = _loads.Claim(nameof(LoadReadinessAsync));
        /* A verdict already on screen stays until the new one lands. */
        if (!_readinessLoaded)
        {
            ReadinessTextBlock.Text = "Checking whether now is a safe time for maintenance…";
        }

        try
        {
            var readiness = await ViewerServerTab.Timed(timer, "store maintenance check", ReadinessRead?.Invoke() ?? _dataService!.GetStoreMaintenanceReadinessAsync());
            if (_loads.Superseded(nameof(LoadReadinessAsync), gen)) return;

            ShowReadiness(StoreHealthRows.ReadinessParts(readiness));
            _readinessLoaded = true;
        }
        catch (Exception ex)
        {
            if (_loads.Superseded(nameof(LoadReadinessAsync), gen)) return;

            _readinessLoaded = false;
            ReadinessTextBlock.Text = $"The maintenance check failed: {ex.Message}";
            StatusChanged?.Invoke($"failed to run the store maintenance check: {ex.Message}");
        }
    }

    /// <summary>Fills the verdict block: the verdict lead in semibold, the sentences and reasons after it in normal weight.</summary>
    private void ShowReadiness((string Lead, string After) parts)
    {
        ReadinessTextBlock.Inlines.Clear();
        if (parts.Lead.Length > 0)
        {
            ReadinessTextBlock.Inlines.Add(new Run(parts.Lead) { FontWeight = FontWeights.SemiBold });
        }

        if (parts.After.Length > 0)
        {
            ReadinessTextBlock.Inlines.Add(new Run(parts.After));
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await RefreshAsync(force: true);

    private void TablesDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e) => UpdateMeasureButton();

    /// <summary>Measure is on for a plain table, and off while a measure runs (one table at a time).</summary>
    private void UpdateMeasureButton()
    {
        var target = (TablesDataGrid.SelectedItem as StoreTableRow)?.MeasureTarget;
        MeasureButton.IsEnabled = target is not null && !_measuring;
        if (!_measuring && TablesDataGrid.SelectedItem is StoreTableRow { MeasureTarget: null } other)
        {
            MeasureResultText.Text = $"{other.Table} is a {other.Kind}. Measure works on plain tables. {StoreHealthRows.MeasureHint}";
        }
    }

    private async void MeasureButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService == null || _measuring || (TablesDataGrid.SelectedItem as StoreTableRow)?.MeasureTarget is not { } target)
        {
            return;
        }

        _measuring = true;
        MeasureButton.IsEnabled = false;
        MeasureResultText.Text = $"Measuring {target}. This reads every page of the table.";
        var clock = Stopwatch.StartNew();
        try
        {
            var result = await _dataService.MeasureStoreTableAsync(target);
            MeasureResultText.Text = StoreHealthRows.MeasureText(target, result, clock.Elapsed);
        }
        catch (Exception ex)
        {
            MeasureResultText.Text = $"Measuring {target} failed: {ex.Message}";
            StatusChanged?.Invoke($"failed to measure {target}: {ex.Message}");
        }
        finally
        {
            _measuring = false;
            UpdateMeasureButton();
        }
    }
}
