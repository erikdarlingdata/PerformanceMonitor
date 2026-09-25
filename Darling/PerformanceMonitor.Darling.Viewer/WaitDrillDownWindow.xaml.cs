/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.WaitDrillDownHelper;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Wait Stats "Show Queries With &lt;wait&gt;" drill-down window — ported from Lite's
/// <c>Windows/WaitDrillDownWindow.xaml.cs</c>. It classifies the wait via the shared
/// <see cref="WaitDrillDownHelper.Classify"/> into one of: a correlated / uncapturable view (all queries
/// active in the window, sorted by the most correlated metric), a lock-chain view (head blockers, via the
/// shared <see cref="WaitDrillDownHelper.WalkBlockingChains"/>), or a direct wait-type filter. The rows are
/// correlated query snapshots read from Postgres (<see cref="ViewerDataService.GetQuerySnapshotsByWaitTypeAsync"/>
/// / <see cref="ViewerDataService.GetLatestQuerySnapshotsAsync"/>). The only Lite affordance dropped is the
/// live "Get Actual Plan" (the viewer never opens a SqlClient); "View Plan" shows the stored plan in a shared
/// <see cref="PlanViewerControl"/> floated above this window.
/// </summary>
public partial class WaitDrillDownWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly int _serverId;
    private readonly string _waitType;
    private readonly DateTime _fromUtc;
    private readonly DateTime _toUtc;

    private readonly DataGridFilterManager<ViewerQuerySnapshotRow> _filterManager;
    private Popup? _filterPopup;
    private ColumnFilterPopup? _filterPopupContent;
    private System.Threading.CancellationTokenSource? _actualPlanCts;

    public WaitDrillDownWindow(
        ViewerDataService dataService, int serverId, string waitType, DateTime fromUtc, DateTime toUtc)
    {
        InitializeComponent();
        _dataService = dataService;
        _serverId = serverId;
        _waitType = waitType;
        _fromUtc = fromUtc;
        _toUtc = toUtc;

        _filterManager = new DataGridFilterManager<ViewerQuerySnapshotRow>(ResultsDataGrid);

        Title = $"Wait Drill-Down: {waitType}";

        var classification = Classify(waitType);
        HeaderText.Text = classification.Category == WaitCategory.Correlated
            ? $"Queries active during {waitType} spike"
            : $"Queries experiencing {waitType}";

        Loaded += async (_, _) => await LoadDataAsync();
        ThemeManager.ThemeChanged += OnThemeChanged;
        Closed += (_, _) => { ThemeManager.ThemeChanged -= OnThemeChanged; _actualPlanCts?.Cancel(); };
    }

    private async Task LoadDataAsync()
    {
        SummaryText.Text = "Loading...";
        try
        {
            var classification = Classify(_waitType);
            SetWarningBanner(classification);

            if (classification.Category == WaitCategory.Chain)
            {
                await LoadChainDataAsync(classification);
            }
            else if (classification.Category is WaitCategory.Correlated or WaitCategory.Uncapturable)
            {
                await LoadCorrelatedDataAsync(classification);
            }
            else
            {
                await LoadDirectDataAsync(classification);
            }
        }
        catch (Exception ex)
        {
            SummaryText.Text = $"Error: {ex.Message}";
        }
    }

    private async Task LoadDirectDataAsync(WaitClassification classification)
    {
        var data = await _dataService.GetQuerySnapshotsByWaitTypeAsync(_serverId, _waitType, _fromUtc, _toUtc);
        if (data.Count == 0)
        {
            SummaryText.Text = $"No query-level data found for {_waitType} in the selected time range.";
            return;
        }

        data = SortByProperty(data, classification.SortProperty);
        _filterManager.UpdateData(data);
        SummaryText.Text = $"{data.Count} snapshot(s) | {classification.Description} | {GetTimeRangeDescription(data)}";
        ApplyInitialSort(classification.SortProperty);
    }

    private async Task LoadCorrelatedDataAsync(WaitClassification classification)
    {
        /* Fetch ALL queries in the range (no wait-type filter) — the correlated waits are too brief to
           show as the waiter's own wait_type. #4239: capped to the newest 1,000 of the window; TotalCount is
           the pre-cap match count so the note below only appears when the cap actually trimmed something. */
        var (totalCount, data) = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, _fromUtc, _toUtc);
        if (data.Count == 0)
        {
            SummaryText.Text = "No query snapshots found in the selected time range.";
            return;
        }

        data = SortByProperty(data, classification.SortProperty);
        _filterManager.UpdateData(data);
        SummaryText.Text = $"{data.Count} snapshot(s) | {classification.Description} | {GetTimeRangeDescription(data)}" +
            (data.Count < totalCount ? $" | showing newest 1,000 of {totalCount:N0}" : "");
        ApplyInitialSort(classification.SortProperty);
    }

    private async Task LoadChainDataAsync(WaitClassification classification)
    {
        var waiters = await _dataService.GetQuerySnapshotsByWaitTypeAsync(_serverId, _waitType, _fromUtc, _toUtc);
        if (waiters.Count == 0)
        {
            SummaryText.Text = $"No query-level data found for {_waitType} in the selected time range.";
            return;
        }

        /* #4239: allSnapshots is now capped to the newest 1,000 of the window (see LoadCorrelatedDataAsync) —
           the chain walk below only sees those rows, so a head blocker whose own snapshot falls outside the
           newest 1,000 is missed. truncationNote surfaces that instead of silently absorbing it. */
        var (allTotalCount, allSnapshots) = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, _fromUtc, _toUtc);
        var truncationNote = allSnapshots.Count < allTotalCount ? $" | showing newest 1,000 of {allTotalCount:N0} snapshots" : "";

        var headBlockerInfos = WalkBlockingChains(waiters.Select(ToSnapshotInfo), allSnapshots.Select(ToSnapshotInfo));

        if (headBlockerInfos.Count == 0)
        {
            _filterManager.UpdateData(waiters);
            SummaryText.Text = $"{waiters.Count} snapshot(s) | {classification.Description} | {GetTimeRangeDescription(waiters)} | No blocking chains found, showing waiters{truncationNote}";
            return;
        }

        /* Look up the original full rows for each head blocker and stamp the chain metadata. */
        var snapshotLookup = allSnapshots
            .GroupBy(s => (s.CollectionTime, s.SessionId))
            .ToDictionary(g => g.Key, g => g.First());

        var headBlockerRows = new List<ViewerQuerySnapshotRow>();
        foreach (var hb in headBlockerInfos)
        {
            if (snapshotLookup.TryGetValue((hb.CollectionTime, hb.SessionId), out var row))
            {
                row.BlockedSessionCount = hb.BlockedSessionCount;
                row.ChainBlockingPath = hb.BlockingPath;
                headBlockerRows.Add(row);
            }
        }

        if (headBlockerRows.Count == 0)
        {
            _filterManager.UpdateData(waiters);
            SummaryText.Text = $"{waiters.Count} snapshot(s) | {classification.Description} | {GetTimeRangeDescription(waiters)} | Head blockers not in snapshots, showing waiters{truncationNote}";
            return;
        }

        InsertChainColumn();
        _filterManager.UpdateData(headBlockerRows);
        SummaryText.Text = $"{headBlockerRows.Count} head blocker(s) from {waiters.Count} waiting session(s) | {classification.Description} | {GetTimeRangeDescription(headBlockerRows)}{truncationNote}";
    }

    /// <summary>Inserts the "Blocking Path" chain column at the front (Lite's InsertChainColumns —
    /// "Blocked" is already a static column here).</summary>
    private void InsertChainColumn()
    {
        var filterButton = new Button { Tag = "ChainBlockingPath", Margin = new Thickness(0, 0, 4, 0) };
        filterButton.SetResourceReference(StyleProperty, "ColumnFilterButtonStyle");
        filterButton.Click += Filter_Click;

        var header = new StackPanel { Orientation = Orientation.Horizontal };
        header.Children.Add(filterButton);
        header.Children.Add(new TextBlock { Text = "Blocking Path", FontWeight = FontWeights.Bold, VerticalAlignment = VerticalAlignment.Center });

        ResultsDataGrid.Columns.Insert(0, new DataGridTextColumn
        {
            Header = header,
            Binding = new System.Windows.Data.Binding("ChainBlockingPath"),
            Width = new DataGridLength(250),
        });
    }

    private void SetWarningBanner(WaitClassification classification)
    {
        if (classification.Category == WaitCategory.Uncapturable)
        {
            WarningText.Text = $"Sessions experiencing {_waitType} waits may not be captured in query snapshots " +
                               "because they may lack assigned worker threads. Showing all queries in this time range.";
            WarningBanner.Visibility = Visibility.Visible;
        }
        else if (classification.Category == WaitCategory.Correlated)
        {
            WarningText.Text = $"{_waitType} waits are too brief to appear in query snapshots. " +
                               "Showing all queries active during this period, sorted by the most correlated metric.";
            ShowInfoBanner();
        }
        else if (classification.Category == WaitCategory.Chain)
        {
            WarningText.Text = $"Showing head blockers (the cause of {_waitType} waits), not the waiting sessions themselves.";
            ShowInfoBanner();
        }
    }

    private void ShowInfoBanner()
    {
        WarningBanner.Visibility = Visibility.Visible;
        WarningBanner.Background = new SolidColorBrush(Color.FromArgb(0x3D, 0x00, 0x33, 0x66));
        WarningBanner.BorderBrush = new SolidColorBrush(Color.FromArgb(0x66, 0x00, 0x55, 0x99));
        WarningText.Foreground = new SolidColorBrush(Color.FromRgb(0x66, 0xBB, 0xFF));
    }

    private static SnapshotInfo ToSnapshotInfo(ViewerQuerySnapshotRow row) => new()
    {
        SessionId = row.SessionId,
        BlockingSessionId = row.BlockingSessionId,
        CollectionTime = row.CollectionTime,
        DatabaseName = row.DatabaseName,
        Status = row.Status,
        QueryText = row.QueryText,
        WaitType = row.WaitType,
        WaitTimeMs = row.WaitTimeMs,
        CpuTimeMs = row.CpuTimeMs,
        Reads = row.Reads,
        Writes = row.Writes,
        LogicalReads = row.LogicalReads,
    };

    /// <summary>Sorts the rows by the classified metric (Lite's SortByProperty). Pure, unit-tested.</summary>
    internal static List<ViewerQuerySnapshotRow> SortByProperty(List<ViewerQuerySnapshotRow> data, string property) =>
        property switch
        {
            "CpuTimeMs" => data.OrderByDescending(r => r.CpuTimeMs).ToList(),
            "Reads" => data.OrderByDescending(r => r.Reads).ToList(),
            "Writes" => data.OrderByDescending(r => r.Writes).ToList(),
            "Dop" => data.OrderByDescending(r => r.Dop).ToList(),
            "GrantedQueryMemoryGb" => data.OrderByDescending(r => r.GrantedQueryMemoryGb).ToList(),
            "WaitTimeMs" => data.OrderByDescending(r => r.WaitTimeMs).ToList(),
            _ => data,
        };

    private void ApplyInitialSort(string property)
    {
        var columnHeader = property switch
        {
            "CpuTimeMs" => "CPU (ms)",
            "Reads" => "Reads",
            "Writes" => "Writes",
            "Dop" => "DOP",
            "GrantedQueryMemoryGb" => "Memory (GB)",
            "WaitTimeMs" => "Wait (ms)",
            _ => null,
        };
        if (columnHeader == null) return;

        foreach (var column in ResultsDataGrid.Columns)
        {
            if (column.Header is StackPanel sp &&
                sp.Children.OfType<TextBlock>().FirstOrDefault()?.Text == columnHeader)
            {
                column.SortDirection = ListSortDirection.Descending;
                break;
            }
        }
    }

    private string GetTimeRangeDescription(List<ViewerQuerySnapshotRow> data)
    {
        if (data.Count == 0) return "";
        var first = ViewerTimeHelper.ForDisplay(data.Min(r => r.CollectionTime));
        var last = ViewerTimeHelper.ForDisplay(data.Max(r => r.CollectionTime));
        return $"{first:yyyy-MM-dd HH:mm:ss} to {last:yyyy-MM-dd HH:mm:ss}";
    }

    private void OnThemeChanged(string _) => _filterManager.UpdateFilterButtonStyles();

    // ── Column Filter Popup (mirrors ViewerServerTab.Filters.cs) ──

    private void EnsureFilterPopup()
    {
        if (_filterPopup == null)
        {
            _filterPopupContent = new ColumnFilterPopup();
            _filterPopup = new Popup
            {
                Child = _filterPopupContent,
                StaysOpen = false,
                Placement = PlacementMode.Bottom,
                AllowsTransparency = true,
            };
        }
    }

    private void Filter_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not Button button || button.Tag is not string columnName) return;

        EnsureFilterPopup();

        _filterPopupContent!.FilterApplied -= FilterPopup_FilterApplied;
        _filterPopupContent.FilterCleared -= FilterPopup_FilterCleared;
        _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
        _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

        _filterManager.Filters.TryGetValue(columnName, out var existingFilter);
        _filterPopupContent.Initialize(columnName, existingFilter);

        _filterPopup!.PlacementTarget = button;
        _filterPopup.IsOpen = true;
    }

    private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
    {
        if (_filterPopup != null) _filterPopup.IsOpen = false;
        _filterManager.SetFilter(e.FilterState);
    }

    private void FilterPopup_FilterCleared(object? sender, EventArgs e)
    {
        if (_filterPopup != null) _filterPopup.IsOpen = false;
    }

    // ── Copy / Export / Plan ──

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);
    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);
    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);
    private void ExportToCsv_Click(object sender, RoutedEventArgs e) =>
        DataGridExport.ExportToCsv(sender, "wait_drill_down", ViewerExportSettings.CsvSeparator);

    /// <summary>
    /// "View Plan" — shows the row's stored estimated/live plan in a shared <see cref="PlanViewerControl"/>
    /// floated above this window (Lite showed a PlanViewerWindow; Darling reuses the shared
    /// <see cref="GraphViewerWindow"/> host). No live "Get Actual Plan" — the viewer never re-executes.
    /// </summary>
    private async void ViewPlan_Click(object sender, RoutedEventArgs e)
    {
        if ((ResultsDataGrid.CurrentItem ?? ResultsDataGrid.SelectedItem) is not ViewerQuerySnapshotRow row) return;

        string? planXml;
        bool isActual;
        try
        {
            (planXml, isActual) = await FetchSnapshotPlanAsync(row);
        }
        catch (Exception ex)
        {
            /* #4239: FetchSnapshotPlanAsync is a Postgres round-trip inside an async void handler — an
               unguarded await here would let a store error (connection drop, timeout) reach the dispatcher
               as an unhandled exception. Same fetch-failure idiom as ViewerServerTab.Plans.cs. */
            MessageBox.Show(this, $"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            return;
        }

        if (string.IsNullOrEmpty(planXml))
        {
            MessageBox.Show(this,
                "No execution plan was captured for this snapshot row.",
                "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var label = $"{(isActual ? "Live" : "Est")} Plan - SPID {row.SessionId}";
        var viewer = new PlanViewerControl();
        try
        {
            await viewer.LoadPlan(planXml, label, row.QueryText);
        }
        catch (Exception ex)
        {
            viewer.Cleanup();
            MessageBox.Show(this,
                $"Failed to load the execution plan:\n\n{ex.Message}",
                "Plan Load Error", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        /* GraphViewerWindow only auto-Cleanup()s IGraphViewer content; PlanViewerControl is not one, so
           unsubscribe it from ThemeManager on close explicitly. */
        var window = GraphViewerWindow.ShowGraph(this, viewer, label);
        window.Closed += (_, _) => viewer.Cleanup();
    }

    /// <summary>
    /// Fetches this row's plan by its natural key, preferring the stored ACTUAL (live) plan and falling back
    /// to the estimated plan — mirrors the original in-row preference (<c>row.LiveQueryPlan ?? row.QueryPlan</c>).
    /// WaitDrillDownWindow's rows only ever come from the two store reads (<c>GetQuerySnapshotsByWaitTypeAsync</c>
    /// / <c>GetLatestQuerySnapshotsAsync</c>), never the live DMV path, so — unlike the Active Queries grid's
    /// live "Current Active Queries" sub-tab — there is no in-row plan to check first (#4239).
    /// </summary>
    private async Task<(string? PlanXml, bool IsActual)> FetchSnapshotPlanAsync(ViewerQuerySnapshotRow row)
    {
        if (row.HasLiveQueryPlan)
        {
            var live = await _dataService.GetQuerySnapshotPlanXmlAsync(_serverId, row.CollectionTime, row.SessionId, row.RequestId, live: true);
            if (!string.IsNullOrEmpty(live)) return (live, true);
        }
        if (row.HasQueryPlan)
        {
            var est = await _dataService.GetQuerySnapshotPlanXmlAsync(_serverId, row.CollectionTime, row.SessionId, row.RequestId, live: false);
            return (est, false);
        }
        return (null, false);
    }

    /// <summary>"Get Actual Plan" — asks the SERVICE to RE-EXECUTE the RIGHT-CLICKED snapshot's captured query
    /// (SET STATISTICS XML) and floats the captured actual plan. Identifier-only: the service resolves the text
    /// from query_snapshots by (collection_time, session_id). Shared consent + flag + read-only guard via
    /// <see cref="ViewerActualPlanFlow"/>. Re-executing a snapshot's mid-flight statement carries the usual
    /// side-effect risk — the consent gate owns it.</summary>
    private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
    {
        if ((ResultsDataGrid.CurrentItem ?? ResultsDataGrid.SelectedItem) is not ViewerQuerySnapshotRow row) return;
        if (string.IsNullOrWhiteSpace(row.QueryText))
        {
            MessageBox.Show(this, "No query text was captured for this snapshot, so its actual plan cannot be re-executed.",
                "No Query Text", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        /* #4239: fetch the row's stored plan BEFORE the modification-detection gate below — a stored-row read
           no longer carries plan XML in-row, so without this fetch estimatedPlanXml would always be null and
           every snapshot would show as "may modify data" regardless of what was actually captured. A null
           RESULT here (truly no plan was captured for this request) still fails safe to "may modify" — the
           one deliberate behavior change from before #4239, called out in the PR body. A thrown EXCEPTION
           (store error) is different: it aborts the whole re-execute flow below rather than silently falling
           through to "may modify", since the fetch failure means the modification check itself is unreliable. */
        string? estimatedPlanXml;
        try
        {
            (estimatedPlanXml, _) = await FetchSnapshotPlanAsync(row);
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, $"Failed to retrieve plan: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            return;
        }
        var argsJson = ViewerDataService.BuildActualPlanArgsForSnapshot(row.CollectionTime, row.SessionId, row.DatabaseName);
        var label = $"Actual Plan - SPID {row.SessionId}";

        _actualPlanCts?.Dispose();
        _actualPlanCts = new System.Threading.CancellationTokenSource();

        var planXml = await ViewerActualPlanFlow.RequestActualPlanAsync(
            this, _dataService, _serverId, "the monitored server", row.DatabaseName, row.QueryText, estimatedPlanXml, argsJson,
            onStarted: () => System.Windows.Input.Mouse.OverrideCursor = System.Windows.Input.Cursors.Wait,
            onFinished: () => System.Windows.Input.Mouse.OverrideCursor = null,
            _actualPlanCts.Token);

        if (planXml != null)
            await ViewerActualPlanFlow.OpenFloatingPlanAsync(this, planXml, label, row.QueryText);
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
