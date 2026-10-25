/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Fleet Sweeps (#6390): the fleet sweep engine's record, read only. The Viewer twin of the web's Fleet Sweeps page: the
/// sweeps inside the picked span (default one hour, as on the web), the selected sweep in full (the newest until one is
/// selected) and the watch-item worklist. Every read goes through <see cref="ViewerDataService"/>'s Fleet Sweeps reads,
/// which call the same store reads and presentation builders the web serves, so the two surfaces show the same rows.
/// Load failures surface on the host status bar via <see cref="StatusChanged"/>.
/// </summary>
public partial class FleetSweepsTab : UserControl
{
    private ViewerDataService? _dataService;
    private readonly ScopedLoadGenerations _loads = new();

    /// <summary>The selected sweep's id (a string, as on the web: a tick-scale id is past what a double holds). Null = the newest.</summary>
    private string? _selectedSweepId;
    private bool _binding;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    public FleetSweepsTab()
    {
        InitializeComponent();
        TimeRangePickerControl.ZoneProvider = ViewerTimeHelper.FleetWideZone;
        /* The web page's default span: one hour, which at the shipped hourly cadence lands on the newest sweep. */
        TimeRangePickerControl.Value = TimeRangeSpec.Relative(TimeSpan.FromHours(1));
    }

    /// <summary>Wires the data service.</summary>
    public void Initialize(ViewerDataService dataService) => _dataService = dataService;

    /// <summary>Reloads the timeline, the selected sweep and the worklist (the shell calls this when the tab becomes visible / on the timer).</summary>
    public async Task RefreshAsync()
    {
        await LoadTimelineAsync();
        await Task.WhenAll(LoadDetailAsync(), LoadWatchItemsAsync());
    }

    private async Task LoadTimelineAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        var gen = _loads.Claim(nameof(LoadTimelineAsync));
        NoRunsMessage.Visibility = Visibility.Collapsed;
        LoadingMessage.Visibility = Visibility.Visible;
        try
        {
            var (startUtc, endUtc, _) = ViewerTimeRangeWindow.Window(
                TimeRangePickerControl.Value, DateTime.UtcNow, ViewerTimeHelper.FleetWideZone());
            var node = await _dataService.GetFleetSweepTimelineAsync(startUtc, endUtc);
            if (_loads.Superseded(nameof(LoadTimelineAsync), gen)) return;

            /* The cadence line is display only, from the same alert settings the web page reads; a failed read leaves it blank and never costs the timeline. */
            try
            {
                CadenceIndicator.Text = FleetSweepsRows.CadenceText(await _dataService.GetAlertSettingsAsync());
            }
            catch (Exception)
            {
                CadenceIndicator.Text = "";
            }

            if (_loads.Superseded(nameof(LoadTimelineAsync), gen)) return;

            var rows = FleetSweepsRows.Runs(node);
            _binding = true;
            try
            {
                RunsDataGrid.ItemsSource = rows;
                RunsDataGrid.SelectedItem = rows.FirstOrDefault(r => r.SweepId == _selectedSweepId);
            }
            finally
            {
                _binding = false;
            }

            NoRunsMessage.Text = "No sweeps in this span. Widen the span, or check that fleet_sweep.enabled is on — a gap in this timeline is a missed sweep, with its cause on the service log.";
            NoRunsMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            LastRefreshedIndicator.Text = "Last refreshed " + ViewerTimeHelper.FormatFleetWide(DateTime.UtcNow, "HH:mm:ss");
        }
        catch (Exception ex)
        {
            /* A load the operator has moved past does not report its failure over the newer load's status. */
            if (_loads.Superseded(nameof(LoadTimelineAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load the fleet sweep timeline: {ex.Message}");
        }
        finally
        {
            if (!_loads.Superseded(nameof(LoadTimelineAsync), gen))
            {
                LoadingMessage.Visibility = Visibility.Collapsed;
            }
        }
    }

    private async Task LoadDetailAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        var gen = _loads.Claim(nameof(LoadDetailAsync));
        try
        {
            long? id = _selectedSweepId is { } s && long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;
            var detail = await _dataService.GetFleetSweepDetailAsync(id);
            if (_loads.Superseded(nameof(LoadDetailAsync), gen)) return;

            if (detail is null)
            {
                /* Absence is not a fault: no sweep yet (fresh install / engine off), or a selected sweep that retention has pruned. */
                DetailHeaderPanel.Visibility = Visibility.Collapsed;
                DetailTabs.Visibility = Visibility.Collapsed;
                NoDetailMessage.Text = (id is null ? "No sweep has been recorded yet." : "Sweep not found.")
                    + " Sweeps appear here once fleet_sweep.enabled is on and the cadence elapses.";
                NoDetailMessage.Visibility = Visibility.Visible;
                return;
            }

            ShowDetail(detail);
        }
        catch (Exception ex)
        {
            /* A load the operator has moved past does not report its failure over the newer load's status. */
            if (_loads.Superseded(nameof(LoadDetailAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load the fleet sweep: {ex.Message}");
        }
    }

    private void ShowDetail(JsonObject detail)
    {
        NoDetailMessage.Visibility = Visibility.Collapsed;
        DetailHeaderPanel.Visibility = Visibility.Visible;
        DetailTabs.Visibility = Visibility.Visible;

        DetailTitle.Text = "Sweep at " + FleetSweepsRows.Local(detail["swept_at"]?.GetValue<string>() ?? "");
        DetailSummary.Text = FleetSweepsRows.DetailSummary(detail);
        MutedBanner.Visibility = FleetSweepsRows.Bool(detail, "alerts_enabled", true) ? Visibility.Collapsed : Visibility.Visible;
        DeadBanner.Visibility = FleetSweepsRows.Bool(detail, "instruments_alive", true) ? Visibility.Collapsed : Visibility.Visible;
        RestartNotice.Visibility = detail["report"] is JsonObject report && FleetSweepsRows.Bool(report, "post_restart_window", false)
            ? Visibility.Visible : Visibility.Collapsed;

        TransitionsDataGrid.ItemsSource = FleetSweepsRows.Transitions(detail);
        ChangesNote.Text = FleetSweepsRows.ChangesNote(detail);

        /* The ledger tab shows whenever the sweep carries one (every master-off sweep), empty included: the operator sees what silence cost,
           or that it cost nothing. An alerts-on sweep carries none, because no such check was made. */
        var ledger = FleetSweepsRows.WouldHavePaged(detail);
        WouldHavePagedTabItem.Visibility = ledger is null ? Visibility.Collapsed : Visibility.Visible;
        WouldHavePagedDataGrid.ItemsSource = ledger;
        WouldHavePagedEmpty.Visibility = ledger is { Count: 0 } ? Visibility.Visible : Visibility.Collapsed;

        var verdicts = FleetSweepsRows.Verdicts(detail);
        VerdictsDataGrid.ItemsSource = verdicts;
        VerdictsEmpty.Visibility = verdicts.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        LivenessText.Text = FleetSweepsRows.Liveness(detail);

        if (DetailTabs.SelectedItem is TabItem { Visibility: Visibility.Collapsed })
        {
            DetailTabs.SelectedIndex = 0;
        }
    }

    private async Task LoadWatchItemsAsync()
    {
        if (_dataService == null)
        {
            return;
        }

        var gen = _loads.Claim(nameof(LoadWatchItemsAsync));
        try
        {
            var closed = ShowClosedCheckBox.IsChecked == true;
            var node = await _dataService.GetFleetSweepWatchItemsAsync(closed ? "closed" : null);
            if (_loads.Superseded(nameof(LoadWatchItemsAsync), gen)) return;

            var rows = FleetSweepsRows.WatchItems(node);
            WatchItemsDataGrid.ItemsSource = rows;
            WatchLabel.Text = closed ? "Closed watch items" : "Open right now (open + carried)";
            NoWatchMessage.Text = closed
                ? "No closed watch items."
                : "Nothing is open right now — no watch item is open or carried.";
            NoWatchMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            /* A load the operator has moved past does not report its failure over the newer load's status. */
            if (_loads.Superseded(nameof(LoadWatchItemsAsync), gen)) return;

            StatusChanged?.Invoke($"failed to load the fleet sweep watch items: {ex.Message}");
        }
    }

    private async void TimeRangePicker_RangeChanged(object? sender, TimeRangeChangedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadTimelineAsync();
        }
    }

    private async void RunsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_binding)
        {
            return;
        }

        /* A cleared selection (a refresh that no longer lists the sweep) keeps the id, so the sweep stays on screen; a pick replaces it. */
        if (RunsDataGrid.SelectedItem is FleetSweepRunRow row)
        {
            _selectedSweepId = row.SweepId;
            await LoadDetailAsync();
        }
    }

    private async void ShowClosedCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        if (IsLoaded)
        {
            await LoadWatchItemsAsync();
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await RefreshAsync();
}
