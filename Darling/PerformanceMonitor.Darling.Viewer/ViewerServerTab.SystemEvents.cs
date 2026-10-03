/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The System Events inner tab (system_health parity, Stage 2b) — a faithful reproduction of the full
/// Dashboard's System Events, covering all nine Dashboard categories. Two lead sub-tabs (Corruption Events
/// + Contention Events) are the SYSTEM-component counter CHARTS — the Dashboard's presentation for those
/// two — rendered from a single sp_server_diagnostics SYSTEM feed with no significance filter (every
/// snapshot plotted over time), wired in <see cref="ViewerServerTab.SystemHealthCharts"/>. The remaining
/// sub-tabs are parse-on-read GRIDS, one per unique warning category (plus Darling's extra Significant
/// Waits): <see cref="ViewerDataService"/> fetches the raw event_xml for the category over the toolbar's
/// settable window, the Common <c>SystemHealthParser</c> shreds it, and <c>SystemEventSignificance</c>
/// keeps the sp_HealthParser-significant rows. Mirrors the FinOps tab's inner sub-tab-strip + DataGrid
/// conventions (visible-only load, shared column filters, per-sub-tab count indicator + refresh).
/// Timestamps render machine-local like the deadlock / blocked-process grids (the event's naive-UTC XE
/// @timestamp via <see cref="ViewerTimeHelper.ForDisplay"/>).
/// </summary>
public partial class ViewerServerTab
{
    /* System Events sub-tab order (matches the XAML TabItem order). Corruption + Contention lead — the
       two SYSTEM-component counter-chart tabs, mirroring the Dashboard's System Events order (its
       Corruption Events tab is the default). Both read the one SYSTEM-component feed and select their own
       columns, so they share a single load (LoadSystemHealthChartsAsync). The rest keep Darling's existing
       relative order. */
    private const int SystemEventsCorruptionSubTabIndex = 0;
    private const int SystemEventsContentionSubTabIndex = 1;
    private const int SystemEventsSchedulerIssuesSubTabIndex = 2;
    private const int SystemEventsSevereErrorsSubTabIndex = 3;
    private const int SystemEventsMemoryConditionsSubTabIndex = 4;
    private const int SystemEventsMemoryBrokerSubTabIndex = 5;
    private const int SystemEventsMemoryNodeOomSubTabIndex = 6;
    private const int SystemEventsSignificantWaitsSubTabIndex = 7;
    private const int SystemEventsCpuTasksSubTabIndex = 8;
    private const int SystemEventsIoIssuesSubTabIndex = 9;
    private const int SystemEventsDefaultTraceSubTabIndex = 10;

    private DataGridFilterManager<SchedulerIssueRow>? _seSchedulerFilterMgr;
    private DataGridFilterManager<SevereErrorRow>? _seSevereErrorFilterMgr;
    private DataGridFilterManager<MemoryConditionsRow>? _seMemoryConditionsFilterMgr;
    private DataGridFilterManager<MemoryBrokerRow>? _seMemoryBrokerFilterMgr;
    private DataGridFilterManager<MemoryNodeOomRow>? _seMemoryNodeOomFilterMgr;
    private DataGridFilterManager<SignificantWaitRow>? _seSignificantWaitsFilterMgr;
    private DataGridFilterManager<CpuTasksRow>? _seCpuTasksFilterMgr;
    private DataGridFilterManager<IoIssuesRow>? _seIoIssuesFilterMgr;
    private DataGridFilterManager<DefaultTraceEventRow>? _seDefaultTraceFilterMgr;

    /// <summary>
    /// Registers the eight System Events grids' column-filter managers into the shared
    /// <c>_filterManagers</c> map (defined in <c>ViewerServerTab.Filters.cs</c>). Called from the
    /// constructor after InitializeComponent, alongside the other Initialize* hooks. The Corruption /
    /// Contention chart sub-tabs have no grids, so they register no filter manager here.
    /// </summary>
    private void InitializeSystemEventsTab()
    {
        _seSchedulerFilterMgr = new DataGridFilterManager<SchedulerIssueRow>(SchedulerIssuesGrid);
        _seSevereErrorFilterMgr = new DataGridFilterManager<SevereErrorRow>(SevereErrorsGrid);
        _seMemoryConditionsFilterMgr = new DataGridFilterManager<MemoryConditionsRow>(MemoryConditionsGrid);
        _seMemoryBrokerFilterMgr = new DataGridFilterManager<MemoryBrokerRow>(MemoryBrokerGrid);
        _seMemoryNodeOomFilterMgr = new DataGridFilterManager<MemoryNodeOomRow>(MemoryNodeOomGrid);
        _seSignificantWaitsFilterMgr = new DataGridFilterManager<SignificantWaitRow>(SignificantWaitsGrid);
        _seCpuTasksFilterMgr = new DataGridFilterManager<CpuTasksRow>(CpuTasksGrid);
        _seIoIssuesFilterMgr = new DataGridFilterManager<IoIssuesRow>(IoIssuesGrid);
        _seDefaultTraceFilterMgr = new DataGridFilterManager<DefaultTraceEventRow>(DefaultTraceGrid);

        _filterManagers[SchedulerIssuesGrid] = _seSchedulerFilterMgr;
        _filterManagers[SevereErrorsGrid] = _seSevereErrorFilterMgr;
        _filterManagers[MemoryConditionsGrid] = _seMemoryConditionsFilterMgr;
        _filterManagers[MemoryBrokerGrid] = _seMemoryBrokerFilterMgr;
        _filterManagers[MemoryNodeOomGrid] = _seMemoryNodeOomFilterMgr;
        _filterManagers[SignificantWaitsGrid] = _seSignificantWaitsFilterMgr;
        _filterManagers[CpuTasksGrid] = _seCpuTasksFilterMgr;
        _filterManagers[IoIssuesGrid] = _seIoIssuesFilterMgr;
        _filterManagers[DefaultTraceGrid] = _seDefaultTraceFilterMgr;
    }

    /// <summary>
    /// A System Events sub-tab switch reloads through the shell's overlap-guarded
    /// <see cref="RefreshActiveInnerTabAsync"/> (mirrors the FinOps/Queries sub-tab handlers). Gated on
    /// <see cref="FrameworkElement.IsLoaded"/> and the sub-TabControl's own selection so build-time and
    /// bubbled child selections are ignored.
    /// </summary>
    private async void SystemEventsSubTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, SystemEventsSubTabControl) || !IsLoaded)
        {
            return;
        }

        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Loads the System Events tab's ACTIVE sub-tab only (Lite's visible-only rule). The shell's
    /// <see cref="LoadInnerTabAsync"/> owns the try/catch that surfaces failures on the status bar.
    /// </summary>
    private async Task LoadSystemEventsAsync()
    {
        switch (SystemEventsSubTabControl.SelectedIndex)
        {
            case SystemEventsSchedulerIssuesSubTabIndex:
                await LoadSchedulerIssuesAsync();
                break;
            case SystemEventsSevereErrorsSubTabIndex:
                await LoadSevereErrorsAsync();
                break;
            case SystemEventsMemoryConditionsSubTabIndex:
                await LoadMemoryConditionsAsync();
                break;
            case SystemEventsMemoryBrokerSubTabIndex:
                await LoadMemoryBrokerAsync();
                break;
            case SystemEventsMemoryNodeOomSubTabIndex:
                await LoadMemoryNodeOomAsync();
                break;
            case SystemEventsSignificantWaitsSubTabIndex:
                await LoadSignificantWaitsAsync();
                break;
            case SystemEventsCpuTasksSubTabIndex:
                await LoadCpuTasksAsync();
                break;
            case SystemEventsIoIssuesSubTabIndex:
                await LoadIoIssuesAsync();
                break;
            case SystemEventsDefaultTraceSubTabIndex:
                await LoadDefaultTraceEventsAsync();
                break;
            // Corruption Events (0) and Contention Events (1) share the one SYSTEM-component feed, so both
            // render from a single load — mirroring the Dashboard's case 0/case 1 → RefreshSystemHealthAsync.
            // This is also the default, so the first-shown sub-tab (Corruption Events) loads on activation.
            case SystemEventsCorruptionSubTabIndex:
            case SystemEventsContentionSubTabIndex:
            default:
                /* #4966: the two chart sub-tabs plot a time axis that already shows the empty span, so they take the grids' banner down. */
                SystemEventsTruncationBanner.Visibility = Visibility.Collapsed;
                await LoadSystemHealthChartsAsync();
                break;
        }
    }

    private async Task LoadSchedulerIssuesAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetSchedulerIssuesAsync(_server.ServerId, startUtc, endUtc);
        _seSchedulerFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(SchedulerIssuesNoDataMessage, data.Count);
        SchedulerIssuesCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadSevereErrorsAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetSevereErrorsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _seSevereErrorFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(SevereErrorsNoDataMessage, data.Count);
        SevereErrorsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadMemoryConditionsAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetMemoryConditionsAsync(_server.ServerId, startUtc, endUtc);
        _seMemoryConditionsFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(MemoryConditionsNoDataMessage, data.Count);
        MemoryConditionsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadMemoryBrokerAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetMemoryBrokerAsync(_server.ServerId, startUtc, endUtc);
        _seMemoryBrokerFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(MemoryBrokerNoDataMessage, data.Count);
        MemoryBrokerCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadMemoryNodeOomAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetMemoryNodeOomAsync(_server.ServerId, startUtc, endUtc);
        _seMemoryNodeOomFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(MemoryNodeOomNoDataMessage, data.Count);
        MemoryNodeOomCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadSignificantWaitsAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetSignificantWaitsAsync(_server.ServerId, startUtc, endUtc);
        _seSignificantWaitsFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(SignificantWaitsNoDataMessage, data.Count);
        SignificantWaitsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadCpuTasksAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetCpuTasksAsync(_server.ServerId, startUtc, endUtc);
        _seCpuTasksFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(CpuTasksNoDataMessage, data.Count);
        CpuTasksCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadIoIssuesAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetSystemHealthEventsDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetIoIssuesAsync(_server.ServerId, startUtc, endUtc);
        _seIoIssuesFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "System Events", startUtc, data.Select(r => r.EventTime));
        ShowSystemHealthEmptyState(IoIssuesNoDataMessage, data.Count);
        IoIssuesCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async Task LoadDefaultTraceEventsAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = _dataService.GetDefaultTraceDataStartAsync(_server.ServerId, startUtc, endUtc);
        var data = await _dataService.GetDefaultTraceEventsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _seDefaultTraceFilterMgr!.UpdateData(data);
        await ShowEventDataStartAsync(SystemEventsTruncationBanner, dataStartTask, "Default Trace", startUtc, data.Select(r => r.EventTimeUtc));
        if (DefaultTraceGapNote(_server.ServerName, _server.EngineEdition, _server.EngineKind) is { } gap)
            DefaultTraceNoDataMessage.Text = gap;
        DefaultTraceNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        DefaultTraceCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    /// <summary>
    /// The Default Trace grid's note where the default_trace_events collector cannot run (Azure SQL Database has no
    /// default trace): the same sentence the PostgreSQL panels' <see cref="PanelNote"/> and the MCP tools'
    /// <c>not_collected</c> answer give. Null anywhere else, where the grid keeps its "no events in this window" text.
    /// </summary>
    internal static string? DefaultTraceGapNote(string serverName, int engineEdition, string? engineKind) =>
        CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind, "default_trace_events");

    /// <summary>
    /// The note every sub-tab the system_health session feeds shows where the system_health_events collector cannot run
    /// (an Azure SQL Database): the eight grids and the two chart sub-tabs. The same sentence as
    /// <see cref="DefaultTraceGapNote"/>'s, for this collector. Null anywhere else, where each grid keeps its "no events
    /// in this window" text and the charts show.
    /// </summary>
    internal static string? SystemHealthGapNote(string serverName, int engineEdition, string? engineKind) =>
        CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind, "system_health_events");

    /// <summary>A system_health grid's empty state: shown when the window has no rows. Where the collector cannot run it
    /// says so, in place of the grid's "no events in this window" text.</summary>
    private void ShowSystemHealthEmptyState(TextBlock message, int rowCount)
    {
        if (SystemHealthGapNote(_server.ServerName, _server.EngineEdition, _server.EngineKind) is { } gap)
            message.Text = gap;
        message.Visibility = rowCount == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>
    /// The per-sub-tab Refresh button reloads the active System Events sub-tab through the same status-bar
    /// error surfacing the shell's <see cref="LoadInnerTabAsync"/> uses (mirrors FinOps' RunFinOpsLoad).
    /// </summary>
    private async void SystemEventsRefresh_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            await LoadSystemEventsAsync();
            StatusChanged?.Invoke($"{_server.DisplayName} — refreshed {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"refresh failed: {ex.Message}");
        }
    }
}
