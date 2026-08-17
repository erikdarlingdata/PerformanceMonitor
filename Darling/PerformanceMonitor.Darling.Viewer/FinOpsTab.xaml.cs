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
using System.Windows.Media;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The FinOps top-level aggregate surface for the Darling viewer's shell (a 4th <c>MainTabs</c> tab beside
/// Overview / Recommendations / Alerts). This restores Lite's original cross-server FinOps shape: a top-level
/// tab with its OWN server selector (independent of the sidebar, mirroring the Recommendations tab) rather than
/// the earlier per-server inner tab that duplicated the fleet-wide Server Inventory inside every server's tab.
/// The per-server sub-tabs scope to the selector's selected <see cref="DarlingServer"/> (which carries
/// <c>MonthlyCostUsd</c>, so cost attribution is preserved); Server Inventory stays cross-server and now lives
/// ONCE here. This partial owns the shell — the data service, the server selector, the overlap-guarded
/// active-sub-tab refresh, the column-filter popup plumbing, and the copy/export handlers — mirroring the
/// self-contained <see cref="AlertsHistoryTab"/>. The sub-tab loaders live in <c>FinOpsTab.Loaders.cs</c>,
/// Locking in <c>FinOpsTab.Locking.cs</c>, the Storage Growth drill in <c>FinOpsTab.ObjectHeatmap.cs</c>,
/// Index Analysis in <c>FinOpsTab.IndexAnalysis.cs</c>, and Recommendations in
/// <c>FinOpsTab.Recommendations.cs</c>.
/// </summary>
public partial class FinOpsTab : UserControl
{
    /// <summary>Set once via <see cref="Initialize"/>; every load runs after that, so the loaders read it non-null.</summary>
    private ViewerDataService _dataService = null!;

    /// <summary>Suppresses the server selector's SelectionChanged during <see cref="SetServers"/> population.</summary>
    private bool _populatingServers;

    private bool _refreshInFlight;
    private bool _refreshRequested;

    /// <summary>Raised after a load (or on failure) so the shell can surface progress/errors in its status bar.</summary>
    public event Action<string>? StatusChanged;

    /// <summary>
    /// Raised when a FinOps grid's "View Plan" is clicked (planXml, label, queryText). This aggregate tab has
    /// no per-server plan host, so the shell routes it into the standalone Plan Viewer surface.
    /// </summary>
    public event Action<string, string, string?>? PlanRequested;

    public FinOpsTab()
    {
        InitializeComponent();

        /* Register the FinOps grids' column-filter managers into _filterManagers (defined below), after
           InitializeComponent so the named grids exist. Body lives in FinOpsTab.Loaders.cs. */
        InitializeFinOpsTab();
    }

    /// <summary>The selector's currently-selected server. Entry points guard on a valid selection before any loader runs, so the loaders read this non-null.</summary>
    private DarlingServer _server => (DarlingServer)ServerSelector.SelectedItem!;

    /// <summary>Wires the data service. Call once, before the tab is first shown.</summary>
    public void Initialize(ViewerDataService dataService) => _dataService = dataService;

    /// <summary>
    /// Populates the server selector from the shell's server list (mirrors the Recommendations tab's own
    /// selector). Suppresses SelectionChanged during population, preserving the current selection when the list
    /// is re-supplied. The shell drives the first load once the tab becomes visible.
    /// </summary>
    public void SetServers(IReadOnlyList<DarlingServer> servers)
    {
        var previousId = (ServerSelector.SelectedItem as DarlingServer)?.ServerId;

        _populatingServers = true;
        ServerSelector.ItemsSource = servers;
        if (servers.Count > 0)
        {
            var match = previousId is int pid ? servers.FirstOrDefault(s => s.ServerId == pid) : null;
            ServerSelector.SelectedItem = match ?? servers[0];
        }
        _populatingServers = false;
    }

    /// <summary>
    /// Syncs the server selector to the shell's sidebar selection WITHOUT triggering a load — it suppresses
    /// SelectionChanged like <see cref="SetServers"/>, so the tab reloads with the synced server on its next
    /// activation / refresh rather than loading FinOps data while it isn't the visible tab. Keeps this tab's
    /// picker in step with the sidebar (the selector remains independently changeable while FinOps is open).
    /// No-op when the id isn't in the list or is already selected.
    /// </summary>
    public void SelectServer(int serverId)
    {
        if (ServerSelector.ItemsSource is not IEnumerable<DarlingServer> servers)
        {
            return;
        }

        var match = servers.FirstOrDefault(s => s.ServerId == serverId);
        if (match is null || ReferenceEquals(ServerSelector.SelectedItem, match))
        {
            return;
        }

        _populatingServers = true;
        ServerSelector.SelectedItem = match;
        _populatingServers = false;
    }

    /// <summary>The tab's own server selector drives it; single-clicking a sidebar server syncs it here (and
    /// on Recommendations) via the shell, and it stays independently changeable. A new server resets any open
    /// drill, then reloads the active sub-tab.</summary>
    private async void ServerSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_populatingServers)
        {
            return;
        }

        /* A new server invalidates any open Storage Growth / Locking drill (their breadcrumbs + detail views
           belong to the previous server), so reset both to their parent view before reloading. */
        ShowFinOpsStorageView(FinOpsStorageDrillLevel.Parent);
        ShowFinOpsLockingView(FinOpsLockingLevel.Parent);

        /* #2306: column filters belong to the previous server too. A DatabaseName filter set against
           server A silently zeroes server B's grid while the count indicators — computed from the
           unfiltered list — stay full, and Refresh cannot clear it (UpdateData deliberately re-applies
           active filters, which is right for same-server refresh and untouched here). Cleared via the
           map every FinOps manager registers into, so a new grid inherits this without a second edit. */
        foreach (var manager in _filterManagers.Values)
        {
            manager.ClearFilters();
        }

        await RefreshActiveSubTabAsync();
    }

    private async void FinOpsRefresh_Click(object sender, RoutedEventArgs e) => await RefreshActiveSubTabAsync();

    /// <summary>
    /// Loads the ACTIVE FinOps sub-tab, with the overlap guard mirroring the per-server tab's
    /// <c>RefreshActiveInnerTabAsync</c>. The shell calls this on tab activation and on its fleet-refresh timer;
    /// the sub-tab switch handler and the server selector call it too. If the sub-tab (or server) switches
    /// mid-load, the triggering event bounces off the guard and the running loop reloads once more, leaving no
    /// sub-tab stranded.
    /// </summary>
    public async Task RefreshActiveSubTabAsync()
    {
        if (_dataService is null || ServerSelector.SelectedItem is not DarlingServer)
        {
            return;
        }

        if (_refreshInFlight)
        {
            _refreshRequested = true;
            return;
        }

        _refreshInFlight = true;
        try
        {
            do
            {
                _refreshRequested = false;

                int loadedTab;
                do
                {
                    loadedTab = FinOpsSubTabControl.SelectedIndex;
                    await LoadActiveSubTabGuardedAsync();
                }
                while (FinOpsSubTabControl.SelectedIndex != loadedTab);
            }
            while (_refreshRequested);
        }
        finally
        {
            _refreshInFlight = false;
        }
    }

    /// <summary>Runs the active sub-tab's loader (FinOpsTab.Loaders.cs) with the status-bar error surfacing the per-server tab's LoadInnerTabAsync uses.</summary>
    private async Task LoadActiveSubTabGuardedAsync()
    {
        try
        {
            await LoadFinOpsAsync();
            StatusChanged?.Invoke($"{_server.DisplayName} — refreshed {DateTime.Now:HH:mm:ss}");
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"refresh failed: {ex.Message}");
        }
    }

    // ── Copy / export (copied from ViewerServerTab.CopyExport.cs; the FinOps context menus bind to these) ──

    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);

    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);

    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);

    private void ExportToCsv_Click(object sender, RoutedEventArgs e) =>
        DataGridExport.ExportToCsv(sender, _server.DisplayName, ViewerExportSettings.CsvSeparator);

    // ── Column filtering (copied from ViewerServerTab.Filters.cs — the visual-tree-walk variant) ──

    /* Grid -> its filter manager, keyed for the FilterButton_Click visual-tree walk. Populated by
       InitializeFinOpsTab (FinOpsTab.Loaders.cs). */
    private readonly Dictionary<DataGrid, IDataGridFilterManager> _filterManagers = new();

    /* Host/apply plumbing lives in the shared Ui controller. Lazy (a field initializer can't reference the
       instance field _filterManagers); the XAML-wired FilterButton_Click forwards to it. */
    private ColumnFilterPopupController? _filterPopupControllerField;
    private ColumnFilterPopupController FilterPopupController => _filterPopupControllerField ??= new ColumnFilterPopupController(_filterManagers);

    private void FilterButton_Click(object sender, RoutedEventArgs e) => FilterPopupController.HandleFilterButtonClick(sender);
}
