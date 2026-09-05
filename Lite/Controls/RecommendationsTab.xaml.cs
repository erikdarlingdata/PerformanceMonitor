/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Analysis.Recommendations;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The Lite Recommendations surface (WS1c). Renders the advise-only
/// <see cref="LiteRecommendationItem"/> list from <see cref="LiteRecommendationsReader"/> as a
/// card list grouped into collapsible Critical / Warning / Info sections — mirroring the
/// Dashboard's Recommendations tab UX, but ADVISE-ONLY.
///
/// <para>
/// Lite is local-DuckDB and SQL-side remediation is Dashboard-only (per project scope), so there
/// is NO Apply button and NO privileged execution path here. Each card offers "Ask AI" (copies an
/// MCP investigation prompt); cards whose finding carries a derivable copy-paste statement also
/// offer "Copy fix". The Dashboard's "Open in Active Queries" deep-link is omitted: the Lite
/// surface is a shared tab with a server selector, not a per-server tab that owns an Active Queries
/// view.
/// </para>
///
/// <para>
/// "Refresh" re-reads the latest persisted finding batch (advise-only — Lite does not persist the
/// drill-down detail the SQL generators need, so copy-paste SQL is usually absent on this path).
/// "Generate now" runs an on-demand analysis and maps the freshly-enriched in-memory findings
/// directly, so copy-paste SQL IS populated for that pass.
/// </para>
/// </summary>
public partial class RecommendationsTab : UserControl
{
    private DuckDbInitializer? _duckDb;
    private ScheduleManager? _scheduleManager;
    private ServerManager? _serverManager;
    private FindingStore? _findingStore;
    private LiteRecommendationsReader? _reader;

    private int _hoursBack = 24;
    private bool _isBusy;

    /* #2933: the one site in Lite that a generation token cannot help. _isBusy makes two loads of this
       card list mutually exclusive, so there is no interleave to order — what goes wrong here is the
       opposite: the operator switches server during a Generate now, ServerSelector_SelectionChanged's
       RefreshDataAsync bounces off _isBusy and is DROPPED, and the generate then paints the previous
       server's advice under a selector naming the new one, with nothing left to correct it. A drop-based
       mechanism can only make that worse. So this flag is the Darling viewer's replay companion
       (RefreshVisibleAsync's _refreshRequested, ViewerServerTab.RefreshActiveInnerTabAsync's): a request
       that arrives mid-load is remembered instead of discarded, and the holder re-runs once it is free,
       re-reading the selected server at the re-entered load's own entry. */
    private bool _reloadRequested;

    public RecommendationsTab()
    {
        InitializeComponent();
    }

    /// <summary>
    /// Initializes the control with required dependencies. <paramref name="duckDb"/> is the shared
    /// DuckDB handle (for the finding-store read + Generate now); <paramref name="serverManager"/>
    /// provides the server list and connection strings (Generate now). Mirrors the FinOps/Alerts
    /// tabs' Initialize-from-MainWindow contract.
    /// </summary>
    /// <param name="scheduleManager">#1757: lets the baseline provider warn when a source table is retained
    /// for less than the 30-day baseline window. Optional — null just disables that warning.</param>
    public void Initialize(DuckDbInitializer duckDb, ServerManager serverManager, ScheduleManager? scheduleManager = null)
    {
        _duckDb = duckDb ?? throw new ArgumentNullException(nameof(duckDb));
        _scheduleManager = scheduleManager;
        _serverManager = serverManager ?? throw new ArgumentNullException(nameof(serverManager));

        _findingStore = new FindingStore(_duckDb);
        _reader = new LiteRecommendationsReader(_findingStore);

        PopulateServerSelector();
        _ = RefreshDataAsync();
    }

    /// <summary>
    /// Refreshes the server dropdown from the current server list. Called when servers are
    /// added or removed (mirrors <c>FinOpsTab.RefreshServerList</c>).
    /// </summary>
    public void RefreshServerList()
    {
        if (_serverManager == null) return;

        var previousSelection = ServerSelector.SelectedItem as ServerConnection;
        var servers = _serverManager.GetAllServers();
        ServerSelector.ItemsSource = servers;

        if (previousSelection != null)
        {
            var match = servers.FirstOrDefault(s => s.Id == previousSelection.Id);
            if (match != null)
            {
                ServerSelector.SelectedItem = match;
                return;
            }
        }

        if (servers.Count > 0)
            ServerSelector.SelectedIndex = 0;
    }

    private void PopulateServerSelector()
    {
        if (_serverManager == null) return;

        var servers = _serverManager.GetAllServers();
        ServerSelector.ItemsSource = servers;
        if (servers.Count > 0)
            ServerSelector.SelectedIndex = 0;
    }

    private int GetSelectedServerId()
    {
        if (ServerSelector.SelectedItem is ServerConnection server)
            return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        return 0;
    }

    /// <summary>
    /// Re-reads recommendations for the selected server and re-renders. Read-only: surfaces the
    /// Loaded or all-clear Empty state. The insufficient-data state is surfaced by
    /// <see cref="GenerateNowButton_Click"/> (the engine owns that determination), not by this path.
    /// </summary>
    public async Task RefreshDataAsync()
    {
        if (_reader is null)
            return;

        if (_isBusy)
        {
            /* Remembered, not dropped: whoever holds the flag re-runs below and re-reads the selector. */
            _reloadRequested = true;
            return;
        }

        _isBusy = true;
        try
        {
            /* Re-read the selector on every pass, so a request that arrived mid-load is answered for the
               server selected NOW rather than the one selected when this call started. */
            do
            {
                _reloadRequested = false;

                if (ServerSelector.SelectedItem is not ServerConnection server)
                {
                    ApplyViewModel(LiteRecommendationsViewModel.FromItems(Array.Empty<LiteRecommendationItem>()));
                    continue;
                }

                ApplyViewModel(LiteRecommendationsViewModel.Loading());

                var serverId = GetSelectedServerId();
                var serverName = server.DisplayNameWithIntent;

                var items = await Task.Run(() => _reader.GetRecommendationsAsync(serverId, serverName, _hoursBack));

                ApplyViewModel(LiteRecommendationsViewModel.FromItems(items, ServerTimeHelper.UtcOffsetMinutes));
            }
            while (_reloadRequested);
        }
        catch (Exception ex)
        {
            AppLogger.Error("Recommendations", $"Failed to load recommendations: {ex.Message}");
            // Fall back to the empty state rather than leaving the spinner up.
            ApplyViewModel(LiteRecommendationsViewModel.FromItems(Array.Empty<LiteRecommendationItem>()));
        }
        finally
        {
            _isBusy = false;
        }
    }

    /// <summary>
    /// Runs an on-demand analysis for the selected server (same construction path the background
    /// collector uses), then renders the freshly-enriched in-memory findings directly — which, unlike
    /// the stored-finding read path, carry drill-down detail, so copy-paste SQL is populated. If the
    /// engine reports insufficient collected history, surfaces the insufficient-data state.
    /// </summary>
    private async void GenerateNowButton_Click(object sender, RoutedEventArgs e)
    {
        if (_duckDb is null || _serverManager is null)
            return;

        if (_isBusy)
        {
            _reloadRequested = true;
            return;
        }

        if (ServerSelector.SelectedItem is not ServerConnection server)
            return;

        _isBusy = true;
        try
        {
            ApplyViewModel(LiteRecommendationsViewModel.Loading());
            StatusText.Text = "Generating…";

            var serverId = GetSelectedServerId();
            var serverName = server.DisplayNameWithIntent;

            // Fresh per-run AnalysisService (IsAnalyzing is per-instance), mirroring the collector.
            // AnalyzeAsync persists findings AND returns them enriched with drill-down detail.
            var planFetcher = new SqlPlanFetcher(_serverManager);
            /* #1757: the retention accessor reads the DEFAULT schedule — baselines are a per-server
               statistic but retention is configured per collector, and the default is what a user changes
               when they shrink history for disk reasons. */
            var analysisService = new AnalysisService(
                _duckDb,
                planFetcher,
                _scheduleManager is null
                    ? null
                    : (Func<string, int?>)(collector =>
                        _scheduleManager.GetDefaultSchedule()
                            .FirstOrDefault(x => string.Equals(x.Name, collector, StringComparison.OrdinalIgnoreCase))
                            ?.RetentionDays));

            var findings = await Task.Run(() => analysisService.AnalyzeAsync(serverId, serverName, hoursBack: 4));

            if (analysisService.InsufficientDataMessage is { Length: > 0 } message)
            {
                StatusText.Text = string.Empty;
                ApplyViewModel(LiteRecommendationsViewModel.InsufficientData(message));
                return;
            }

            StatusText.Text = string.Empty;
            var items = LiteRecommendationsReader.MapFindings(findings, serverName);
            ApplyViewModel(LiteRecommendationsViewModel.FromItems(items, ServerTimeHelper.UtcOffsetMinutes));
        }
        catch (Exception ex)
        {
            AppLogger.Error("Recommendations", $"Failed to generate recommendations: {ex.Message}");
            StatusText.Text = "Generate failed — see log.";
            ApplyViewModel(LiteRecommendationsViewModel.FromItems(Array.Empty<LiteRecommendationItem>()));
        }
        finally
        {
            _isBusy = false;

            /* A server switch during the generate bounced off _isBusy above. Honour it here rather than
               leave the freshly-generated advice for the PREVIOUS server sitting under the new server's
               name. In the finally and not below the try, because the insufficient-data branch returns
               from inside it and would skip a trailing block. The generate itself is one-shot and is
               never replayed; only the read that was refused is. */
            if (_reloadRequested)
            {
                await RefreshDataAsync();
            }
        }
    }

    private async void RefreshButton_Click(object sender, RoutedEventArgs e)
    {
        StatusText.Text = string.Empty;
        await RefreshDataAsync();
    }

    private async void ServerSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        StatusText.Text = string.Empty;
        await RefreshDataAsync();
    }

    /// <summary>
    /// Copies a card's suggested T-SQL to the clipboard (advise-only). SetDataObject with
    /// copy=false avoids WPF's problematic Clipboard.Flush() (matches the Lite convention).
    /// </summary>
    private void CopyFix_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not LiteRecommendationCardViewModel card)
            return;

        if (string.IsNullOrEmpty(card.CopyPasteSql))
            return;

        Clipboard.SetDataObject(card.CopyPasteSql, false);
        StatusText.Text = "Fix copied to clipboard.";
    }

    /// <summary>
    /// Copies the MCP investigation prompt for a card to the clipboard (no dialog; a brief status
    /// confirmation). Lite exposes the same MCP analysis tools the prompt references.
    /// </summary>
    private void AskAi_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not LiteRecommendationCardViewModel card)
            return;

        Clipboard.SetDataObject(card.AskAiPrompt, false);
        StatusText.Text = "AI prompt copied to clipboard.";
    }

    /// <summary>
    /// Swaps the visible region to match the view-model's state and binds the sections. Mirrors the
    /// Dashboard control's ApplyViewModel.
    /// </summary>
    private void ApplyViewModel(LiteRecommendationsViewModel vm)
    {
        switch (vm.State)
        {
            case LiteRecommendationsState.Loading:
                LoadingOverlay.IsLoading = true;
                SectionsScroll.Visibility = Visibility.Collapsed;
                EmptyMessage.Visibility = Visibility.Collapsed;
                InsufficientDataMessage.Visibility = Visibility.Collapsed;
                break;

            case LiteRecommendationsState.InsufficientData:
                LoadingOverlay.IsLoading = false;
                SectionsScroll.Visibility = Visibility.Collapsed;
                EmptyMessage.Visibility = Visibility.Collapsed;
                InsufficientDataMessage.Text = vm.InsufficientDataMessage;
                InsufficientDataMessage.Visibility = Visibility.Visible;
                break;

            case LiteRecommendationsState.Empty:
                LoadingOverlay.IsLoading = false;
                SectionsList.ItemsSource = null;
                SectionsScroll.Visibility = Visibility.Collapsed;
                InsufficientDataMessage.Visibility = Visibility.Collapsed;
                EmptyMessage.Visibility = Visibility.Visible;
                break;

            case LiteRecommendationsState.Loaded:
            default:
                LoadingOverlay.IsLoading = false;
                EmptyMessage.Visibility = Visibility.Collapsed;
                InsufficientDataMessage.Visibility = Visibility.Collapsed;
                SectionsList.ItemsSource = vm.Sections;
                SectionsScroll.Visibility = Visibility.Visible;
                break;
        }
    }
}
