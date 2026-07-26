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
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Darling viewer shell (headless plan M3 + viewer waves 2-4, W0 IA inversion): the server list
/// from the central store on the left, and — mirroring Lite's navigation shape — ONE top tab strip on
/// the right holding the fixed aggregate tabs (Overview and the all-servers Alert History span every
/// server; Recommendations and FinOps are server-scoped, their own pickers synced to the sidebar
/// selection) plus dynamically-added, closable per-server tabs. Single-clicking a server drives the
/// server-scoped aggregate tabs to it; double-clicking opens (or focuses) that server's
/// <see cref="ViewerServerTab"/>,
/// whose inner tabs (Overview charts, Queries, Blocking, Collection Health) hold the per-server surfaces.
/// All reads go straight to Postgres via <see cref="ViewerDataService"/>. Loads are lazy per visible
/// tab (Lite's visible-only rule): the fleet-refresh timer (the configurable
/// <see cref="ViewerAppSettings.NocRefreshIntervalSeconds"/>) refreshes only the visible tab — an aggregate
/// tab for the selected server, or the visible server tab's active inner tab. Every data load is async.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable",
    Justification = "WPF windows are never disposed by the framework; the data service is disposed in OnClosed.")]
public partial class MainWindow : Window
{
    /// <summary>
    /// The fleet auto-refresh cadence for the Overview and the aggregate tabs — the single operator knob
    /// (<see cref="ViewerAppSettings.NocRefreshIntervalSeconds"/>) that replaced the two hardcoded cadences
    /// (60s aggregate / 30s Overview), mirroring the Dashboard's one NocRefreshIntervalSeconds. Both fleet-scope
    /// timers run at this interval so an operator can dial the store-query load back at 500-server scale. Seeded
    /// in the constructor; re-applied to the live timers when the Settings window closes.
    /// </summary>
    private TimeSpan _refreshInterval;

    /// <summary>The viewer's connect-timeout preference, applied to the store connection string when the data
    /// service is created (see <see cref="ViewerDataService(string, int?)"/>). Seeded in the constructor.</summary>
    private readonly int _connectionTimeoutSeconds;

    private ViewerDataService? _dataService;
    private DispatcherTimer? _refreshTimer;
    private DispatcherTimer? _overviewTimer;
    private bool _refreshInFlight;
    private bool _refreshRequested;

    /// <summary>Suppresses the Recommendations server combo's SelectionChanged during initial population.</summary>
    private bool _populatingRecoServers;

    /// <summary>
    /// Open per-server tabs keyed by server id, so a double-click on an already-open server focuses its
    /// existing tab instead of opening a duplicate (Lite's dedupe-by-id rule).
    /// </summary>
    private readonly OpenServerTabRegistry<TabItem> _openServerTabs = new();

    /// <summary>The fleet: the authoritative full server set plus the filtered subset the sidebar shows.
    /// Consumers asking "what servers exist?" must read <c>_fleet.All</c>, never the bound list — see
    /// <see cref="FleetView"/> for why reading ItemsSource back is the bug this replaces.</summary>
    private readonly FleetView _fleet = new();

    /// <summary>
    /// The viewer's per-user preferences store (a small JSON file in %APPDATA%) and the current in-memory
    /// copy. These are the persisted DEFAULTS that seed each newly-opened <see cref="ViewerServerTab"/>'s
    /// toolbar (time window + auto-refresh); the Settings dialog edits them. Loaded once on startup.
    /// </summary>
    private readonly ViewerPreferencesStore _preferencesStore = new();
    private ViewerPreferences _preferences;

    /// <summary>
    /// The viewer's per-user application-settings store (the Darling equivalent of Lite's settings.json):
    /// the MCP toggle, alert thresholds + toggles, notification options, SMTP/webhook config, and the
    /// automated-analysis knobs the full Settings window edits. Held here so the Settings dialog loads from
    /// and saves to one shared instance.
    /// </summary>
    private readonly ViewerAppSettingsStore _appSettingsStore = new();

    /// <summary>
    /// The system-tray icon + minimize-to-tray behavior (ported from Lite's SystemTrayService, adapted for
    /// the headless viewer). Null until <see cref="OnLoaded"/> initializes it once the store connects.
    /// </summary>
    private SystemTrayService? _trayService;

    /// <summary>
    /// Turns polled alert-history rows into non-duplicated, cooldown-gated tray toasts — the headless
    /// stand-in for Lite's in-process alert-engine toast. Primed at startup so history isn't replayed.
    /// </summary>
    private AlertToastCoordinator? _toastCoordinator;
    private bool _alertPollInFlight;

    /// <summary>
    /// Restores the window from the tray if a sleep-/lock-driven minimize hid it (Lite #1050). Reachable now
    /// that minimize-to-tray can hide the viewer; paired with the App-startup SoftwareOnly render mode.
    /// </summary>
    private WindowResumeGuard? _resumeGuard;

    /* Live copies of the three ViewerAppSettings values the tray/toast path honors at runtime: seeded in
       the constructor and refreshed when the Settings window closes (mirroring ViewerExportSettings). */
    private bool _minimizeToTray;
    private bool _alertsEnabled;
    private int _alertCooldownMinutes;

    /// <summary>How far back each toast poll reads alert history; the coordinator dedupes the overlap.</summary>
    private static readonly TimeSpan s_alertPollWindow = TimeSpan.FromMinutes(15);

    /// <summary>Seen-row retention (poll window + margin) so a row still inside the window is never pruned early.</summary>
    private static readonly TimeSpan s_alertSeenRetention = TimeSpan.FromMinutes(20);

    /// <summary>The Overview tile sort mode (CPU% descending default / Name): seeded from ViewerAppSettings in
    /// the constructor, persisted back on selector change, and applied by <see cref="ServerOverviewSort.Order"/>
    /// at the fleet bind site so tiles re-sort deterministically every refresh.</summary>
    private ServerOverviewSortMode _overviewSortMode;

    public MainWindow()
    {
        InitializeComponent();
        _preferences = _preferencesStore.Load();
        /* Seed the persisted app settings the viewer honors at runtime BEFORE any tab/chart renders: the CSV
           export separator (grid exports) and the Server/Local/UTC time-display mode (every timestamp render
           routes through ViewerTimeHelper, which reads CurrentDisplayMode). UiTimeContext is deliberately left
           at its identity default — Darling charts pre-convert their X through ForDisplay, so wiring it would
           double-convert on hover/crosshair. */
        var appSettings = _appSettingsStore.Load();
        ViewerExportSettings.Apply(appSettings);
        /* Seed the new-mute-rule default expiration preference so every MuteRuleEditDialog opens on it, and the
           dismiss/mute action-logging opt-in the Alerts tab honors. */
        MuteRuleEditDialog.DefaultExpiration = appSettings.MuteRuleDefaultExpiration;
        AlertsHistoryTab.LogDismissals = appSettings.LogAlertDismissals;
        /* Seed the runtime tray/toast knobs. Minimize-to-tray is genuinely viewer-local (ViewerAppSettings).
           The alerts-master and the "Tray notification cooldown" are STORE-backed (AlertSettingsRow.Enabled /
           .CooldownMinutes — the same fields the Settings window's "Enable notifications" + cooldown drive and
           the service honors); these ViewerAppSettings copies are only a pre-connect fallback, overwritten by
           RefreshAlertToastSettingsFromStoreAsync once the store connects and again on Settings close. */
        _minimizeToTray = appSettings.MinimizeToTray;
        _alertsEnabled = appSettings.AlertsEnabled;
        _alertCooldownMinutes = appSettings.AlertCooldownMinutes;
        /* Seed the connect-timeout applied to the store connection (at ViewerDataService creation, on connect)
           and the single fleet-refresh cadence both shell timers run at. */
        _connectionTimeoutSeconds = appSettings.ConnectionTimeoutSeconds;
        _refreshInterval = TimeSpan.FromSeconds(appSettings.NocRefreshIntervalSeconds);
        ApplyTimeDisplayMode(appSettings.TimeDisplayMode);
        /* Overview tile sort: seed the mode from settings and select the matching header-selector item. There is
           deliberately NO XAML default selection, so this is the only pre-load SelectedIndex assignment; the
           handler's IsLoaded guard suppresses the SelectionChanged it raises here. */
        _overviewSortMode = ServerOverviewSort.ParseMode(appSettings.OverviewSortMode);
        OverviewSortSelector.SelectedIndex = _overviewSortMode == ServerOverviewSortMode.Name ? 1 : 0;
        Loaded += OnLoaded;
        Closed += OnClosed;
    }

    private async void OnLoaded(object sender, RoutedEventArgs e)
    {
        Loaded -= OnLoaded;

        ViewerSettings? settings;
        try
        {
            settings = await Task.Run(() => ViewerSettings.TryLoad(ExplicitConfigPathFromArgs()));
        }
        catch (Exception ex)
        {
            ShowMessage($"darling.json could not be read: {ex.Message}");
            return;
        }

        if (settings is null)
        {
            ShowMessage("darling.json not found — copy darling.sample.json from the service and point DARLING_CONFIG at it.");
            return;
        }

        _dataService = new ViewerDataService(settings.ConnectionString, _connectionTimeoutSeconds);

        try
        {
            /* Finding B3: prove the store is reachable with one real connect, so an unreachable store (the
               service down / wrong postgres section in darling.json) shows a dedicated message instead of
               being misread as a read-only seat (DetectReadOnlyAsync used to collapse both to read-only). */
            await _dataService.EnsureStoreReachableAsync();

            /* Finding B1: gate on schema skew BEFORE any control-plane write can hit a raw Postgres
               42703/42P01 (e.g. Add/Edit Server against a store missing the V18 alert_delivery_mode_override
               column). Probe the store's effective version via information_schema — the darling_schema_version
               table is owner-only — and block if it is behind this build. NEVER migrate from the viewer: that
               is the service's job. The probe fails open (null), so a healthy store is never falsely blocked. */
            var storeVersion = await _dataService.GetStoreSchemaVersionAsync();
            if (storeVersion is int version && version < ViewerDataService.RequiredStoreSchemaVersion)
            {
                ShowMessage(
                    $"The Darling store is at schema v{version}, but this viewer needs v{ViewerDataService.RequiredStoreSchemaVersion}. " +
                    "Update or restart the Darling service so it migrates the store, then reopen the viewer.");
                return;
            }

            /* V8 security hardening: probe whether this connection can write the operator-config tables
               (admin/owner) or is the read-only viewer role, before showing any write affordance. The
               probe fails safe to read-only; the write surfaces gate on ViewerDataService.IsReadOnly and
               the write paths translate a live 42501 into a friendly message as a backstop. */
            await _dataService.DetectReadOnlyAsync();
        }
        catch (ViewerStoreUnreachableException ex)
        {
            ViewerLogger.Error("App", "Darling store unreachable", ex);
            ShowMessage(ex.Message);
            return;
        }
        catch (Exception ex)
        {
            /* Reachable but the first connection failed for another reason (e.g. authentication, or the
               configured database does not exist) — show it rather than dead-ending on a blank window. */
            ViewerLogger.Error("App", "Darling store connection failed", ex);
            ShowMessage($"Couldn't connect to the Darling store: {ex.Message}");
            return;
        }

        /* A read-only seat cannot command the service, so "Generate now" (analyze_now) is disabled. */
        RecommendationsGenerateButton.IsEnabled = !_dataService.IsReadOnly;

        /* The Alerts tab is a self-loading control (all-servers by default); give it the store and let
           it surface load/dismiss/mute outcomes on the shared status bar. */
        AlertsHistoryContent.Initialize(_dataService);
        AlertsHistoryContent.StatusChanged += OnServerTabStatusChanged;

        /* The Job History tab is a self-loading all-servers control (retained SQL Agent job-run history);
           give it the store and surface its load outcomes on the shared status bar. */
        JobHistoryContent.Initialize(_dataService);
        JobHistoryContent.StatusChanged += OnServerTabStatusChanged;

        /* Availability Groups (#991): a self-loading fleet-wide control. Its TabItem ships Collapsed and is
           revealed only once a load finds AG rows, so an Always-On-less fleet never carries a dead tab. */
        AvailabilityGroupsContent.Initialize(_dataService);
        AvailabilityGroupsContent.StatusChanged += OnServerTabStatusChanged;

        /* The FinOps tab is a self-loading cross-server aggregate control with its own server selector; give
           it the store, surface its load/refresh outcomes on the shared status bar, and route its query grids'
           "View Plan" requests into the standalone Plan Viewer surface (it has no per-server plan host). */
        FinOpsContent.Initialize(_dataService);
        FinOpsContent.StatusChanged += OnServerTabStatusChanged;
        FinOpsContent.PlanRequested += OpenStoredPlanInPlanViewer;

        /* One-time import of any pre-Stage-3 viewer-servers.json definitions into the store, before the first
           list load so imported servers appear immediately. Guarded (marker + ON CONFLICT DO NOTHING) so it
           never double-seeds the service's darling.json seed nor resurrects a removed server. */
        await MigrateViewerServersAsync();

        /* One-time import of any pre-Stage-3b viewer-local operational settings (alerts, analysis, SMTP,
           Teams/Slack, MCP) into the control-plane store — only when the store section is still at defaults,
           so an operator-tuned store is never clobbered. Guarded by a marker; read-only/disconnected skip. */
        await MigrateControlPlaneSettingsAsync();

        await LoadServersAsync();

        /* --open-server <name>: deep-link straight into a server's per-server tab on startup —
           the same tab a double-click opens. Case-insensitive on the registered server name;
           an unknown name is ignored (the window still opens normally). */
        var openServer = OpenServerNameFromArgs();
        if (openServer is not null)
        {
            /* Against the whole fleet: a deep link must open a server the current filter is hiding. */
            var match = _fleet.All.FirstOrDefault(s =>
                string.Equals(s.ServerName, openServer, StringComparison.OrdinalIgnoreCase));
            if (match is not null)
            {
                OpenServerTab(match);
            }
        }

        /* System tray + in-app alert toasts (the ported Lite tray, adapted for headless). The tray icon lives
           for the process; restore shares the single-instance SurfaceWindow path so the tray Restore and a
           second-launch surface behave identically. Minimize-to-tray reads the live _minimizeToTray value. */
        _trayService = new SystemTrayService(this, SurfaceWindow, () => _minimizeToTray);
        _trayService.Initialize();

        /* #1050: restore the window from the tray on resume/unlock if a sleep- or lock-driven minimize hid it.
           ??= so a repeated Loaded can't double-subscribe (static SystemEvents). Shares the SurfaceWindow path. */
        _resumeGuard ??= new WindowResumeGuard(this, SurfaceWindow);

        /* Pull the store-authoritative alerts-master + toast cooldown (AlertSettingsRow) so the toast path
           honors the same "Enable notifications" + "Tray notification cooldown" the service does. */
        await RefreshAlertToastSettingsFromStoreAsync();

        /* Prime the toast watermark with the rows already present so startup doesn't replay history as a
           toast storm — only alerts that arrive after this point can toast. Best-effort: a failed prime just
           means the first poll may toast up to one card per condition in the window (bounded by the cooldown). */
        _toastCoordinator = new AlertToastCoordinator(s_alertSeenRetention);
        try
        {
            var existing = await _dataService.GetAlertHistoryAsync(
                DateTime.UtcNow - s_alertPollWindow, serverId: null, limit: 500);
            _toastCoordinator.Prime(existing);

            /* Paint the per-server "needs attention" badges straight away from the same read (they are a
               state indicator, not a nudge, so — unlike the toast prime — startup SHOULD surface them),
               rather than leaving the sidebar/tabs blank until the first refresh tick ~30s later. */
            UpdateServerAttention(existing);
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("AlertToasts", $"Priming the alert-toast watermark failed: {ex.Message}");
        }

        _refreshTimer = new DispatcherTimer { Interval = _refreshInterval };
        _refreshTimer.Tick += OnRefreshTimerTick;
        _refreshTimer.Start();

        /* The Overview keeps its own timer so it refreshes when it is the visible tab and the aggregate timer
           skips it (they never double-refresh the same grid); both run at the one configurable fleet interval. */
        _overviewTimer = new DispatcherTimer { Interval = _refreshInterval };
        _overviewTimer.Tick += OnOverviewTimerTick;
        _overviewTimer.Start();
    }

    /// <summary>
    /// First non-option command-line argument = explicit config path, mirroring the service
    /// (option pairs like --open-server &lt;name&gt; are skipped).
    /// </summary>
    private static string? ExplicitConfigPathFromArgs()
    {
        var args = Environment.GetCommandLineArgs();
        for (var i = 1; i < args.Length; i++)
        {
            if (string.Equals(args[i], "--open-server", StringComparison.OrdinalIgnoreCase))
            {
                i++; /* Skip the option's value too. */
                continue;
            }

            return args[i];
        }

        return null;
    }

    /// <summary>The value following --open-server, or null when absent/dangling.</summary>
    private static string? OpenServerNameFromArgs()
    {
        var args = Environment.GetCommandLineArgs();
        for (var i = 1; i < args.Length - 1; i++)
        {
            if (string.Equals(args[i], "--open-server", StringComparison.OrdinalIgnoreCase))
            {
                return args[i + 1];
            }
        }

        return null;
    }

    private async void OnRefreshTimerTick(object? sender, EventArgs e)
    {
        /* Refresh the sidebar status dots + the status bar every cycle regardless of the visible tab (a
           cheap pair of single-query reads), so freshness stays current even while a per-server tab is up. */
        _ = RefreshServerStatusAsync();
        _ = RefreshStoreSizeAsync();

        /* Poll alert history once, regardless of the visible tab: refresh the per-server "needs attention"
           badges (sidebar + open tabs) and surface genuinely-new rows as tray toasts. */
        _ = PollAlertsAsync();

        /* Probe for Availability Groups while the tab is still hidden, so standing an AG up reveals it without
           a restart. Converge-then-stop: once revealed this does nothing and the tab refreshes through the
           normal visible-tab path, so an AG fleet does not pay for the probe twice. */
        if (AvailabilityGroupsTabItem.Visibility != Visibility.Visible)
        {
            _ = RefreshAvailabilityGroupsAsync();
        }

        /* Two tabs opt out of this timer: Recommendations refreshes on tab-activation only (matching Lite —
           analysis findings change on the service's 30-minute cadence, so an interval auto-refresh is pointless
           churn and would reset the incident expanders' state under the reader), and the Overview has its OWN
           timer, so this one skips it (they'd otherwise double-refresh the same grid). Every other aggregate tab
           still auto-refreshes. */
        if (ReferenceEquals(MainTabs.SelectedItem, RecommendationsTab)
            || ReferenceEquals(MainTabs.SelectedItem, OverviewTab))
        {
            return;
        }

        /* Per-server tabs now self-refresh on their own toolbar auto-refresh timer (at the user's chosen
           interval / on/off), so the global fleet-refresh timer skips them — otherwise both would fire and the
           user's interval choice wouldn't stick. */
        if (MainTabs.SelectedItem is TabItem { Content: ViewerServerTab })
        {
            return;
        }

        await RefreshVisibleAsync();
    }

    private async void OnOverviewTimerTick(object? sender, EventArgs e)
    {
        if (ReferenceEquals(MainTabs.SelectedItem, OverviewTab))
        {
            /* Refresh the sidebar dots on the Overview cadence too, so they track the cards. */
            _ = RefreshServerStatusAsync();
            await RefreshVisibleAsync();
        }
    }

    // ── In-app alert toasts (headless poll of alert history) ─────────────────────────────

    /// <summary>
    /// Refreshes the two store-authoritative toast knobs — the alerts master (<see cref="AlertSettingsRow.Enabled"/>,
    /// the Settings window's "Enable notifications") and the per-condition cooldown
    /// (<see cref="AlertSettingsRow.CooldownMinutes"/>, the "Tray notification cooldown") — from the store, so
    /// the viewer's toasts honor exactly what the Settings window drives + the service applies. Best-effort:
    /// a null row (store not seeded yet) or a read failure leaves the pre-connect fallback values in place.
    /// </summary>
    private async Task RefreshAlertToastSettingsFromStoreAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            var row = await _dataService.GetAlertSettingsAsync();
            if (row is not null)
            {
                _alertsEnabled = row.Enabled;
                _alertCooldownMinutes = row.CooldownMinutes;
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("AlertToasts", $"Reading alert settings for toasts failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Polls recent, non-dismissed alert history once per fleet-refresh tick and drives BOTH the per-server
    /// "needs attention" badges and the tray toasts off that single read (no second query):
    /// <list type="bullet">
    /// <item>the badges (sidebar rows + open tab headers) are refreshed every poll regardless of the toast
    /// master switch — they reflect alert HISTORY, the same rows the Alert History tab shows, gated only by the
    /// viewer-local ack-until-worse state (<see cref="ViewerAlertStateService"/>);</item>
    /// <item>the tray toasts — the headless substitute for Lite's in-process alert-engine toast — fire only when
    /// notifications are enabled and the tray is up, with duplicate-suppression + the per-condition "Tray
    /// notification cooldown" gate in <see cref="AlertToastCoordinator"/>.</item>
    /// </list>
    /// Never throws (a broken poll must not disturb the refresh loop).
    /// </summary>
    private async Task PollAlertsAsync()
    {
        if (_dataService is null || _toastCoordinator is null)
        {
            return;
        }

        if (_alertPollInFlight)
        {
            return;
        }

        _alertPollInFlight = true;
        try
        {
            var rows = await _dataService.GetAlertHistoryAsync(
                DateTime.UtcNow - s_alertPollWindow, serverId: null, limit: 500);

            /* Per-server badges: always, independent of the toast master switch. */
            UpdateServerAttention(rows);

            /* Tray toasts: only when notifications are enabled and the tray exists. */
            if (_alertsEnabled && _trayService is not null)
            {
                var cooldown = TimeSpan.FromMinutes(Math.Max(0, _alertCooldownMinutes));
                foreach (var row in _toastCoordinator.SelectToasts(rows, DateTime.UtcNow, cooldown))
                {
                    ShowAlertToast(row);
                }
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("AlertToasts", $"Alert poll failed: {ex.Message}");
        }
        finally
        {
            _alertPollInFlight = false;
        }
    }

    /// <summary>
    /// Renders one polled alert row as a tray toast: a green "resolved" styled card for resolution notices
    /// (Cleared/Resolved/Restored), otherwise an interactive snoozable card (Error accent for
    /// deadlock/poison, Warning otherwise) whose Snooze writes a scoped mute rule to the store.
    /// </summary>
    private void ShowAlertToast(ViewerAlertRow row)
    {
        if (_trayService is null)
        {
            return;
        }

        var body = $"{row.ServerName}: {ToastBody(row)}";

        if (row.IsResolved)
        {
            _trayService.ShowStyledNotification(row.MetricName, body, ToastSeverity.Success);
            return;
        }

        var icon = row.IsCritical
            ? Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Error
            : Hardcodet.Wpf.TaskbarNotification.BalloonIcon.Warning;

        /* Capture the identity so the snooze callback scopes the mute rule to THIS alert's server + metric. */
        var serverName = row.ServerName;
        var metricName = row.MetricName;
        _trayService.ShowSnoozableNotification(
            row.MetricName, body, icon, duration => SnoozeAlertAsync(serverName, metricName, duration));
    }

    /// <summary>
    /// The toast one-liner: the first meaningful line of the stored detail (the richest summary, the analog
    /// of Lite's per-metric ShortMessage), falling back to the formatted current value.
    /// </summary>
    private static string ToastBody(ViewerAlertRow row)
    {
        if (!string.IsNullOrEmpty(row.DetailText))
        {
            foreach (var line in row.DetailText.Split('\n'))
            {
                var trimmed = line.Trim();
                if (!string.IsNullOrEmpty(trimmed))
                {
                    return trimmed;
                }
            }
        }

        return row.CurrentValueDisplay;
    }

    /// <summary>
    /// Persists a snooze from a tray toast: a temporary mute rule scoped to the alert's server + metric
    /// (Lite's SnoozeBalloon semantics), written straight to <c>config_mute_rules</c>. The running Darling
    /// service re-reads it on its next config load, so the condition stops re-alerting until it expires.
    /// </summary>
    private async Task SnoozeAlertAsync(string serverName, string metricName, TimeSpan duration)
    {
        if (_dataService is null)
        {
            return;
        }

        var rule = new MuteRule
        {
            ServerName = string.IsNullOrEmpty(serverName) ? null : serverName,
            MetricName = metricName,
            ExpiresAtUtc = DateTime.UtcNow + duration,
            Reason = $"Snoozed from tray ({FormatSnoozeDuration(duration)})",
        };

        await _dataService.InsertMuteRuleAsync(rule);
        StatusText.Text = $"Snoozed {metricName} on {serverName} for {FormatSnoozeDuration(duration)} — {DateTime.Now:HH:mm:ss}";
    }

    private static string FormatSnoozeDuration(TimeSpan d) =>
        d.TotalHours >= 1 ? $"{(int)d.TotalHours}h" : $"{(int)d.TotalMinutes}m";

    /// <summary>
    /// Single-clicking a sidebar server drives the server-scoped aggregate tabs to it: it syncs the
    /// Recommendations and FinOps server pickers to the selected server (each remains independently
    /// changeable) so the tabs show the sidebar server instead of whatever the dropdowns last held, then
    /// reloads the visible tab. The two syncs suppress their own SelectionChanged, so the visible tab is
    /// loaded exactly once here by <see cref="RefreshVisibleAsync"/>.
    /// </summary>
    private bool _suppressSidebarSelection;

    /// <summary>
    /// Sidebar selection. A server row syncs the aggregate tabs and loads the visible tab; a group header
    /// is never a real selection — clicking one expands/collapses its group and the selection is restored
    /// to a server. The suppression guard stops that restore from re-entering this handler.
    /// </summary>
    private async void ServerList_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressSidebarSelection)
        {
            return;
        }

        if (ServerList.SelectedItem is FleetHeaderRow header)
        {
            /* The header just took the selection, so the previously-selected server is in RemovedItems. */
            var previousServerId = e.RemovedItems.OfType<FleetServerRow>().FirstOrDefault()?.Server.ServerId;
            ToggleGroup(header, previousServerId);
            return;
        }

        if (ServerList.SelectedItem is FleetServerRow row)
        {
            SyncAggregateServerSelectors(row.Server);
            await RefreshVisibleAsync();
        }
    }

    /// <summary>
    /// Points the Recommendations and FinOps tabs' own server pickers at the sidebar-selected server so the
    /// two server-scoped aggregate tabs track the sidebar. Both selections are set under a suppression guard
    /// (the Recommendations combo via <see cref="_populatingRecoServers"/>, FinOps inside
    /// <see cref="FinOpsTab.SelectServer"/>), so neither fires a redundant refresh — the caller reloads the
    /// visible tab once. The lists are populated from the same managed-server set as the sidebar, so a
    /// selected server is present in both (matched by id).
    /// </summary>
    private void SyncAggregateServerSelectors(DarlingServer server)
    {
        if (RecommendationsServerSelector.ItemsSource is IEnumerable<DarlingServer> recoServers)
        {
            var match = recoServers.FirstOrDefault(s => s.ServerId == server.ServerId);
            if (match is not null && !ReferenceEquals(RecommendationsServerSelector.SelectedItem, match))
            {
                _populatingRecoServers = true;
                RecommendationsServerSelector.SelectedItem = match;
                _populatingRecoServers = false;
            }
        }

        FinOpsContent.SelectServer(server.ServerId);
    }

    /// <summary>Double-click opens (or focuses) the selected server's per-server tab (Lite's rule).</summary>
    private void ServerList_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (ServerList.SelectedItem is FleetServerRow row)
        {
            OpenServerTab(row.Server);
        }
    }

    /// <summary>
    /// Expands or collapses a sidebar group and re-selects a server so the sidebar never rests on a header.
    /// Reprojects through <see cref="FleetView"/> (the single seam), rebinds, and restores the prior server
    /// if it is still visible — else the first visible one — under the suppression guard so the
    /// programmatic re-selection does not recurse into <see cref="ServerList_SelectionChanged"/>.
    /// </summary>
    private void ToggleGroup(FleetHeaderRow header, int? previousServerId)
    {
        _suppressSidebarSelection = true;
        try
        {
            _fleet.ToggleExpanded(header);
            ServerList.ItemsSource = _fleet.Visible;
            ServerList.SelectedItem = _fleet.ResolveSelection(previousServerId);
            PersistCollapseState();
        }
        finally
        {
            _suppressSidebarSelection = false;
        }
    }

    /// <summary>
    /// Lazy per-tab load: switching top-level tabs loads the newly visible one. SelectionChanged is a
    /// bubbling routed event, so selections inside the tab content (a findings grid, an inner server
    /// tab, the server list template) reach here too — only react to the top TabControl's own.
    /// </summary>
    private async void MainTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, MainTabs))
        {
            return;
        }

        await RefreshVisibleAsync();
        /* The status bar's collector-health scope flips with the active tab (a per-server tab shows that
           server; an aggregate tab shows the fleet-cumulative total), so refresh it now rather than waiting
           for the next fleet-refresh tick. */
        await UpdateCollectorHealthTextAsync();
    }

    private async Task LoadServersAsync(bool preserveSelection = false)
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            var previousSelection = (ServerList.SelectedItem as FleetServerRow)?.Server.ServerId;

            /* The DESIRED-state managed set (config_monitored_servers), enriched with the observed
               collect.servers facts by the shared server_id, so a viewer add/remove/enable is reflected at
               once. Stamp the viewer's favorite pins (matched by server name) and sort favorites-first. */
            var servers = ApplyFavoritesAndSort(await _dataService.GetManagedServersAsync());
            _fleet.SetAll(servers);
            ServerList.ItemsSource = _fleet.Visible;
            ServerCountText.Text = $"Servers: {_fleet.TotalCount}";

            /* The Recommendations tab has its OWN server selector, synced to the sidebar selection on a
               single-click (SyncAggregateServerSelectors) yet independently changeable while the tab is open.
               Populate it from the same list; the guard suppresses its SelectionChanged during this initial
               population so the first load comes from the sidebar-driven RefreshVisibleAsync below (which
               reads the now-populated combo). */
            _populatingRecoServers = true;
            RecommendationsServerSelector.ItemsSource = servers;
            if (servers.Count > 0)
            {
                RecommendationsServerSelector.SelectedIndex = 0;
            }
            _populatingRecoServers = false;

            /* The FinOps aggregate tab has its OWN server selector too; hand it the same list. It suppresses
               its selector's SelectionChanged during population (like the Recommendations selector above), so
               the first FinOps load comes from tab activation, not this call. */
            FinOpsContent.SetServers(servers);

            var hasServers = servers.Count > 0;
            ServersHintText.Visibility = hasServers ? Visibility.Collapsed : Visibility.Visible;
            EmptyStatePanel.Visibility = hasServers ? Visibility.Collapsed : Visibility.Visible;
            MainTabs.Visibility = hasServers ? Visibility.Visible : Visibility.Collapsed;

            if (hasServers)
            {
                /* Triggers SelectionChanged, which loads the active aggregate tab. Restore the prior
                   selection after a server add/edit/remove (preserveSelection) so the view doesn't jump
                   back to the first server; the initial load selects the first.

                   Resolved by SERVER, never by index: once tag rows share this list, row 0 is a header
                   rather than a server, so a SelectedIndex = 0 fallback would select a non-server and the
                   aggregate-tab sync would silently never run. */
                ServerList.SelectedItem = _fleet.ResolveSelection(preserveSelection ? previousSelection : null);

                /* Fold the fleet tags into the sidebar (opt-in: no tags => the list stays flat). Done after
                   the selectors are populated and the initial selection is set, so the tag re-projection
                   just preserves that selection under its own suppression guard. */
                await LoadTagsAsync();
            }
            else
            {
                StatusText.Text = $"connected — no servers registered yet ({DateTime.Now:HH:mm:ss})";
            }

            /* Seed the sidebar status dots + the status bar's collector-health / collection / database-size
               fields (each a single lightweight store read). */
            _ = RefreshServerStatusAsync();
            _ = RefreshStoreSizeAsync();
        }
        catch (Exception ex)
        {
            ShowMessage($"Cannot read the Darling store: {ex.Message}");
        }
    }

    /// <summary>
    /// Refreshes whichever top-level tab is visible, with the overlap guard: a per-server tab delegates
    /// to its own inner-tab refresh; the aggregate tabs reload for the sidebar-selected server. If the
    /// user switches tab or server while a load is in flight, the triggering event bounces off the guard
    /// and sets <see cref="_refreshRequested"/>, so the running loop reloads once more when it finishes —
    /// no user action is silently swallowed.
    /// </summary>
    /// <summary>
    /// Loads the Availability Groups tab and reveals it once the store actually has AG rows (#991). Always On is
    /// opt-in and most fleets run none, so the tab ships Collapsed rather than standing there permanently empty.
    /// The reveal is one-way within a session: a fleet that HAS AGs keeps the tab even if a later sweep reads
    /// zero (a collector hiccup should not make a tab vanish under the operator mid-look).
    /// </summary>
    private async Task RefreshAvailabilityGroupsAsync()
    {
        await AvailabilityGroupsContent.RefreshAgAsync();

        if (AvailabilityGroupsContent.HasAvailabilityGroups
            && AvailabilityGroupsTabItem.Visibility != Visibility.Visible)
        {
            AvailabilityGroupsTabItem.Visibility = Visibility.Visible;
        }
    }

    private async Task RefreshVisibleAsync()
    {
        if (_dataService is null)
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
                await LoadVisibleTabAsync();
            }
            while (_refreshRequested);
        }
        finally
        {
            _refreshInFlight = false;
        }
    }

    private async Task LoadVisibleTabAsync()
    {
        try
        {
            switch (MainTabs.SelectedItem)
            {
                case TabItem { Content: ViewerServerTab serverTab }:
                    await serverTab.RefreshActiveInnerTabAsync();
                    break;
                case TabItem tab when ReferenceEquals(tab, OverviewTab):
                    await LoadOverviewAsync();
                    break;
                case TabItem tab when ReferenceEquals(tab, RecommendationsTab):
                    await LoadRecommendationsAsync();
                    break;
                case TabItem tab when ReferenceEquals(tab, AlertsTab):
                    await AlertsHistoryContent.RefreshAlertsAsync();
                    break;
                case TabItem tab when ReferenceEquals(tab, JobHistoryTabItem):
                    await JobHistoryContent.RefreshJobsAsync();
                    break;
                case TabItem tab when ReferenceEquals(tab, AvailabilityGroupsTabItem):
                    await RefreshAvailabilityGroupsAsync();
                    break;
                case TabItem tab when ReferenceEquals(tab, FinOpsTab):
                    await FinOpsContent.RefreshActiveSubTabAsync();
                    break;
            }
        }
        catch (Exception ex)
        {
            StatusText.Text = $"refresh failed: {ex.Message}";
        }
    }

    // ── Per-server tabs (open / focus / close) ──────────────────────────────────────

    /// <summary>
    /// Opens the given server's per-server tab, or focuses it if already open (dedupe by server id).
    /// The new tab's <see cref="ViewerServerTab"/> loads its active inner tab on first show. Returns the
    /// tab's <see cref="ViewerServerTab"/> so a caller (the Recommendations deep-link) can navigate inside
    /// it after opening; null only when the data service is not yet ready.
    /// </summary>
    private ViewerServerTab? OpenServerTab(DarlingServer server)
    {
        if (_dataService is null)
        {
            return null;
        }

        if (_openServerTabs.TryGet(server.ServerId, out var existing))
        {
            MainTabs.SelectedItem = existing;
            return existing.Content as ViewerServerTab;
        }

        var serverTab = new ViewerServerTab(_dataService, server, _preferences, _serverStore);
        serverTab.StatusChanged += OnServerTabStatusChanged;
        serverTab.ApplyTimeRangeRequested += OnApplyTimeRangeToAllRequested;
        serverTab.DisplayModeChanged += OnDisplayModeChanged;

        var tabItem = new TabItem
        {
            Header = CreateServerTabHeader(server),
            Content = serverTab
        };

        _openServerTabs.Add(server.ServerId, tabItem);
        MainTabs.Items.Add(tabItem);
        MainTabs.SelectedItem = tabItem;
        return serverTab;
    }

    /// <summary>Removes a per-server tab and unwires it. WPF selects an adjacent tab when the closed one was active.</summary>
    private void CloseServerTab(int serverId)
    {
        if (!_openServerTabs.TryGet(serverId, out var tab))
        {
            return;
        }

        if (tab.Content is ViewerServerTab serverTab)
        {
            serverTab.StatusChanged -= OnServerTabStatusChanged;
            serverTab.ApplyTimeRangeRequested -= OnApplyTimeRangeToAllRequested;
            serverTab.DisplayModeChanged -= OnDisplayModeChanged;
            /* One dispose path: tears down every chart hover helper the tab's partials own. */
            serverTab.Dispose();
        }

        MainTabs.Items.Remove(tab);
        _openServerTabs.Remove(serverId);
        /* NOTE: the acknowledgement state is deliberately NOT dropped here — unlike Lite, the viewer's badge also
           lives on the always-present sidebar row, so an ack must survive a tab close. It is cleared only when the
           server is removed from the managed set (see ServerContextMenu_Remove_Click). */
    }

    /// <summary>
    /// The per-server tab header: the server name, the alert badge (hidden until the server has active alerts —
    /// left-click or right-click to acknowledge), and a close button. Mirrors Lite's CreateTabHeader shape; the
    /// close affordance uses the shared theme's TabCloseButton style.
    /// </summary>
    private StackPanel CreateServerTabHeader(DarlingServer server)
    {
        var panel = new StackPanel { Orientation = Orientation.Horizontal };

        panel.Children.Add(new TextBlock
        {
            Text = server.DisplayName,
            VerticalAlignment = VerticalAlignment.Center,
            Margin = new Thickness(0, 0, 4, 0)
        });

        /* The per-server alert badge (collapsed until UpdateServerAttention lights it). */
        AttachAlertBadge(panel, server.ServerId);

        var closeButton = new Button
        {
            Cursor = Cursors.Hand,
            ToolTip = "Close tab"
        };
        if (TryFindResource("TabCloseButton") is Style closeStyle)
        {
            closeButton.Style = closeStyle;
        }
        else
        {
            closeButton.Content = "✕";
        }
        closeButton.Click += (s, e) => CloseServerTab(server.ServerId);
        panel.Children.Add(closeButton);

        return panel;
    }

    private void OnServerTabStatusChanged(string message) => StatusText.Text = message;

    /// <summary>
    /// "Apply to All" from one server tab's toolbar: copy its selected range (and, for a custom range, the
    /// From/To in the current display-mode wall clock) to every OTHER open server tab so they window on the
    /// same period. The source tab is skipped — it already holds the range.
    /// </summary>
    private void OnApplyTimeRangeToAllRequested(ViewerServerTab source, int index, DateTime? customFromLocal, DateTime? customToLocal)
    {
        foreach (var tab in _openServerTabs.Values)
        {
            if (tab.Content is ViewerServerTab serverTab && !ReferenceEquals(serverTab, source))
            {
                serverTab.ApplyExternalTimeRange(index, customFromLocal, customToLocal);
            }
        }
    }

    // ── Time-display mode (Server / Local / UTC) ─────────────────────────────────────

    /// <summary>
    /// A server tab's Server/Local/UTC picker changed (the raising tab already set the global mode via
    /// <see cref="ViewerTimeHelper.CurrentDisplayMode"/> and reloaded itself). Persist the new mode and sync
    /// every OTHER open tab's picker so they agree — the mode is process-wide, and a background tab reloads
    /// on activation and picks it up.
    /// </summary>
    private void OnDisplayModeChanged(TimeDisplayMode mode)
    {
        var settings = _appSettingsStore.Load();
        settings.TimeDisplayMode = mode.ToString();
        _appSettingsStore.Save(settings);
        SyncDisplayModeToOpenTabs(mode);
    }

    /// <summary>Sets every open server tab's toolbar picker to <paramref name="mode"/> without raising a
    /// change (each tab suppresses it), keeping the process-wide mode's UI in sync across tabs.</summary>
    private void SyncDisplayModeToOpenTabs(TimeDisplayMode mode)
    {
        foreach (var tab in _openServerTabs.Values)
        {
            if (tab.Content is ViewerServerTab serverTab)
            {
                serverTab.SetDisplayModeSelection(mode);
            }
        }
    }

    /// <summary>Sets the process-wide display mode from a persisted string ("ServerTime"/"LocalTime"/"UTC");
    /// an invalid/absent value falls to Server-time (Lite's default, matching ViewerAppSettings.Normalize).</summary>
    private static void ApplyTimeDisplayMode(string? modeText)
    {
        ViewerTimeHelper.CurrentDisplayMode =
            Enum.TryParse<TimeDisplayMode>(modeText, ignoreCase: true, out var mode) ? mode : TimeDisplayMode.ServerTime;
    }

    // ── Overview tab (all-servers server cards; refreshes on its own timer at the fleet interval) ──────

    /// <summary>Re-sorts the Overview tiles when the header selector changes and persists the choice to
    /// ViewerAppSettings (mirroring <see cref="OnDisplayModeChanged"/>'s load/set/save). Maps the two fixed
    /// selector positions EXPLICITLY (index 1 = Name, else CPU%) so a transient -1 during seeding is never
    /// treated as an undefined enum. Re-sorts the already-bound list directly — no store round-trip.</summary>
    private void OverviewSortSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded)
        {
            return;
        }

        _overviewSortMode = OverviewSortSelector.SelectedIndex == 1
            ? ServerOverviewSortMode.Name
            : ServerOverviewSortMode.Cpu;

        var settings = _appSettingsStore.Load();
        settings.OverviewSortMode = ServerOverviewSort.ToToken(_overviewSortMode);
        _appSettingsStore.Save(settings);

        if (OverviewItemsControl.ItemsSource is IEnumerable<ServerSummaryItem> current)
        {
            OverviewItemsControl.ItemsSource = ServerOverviewSort.Order(
                current.ToList(), _overviewSortMode,
                s => s.CpuPercentForAlert, s => s.DisplayName, s => s.ServerId);
        }
    }

    /// <summary>
    /// Loads a summary card for every registered server (Lite's RefreshOverviewAsync), reading each
    /// server's latest metrics concurrently over the pooled data source. Status is derived from
    /// collection freshness — the viewer has no live ping — via <see cref="ServerSummaryItem.ApplyFreshness"/>.
    /// This aggregate ignores the sidebar selection; it always shows all servers.
    /// </summary>
    private async Task LoadOverviewAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        var servers = _fleet.All;
        if (servers.Count == 0)
        {
            OverviewItemsControl.ItemsSource = null;
            FleetRollupContainer.Visibility = Visibility.Collapsed;
            return;
        }

        var list = servers.ToList();
        if (list.Count == 0)
        {
            OverviewItemsControl.ItemsSource = null;
            FleetRollupContainer.Visibility = Visibility.Collapsed;
            return;
        }

        var service = _dataService;
        var nowUtc = DateTime.UtcNow;
        var summaries = await Task.WhenAll(list.Select(async server =>
        {
            try
            {
                var summary = await service.GetServerSummaryAsync(server.ServerId, server.DisplayName);
                summary.ServerName = server.ServerName;
                summary.ApplyFreshness(nowUtc);
                return summary;
            }
            catch
            {
                /* A per-server read failure shouldn't blank the whole grid (Lite logs + continues). */
                return null;
            }
        }));

        var cards = ServerOverviewSort.Order(
            summaries.OfType<ServerSummaryItem>().ToList(),
            _overviewSortMode,
            s => s.CpuPercentForAlert, s => s.DisplayName, s => s.ServerId);
        OverviewItemsControl.ItemsSource = cards;

        /* Fleet-wide NOC roll-up above the cards — the cross-server totals SQL aggregate (the read only
           Darling's central store can serve) plus the band counts / worst-N ranking reduced from the
           cards' own #1426 banding. The totals use the SAME one-hour window the per-server cards use, so
           they reconcile with the sum of the card counts. A totals-read failure degrades to zero totals
           rather than blanking the Overview — the band counts and ranking still render from the cards. */
        FleetTotals totals;
        try
        {
            totals = await service.GetFleetTotalsAsync(nowUtc.AddHours(-1), nowUtc);
        }
        catch
        {
            totals = new FleetTotals();
        }

        ApplyFleetRollup(FleetRollup.Build(cards, totals));

        StatusText.Text = $"overview — refreshed {DateTime.Now:HH:mm:ss}";
    }

    /// <summary>
    /// Renders the fleet roll-up above the Overview cards: sets the FleetRollupContainer's DataContext and
    /// toggles the worst-first "Needs Attention" ranking against the all-clear line (there is no
    /// inverse-visibility converter, so the swap is done here). The container shows only when there are
    /// servers to roll up.
    /// </summary>
    private void ApplyFleetRollup(FleetRollup rollup)
    {
        FleetRollupContainer.DataContext = rollup;
        FleetRollupContainer.Visibility = rollup.TotalServers > 0 ? Visibility.Visible : Visibility.Collapsed;

        FleetWorstServersList.Visibility = rollup.HasProblems ? Visibility.Visible : Visibility.Collapsed;
        FleetAdditionalProblemsText.Visibility =
            rollup.AdditionalProblemCount > 0 ? Visibility.Visible : Visibility.Collapsed;
        FleetAllHealthyText.Visibility = rollup.HasProblems ? Visibility.Collapsed : Visibility.Visible;
    }

    /// <summary>Double-clicking an Overview card opens (or focuses) that server's tab (Lite's rule).</summary>
    private void OverviewCard_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        if (e.ClickCount == 2
            && sender is FrameworkElement { DataContext: ServerSummaryItem summary })
        {
            /* Resolved against the whole fleet, so a card click still opens a filtered-out server. */
            var server = _fleet.Find(summary.ServerId);
            if (server is not null)
            {
                OpenServerTab(server);
            }
        }
    }

    /// <summary>
    /// Clicking a "Needs Attention" ranking row opens (or focuses) that server's tab — the same
    /// server-id → sidebar-server → OpenServerTab hop the Overview cards use (#1427 made OpenServerTab
    /// return the tab).
    /// </summary>
    private void FleetWorstServer_Click(object sender, MouseButtonEventArgs e)
    {
        if (sender is FrameworkElement { DataContext: FleetRankedServer ranked })
        {
            var server = _fleet.Find(ranked.ServerId);
            if (server is not null)
            {
                OpenServerTab(server);
            }
        }
    }

    // ── Recommendations tab (advise-only cards; own server selector, tab-activation refresh) ─────

    /// <summary>
    /// Reads the latest analysis findings AND the per-server analysis-state marker (V19) for the
    /// Recommendations tab's OWN selected server, and renders Lite's advise-only, incident-grouped cards.
    /// Advise-only: no Apply, and no mute (mute lives on the Alert History surface per the re-skin). The
    /// marker lets a zero-finding read on a young deployment show "still collecting" (the engine skipped
    /// for its 24h data-span gate) instead of a false all-clear; a server with findings shows them, and a
    /// server with enough data and zero findings shows the genuine all-clear. The tab's status line
    /// surfaces the last analysis time; "Generate now" forces an immediate pass via the analyze_now command.
    /// </summary>
    private async Task LoadRecommendationsAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        if (RecommendationsServerSelector.SelectedItem is not DarlingServer server)
        {
            ApplyRecommendationsViewModel(
                RecommendationsViewModel.FromFindings(Array.Empty<ViewerFindingRow>(), string.Empty));
            RecommendationsStatusText.Text = string.Empty;
            return;
        }

        ApplyRecommendationsViewModel(RecommendationsViewModel.Loading());

        var rows = await _dataService.GetLatestFindingsAsync(server.ServerId);

        /* Read the per-server analysis-state marker (V19) the Darling service persists after each pass, so
           a zero-finding read on a young deployment renders "still collecting" (the engine skipped for its
           24h data-span gate) instead of a false all-clear. The marker only decides the ZERO-finding case;
           a server with findings shows them regardless. Null (no pass yet / pre-V19 store) = not insufficient. */
        var analysisState = await _dataService.GetAnalysisStateAsync(server.ServerId);

        ApplyRecommendationsViewModel(
            RecommendationsViewModel.FromFindings(
                rows, server.DisplayName, LocalUtcOffsetMinutes(),
                insufficientData: analysisState?.InsufficientData == true,
                insufficientDataMessage: analysisState?.Message));

        RecommendationsStatusText.Text = rows.Count > 0
            ? $"Last analyzed {rows[0].AnalysisTimeLocal:yyyy-MM-dd HH:mm:ss} (local)"
            : string.Empty;
        StatusText.Text = $"{server.DisplayName} — refreshed {DateTime.Now:HH:mm:ss}";
    }

    /// <summary>Swaps the visible content region to match the view-model's state (mirrors Lite's ApplyViewModel).</summary>
    private void ApplyRecommendationsViewModel(RecommendationsViewModel vm)
    {
        switch (vm.State)
        {
            case RecommendationsState.Loading:
                RecommendationsLoadingText.Visibility = Visibility.Visible;
                RecommendationsScroll.Visibility = Visibility.Collapsed;
                RecommendationsEmptyText.Visibility = Visibility.Collapsed;
                RecommendationsInsufficientText.Visibility = Visibility.Collapsed;
                break;

            case RecommendationsState.InsufficientData:
                RecommendationsLoadingText.Visibility = Visibility.Collapsed;
                RecommendationsSectionsList.ItemsSource = null;
                RecommendationsScroll.Visibility = Visibility.Collapsed;
                RecommendationsEmptyText.Visibility = Visibility.Collapsed;
                RecommendationsInsufficientText.Text = vm.InsufficientDataMessage;
                RecommendationsInsufficientText.Visibility = Visibility.Visible;
                break;

            case RecommendationsState.Empty:
                RecommendationsLoadingText.Visibility = Visibility.Collapsed;
                RecommendationsSectionsList.ItemsSource = null;
                RecommendationsScroll.Visibility = Visibility.Collapsed;
                RecommendationsInsufficientText.Visibility = Visibility.Collapsed;
                RecommendationsEmptyText.Visibility = Visibility.Visible;
                break;

            case RecommendationsState.Loaded:
            default:
                RecommendationsLoadingText.Visibility = Visibility.Collapsed;
                RecommendationsEmptyText.Visibility = Visibility.Collapsed;
                RecommendationsInsufficientText.Visibility = Visibility.Collapsed;
                RecommendationsSectionsList.ItemsSource = vm.Sections;
                RecommendationsScroll.Visibility = Visibility.Visible;
                break;
        }
    }

    /// <summary>The viewer machine's current UTC offset in minutes, for the Ask-AI prompt's local-time window.</summary>
    private static int LocalUtcOffsetMinutes()
        => (int)Math.Round(TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes);

    /// <summary>The tab's own server selector drives it (independent of the sidebar); reload on change.</summary>
    private async void RecommendationsServerSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_populatingRecoServers)
        {
            return;
        }

        await RefreshVisibleAsync();
    }

    private async void RecommendationsRefresh_Click(object sender, RoutedEventArgs e)
        => await RefreshVisibleAsync();

    /// <summary>
    /// "Generate now" — asks the Darling service to run an immediate analysis pass for the selected server via
    /// an <c>analyze_now</c> command, then refreshes the cards from the fresh findings. Restores Lite's
    /// on-demand analysis (the Darling service otherwise runs it on its own ~30-minute cadence). A read-only
    /// seat can't command the service (the button is disabled); a timeout surfaces a "still running" note.
    /// </summary>
    private async void RecommendationsGenerateNow_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || _dataService.IsReadOnly)
        {
            return;
        }

        if (RecommendationsServerSelector.SelectedItem is not DarlingServer server)
        {
            return;
        }

        RecommendationsGenerateButton.IsEnabled = false;
        RecommendationsStatusText.Text = $"Analyzing {server.DisplayName}...";
        try
        {
            var result = await _dataService.RequestAnalyzeNowAsync(server.ServerId);
            if (result is null)
            {
                RecommendationsStatusText.Text = "Analysis is still running — refresh in a moment.";
            }
            else if (result.Status != ViewerDataService.StatusSucceeded)
            {
                RecommendationsStatusText.Text = $"Analysis did not complete: {result.ResultStatus}";
            }

            await LoadRecommendationsAsync();
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (ViewerSchemaSkewException ex)
        {
            MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            RecommendationsStatusText.Text = $"Generate now failed: {ex.Message}";
        }
        finally
        {
            RecommendationsGenerateButton.IsEnabled = _dataService?.IsReadOnly == false;
        }
    }

    /// <summary>
    /// Copies a card's suggested T-SQL to the clipboard (advise-only). SetDataObject with copy=false
    /// avoids WPF's problematic Clipboard.Flush() (matches the Lite convention).
    /// </summary>
    private void CopyFix_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
        {
            return;
        }

        if (string.IsNullOrEmpty(card.CopyPasteSql))
        {
            return;
        }

        Clipboard.SetDataObject(card.CopyPasteSql, false);
        RecommendationsStatusText.Text = "Fix copied to clipboard.";
    }

    /// <summary>Copies a card's MCP investigation prompt to the clipboard (mirrors Lite's Ask AI).</summary>
    private void AskAi_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
        {
            return;
        }

        Clipboard.SetDataObject(card.AskAiPrompt, false);
        RecommendationsStatusText.Text = "AI prompt copied to clipboard.";
    }

    /// <summary>
    /// "Open in Active Queries" deep-link on an incident card (Dashboard-parity port): resolves the
    /// finding's server (the aggregate Recommendations tab is cross-server, so the card carries its own
    /// server id), opens/focuses that server's per-server tab via the shell's existing open-tab path, then
    /// navigates it straight to Active Queries scoped to the finding's window. The whole hop is pure viewer
    /// navigation over already-stored data — no live query, no control-plane write. Only incident cards
    /// (a finding with a time window + server id) show the button, so the predicate is already satisfied.
    /// </summary>
    private async void OpenInActiveQueries_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not RecommendationCardViewModel card)
        {
            return;
        }

        if (!card.ShowOpenInActiveQueries)
        {
            return;
        }

        var server = FindManagedServerById(card.ServerId);
        if (server is null)
        {
            RecommendationsStatusText.Text = "Cannot open Active Queries — the finding's server is no longer registered.";
            return;
        }

        var (fromUtc, toUtc) = card.DeepLinkWindowUtc();
        var serverTab = OpenServerTab(server);
        if (serverTab is null)
        {
            return;
        }

        try
        {
            await serverTab.DeepLinkToActiveQueriesAsync(fromUtc, toUtc);
        }
        catch (Exception ex)
        {
            RecommendationsStatusText.Text = $"Open in Active Queries failed: {ex.Message}";
        }
    }

    /// <summary>Finds a registered server by id in the Recommendations tab's own selector list (populated
    /// from the same managed-server set as the sidebar); null when the server is no longer registered.</summary>
    private DarlingServer? FindManagedServerById(int serverId)
        => RecommendationsServerSelector.ItemsSource is IEnumerable<DarlingServer> servers
            ? servers.FirstOrDefault(s => s.ServerId == serverId)
            : null;

    // ── Settings ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Opens the viewer's full Settings window (the faithful port of Lite's SettingsWindow). It self-persists
    /// the application settings (MCP, alerts, notifications, SMTP, webhooks, analysis) to
    /// <see cref="_appSettingsStore"/> and secrets to Windows Credential Manager; the folded-in viewer
    /// preferences (default time range + auto-refresh) come back via <see cref="SettingsWindow.Result"/> on a
    /// successful Save, so we save them and update the in-memory copy that seeds newly-opened server tabs.
    /// Already-open tabs keep their own toolbar state (the simple, predictable rule). The store connection is
    /// passed through for "Manage Mute Rules" (null before the store is connected disables that button).
    /// </summary>
    private void SettingsButton_Click(object sender, RoutedEventArgs e)
    {
        /* Hand the current managed-server list to the collector-schedule editor's per-server scope. */
        /* The schedule editor edits CONFIG, so it must see every server — a filtered sidebar must never
           mean you cannot edit the schedule of a server it happens to be hiding. */
        var settings = new SettingsWindow(_preferences, _appSettingsStore, _dataService, _fleet.All) { Owner = this };
        if (settings.ShowDialog() == true && settings.Result is not null)
        {
            _preferences = settings.Result;
            _preferencesStore.Save(_preferences);
        }

        /* Re-seed the runtime settings the viewer honors from whatever the window persisted (it self-saves):
           the CSV export separator (next export) and the Server/Local/UTC time-display mode. If the mode
           changed there, sync every open tab's picker and reload the visible tab so its timestamps re-render. */
        var reloaded = _appSettingsStore.Load();
        ViewerExportSettings.Apply(reloaded);
        /* Re-seed the new-mute-rule default expiration + dismiss/mute logging opt-in in case the operator changed them. */
        MuteRuleEditDialog.DefaultExpiration = reloaded.MuteRuleDefaultExpiration;
        AlertsHistoryTab.LogDismissals = reloaded.LogAlertDismissals;
        /* Re-seed the runtime tray/toast knobs so a change takes effect immediately (the tray reads
           _minimizeToTray live). Minimize-to-tray is viewer-local (the reloaded file); the alerts-master +
           "Tray notification cooldown" the window just wrote to the STORE are re-read from there. */
        _minimizeToTray = reloaded.MinimizeToTray;
        _ = RefreshAlertToastSettingsFromStoreAsync();
        /* Re-apply the fleet refresh interval to the live shell timers so a change takes effect immediately
           (the connection timeout is a connect-time setting and applies on the next viewer launch). */
        _refreshInterval = TimeSpan.FromSeconds(reloaded.NocRefreshIntervalSeconds);
        if (_refreshTimer is not null)
        {
            _refreshTimer.Interval = _refreshInterval;
        }
        if (_overviewTimer is not null)
        {
            _overviewTimer.Interval = _refreshInterval;
        }
        var previousMode = ViewerTimeHelper.CurrentDisplayMode;
        ApplyTimeDisplayMode(reloaded.TimeDisplayMode);
        if (ViewerTimeHelper.CurrentDisplayMode != previousMode)
        {
            SyncDisplayModeToOpenTabs(ViewerTimeHelper.CurrentDisplayMode);
            _ = LoadVisibleTabAsync();
        }
    }

    // ── About ────────────────────────────────────────────────────────────────────────

    /// <summary>Opens the viewer's About dialog (app name, version, copyright, links).</summary>
    private void AboutButton_Click(object sender, RoutedEventArgs e)
    {
        var about = new AboutWindow { Owner = this };
        about.ShowDialog();
    }

    /// <summary>
    /// The one true window-restore path, shared by the tray (Show Window / double-click), minimize-to-tray
    /// restore, and the single-instance second-launch surface, so the window can never be left hidden-but-blank.
    /// Reconciles a tray-hidden window — minimize-to-tray calls <see cref="Window.Hide"/>, which sets
    /// Visibility=Hidden; only <see cref="Window.Show"/> re-runs layout/render (a raw Win32 ShowWindow would
    /// leave it blank, Lite #1050) — and restores a minimized window to the viewer's Maximized startup state.
    /// Must run on the UI thread.
    /// </summary>
    public void SurfaceWindow()
    {
        if (WindowState == WindowState.Minimized)
        {
            WindowState = WindowState.Maximized;
        }

        Show();
        ShowInTaskbar = true;
        Activate();
        /* Nudge to the top without pinning topmost. */
        Topmost = true;
        Topmost = false;
    }

    private void ShowMessage(string message)
    {
        MessageText.Text = message;
        MessageOverlay.Visibility = Visibility.Visible;
        StatusText.Text = "";
    }

    private async void OnClosed(object? sender, EventArgs e)
    {
        _refreshTimer?.Stop();
        _overviewTimer?.Stop();
        _resumeGuard?.Dispose();
        _trayService?.Dispose();

        if (_dataService is not null)
        {
            await _dataService.DisposeAsync();
        }
    }
}
