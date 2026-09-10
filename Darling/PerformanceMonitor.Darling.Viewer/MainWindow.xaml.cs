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
using PerformanceMonitor.Darling.Storage;
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
    /// The per-user settings files that were on disk at startup and could not be read AT ALL, each with why
    /// (#2434). Collected in the constructor, where all three stores are loaded, and reported once from
    /// <see cref="OnLoaded"/> — a dialog raised from the constructor has no window behind it. Empty on
    /// every ordinary run INCLUDING a first run: an absent file is not a problem, and saying so would make
    /// a warning out of the most normal thing the viewer ever does.
    /// </summary>
    private readonly List<string> _unreadableSettingsFiles = new();

    /// <summary>
    /// The individual SETTINGS that could not be read, per file, when the rest of that file loaded normally
    /// (#2456). Kept apart from <see cref="_unreadableSettingsFiles"/> because the two need different
    /// sentences: one says every setting in the file reverted, the other says these did and nothing else
    /// did. Collapsing them would put an overstatement in front of whichever case was not being described,
    /// and the whole complaint here was a dialog that could not say which settings were lost.
    /// </summary>
    private readonly List<string> _unreadableSettingsValues = new();

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

    /// <summary>
    /// The FULL, sorted Overview card set — the authority behind <c>OverviewItemsControl.ItemsSource</c> since
    /// #2424, which now shows a possibly-filtered projection of it. Everything that used to read the bound list
    /// back to answer "what cards are there" reads this instead, for the same reason <see cref="FleetView"/>
    /// separates <c>All</c> from <c>Visible</c>: the moment the bound list is a SUBSET, a consumer that reads it
    /// back is silently answering a different question.
    /// </summary>
    private List<ServerSummaryItem> _overviewCards = new();

    /// <summary>
    /// True while the Overview grid is filtered to servers that need attention (#2424) — the destination the
    /// "+N more need attention" line finally has. Deliberately NOT persisted to ViewerAppSettings the way
    /// <see cref="_overviewSortMode"/> is: a sort is a preference, this is a triage action tied to a moment, and
    /// an Overview that opens with 52 of 57 servers already hidden is a support ticket even with the toggle in
    /// plain sight.
    /// </summary>
    private bool _overviewAttentionOnly;

    /// <summary>
    /// The non-secret configuration block shown on every connection/config failure (#1954): which
    /// darling.json won and what was parsed from it. Built at startup before the load is attempted (so a
    /// missing file still reports where the viewer looked) and extended with the parse summary once the file
    /// loads. Never carries credentials — <see cref="ViewerConfigDiagnostics"/> builds it from an allowlist.
    /// </summary>
    private string _connectionDiagnostics = "";

    /// <summary>
    /// The EFFECTIVE store connection string (post-#1970 certificate anchoring — what Npgsql
    /// actually opens), kept for the failure overlay's "Run self-test" (#1954 bullet 3). Null until
    /// config resolves; the button reports that instead of probing nothing.
    /// </summary>
    private string? _storeConnectionString;

    public MainWindow()
    {
        InitializeComponent();
        _preferences = _preferencesStore.Load();
        NoteUnreadableSettingsFile(_preferencesStore.FilePath, _preferencesStore.LastLoadState, _preferencesStore.LastLoadProblem,
            _preferencesStore.LastLoadUnreadableMembers);
        /* The registry loads in its own field initializer, which has already run by the time the
           constructor body does, so its state is available here — and it is the one of the three worth
           surfacing most: a viewer that opens with no servers because it could not read viewer-servers.json
           looks exactly like a viewer nobody has configured yet. */
        NoteUnreadableSettingsFile(_serverStore.FilePath, _serverStore.LastLoadState, _serverStore.LastLoadProblem,
            _serverStore.LastLoadUnreadableMembers);
        /* Seed the persisted app settings the viewer honors at runtime BEFORE any tab/chart renders: the CSV
           export separator (grid exports) and the Server/Local/UTC time-display mode (every timestamp render
           routes through ViewerTimeHelper, which reads CurrentDisplayMode). UiTimeContext is deliberately left
           at its identity default — Darling charts pre-convert their X through ForDisplay, so wiring it would
           double-convert on hover/crosshair. */
        var appSettings = _appSettingsStore.Load();
        NoteUnreadableSettingsFile(_appSettingsStore.FilePath, _appSettingsStore.LastLoadState, _appSettingsStore.LastLoadProblem,
            _appSettingsStore.LastLoadUnreadableMembers);
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

    /// <summary>
    /// Records a settings file that is present and could not be read, for the one startup report. Absent
    /// and readable both record nothing, which is the whole point of the three-way split.
    ///
    /// <para>Since #2456 the state carries a fourth possibility inside <c>Unreadable</c>: the file was read
    /// after dropping members the deserializer could not understand, and <paramref name="members"/> names
    /// them. That goes on the other list, because it is a different claim — those settings reverted and the
    /// rest of the file did not.</para>
    /// </summary>
    private void NoteUnreadableSettingsFile(
        string filePath,
        SettingsFileState state,
        string? problem,
        IReadOnlyList<SettingsMemberProblem> members)
    {
        if (state != SettingsFileState.Unreadable)
        {
            return;
        }

        var fileName = System.IO.Path.GetFileName(filePath);

        if (members.Count > 0)
        {
            _unreadableSettingsValues.Add($"{fileName} — {string.Join(", ", members)}");
            return;
        }

        _unreadableSettingsFiles.Add($"{fileName} — {problem}");
    }

    /// <summary>
    /// One dialog, once, when a settings file the viewer has just fallen back to defaults for is still
    /// sitting on disk unreadable (#2434).
    ///
    /// <para>The fallback used to report itself through <c>Debug.WriteLine</c>, which the compiler removes
    /// from a Release build — so in the viewer anyone actually runs, the operator's settings quietly became
    /// defaults and there was nowhere whatsoever to find out why. The log carries it now, but a person
    /// looking at an app that has forgotten its configuration should not have to go and find a log to learn
    /// that it did.</para>
    ///
    /// <para>Raised from Loaded so it has a window behind it, and before the store connect below on
    /// purpose: it costs nothing when there is nothing to say, and the connection-failure overlay would
    /// otherwise bury the one message that explains why the settings changed.</para>
    ///
    /// <para>Two paragraphs rather than one list, because there are two different facts and #2456 is the
    /// issue about telling them apart. A file that could not be read at all costs every setting in it; a
    /// file that was read after dropping named members costs only those. One sentence covering both would
    /// have to overstate one of them, and an overstated capability is worse than an admitted gap — which
    /// is also why the second paragraph says the settings are NAMED rather than implying the list is
    /// exhaustive of everything wrong with the file.</para>
    /// </summary>
    private void ReportUnreadableSettingsFiles()
    {
        if (_unreadableSettingsFiles.Count == 0 && _unreadableSettingsValues.Count == 0)
        {
            return;
        }

        var message = new System.Text.StringBuilder();

        if (_unreadableSettingsFiles.Count > 0)
        {
            message
                .Append("The viewer could not read these settings files, so the settings they hold are at ")
                .Append("their defaults for this session:\n\n")
                .AppendJoin('\n', _unreadableSettingsFiles)
                .Append("\n\n");
        }

        if (_unreadableSettingsValues.Count > 0)
        {
            message
                .Append("These individual settings could not be read and are at their defaults for this ")
                .Append("session. Everything else in their file loaded normally:\n\n")
                .AppendJoin('\n', _unreadableSettingsValues)
                .Append("\n\n");
        }

        message
            .Append("The files have been left exactly as they are. Fix the JSON and restart to get those ")
            .Append("settings back — or save from the Settings window, which copies an unreadable file ")
            .Append("aside as '.unreadable-<timestamp>' before replacing it.");

        MessageBox.Show(message.ToString(), "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
    }

    private async void OnLoaded(object sender, RoutedEventArgs e)
    {
        Loaded -= OnLoaded;

        ReportUnreadableSettingsFiles();

        /* #1954: say WHERE the config came from before trying to read it, so a missing or unparseable
           darling.json still names the file the viewer looked at and the rule that picked it. Resolving
           here (rather than inside TryLoad) also means the path we report is provably the path we load —
           the resolved path is what gets handed to TryLoad as its explicit path. */
        var configLocation = ViewerSettings.ResolveConfigLocation(ExplicitConfigPathFromArgs());
        LogDiagnostics(ViewerConfigDiagnostics.DescribeConfigLocation(configLocation));
        _connectionDiagnostics = ViewerConfigDiagnostics.BuildDetails(
            configLocation, connectionString: null, managed: false, configLocation.Directory);

        ViewerSettings? settings;
        try
        {
            settings = await Task.Run(() => ViewerSettings.TryLoad(configLocation.Path));
        }
        catch (Exception ex)
        {
            ViewerLogger.Error(ViewerConfigDiagnostics.LogSource, "darling.json could not be read", ex);
            ShowConnectionFailure($"darling.json could not be read: {ex.Message}");
            return;
        }

        if (settings is null)
        {
            ShowConnectionFailure("darling.json not found — copy darling.sample.json from the service and point DARLING_CONFIG at it.");
            return;
        }

        /* The non-secret parse summary (#1954): host, port, username, database, SSL mode, search path, the
           resolved certificate path and whether it exists, and whether the string was derived or read
           verbatim — everything but the credential. This is what separates "it read the wrong file" from
           "it read the right file and the value in it is wrong".
           Summarized from the CONFIGURED string plus darling.json's directory, so the certificate is reported
           as the operator wrote it AND as the anchor resolves it (#1970) — the same two inputs
           ViewerCertificateAnchor already used to rewrite settings.ConnectionString, so the "resolves to" line
           is the path Npgsql opens by construction, not by agreement. */
        var connectionSummary = ViewerConfigDiagnostics.DescribeConnection(
            settings.ConfiguredConnectionString, settings.Managed, configLocation.Directory);
        LogDiagnostics(connectionSummary);
        _connectionDiagnostics = ViewerConfigDiagnostics.BuildDetails(
            configLocation, settings.ConfiguredConnectionString, settings.Managed, configLocation.Directory);

        _storeConnectionString = settings.ConnectionString;
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
                /* The store answered, so this is not Unreachable — but the seat probe never ran, so the
                   field stays at "Seat: --" rather than claiming a verdict nothing measured (#2479). */
                ApplySeatState(ViewerSeatState.Unknown);
                ShowConnectionFailure(
                    $"The Darling store is at schema v{version}, but this viewer needs v{ViewerDataService.RequiredStoreSchemaVersion}. " +
                    "Update or restart the Darling service so it migrates the store, then reopen the viewer.");
                return;
            }

            /* V8 security hardening: probe whether this connection can write the operator-config tables
               (admin/owner) or is the read-only viewer role, before showing any write affordance. The
               probe fails safe to read-only; the write surfaces gate on ViewerDataService.IsReadOnly and
               the write paths translate a live 42501 into a friendly message as a backstop. */
            await _dataService.DetectReadOnlyAsync();

            /* #2479 items 3+4: publish the verdict ONCE, globally, instead of letting it be discovered a
               dialog at a time. Same probe, same fail-safe — this only makes the answer visible before a
               write is attempted, which is the whole of #2400. */
            ApplySeatState(_dataService.IsReadOnly ? ViewerSeatState.ReadOnly : ViewerSeatState.ReadWrite);
        }
        catch (ViewerStoreUnreachableException ex)
        {
            /* NOT ReadOnly. #2117 built this arm precisely so an unreachable store stops being misread as
               a read-only seat, and a status field that collapsed them would undo it in the one place an
               operator now looks first — sending them to fix a role they never needed to touch. */
            ApplySeatState(ViewerSeatState.Unreachable);
            ViewerLogger.Error("App", "Darling store unreachable", ex);
            ShowConnectionFailure(ex.Message);
            return;
        }
        catch (Exception ex)
        {
            /* Reachable but the first connection failed for another reason (e.g. authentication, or the
               configured database does not exist) — show it rather than dead-ending on a blank window. */
            ApplySeatState(ViewerSeatState.Unreachable);
            ViewerLogger.Error("App", "Darling store connection failed", ex);
            ShowConnectionFailure($"Couldn't connect to the Darling store: {ex.Message}");
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
           skips it; both run at the one configurable fleet interval.

           "They never double-refresh the same GRID" is the whole of the claim, and it is worth stating that
           narrowly: the aggregate timer's Overview early-return is what guarantees it, and that return sits
           BELOW its freshness fan-out. So the grid was never double-refreshed while the freshness reads were,
           on every Overview cycle, until OnOverviewTimerTick stopped duplicating them. */
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
        /* Refresh the sidebar status dots + the status bar every cycle regardless of the visible tab, so
           freshness stays current even while a per-server tab is up.

           NOT the "cheap pair of single-query reads" this comment used to claim, which is why all three of
           these are single-flight: RefreshServerStatusAsync is a pair BY ITSELF (the freshness query, then
           UpdateCollectorHealthTextAsync's collector-health read) and PollAlertsAsync is another (history,
           then UpdateServerSilencedAsync's mute rules), so this fan-out is FIVE store reads before
           RefreshVisibleAsync starts — six on the fleets that ship with the AG tab hidden, which is most of
           them. They run TOGETHER, so each one's deadline has to cover contending with the other five for
           the ten-connection pool rather than a solo read's — hence the declared width, on an interval an
           operator can set to 10s. The deadline bounds how long one of them holds a permit and the guards are
           what stop ticks from stacking.

           This is a fan-out without a Task.WhenAll: nothing joins these, they are simply all in flight at
           once. ViewerCommandTimeoutTests can only scan the WhenAll shape, so this site and the connect-path
           pair below are the two the width is declared on by hand.

           The scope deliberately runs to the end of the tick rather than closing after the three starts: the
           visible-tab load below begins while these are still in flight, so it really is contending with
           them, and a tab load that declares its own fan-out nests to the pool ceiling — which is the width
           the concurrent measurement actually covers. */
        using var readFanOut = ViewerReadFanOut.Of(6);

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
            /* The sidebar dots are deliberately NOT refreshed here, even though they read the same freshness
               signal the Overview cards do. OnRefreshTimerTick already fans them out unconditionally, at this
               same interval, and its Overview early-return happens AFTER that fan-out — so firing them here
               too issued two concurrent freshness read-pairs per cycle whenever the Overview was the visible
               tab, which is the one tab that ships selected. The dots still track the cards: both timers are
               started back-to-back at the one interval, so the aggregate timer's fan-out lands in the same
               cycle this tick does. Re-adding the call would now be worse than redundant rather than merely
               wasteful — RefreshServerStatusAsync is single-flight, so a periodic caller's second call can
               only be dropped, and one that asked to replay would guarantee the extra pass the guard exists
               to prevent. */
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

            /* The sidebar's muted-bell (#2031): same cadence, its own small read (mute rules, not history). */
            await UpdateServerSilencedAsync();

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

    /// <summary>
    /// Live server-search box (sidebar). Pushes the term into the fleet projection — the one reserved
    /// <see cref="FleetView.SetSearch"/> seam, which matches server name AND tag names — then rebinds the
    /// list and updates the count. Grouping, favourites, and selection all follow the projection, so nothing
    /// else needs touching. Filtering is a cheap in-memory pass, so running it per keystroke is fine.
    /// </summary>
    private void ServerSearchBox_TextChanged(object sender, System.Windows.Controls.TextChangedEventArgs e)
    {
        _fleet.SetSearch(ServerSearchBox.Text);
        ServerList.ItemsSource = _fleet.Visible;
        UpdateServerCountText();
    }

    /// <summary>The sidebar "Servers: N" label — "N of total" while a search narrows the fleet, plain total
    /// otherwise. Centralised so the load path and the search path can't drift.</summary>
    private void UpdateServerCountText() =>
        ServerCountText.Text = _fleet.IsSearching
            ? $"Servers: {_fleet.VisibleServerCount} of {_fleet.TotalCount}"
            : $"Servers: {_fleet.TotalCount}";

    /// <summary>
    /// The status bar's "Seat:" field (#2479, items 3 and 4) — text, colour and the tooltip that explains
    /// WHY, in one place so no caller can set half of it.
    ///
    /// <para>The colours are the existing status-bar vocabulary, not a new one:
    /// <c>ErrorBrush</c> for a store that cannot be reached (the same severity <c>CollectorHealthText</c>
    /// gives an erroring collector), <c>WarningBrush</c> for a read-only seat — visible, because a tester
    /// who does not notice it is back to discovering the seat one refusal at a time, but not alarming,
    /// because on a remote seat it is the correct and expected default — and the muted foreground for
    /// read-write and for not-yet-probed, which are the unremarkable states.</para>
    ///
    /// <para><see cref="ViewerDataService.StoreIsOnThisMachine"/> only ORDERS the two sentences in the
    /// tooltip by which default is likelier to have decided this seat. It cannot claim which one did: a
    /// non-loopback store host does not prove a remote seat (#2279), so the tooltip names both.</para>
    /// </summary>
    private void ApplySeatState(ViewerSeatState state)
    {
        SeatText.Text = ViewerSeatIndicator.Text(state);
        SeatText.ToolTip = ViewerSeatIndicator.ToolTip(state, _dataService?.StoreIsOnThisMachine ?? true);

        /* TryFindResource rather than the hard FindResource the sibling status fields use, and the reason is
           the call site rather than doubt about the keys: all three brushes exist in all three shipped
           themes, but ApplySeatState is called from INSIDE the catch that handles an unreachable store. A
           ResourceReferenceKeyNotFoundException thrown from a catch block is unhandled, so a missing key in
           some future theme would turn "the service is down" into "the viewer crashed" - the one moment the
           viewer most needs to stay up and explain itself. Falling back to the muted foreground costs a
           colour and nothing else. */
        SeatText.Foreground =
            (state switch
            {
                ViewerSeatState.Unreachable => TryFindResource("ErrorBrush"),
                ViewerSeatState.ReadOnly => TryFindResource("WarningBrush"),
                _ => TryFindResource("ForegroundMutedBrush"),
            } as System.Windows.Media.Brush)
            ?? TryFindResource("ForegroundMutedBrush") as System.Windows.Media.Brush
            ?? SeatText.Foreground;
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
            UpdateServerCountText();

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
               fields.

               replayIfBusy, because this is the one caller whose request a timer's in-flight read cannot
               answer: this path runs on connect and after every add/edit/remove, and a freshness dictionary
               fetched before a server was registered has no entry for it — the row it paints after the fleet
               rebuild reads as never-collected. Dropping that would leave a just-added server's dot wrong for
               a whole interval. The store-size read takes no scope, so it drops like any other caller.

               Three reads in flight together — the freshness pair plus the store size — so the width is
               declared for the same reason the fleet timer's is. */
            using var readFanOut = ViewerReadFanOut.Of(3);

            _ = RefreshServerStatusAsync(replayIfBusy: true);
            _ = RefreshStoreSizeAsync();
        }
        catch (Exception ex)
        {
            ShowConnectionFailure($"Cannot read the Darling store: {ex.Message}");
        }
    }

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

    /// <summary>
    /// Refreshes whichever top-level tab is visible, with the overlap guard: a per-server tab delegates
    /// to its own inner-tab refresh; the aggregate tabs reload for the sidebar-selected server. If the
    /// user switches tab or server while a load is in flight, the triggering event bounces off the guard
    /// and sets <see cref="_refreshRequested"/>, so the running loop reloads once more when it finishes —
    /// no user action is silently swallowed.
    /// </summary>
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
    /// Says that a setting the user just changed did not reach disk (#2434).
    ///
    /// <para>The two handlers below persist on an ordinary click — picking a time-display mode, re-sorting
    /// the Overview — so a failure there has no Save button to report through, and until now had nowhere to
    /// go at all: the store's Save could throw, neither handler caught it, and the whole-object write had
    /// already replaced whatever it could not read. Save answers with a bool instead of throwing now, and
    /// this is what does something with the answer. Which file and what was wrong with it is in the log;
    /// what the user needs from a dialog is that the click did not stick.</para>
    /// </summary>
    private void WarnSettingNotSaved(string what, string filePath)
    {
        MessageBox.Show(
            $"Your {what} could not be saved to '{System.IO.Path.GetFileName(filePath)}'. It applies for "
            + "the rest of this session but will not survive a restart. The viewer log (sidebar: View Log) "
            + "says why.",
            "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
    }

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
        if (!_appSettingsStore.Save(settings))
        {
            WarnSettingNotSaved("time display mode", _appSettingsStore.FilePath);
        }

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
        if (!_appSettingsStore.Save(settings))
        {
            WarnSettingNotSaved("Overview sort order", _appSettingsStore.FilePath);
        }

        /* Sorts the FULL card set, never the bound list: with the needs-attention filter on, the bound list is
           a subset, and sorting that would quietly discard every card the filter is hiding. */
        _overviewCards = ServerOverviewSort.Order(
            _overviewCards, _overviewSortMode,
            s => s.CpuPercentForAlert, s => s.DisplayName, s => s.ServerId);
        ApplyOverviewCardFilter();
    }

    /// <summary>Attaches each server's tags as coloured pills to its Overview summary, joining the loaded tag
    /// list (<c>_tags</c> / <c>_serverTagIds</c>) — the summary carries no tag query of its own. Pills are
    /// ordered by tag name so a card is stable. No tags anywhere → every card keeps its empty default.</summary>
    private void StampTagPills(IReadOnlyList<ServerSummaryItem> summaries)
    {
        if (_tags.Count == 0 || _serverTagIds.Count == 0)
        {
            return;
        }

        var tagById = _tags.ToDictionary(t => t.Id);
        foreach (var summary in summaries)
        {
            if (!_serverTagIds.TryGetValue(summary.ServerId, out var tagIds) || tagIds.Count == 0)
            {
                summary.TagPills = System.Array.Empty<ServerTagPill>();
                continue;
            }

            summary.TagPills = tagIds
                .Select(id => tagById.TryGetValue(id, out var tag) ? tag : null)
                .Where(t => t is not null)
                .OrderBy(t => t!.Name, StringComparer.OrdinalIgnoreCase)
                .Select(t => new ServerTagPill(t!.Name, t!.Colour))
                .ToList();
        }
    }

    /// <summary>Re-stamps tag pills onto the Overview's cards (after a tag colour / assignment change) and
    /// re-projects so the change renders without a full per-server reload. Stamps the FULL card set rather than
    /// the bound list, so a card the needs-attention filter is hiding does not come back missing its pills.
    /// A no-op unless the Overview is populated.</summary>
    private void RestampOverviewTagPills()
    {
        if (_overviewCards.Count == 0)
        {
            return;
        }

        StampTagPills(_overviewCards);
        ApplyOverviewCardFilter();
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
            ClearOverviewCards();
            return;
        }

        var list = servers.ToList();
        if (list.Count == 0)
        {
            ClearOverviewCards();
            return;
        }

        var service = _dataService;
        var nowUtc = DateTime.UtcNow;
        /* #3016: pool-many lanes, not fleet-many reads — see the inventory overlay in FinOpsTab.Loaders.
           A fleet wider than the pool used to render SHORT here rather than slowly: the per-server catch
           below turned each pool-exhaustion failure into an absent card, so the panel a fleet is watched
           from silently dropped every server past the permits whenever the store was slow enough for the
           first ten to hold their slots for ConnectionTimeoutSeconds. Lanes are contiguous, so
           concatenating them in lane order restores the fleet order this used to read in. */
        var lanes = ViewerReadFanOut.Lanes(list);
        using var readFanOut = ViewerReadFanOut.Of(lanes.Count);

        var perLane = await Task.WhenAll(lanes.Select(async lane =>
        {
            var found = new List<ServerSummaryItem>(lane.Count);

            foreach (var server in lane)
            {
                try
                {
                    var summary = await service.GetServerSummaryAsync(server.ServerId, server.DisplayName);
                    summary.ServerName = server.ServerName;
                    /* #3029: the engine discriminator comes from the REGISTRY row, which already carries it
                       (servers.engine_kind, via ManagedServersSql / ServersSql) — the per-server summary
                       reads have no engine column and need none. It is what tells the fleet deadlock total's
                       coverage apart from a quiet SQL Server fleet: v_deadlocks holds the SQL Server
                       extended-event capture and nothing else. */
                    summary.IsPostgres = server.IsPostgres;
                    summary.ApplyFreshness(nowUtc);
                    found.Add(summary);
                }
                catch
                {
                    /* A per-server read failure shouldn't blank the whole grid (Lite logs + continues). */
                }
            }

            return found;
        }));

        /* The per-server reads are done; the fleet-totals read below does not contend with them. */
        readFanOut.Release();

        var built = perLane.SelectMany(lane => lane).ToList();
        StampTagPills(built);

        var cards = ServerOverviewSort.Order(
            built,
            _overviewSortMode,
            s => s.CpuPercentForAlert, s => s.DisplayName, s => s.ServerId);

        /* The full set is the authority; the grid shows whatever the needs-attention filter leaves of it. A
           refresh must not silently drop the filter, so this re-applies it rather than binding `cards` directly. */
        _overviewCards = cards;
        ApplyOverviewCardFilter();

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

        /* #2753: TotalServers must be the registered fleet size (list.Count, the same source the sidebar's
           "Servers: N" reads), not cards.Count — cards silently drops any server whose per-server summary
           read failed this cycle, which made the Overview's total wobble against the stable sidebar count. */
        ApplyFleetRollup(FleetRollup.Build(cards, totals, totalServerCount: list.Count));

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
        FleetAdditionalProblems.Visibility =
            rollup.AdditionalProblemCount > 0 ? Visibility.Visible : Visibility.Collapsed;
        /* #2753 review: the all-clear line must not claim "All N healthy" while some of those N have no
           summary this cycle at all (IsAllClear, not HasProblems, is the gate) — and that gap gets its own
           line rather than being silently absorbed into either the healthy or the all-clear text. */
        FleetUnknownStatusText.Visibility = rollup.UnknownCount > 0 ? Visibility.Visible : Visibility.Collapsed;
        FleetAllHealthyText.Visibility = rollup.IsAllClear ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>Empties the Overview: no cards, no roll-up, and no stale authority left behind for the filter
    /// or the tag re-stamp to project from.</summary>
    private void ClearOverviewCards()
    {
        _overviewCards = new List<ServerSummaryItem>();
        OverviewItemsControl.ItemsSource = null;
        FleetRollupContainer.Visibility = Visibility.Collapsed;
        ApplyOverviewAttentionCount(shown: 0);
    }

    /// <summary>
    /// Projects <see cref="_overviewCards"/> onto the grid through the needs-attention filter (#2424) — the
    /// ONE place ItemsSource is set for a populated Overview, so a refresh, a re-sort and a tag re-stamp cannot
    /// each have their own opinion about whether the filter is still on.
    ///
    /// <para>The predicate is <see cref="FleetRollup.NeedsAttention"/>, the same banding the roll-up counted
    /// the "+N more" with, so the grid the link lands on holds exactly the servers the link was counting.</para>
    /// </summary>
    private void ApplyOverviewCardFilter()
    {
        var shown = _overviewAttentionOnly
            ? FleetRollup.NeedsAttention(_overviewCards)
            : _overviewCards;

        OverviewItemsControl.ItemsSource = shown;
        ApplyOverviewAttentionCount(shown.Count);
    }

    /// <summary>
    /// Shows what the filter did, beside the toggle that did it. Only rendered while the filter is on: a grid
    /// showing every server needs no arithmetic, and a filtered one must never be mistakable for it.
    ///
    /// <para>The colour follows the sentence. This line has two of them — a count of servers wanting attention,
    /// and an all-clear when the filter matched none — and painting the all-clear amber would be a colour
    /// contradicting its own text, which is the family of defect this whole change is about. Set through
    /// SetResourceReference (the alert badge's idiom) so it still tracks a theme change.</para>
    /// </summary>
    private void ApplyOverviewAttentionCount(int shown)
    {
        if (_overviewAttentionOnly)
        {
            OverviewAttentionCountText.Text = FleetRollup.AttentionFilterCountText(shown, _overviewCards.Count);
            OverviewAttentionCountText.SetResourceReference(
                ForegroundProperty, shown > 0 ? "WarningBrush" : "SuccessBrush");
            OverviewAttentionCountText.Visibility = Visibility.Visible;
            return;
        }

        OverviewAttentionCountText.Text = string.Empty;
        OverviewAttentionCountText.Visibility = Visibility.Collapsed;
    }

    /// <summary>The needs-attention toggle: re-projects the grid from the unchanged card set, so turning it off
    /// restores the full fleet without a store round-trip. Deliberately carries no IsLoaded guard, unlike the sort
    /// selector — that one needs it because seeding the ComboBox raises SelectionChanged during construction, and
    /// nothing seeds this box. A guard here would only be able to drop a real transition and leave a ticked box
    /// over an unfiltered grid, which is the same lie as the one this fixes, in the other direction.</summary>
    private void OverviewAttentionOnlyCheck_Changed(object sender, RoutedEventArgs e)
    {
        _overviewAttentionOnly = OverviewAttentionOnlyCheck.IsChecked == true;
        ApplyOverviewCardFilter();
    }

    /// <summary>
    /// "+N more need attention" — the servers the ranking's cap could not show. Clicking it turns the filter on
    /// rather than doing the filtering itself, so the state lands in the toggle where the reader can see it is on
    /// and can turn it off; the alternative, a grid that silently shrank, is the worse version of this defect.
    /// </summary>
    private void FleetAdditionalProblems_Click(object sender, MouseButtonEventArgs e)
    {
        OverviewAttentionOnlyCheck.IsChecked = true;
        e.Handled = true;
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
    /// preferences (default time range + auto-refresh) come back via <see cref="SettingsWindow.Result"/>, so we
    /// save them and update the in-memory copy that seeds newly-opened server tabs. Already-open tabs keep
    /// their own toolbar state (the simple, predictable rule). The store connection is passed through for
    /// "Manage Mute Rules" (null before the store is connected disables that button).
    ///
    /// <para>#2715: persisting <see cref="SettingsWindow.Result"/> depends ONLY on whether Save produced one
    /// (<see cref="ShouldPersistViewerPreferences"/>), never on <c>ShowDialog()</c>'s own return value. Default
    /// Time Range and Auto-refresh interval are purely local, per-user preferences with no relationship to the
    /// shared Postgres/TimescaleDB control-plane store the SAME Save click also writes; gating their local file
    /// write on that unrelated write's outcome meant a read-only seat's <see cref="ViewerReadOnlyException"/>
    /// (or any other store-write failure) silently discarded a local preference change it had nothing to do
    /// with. <see cref="SettingsWindow.Result"/> is captured before the store write is even attempted, so this
    /// is safe unconditionally.</para>
    /// </summary>
    private void SettingsButton_Click(object sender, RoutedEventArgs e)
    {
        /* Hand the current managed-server list to the collector-schedule editor's per-server scope. */
        /* The schedule editor edits CONFIG, so it must see every server — a filtered sidebar must never
           mean you cannot edit the schedule of a server it happens to be hiding. */
        var settings = new SettingsWindow(_preferences, _appSettingsStore, _dataService, _fleet.All) { Owner = this };
        settings.ShowDialog();
        if (ShouldPersistViewerPreferences(settings.Result))
        {
            _preferences = settings.Result!;
            if (!_preferencesStore.Save(_preferences))
            {
                WarnSettingNotSaved("default time range and auto-refresh", _preferencesStore.FilePath);
            }
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
        /* The toast-settings read and the display-mode tab reload below are both fired unawaited, so they
           are in flight together whenever the mode changed. */
        using var readFanOut = ViewerReadFanOut.Of(2);
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

    /// <summary>
    /// Whether a <see cref="SettingsWindow"/> session's <see cref="SettingsWindow.Result"/> should be written
    /// to the viewer's local <see cref="ViewerPreferencesStore"/> (#2715). <see cref="SettingsWindow.Result"/>
    /// (Default Time Range + Auto-refresh interval) is a purely local, per-user preference — a small JSON file
    /// in the viewer's own app-data folder — entirely unrelated to the shared Postgres/TimescaleDB control-plane
    /// store the SAME Settings-window Save also writes (alert engine, notifications, service flags). The window
    /// builds <see cref="SettingsWindow.Result"/> BEFORE attempting that unrelated store write, so whether it is
    /// non-null depends only on the operator having clicked Save and it getting past the (unrelated) cleartext-
    /// webhook confirmation — never on whether the store write succeeded, and never on read-only status. Pure,
    /// so it is unit-testable without an STA window: the fix for #2715 is exactly this predicate replacing the
    /// old <c>ShowDialog() == true &amp;&amp; Result is not null</c> gate, whose <c>ShowDialog() == true</c> half
    /// tied a local-only preference's persistence to a read-only seat's <see cref="ViewerReadOnlyException"/> on
    /// the store write for completely unrelated settings.
    /// </summary>
    internal static bool ShouldPersistViewerPreferences(ViewerPreferences? result) => result is not null;

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

    /// <summary>
    /// The full-window message state. <paramref name="details"/> is the non-secret configuration block
    /// (#1954) rendered under the message; pass null only for a message that genuinely has no configuration
    /// context. Every connection/config failure goes through <see cref="ShowConnectionFailure"/> instead —
    /// pinned by <c>ViewerConfigDiagnosticsTests</c>, so a new failure branch cannot quietly ship without it.
    /// </summary>
    private void ShowMessage(string message, string? details)
    {
        MessageText.Text = message;
        MessageDetailsText.Text = details ?? "";
        MessageDetailsPanel.Visibility = string.IsNullOrWhiteSpace(details) ? Visibility.Collapsed : Visibility.Visible;
        MessageOverlay.Visibility = Visibility.Visible;
        StatusText.Text = "";
    }

    /// <summary>
    /// A connection or configuration failure: the message, plus the diagnostics that say which darling.json
    /// the viewer read and what it parsed out of it (#1954). The operator sees it in the window, so
    /// diagnosing "wrong file" vs "right file, wrong value" needs no log hunt at all; the log gets it too,
    /// flushed immediately because a stuck viewer is usually killed before the 5-second timer fires.
    /// </summary>
    private void ShowConnectionFailure(string message)
    {
        ViewerLogger.Error(ViewerConfigDiagnostics.LogSource, message);
        ViewerLogger.Flush();
        ShowMessage(message, _connectionDiagnostics);
    }

    /// <summary>Writes one diagnostic line per entry under the shared config tag, so an operator can grep one source.</summary>
    private static void LogDiagnostics(IReadOnlyList<string> lines)
    {
        foreach (var line in lines)
        {
            ViewerLogger.Info(ViewerConfigDiagnostics.LogSource, line);
        }
    }

    /// <summary>
    /// The layered store-connection self-test (#1954 bullet 3): DNS → TCP → TLS → auth → schema
    /// gate, against the SAME effective connection string the failed connect used, so the operator
    /// learns WHICH layer failed instead of re-reading one collapsed Npgsql message. The report
    /// appends under the configuration block (a re-run replaces the previous report rather than
    /// stacking) and goes to the log, flushed — the same treatment the failure itself gets. The
    /// probe core is shared and never throws; the catch here is a UI-thread belt-and-braces.
    /// </summary>
    private async void OnRunSelfTestClick(object sender, RoutedEventArgs e)
    {
        if (string.IsNullOrWhiteSpace(_storeConnectionString))
        {
            /* No connection string means config never resolved (e.g. darling.json missing) — there
               is nothing to probe, and the overlay message already says what to fix. */
            ViewerLogger.Warn(ViewerConfigDiagnostics.LogSource,
                "Self-test requested, but no store connection string resolved — fix the configuration first.");
            return;
        }

        RunSelfTestButton.IsEnabled = false;
        var originalContent = RunSelfTestButton.Content;
        RunSelfTestButton.Content = "Testing…";
        try
        {
            var results = await StoreConnectionSelfTest.RunAsync(_storeConnectionString);
            var report = StoreConnectionSelfTest.FormatReport(results);

            foreach (var line in report.Split('\n'))
            {
                ViewerLogger.Info(ViewerConfigDiagnostics.LogSource, line.TrimEnd());
            }

            ViewerLogger.Flush();

            /* Splice over a previous run's report (the header is the core's public seam), keep
               whatever configuration block was already showing above it. */
            var current = MessageDetailsText.Text ?? "";
            var previousReport = current.IndexOf(StoreConnectionSelfTest.ReportHeader, StringComparison.Ordinal);
            var baseText = (previousReport >= 0 ? current[..previousReport] : current).TrimEnd();
            MessageDetailsText.Text = baseText.Length == 0
                ? report
                : baseText + Environment.NewLine + Environment.NewLine + report;
            MessageDetailsPanel.Visibility = Visibility.Visible;
        }
        catch (Exception ex)
        {
            ViewerLogger.Error(ViewerConfigDiagnostics.LogSource, "Connection self-test failed to run", ex);
        }
        finally
        {
            RunSelfTestButton.Content = originalContent;
            RunSelfTestButton.IsEnabled = true;
        }
    }

    /// <summary>Copies the failure message plus the configuration block, so a bug report carries both.</summary>
    private void OnCopyDiagnosticsClick(object sender, RoutedEventArgs e)
    {
        try
        {
            Clipboard.SetText(string.Join(Environment.NewLine + Environment.NewLine, MessageText.Text, MessageDetailsText.Text));
        }
        catch (Exception ex)
        {
            /* Another process can hold the clipboard open; a failed copy must not take the window down. */
            ViewerLogger.Warn(ViewerConfigDiagnostics.LogSource, $"Copying the diagnostics failed: {ex.Message}");
        }
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
