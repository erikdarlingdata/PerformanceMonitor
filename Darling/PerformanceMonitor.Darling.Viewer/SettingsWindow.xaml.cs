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
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Navigation;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's full Settings window — a faithful, section-for-section port of Performance Monitor Lite's
/// <c>SettingsWindow</c>, now (Stage 3b) writing the CONTROL-PLANE store so the running Darling service honors
/// the operator's changes. The Notifications (alert engine + automated analysis), Email (SMTP), and Teams/Slack
/// sections write <c>config.config_alert_settings</c> + <c>config.config_notification</c>; the MCP toggle/port
/// and the global plan-capture flag write <c>config.config_service</c>; the Data Collection Pause/Resume button
/// drives a <c>pause</c>/<c>resume</c> command; and the Collection Schedule section opens the real per-server /
/// per-collector editor (<see cref="CollectorScheduleEditorWindow"/>) writing <c>config.config_collector_schedules</c>.
/// Each store write flows through <see cref="ViewerDataService"/> (the shared 42501 → <see
/// cref="ViewerReadOnlyException"/> gate); a read-only seat shows a banner and the writes degrade gracefully.
///
/// <para>The genuinely viewer-LOCAL preferences stay in <see cref="ViewerAppSettings"/> /
/// <see cref="ViewerPreferences"/>: the default time range + auto-refresh, connection timeout, CSV separator,
/// timestamp-display mode, tray-minimize, the connection-change/LRQ-noise/delivery-mode/mute-default/dismissal-
/// logging toggles the service has no honored column for, and the theme (dark-only). Send Test Email / Send Test
/// Notification stay working — they render + send straight from the live UI via the shared, connection-
/// independent renderers, so the operator verifies exactly what they typed before saving.</para>
/// </summary>
public partial class SettingsWindow : Window
{
    /// <summary>#6875: the Identity Capacity bars as loaded. The window shows only the switch, and a Save writes these back.</summary>
    private int _identityCapacityWarningPercent = 70;
    private int _identityCapacityCriticalPercent = 90;

    /// <summary>Branding the shared email/webhook renderers stamp on test messages (mirrors the service's).</summary>
    private static readonly AlertBranding s_branding = new("Performance Monitor Darling", null);

    private readonly ViewerAppSettingsStore _appSettingsStore;
    private readonly ViewerAppSettings _appSettings;
    private readonly ViewerDataService? _dataService;
    private readonly IReadOnlyList<DarlingServer> _servers;

    /// <summary>The notification row as the window loaded it: its stored SMTP password survives a Save that leaves the
    /// box blank, and its host, port, SSL flag and user name say whether a blank box is allowed (#5366).</summary>
    private NotificationRow _loadedNotification = NotificationRow.Defaults();

    /// <summary>The password the box was pre-filled with: a value saved before passwords were sealed that this machine
    /// could read. Null when the box was left blank (nothing saved, or a sealed value nobody here can read).</summary>

    /// <summary>The password typed for a save that seals it, held between reading the controls and the async key read.</summary>
    private string? _smtpPasswordToSeal;


    /// <summary>The service's paused state as last read from <c>config_service</c>, reflected on the button.</summary>
    private bool _paused;

    /// <summary>Guards the theme combo's SelectionChanged from firing an Apply while <see cref="LoadColorTheme"/>
    /// is seeding the selection.</summary>
    private bool _isLoadingTheme;

    /// <summary>The theme active when the window opened, so a Cancel/close-without-save reverts the live preview.</summary>
    private readonly string _originalTheme = ThemeManager.CurrentTheme;

    /// <summary>Set once the operator saves, so the close handlers do not revert the (now persisted) theme.</summary>
    private bool _themeSaved;

    /// <summary>
    /// True once the operator config was READ from the store without error. Save writes the store sections only
    /// when this is set — so a transient read failure (which leaves the controls showing defaults) can never
    /// clobber an operator-tuned store back to defaults. False when disconnected or a read threw.
    /// </summary>
    private bool _storeLoaded;

    /// <summary>
    /// The edited viewer preferences (default time range + auto-refresh), populated on a successful Save so
    /// <see cref="MainWindow"/> can refresh its in-memory copy that seeds newly-opened server tabs. Null until
    /// Save succeeds (Close without saving leaves it null).
    /// </summary>
    public ViewerPreferences? Result { get; private set; }

    /// <param name="preferences">Current viewer preferences (the toolbar-seed defaults) to seed the controls.</param>
    /// <param name="appSettingsStore">The store for the viewer-LOCAL preferences (operational settings now live in the control-plane store).</param>
    /// <param name="dataService">The control-plane connection; null when not connected yet (store surfaces disabled).</param>
    /// <param name="servers">The managed servers, for the collector-schedule editor's per-server scope.</param>
    public SettingsWindow(
        ViewerPreferences preferences,
        ViewerAppSettingsStore appSettingsStore,
        ViewerDataService? dataService,
        IReadOnlyList<DarlingServer>? servers = null)
    {
        ArgumentNullException.ThrowIfNull(preferences);
        ArgumentNullException.ThrowIfNull(appSettingsStore);

        InitializeComponent();

        _appSettingsStore = appSettingsStore;
        _appSettings = appSettingsStore.Load();
        SelectTabByName(_appSettings.SettingsWindowTab);
        _dataService = dataService;
        _servers = servers ?? Array.Empty<DarlingServer>();

        LoadViewerPreferences(preferences);
        UpdateCollectionStatus();
        LoadMcpSettings();
        UpdateMcpStatus();
        LoadWebSettings();
        UpdateWebStatus();
        LoadConnectionTimeout();
        LoadNocRefreshInterval();
        LoadCsvSeparator();
        LoadTimeDisplayMode();
        LoadColorTheme();
        LoadViewerLocalAlertFields();
        SeedAlertControlsFrom(AlertSettingsRow.Defaults());
        SeedNotificationControlsFrom(NotificationRow.Defaults());
        /* Default the global plan-capture flag to the store's default (TRUE) so an unread/unseeded state never
           shows it off — the store read overwrites it, and Save is guarded by _storeLoaded regardless. */
        CapturePlansCheckBox.IsChecked = true;
        QueryStoreBackfillCheckBox.IsChecked = true;
        QueryStoreTextBudgetMbTextBox.Text = "64";
        MaxConcurrentSweepsTextBox.Text = "4";
        QueryStoreFetchTopNTextBox.Text = "100";

        /* Manage Mute Rules writes the shared Postgres config; needs a live store connection. */
        ManageMuteRulesButton.IsEnabled = _dataService is not null;
        /* #3598: so do the notification routes — same store, same reason. */
        ManageNotificationRoutesButton.IsEnabled = _dataService is not null;
        EditSchedulesButton.IsEnabled = _dataService is not null;

        /* Read the authoritative operator config from the store (async), then apply read-only gating. */
        Loaded += async (_, _) => await LoadFromStoreAsync();
    }

    /// <summary>
    /// Overwrites the store-backed controls with the authoritative values from the control-plane store (the
    /// alert engine + analysis, SMTP + webhooks, MCP + plan capture, and the paused state), leaving the
    /// synchronous defaults in place if the store is unreachable or has not seeded a section yet. Then applies
    /// read-only gating. Runs once on load.
    /// </summary>
    private async Task LoadFromStoreAsync()
    {
        if (_dataService is not null)
        {
            /* Read the three store-backed sections INDEPENDENTLY (D7): a read-only viewer role is column-denied
               the config_notification secrets, so selecting them 42501s — and in a single shared try-block one
               such throw blanks every LATER section. GetNotificationAsync already falls back to a secret-free
               projection for a read-only seat, and this per-section isolation is the belt-and-suspenders that
               keeps any other single-section failure from blanking the rest. */
            var sections = await ReadStoreSectionsAsync(
                () => _dataService.GetAlertSettingsAsync(),
                () => _dataService.GetNotificationAsync(),
                () => _dataService.GetServiceConfigAsync());

            if (sections.Alert is not null)
            {
                SeedAlertControlsFrom(sections.Alert);
            }

            if (sections.Notification is not null)
            {
                SeedNotificationControlsFrom(sections.Notification);
                await RefreshGenericSigningKeyAsync();
            }

            if (sections.Service is not null)
            {
                CapturePlansCheckBox.IsChecked = sections.Service.CapturePlans;
                QueryStoreBackfillCheckBox.IsChecked = sections.Service.QueryStoreBackfillEnabled;
                QueryStoreTextBudgetMbTextBox.Text = sections.Service.QueryStoreTextBudgetMb.ToString(CultureInfo.InvariantCulture);
                MaxConcurrentSweepsTextBox.Text = sections.Service.MaxConcurrentSweeps.ToString(CultureInfo.InvariantCulture);
                QueryStoreFetchTopNTextBox.Text = sections.Service.QueryStoreFetchTopN.ToString(CultureInfo.InvariantCulture);
                McpEnabledCheckBox.IsChecked = sections.Service.McpEnabled;
                McpPortTextBox.Text = sections.Service.McpPort.ToString(CultureInfo.InvariantCulture);
                WebEnabledCheckBox.IsChecked = sections.Service.WebEnabled;
                WebPortTextBox.Text = sections.Service.WebPort.ToString(CultureInfo.InvariantCulture);
                _paused = sections.Service.Paused;
            }

            /* Save may write the store sections only when ALL THREE loaded. config_service is written LAST by
               the service (its presence marks the seed complete), and on a seeded store the alert/notification
               rows are present too — so requiring all three preserves the original anti-clobber guard while
               tolerating a per-section read failure: any missing section leaves this false so Save declines
               rather than overwriting a section it could not read with the on-screen defaults. (On a
               reachable-but-UNSEEDED store every read returns null → false, exactly as before.) */
            _storeLoaded = sections.Alert is not null && sections.Notification is not null && sections.Service is not null;
        }

        ApplyReadOnlyGating();
        UpdateCollectionStatus();
        UpdateMcpStatus();
        UpdateWebStatus();
        UpdateAlertControlStates();
        UpdateSmtpControlStates();
        UpdateTeamsControlStates();
        UpdateSlackControlStates();
    }

    /// <summary>
    /// Reads the three control-plane Settings sections (alert engine, notification, service flags)
    /// INDEPENDENTLY, so one section's failure — most importantly a read-only <c>viewer</c> seat's SQLSTATE
    /// 42501 on the secret-bearing <c>config_notification</c> columns — degrades only THAT section to its
    /// on-screen defaults instead of aborting a shared read and blanking the rest (D7). Each getter runs in its
    /// own try/catch; a thrown or null result leaves that section null and its synchronous defaults stand.
    /// Pure of WPF (the caller applies the results to controls), so it is unit-testable without a live store or
    /// an STA window.
    /// </summary>
    internal static async Task<StoreSections> ReadStoreSectionsAsync(
        Func<Task<AlertSettingsRow?>> getAlert,
        Func<Task<NotificationRow?>> getNotification,
        Func<Task<ServiceConfigRow?>> getService)
    {
        ArgumentNullException.ThrowIfNull(getAlert);
        ArgumentNullException.ThrowIfNull(getNotification);
        ArgumentNullException.ThrowIfNull(getService);

        return new StoreSections(
            await TryReadSectionAsync(getAlert),
            await TryReadSectionAsync(getNotification),
            await TryReadSectionAsync(getService));
    }

    /// <summary>Runs one section read, degrading any non-cancellation failure (a read-only 42501, a transient
    /// blip) to <c>null</c> so that section keeps its on-screen defaults; cancellation propagates.</summary>
    private static async Task<T?> TryReadSectionAsync<T>(Func<Task<T?>> read) where T : class
    {
        try
        {
            return await read();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"SettingsWindow: a control-plane section could not be read, keeping its defaults: {ex.Message}");
            return null;
        }
    }

    /// <summary>The three independently-loaded Settings store sections; a null member means that section could
    /// not be read (an unseeded store, or a per-section failure such as a read-only 42501) and its on-screen
    /// defaults stand.</summary>
    internal readonly record struct StoreSections(
        AlertSettingsRow? Alert, NotificationRow? Notification, ServiceConfigRow? Service);

    /// <summary>Reflects a read-only seat: a banner + the operator-action buttons disabled. The threshold
    /// inputs stay visible for reference; a Save attempt surfaces the friendly read-only message.</summary>
    // ── Tabs (#5846) ──

    /// <summary>The tab that holds <paramref name="element"/>, or null for the window-wide parts (the title, the
    /// read-only banner, Save and Close). Walks the LOGICAL tree: a tab that is not selected keeps its content as a
    /// logical child, while that content is out of the visual tree.</summary>
    internal static TabItem? TabOf(DependencyObject? element)
    {
        for (var node = element; node is not null; node = LogicalTreeHelper.GetParent(node))
        {
            if (node is TabItem tab)
            {
                return tab;
            }
        }
        return null;
    }

    /// <summary>Opens the tab holding <paramref name="element"/>, if it is on one.</summary>
    private void SelectTabOf(DependencyObject element)
    {
        if (TabOf(element) is { } tab)
        {
            SettingsTabs.SelectedItem = tab;
        }
    }

    /// <summary>Opens the first tab, in tab order, that holds a control with an invalid value, and returns it
    /// (null when no error sits on a tab). Tab order, not the order Save checked the fields in, so the operator
    /// starts from the left.</summary>
    internal TabItem? SelectFirstTabWithError(SettingsValidationErrors errors)
    {
        var first = errors.Sources
            .Select(TabOf)
            .OfType<TabItem>()
            .OrderBy(tab => SettingsTabs.Items.IndexOf(tab))
            .FirstOrDefault();
        if (first is not null)
        {
            SettingsTabs.SelectedItem = first;
        }
        return first;
    }

    /// <summary>Each message prefixed with the name of the tab its field is on.</summary>
    private static IEnumerable<string> DescribeErrors(SettingsValidationErrors errors) =>
        errors.Messages.Zip(errors.Sources, (message, source) =>
            TabOf(source) is { } tab ? $"{tab.Header}: {message}" : message);

    private void SelectTabByName(string? name)
    {
        var tab = SettingsTabs.Items.OfType<TabItem>().FirstOrDefault(t => t.Name == name);
        if (tab is not null)
        {
            SettingsTabs.SelectedItem = tab;
        }
    }

    private void SettingsTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        /* SelectionChanged bubbles: a ComboBox inside a tab raises it here too. Only the tab strip's own counts,
           and not while InitializeComponent is still building the window (the settings are not loaded yet). */
        if (!IsInitialized || !ReferenceEquals(e.OriginalSource, SettingsTabs) || SettingsTabs.SelectedItem is not TabItem tab)
        {
            return;
        }
        /* Kept on the in-memory copy too, so a Save (which writes that whole copy) carries the current tab. */
        _appSettings.SettingsWindowTab = tab.Name;
    }

    /// <summary>Writes the open tab to the viewer's own settings (#5846), the way the overview sort is kept: read
    /// the file fresh, change the one value, write it. Runs on close, after any Save, and only on a change.</summary>
    internal void RememberSelectedTab()
    {
        var current = (SettingsTabs.SelectedItem as TabItem)?.Name ?? "";
        var settings = _appSettingsStore.Load();
        if (settings.SettingsWindowTab == current)
        {
            return;
        }
        settings.SettingsWindowTab = current;
        /* A failed write only means the window opens on another tab next time; the store logs why. */
        _appSettingsStore.Save(settings);
    }

    private void ApplyReadOnlyGating()
    {
        var readOnly = _dataService?.IsReadOnly == true;
        SettingsReadOnlyBanner.Visibility = readOnly ? Visibility.Visible : Visibility.Collapsed;
        if (readOnly)
        {
            PauseResumeButton.IsEnabled = false;
        }
    }

    // ── Viewer preferences (folded in from #1401: default time range + auto-refresh) ──

    private void LoadViewerPreferences(ViewerPreferences preferences)
    {
        /* Normalize so an out-of-range persisted index can never leave a combo unselected; the combos mirror
           the toolbar's item order, so the stored index selects the matching row directly. */
        var normalized = new ViewerPreferences
        {
            DefaultTimeRangeIndex = preferences.DefaultTimeRangeIndex,
            DefaultTimeRange = preferences.DefaultTimeRange,
            AutoRefreshEnabled = preferences.AutoRefreshEnabled,
            AutoRefreshIntervalIndex = preferences.AutoRefreshIntervalIndex,
        }.Normalize();

        DefaultTimeRangeCombo.ZoneProvider = ViewerTimeHelper.CurrentDisplayZone;
        DefaultTimeRangeCombo.Value = ViewerTimeRangeWindow.DefaultFor(normalized);
        _lastPersistableDefaultRange = DefaultTimeRangeCombo.Value;
        AutoRefreshCheckBox.IsChecked = normalized.AutoRefreshEnabled;
        AutoRefreshIntervalCombo.SelectedIndex = normalized.AutoRefreshIntervalIndex;
        AutoRefreshIntervalCombo.IsEnabled = normalized.AutoRefreshEnabled;
    }

    /// <summary>A fixed or since range cannot be a standing default (#5562): it stays on the picker's last rolling or period
    /// pick, so Save never writes concrete dates.</summary>
    private void DefaultTimeRange_RangeChanged(object? sender, TimeRangeChangedEventArgs e)
    {
        if (ViewerTimeRangeWindow.IsPersistable(e.Spec))
        {
            _lastPersistableDefaultRange = e.Spec;
        }
        else
        {
            DefaultTimeRangeCombo.Value = _lastPersistableDefaultRange;
        }
    }

    private TimeRangeSpec _lastPersistableDefaultRange = TimeRangePresets.Find("1d")!;

    private ViewerPreferences BuildViewerPreferences() => new ViewerPreferences
    {
        DefaultTimeRange = ViewerTimeRangeWindow.IsPersistable(DefaultTimeRangeCombo.Value)
            ? DefaultTimeRangeCombo.Value.Id
            : _lastPersistableDefaultRange.Id,
        AutoRefreshEnabled = AutoRefreshCheckBox.IsChecked == true,
        AutoRefreshIntervalIndex = AutoRefreshIntervalCombo.SelectedIndex,
    }.Normalize();

    private void AutoRefreshCheckBox_Toggled(object sender, RoutedEventArgs e)
    {
        /* Guard the load-time raise before the combo exists (Checked can fire during InitializeComponent). */
        if (AutoRefreshIntervalCombo is not null)
        {
            AutoRefreshIntervalCombo.IsEnabled = AutoRefreshCheckBox.IsChecked == true;
        }
    }

    // ── Data Collection (pause/resume drives the service via the command plane) ──

    private void UpdateCollectionStatus()
    {
        if (_dataService is null)
        {
            CollectionStatusText.Text = "Status: not connected to the Darling store";
            PauseResumeButton.IsEnabled = false;
            PauseResumeButton.Content = "Pause Co_llection";
            return;
        }

        CollectionStatusText.Text = _paused
            ? "Status: Paused — the Darling service is not collecting"
            : "Status: Collecting — managed by the Darling service";
        /* The "_" keeps Alt+L alive across the state swap — see the XAML for why the key is on
           "Collection" (the word both states share) rather than on the verb. */
        PauseResumeButton.Content = _paused ? "Resume Co_llection" : "Pause Co_llection";
        PauseResumeButton.IsEnabled = !_dataService.IsReadOnly;
    }

    /// <summary>Enqueues a <c>pause</c>/<c>resume</c> command, waits for the result, then reflects the new
    /// state. A read-only seat can't command the service (the button is disabled; a stray click no-ops).</summary>
    private async void PauseResumeButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || _dataService.IsReadOnly)
        {
            return;
        }

        var pausing = !_paused;
        PauseResumeButton.IsEnabled = false;
        PauseResumeButton.Content = pausing ? "Pausing..." : "Resuming...";
        /* The pause waits for an agent change in progress to finish, up to a minute (#5632). */
        CollectionStatusText.Text = "Status: waiting for an agent change in progress to finish, if there is one...";
        try
        {
            var result = pausing
                ? await _dataService.PauseServiceAsync()
                : await _dataService.ResumeServiceAsync();

            if (result is null)
            {
                CollectionStatusText.Text = "Status: the service is running the command — check again later";
            }
            else if (result.Status == ViewerDataService.StatusSucceeded)
            {
                _paused = pausing;
            }
            else
            {
                MessageBox.Show(
                    $"The service could not {(pausing ? "pause" : "resume")} collection: {result.ResultStatus}",
                    "Data Collection", MessageBoxButton.OK, MessageBoxImage.Warning);
            }
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
            MessageBox.Show($"Could not reach the service: {ex.Message}", "Data Collection", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            UpdateCollectionStatus();
        }
    }

    // ── MCP server (config_service.mcp_enabled / mcp_port) ──

    private void LoadMcpSettings()
    {
        /* Seed from the last-known viewer copy for an immediate, non-blank UI; the store read overwrites it. */
        McpEnabledCheckBox.IsChecked = _appSettings.McpEnabled;
        McpPortTextBox.Text = _appSettings.McpPort.ToString(CultureInfo.InvariantCulture);
    }

    private void UpdateMcpStatus()
    {
        McpStatusText.Text = McpEnabledCheckBox.IsChecked == true
            ? "The Darling service hosts the MCP server; it applies on the service's next reload."
            : "Status: Disabled";
    }

    // ── Web dashboard (config_service.web_enabled / web_port), #1562 — mirrors the MCP section above ──

    private void LoadWebSettings()
    {
        /* Seed from the last-known viewer copy for an immediate, non-blank UI; the store read overwrites it. */
        WebEnabledCheckBox.IsChecked = _appSettings.WebEnabled;
        WebPortTextBox.Text = _appSettings.WebPort.ToString(CultureInfo.InvariantCulture);
    }

    private void UpdateWebStatus()
    {
        WebStatusText.Text = WebEnabledCheckBox.IsChecked == true
            ? "The Darling service hosts the web dashboard; it applies on the service's next reload."
            : "Status: Disabled";
    }

    /// <summary>Validates the MCP + web dashboard ports and the (always-valid) capture-plans + enabled flags for
    /// the <c>config_service</c> write. Returns false when an ENABLED surface has a bad port (a disabled surface
    /// with a bad port keeps its last-known value and does not block the save).</summary>
    private bool TryReadServiceFlags(
        out bool capturePlans, out bool mcpEnabled, out int mcpPort, out bool webEnabled, out int webPort,
        out int textBudgetMb, out int maxSweeps, out int fetchTopN)
    {
        capturePlans = CapturePlansCheckBox.IsChecked == true;
        textBudgetMb = 64;
        maxSweeps = 4;
        fetchTopN = 100;
        mcpEnabled = McpEnabledCheckBox.IsChecked == true;
        mcpPort = _appSettings.McpPort;
        webEnabled = WebEnabledCheckBox.IsChecked == true;
        webPort = _appSettings.WebPort;

        if (int.TryParse(McpPortTextBox.Text, out var parsedMcpPort) && parsedMcpPort is >= 1024 and <= 65535)
        {
            mcpPort = parsedMcpPort;
        }
        else if (mcpEnabled)
        {
            SelectTabOf(McpPortTextBox);
            MessageBox.Show(
                "MCP port must be between 1024 and 65535.\nPorts 0-1023 are well-known privileged ports reserved by the operating system.",
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        if (int.TryParse(WebPortTextBox.Text, out var parsedWebPort) && parsedWebPort is >= 1024 and <= 65535)
        {
            webPort = parsedWebPort;
        }
        else if (webEnabled)
        {
            SelectTabOf(WebPortTextBox);
            MessageBox.Show(
                "Web dashboard port must be between 1024 and 65535.\nPorts 0-1023 are well-known privileged ports reserved by the operating system.",
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        /* #2164 / #2170 collector memory knobs. Unlike the ports these are always in force (there is no
           "disabled" state to excuse a bad value), so a bad entry always blocks the save rather than
           silently keeping a last-known value. The service clamps on read as defense in depth. */
        if (int.TryParse(QueryStoreTextBudgetMbTextBox.Text, out var parsedBudget) && ServiceFlagLimits.TextBudgetMbError(parsedBudget) is null)
        {
            textBudgetMb = parsedBudget;
        }
        else
        {
            SelectTabOf(QueryStoreTextBudgetMbTextBox);
            MessageBox.Show(
                ServiceFlagLimits.TextBudgetMbMessage,
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        if (int.TryParse(MaxConcurrentSweepsTextBox.Text, out var parsedSweeps) && ServiceFlagLimits.ConcurrentSweepsError(parsedSweeps) is null)
        {
            maxSweeps = parsedSweeps;
        }
        else
        {
            SelectTabOf(MaxConcurrentSweepsTextBox);
            MessageBox.Show(
                ServiceFlagLimits.ConcurrentSweepsMessage,
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        /* #6211: plans per database-hour and metric whose text and plan the Query Store collector fetches. 0 means every new plan, so the floor
           is 0; the service clamps on read as defense in depth. */
        if (int.TryParse(QueryStoreFetchTopNTextBox.Text, out var parsedTopN) && ServiceFlagLimits.FetchTopNError(parsedTopN) is null)
        {
            fetchTopN = parsedTopN;
        }
        else
        {
            SelectTabOf(QueryStoreFetchTopNTextBox);
            MessageBox.Show(
                ServiceFlagLimits.FetchTopNMessage,
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        return true;
    }

    private void CopyMcpCommandButton_Click(object sender, RoutedEventArgs e)
    {
        var port = McpPortTextBox.Text;
        /* The same text the service's --enable-mcp prints (#6474). The service needs the MCP token on loopback too
           unless mcp.allowTokenlessLoopback is on, so the command carries the header with a placeholder; the token
           itself never reaches the clipboard from here. */
        var command = DarlingEndpointClientText.ClaudeMcpAddCommand(port);
        /* SetDataObject with copy=false avoids WPF's problematic Clipboard.Flush(). */
        McpStatusText.Text = ClipboardText.TrySetDataObject(command)
            ? "Copied to clipboard! Replace <token> with the MCP token: --print-mcp-token on the service host prints it. If the service's MCP network listener has a tls block, change http to https in the copied command."
            : "Couldn't copy: the clipboard is in use.";
    }

    private void AutoPortButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            McpPortTextBox.Text = FindFreeTcpPort().ToString(CultureInfo.InvariantCulture);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not find an available port: {ex.Message}",
                "Error", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private void WebAutoPortButton_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            WebPortTextBox.Text = FindFreeTcpPort().ToString(CultureInfo.InvariantCulture);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Could not find an available port: {ex.Message}",
                "Error", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private void CopyWebUrlButton_Click(object sender, RoutedEventArgs e)
    {
        var url = $"http://localhost:{WebPortTextBox.Text}/";
        /* SetDataObject with copy=false avoids WPF's problematic Clipboard.Flush(). */
        WebStatusText.Text = ClipboardText.TrySetDataObject(url)
            ? "Copied to clipboard!"
            : "Couldn't copy: the clipboard is in use.";
    }

    /// <summary>Asks the OS for a free loopback TCP port (bind to port 0, read the assignment, release).</summary>
    private static int FindFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    // ── Collection schedule (the real per-server / per-collector editor) ──

    private void EditSchedulesButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        var editor = new CollectorScheduleEditorWindow(_dataService, _servers) { Owner = this };
        editor.ShowDialog();
    }

    private void ConfigureDatabaseStatesButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        var editor = new DatabaseStateOverridesWindow(_dataService, _servers) { Owner = this };
        editor.ShowDialog();
    }

    // ── Viewer defaults (connection timeout, CSV separator, timestamp display) — viewer-local ──

    private void LoadConnectionTimeout() =>
        ConnectionTimeoutBox.Text = _appSettings.ConnectionTimeoutSeconds.ToString(CultureInfo.InvariantCulture);

    private void SaveConnectionTimeout()
    {
        if (int.TryParse(ConnectionTimeoutBox.Text, out var timeout) && timeout is >= 5 and <= 60)
        {
            _appSettings.ConnectionTimeoutSeconds = timeout;
        }
    }

    /// <summary>Selects the combo row whose Tag matches the persisted fleet-refresh interval; falls back to the
    /// 30-second default when the stored value isn't one of the presets.</summary>
    private void LoadNocRefreshInterval()
    {
        foreach (ComboBoxItem item in NocRefreshIntervalCombo.Items)
        {
            if (item.Tag is string tag
                && int.TryParse(tag, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds)
                && seconds == _appSettings.NocRefreshIntervalSeconds)
            {
                NocRefreshIntervalCombo.SelectedItem = item;
                break;
            }
        }

        if (NocRefreshIntervalCombo.SelectedItem == null)
        {
            NocRefreshIntervalCombo.SelectedIndex = 1; // 30 seconds (the default)
        }
    }

    private void SaveNocRefreshInterval()
    {
        if (NocRefreshIntervalCombo.SelectedItem is ComboBoxItem { Tag: string tag }
            && int.TryParse(tag, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds))
        {
            _appSettings.NocRefreshIntervalSeconds = seconds;
        }
    }

    private void LoadCsvSeparator()
    {
        foreach (ComboBoxItem item in CsvSeparatorCombo.Items)
        {
            if (item.Tag?.ToString() == _appSettings.CsvSeparator)
            {
                CsvSeparatorCombo.SelectedItem = item;
                break;
            }
        }
        if (CsvSeparatorCombo.SelectedItem == null)
        {
            CsvSeparatorCombo.SelectedIndex = 0;
        }
    }

    private void SaveCsvSeparator()
    {
        if (CsvSeparatorCombo.SelectedItem is ComboBoxItem { Tag: string sep })
        {
            _appSettings.CsvSeparator = sep;
        }
    }

    private void LoadTimeDisplayMode()
    {
        foreach (ComboBoxItem item in TimeDisplayModeCombo.Items)
        {
            if (item.Tag?.ToString() == _appSettings.TimeDisplayMode)
            {
                TimeDisplayModeCombo.SelectedItem = item;
                break;
            }
        }
        if (TimeDisplayModeCombo.SelectedItem == null)
        {
            TimeDisplayModeCombo.SelectedIndex = 0;
        }
    }

    private void SaveTimeDisplayMode()
    {
        if (TimeDisplayModeCombo.SelectedItem is ComboBoxItem { Tag: string mode })
        {
            _appSettings.TimeDisplayMode = mode;
        }
    }

    // ── Color theme (viewer-local; live-previewed via the shared ThemeManager, mirrors Lite) ──

    /// <summary>Live-applies the picked theme so the operator sees Dark / Light / CoolBreeze immediately.
    /// A Cancel/close-without-save reverts to <see cref="_originalTheme"/>; Save persists it.</summary>
    private void ColorThemeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_isLoadingTheme)
        {
            return;
        }

        if (ColorThemeCombo.SelectedItem is ComboBoxItem { Tag: string theme })
        {
            ThemeManager.Apply(theme);
        }
    }

    private void LoadColorTheme()
    {
        _isLoadingTheme = true;
        foreach (ComboBoxItem item in ColorThemeCombo.Items)
        {
            if (item.Tag?.ToString() == _appSettings.ColorTheme)
            {
                ColorThemeCombo.SelectedItem = item;
                break;
            }
        }
        if (ColorThemeCombo.SelectedItem == null)
        {
            ColorThemeCombo.SelectedIndex = 0;
        }
        _isLoadingTheme = false;
    }

    private void SaveColorTheme()
    {
        if (ColorThemeCombo.SelectedItem is ComboBoxItem { Tag: string theme })
        {
            _appSettings.ColorTheme = theme;
            ThemeManager.Apply(theme);
        }
        _themeSaved = true;
    }

    // ── Notifications / alert thresholds ──

    /// <summary>Seeds the viewer-LOCAL alert fields (no honored store column: tray minimize, mute-rule default,
    /// dismissal logging).</summary>
    private void LoadViewerLocalAlertFields()
    {
        MinimizeToTrayCheckBox.IsChecked = _appSettings.MinimizeToTray;
        /* AlertDeliveryMode/PerEventMax (#1141/#1236), the long-running-query read shape (max-results + the five
           noise filters), and connection-change notify (V20) moved to the STORE-backed controls
           (SeedAlertControlsFrom / BuildAlertRowFromControls) now that the service honors them; no longer viewer-local. */
        MuteRuleDefaultExpirationCombo.SelectedIndex = _appSettings.MuteRuleDefaultExpiration switch
        {
            "1 hour" => 0,
            "24 hours" => 1,
            "7 days" => 2,
            _ => 3
        };
        LogAlertDismissalsCheckBox.IsChecked = _appSettings.LogAlertDismissals;
    }

    /// <summary>Seeds the STORE-backed alert + analysis controls from a <see cref="AlertSettingsRow"/> (the
    /// store's authoritative values, or defaults before the store read completes).</summary>
    private void SeedAlertControlsFrom(AlertSettingsRow r)
    {
        AlertsEnabledCheckBox.IsChecked = r.Enabled;
        /* V20: connection-change notify is store-backed now (the service gates the connect edge on it). */
        NotifyConnectionCheckBox.IsChecked = r.NotifyConnectionChanges;
        /* #1659 (V33): the two connection opt-ins ride the same row. */
        NotifyConnectionDownAtStartupCheckBox.IsChecked = r.NotifyConnectionDownAtStartup;
        ConnectionRefireMinutesBox.Text = r.ConnectionRefireMinutes.ToString(CultureInfo.InvariantCulture);
        /* #991 (V35): the Availability Group family's master switch and its two sync-behind triggers. */
        NotifyAgHealthCheckBox.IsChecked = r.NotifyAgHealth;
        AgLagAlertSecondsBox.Text = r.AgLagAlertSeconds.ToString(CultureInfo.InvariantCulture);
        AgRedoQueueAlertKbBox.Text = r.AgRedoQueueAlertKb.ToString(CultureInfo.InvariantCulture);
        AgDisconnectRefireMinutesBox.Text = r.AgDisconnectRefireMinutes.ToString(CultureInfo.InvariantCulture);
        AlertCpuCheckBox.IsChecked = r.CpuEnabled;
        AlertCpuThresholdBox.Text = r.CpuThresholdPercent.ToString(CultureInfo.InvariantCulture);
        AlertCpuModeBox.SelectedIndex = ViewerDataService.MapCpuModeFromStore(r.CpuMode) == "SqlOnly" ? 1 : 0;
        AlertBlockingCheckBox.IsChecked = r.BlockingEnabled;
        AlertBlockingThresholdBox.Text = r.BlockingCountThreshold.ToString(CultureInfo.InvariantCulture);
        AlertBlockingWaitSecondsBox.Text = r.BlockingWaitSecondsThreshold.ToString(CultureInfo.InvariantCulture);
        /* #3444 (V122): the PostgreSQL count gates, prefilled beside their SQL Server twins. Separate
           boxes because they are separate columns — see the V122 rung for why one number cannot serve
           both engines. Both follow AlertBlockingCheckBox/AlertDeadlockCheckBox, which govern the
           condition on either engine. */
        AlertPgBlockingThresholdBox.Text = r.PgBlockingCountThreshold.ToString(CultureInfo.InvariantCulture);
        AlertDeadlockCheckBox.IsChecked = r.DeadlockEnabled;
        AlertDeadlockThresholdBox.Text = r.DeadlockCountThreshold.ToString(CultureInfo.InvariantCulture);
        AlertPgDeadlockThresholdBox.Text = r.PgDeadlockCountThreshold.ToString(CultureInfo.InvariantCulture);
        AlertPoisonWaitCheckBox.IsChecked = r.PoisonWaitEnabled;
        AlertPoisonWaitThresholdBox.Text = r.PoisonWaitThresholdMs.ToString(CultureInfo.InvariantCulture);
        AlertRunnableQueueCheckBox.IsChecked = r.RunnableQueueEnabled;
        AlertRunnableQueueWarningBox.Text = r.RunnableQueueWarningPerScheduler.ToString(CultureInfo.InvariantCulture);
        AlertRunnableQueueCriticalBox.Text = r.RunnableQueueCriticalPerScheduler.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsCheckBox.IsChecked = r.ClientTimeoutsEnabled;
        AlertClientTimeoutsWarningBox.Text = r.ClientTimeoutsWarningPerMinute.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsCriticalBox.Text = r.ClientTimeoutsCriticalPerMinute.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsWarningMultipleBox.Text = r.ClientTimeoutsWarningMultiple.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsCriticalMultipleBox.Text = r.ClientTimeoutsCriticalMultiple.ToString(CultureInfo.InvariantCulture);
        AlertLongRunningQueryCheckBox.IsChecked = r.LongRunningQueryEnabled;
        AlertLongRunningQueryThresholdBox.Text = r.LongRunningQueryThresholdMinutes.ToString(CultureInfo.InvariantCulture);
        /* V20: the long-running-query read shape (max-results + the five noise filters) is store-backed now. */
        AlertLongRunningQueryMaxResultsBox.Text = r.LongRunningQueryMaxResults.ToString(CultureInfo.InvariantCulture);
        LrqExcludeSpServerDiagnosticsCheckBox.IsChecked = r.LongRunningQueryExcludeSpServerDiagnostics;
        LrqExcludeWaitForCheckBox.IsChecked = r.LongRunningQueryExcludeWaitFor;
        LrqExcludeBackupsCheckBox.IsChecked = r.LongRunningQueryExcludeBackups;
        LrqExcludeMiscWaitsCheckBox.IsChecked = r.LongRunningQueryExcludeMiscWaits;
        LrqExcludeCdcCheckBox.IsChecked = r.LongRunningQueryExcludeCdc;
        AlertLrqExcludedProgramNamePrefixesBox.Text = string.Join(", ", r.LongRunningQueryExcludedProgramNamePrefixes);
        AlertLrqExcludedLoginsBox.Text = string.Join(", ", r.LongRunningQueryExcludedLogins);
        AlertExcludedDatabasesBox.Text = SettingsListText.Join(r.ExcludedDatabases);
        AlertExcludedVolumesBox.Text = SettingsListText.Join(r.ExcludedVolumes);
        AlertTempDbSpaceCheckBox.IsChecked = r.TempDbSpaceEnabled;
        AlertTempDbSpaceThresholdBox.Text = r.TempDbSpaceThresholdPercent.ToString(CultureInfo.InvariantCulture);
        AlertLowDiskCheckBox.IsChecked = r.LowDiskEnabled;
        AlertLowDiskThresholdPercentBox.Text = r.LowDiskThresholdPercent.ToString(CultureInfo.InvariantCulture);
        AlertLowDiskThresholdGbBox.Text = r.LowDiskThresholdGb.ToString(CultureInfo.InvariantCulture);
        AlertDiskCriticalPercentBox.Text = r.DiskCriticalFreePercent.ToString(CultureInfo.InvariantCulture);
        AlertDiskCriticalGbBox.Text = r.DiskCriticalFreeGb.ToString(CultureInfo.InvariantCulture);
        AlertSelfDiskWarnPercentBox.Text = r.SelfDiskFreeWarnPercent.ToString(CultureInfo.InvariantCulture);
        AlertSelfDiskWarnGbBox.Text = r.SelfDiskFreeWarnGb.ToString(CultureInfo.InvariantCulture);
        AlertCollectionStaleMinutesBox.Text = r.CollectionStaleMinutes.ToString(CultureInfo.InvariantCulture);
        AlertCollectionFailureThresholdBox.Text = r.CollectionFailureThreshold.ToString(CultureInfo.InvariantCulture);
        AlertStoreJobCadenceWarnPercentBox.Text = r.StoreJobCadenceWarnPercent.ToString(CultureInfo.InvariantCulture);
        /* #3297: one decimal, matching how the alert itself renders the ratio ("held at 4.5x"), so the box
           and the alert text agree about what the number looks like. */
        AlertRetentionHoldWarnRatioBox.Text = r.RetentionHoldWarnRatio.ToString("0.0", CultureInfo.InvariantCulture);
        AlertRetentionHoldCriticalRatioBox.Text = r.RetentionHoldCriticalRatio.ToString("0.0", CultureInfo.InvariantCulture);
        /* #3368: one decimal, matching how both cards render the rate beside the count, so the box and the
           card agree about what the number looks like. */
        BandDeadlockWarnPerHourBox.Text = r.DeadlockWarnPerHour.ToString("0.0", CultureInfo.InvariantCulture);
        BandDeadlockCriticalPerHourBox.Text = r.DeadlockCriticalPerHour.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingWarnPerHourBox.Text = r.BlockingWarnPerHour.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingCriticalPerHourBox.Text = r.BlockingCriticalPerHour.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingWarnWaitSecondsBox.Text = r.BlockingWarnWaitSeconds.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingCriticalWaitSecondsBox.Text = r.BlockingCriticalWaitSeconds.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingWarnSnapshotWaitSecondsBox.Text = r.BlockingWarnSnapshotWaitSeconds.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingCriticalSnapshotWaitSecondsBox.Text = r.BlockingCriticalSnapshotWaitSeconds.ToString("0.0", CultureInfo.InvariantCulture);
        AlertPvsCheckBox.IsChecked = r.PvsEnabled;
        AlertPvsThresholdPercentBox.Text = r.PvsThresholdPercent.ToString(CultureInfo.InvariantCulture);
        AlertPvsFloorGbBox.Text = r.PvsFloorGb.ToString(CultureInfo.InvariantCulture);
        AlertFileGrowthCheckBox.IsChecked = r.FileGrowthEnabled;
        AlertFileGrowthRiseMbBox.Text = r.FileGrowthRiseMb.ToString(CultureInfo.InvariantCulture);
        AlertFileGrowthVolumePercentBox.Text = r.FileGrowthVolumePercent.ToString(CultureInfo.InvariantCulture);
        AlertFileGrowthLookbackMinutesBox.Text = r.FileGrowthLookbackMinutes.ToString(CultureInfo.InvariantCulture);
        AlertLongRunningJobCheckBox.IsChecked = r.LongRunningJobEnabled;
        AlertLongRunningJobMultiplierBox.Text = r.LongRunningJobMultiplier.ToString(CultureInfo.InvariantCulture);
        AlertFailedJobCheckBox.IsChecked = r.FailedJobEnabled;
        AlertFailedJobLookbackBox.Text = r.FailedJobLookbackMinutes.ToString(CultureInfo.InvariantCulture);
        AlertDatabaseStateCheckBox.IsChecked = r.DatabaseStateEnabled;
        AlertAgentAlertGapsCheckBox.IsChecked = r.AgentAlertGapsEnabled;
        AlertBackupOverdueCheckBox.IsChecked = r.BackupOverdueEnabled;
        AlertLogBackupOverdueCheckBox.IsChecked = r.LogBackupOverdueEnabled;
        AlertCheckDbOverdueCheckBox.IsChecked = r.CheckDbOverdueEnabled;
        AlertIdentityCapacityCheckBox.IsChecked = r.IdentityCapacityEnabled;
        /* #6875: the two bars have no box here (update_alert_settings and the threshold overrides set them), so a Save carries the loaded values back unchanged. */
        _identityCapacityWarningPercent = r.IdentityCapacityWarningPercent;
        _identityCapacityCriticalPercent = r.IdentityCapacityCriticalPercent;
        AlertCooldownBox.Text = r.CooldownMinutes.ToString(CultureInfo.InvariantCulture);
        AnalysisEnabledCheckBox.IsChecked = r.AnalysisEnabled;
        AnalysisIntervalBox.Text = r.AnalysisIntervalMinutes.ToString(CultureInfo.InvariantCulture);
        AnalysisNotificationsCheckBox.IsChecked = r.AnalysisNotificationsEnabled;
        AnalysisNotifySeverityBox.Text = r.AnalysisNotifySeverity.ToString("0.0", CultureInfo.InvariantCulture);
        AnalysisNotifyCooldownBox.Text = r.AnalysisNotifyCooldownMinutes.ToString(CultureInfo.InvariantCulture);
        /* #3712 (V137): the route knob's store half, a tri-state -- the combo's Tag is the stored wire spelling
           ('digest' / 'page') or empty for NULL ("use the service's darling.json value"), which is what every store
           reads after the upgrade and what a store nobody has touched here prefills as. Parsed through the same
           FindingRouting.TryParseRoute the service applies, so a row value the service would ignore (impossible under
           the CHECK) lands on the NULL item rather than on a wrong route. */
        SelectAnalysisUncorroboratedRoute(r.AnalysisUncorroboratedRoute);
        /* #3466 (V124): the fleet sweep's own switch and cadence, prefilled beside Automated Analysis —
           the other scheduled whole-fleet evaluation — and deliberately OUTSIDE the master-toggle
           enable/disable group: sweeps keep running under a fleet-wide mute by contract. */
        FleetSweepEnabledCheckBox.IsChecked = r.FleetSweepEnabled;
        FleetSweepIntervalBox.Text = r.FleetSweepIntervalMinutes.ToString(CultureInfo.InvariantCulture);
        /* #7041 (V232): the Daily Health Report, outside the master-toggle group like the fleet sweep. A report that is off with no zone yet
           starts at this machine's zone, the way the web editor starts at the browser's: the person setting it up is most likely in the
           zone they want. A saved zone, or a blank on a report that is already on, is left as it is. */
        DailyReportEnabledCheckBox.IsChecked = r.DailyReportEnabled;
        DailyReportSendAtBox.Text = r.DailyReportSendAt;
        DailyReportTimeZoneBox.Text = r.DailyReportTimeZone ?? (r.DailyReportEnabled ? "" : ViewerDataService.DefaultKnownPatternTimeZone());
        DailyReportTitleBox.Text = r.DailyReportTitle ?? "";
        /* #1141/#1236: the delivery mode + per-event cap are now STORE-backed (the service honors them),
           seeded from the row like every other alert-engine control. */
        AlertDeliveryModeBox.SelectedIndex = r.DeliveryMode == "PerEvent" ? 1 : 0;
        AlertPerEventMaxBox.Text = r.PerEventMax.ToString(CultureInfo.InvariantCulture);
        UpdateAlertControlStates();
    }

    /// <summary>Parses one blocking band box (#6927): InvariantCulture, within the named bounds the classifier clamps to and the MCP writer
    /// accepts. A bad value adds an error naming the box and the range, and leaves <paramref name="value"/> null.</summary>
    private static void BlockingBandBox(
        System.Windows.Controls.TextBox box, double min, double max, string label, SettingsValidationErrors errors, out double? value)
    {
        if (double.TryParse(box.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) && parsed >= min && parsed <= max)
        {
            value = parsed;
            return;
        }

        value = null;
        errors.Add(
            $"{label} must be a number between {min.ToString("0.0", CultureInfo.InvariantCulture)} and {max.ToString("0.0", CultureInfo.InvariantCulture)}.",
            box);
    }

    /// <summary>Parses one optional blocking band box (#7028): 0 turns the tier off, otherwise the named bounds the classifier clamps to.
    /// A value between 0 and the floor is refused, because the clamp would rewrite it to the floor and the setting would not stick.</summary>
    private static void BlockingBandOptionalBox(
        System.Windows.Controls.TextBox box, double min, double max, string label, SettingsValidationErrors errors, out double? value)
    {
        if (double.TryParse(box.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) && (parsed == 0 || (parsed >= min && parsed <= max)))
        {
            value = parsed;
            return;
        }

        value = null;
        errors.Add(
            $"{label} must be 0 (that tier is off) or a number between {min.ToString("0.0", CultureInfo.InvariantCulture)} and {max.ToString("0.0", CultureInfo.InvariantCulture)}.",
            box);
    }

    /// <summary>Builds the store row from the alert + analysis controls, validating the numeric fields. Adds
    /// range errors to <paramref name="errors"/>; a bad field keeps its default rather than throwing.</summary>
    internal AlertSettingsRow BuildAlertRowFromControls(SettingsValidationErrors errors)
    {
        var row = new AlertSettingsRow
        {
            Enabled = AlertsEnabledCheckBox.IsChecked == true,
            NotifyConnectionChanges = NotifyConnectionCheckBox.IsChecked == true,
            NotifyConnectionDownAtStartup = NotifyConnectionDownAtStartupCheckBox.IsChecked == true,
            ConnectionRefireMinutes = int.TryParse(ConnectionRefireMinutesBox.Text, out var refire)
                ? Math.Clamp(refire, 0, 1440) : 0,
            NotifyAgHealth = NotifyAgHealthCheckBox.IsChecked == true,
            /* Clamped to the same range DarlingAlertSettings clamps on read, so the stored row and the
               service's effective value can never disagree. An unparseable box falls back to the DDL default
               rather than to 0, which for the lag trigger would mean silently DISABLING it. */
            AgLagAlertSeconds = int.TryParse(AgLagAlertSecondsBox.Text, out var agLag)
                ? Math.Clamp(agLag, 0, 86400) : 300,
            AgRedoQueueAlertKb = long.TryParse(AgRedoQueueAlertKbBox.Text, out var agRedo)
                ? Math.Clamp(agRedo, 0L, 1073741824L) : 0L,
            AgDisconnectRefireMinutes = int.TryParse(AgDisconnectRefireMinutesBox.Text, out var agRefire)
                ? Math.Clamp(agRefire, 0, 1440) : 0,
            CpuEnabled = AlertCpuCheckBox.IsChecked == true,
            CpuMode = ViewerDataService.MapCpuModeToStore((AlertCpuModeBox.SelectedItem as ComboBoxItem)?.Tag?.ToString() ?? "Total"),
            BlockingEnabled = AlertBlockingCheckBox.IsChecked == true,
            DeadlockEnabled = AlertDeadlockCheckBox.IsChecked == true,
            PoisonWaitEnabled = AlertPoisonWaitCheckBox.IsChecked == true,
            RunnableQueueEnabled = AlertRunnableQueueCheckBox.IsChecked == true,
            ClientTimeoutsEnabled = AlertClientTimeoutsCheckBox.IsChecked == true,
            LongRunningQueryEnabled = AlertLongRunningQueryCheckBox.IsChecked == true,
            /* V20 long-running-query read shape (the five noise-filter opt-outs; max-results parsed below). */
            LongRunningQueryExcludeSpServerDiagnostics = LrqExcludeSpServerDiagnosticsCheckBox.IsChecked == true,
            LongRunningQueryExcludeWaitFor = LrqExcludeWaitForCheckBox.IsChecked == true,
            LongRunningQueryExcludeBackups = LrqExcludeBackupsCheckBox.IsChecked == true,
            LongRunningQueryExcludeMiscWaits = LrqExcludeMiscWaitsCheckBox.IsChecked == true,
            LongRunningQueryExcludeCdc = LrqExcludeCdcCheckBox.IsChecked == true,
            /* #3653 (A5, Q5): comma-separated in the boxes; the row's bind normalises through the shared rule. */
            LongRunningQueryExcludedProgramNamePrefixes = LongRunningQueryExclusions.Normalize(AlertLrqExcludedProgramNamePrefixesBox.Text.Split(',')).ToList(),
            LongRunningQueryExcludedLogins = LongRunningQueryExclusions.Normalize(AlertLrqExcludedLoginsBox.Text.Split(',')).ToList(),
            TempDbSpaceEnabled = AlertTempDbSpaceCheckBox.IsChecked == true,
            LowDiskEnabled = AlertLowDiskCheckBox.IsChecked == true,
            PvsEnabled = AlertPvsCheckBox.IsChecked == true,
            FileGrowthEnabled = AlertFileGrowthCheckBox.IsChecked == true,
            LongRunningJobEnabled = AlertLongRunningJobCheckBox.IsChecked == true,
            FailedJobEnabled = AlertFailedJobCheckBox.IsChecked == true,
            DatabaseStateEnabled = AlertDatabaseStateCheckBox.IsChecked == true,
            AgentAlertGapsEnabled = AlertAgentAlertGapsCheckBox.IsChecked == true,
            BackupOverdueEnabled = AlertBackupOverdueCheckBox.IsChecked == true,
            LogBackupOverdueEnabled = AlertLogBackupOverdueCheckBox.IsChecked == true,
            CheckDbOverdueEnabled = AlertCheckDbOverdueCheckBox.IsChecked == true,
            IdentityCapacityEnabled = AlertIdentityCapacityCheckBox.IsChecked == true,
            IdentityCapacityWarningPercent = _identityCapacityWarningPercent,
            IdentityCapacityCriticalPercent = _identityCapacityCriticalPercent,
            AnalysisEnabled = AnalysisEnabledCheckBox.IsChecked == true,
            AnalysisNotificationsEnabled = AnalysisNotificationsCheckBox.IsChecked == true,
            /* #3712 (V137): the combo's Tag IS the stored value -- 'digest' / 'page', or empty for NULL ("the file
               governs"). No validation arm: the combo can only produce the three states the CHECK admits, and
               the bind normalises the spelling. */
            AnalysisUncorroboratedRoute = SelectedAnalysisUncorroboratedRoute(),
            FleetSweepEnabled = FleetSweepEnabledCheckBox.IsChecked == true,
            DailyReportEnabled = DailyReportEnabledCheckBox.IsChecked == true,
            ExcludedDatabases = SettingsListText.SplitLines(AlertExcludedDatabasesBox.Text),
            ExcludedVolumes = VolumeExclusions.Normalize(SettingsListText.SplitLinesAndCommas(AlertExcludedVolumesBox.Text)).ToList(),
        };

        if (int.TryParse(AlertCpuThresholdBox.Text, out var cpu) && cpu is > 0 and <= 100)
            row.CpuThresholdPercent = cpu;
        if (int.TryParse(AlertBlockingThresholdBox.Text, out var blocking) && blocking > 0)
            row.BlockingCountThreshold = blocking;
        /* #1839: >= 0, not > 0 like its siblings — 0 is this setting's OFF value, so rejecting it would
           make the gate impossible to turn back off once enabled. */
        if (int.TryParse(AlertBlockingWaitSecondsBox.Text, out var blockingWait) && blockingWait >= 0)
            row.BlockingWaitSecondsThreshold = blockingWait;
        if (int.TryParse(AlertDeadlockThresholdBox.Text, out var deadlock) && deadlock > 0)
            row.DeadlockCountThreshold = deadlock;
        /* #3444 (V122): the floor is the same named constant update_alert_settings enforces and
           DarlingAlertSettings clamps to, so the three writers into this column cannot disagree about
           what is acceptable. Out-of-range input leaves the row's prefilled value, matching every
           sibling above. */
        if (int.TryParse(AlertPgDeadlockThresholdBox.Text, out var pgDeadlock)
            && pgDeadlock >= PostgresAlertEvaluator.CountThresholdFloor)
            row.PgDeadlockCountThreshold = pgDeadlock;
        if (int.TryParse(AlertPgBlockingThresholdBox.Text, out var pgBlocking)
            && pgBlocking >= PostgresAlertEvaluator.CountThresholdFloor)
            row.PgBlockingCountThreshold = pgBlocking;
        if (int.TryParse(AlertPoisonWaitThresholdBox.Text, out var poisonWait) && poisonWait > 0)
            row.PoisonWaitThresholdMs = poisonWait;
        /* #5641: the Runnable Queue bars, the engine's own rule (RunnableQueueEvaluator.ClampBars): Warning at least 1 and
           Critical never below Warning. An invalid entry is reported here rather than silently clamped, so what is saved is
           what the operator typed. A bad pair is refused whatever the checkbox says (as Lite does, round 1 of #5709): the row
           is built fresh with the 8/16 defaults, so letting an unchecked alert skip the error would save 8/16 over the
           operator's stored bars. */
        var warningOk = int.TryParse(AlertRunnableQueueWarningBox.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rqWarning) && rqWarning >= 1;
        var criticalOk = int.TryParse(AlertRunnableQueueCriticalBox.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var rqCritical) && rqCritical >= 1;
        if (warningOk && criticalOk && rqCritical >= rqWarning)
        {
            row.RunnableQueueWarningPerScheduler = rqWarning;
            row.RunnableQueueCriticalPerScheduler = rqCritical;
        }
        else
        {
            errors.Add(!warningOk
                ? "Runnable queue Warning must be a whole number of at least 1 runnable task per scheduler."
                : "Runnable queue Critical must be a whole number no lower than the Warning bar.",
                AlertRunnableQueueWarningBox);
        }
        /* #5640: the Client Timeouts bars, the engine's own rule (AttentionEvaluator.ClampBars: Warning at least 1, Critical never
           below Warning), refused rather than clamped, and whatever the checkbox says, for the reason above (the row is built fresh
           with the 10/60 defaults). */
        var ctWarningOk = int.TryParse(AlertClientTimeoutsWarningBox.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ctWarning) && ctWarning >= 1;
        var ctCriticalOk = int.TryParse(AlertClientTimeoutsCriticalBox.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ctCritical) && ctCritical >= 1;
        if (ctWarningOk && ctCriticalOk && ctCritical >= ctWarning)
        {
            row.ClientTimeoutsWarningPerMinute = ctWarning;
            row.ClientTimeoutsCriticalPerMinute = ctCritical;
        }
        else
        {
            errors.Add(!ctWarningOk
                ? "Client timeouts Warning must be a whole number of at least 1 attention per minute."
                : !ctCriticalOk
                    ? "Client timeouts Critical must be a whole number of at least 1 attention per minute."
                    : "Client timeouts Critical must be a whole number no lower than the Warning bar.",
                AlertClientTimeoutsWarningBox);
        }
        /* #5736: the two multiples, the same rule as the floors (at least 1, Critical never below Warning), refused rather than clamped. */
        var ctWarningMultipleOk = int.TryParse(AlertClientTimeoutsWarningMultipleBox.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ctWarningMultiple) && ctWarningMultiple >= 1;
        var ctCriticalMultipleOk = int.TryParse(AlertClientTimeoutsCriticalMultipleBox.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ctCriticalMultiple) && ctCriticalMultiple >= 1;
        if (ctWarningMultipleOk && ctCriticalMultipleOk && ctCriticalMultiple >= ctWarningMultiple)
        {
            row.ClientTimeoutsWarningMultiple = ctWarningMultiple;
            row.ClientTimeoutsCriticalMultiple = ctCriticalMultiple;
        }
        else
        {
            errors.Add(!ctWarningMultipleOk
                ? "Client timeouts Warning multiple must be a whole number of at least 1."
                : !ctCriticalMultipleOk
                    ? "Client timeouts Critical multiple must be a whole number of at least 1."
                    : "Client timeouts Critical multiple must be a whole number no lower than the Warning multiple.",
                AlertClientTimeoutsWarningMultipleBox);
        }
        if (int.TryParse(AlertLongRunningQueryThresholdBox.Text, out var lrq) && lrq > 0)
            row.LongRunningQueryThresholdMinutes = lrq;
        /* V20: max-results (the read adapter re-clamps 1–1000; a bad value keeps the default 5). */
        if (int.TryParse(AlertLongRunningQueryMaxResultsBox.Text, out var lrqMax) && lrqMax is >= 1 and <= 1000)
            row.LongRunningQueryMaxResults = lrqMax;
        if (int.TryParse(AlertTempDbSpaceThresholdBox.Text, out var tempDb) && tempDb is > 0 and <= 100)
            row.TempDbSpaceThresholdPercent = tempDb;
        if (int.TryParse(AlertLowDiskThresholdPercentBox.Text, out var lowDiskPct) && lowDiskPct is >= 0 and <= 100)
            row.LowDiskThresholdPercent = lowDiskPct;
        if (int.TryParse(AlertLowDiskThresholdGbBox.Text, out var lowDiskGb) && lowDiskGb >= 0)
            row.LowDiskThresholdGb = lowDiskGb;
        /* #2107: the previously-hardcoded knobs, validated to the same ranges the service clamps. */
        if (int.TryParse(AlertDiskCriticalPercentBox.Text, out var critPct) && critPct is >= 0 and <= 100)
            row.DiskCriticalFreePercent = critPct;
        if (int.TryParse(AlertDiskCriticalGbBox.Text, out var critGb) && critGb >= 0)
            row.DiskCriticalFreeGb = critGb;
        if (int.TryParse(AlertSelfDiskWarnPercentBox.Text, out var selfDiskPct) && selfDiskPct is >= 0 and <= 100)
            row.SelfDiskFreeWarnPercent = selfDiskPct;
        /* #3528: validated to the same bound DarlingAlertSettings clamps (Math.Max(0, ...)) and the MCP
           writer accepts ([0, int.MaxValue]) — 0 is IN range because it removes the floor. */
        if (int.TryParse(AlertSelfDiskWarnGbBox.Text, out var selfDiskGb) && selfDiskGb >= 0)
            row.SelfDiskFreeWarnGb = selfDiskGb;
        if (int.TryParse(AlertCollectionStaleMinutesBox.Text, out var staleMin) && staleMin is >= 5 and <= 1440)
            row.CollectionStaleMinutes = staleMin;
        if (int.TryParse(AlertCollectionFailureThresholdBox.Text, out var failThresh) && failThresh is >= 1 and <= 1000)
            row.CollectionFailureThreshold = failThresh;
        /* #2136: validated to the same range DarlingAlertSettings clamps ([5, 100]). */
        if (int.TryParse(AlertStoreJobCadenceWarnPercentBox.Text, out var cadencePct) && cadencePct is >= 5 and <= 100)
            row.StoreJobCadenceWarnPercent = cadencePct;
        /* #3297: validated against the SAME named bounds DarlingAlertSettings clamps to and the MCP writer
           accepts, so the three surfaces cannot disagree about what is settable. InvariantCulture on the
           parse deliberately: this writes a double precision column read by the service and by
           get_alert_settings, so a comma decimal separator from the operator's locale must be rejected
           rather than silently parsed as a different number on one machine and not another. The two are
           validated INDEPENDENTLY — critical below warn is a legitimate setting (every fire is Critical),
           not a pair to reject. */
        if (double.TryParse(AlertRetentionHoldWarnRatioBox.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var holdWarn)
            && holdWarn >= TimescaleSupport.RetentionHoldRatioFloor
            && holdWarn <= TimescaleSupport.RetentionHoldRatioCeiling)
            row.RetentionHoldWarnRatio = holdWarn;
        if (double.TryParse(AlertRetentionHoldCriticalRatioBox.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var holdCritical)
            && holdCritical >= TimescaleSupport.RetentionHoldRatioFloor
            && holdCritical <= TimescaleSupport.RetentionHoldRatioCeiling)
            row.RetentionHoldCriticalRatio = holdCritical;
        /* #3368: validated against the SAME named bounds DeadlockRateThresholds clamps to and the MCP
           writer accepts, InvariantCulture on the parse, and the two validated INDEPENDENTLY — all three
           for the reasons the pair above gives. */
        if (double.TryParse(BandDeadlockWarnPerHourBox.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var dlWarn)
            && dlWarn >= ServerHealthThresholds.DeadlockRatePerHourFloor
            && dlWarn <= ServerHealthThresholds.DeadlockRatePerHourCeiling)
            row.DeadlockWarnPerHour = dlWarn;
        if (double.TryParse(BandDeadlockCriticalPerHourBox.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var dlCritical)
            && dlCritical >= ServerHealthThresholds.DeadlockRatePerHourFloor
            && dlCritical <= ServerHealthThresholds.DeadlockRatePerHourCeiling)
            row.DeadlockCriticalPerHour = dlCritical;
        /* #6927: the blocking band, validated against the SAME named bounds BlockingBandThresholds clamps to and the MCP writer accepts,
           InvariantCulture on the parse. Unlike the deadlock pair, a pair with critical below warn is refused, the rule update_alert_settings
           applies, so the window cannot save a pair the tool would refuse. */
        BlockingBandBox(BandBlockingWarnPerHourBox, ServerHealthThresholds.BlockingRatePerHourFloor, ServerHealthThresholds.BlockingRatePerHourCeiling,
            "Blocking warn per hour", errors, out var blWarnRate);
        BlockingBandBox(BandBlockingCriticalPerHourBox, ServerHealthThresholds.BlockingRatePerHourFloor, ServerHealthThresholds.BlockingRatePerHourCeiling,
            "Blocking critical per hour", errors, out var blCriticalRate);
        BlockingBandBox(BandBlockingWarnWaitSecondsBox, ServerHealthThresholds.BlockingWaitSecondsFloor, ServerHealthThresholds.BlockingWaitSecondsCeiling,
            "Blocking warn seconds", errors, out var blWarnWait);
        BlockingBandBox(BandBlockingCriticalWaitSecondsBox, ServerHealthThresholds.BlockingWaitSecondsFloor, ServerHealthThresholds.BlockingWaitSecondsCeiling,
            "Blocking critical seconds", errors, out var blCriticalWait);
        if (blWarnRate.HasValue && blCriticalRate.HasValue)
        {
            if (blCriticalRate.Value < blWarnRate.Value)
                errors.Add("Blocking critical per hour must not be lower than blocking warn per hour.", BandBlockingCriticalPerHourBox);
            else
            {
                row.BlockingWarnPerHour = blWarnRate.Value;
                row.BlockingCriticalPerHour = blCriticalRate.Value;
            }
        }
        if (blWarnWait.HasValue && blCriticalWait.HasValue)
        {
            if (blCriticalWait.Value < blWarnWait.Value)
                errors.Add("Blocking critical seconds must not be lower than blocking warn seconds.", BandBlockingCriticalWaitSecondsBox);
            else
            {
                row.BlockingWarnWaitSeconds = blWarnWait.Value;
                row.BlockingCriticalWaitSeconds = blCriticalWait.Value;
            }
        }
        /* #7028: the snapshot arm. 0 turns a tier off, so the order only binds when both are above 0. */
        BlockingBandOptionalBox(BandBlockingWarnSnapshotWaitSecondsBox, ServerHealthThresholds.BlockingWaitSecondsFloor, ServerHealthThresholds.BlockingWaitSecondsCeiling,
            "Blocking snapshot warn seconds", errors, out var blWarnSnapshot);
        BlockingBandOptionalBox(BandBlockingCriticalSnapshotWaitSecondsBox, ServerHealthThresholds.BlockingWaitSecondsFloor, ServerHealthThresholds.BlockingWaitSecondsCeiling,
            "Blocking snapshot critical seconds", errors, out var blCriticalSnapshot);
        if (blWarnSnapshot.HasValue && blCriticalSnapshot.HasValue)
        {
            if (blWarnSnapshot.Value > 0 && blCriticalSnapshot.Value > 0 && blCriticalSnapshot.Value < blWarnSnapshot.Value)
                errors.Add("Blocking snapshot critical seconds must not be lower than blocking snapshot warn seconds.", BandBlockingCriticalSnapshotWaitSecondsBox);
            else
            {
                row.BlockingWarnSnapshotWaitSeconds = blWarnSnapshot.Value;
                row.BlockingCriticalSnapshotWaitSeconds = blCriticalSnapshot.Value;
            }
        }
        if (int.TryParse(AlertPvsThresholdPercentBox.Text, out var pvsPct) && pvsPct is >= 0 and <= 100)
            row.PvsThresholdPercent = pvsPct;
        if (int.TryParse(AlertPvsFloorGbBox.Text, out var pvsFloor) && pvsFloor >= 0)
            row.PvsFloorGb = pvsFloor;
        /* #2391: validated to the same ranges DarlingAlertSettings clamps. */
        if (int.TryParse(AlertFileGrowthRiseMbBox.Text, out var growthRise) && growthRise >= 0)
            row.FileGrowthRiseMb = growthRise;
        if (int.TryParse(AlertFileGrowthVolumePercentBox.Text, out var growthPct) && growthPct is >= 0 and <= 100)
            row.FileGrowthVolumePercent = growthPct;
        if (int.TryParse(AlertFileGrowthLookbackMinutesBox.Text, out var growthLookback) && growthLookback is >= 5 and <= 1440)
            row.FileGrowthLookbackMinutes = growthLookback;
        if (int.TryParse(AlertLongRunningJobMultiplierBox.Text, out var jobMult) && jobMult is >= 2 and <= 20)
            row.LongRunningJobMultiplier = jobMult;
        if (int.TryParse(AlertFailedJobLookbackBox.Text, out var failedJobLookback) && failedJobLookback is >= 1 and <= 1440)
            row.FailedJobLookbackMinutes = failedJobLookback;

        if (int.TryParse(AlertCooldownBox.Text, out var alertCooldown) && alertCooldown is >= 1 and <= 120)
            row.CooldownMinutes = alertCooldown;
        else
            errors.Add("Tray notification cooldown must be between 1 and 120 minutes.", AlertCooldownBox);

        if (int.TryParse(AnalysisIntervalBox.Text, out var analysisInterval) && analysisInterval is >= 5 and <= 360)
            row.AnalysisIntervalMinutes = analysisInterval;
        else
            errors.Add("Analysis interval must be between 5 and 360 minutes.", AnalysisIntervalBox);

        if (double.TryParse(AnalysisNotifySeverityBox.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var analysisSeverity)
            && analysisSeverity is >= 0.0 and <= 2.0)
            row.AnalysisNotifySeverity = analysisSeverity;
        else
            errors.Add("Analysis notify severity must be between 0.0 and 2.0.", AnalysisNotifySeverityBox);

        if (int.TryParse(AnalysisNotifyCooldownBox.Text, out var analysisCooldown) && analysisCooldown is >= 30 and <= 10080)
            row.AnalysisNotifyCooldownMinutes = analysisCooldown;
        else
            errors.Add("Analysis re-notify cooldown must be between 30 and 10080 minutes.", AnalysisNotifyCooldownBox);

        /* #3466 (V124): the bounds are the same named constants update_alert_settings enforces and the
           worker clamps to, so the three writers into this column cannot disagree about what is
           acceptable — the #3444 discipline, restated for a cadence. */
        if (int.TryParse(FleetSweepIntervalBox.Text, out var fleetSweepInterval)
            && fleetSweepInterval >= FleetSweepCadence.IntervalMinutesFloor
            && fleetSweepInterval <= FleetSweepCadence.IntervalMinutesCeiling)
            row.FleetSweepIntervalMinutes = fleetSweepInterval;
        else
            errors.Add($"Fleet sweep interval must be between {FleetSweepCadence.IntervalMinutesFloor} and {FleetSweepCadence.IntervalMinutesCeiling} minutes.", FleetSweepIntervalBox);

        /* #7041 (V232): the same rules update_alert_settings enforces (ViewerDailyReportRules mirrors the service's), so the two writers
           cannot disagree. A blank zone or title is stored as NULL: the host's zone, the default title. */
        if (ViewerDailyReportRules.TryParseSendAt(DailyReportSendAtBox.Text, out var dailyReportSendAt))
            row.DailyReportSendAt = ViewerDailyReportRules.FormatSendAt(dailyReportSendAt);
        else
            errors.Add("Daily health report send time must be a time of day written HH:mm, for example 07:00.", DailyReportSendAtBox);

        var dailyReportZone = DailyReportTimeZoneBox.Text.Trim();
        if (ViewerDailyReportRules.IsKnownZone(dailyReportZone))
            row.DailyReportTimeZone = dailyReportZone.Length == 0 ? null : dailyReportZone;
        else
            errors.Add($"Daily health report time zone \"{dailyReportZone}\" is not a time zone this machine knows. Use an IANA id such as America/New_York, or leave it blank for the service host's zone.", DailyReportTimeZoneBox);

        var dailyReportTitle = DailyReportTitleBox.Text.Trim();
        if (dailyReportTitle.Length == 0)
            row.DailyReportTitle = null;
        else if (ViewerDailyReportRules.TryValidateTitle(dailyReportTitle, out var dailyReportTitleError))
            row.DailyReportTitle = dailyReportTitle;
        else
            errors.Add($"Daily health report title {dailyReportTitleError}", DailyReportTitleBox);

        /* #1141/#1236: delivery mode + per-event cap (store-backed). */
        row.DeliveryMode = AlertDeliveryModeBox.SelectedIndex == 1 ? "PerEvent" : "Summary";
        if (int.TryParse(AlertPerEventMaxBox.Text, out var perEventMax) && perEventMax is >= 1 and <= 100)
            row.PerEventMax = perEventMax;
        else
            errors.Add("Per-event max-per-cycle must be between 1 and 100.", AlertPerEventMaxBox);

        return row;
    }

    /// <summary>Persists the viewer-LOCAL alert fields to <see cref="ViewerAppSettings"/> (the store never sees them).</summary>
    private void SaveViewerLocalAlertFields(SettingsValidationErrors errors)
    {
        _appSettings.MinimizeToTray = MinimizeToTrayCheckBox.IsChecked == true;
        /* AlertDeliveryMode/PerEventMax, the long-running-query read shape (max-results + the five noise filters),
           and connection-change notify (V20) are STORE-backed now (BuildAlertRowFromControls writes them); not here. */
        _appSettings.MuteRuleDefaultExpiration = (MuteRuleDefaultExpirationCombo.SelectedItem as ComboBoxItem)?.Content?.ToString() ?? "24 hours";
        _appSettings.LogAlertDismissals = LogAlertDismissalsCheckBox.IsChecked == true;
    }

    private void AlertsEnabledCheckBox_Changed(object sender, RoutedEventArgs e) => UpdateAlertControlStates();

    private void RestoreAlertDefaultsButton_Click(object sender, RoutedEventArgs e)
    {
        AlertCpuThresholdBox.Text = "80";
        AlertCpuModeBox.SelectedIndex = 0; // Total
        AlertBlockingThresholdBox.Text = "1";
        AlertBlockingWaitSecondsBox.Text = "0";
        AlertDeadlockThresholdBox.Text = "1";
        /* #3444 (V122): from the shared constants rather than a literal "1", so a moved default cannot
           leave this button writing a threshold nobody chose. */
        AlertPgDeadlockThresholdBox.Text =
            PostgresAlertEvaluator.DeadlockCountThresholdDefault.ToString(CultureInfo.InvariantCulture);
        AlertPgBlockingThresholdBox.Text =
            PostgresAlertEvaluator.BlockingCountThresholdDefault.ToString(CultureInfo.InvariantCulture);
        AlertPoisonWaitThresholdBox.Text = "500";
        AlertRunnableQueueWarningBox.Text = RunnableQueueEvaluator.DefaultWarningPerScheduler.ToString(CultureInfo.InvariantCulture);
        AlertRunnableQueueCriticalBox.Text = RunnableQueueEvaluator.DefaultCriticalPerScheduler.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsWarningBox.Text = AttentionEvaluator.DefaultWarningPerMinute.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsCriticalBox.Text = AttentionEvaluator.DefaultCriticalPerMinute.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsWarningMultipleBox.Text = AttentionBaselineMath.DefaultWarningMultiple.ToString(CultureInfo.InvariantCulture);
        AlertClientTimeoutsCriticalMultipleBox.Text = AttentionBaselineMath.DefaultCriticalMultiple.ToString(CultureInfo.InvariantCulture);
        AlertLongRunningQueryThresholdBox.Text = "30";
        /* V20: the long-running-query read shape resets to Lite's App defaults (5 rows, every filter on). */
        AlertLongRunningQueryMaxResultsBox.Text = "5";
        LrqExcludeSpServerDiagnosticsCheckBox.IsChecked = true;
        LrqExcludeWaitForCheckBox.IsChecked = true;
        LrqExcludeBackupsCheckBox.IsChecked = true;
        LrqExcludeMiscWaitsCheckBox.IsChecked = true;
        LrqExcludeCdcCheckBox.IsChecked = true;
        /* #3653 (A5, Q5): "defaults" for the opt-out knob are the SEEDS (the rung's column DEFAULT), not empty boxes. */
        AlertLrqExcludedProgramNamePrefixesBox.Text = string.Join(", ", LongRunningQueryExclusions.DefaultProgramNamePrefixes);
        AlertLrqExcludedLoginsBox.Text = string.Join(", ", LongRunningQueryExclusions.DefaultLogins);
        AlertTempDbSpaceThresholdBox.Text = "80";
        AlertLowDiskThresholdPercentBox.Text = "10";
        AlertLowDiskThresholdGbBox.Text = "5";
        /* #2107: the previously-hardcoded knobs reset to the constants they replaced. */
        AlertDiskCriticalPercentBox.Text = "3";
        AlertDiskCriticalGbBox.Text = "2";
        AlertSelfDiskWarnPercentBox.Text = "10";
        /* #3528: the shipped GB floor (DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb) as a literal — the
           constant lives on the Service assembly the viewer does not reference, and the V126 rung test
           pins this literal equal to it. */
        AlertSelfDiskWarnGbBox.Text = "50";
        AlertCollectionStaleMinutesBox.Text = "30";
        AlertCollectionFailureThresholdBox.Text = "10";
        /* #3060: derived, unlike its neighbours, because this one is not merely a mirrored default — it is
           one refresh slot as a share of the hourly cadence, and a reset that handed out a stale 25 after
           the grid moved would arm the alert past the point it exists to precede. */
        AlertStoreJobCadenceWarnPercentBox.Text =
            TimescaleSupport.RefreshSlotPercentOfHourlyCadence.ToString(CultureInfo.InvariantCulture);
        /* #3297: derived for the reason one line up — this button writes the row when the boxes do not
           parse, so a frozen literal here is the value an operator would be handed back. */
        AlertRetentionHoldWarnRatioBox.Text =
            TimescaleSupport.RetentionHoldWarnRatioDefault.ToString("0.0", CultureInfo.InvariantCulture);
        AlertRetentionHoldCriticalRatioBox.Text =
            TimescaleSupport.RetentionHoldCriticalRatioDefault.ToString("0.0", CultureInfo.InvariantCulture);
        /* #3368: derived for the reason one line up — this button writes the row when the boxes do not
           parse, so a frozen literal here is the value an operator would be handed back. */
        BandDeadlockWarnPerHourBox.Text =
            ServerHealthThresholds.DeadlockWarnPerHourDefault.ToString("0.0", CultureInfo.InvariantCulture);
        BandDeadlockCriticalPerHourBox.Text =
            ServerHealthThresholds.DeadlockCriticalPerHourDefault.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingWarnPerHourBox.Text =
            ServerHealthThresholds.BlockingWarnPerHour.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingCriticalPerHourBox.Text =
            ServerHealthThresholds.BlockingCriticalPerHour.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingWarnWaitSecondsBox.Text =
            ServerHealthThresholds.BlockingWarnWaitSeconds.ToString("0.0", CultureInfo.InvariantCulture);
        BandBlockingCriticalWaitSecondsBox.Text =
            ServerHealthThresholds.BlockingCriticalWaitSeconds.ToString("0.0", CultureInfo.InvariantCulture);
        /* #7028: both snapshot tiers default to 0, which is off. */
        BandBlockingWarnSnapshotWaitSecondsBox.Text = "0.0";
        BandBlockingCriticalSnapshotWaitSecondsBox.Text = "0.0";
        AnalysisNotifyCooldownBox.Text = "360";
        AlertPvsThresholdPercentBox.Text = "40";
        AlertPvsFloorGbBox.Text = "1";
        AlertFileGrowthRiseMbBox.Text = "10240";
        AlertFileGrowthVolumePercentBox.Text = "60";
        AlertFileGrowthLookbackMinutesBox.Text = "60";
        AlertLongRunningJobMultiplierBox.Text = "3";
        AlertFailedJobLookbackBox.Text = "60";
        AlertCooldownBox.Text = "5";
        EmailCooldownBox.Text = "15";
        AlertDeliveryModeBox.SelectedIndex = 0;
        AlertPerEventMaxBox.Text = "5";
        AnalysisIntervalBox.Text = "30";
        AnalysisNotifySeverityBox.Text = "1.5";
        /* #3712 (V137): "defaults" for the route knob is the NULL item -- the store column cleared, the service's
           darling.json value (shipped 'digest') governing -- not 'digest' pinned into the store. A Restore Defaults
           that wrote 'digest' would silently move the decision off the file for every operator who pressed it,
           which is the provenance shift the MCP writer warns about; the honest default is the row's own
           (AlertSettingsRow.Defaults() holds null here for the same reason). */
        SelectAnalysisUncorroboratedRoute(null);
        /* #3466 (V124): from the shared constant rather than a literal, so a moved default cannot
           leave this button writing a cadence nobody chose. The checkbox resets to the shipped ON. */
        FleetSweepEnabledCheckBox.IsChecked = true;
        FleetSweepIntervalBox.Text = FleetSweepCadence.DefaultIntervalMinutes.ToString(CultureInfo.InvariantCulture);
        /* #7041 (V232): the report ships off, at 07:00, in the host's zone, with the default title. */
        DailyReportEnabledCheckBox.IsChecked = false;
        DailyReportSendAtBox.Text = ViewerDailyReportRules.DefaultSendAt;
        DailyReportTimeZoneBox.Text = "";
        DailyReportTitleBox.Text = "";
        AlertExcludedDatabasesBox.Text = "";
        AlertExcludedVolumesBox.Text = "";
        MuteRuleDefaultExpirationCombo.SelectedIndex = 1; // 24 hours
        UpdateAlertPreviewText();
    }

    private void ManageMuteRulesButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        var window = new MuteRulesWindow(_dataService) { Owner = this };
        window.ShowDialog();
    }

    private void ManageNotificationRoutesButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        var window = new NotificationRoutesWindow(_dataService) { Owner = this };
        window.ShowDialog();
    }

    /// <summary>#5916: the per-server threshold overrides, beside the global thresholds they replace.</summary>
    private void ManageThresholdOverridesButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        var window = new ThresholdOverridesWindow(_dataService) { Owner = this };
        window.ShowDialog();
    }

    /// <summary>#5632 Phase A: the investigations an agent route opens, beside the routes that open them.</summary>
    private void ManageInvestigationsButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        var window = new InvestigationsWindow(_dataService) { Owner = this };
        window.ShowDialog();
    }

    private void UpdateAlertPreviewText()
    {
        var parts = new List<string>();

        if (AlertCpuCheckBox.IsChecked == true)
        {
            var cpuLabel = AlertCpuModeBox.SelectedIndex == 1 ? "SQL CPU" : "Total CPU";
            parts.Add($"{cpuLabel} > {AlertCpuThresholdBox.Text}%");
        }
        if (AlertBlockingCheckBox.IsChecked == true)
        {
            parts.Add($"blocking >= {AlertBlockingThresholdBox.Text}");
            /* #1839: only summarize the wait gate when it is actually on (0 = off). */
            if (int.TryParse(AlertBlockingWaitSecondsBox.Text, out var blockingWaitPreview) && blockingWaitPreview > 0)
                parts.Add($"blocked wait >= {blockingWaitPreview}s");
        }
        if (AlertDeadlockCheckBox.IsChecked == true)
            parts.Add($"deadlocks >= {AlertDeadlockThresholdBox.Text}");
        /* #3444: only summarize the PostgreSQL gates when they differ from their SQL Server twin — on the
           common single-engine install the two agree and a second identical clause would read as a
           duplicate rather than as information. */
        if (AlertDeadlockCheckBox.IsChecked == true
            && !string.Equals(AlertPgDeadlockThresholdBox.Text, AlertDeadlockThresholdBox.Text, StringComparison.Ordinal))
            parts.Add($"pg deadlocks >= {AlertPgDeadlockThresholdBox.Text}");
        if (AlertBlockingCheckBox.IsChecked == true
            && !string.Equals(AlertPgBlockingThresholdBox.Text, AlertBlockingThresholdBox.Text, StringComparison.Ordinal))
            parts.Add($"pg blocking >= {AlertPgBlockingThresholdBox.Text}");
        if (AlertPoisonWaitCheckBox.IsChecked == true)
            /* #3539 A4: the live bar, from the shared constants — not the retired ms box, which nothing reads. */
            parts.Add(string.Create(CultureInfo.InvariantCulture,
                $"poison waits >= {PoisonWaitEvaluator.WarningAvgWaiters * PoisonWaitEvaluator.WindowMinutes * 60:N0}s accumulated in {PoisonWaitEvaluator.WindowMinutes}min"));
        if (AlertRunnableQueueCheckBox.IsChecked == true)
            parts.Add(string.Create(CultureInfo.InvariantCulture,
                $"runnable queue >= {AlertRunnableQueueWarningBox.Text}/{AlertRunnableQueueCriticalBox.Text} per scheduler (avg {RunnableQueueEvaluator.WindowMinutes}min)"));
        if (AlertClientTimeoutsCheckBox.IsChecked == true)
            parts.Add(string.Create(CultureInfo.InvariantCulture,
                $"client timeouts >= max({AlertClientTimeoutsWarningBox.Text}/{AlertClientTimeoutsCriticalBox.Text} per minute, {AlertClientTimeoutsWarningMultipleBox.Text}x/{AlertClientTimeoutsCriticalMultipleBox.Text}x the server's own baseline) (avg {AttentionEvaluator.WindowMinutes}min)"));
        if (AlertLongRunningQueryCheckBox.IsChecked == true)
            parts.Add($"queries > {AlertLongRunningQueryThresholdBox.Text}min");
        if (AlertTempDbSpaceCheckBox.IsChecked == true)
            parts.Add($"tempdb > {AlertTempDbSpaceThresholdBox.Text}%");
        if (AlertLowDiskCheckBox.IsChecked == true)
            parts.Add($"disk free < {AlertLowDiskThresholdPercentBox.Text}% or {AlertLowDiskThresholdGbBox.Text}GB");
        if (AlertPvsCheckBox.IsChecked == true)
            parts.Add($"PVS >= {AlertPvsThresholdPercentBox.Text}% of database");
        if (AlertAgentAlertGapsCheckBox.IsChecked == true)
            parts.Add("Agent alert setup gaps");
        if (AlertBackupOverdueCheckBox.IsChecked == true)
            parts.Add("full backup overdue");
        if (AlertLogBackupOverdueCheckBox.IsChecked == true)
            parts.Add("log backup overdue");
        if (AlertCheckDbOverdueCheckBox.IsChecked == true)
            parts.Add("CHECKDB overdue");
        if (AlertFileGrowthCheckBox.IsChecked == true)
            /* #3539 A8c: the rise is a RATE (MB per hour) averaged over the lookback, in the same unit phrase the
               row's label, the alert's threshold line and the MCP payload description use. It used to read
               "10240MB/60m", which was the per-window delta the engine then compared literally. */
            parts.Add($"file growth > {AlertFileGrowthRiseMbBox.Text} {AlertContextBuilders.FileGrowthRiseUnit} over {AlertFileGrowthLookbackMinutesBox.Text}m or volume > {AlertFileGrowthVolumePercentBox.Text}%");
        if (AlertLongRunningJobCheckBox.IsChecked == true)
            parts.Add($"jobs > {AlertLongRunningJobMultiplierBox.Text}x avg");
        if (AlertIdentityCapacityCheckBox.IsChecked == true)
            parts.Add("identity columns near the end of their type");
        if (AlertFailedJobCheckBox.IsChecked == true)
            parts.Add($"failed jobs (last {AlertFailedJobLookbackBox.Text}m)");

        AlertPreviewText.Text = parts.Count > 0
            ? $"Will alert when: {string.Join(", ", parts)}"
            : "No alerts enabled";
    }

    /// <summary>#3712 (V137): selects the route combo's item for a stored value — the item whose Tag is the
    /// parsed route's wire spelling, or the empty-Tag item for NULL and for anything <see cref="FindingRouting.TryParseRoute"/>
    /// reads as neither route (impossible under the V137 CHECK; on a hand-edited row the honest item is "the
    /// file governs", which is what the service resolves such a value to as well).</summary>
    private void SelectAnalysisUncorroboratedRoute(string? stored)
    {
        var tag = FindingRouting.TryParseRoute(stored) is { } route ? FindingRouting.RouteText(route) : "";
        foreach (ComboBoxItem item in AnalysisUncorroboratedRouteBox.Items)
        {
            if (string.Equals(item.Tag as string, tag, StringComparison.Ordinal))
            {
                AnalysisUncorroboratedRouteBox.SelectedItem = item;
                return;
            }
        }

        /* Unreachable while the XAML carries the three items; if one is ever removed, land on the NULL item
           rather than leave the combo unselected and the save writing whatever a null Tag maps to. */
        AnalysisUncorroboratedRouteBox.SelectedIndex = AnalysisUncorroboratedRouteBox.Items.Count - 1;
    }

    /// <summary>#3712 (V137): the route combo's selection as the value the row stores — the item's Tag, with the
    /// empty Tag (and no selection at all) mapping to null, "the file governs".</summary>
    private string? SelectedAnalysisUncorroboratedRoute()
    {
        var tag = (AnalysisUncorroboratedRouteBox.SelectedItem as ComboBoxItem)?.Tag as string;
        return string.IsNullOrEmpty(tag) ? null : tag;
    }

    private void UpdateAlertControlStates()
    {
        var enabled = AlertsEnabledCheckBox.IsChecked == true;
        NotifyConnectionCheckBox.IsEnabled = enabled;
        NotifyConnectionDownAtStartupCheckBox.IsEnabled = enabled;
        ConnectionRefireMinutesBox.IsEnabled = enabled;
        NotifyAgHealthCheckBox.IsEnabled = enabled;
        AgLagAlertSecondsBox.IsEnabled = enabled;
        AgRedoQueueAlertKbBox.IsEnabled = enabled;
        AgDisconnectRefireMinutesBox.IsEnabled = enabled;
        AlertCpuCheckBox.IsEnabled = enabled;
        AlertCpuThresholdBox.IsEnabled = enabled;
        AlertCpuModeBox.IsEnabled = enabled;
        AlertBlockingCheckBox.IsEnabled = enabled;
        AlertBlockingThresholdBox.IsEnabled = enabled;
        AlertBlockingWaitSecondsBox.IsEnabled = enabled;
        AlertDeadlockCheckBox.IsEnabled = enabled;
        AlertDeadlockThresholdBox.IsEnabled = enabled;
        AlertPgDeadlockThresholdBox.IsEnabled = enabled;
        AlertPgBlockingThresholdBox.IsEnabled = enabled;
        AlertPoisonWaitCheckBox.IsEnabled = enabled;
        /* #3539 A4: the poison-wait ms box is retired (nothing reads it) and stays disabled regardless of the
           master switch — the XAML sets IsEnabled="False", and this loop must not re-enable it on load or
           on toggle, or the operator is back to tuning a number the engine ignores. */
        AlertPoisonWaitThresholdBox.IsEnabled = false;
        AlertRunnableQueueCheckBox.IsEnabled = enabled;
        AlertRunnableQueueWarningBox.IsEnabled = enabled;
        AlertRunnableQueueCriticalBox.IsEnabled = enabled;
        AlertClientTimeoutsCheckBox.IsEnabled = enabled;
        AlertClientTimeoutsWarningBox.IsEnabled = enabled;
        AlertClientTimeoutsCriticalBox.IsEnabled = enabled;
        AlertClientTimeoutsWarningMultipleBox.IsEnabled = enabled;
        AlertClientTimeoutsCriticalMultipleBox.IsEnabled = enabled;
        AlertLongRunningQueryCheckBox.IsEnabled = enabled;
        AlertLongRunningQueryThresholdBox.IsEnabled = enabled;
        /* V20 long-running-query read-shape controls follow the master switch like the rest of the engine. */
        AlertLongRunningQueryMaxResultsBox.IsEnabled = enabled;
        LrqExcludeSpServerDiagnosticsCheckBox.IsEnabled = enabled;
        LrqExcludeWaitForCheckBox.IsEnabled = enabled;
        LrqExcludeBackupsCheckBox.IsEnabled = enabled;
        LrqExcludeMiscWaitsCheckBox.IsEnabled = enabled;
        LrqExcludeCdcCheckBox.IsEnabled = enabled;
        AlertLrqExcludedProgramNamePrefixesBox.IsEnabled = enabled;
        AlertLrqExcludedLoginsBox.IsEnabled = enabled;
        AlertTempDbSpaceCheckBox.IsEnabled = enabled;
        AlertTempDbSpaceThresholdBox.IsEnabled = enabled;
        AlertLowDiskCheckBox.IsEnabled = enabled;
        AlertLowDiskThresholdPercentBox.IsEnabled = enabled;
        AlertPvsCheckBox.IsEnabled = enabled;
        AlertAgentAlertGapsCheckBox.IsEnabled = enabled;
        AlertBackupOverdueCheckBox.IsEnabled = enabled;
        AlertLogBackupOverdueCheckBox.IsEnabled = enabled;
        AlertCheckDbOverdueCheckBox.IsEnabled = enabled;
        AlertPvsThresholdPercentBox.IsEnabled = enabled;
        AlertPvsFloorGbBox.IsEnabled = enabled;
        AlertLowDiskThresholdGbBox.IsEnabled = enabled;
        /* #2107: the new threshold boxes follow the master switch like every sibling. */
        AlertDiskCriticalPercentBox.IsEnabled = enabled;
        AlertDiskCriticalGbBox.IsEnabled = enabled;
        AlertSelfDiskWarnPercentBox.IsEnabled = enabled;
        AlertSelfDiskWarnGbBox.IsEnabled = enabled;
        AlertCollectionStaleMinutesBox.IsEnabled = enabled;
        AlertCollectionFailureThresholdBox.IsEnabled = enabled;
        AlertStoreJobCadenceWarnPercentBox.IsEnabled = enabled;
        AlertRetentionHoldWarnRatioBox.IsEnabled = enabled;
        AlertRetentionHoldCriticalRatioBox.IsEnabled = enabled;
        /* #3368's BandDeadlockWarnPerHourBox / BandDeadlockCriticalPerHourBox are ABSENT from this list on
           purpose. They are health-BAND tiers, not alert thresholds: a card's colour and the fleet
           roll-up's band counts are rendered whether or not the alert engine is switched on, so greying
           them here would make the one setting that still has an effect look inert. */
        AlertFileGrowthCheckBox.IsEnabled = enabled;
        AlertFileGrowthRiseMbBox.IsEnabled = enabled;
        AlertFileGrowthVolumePercentBox.IsEnabled = enabled;
        AlertFileGrowthLookbackMinutesBox.IsEnabled = enabled;
        AlertLongRunningJobCheckBox.IsEnabled = enabled;
        AlertLongRunningJobMultiplierBox.IsEnabled = enabled;
        AlertFailedJobCheckBox.IsEnabled = enabled;
        AlertIdentityCapacityCheckBox.IsEnabled = enabled;
        AlertFailedJobLookbackBox.IsEnabled = enabled;
        UpdateAlertPreviewText();
    }

    // ── SMTP email + webhooks (config_notification) ──

    /// <summary>Seeds the SMTP + Teams/Slack controls from a <see cref="NotificationRow"/> (the store's values,
    /// or defaults). The SMTP enable toggle reflects whether the store carries connect fields; the password box
    /// is prefilled from the decrypted store blob (blank + remembered when it was sealed on another machine).</summary>
    private void SeedNotificationControlsFrom(NotificationRow r)
    {
        var smtpConfigured = !string.IsNullOrWhiteSpace(r.SmtpHost)
            || !string.IsNullOrWhiteSpace(r.SmtpFromAddress)
            || !string.IsNullOrWhiteSpace(r.SmtpRecipients);
        SmtpEnabledCheckBox.IsChecked = smtpConfigured;
        SmtpServerBox.Text = r.SmtpHost;
        SmtpPortBox.Text = r.SmtpPort.ToString(CultureInfo.InvariantCulture);
        SmtpSslCheckBox.IsChecked = r.SmtpUseSsl;
        SmtpUsernameBox.Text = r.SmtpUsername ?? "";
        SmtpFromBox.Text = r.SmtpFromAddress;
        SmtpRecipientsBox.Text = r.SmtpRecipients;
        EmailCooldownBox.Text = r.EmailCooldownMinutes.ToString(CultureInfo.InvariantCulture);
        EmailSendResolutionsCheckBox.IsChecked = r.EmailSendResolutions;

        /* A saved password is never put back in the box, sealed or in the older format: the box stays blank and says the
           password is saved (#5366). A blank box keeps it; a changed host, port, SSL flag or user name asks for it again. */
        _loadedNotification = r;
        SmtpPasswordBox.Password = "";
        SmtpStatusText.Text = !string.IsNullOrEmpty(r.SmtpEncryptedPassword)
            ? ViewerSmtpSeal.SavedText
            : "";

        TeamsWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.TeamsUrl);
        TeamsWebhookUrlBox.Text = ViewerWebhookSealing.ShownText(r.TeamsUrl);
        TeamsStatusText.Text = ViewerWebhookSealing.IsSaved(r.TeamsUrl) ? ViewerWebhookSealing.KeepHint : "";
        TeamsProxyAddressBox.Text = r.TeamsProxy;
        TeamsSendResolutionsCheckBox.IsChecked = r.TeamsSendResolutions;
        SlackWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.SlackUrl);
        SlackWebhookUrlBox.Text = ViewerWebhookSealing.ShownText(r.SlackUrl);
        SlackStatusText.Text = ViewerWebhookSealing.IsSaved(r.SlackUrl) ? ViewerWebhookSealing.KeepHint : "";
        SlackProxyAddressBox.Text = r.SlackProxy;
        SlackSendResolutionsCheckBox.IsChecked = r.SlackSendResolutions;

        GenericWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.GenericUrl);
        GenericWebhookUrlBox.Text = ViewerWebhookSealing.ShownText(r.GenericUrl);
        GenericWebhookHeadersBox.Text = ViewerWebhookSealing.ShownText(r.GenericHeaders);
        GenericStatusText.Text = ViewerWebhookSealing.IsSaved(r.GenericUrl) || ViewerWebhookSealing.IsSaved(r.GenericHeaders)
            ? ViewerWebhookSealing.KeepHint
            : "";
        /* Blank means "use the built-in default" — show it, so the operator has something to edit rather
           than a blank box whose shape they have to guess. */
        GenericWebhookBodyBox.Text = string.IsNullOrWhiteSpace(r.GenericBodyTemplate)
            ? WebhookAlertService.DefaultGenericBodyTemplate
            : r.GenericBodyTemplate;
        GenericWebhookProxyAddressBox.Text = r.GenericProxy;
        GenericSendResolutionsCheckBox.IsChecked = r.GenericSendResolutions;

        PagerDutyWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.PagerDutyRoutingKey);
        PagerDutyRoutingKeyBox.Text = ViewerWebhookSealing.ShownText(r.PagerDutyRoutingKey);
        PagerDutyStatusText.Text = ViewerWebhookSealing.IsSaved(r.PagerDutyRoutingKey) ? ViewerWebhookSealing.KeepHint : "";
        PagerDutyEuRegionCheckBox.IsChecked = r.PagerDutyUseEuRegion;
        PagerDutyAutoResolveCheckBox.IsChecked = r.PagerDutyAutoResolve;
        PagerDutyProxyAddressBox.Text = r.PagerDutyProxy;

        UpdateSmtpControlStates();
        UpdateTeamsControlStates();
        UpdateSlackControlStates();
        UpdateGenericControlStates();
        UpdatePagerDutyControlStates();
    }

    /// <summary>Builds the <see cref="NotificationRow"/> from the SMTP + webhook controls. A DISABLED channel
    /// writes EMPTY key fields so the service (which derives enablement from non-empty fields) treats it as off;
    /// an enabled SMTP channel seals the typed password (or preserves the existing blob when left unchanged).</summary>
    private NotificationRow BuildNotificationRowFromControls(SettingsValidationErrors errors)
    {
        var row = new NotificationRow();
        _smtpPasswordToSeal = null;

        if (int.TryParse(EmailCooldownBox.Text, out var emailCooldown) && emailCooldown is >= 1 and <= 120)
            row.EmailCooldownMinutes = emailCooldown;
        else
            errors.Add("Email alert cooldown must be between 1 and 120 minutes.", EmailCooldownBox);

        if (SmtpEnabledCheckBox.IsChecked == true)
        {
            row.SmtpHost = SmtpServerBox.Text?.Trim() ?? "";
            if (int.TryParse(SmtpPortBox.Text, out var port) && port is > 0 and < 65536)
                row.SmtpPort = port;
            row.SmtpUseSsl = SmtpSslCheckBox.IsChecked == true;
            var username = SmtpUsernameBox.Text?.Trim();
            row.SmtpUsername = string.IsNullOrWhiteSpace(username) ? null : username;
            row.SmtpFromAddress = SmtpFromBox.Text?.Trim() ?? "";
            row.SmtpRecipients = SmtpRecipientsBox.Text?.Trim() ?? "";
            row.EmailSendResolutions = EmailSendResolutionsCheckBox.IsChecked == true;
            ResolveSmtpPassword(row, errors);
        }

        if (TeamsWebhookEnabledCheckBox.IsChecked == true)
        {
            row.TeamsUrl = ViewerWebhookSealing.CarryKept(TeamsWebhookUrlBox.Text, _loadedNotification?.TeamsUrl);
            row.TeamsProxy = TeamsProxyAddressBox.Text?.Trim() ?? "";
            row.TeamsSendResolutions = TeamsSendResolutionsCheckBox.IsChecked == true;
        }

        if (SlackWebhookEnabledCheckBox.IsChecked == true)
        {
            row.SlackUrl = ViewerWebhookSealing.CarryKept(SlackWebhookUrlBox.Text, _loadedNotification?.SlackUrl);
            row.SlackProxy = SlackProxyAddressBox.Text?.Trim() ?? "";
            row.SlackSendResolutions = SlackSendResolutionsCheckBox.IsChecked == true;
        }

        if (GenericWebhookEnabledCheckBox.IsChecked == true)
        {
            row.GenericUrl = ViewerWebhookSealing.CarryKept(GenericWebhookUrlBox.Text, _loadedNotification?.GenericUrl);
            row.GenericHeaders = ViewerWebhookSealing.CarryKept(GenericWebhookHeadersBox.Text, _loadedNotification?.GenericHeaders);
            if (ViewerWebhookSealing.HeadersNeedUrlRetyped(
                    GenericWebhookHeadersBox.Text, GenericWebhookUrlBox.Text,
                    _loadedNotification?.GenericHeaders, _loadedNotification?.GenericUrl))
            {
                errors.Add(ViewerWebhookSealing.RetypeUrlForHeadersText, GenericWebhookUrlBox);
            }
            /* Persist the empty "use built-in default" sentinel unless the operator actually edited the body box
               (the Settings load pre-fills it with the default), so a future release can still improve it. */
            row.GenericBodyTemplate = WebhookAlertService.IsDefaultBodyTemplate(GenericWebhookBodyBox.Text)
                ? ""
                : GenericWebhookBodyBox.Text?.Trim() ?? "";
            row.GenericProxy = GenericWebhookProxyAddressBox.Text?.Trim() ?? "";
            row.GenericSendResolutions = GenericSendResolutionsCheckBox.IsChecked == true;

            /* A malformed headers JSON / body template would let the service accept the channel and then
               drop every alert with only a log line to show for it — block the Save instead (#1506). */
            var configError = WebhookAlertService.ValidateGenericConfig(
                PasswordSeal.IsSealed(row.GenericHeaders) || row.GenericHeaders == ViewerWebhookSealing.ClearMarker ? "" : row.GenericHeaders,
                row.GenericBodyTemplate);
            if (configError != null)
            {
                errors.Add(configError, GenericWebhookHeadersBox);
            }
        }

        if (PagerDutyWebhookEnabledCheckBox.IsChecked == true)
        {
            row.PagerDutyRoutingKey = ViewerWebhookSealing.CarryKept(PagerDutyRoutingKeyBox.Text, _loadedNotification?.PagerDutyRoutingKey);
            row.PagerDutyUseEuRegion = PagerDutyEuRegionCheckBox.IsChecked == true;
            row.PagerDutyAutoResolve = PagerDutyAutoResolveCheckBox.IsChecked == true;
            row.PagerDutyProxy = PagerDutyProxyAddressBox.Text?.Trim() ?? "";
        }

        return row;
    }

    // ── Generic webhook signing key (#5998) ──

    /// <summary>The key block of the generic webhook, as last read from the store. Null for a read-only seat.</summary>
    private ViewerSigningKeyState? _genericSigningState;

    /// <summary>Reads the settings row's key columns and puts the block in line with them. The key itself is never read in the
    /// clear: only whether a sealed key is saved and when the previous key stops being sent.</summary>
    private async Task RefreshGenericSigningKeyAsync()
    {
        _genericSigningState = null;
        var readOnly = _dataService is null || _dataService.IsReadOnly;
        if (!readOnly)
        {
            try
            {
                _genericSigningState = await _dataService!.GetSettingsSigningKeyStateAsync();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                ViewerLogger.Warn("SettingsWindow", "The generic webhook signing key state could not be read: " + ex.Message);
            }
        }

        GenericSigningKeyButton.Content = ViewerWebhookSigningKeys.ActionText(_genericSigningState);
        GenericSigningKeyButton.IsEnabled = !readOnly;
        GenericSigningKeyRemoveButton.IsEnabled = !readOnly && _genericSigningState is { HasKey: true };
        GenericSigningKeyStatusText.Text = readOnly
            ? "A read-only connection cannot make or remove a key."
            : ViewerWebhookSigningKeys.StatusText(_genericSigningState, DateTime.UtcNow);
        GenericUnsignedTestText.Text = _genericSigningState is { HasKey: true } ? ViewerWebhookSigningKeys.UnsignedTestText : "";
    }

    /// <summary>Shows a new key once, in a box to copy from. It is not logged, not copied for the person, and is gone when the window closes.</summary>
    private void ShowNewGenericSigningKey(string key)
    {
        GenericSigningKeyShownBox.Text = key;
        GenericSigningKeyShownText.Text = ViewerWebhookSigningKeys.ShownOnceText;
        GenericSigningKeyShownPanel.Visibility = Visibility.Visible;
        GenericSigningKeyShownBox.SelectAll();
    }

    private void HideNewGenericSigningKey()
    {
        GenericSigningKeyShownBox.Text = "";
        GenericSigningKeyShownText.Text = "";
        GenericSigningKeyShownPanel.Visibility = Visibility.Collapsed;
    }

    private async void GenericSigningKeyButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        /* A key is tied to the saved URL, so a URL typed here and not yet saved would be the wrong one. */
        if (!string.Equals(
                GenericWebhookUrlBox.Text?.Trim() ?? "",
                ViewerWebhookSealing.ShownText(_loadedNotification.GenericUrl).Trim(), StringComparison.Ordinal))
        {
            MessageBox.Show(ViewerWebhookSigningKeys.SaveUrlFirstText, "Signing key", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        if (_genericSigningState is { HasKey: true }
            && MessageBox.Show(
                $"Make a new key? The current key keeps signing alongside it for {WebhookSigning.RotationOverlapDays} days, so the receiver has time to switch.",
                "Rotate signing key", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
        {
            return;
        }

        GenericSigningKeyButton.IsEnabled = false;
        try
        {
            var key = await ViewerPasswordKey.GetSealKeyAsync(_dataService, this);
            if (key.Notice is { } notice)
            {
                MessageBox.Show(notice, "Signing key", MessageBoxButton.OK, MessageBoxImage.Information);
            }

            if (key.Sealer is null)
            {
                MessageBox.Show(key.Refusal ?? ViewerPasswordKey.NoKeyText, "Signing key", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            ShowNewGenericSigningKey(await _dataService.GenerateSettingsSigningKeyAsync(key.Sealer));
        }
        catch (ViewerPasswordRefusedException ex)
        {
            MessageBox.Show(ex.Message, "Signing key", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (ViewerSchemaSkewException ex)
        {
            MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MessageBox.Show("The signing key was not saved:\n\n" + ex.Message, "Signing key", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            await RefreshGenericSigningKeyAsync();
        }
    }

    private async void GenericSigningKeyRemoveButton_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null
            || MessageBox.Show(
                "Remove the signing key and the previous key? Posts to this URL go out unsigned from the next alert, and a receiver that checks signatures will refuse them.",
                "Remove signing key", MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            await _dataService.RemoveSettingsSigningKeyAsync();
            HideNewGenericSigningKey();
        }
        catch (ViewerReadOnlyException ex)
        {
            MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            MessageBox.Show("The signing key was not removed:\n\n" + ex.Message, "Signing key", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            await RefreshGenericSigningKeyAsync();
        }
    }

    /// <summary>
    /// The sentence that refuses a save that would leave a signing key unreadable (#5998), or null: a key is sealed for the stored
    /// generic URL text and the generic proxy, and a route's key for the settings row's proxy. It runs inside the save's own
    /// transaction (<see cref="ViewerDataService.UpsertNotificationCheckingSigningKeysAsync"/>), on rows that transaction has locked.
    /// </summary>
    private string? SigningKeyRefusal(NotificationRow row, ViewerSigningKeyState? stored, System.Collections.Generic.IReadOnlyList<int> routeIdsHoldingKeys) =>
        ViewerWebhookSigningKeys.SaveRefusal(stored, row.GenericUrl, row.GenericProxy, checkProxy: true)
            ?? ViewerWebhookSigningKeys.RouteKeyRefusal(routeIdsHoldingKeys, row.GenericProxy, stored?.Proxy ?? _loadedNotification.GenericProxy);

    /// <summary>
    /// Seals the typed webhook values for the settings row (#5366): kept values stay as stored, anything typed is sealed to the
    /// service's key for its slot, proxy and (for the headers) generic URL. The key is asked for only when something was typed.
    /// A key notice is shown once. Throws <see cref="ViewerPasswordRefusedException"/> when a value cannot be saved.
    /// </summary>
    private async Task SealWebhookValuesAsync(NotificationRow row)
    {
        ViewerPasswordSealer? sealer = null;
        string? refusal = null;
        if (ViewerWebhookSealing.NeedsKey(row, _loadedNotification))
        {
            var key = await ViewerPasswordKey.GetSealKeyAsync(_dataService!, this);
            sealer = key.Sealer;
            refusal = key.Refusal;
            if (key.Notice is { } notice)
            {
                MessageBox.Show(notice, "Settings", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        ViewerWebhookSealing.ResolveSettingsRow(row, _loadedNotification, sealer, refusal);
    }

    /// <summary>
    /// Whether this save changes a channel's proxy while a route holds a value sealed for the old proxy (#5366). A route list
    /// that cannot be read says nothing: the save itself does not depend on it.
    /// </summary>
    private async Task<bool> RouteValuesNeedEnteringAgainAsync(NotificationRow row)
    {
        try
        {
            var routes = await _dataService!.GetNotificationRoutesAsync();
            return ViewerWebhookSealing.RouteValuesNeedEnteringAgain(_loadedNotification, row, routes);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ViewerLogger.Warn("SettingsWindow", "The routes could not be read to check them against a proxy change: " + ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Decides the SMTP password for a save (#5366). A blank box keeps the stored value, and is refused when the host, port,
    /// SSL flag or user name changed (the stored password only opens for the settings it was sealed for). A typed password
    /// is sealed later, in <see cref="SealSmtpPasswordAsync"/>, once the service's key has been read; its text is checked here.
    /// </summary>
    private void ResolveSmtpPassword(NotificationRow row, SettingsValidationErrors errors)
    {
        _smtpPasswordToSeal = null;
        row.SmtpEncryptedPassword = _loadedNotification.SmtpEncryptedPassword;

        var typed = SmtpPasswordBox.Password;
        switch (ViewerSmtpSeal.Decide(_loadedNotification, null, row, typed))
        {
            case ViewerSmtpPasswordAction.Refuse:
                errors.Add(ViewerSmtpSeal.ReenterText, SmtpPasswordBox);
                break;
            case ViewerSmtpPasswordAction.Seal:
                if (!ViewerSmtpSeal.IsValidText(typed))
                {
                    errors.Add(ViewerPasswordSealer.PasswordCharactersText, SmtpPasswordBox);
                }
                else if (ViewerSmtpSeal.FirstInvalidBoundField(row.SmtpHost, row.SmtpUsername) is { } field)
                {
                    errors.Add(ViewerPasswordSealer.FieldCharactersText(field), SmtpServerBox);
                }
                else
                {
                    _smtpPasswordToSeal = typed;
                    row.SmtpEncryptedPassword = null;
                }

                break;
        }
    }

    /// <summary>
    /// Seals the typed SMTP password to the service's published key for the row's host, port, SSL flag and user name
    /// (#5366). Returns the sentence that says why not when it cannot; it never carries the password.
    /// </summary>
    private async Task<string?> SealSmtpPasswordAsync(NotificationRow row)
    {
        if (_smtpPasswordToSeal is null)
        {
            return null;
        }

        var key = await ViewerPasswordKey.GetSealKeyAsync(_dataService!, this);
        if (key.Notice is not null)
        {
            /* The window closes when the save succeeds, so the status line alone would never be seen. */
            SmtpStatusText.Text = key.Notice;
            MessageBox.Show(key.Notice, "SMTP password", MessageBoxButton.OK, MessageBoxImage.Information);
        }

        if (key.Sealer is null)
        {
            return key.Refusal;
        }

        try
        {
            row.SmtpEncryptedPassword = key.Sealer.SealSmtp(_smtpPasswordToSeal, row.SmtpHost, row.SmtpPort, row.SmtpUseSsl, row.SmtpUsername);
        }
        catch (ViewerPasswordRefusedException ex)
        {
            return ex.Message;
        }

        _smtpPasswordToSeal = null;
        return null;
    }

    private void SmtpEnabledCheckBox_Changed(object sender, RoutedEventArgs e) => UpdateSmtpControlStates();

    private void UpdateSmtpControlStates()
    {
        var enabled = SmtpEnabledCheckBox.IsChecked == true;
        SmtpServerBox.IsEnabled = enabled;
        SmtpPortBox.IsEnabled = enabled;
        SmtpSslCheckBox.IsEnabled = enabled;
        SmtpUsernameBox.IsEnabled = enabled;
        SmtpPasswordBox.IsEnabled = enabled;
        SmtpFromBox.IsEnabled = enabled;
        SmtpRecipientsBox.IsEnabled = enabled;
        EmailSendResolutionsCheckBox.IsEnabled = enabled;
        TestEmailButton.IsEnabled = enabled;
        ValidateSmtpButton.IsEnabled = enabled;
    }

    private void ValidateSmtpButton_Click(object sender, RoutedEventArgs e)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(SmtpServerBox.Text))
            errors.Add("SMTP server is required");
        if (!int.TryParse(SmtpPortBox.Text, out var port) || port is < 1 or > 65535)
            errors.Add("Port must be between 1 and 65535");
        if (string.IsNullOrWhiteSpace(SmtpFromBox.Text))
            errors.Add("From address is required");
        else if (!SmtpFromBox.Text.Trim().Contains('@'))
            errors.Add("From address must be a valid email");
        if (string.IsNullOrWhiteSpace(SmtpRecipientsBox.Text))
            errors.Add("Default recipients are needed for alerts that no notification route covers.");

        if (errors.Count == 0)
        {
            SmtpStatusText.Text = CleartextSmtpWarning(includeStoredPassword: true)
                ?? "Settings look good. Use 'Send Test Email' to verify delivery.";
        }
        else
        {
            SmtpStatusText.Text = "";
            MessageBox.Show(
                "SMTP configuration has issues:\n\n" + string.Join("\n", errors),
                "SMTP Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    /// <summary>
    /// The unencrypted-login warning for the SMTP settings as they stand in the boxes, or null. A Save keeps
    /// the store's existing password when the box is empty (<see cref="ResolveSmtpPassword"/>), so it counts that
    /// one too (<paramref name="includeStoredPassword"/>); a test sends only what the box holds.
    /// </summary>
    private string? CleartextSmtpWarning(bool includeStoredPassword) =>
        EmailSendCore.CleartextCredentialsWarning(
            SmtpSslCheckBox.IsChecked == true,
            SmtpUsernameBox.Text?.Trim(),
            !string.IsNullOrEmpty(SmtpPasswordBox.Password)
                || (includeStoredPassword && !string.IsNullOrEmpty(_loadedNotification.SmtpEncryptedPassword)));

    /// <summary>
    /// Non-blocking confirm shown at Save and Send Test when SSL is off and a login is set, so the username
    /// and password would go to the mail server unencrypted. Yes proceeds; No cancels the action.
    /// <paramref name="action"/> is the verb ("Save" / "Send"). Not blocked outright: an internal relay on
    /// port 25 is a real setup.
    /// </summary>
    private bool ConfirmCleartextSmtp(string action, bool includeStoredPassword)
    {
        if (SmtpEnabledCheckBox.IsChecked != true)
        {
            return true;
        }

        var warning = CleartextSmtpWarning(includeStoredPassword);
        if (warning == null)
        {
            return true;
        }

        var result = MessageBox.Show(
            $"{warning}\n\n{action} anyway?",
            "Unencrypted SMTP Login", MessageBoxButton.YesNo, MessageBoxImage.Warning);
        return result == MessageBoxResult.Yes;
    }

    private async void TestEmailButton_Click(object sender, RoutedEventArgs e)
    {
        if (!ConfirmCleartextSmtp("Send", includeStoredPassword: false))
        {
            return;
        }

        TestEmailButton.IsEnabled = false;
        TestEmailButton.Content = "Sending...";

        try
        {
            /* Build the test settings straight from the live UI (test before save), so the user verifies
               exactly what they typed. The shared EmailSendCore renders + sends — no store/service needed. */
            var settings = TestAlertSettings.FromUi(this);
            if (SmtpPasswordBox.Password.Length == 0 && !string.IsNullOrEmpty(_loadedNotification.SmtpEncryptedPassword))
            {
                /* The saved password is never read back into this window: nothing here can send with it. */
                MessageBox.Show(ViewerSmtpSeal.SavedCannotTestText, "Test Email", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var error = await EmailSendCore.SendTestEmailAsync(settings, s_branding);
            if (error == null)
            {
                MessageBox.Show("Test email sent successfully!", "Test Email", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send test email:\n\n{error}", "Test Email Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        finally
        {
            TestEmailButton.Content = "Send Test _Email";
            TestEmailButton.IsEnabled = true;
        }
    }

    private void TeamsWebhookEnabledCheckBox_Changed(object sender, RoutedEventArgs e) => UpdateTeamsControlStates();

    private void SlackWebhookEnabledCheckBox_Changed(object sender, RoutedEventArgs e) => UpdateSlackControlStates();

    private void UpdateTeamsControlStates()
    {
        var enabled = TeamsWebhookEnabledCheckBox.IsChecked == true;
        TeamsWebhookUrlBox.IsEnabled = enabled;
        TeamsProxyAddressBox.IsEnabled = enabled;
        TeamsSendResolutionsCheckBox.IsEnabled = enabled;
        TestTeamsButton.IsEnabled = enabled;
    }

    private void UpdateSlackControlStates()
    {
        var enabled = SlackWebhookEnabledCheckBox.IsChecked == true;
        SlackWebhookUrlBox.IsEnabled = enabled;
        SlackProxyAddressBox.IsEnabled = enabled;
        SlackSendResolutionsCheckBox.IsEnabled = enabled;
        TestSlackButton.IsEnabled = enabled;
    }

    private void GenericWebhookEnabledCheckBox_Changed(object sender, RoutedEventArgs e) => UpdateGenericControlStates();

    private void UpdateGenericControlStates()
    {
        var enabled = GenericWebhookEnabledCheckBox.IsChecked == true;
        GenericWebhookUrlBox.IsEnabled = enabled;
        GenericWebhookHeadersBox.IsEnabled = enabled;
        GenericWebhookBodyBox.IsEnabled = enabled;
        GenericWebhookProxyAddressBox.IsEnabled = enabled;
        GenericSendResolutionsCheckBox.IsEnabled = enabled;
        TestGenericButton.IsEnabled = enabled;
    }

    private void PagerDutyWebhookEnabledCheckBox_Changed(object sender, RoutedEventArgs e) => UpdatePagerDutyControlStates();

    private void UpdatePagerDutyControlStates()
    {
        var enabled = PagerDutyWebhookEnabledCheckBox.IsChecked == true;
        PagerDutyRoutingKeyBox.IsEnabled = enabled;
        PagerDutyEuRegionCheckBox.IsEnabled = enabled;
        PagerDutyAutoResolveCheckBox.IsEnabled = enabled;
        PagerDutyProxyAddressBox.IsEnabled = enabled;
        TestPagerDutyButton.IsEnabled = enabled;
    }

    /// <summary>Shows why a Send Test cannot run when its box is blank because the value is saved (#5366). True when it was shown.</summary>
    private static bool RefuseTestOfSavedValue(string? typed, string? stored, string label)
    {
        var text = ViewerWebhookSealing.CannotTestSavedValueText(typed, stored, label);
        if (text is null)
        {
            return false;
        }

        MessageBox.Show(text, "Test Webhook", MessageBoxButton.OK, MessageBoxImage.Information);
        return true;
    }

    private async void TestPagerDutyButton_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseTestOfSavedValue(PagerDutyRoutingKeyBox.Text, _loadedNotification.PagerDutyRoutingKey, "PagerDuty routing key"))
        {
            return;
        }

        TestPagerDutyButton.IsEnabled = false;
        TestPagerDutyButton.Content = "Sending...";

        try
        {
            var routingKey = PagerDutyRoutingKeyBox.Text?.Trim() ?? "";
            var useEuRegion = PagerDutyEuRegionCheckBox.IsChecked == true;
            var error = await WebhookAlertService.SendTestPagerDutyAsync(routingKey, useEuRegion, s_branding, PagerDutyProxyAddressBox.Text?.Trim());

            if (error == null)
            {
                MessageBox.Show("PagerDuty test notification sent successfully!", "Test Webhook", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send PagerDuty test notification:\n\n{error}", "Test Webhook Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to send PagerDuty test notification:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            TestPagerDutyButton.Content = "Send Test Notification";
            TestPagerDutyButton.IsEnabled = true;
        }
    }

    private async void TestTeamsButton_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseTestOfSavedValue(TeamsWebhookUrlBox.Text, _loadedNotification.TeamsUrl, "Teams webhook URL"))
        {
            return;
        }

        TestTeamsButton.IsEnabled = false;
        TestTeamsButton.Content = "Sending...";

        try
        {
            var url = TeamsWebhookUrlBox.Text?.Trim() ?? "";
            var proxy = TeamsProxyAddressBox.Text?.Trim();
            var error = await WebhookAlertService.SendTestTeamsAsync(url, proxy, s_branding);

            if (error == null)
            {
                MessageBox.Show("Teams test notification sent successfully!", "Test Webhook", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send Teams test notification:\n\n{error}", "Test Webhook Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to send Teams test notification:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            TestTeamsButton.Content = "Send Test to _Teams";
            TestTeamsButton.IsEnabled = true;
        }
    }

    private async void TestSlackButton_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseTestOfSavedValue(SlackWebhookUrlBox.Text, _loadedNotification.SlackUrl, "Slack webhook URL"))
        {
            return;
        }

        TestSlackButton.IsEnabled = false;
        TestSlackButton.Content = "Sending...";

        try
        {
            var url = SlackWebhookUrlBox.Text?.Trim() ?? "";
            var proxy = SlackProxyAddressBox.Text?.Trim();
            var error = await WebhookAlertService.SendTestSlackAsync(url, proxy, s_branding);

            if (error == null)
            {
                MessageBox.Show("Slack test notification sent successfully!", "Test Webhook", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send Slack test notification:\n\n{error}", "Test Webhook Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to send Slack test notification:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            TestSlackButton.Content = "Send Test to Slac_k";
            TestSlackButton.IsEnabled = true;
        }
    }

    /// <summary>
    /// Non-blocking confirm shown when an http:// generic-webhook URL carries headers — the Authorization
    /// token would go on the wire in cleartext (#1506). Yes proceeds; No cancels the action. <paramref
    /// name="action"/> is the verb ("Save" / "Send"). Not blocked outright: a plaintext POST to a trusted LAN
    /// listener is a legitimate setup.
    /// </summary>
    private static bool ConfirmCleartextWebhook(string action)
    {
        var result = MessageBox.Show(
            "The webhook URL uses http://, so the Authorization header and any credentials are sent in cleartext " +
            $"and can be intercepted on the network.\n\n{action} anyway?",
            "Insecure Webhook URL", MessageBoxButton.YesNo, MessageBoxImage.Warning);
        return result == MessageBoxResult.Yes;
    }

    /// <summary>
    /// Tests the generic webhook with the values currently in the boxes (not the stored ones), like the
    /// Teams/Slack test buttons. A malformed headers JSON or body template comes back as the error message
    /// rather than an exception, so the operator fixes it here instead of discovering it when a real alert
    /// silently fails to deliver.
    /// </summary>
    private async void TestGenericButton_Click(object sender, RoutedEventArgs e)
    {
        if (RefuseTestOfSavedValue(GenericWebhookUrlBox.Text, _loadedNotification.GenericUrl, "generic webhook URL")
            || RefuseTestOfSavedValue(GenericWebhookHeadersBox.Text, _loadedNotification.GenericHeaders, "generic webhook headers"))
        {
            return;
        }

        var url = GenericWebhookUrlBox.Text?.Trim() ?? "";
        var headers = GenericWebhookHeadersBox.Text?.Trim();

        /* Warn before a cleartext http:// POST would send the Authorization header unencrypted (#1506). */
        if (WebhookAlertService.IsCleartextHttpWithHeaders(url, headers) && !ConfirmCleartextWebhook("Send"))
        {
            return;
        }

        TestGenericButton.IsEnabled = false;
        TestGenericButton.Content = "Sending...";

        try
        {
            var body = GenericWebhookBodyBox.Text?.Trim();
            var proxy = GenericWebhookProxyAddressBox.Text?.Trim();
            var error = await WebhookAlertService.SendTestGenericAsync(url, headers, body, proxy, s_branding);

            if (error == null)
            {
                MessageBox.Show("Generic webhook test notification sent successfully!", "Test Webhook", MessageBoxButton.OK, MessageBoxImage.Information);
            }
            else
            {
                MessageBox.Show($"Failed to send generic webhook test notification:\n\n{error}", "Test Webhook Failed", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to send generic webhook test notification:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            TestGenericButton.Content = "Send Test to _Webhook";
            TestGenericButton.IsEnabled = true;
        }
    }

    private void Hyperlink_RequestNavigate(object sender, RequestNavigateEventArgs e)
    {
        try
        {
            Process.Start(new ProcessStartInfo { FileName = e.Uri.AbsoluteUri, UseShellExecute = true });
        }
        catch { /* A missing default browser must not crash the settings window. */ }
        e.Handled = true;
    }

    // ── Save / Close ──

    private async void SaveButton_Click(object sender, RoutedEventArgs e)
    {
        var errors = new SettingsValidationErrors();
        var mcpValid = TryReadServiceFlags(
            out var capturePlans, out var mcpEnabled, out var mcpPort, out var webEnabled, out var webPort,
            out var textBudgetMb, out var maxSweeps, out var fetchTopN);
        var alertRow = BuildAlertRowFromControls(errors);
        SaveViewerLocalAlertFields(errors);
        var notifyRow = BuildNotificationRowFromControls(errors);

        /* Cleartext warning (#1506): an http:// generic-webhook URL carrying headers sends the Authorization
           token in the clear. Confirm before persisting anything; No cancels the save and the window stays open
           to fix the URL. NOT blocked — a plaintext POST to a trusted LAN listener is legitimate. */
        if (GenericWebhookEnabledCheckBox.IsChecked == true
            && WebhookAlertService.IsCleartextHttpWithHeaders(GenericWebhookUrlBox.Text?.Trim(), GenericWebhookHeadersBox.Text?.Trim())
            && !ConfirmCleartextWebhook("Save"))
        {
            return;
        }

        /* An SMTP login that would go to the mail server unencrypted (SSL off with a password set) is confirmed
           the same way, before anything is persisted. NOT blocked - an internal relay on port 25 is a real
           setup. */
        if (!ConfirmCleartextSmtp("Save", includeStoredPassword: true))
        {
            return;
        }

        /* Persist the viewer-LOCAL preferences immediately (valid values applied above), and capture the
           edited viewer preferences for MainWindow to save + re-seed tabs from. */
        SaveConnectionTimeout();
        SaveNocRefreshInterval();
        SaveCsvSeparator();
        SaveTimeDisplayMode();
        SaveColorTheme();

        /* #2434: a whole-object replace that did not happen must not pass for one that did. Said here,
           at the point it happens, rather than folded into the validation list below — that list's
           sentence is about values the window rejected, which is a different thing from a file it could
           not write. The operator config further down goes to the Darling store and reports separately,
           so a failure here does not stop it. */
        if (!_appSettingsStore.Save(_appSettings))
        {
            MessageBox.Show(
                "The viewer's own settings could not be written to "
                + $"'{System.IO.Path.GetFileName(_appSettingsStore.FilePath)}', so the viewer-local "
                + "preferences on this page (theme, CSV separator, timestamp display, tray options) will be "
                + "back to their previous values on the next launch. The viewer log says why.",
                "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
        }

        Result = BuildViewerPreferences();

        if (errors.Count > 0)
        {
            /* #5846: the field may sit on a tab that is not showing. Open the first tab holding one, and name
               each message's tab, so the warning never points at a page the operator cannot see. */
            SelectFirstTabWithError(errors);
            MessageBox.Show(
                "Some settings have invalid values and were not saved:\n\n" + string.Join("\n", DescribeErrors(errors)),
                "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        if (!mcpValid)
        {
            return; /* TryReadServiceFlags already warned about the bad MCP or web dashboard port. */
        }

        /* Guard against clobbering: if the current settings could not be READ (a transient store error left the
           controls at defaults), do NOT write them back — that would overwrite an operator-tuned store with the
           on-screen defaults. Save the viewer-local preferences (already done above) and say so. */
        if (_dataService is not null && !_storeLoaded)
        {
            MessageBox.Show(
                "Your viewer preferences were saved. The current monitoring settings could not be read from the " +
                "Darling store (it may be unreachable, or the service hasn't finished initializing its settings " +
                "yet), so they were left unchanged to avoid overwriting them. Try again in a moment.",
                "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        /* Write the operator config to the control-plane store (the service reloads on the config_version bump).
           A read-only seat surfaces the friendly message and the window stays open. */
        if (_dataService is not null)
        {
            try
            {
                var sealRefusal = await SealSmtpPasswordAsync(notifyRow);
                if (sealRefusal is not null)
                {
                    SmtpStatusText.Text = sealRefusal;
                    MessageBox.Show(sealRefusal, "SMTP password not saved", MessageBoxButton.OK, MessageBoxImage.Warning);
                    return;
                }

                await SealWebhookValuesAsync(notifyRow);
                var routeValuesNeedEnteringAgain = await RouteValuesNeedEnteringAgainAsync(notifyRow);
                await _dataService.UpsertAlertSettingsAsync(alertRow);
                await _dataService.UpsertNotificationCheckingSigningKeysAsync(notifyRow, (stored, ids) => SigningKeyRefusal(notifyRow, stored, ids));
                await _dataService.UpdateServiceFlagsAsync(capturePlans, mcpEnabled, mcpPort, webEnabled, webPort,
                    QueryStoreBackfillCheckBox.IsChecked == true, textBudgetMb, maxSweeps, fetchTopN);
                if (routeValuesNeedEnteringAgain)
                {
                    /* The settings were saved; the routes' sealed values were bound to the old proxy (#5366). */
                    MessageBox.Show(
                        ViewerWebhookSealing.RouteValuesNeedEnteringAgainText,
                        "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
                }
            }
            catch (ViewerReadOnlyException ex)
            {
                MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            catch (ViewerPasswordRefusedException ex)
            {
                MessageBox.Show(
                    "The webhook settings were not saved:\n\n" + ex.Message,
                    "Settings", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            catch (ViewerSchemaSkewException ex)
            {
                MessageBox.Show(ex.Message, "Store out of date", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"The viewer-local preferences were saved, but the monitoring settings could not be written to the store:\n\n{ex.Message}",
                    "Settings", MessageBoxButton.OK, MessageBoxImage.Error);
                return;
            }
        }

        DialogResult = true;
        Close();
    }

    private void CloseButton_Click(object sender, RoutedEventArgs e)
    {
        /* Revert the live theme preview when closing without saving (the X button is handled in OnClosing). */
        if (!_themeSaved)
        {
            ThemeManager.Apply(_originalTheme);
        }
        Close();
    }

    protected override void OnClosing(System.ComponentModel.CancelEventArgs e)
    {
        RememberSelectedTab();

        /* Catches the title-bar X / Esc: an unsaved theme preview reverts to what was active on open. */
        if (!_themeSaved)
        {
            ThemeManager.Apply(_originalTheme);
        }
        base.OnClosing(e);
    }

    /// <summary>
    /// A throwaway <see cref="IAlertSettings"/> built from the live SMTP/webhook controls, so "Send Test
    /// Email" verifies exactly what the user typed without first saving (Lite's "test before save"). Only the
    /// SMTP members matter for the email test; the rest satisfy the interface with the current UI values.
    /// </summary>
    private sealed class TestAlertSettings : IAlertSettings
    {
        public bool SmtpEnabled { get; private init; }
        public string SmtpServer { get; private init; } = "";
        public int SmtpPort { get; private init; }
        public bool SmtpUseSsl { get; private init; }
        public string SmtpUsername { get; private init; } = "";
        public string SmtpFromAddress { get; private init; } = "";
        public string SmtpRecipients { get; private init; } = "";
        private string SmtpPassword { get; init; } = "";
        public string? GetSmtpPassword() => string.IsNullOrEmpty(SmtpPassword) ? null : SmtpPassword;

        public int EmailCooldownMinutes { get; private init; }

        public bool TeamsWebhookEnabled { get; private init; }
        public string TeamsWebhookUrl { get; private init; } = "";
        public string TeamsProxyAddress { get; private init; } = "";

        public bool SlackWebhookEnabled { get; private init; }
        public string SlackWebhookUrl { get; private init; } = "";
        public string SlackProxyAddress { get; private init; } = "";

        public bool GenericWebhookEnabled { get; private init; }
        public string GenericWebhookUrl { get; private init; } = "";
        public string GenericWebhookHeadersJson { get; private init; } = "";
        public string GenericWebhookBodyTemplate { get; private init; } = "";
        public string GenericWebhookProxyAddress { get; private init; } = "";

        public bool PagerDutyEnabled { get; private init; }
        public string PagerDutyRoutingKey { get; private init; } = "";
        public bool PagerDutyUseEuRegion { get; private init; }
        public string PagerDutyProxyAddress { get; private init; } = "";

        public double AnalysisNotifySeverity { get; private init; }
        public int AnalysisNotifyCooldownMinutes { get; private init; }
        public int AnalysisPageCap => 5;

        /* #2710: test sends never carry a triage link (the builders' isTest paths skip it anyway), and the
           Viewer edits the store, not the headless box's darling.json where web.publicBaseUrl lives. */
        public string TriageBaseUrl => "";

        public static TestAlertSettings FromUi(SettingsWindow w)
        {
            int.TryParse(w.SmtpPortBox.Text, out var smtpPort);
            int.TryParse(w.EmailCooldownBox.Text, out var emailCooldown);
            return new TestAlertSettings
            {
                SmtpEnabled = w.SmtpEnabledCheckBox.IsChecked == true,
                SmtpServer = w.SmtpServerBox.Text?.Trim() ?? "",
                SmtpPort = smtpPort,
                SmtpUseSsl = w.SmtpSslCheckBox.IsChecked == true,
                SmtpUsername = w.SmtpUsernameBox.Text?.Trim() ?? "",
                SmtpFromAddress = w.SmtpFromBox.Text?.Trim() ?? "",
                SmtpRecipients = w.SmtpRecipientsBox.Text?.Trim() ?? "",
                SmtpPassword = w.SmtpPasswordBox.Password,
                EmailCooldownMinutes = emailCooldown,
                TeamsWebhookEnabled = w.TeamsWebhookEnabledCheckBox.IsChecked == true,
                TeamsWebhookUrl = w.TeamsWebhookUrlBox.Text?.Trim() ?? "",
                TeamsProxyAddress = w.TeamsProxyAddressBox.Text?.Trim() ?? "",
                SlackWebhookEnabled = w.SlackWebhookEnabledCheckBox.IsChecked == true,
                SlackWebhookUrl = w.SlackWebhookUrlBox.Text?.Trim() ?? "",
                SlackProxyAddress = w.SlackProxyAddressBox.Text?.Trim() ?? "",
                GenericWebhookEnabled = w.GenericWebhookEnabledCheckBox.IsChecked == true,
                GenericWebhookUrl = w.GenericWebhookUrlBox.Text?.Trim() ?? "",
                GenericWebhookHeadersJson = w.GenericWebhookHeadersBox.Text?.Trim() ?? "",
                GenericWebhookBodyTemplate = w.GenericWebhookBodyBox.Text?.Trim() ?? "",
                GenericWebhookProxyAddress = w.GenericWebhookProxyAddressBox.Text?.Trim() ?? "",
                AnalysisNotifySeverity = w._appSettings.AnalysisNotifySeverity,
                AnalysisNotifyCooldownMinutes = w._appSettings.AnalysisNotifyCooldownMinutes,
            };
        }
    }
}
