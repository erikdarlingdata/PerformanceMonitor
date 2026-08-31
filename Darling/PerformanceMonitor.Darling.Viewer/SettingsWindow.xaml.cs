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
    /// <summary>Branding the shared email/webhook renderers stamp on test messages (mirrors the service's).</summary>
    private static readonly AlertBranding s_branding = new("Performance Monitor Darling", null);

    private readonly ViewerAppSettingsStore _appSettingsStore;
    private readonly ViewerAppSettings _appSettings;
    private readonly ViewerDataService? _dataService;
    private readonly IReadOnlyList<DarlingServer> _servers;

    /// <summary>The store's current SMTP blob, held so an unchanged (or undecryptable) password survives a Save
    /// without being wiped — mirrors the server dialog's DPAPI "re-enter to change" handling.</summary>
    private string? _loadedSmtpBlob;

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

        /* Manage Mute Rules writes the shared Postgres config; needs a live store connection. */
        ManageMuteRulesButton.IsEnabled = _dataService is not null;
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
            }

            if (sections.Service is not null)
            {
                CapturePlansCheckBox.IsChecked = sections.Service.CapturePlans;
                QueryStoreBackfillCheckBox.IsChecked = sections.Service.QueryStoreBackfillEnabled;
                QueryStoreTextBudgetMbTextBox.Text = sections.Service.QueryStoreTextBudgetMb.ToString(CultureInfo.InvariantCulture);
                MaxConcurrentSweepsTextBox.Text = sections.Service.MaxConcurrentSweeps.ToString(CultureInfo.InvariantCulture);
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
            AutoRefreshEnabled = preferences.AutoRefreshEnabled,
            AutoRefreshIntervalIndex = preferences.AutoRefreshIntervalIndex,
        }.Normalize();

        DefaultTimeRangeCombo.SelectedIndex = normalized.DefaultTimeRangeIndex;
        AutoRefreshCheckBox.IsChecked = normalized.AutoRefreshEnabled;
        AutoRefreshIntervalCombo.SelectedIndex = normalized.AutoRefreshIntervalIndex;
        AutoRefreshIntervalCombo.IsEnabled = normalized.AutoRefreshEnabled;
    }

    private ViewerPreferences BuildViewerPreferences() => new ViewerPreferences
    {
        DefaultTimeRangeIndex = DefaultTimeRangeCombo.SelectedIndex,
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
        try
        {
            var result = pausing
                ? await _dataService.PauseServiceAsync()
                : await _dataService.ResumeServiceAsync();

            if (result is null)
            {
                CollectionStatusText.Text = "Status: command sent — the service has not confirmed yet";
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
        out int textBudgetMb, out int maxSweeps)
    {
        capturePlans = CapturePlansCheckBox.IsChecked == true;
        textBudgetMb = 64;
        maxSweeps = 4;
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
            MessageBox.Show(
                "Web dashboard port must be between 1024 and 65535.\nPorts 0-1023 are well-known privileged ports reserved by the operating system.",
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        /* #2164 / #2170 collector memory knobs. Unlike the ports these are always in force (there is no
           "disabled" state to excuse a bad value), so a bad entry always blocks the save rather than
           silently keeping a last-known value. The service clamps on read as defense in depth. */
        if (int.TryParse(QueryStoreTextBudgetMbTextBox.Text, out var parsedBudget) && parsedBudget is >= 4 and <= 256)
        {
            textBudgetMb = parsedBudget;
        }
        else
        {
            MessageBox.Show(
                "Query Store text budget must be between 4 and 256 MB.",
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        if (int.TryParse(MaxConcurrentSweepsTextBox.Text, out var parsedSweeps) && parsedSweeps is >= 1 and <= 16)
        {
            maxSweeps = parsedSweeps;
        }
        else
        {
            MessageBox.Show(
                "Concurrent server sweeps must be between 1 and 16.",
                "Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
            return false;
        }

        return true;
    }

    private void CopyMcpCommandButton_Click(object sender, RoutedEventArgs e)
    {
        var port = McpPortTextBox.Text;
        var command = $"claude mcp add --transport http --scope user sql-monitor-darling http://localhost:{port}/";
        /* SetDataObject with copy=false avoids WPF's problematic Clipboard.Flush(). */
        Clipboard.SetDataObject(command, false);
        McpStatusText.Text = "Copied to clipboard!";
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
        Clipboard.SetDataObject(url, false);
        WebStatusText.Text = "Copied to clipboard!";
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
        AlertDeadlockCheckBox.IsChecked = r.DeadlockEnabled;
        AlertDeadlockThresholdBox.Text = r.DeadlockCountThreshold.ToString(CultureInfo.InvariantCulture);
        AlertPoisonWaitCheckBox.IsChecked = r.PoisonWaitEnabled;
        AlertPoisonWaitThresholdBox.Text = r.PoisonWaitThresholdMs.ToString(CultureInfo.InvariantCulture);
        AlertLongRunningQueryCheckBox.IsChecked = r.LongRunningQueryEnabled;
        AlertLongRunningQueryThresholdBox.Text = r.LongRunningQueryThresholdMinutes.ToString(CultureInfo.InvariantCulture);
        /* V20: the long-running-query read shape (max-results + the five noise filters) is store-backed now. */
        AlertLongRunningQueryMaxResultsBox.Text = r.LongRunningQueryMaxResults.ToString(CultureInfo.InvariantCulture);
        LrqExcludeSpServerDiagnosticsCheckBox.IsChecked = r.LongRunningQueryExcludeSpServerDiagnostics;
        LrqExcludeWaitForCheckBox.IsChecked = r.LongRunningQueryExcludeWaitFor;
        LrqExcludeBackupsCheckBox.IsChecked = r.LongRunningQueryExcludeBackups;
        LrqExcludeMiscWaitsCheckBox.IsChecked = r.LongRunningQueryExcludeMiscWaits;
        LrqExcludeCdcCheckBox.IsChecked = r.LongRunningQueryExcludeCdc;
        AlertExcludedDatabasesBox.Text = string.Join(", ", r.ExcludedDatabases);
        AlertTempDbSpaceCheckBox.IsChecked = r.TempDbSpaceEnabled;
        AlertTempDbSpaceThresholdBox.Text = r.TempDbSpaceThresholdPercent.ToString(CultureInfo.InvariantCulture);
        AlertLowDiskCheckBox.IsChecked = r.LowDiskEnabled;
        AlertLowDiskThresholdPercentBox.Text = r.LowDiskThresholdPercent.ToString(CultureInfo.InvariantCulture);
        AlertLowDiskThresholdGbBox.Text = r.LowDiskThresholdGb.ToString(CultureInfo.InvariantCulture);
        AlertDiskCriticalPercentBox.Text = r.DiskCriticalFreePercent.ToString(CultureInfo.InvariantCulture);
        AlertDiskCriticalGbBox.Text = r.DiskCriticalFreeGb.ToString(CultureInfo.InvariantCulture);
        AlertSelfDiskWarnPercentBox.Text = r.SelfDiskFreeWarnPercent.ToString(CultureInfo.InvariantCulture);
        AlertCollectionStaleMinutesBox.Text = r.CollectionStaleMinutes.ToString(CultureInfo.InvariantCulture);
        AlertCollectionFailureThresholdBox.Text = r.CollectionFailureThreshold.ToString(CultureInfo.InvariantCulture);
        AlertStoreJobCadenceWarnPercentBox.Text = r.StoreJobCadenceWarnPercent.ToString(CultureInfo.InvariantCulture);
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
        AlertCooldownBox.Text = r.CooldownMinutes.ToString(CultureInfo.InvariantCulture);
        AnalysisEnabledCheckBox.IsChecked = r.AnalysisEnabled;
        AnalysisIntervalBox.Text = r.AnalysisIntervalMinutes.ToString(CultureInfo.InvariantCulture);
        AnalysisNotificationsCheckBox.IsChecked = r.AnalysisNotificationsEnabled;
        AnalysisNotifySeverityBox.Text = r.AnalysisNotifySeverity.ToString("0.0", CultureInfo.InvariantCulture);
        AnalysisNotifyCooldownBox.Text = r.AnalysisNotifyCooldownMinutes.ToString(CultureInfo.InvariantCulture);
        /* #1141/#1236: the delivery mode + per-event cap are now STORE-backed (the service honors them),
           seeded from the row like every other alert-engine control. */
        AlertDeliveryModeBox.SelectedIndex = r.DeliveryMode == "PerEvent" ? 1 : 0;
        AlertPerEventMaxBox.Text = r.PerEventMax.ToString(CultureInfo.InvariantCulture);
        UpdateAlertControlStates();
    }

    /// <summary>Builds the store row from the alert + analysis controls, validating the numeric fields. Adds
    /// range errors to <paramref name="errors"/>; a bad field keeps its default rather than throwing.</summary>
    private AlertSettingsRow BuildAlertRowFromControls(List<string> errors)
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
            LongRunningQueryEnabled = AlertLongRunningQueryCheckBox.IsChecked == true,
            /* V20 long-running-query read shape (the five noise-filter opt-outs; max-results parsed below). */
            LongRunningQueryExcludeSpServerDiagnostics = LrqExcludeSpServerDiagnosticsCheckBox.IsChecked == true,
            LongRunningQueryExcludeWaitFor = LrqExcludeWaitForCheckBox.IsChecked == true,
            LongRunningQueryExcludeBackups = LrqExcludeBackupsCheckBox.IsChecked == true,
            LongRunningQueryExcludeMiscWaits = LrqExcludeMiscWaitsCheckBox.IsChecked == true,
            LongRunningQueryExcludeCdc = LrqExcludeCdcCheckBox.IsChecked == true,
            TempDbSpaceEnabled = AlertTempDbSpaceCheckBox.IsChecked == true,
            LowDiskEnabled = AlertLowDiskCheckBox.IsChecked == true,
            PvsEnabled = AlertPvsCheckBox.IsChecked == true,
            FileGrowthEnabled = AlertFileGrowthCheckBox.IsChecked == true,
            LongRunningJobEnabled = AlertLongRunningJobCheckBox.IsChecked == true,
            FailedJobEnabled = AlertFailedJobCheckBox.IsChecked == true,
            DatabaseStateEnabled = AlertDatabaseStateCheckBox.IsChecked == true,
            AnalysisEnabled = AnalysisEnabledCheckBox.IsChecked == true,
            AnalysisNotificationsEnabled = AnalysisNotificationsCheckBox.IsChecked == true,
            ExcludedDatabases = AlertExcludedDatabasesBox.Text
                .Split(',')
                .Select(s => s.Trim())
                .Where(s => s.Length > 0)
                .ToList(),
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
        if (int.TryParse(AlertPoisonWaitThresholdBox.Text, out var poisonWait) && poisonWait > 0)
            row.PoisonWaitThresholdMs = poisonWait;
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
        if (int.TryParse(AlertCollectionStaleMinutesBox.Text, out var staleMin) && staleMin is >= 5 and <= 1440)
            row.CollectionStaleMinutes = staleMin;
        if (int.TryParse(AlertCollectionFailureThresholdBox.Text, out var failThresh) && failThresh is >= 1 and <= 1000)
            row.CollectionFailureThreshold = failThresh;
        /* #2136: validated to the same range DarlingAlertSettings clamps ([5, 100]). */
        if (int.TryParse(AlertStoreJobCadenceWarnPercentBox.Text, out var cadencePct) && cadencePct is >= 5 and <= 100)
            row.StoreJobCadenceWarnPercent = cadencePct;
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
            errors.Add("Tray notification cooldown must be between 1 and 120 minutes.");

        if (int.TryParse(AnalysisIntervalBox.Text, out var analysisInterval) && analysisInterval is >= 5 and <= 360)
            row.AnalysisIntervalMinutes = analysisInterval;
        else
            errors.Add("Analysis interval must be between 5 and 360 minutes.");

        if (double.TryParse(AnalysisNotifySeverityBox.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var analysisSeverity)
            && analysisSeverity is >= 0.0 and <= 2.0)
            row.AnalysisNotifySeverity = analysisSeverity;
        else
            errors.Add("Analysis notify severity must be between 0.0 and 2.0.");

        if (int.TryParse(AnalysisNotifyCooldownBox.Text, out var analysisCooldown) && analysisCooldown is >= 30 and <= 10080)
            row.AnalysisNotifyCooldownMinutes = analysisCooldown;
        else
            errors.Add("Analysis re-notify cooldown must be between 30 and 10080 minutes.");

        /* #1141/#1236: delivery mode + per-event cap (store-backed). */
        row.DeliveryMode = AlertDeliveryModeBox.SelectedIndex == 1 ? "PerEvent" : "Summary";
        if (int.TryParse(AlertPerEventMaxBox.Text, out var perEventMax) && perEventMax is >= 1 and <= 100)
            row.PerEventMax = perEventMax;
        else
            errors.Add("Per-event max-per-cycle must be between 1 and 100.");

        return row;
    }

    /// <summary>Persists the viewer-LOCAL alert fields to <see cref="ViewerAppSettings"/> (the store never sees them).</summary>
    private void SaveViewerLocalAlertFields(List<string> errors)
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
        AlertPoisonWaitThresholdBox.Text = "500";
        AlertLongRunningQueryThresholdBox.Text = "30";
        /* V20: the long-running-query read shape resets to Lite's App defaults (5 rows, every filter on). */
        AlertLongRunningQueryMaxResultsBox.Text = "5";
        LrqExcludeSpServerDiagnosticsCheckBox.IsChecked = true;
        LrqExcludeWaitForCheckBox.IsChecked = true;
        LrqExcludeBackupsCheckBox.IsChecked = true;
        LrqExcludeMiscWaitsCheckBox.IsChecked = true;
        LrqExcludeCdcCheckBox.IsChecked = true;
        AlertTempDbSpaceThresholdBox.Text = "80";
        AlertLowDiskThresholdPercentBox.Text = "10";
        AlertLowDiskThresholdGbBox.Text = "5";
        /* #2107: the previously-hardcoded knobs reset to the constants they replaced. */
        AlertDiskCriticalPercentBox.Text = "3";
        AlertDiskCriticalGbBox.Text = "2";
        AlertSelfDiskWarnPercentBox.Text = "10";
        AlertCollectionStaleMinutesBox.Text = "30";
        AlertCollectionFailureThresholdBox.Text = "10";
        AlertStoreJobCadenceWarnPercentBox.Text = "25";
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
        AlertExcludedDatabasesBox.Text = "";
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
        if (AlertPoisonWaitCheckBox.IsChecked == true)
            parts.Add($"poison waits >= {AlertPoisonWaitThresholdBox.Text}ms avg");
        if (AlertLongRunningQueryCheckBox.IsChecked == true)
            parts.Add($"queries > {AlertLongRunningQueryThresholdBox.Text}min");
        if (AlertTempDbSpaceCheckBox.IsChecked == true)
            parts.Add($"tempdb > {AlertTempDbSpaceThresholdBox.Text}%");
        if (AlertLowDiskCheckBox.IsChecked == true)
            parts.Add($"disk free < {AlertLowDiskThresholdPercentBox.Text}% or {AlertLowDiskThresholdGbBox.Text}GB");
        if (AlertPvsCheckBox.IsChecked == true)
            parts.Add($"PVS >= {AlertPvsThresholdPercentBox.Text}% of database");
        if (AlertFileGrowthCheckBox.IsChecked == true)
            parts.Add($"file growth > {AlertFileGrowthRiseMbBox.Text}MB/{AlertFileGrowthLookbackMinutesBox.Text}m or volume > {AlertFileGrowthVolumePercentBox.Text}%");
        if (AlertLongRunningJobCheckBox.IsChecked == true)
            parts.Add($"jobs > {AlertLongRunningJobMultiplierBox.Text}x avg");
        if (AlertFailedJobCheckBox.IsChecked == true)
            parts.Add($"failed jobs (last {AlertFailedJobLookbackBox.Text}m)");

        AlertPreviewText.Text = parts.Count > 0
            ? $"Will alert when: {string.Join(", ", parts)}"
            : "No alerts enabled";
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
        AlertPoisonWaitCheckBox.IsEnabled = enabled;
        AlertPoisonWaitThresholdBox.IsEnabled = enabled;
        AlertLongRunningQueryCheckBox.IsEnabled = enabled;
        AlertLongRunningQueryThresholdBox.IsEnabled = enabled;
        /* V20 long-running-query read-shape controls follow the master switch like the rest of the engine. */
        AlertLongRunningQueryMaxResultsBox.IsEnabled = enabled;
        LrqExcludeSpServerDiagnosticsCheckBox.IsEnabled = enabled;
        LrqExcludeWaitForCheckBox.IsEnabled = enabled;
        LrqExcludeBackupsCheckBox.IsEnabled = enabled;
        LrqExcludeMiscWaitsCheckBox.IsEnabled = enabled;
        LrqExcludeCdcCheckBox.IsEnabled = enabled;
        AlertTempDbSpaceCheckBox.IsEnabled = enabled;
        AlertTempDbSpaceThresholdBox.IsEnabled = enabled;
        AlertLowDiskCheckBox.IsEnabled = enabled;
        AlertLowDiskThresholdPercentBox.IsEnabled = enabled;
        AlertPvsCheckBox.IsEnabled = enabled;
        AlertPvsThresholdPercentBox.IsEnabled = enabled;
        AlertPvsFloorGbBox.IsEnabled = enabled;
        AlertLowDiskThresholdGbBox.IsEnabled = enabled;
        /* #2107: the new threshold boxes follow the master switch like every sibling. */
        AlertDiskCriticalPercentBox.IsEnabled = enabled;
        AlertDiskCriticalGbBox.IsEnabled = enabled;
        AlertSelfDiskWarnPercentBox.IsEnabled = enabled;
        AlertCollectionStaleMinutesBox.IsEnabled = enabled;
        AlertCollectionFailureThresholdBox.IsEnabled = enabled;
        AlertStoreJobCadenceWarnPercentBox.IsEnabled = enabled;
        AlertFileGrowthCheckBox.IsEnabled = enabled;
        AlertFileGrowthRiseMbBox.IsEnabled = enabled;
        AlertFileGrowthVolumePercentBox.IsEnabled = enabled;
        AlertFileGrowthLookbackMinutesBox.IsEnabled = enabled;
        AlertLongRunningJobCheckBox.IsEnabled = enabled;
        AlertLongRunningJobMultiplierBox.IsEnabled = enabled;
        AlertFailedJobCheckBox.IsEnabled = enabled;
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

        _loadedSmtpBlob = r.SmtpEncryptedPassword;
        SmtpPasswordBox.Password = OperatingSystem.IsWindows()
            ? ViewerServerSecret.TryUnprotect(r.SmtpEncryptedPassword) ?? ""
            : "";

        TeamsWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.TeamsUrl);
        TeamsWebhookUrlBox.Text = r.TeamsUrl;
        TeamsProxyAddressBox.Text = r.TeamsProxy;
        SlackWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.SlackUrl);
        SlackWebhookUrlBox.Text = r.SlackUrl;
        SlackProxyAddressBox.Text = r.SlackProxy;

        GenericWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.GenericUrl);
        GenericWebhookUrlBox.Text = r.GenericUrl;
        GenericWebhookHeadersBox.Text = r.GenericHeaders;
        /* Blank means "use the built-in default" — show it, so the operator has something to edit rather
           than a blank box whose shape they have to guess. */
        GenericWebhookBodyBox.Text = string.IsNullOrWhiteSpace(r.GenericBodyTemplate)
            ? WebhookAlertService.DefaultGenericBodyTemplate
            : r.GenericBodyTemplate;
        GenericWebhookProxyAddressBox.Text = r.GenericProxy;

        PagerDutyWebhookEnabledCheckBox.IsChecked = !string.IsNullOrWhiteSpace(r.PagerDutyRoutingKey);
        PagerDutyRoutingKeyBox.Text = r.PagerDutyRoutingKey;
        PagerDutyEuRegionCheckBox.IsChecked = r.PagerDutyUseEuRegion;
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
    private NotificationRow BuildNotificationRowFromControls(List<string> errors)
    {
        var row = new NotificationRow();

        if (int.TryParse(EmailCooldownBox.Text, out var emailCooldown) && emailCooldown is >= 1 and <= 120)
            row.EmailCooldownMinutes = emailCooldown;
        else
            errors.Add("Email alert cooldown must be between 1 and 120 minutes.");

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
            row.SmtpEncryptedPassword = ResolveSmtpBlob();
        }

        if (TeamsWebhookEnabledCheckBox.IsChecked == true)
        {
            row.TeamsUrl = TeamsWebhookUrlBox.Text?.Trim() ?? "";
            row.TeamsProxy = TeamsProxyAddressBox.Text?.Trim() ?? "";
        }

        if (SlackWebhookEnabledCheckBox.IsChecked == true)
        {
            row.SlackUrl = SlackWebhookUrlBox.Text?.Trim() ?? "";
            row.SlackProxy = SlackProxyAddressBox.Text?.Trim() ?? "";
        }

        if (GenericWebhookEnabledCheckBox.IsChecked == true)
        {
            row.GenericUrl = GenericWebhookUrlBox.Text?.Trim() ?? "";
            row.GenericHeaders = GenericWebhookHeadersBox.Text?.Trim() ?? "";
            /* Persist the empty "use built-in default" sentinel unless the operator actually edited the body box
               (the Settings load pre-fills it with the default), so a future release can still improve it. */
            row.GenericBodyTemplate = WebhookAlertService.IsDefaultBodyTemplate(GenericWebhookBodyBox.Text)
                ? ""
                : GenericWebhookBodyBox.Text?.Trim() ?? "";
            row.GenericProxy = GenericWebhookProxyAddressBox.Text?.Trim() ?? "";

            /* A malformed headers JSON / body template would let the service accept the channel and then
               drop every alert with only a log line to show for it — block the Save instead (#1506). */
            var configError = WebhookAlertService.ValidateGenericConfig(row.GenericHeaders, row.GenericBodyTemplate);
            if (configError != null)
            {
                errors.Add(configError);
            }
        }

        if (PagerDutyWebhookEnabledCheckBox.IsChecked == true)
        {
            row.PagerDutyRoutingKey = PagerDutyRoutingKeyBox.Text?.Trim() ?? "";
            row.PagerDutyUseEuRegion = PagerDutyEuRegionCheckBox.IsChecked == true;
            row.PagerDutyProxy = PagerDutyProxyAddressBox.Text?.Trim() ?? "";
        }

        return row;
    }

    /// <summary>The SMTP blob to persist: seal the typed password when the box holds one; otherwise keep the
    /// store's existing blob (so an unchanged — or another-machine, undecryptable — password survives Save).</summary>
    private string? ResolveSmtpBlob()
    {
        var typed = SmtpPasswordBox.Password;
        if (!string.IsNullOrEmpty(typed) && OperatingSystem.IsWindows())
        {
            return ViewerServerSecret.Protect(typed);
        }

        return string.IsNullOrEmpty(typed) ? _loadedSmtpBlob : null;
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
            errors.Add("At least one recipient is required");

        if (errors.Count == 0)
        {
            SmtpStatusText.Text = "Settings look good. Use 'Send Test Email' to verify delivery.";
        }
        else
        {
            SmtpStatusText.Text = "";
            MessageBox.Show(
                "SMTP configuration has issues:\n\n" + string.Join("\n", errors),
                "SMTP Validation", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private async void TestEmailButton_Click(object sender, RoutedEventArgs e)
    {
        TestEmailButton.IsEnabled = false;
        TestEmailButton.Content = "Sending...";

        try
        {
            /* Build the test settings straight from the live UI (test before save), so the user verifies
               exactly what they typed. The shared EmailSendCore renders + sends — no store/service needed. */
            var settings = TestAlertSettings.FromUi(this);
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
        TestTeamsButton.IsEnabled = enabled;
    }

    private void UpdateSlackControlStates()
    {
        var enabled = SlackWebhookEnabledCheckBox.IsChecked == true;
        SlackWebhookUrlBox.IsEnabled = enabled;
        SlackProxyAddressBox.IsEnabled = enabled;
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
        TestGenericButton.IsEnabled = enabled;
    }

    private void PagerDutyWebhookEnabledCheckBox_Changed(object sender, RoutedEventArgs e) => UpdatePagerDutyControlStates();

    private void UpdatePagerDutyControlStates()
    {
        var enabled = PagerDutyWebhookEnabledCheckBox.IsChecked == true;
        PagerDutyRoutingKeyBox.IsEnabled = enabled;
        PagerDutyEuRegionCheckBox.IsEnabled = enabled;
        PagerDutyProxyAddressBox.IsEnabled = enabled;
        TestPagerDutyButton.IsEnabled = enabled;
    }

    private async void TestPagerDutyButton_Click(object sender, RoutedEventArgs e)
    {
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
        var errors = new List<string>();
        var mcpValid = TryReadServiceFlags(
            out var capturePlans, out var mcpEnabled, out var mcpPort, out var webEnabled, out var webPort,
            out var textBudgetMb, out var maxSweeps);
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
            MessageBox.Show(
                "Some settings have invalid values and were not saved:\n\n" + string.Join("\n", errors),
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
                await _dataService.UpsertAlertSettingsAsync(alertRow);
                await _dataService.UpsertNotificationAsync(notifyRow);
                await _dataService.UpdateServiceFlagsAsync(capturePlans, mcpEnabled, mcpPort, webEnabled, webPort,
                    QueryStoreBackfillCheckBox.IsChecked == true, textBudgetMb, maxSweeps);
            }
            catch (ViewerReadOnlyException ex)
            {
                MessageBox.Show(ex.Message, "Read-only connection", MessageBoxButton.OK, MessageBoxImage.Warning);
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
