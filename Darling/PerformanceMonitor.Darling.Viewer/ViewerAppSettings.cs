/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's per-USER application settings — the Darling equivalent of Performance Monitor Lite's
/// <c>App.*</c> statics (persisted to Lite's settings.json). This is the settings model the full
/// <see cref="SettingsWindow"/> port reads and writes: the MCP server toggle/port, every alert threshold
/// and toggle, notification options, SMTP (non-secret fields), Teams/Slack (non-secret fields), automated
/// analysis, the mute-rule default, and the viewer-side display preferences (connection timeout, CSV
/// separator, timestamp display mode). Every default mirrors Lite's <c>App.*</c> exactly.
///
/// <para>
/// This is deliberately distinct from the two existing viewer stores:
/// <see cref="ViewerPreferences"/> holds the tiny toolbar-seed defaults (time range + auto-refresh) that the
/// server tabs consume directly, and <see cref="ViewerSettings"/> reads the SERVICE's darling.json (the
/// Postgres connection). These are the front-end's own copy of the operator-facing settings.
/// </para>
///
/// <para>
/// Scope note (persistence adaptation, matching the rest of the Lite→Darling port): in Lite these values
/// drive Lite's in-process collector and alert engine; in Darling those are the SERVICE's domain
/// (darling.json). The viewer persists them here faithfully so the Settings window is complete and its
/// state survives a restart — making the running service honor a change is a separate wiring concern
/// (the service reads darling.json, not this file). The controls that DO act inside the viewer today —
/// Manage Mute Rules (writes Postgres), Send Test Email / Send Test Notification (the shared, connection-
/// independent renderers), the viewer preferences folded in, <see cref="MinimizeToTray"/> (hides the window
/// to the system tray on minimize), and the in-app alert toasts (the MainWindow polls alert history and
/// surfaces new rows through the ported tray, honoring the store-backed "Tray notification cooldown") —
/// work immediately.
/// </para>
///
/// <para>
/// Secrets (the SMTP password and the Teams/Slack webhook URLs) are NOT stored in this file — like Lite,
/// they go to Windows Credential Manager via <see cref="ViewerSecretStore"/>. The mutable properties here
/// are what <see cref="JsonSerializer"/> round-trips; <see cref="Normalize"/> clamps hand-edited or
/// forward-version values back into range on load.
/// </para>
/// </summary>
public sealed class ViewerAppSettings
{
    /* ---------------- MCP server (service's embedded MCP; darling.json mcp.*) ---------------- */

    /// <summary>Whether the Darling service's embedded MCP server should run. Persisted here; honored by the service.</summary>
    public bool McpEnabled { get; set; }

    /// <summary>MCP HTTP port. Darling's default (5152) avoids Lite (5151) and the Dashboard (5150).</summary>
    public int McpPort { get; set; } = 5152;

    /// <summary>Whether the Darling service's read-only web dashboard should run. Persisted here (an immediate
    /// non-blank seed for the Settings window); the control-plane store is authoritative and honored by the service.</summary>
    public bool WebEnabled { get; set; }

    /// <summary>Web dashboard HTTP port. Darling's default (5153) avoids the MCP family (5150-5152).</summary>
    public int WebPort { get; set; } = 5153;

    /* ---------------- Viewer display preferences ---------------- */

    /// <summary>Connection timeout in seconds for the viewer's store reads (5-60).</summary>
    public int ConnectionTimeoutSeconds { get; set; } = 5;

    /// <summary>
    /// How often (seconds) the shell auto-refreshes the fleet Overview and the aggregate tabs — the single
    /// operator knob over what were two hardcoded cadences (60s aggregate / 30s Overview), mirroring the
    /// Dashboard's one <c>NocRefreshIntervalSeconds</c>. Both fleet-scope timers run at this interval so an
    /// operator can dial the store-query load back at 500-server scale. Default 30 (the Dashboard's default and
    /// the former Overview cadence). Clamped to 10–600.
    /// </summary>
    public int NocRefreshIntervalSeconds { get; set; } = 30;

    /// <summary>CSV export separator: "," ";" or a tab. Auto-detected from locale on first run.</summary>
    public string CsvSeparator { get; set; } = DefaultCsvSeparator();

    /// <summary>Timestamp display mode: "ServerTime", "LocalTime", or "UTC".</summary>
    public string TimeDisplayMode { get; set; } = "ServerTime";

    /// <summary>Overview tile sort: "Cpu" (CPU% descending, default) or "Name".</summary>
    public string OverviewSortMode { get; set; } = "Cpu";

    /// <summary>Color theme: "Dark" (default), "Light", or "CoolBreeze". Mirrors Lite's <c>App.ColorTheme</c>;
    /// applied at startup and live-previewed from the Settings window via the shared <c>ThemeManager</c>.</summary>
    public string ColorTheme { get; set; } = "Dark";

    /* ---------------- Notifications / alert thresholds (mirrors Lite App.*) ---------------- */

    public bool MinimizeToTray { get; set; } = true;
    public bool AlertsEnabled { get; set; } = true;

    /// <summary>Whether the service delivers the Server-Unreachable/Restored connect-edge alerts. Store-backed since
    /// V20 (the service honors it); retained here only as the pre-3b migrate-in source, its default matching
    /// <see cref="AlertSettingsRow.Defaults"/> so a fresh viewer imports nothing.</summary>
    public bool NotifyConnectionChanges { get; set; } = true;

    public bool AlertCpuEnabled { get; set; } = true;
    public int AlertCpuThreshold { get; set; } = 80;

    /// <summary>Which CPU metric the alert evaluates against: "Total" (SQL + other processes) or "SqlOnly".</summary>
    public string AlertCpuMode { get; set; } = "Total";

    public bool AlertBlockingEnabled { get; set; } = true;
    public int AlertBlockingThreshold { get; set; } = 1;
    public bool AlertDeadlockEnabled { get; set; } = true;
    public int AlertDeadlockThreshold { get; set; } = 1;
    public bool AlertPoisonWaitEnabled { get; set; } = true;
    public int AlertPoisonWaitThresholdMs { get; set; } = 500;

    public bool AlertLongRunningQueryEnabled { get; set; } = true;
    public int AlertLongRunningQueryThresholdMinutes { get; set; } = 30;

    /* The long-running-query read shape (max-results + the five noise filters). Store-backed since V20 (the
       service forwards them to the LRQ read); retained here only as the pre-3b migrate-in source, defaults
       matching AlertSettingsRow.Defaults so a fresh viewer imports nothing. */
    public int AlertLongRunningQueryMaxResults { get; set; } = 5;
    public bool AlertLongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;
    public bool AlertLongRunningQueryExcludeWaitFor { get; set; } = true;
    public bool AlertLongRunningQueryExcludeBackups { get; set; } = true;
    public bool AlertLongRunningQueryExcludeMiscWaits { get; set; } = true;
    public bool AlertLongRunningQueryExcludeCdc { get; set; } = true;

    public List<string> AlertExcludedDatabases { get; set; } = new();

    public bool AlertTempDbSpaceEnabled { get; set; } = true;
    public int AlertTempDbSpaceThresholdPercent { get; set; } = 80;

    public bool AlertLowDiskEnabled { get; set; } = true;
    public int AlertLowDiskThresholdPercent { get; set; } = 10;
    public int AlertLowDiskThresholdGb { get; set; } = 5;

    public bool AlertLongRunningJobEnabled { get; set; } = true;
    public int AlertLongRunningJobMultiplier { get; set; } = 3;
    public bool AlertFailedJobEnabled { get; set; } = true;
    public int AlertFailedJobLookbackMinutes { get; set; } = 60;

    public int AlertCooldownMinutes { get; set; } = 5;
    public int EmailCooldownMinutes { get; set; } = 15;

    /// <summary>Deadlock/blocking notification delivery: "Summary" (one card per cycle) or "PerEvent".
    /// Store-backed since V18 (the service honors it); retained here only as the pre-3b migrate-in source, so its
    /// defaults match <see cref="AlertSettingsRow.Defaults"/> (Summary / 5) — a fresh viewer imports nothing.</summary>
    public string AlertDeliveryMode { get; set; } = "Summary";
    public int AlertPerEventMaxPerCycle { get; set; } = 5;

    /// <summary>Default expiration for new mute rules: "1 hour", "24 hours", "7 days", or "Never".</summary>
    public string MuteRuleDefaultExpiration { get; set; } = "24 hours";
    public bool LogAlertDismissals { get; set; } = true;

    public bool AnalysisEnabled { get; set; } = true;
    public bool AnalysisNotificationsEnabled { get; set; }
    public int AnalysisIntervalMinutes { get; set; } = 30;
    public double AnalysisNotifySeverity { get; set; } = 1.5;
    public int AnalysisNotifyCooldownMinutes { get; set; } = 360;

    /* ---------------- SMTP (non-secret; password lives in Credential Manager) ---------------- */

    public bool SmtpEnabled { get; set; }
    public string SmtpServer { get; set; } = "";
    public int SmtpPort { get; set; } = 587;
    public bool SmtpUseSsl { get; set; } = true;
    public string SmtpUsername { get; set; } = "";
    public string SmtpFromAddress { get; set; } = "";
    public string SmtpRecipients { get; set; } = "";

    /* ---------------- Webhooks (non-secret; URLs live in Credential Manager) ---------------- */

    public bool TeamsWebhookEnabled { get; set; }
    public string TeamsProxyAddress { get; set; } = "";
    public bool SlackWebhookEnabled { get; set; }
    public string SlackProxyAddress { get; set; } = "";

    /// <summary>
    /// Auto-detect the CSV separator: semicolon when the locale's decimal separator is a comma
    /// (Italian, German, French, ...), otherwise a comma. Mirrors Lite's <c>App.GetDefaultCsvSeparator</c>.
    /// </summary>
    internal static string DefaultCsvSeparator() =>
        CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator == "," ? ";" : ",";

    /// <summary>
    /// Clamps every numeric value back into the range the Settings window accepts and coerces the small
    /// string enums to a known value, then returns this same instance for chaining. Mirrors the clamps in
    /// Lite's <c>App.LoadAlertSettings</c> plus the window's save-time validation, so a corrupt, hand-edited,
    /// or forward-version file can never seed an out-of-range control. Pure and file-I/O-free (unit-testable).
    /// </summary>
    internal ViewerAppSettings Normalize()
    {
        McpPort = Clamp(McpPort, 1024, 65535, 5152);
        WebPort = Clamp(WebPort, 1024, 65535, 5153);
        ConnectionTimeoutSeconds = Clamp(ConnectionTimeoutSeconds, 5, 60, 5);
        NocRefreshIntervalSeconds = Clamp(NocRefreshIntervalSeconds, 10, 600, 30);
        CsvSeparator = (CsvSeparator == "," || CsvSeparator == ";" || CsvSeparator == "\t") ? CsvSeparator : DefaultCsvSeparator();
        TimeDisplayMode = (TimeDisplayMode is "ServerTime" or "LocalTime" or "UTC") ? TimeDisplayMode : "ServerTime";
        OverviewSortMode = (OverviewSortMode is "Cpu" or "Name") ? OverviewSortMode : "Cpu";
        ColorTheme = (ColorTheme is "Dark" or "Light" or "CoolBreeze") ? ColorTheme : "Dark";

        AlertCpuThreshold = Clamp(AlertCpuThreshold, 1, 100, 80);
        AlertCpuMode = (AlertCpuMode is "Total" or "SqlOnly") ? AlertCpuMode : "Total";
        AlertBlockingThreshold = Clamp(AlertBlockingThreshold, 1, int.MaxValue, 1);
        AlertDeadlockThreshold = Clamp(AlertDeadlockThreshold, 1, int.MaxValue, 1);
        AlertPoisonWaitThresholdMs = Clamp(AlertPoisonWaitThresholdMs, 1, int.MaxValue, 500);
        AlertLongRunningQueryThresholdMinutes = Clamp(AlertLongRunningQueryThresholdMinutes, 1, int.MaxValue, 30);
        AlertLongRunningQueryMaxResults = Clamp(AlertLongRunningQueryMaxResults, 1, 1000, 5);
        AlertTempDbSpaceThresholdPercent = Clamp(AlertTempDbSpaceThresholdPercent, 1, 100, 80);
        AlertLowDiskThresholdPercent = Clamp(AlertLowDiskThresholdPercent, 0, 100, 10);
        AlertLowDiskThresholdGb = Clamp(AlertLowDiskThresholdGb, 0, int.MaxValue, 5);
        AlertLongRunningJobMultiplier = Clamp(AlertLongRunningJobMultiplier, 2, 20, 3);
        AlertFailedJobLookbackMinutes = Clamp(AlertFailedJobLookbackMinutes, 1, 1440, 60);
        AlertCooldownMinutes = Clamp(AlertCooldownMinutes, 1, 120, 5);
        EmailCooldownMinutes = Clamp(EmailCooldownMinutes, 1, 120, 15);
        AlertDeliveryMode = (AlertDeliveryMode is "Summary" or "PerEvent") ? AlertDeliveryMode : "Summary";
        AlertPerEventMaxPerCycle = Clamp(AlertPerEventMaxPerCycle, 1, 100, 5);
        MuteRuleDefaultExpiration = (MuteRuleDefaultExpiration is "1 hour" or "24 hours" or "7 days" or "Never")
            ? MuteRuleDefaultExpiration : "24 hours";

        AnalysisIntervalMinutes = Clamp(AnalysisIntervalMinutes, 5, 360, 30);
        AnalysisNotifySeverity = Math.Clamp(AnalysisNotifySeverity, 0.0, 2.0);
        AnalysisNotifyCooldownMinutes = Clamp(AnalysisNotifyCooldownMinutes, 30, 10080, 360);

        if (SmtpPort is < 1 or > 65535)
        {
            SmtpPort = 587;
        }

        AlertExcludedDatabases ??= new List<string>();

        return this;
    }

    private static int Clamp(int value, int min, int max, int fallback)
    {
        if (value < min || value > max)
        {
            /* Snap a value that landed off the low/high end to the nearest bound, but a completely
               absent (zero) numeric on a min>=1 field reads as "unset" -> use the historical default. */
            return value <= 0 && min >= 1 ? fallback : Math.Clamp(value, min, max);
        }

        return value;
    }
}

/// <summary>
/// Loads and saves <see cref="ViewerAppSettings"/> as a JSON file in the viewer's per-user app-data
/// directory, mirroring <see cref="ViewerPreferencesStore"/> exactly (indented JSON, defaults on a missing
/// or corrupt file, an injectable path so tests round-trip against a temp file). Default path:
/// %APPDATA%\PerformanceMonitorDarling\viewer-settings.json — the same Darling-branded per-user folder
/// <see cref="ViewerPreferencesStore"/> uses, with the file namespaced to the settings.
/// </summary>
public sealed class ViewerAppSettingsStore
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    /// <summary>The <see cref="ViewerLogger"/> source every diagnostic from this store is filed under.</summary>
    private const string LogSource = "ViewerAppSettingsStore";

    private readonly string _filePath;

    /// <param name="filePath">Override the on-disk location (tests pass a temp file); null uses <see cref="DefaultFilePath"/>.</param>
    public ViewerAppSettingsStore(string? filePath = null)
    {
        _filePath = filePath ?? DefaultFilePath();
    }

    /// <summary>The resolved settings file path (surfaced mainly so tests and diagnostics can name it).</summary>
    public string FilePath => _filePath;

    /// <summary>
    /// What the last <see cref="Load"/> on this instance found: <see cref="SettingsFileState.Absent"/> for
    /// a first run, <see cref="SettingsFileState.Readable"/> for an ordinary one, and
    /// <see cref="SettingsFileState.Unreadable"/> when defaults were substituted for a file that is still
    /// on disk. <see cref="MainWindow"/> reads it to raise ONE startup dialog for the last case; every
    /// other caller can ignore it, because <see cref="ViewerSettingsFile"/> has already logged.
    /// </summary>
    public SettingsFileState LastLoadState { get; private set; } = SettingsFileState.Absent;

    /// <summary>Why the last <see cref="Load"/> could not read the file — the line and position of a parse
    /// error where there is one — or null when there was nothing wrong with it.</summary>
    public string? LastLoadProblem { get; private set; }

    /// <summary>
    /// The settings the last <see cref="Load"/> could not read, by NAME, and empty when there were none
    /// (#2456). Non-empty means the rest of the file loaded normally and only these reverted to their
    /// defaults — the distinction <see cref="LastLoadProblem"/> alone cannot make, and the reason the
    /// startup dialog can now say which settings were lost instead of only where the parse stopped.
    /// </summary>
    public IReadOnlyList<SettingsMemberProblem> LastLoadUnreadableMembers { get; private set; } =
        Array.Empty<SettingsMemberProblem>();

    /// <summary>%APPDATA%\PerformanceMonitorDarling\viewer-settings.json.</summary>
    public static string DefaultFilePath()
    {
        var directory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "PerformanceMonitorDarling");
        return Path.Combine(directory, "viewer-settings.json");
    }

    /// <summary>
    /// Reads the settings, returning defaults when the file does not exist yet or cannot be read/parsed.
    /// Loaded values are normalized into range. A file that is present and unreadable is reported to the
    /// log and left on <see cref="LastLoadState"/> — it is never treated as a first run (#2434).
    /// </summary>
    public ViewerAppSettings Load()
    {
        var read = ViewerSettingsFile.Load<ViewerAppSettings>(_filePath, LogSource, s_jsonOptions);
        LastLoadState = read.State;
        LastLoadProblem = read.Problem;
        LastLoadUnreadableMembers = read.UnreadableMembers ?? Array.Empty<SettingsMemberProblem>();
        return read.Value!.Normalize();
    }

    /// <summary>
    /// Writes the settings as indented JSON, creating the app-data directory on first save, and returns
    /// whether the write happened. False means nothing reached disk — either the write itself failed, or
    /// the existing file could not be read AND could not be copied aside, in which case it is deliberately
    /// left exactly as it is rather than replaced with defaults (#2434).
    /// </summary>
    public bool Save(ViewerAppSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        return ViewerSettingsFile.Save(_filePath, settings, LogSource, s_jsonOptions);
    }
}

/// <summary>
/// The one <see cref="ViewerAppSettings"/> value the viewer honors at RUNTIME today: the CSV export
/// separator, read by the grid copy/export handlers (they call <c>DataGridExport.ExportToCsv(..., separator)</c>).
/// Held as a process-wide value that <see cref="MainWindow"/> seeds from the persisted settings on startup and
/// re-applies whenever the Settings window closes, so a change takes effect on the next export without a
/// restart — mirroring how Lite's export reads <c>App.CsvSeparator</c>. The connection timeout is honored at
/// CONNECT (applied to the store connection string in the <see cref="ViewerDataService"/> constructor) and the
/// fleet refresh interval drives the shell's aggregate/Overview timers; the remaining persisted settings
/// (alerts/SMTP/webhooks/MCP/timestamp-display) are the service's or a larger wiring concern.
/// </summary>
public static class ViewerExportSettings
{
    /// <summary>Current CSV export separator ("," ";" or a tab). Defaults to the locale-based default.</summary>
    public static string CsvSeparator { get; set; } = ViewerAppSettings.DefaultCsvSeparator();

    /// <summary>Seeds the runtime export values from a loaded settings instance.</summary>
    public static void Apply(ViewerAppSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        CsvSeparator = settings.CsvSeparator;
    }
}

/// <summary>
/// The viewer's secret store for the Settings window: the SMTP password and the Teams/Slack webhook URLs,
/// kept out of viewer-settings.json and in Windows Credential Manager (DPAPI-encrypted, per-user), exactly
/// as Lite keeps them. Wraps the shared <see cref="CredentialStore"/> with a Darling-specific prefix so the
/// viewer's secrets never collide with Lite's or the Dashboard's. Failures are swallowed to null/no-op —
/// a Settings window must never crash because the credential vault is unavailable.
/// </summary>
public static class ViewerSecretStore
{
    /* Distinct per-app prefix (see CredentialStore's load-bearing-prefix note). */
    private static readonly CredentialStore s_store = new("PerformanceMonitorDarling_");

    private const string SmtpKey = "SMTP";
    private const string TeamsKey = "TeamsWebhook";
    private const string SlackKey = "SlackWebhook";

    public static string? GetSmtpPassword() => s_store.GetCredential(SmtpKey)?.Password;

    public static void SaveSmtpPassword(string username, string password) =>
        s_store.SaveCredential(SmtpKey, string.IsNullOrEmpty(username) ? "smtp" : username, password);

    public static string GetTeamsWebhookUrl() => s_store.GetCredential(TeamsKey)?.Password ?? "";

    public static string GetSlackWebhookUrl() => s_store.GetCredential(SlackKey)?.Password ?? "";

    public static void SaveTeamsWebhookUrl(string url) => SaveWebhookUrl(TeamsKey, url);

    public static void SaveSlackWebhookUrl(string url) => SaveWebhookUrl(SlackKey, url);

    private static void SaveWebhookUrl(string key, string url)
    {
        if (string.IsNullOrWhiteSpace(url))
        {
            s_store.DeleteCredential(key);
        }
        else
        {
            s_store.SaveCredential(key, "webhook", url);
        }
    }
}
