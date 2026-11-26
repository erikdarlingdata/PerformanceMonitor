using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Windows;
using System.Windows.Threading;
using ModelContextProtocol.Server;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// <c>update_alert_settings</c> (#9639), the twin of Darling's tool of the same name: a PARTIAL update of the alert
/// settings Settings &gt; Alerts edits, in the nested shape <c>get_alert_settings</c> returns. The tool sets the same
/// <see cref="App"/> statics the window's Save sets and writes settings.json through
/// <see cref="SettingsWindow.WriteAlertKeys"/>, the method the window's Save calls, so the running app uses the new
/// value at once, as it does after a Save, and the next launch reads it back.
/// </summary>
[McpServerToolType]
public sealed class McpAlertSettingsWriteTools
{
    private enum Kind { Bool, Int, Long, Double, Text, List }

    /// <summary>A refusal: the wire name of the field that caused it, and the sentence.</summary>
    private readonly record struct Refused(string Parameter, string Message);

    /// <summary>One writable setting: its wire path, its type and range, and how to read and set the static it maps to.
    /// The ranges are the Settings window's, so a value the window refuses is refused here too.</summary>
    private sealed record Field(string Path, Kind Kind, double Min, double Max, Func<object> Get, Action<object> Set, string[]? Allowed = null);

    private static Field B(string path, Func<bool> get, Action<bool> set) =>
        new(path, Kind.Bool, 0, 0, () => get(), v => set((bool)v));

    private static Field I(string path, double min, double max, Func<int> get, Action<int> set) =>
        new(path, Kind.Int, min, max, () => get(), v => set((int)v));

    private static Field L(string path, Func<List<string>> get, Action<List<string>> set) =>
        new(path, Kind.List, 0, 0, () => get(), v => set((List<string>)v));

    private static readonly Field[] s_fields =
    {
        B("alerts_enabled", () => App.AlertsEnabled, v => App.AlertsEnabled = v),
        B("notify_connection_changes", () => App.NotifyConnectionChanges, v => App.NotifyConnectionChanges = v),
        B("notify_connection_down_at_startup", () => App.NotifyConnectionDownAtStartup, v => App.NotifyConnectionDownAtStartup = v),
        I("connection_refire_minutes", 0, 1440, () => App.ConnectionRefireMinutes, v => App.ConnectionRefireMinutes = v),
        B("cpu.enabled", () => App.AlertCpuEnabled, v => App.AlertCpuEnabled = v),
        I("cpu.threshold_percent", 1, 100, () => App.AlertCpuThreshold, v => App.AlertCpuThreshold = v),
        new("cpu.mode", Kind.Text, 0, 0, () => McpAlertTools.CpuModeFor(App.AlertCpuMode),
            v => App.AlertCpuMode = string.Equals((string)v, McpAlertTools.CpuModeSql, StringComparison.OrdinalIgnoreCase) ? CpuAlertMode.SqlOnly : CpuAlertMode.Total,
            new[] { McpAlertTools.CpuModeSql, McpAlertTools.CpuModeTotal }),
        B("blocking.enabled", () => App.AlertBlockingEnabled, v => App.AlertBlockingEnabled = v),
        I("blocking.count_threshold", 1, int.MaxValue, () => App.AlertBlockingThreshold, v => App.AlertBlockingThreshold = v),
        I("blocking.wait_threshold_seconds", 0, int.MaxValue, () => App.AlertBlockingWaitSecondsThreshold, v => App.AlertBlockingWaitSecondsThreshold = v),
        B("deadlocks.enabled", () => App.AlertDeadlockEnabled, v => App.AlertDeadlockEnabled = v),
        I("deadlocks.count_threshold", 1, int.MaxValue, () => App.AlertDeadlockThreshold, v => App.AlertDeadlockThreshold = v),
        B("poison_wait.enabled", () => App.AlertPoisonWaitEnabled, v => App.AlertPoisonWaitEnabled = v),
        I(PoisonWaitThresholdPath, 1, int.MaxValue, () => App.AlertPoisonWaitThresholdMs, v => App.AlertPoisonWaitThresholdMs = v),
        B("runnable_queue.enabled", () => App.AlertRunnableQueueEnabled, v => App.AlertRunnableQueueEnabled = v),
        I("runnable_queue.warning_per_scheduler", 1, int.MaxValue, () => App.AlertRunnableQueueWarningPerScheduler, v => App.AlertRunnableQueueWarningPerScheduler = v),
        I("runnable_queue.critical_per_scheduler", 1, int.MaxValue, () => App.AlertRunnableQueueCriticalPerScheduler, v => App.AlertRunnableQueueCriticalPerScheduler = v),
        B("client_timeouts.enabled", () => App.AlertClientTimeoutsEnabled, v => App.AlertClientTimeoutsEnabled = v),
        I("client_timeouts.warning_per_minute", 1, int.MaxValue, () => App.AlertClientTimeoutsWarningPerMinute, v => App.AlertClientTimeoutsWarningPerMinute = v),
        I("client_timeouts.critical_per_minute", 1, int.MaxValue, () => App.AlertClientTimeoutsCriticalPerMinute, v => App.AlertClientTimeoutsCriticalPerMinute = v),
        I("client_timeouts.warning_multiple", 1, int.MaxValue, () => App.AlertClientTimeoutsWarningMultiple, v => App.AlertClientTimeoutsWarningMultiple = v),
        I("client_timeouts.critical_multiple", 1, int.MaxValue, () => App.AlertClientTimeoutsCriticalMultiple, v => App.AlertClientTimeoutsCriticalMultiple = v),
        B("long_running_query.enabled", () => App.AlertLongRunningQueryEnabled, v => App.AlertLongRunningQueryEnabled = v),
        I("long_running_query.threshold_minutes", 1, int.MaxValue, () => App.AlertLongRunningQueryThresholdMinutes, v => App.AlertLongRunningQueryThresholdMinutes = v),
        I("long_running_query.max_results", 1, int.MaxValue, () => App.AlertLongRunningQueryMaxResults, v => App.AlertLongRunningQueryMaxResults = v),
        B("long_running_query.exclude_sp_server_diagnostics", () => App.AlertLongRunningQueryExcludeSpServerDiagnostics, v => App.AlertLongRunningQueryExcludeSpServerDiagnostics = v),
        B("long_running_query.exclude_wait_for", () => App.AlertLongRunningQueryExcludeWaitFor, v => App.AlertLongRunningQueryExcludeWaitFor = v),
        B("long_running_query.exclude_backups", () => App.AlertLongRunningQueryExcludeBackups, v => App.AlertLongRunningQueryExcludeBackups = v),
        B("long_running_query.exclude_misc_waits", () => App.AlertLongRunningQueryExcludeMiscWaits, v => App.AlertLongRunningQueryExcludeMiscWaits = v),
        B("long_running_query.exclude_cdc", () => App.AlertLongRunningQueryExcludeCdc, v => App.AlertLongRunningQueryExcludeCdc = v),
        L("long_running_query.excluded_program_name_prefixes", () => App.AlertLongRunningQueryExcludedProgramNamePrefixes,
            v => App.AlertLongRunningQueryExcludedProgramNamePrefixes = LongRunningQueryExclusions.Normalize(v).ToList()),
        L("long_running_query.excluded_logins", () => App.AlertLongRunningQueryExcludedLogins,
            v => App.AlertLongRunningQueryExcludedLogins = LongRunningQueryExclusions.Normalize(v).ToList()),
        B("instance_restarted.enabled", () => App.AlertInstanceRestartedEnabled, v => App.AlertInstanceRestartedEnabled = v),
        B("memory_dumps.enabled", () => App.AlertMemoryDumpsEnabled, v => App.AlertMemoryDumpsEnabled = v),
        B("tempdb_space.enabled", () => App.AlertTempDbSpaceEnabled, v => App.AlertTempDbSpaceEnabled = v),
        I("tempdb_space.threshold_percent", 1, 100, () => App.AlertTempDbSpaceThresholdPercent, v => App.AlertTempDbSpaceThresholdPercent = v),
        B("tempdb_log_space.enabled", () => App.AlertTempDbLogSpaceEnabled, v => App.AlertTempDbLogSpaceEnabled = v),
        I("tempdb_log_space.threshold_percent", 1, 100, () => App.AlertTempDbLogSpaceThresholdPercent, v => App.AlertTempDbLogSpaceThresholdPercent = v),
        B("tempdb_growth_fit.enabled", () => App.AlertTempDbGrowthFitEnabled, v => App.AlertTempDbGrowthFitEnabled = v),
        B("low_disk.enabled", () => App.AlertLowDiskEnabled, v => App.AlertLowDiskEnabled = v),
        I("low_disk.threshold_percent", 0, 100, () => App.AlertLowDiskThresholdPercent, v => App.AlertLowDiskThresholdPercent = v),
        I("low_disk.threshold_gb", 0, int.MaxValue, () => App.AlertLowDiskThresholdGb, v => App.AlertLowDiskThresholdGb = v),
        I("low_disk.critical_free_percent", 0, 100, () => App.AlertDiskCriticalFreePercent, v => App.AlertDiskCriticalFreePercent = v),
        I("low_disk.critical_free_gb", 0, int.MaxValue, () => App.AlertDiskCriticalFreeGb, v => App.AlertDiskCriticalFreeGb = v),
        B("pvs.enabled", () => App.AlertPvsEnabled, v => App.AlertPvsEnabled = v),
        I("pvs.threshold_percent", 0, 100, () => App.AlertPvsThresholdPercent, v => App.AlertPvsThresholdPercent = v),
        I("pvs.floor_gb", 0, int.MaxValue, () => App.AlertPvsFloorGb, v => App.AlertPvsFloorGb = v),
        B("file_growth.enabled", () => App.AlertFileGrowthEnabled, v => App.AlertFileGrowthEnabled = v),
        I("file_growth.rise_mb", 0, int.MaxValue, () => App.AlertFileGrowthRiseMb, v => App.AlertFileGrowthRiseMb = v),
        I("file_growth.volume_percent", 0, 100, () => App.AlertFileGrowthVolumePercent, v => App.AlertFileGrowthVolumePercent = v),
        I("file_growth.lookback_minutes", 5, 1440, () => App.AlertFileGrowthLookbackMinutes, v => App.AlertFileGrowthLookbackMinutes = v),
        B("long_running_job.enabled", () => App.AlertLongRunningJobEnabled, v => App.AlertLongRunningJobEnabled = v),
        I("long_running_job.multiplier", 2, 20, () => App.AlertLongRunningJobMultiplier, v => App.AlertLongRunningJobMultiplier = v),
        B("failed_job.enabled", () => App.AlertFailedJobEnabled, v => App.AlertFailedJobEnabled = v),
        I("failed_job.lookback_minutes", 1, 1440, () => App.AlertFailedJobLookbackMinutes, v => App.AlertFailedJobLookbackMinutes = v),
        B("database_state.enabled", () => App.AlertDatabaseStateEnabled, v => App.AlertDatabaseStateEnabled = v),
        B("ag.enabled", () => App.NotifyAgHealth, v => App.NotifyAgHealth = v),
        I("ag.lag_threshold_seconds", 0, 86400, () => App.AgLagAlertSeconds, v => App.AgLagAlertSeconds = v),
        new("ag.redo_queue_threshold_kb", Kind.Long, 0, 1073741824, () => App.AgRedoQueueAlertKb, v => App.AgRedoQueueAlertKb = (long)v),
        I("ag.disconnect_refire_minutes", 0, 1440, () => App.AgDisconnectRefireMinutes, v => App.AgDisconnectRefireMinutes = v),
        I("cooldown_minutes", 1, 120, () => App.AlertCooldownMinutes, v => App.AlertCooldownMinutes = v),
        L("excluded_databases", () => App.AlertExcludedDatabases,
            v => App.AlertExcludedDatabases = SettingsListText.SplitLines(string.Join('\n', v))),
        L("excluded_volumes", () => App.AlertExcludedVolumes,
            v => App.AlertExcludedVolumes = VolumeExclusions.Normalize(SettingsListText.SplitLinesAndCommas(string.Join('\n', v))).ToList()),
        new("delivery.mode", Kind.Text, 0, 0, () => App.AlertDeliveryMode.ToString(),
            v => App.AlertDeliveryMode = string.Equals((string)v, nameof(AlertNotificationMode.PerEvent), StringComparison.OrdinalIgnoreCase) ? AlertNotificationMode.PerEvent : AlertNotificationMode.Summary,
            new[] { nameof(AlertNotificationMode.Summary), nameof(AlertNotificationMode.PerEvent) }),
        I("delivery.per_event_max", 1, 100, () => App.AlertPerEventMaxPerCycle, v => App.AlertPerEventMaxPerCycle = v),
        I("delivery.cooldown_minutes", 1, 120, () => App.EmailCooldownMinutes, v => App.EmailCooldownMinutes = v),
        B("analysis.enabled", () => App.AnalysisEnabled, v => App.AnalysisEnabled = v),
        I("analysis.interval_minutes", 5, 360, () => App.AnalysisIntervalMinutes, v => App.AnalysisIntervalMinutes = v),
        B("analysis.notifications_enabled", () => App.AnalysisNotificationsEnabled, v => App.AnalysisNotificationsEnabled = v),
        new("analysis.notify_severity", Kind.Double, 0.0, 2.0, () => App.AnalysisNotifySeverity, v => App.AnalysisNotifySeverity = (double)v),
        I("analysis.notify_cooldown_minutes", 30, 10080, () => App.AnalysisNotifyCooldownMinutes, v => App.AnalysisNotifyCooldownMinutes = v),
        new("analysis.uncorroborated_route", Kind.Text, 0, 0, () => FindingRouting.RouteText(App.AnalysisUncorroboratedRoute),
            v => App.AnalysisUncorroboratedRoute = string.Equals((string)v, "page", StringComparison.OrdinalIgnoreCase) ? FindingRoute.Page : FindingRoute.Digest,
            new[] { "digest", "page" }),
    };

    private const string PoisonWaitThresholdPath = "poison_wait.threshold_ms";

    /// <summary>Keys get_alert_settings reports beside a value, as notes. Accepted and ignored on the way in, so a whole
    /// payload read from get_alert_settings can be handed back after editing.</summary>
    private static readonly HashSet<string> s_readOnlyCompanions = new(StringComparer.Ordinal)
    {
        "poison_wait.threshold_ms_note",
        "analysis.uncorroborated_route_note",
    };

    private static readonly Dictionary<string, Field> s_byPath = s_fields.ToDictionary(f => f.Path, StringComparer.Ordinal);

    /// <summary>One update at a time: the statics are process-wide and the file is a whole document.</summary>
    private static readonly object s_writeLock = new();

    [McpServerTool(Name = "update_alert_settings"), Description(
        "Write: PARTIAL update of this instance's alert settings, the same ones Settings > Alerts edits. Call get_alert_settings FIRST; send back only the changed fields, in the same nested shape; omitted fields stay unchanged. An invalid value or unknown field writes NOTHING and returns status invalid naming the field. Takes effect at once in the running app and is saved to settings.json. Served only when mcp_allow_writes is true.<<GUIDE>>" +
        "Write: PARTIAL update of the alert settings this instance runs on, the ones Settings > Alerts edits. Call " +
        "get_alert_settings FIRST, change only the fields you want, and pass THOSE fields back here as JSON in the SAME nested shape " +
        "get_alert_settings returns (e.g. {\"cpu\":{\"threshold_percent\":90},\"cooldown_minutes\":10}); every setting you do not include is " +
        "left unchanged. Each provided field is validated against the rules the Settings window enforces (thresholds in range, cpu.mode " +
        "'sql'|'total', delivery.mode 'Summary'|'PerEvent', analysis.uncorroborated_route 'digest'|'page', counts within their bounds, " +
        "runnable_queue and client_timeouts keeping Critical at or above Warning once both are applied); an out-of-range value, a wrong type " +
        "or an unknown field returns {status:\"invalid\", message} naming the field and writes NOTHING. The change is applied to the running " +
        "app and written to settings.json, so it is in force on the next collection and survives a restart. TWO cooldowns are writable and " +
        "they are different stages: cooldown_minutes gates whether an alert FIRES, delivery.cooldown_minutes bounds the resulting " +
        "Slack/Teams/PagerDuty/webhook/email post. excluded_databases, excluded_volumes and the two long_running_query exclusion arrays are " +
        "replaced WHOLE by what you send (an empty array clears it). poison_wait.threshold_ms is RETIRED: accepted and stored, consulted by " +
        "nothing, and the answer carries a warnings entry; poison_wait.enabled governs the alert. The read-only notes get_alert_settings " +
        "reports (poison_wait.threshold_ms_note, analysis.uncorroborated_route_note) are accepted and ignored. NOT settable here: the smtp " +
        "group (delivery credentials are set in the Settings window), and Darling's health_bands, fleet_sweep, self_alerts, daily_report, " +
        "pg_* counts and delivery.send_resolutions groups, which Lite has no setting for. " +
        "Returns {status:\"updated\", updated_fields:[...], warnings:[...], settings:{...} (the full settings as get_alert_settings now " +
        "reports them), previous:{...} (the sent fields as they were, same nested shape), undo:{tool, arguments}}, or {status:\"invalid\"}, " +
        "or {status:\"failed\"} when settings.json could not be written (the running app is put back as it was). " +
        "Served only when mcp_allow_writes is true.")]
    public static Task<string> UpdateAlertSettings(
        [Description("A JSON object with ONLY the alert-settings fields to change, in the nested shape get_alert_settings returns (e.g. {\"cpu\":{\"threshold_percent\":90},\"blocking\":{\"enabled\":false},\"cooldown_minutes\":10}).")] string settings_json) =>
        Task.FromResult(UpdateAlertSettingsCore(settings_json, dispatcher: Application.Current?.Dispatcher, settingsWindowOpen: IsSettingsWindowOpen));

    /// <summary>True while a Settings window is open (it is shown modally from the main window). Reads the window list, so it
    /// runs on the UI thread.</summary>
    private static bool IsSettingsWindowOpen() => Application.Current?.Windows.OfType<SettingsWindow>().Any() == true;

    /// <summary>The tool body. <paramref name="readRoot"/> and <paramref name="write"/> default to the settings.json the app reads
    /// and the Settings window writes; a test passes its own so no real file is touched.
    /// <para>#9639: every other settings.json writer (<c>SettingsWindow</c>'s Save, <c>App.WriteSetting</c> for the sidebar, the time
    /// range, the overview sort, the alert window size) reads the whole document, changes it and writes it back on the UI thread,
    /// headless or not (a headless Lite is still a WPF application with its dispatcher). This tool is called off the UI thread, so
    /// when <paramref name="dispatcher"/> is given the whole read-modify-write runs on it, the same thread as those writers, and
    /// none of them can interleave with it. <paramref name="settingsWindowOpen"/> (asked on that thread) refuses the call while the
    /// Settings window is open: its Save re-applies every alert control and would undo the update. Without a dispatcher (a test)
    /// the body runs inline.</para></summary>
    internal static string UpdateAlertSettingsCore(string settings_json, Func<JsonObject>? readRoot = null, Func<JsonNode, bool>? write = null,
        Func<bool>? settingsWindowOpen = null, Dispatcher? dispatcher = null)
    {
        try
        {
            JsonNode? root = null;
            string? notJson = null;
            try
            {
                root = JsonNode.Parse(settings_json);
            }
            catch (JsonException ex)
            {
                notJson = ex.Message;
            }

            if (notJson is not null)
            {
                return McpHelpers.Refusal("settings_json", $"settings_json is not valid JSON ({notJson}).");
            }

            if (root is not JsonObject body)
            {
                return McpHelpers.Refusal("settings_json", "settings_json must be a JSON object of the fields to change (see get_alert_settings for the shape).");
            }

            var parsed = new List<(Field Field, object Value)>();
            var warnings = new List<string>();
            var error = Parse(body, parsed, warnings);
            if (error is null && parsed.Count == 0)
            {
                error = new Refused("settings_json", "No known alert-settings fields were provided. Send fields in the shape get_alert_settings returns (e.g. {\"cpu\":{\"threshold_percent\":90}}).");
            }

            if (error is not null)
            {
                return McpHelpers.Refusal(error.Value.Parameter, error.Value.Message);
            }

            string Run()
            {
                lock (s_writeLock)
                {
                    if ((settingsWindowOpen ?? (() => false))())
                    {
                        return McpHelpers.Refusal("settings_json", "The Settings window is open, so the alert settings were not changed. Close it or change them there.");
                    }

                    var pairError = CheckPairs(parsed);
                    if (pairError is not null)
                    {
                        return McpHelpers.Refusal(pairError.Value.Parameter, pairError.Value.Message);
                    }

                    var previous = new JsonObject();
                    var previousValues = new List<(Field Field, object Value)>();
                    foreach (var (field, _) in parsed)
                    {
                        var before = field.Get();
                        previousValues.Add((field, before));
                        SetPath(previous, field.Path, ToNode(field, before));
                    }

                    Apply(parsed);
                    string? saveFailure = null;
                    try
                    {
                        var node = (readRoot ?? App.SettingsRootForWrite)();
                        SettingsWindow.WriteAlertKeys(node, App.AlertCooldownMinutes);
                        if (!(write ?? (n => App.WriteSettingsDocument(n, "alert settings")))(node))
                        {
                            throw new IOException("settings.json could not be written.");
                        }
                    }
                    catch (Exception ex)
                    {
                        saveFailure = ex.Message;
                    }

                    if (saveFailure is not null)
                    {
                        Apply(previousValues);
                        return McpHelpers.Status("failed", $"Nothing was saved: {saveFailure} The running settings were put back as they were.");
                    }

                    var settings = JsonNode.Parse(McpAlertTools.GetAlertSettings().GetAwaiter().GetResult());
                    return JsonSerializer.Serialize(new
                    {
                        status = "updated",
                        updated_fields = parsed.Select(p => p.Field.Path).ToArray(),
                        warnings = warnings.ToArray(),
                        settings,
                        previous,
                        undo = new { tool = "update_alert_settings", arguments = new { settings_json = previous.ToJsonString() } },
                    }, McpHelpers.JsonOptions);
                }
            }

            return dispatcher is not null && !dispatcher.CheckAccess() ? dispatcher.Invoke(Run) : Run();
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_alert_settings", ex);
        }
    }

    /// <summary>Reads the body into typed values, one per known field. Returns the first refusal (naming the field), or null.</summary>
    private static Refused? Parse(JsonObject body, List<(Field Field, object Value)> parsed, List<string> warnings)
    {
        foreach (var (key, node) in body)
        {
            if (string.Equals(key, "smtp", StringComparison.Ordinal))
            {
                return new Refused("smtp", "smtp is not settable through update_alert_settings: delivery credentials are set in the Settings window.");
            }

            if (node is JsonObject group && !s_byPath.ContainsKey(key))
            {
                foreach (var (inner, innerNode) in group)
                {
                    var refusal = ParseOne($"{key}.{inner}", innerNode, parsed, warnings);
                    if (refusal is not null)
                    {
                        return refusal;
                    }
                }

                continue;
            }

            var topRefusal = ParseOne(key, node, parsed, warnings);
            if (topRefusal is not null)
            {
                return topRefusal;
            }
        }

        return null;
    }

    private static Refused? ParseOne(string path, JsonNode? node, List<(Field Field, object Value)> parsed, List<string> warnings)
    {
        if (s_readOnlyCompanions.Contains(path))
        {
            return null;
        }

        if (!s_byPath.TryGetValue(path, out var field))
        {
            return new Refused(path, $"Unknown field '{path}'. Send fields in the nested shape get_alert_settings returns.");
        }

        if (parsed.Any(p => p.Field.Path == path))
        {
            return new Refused(path, $"{path} was sent twice (nested and as a dotted key). Send it once.");
        }

        var (value, error) = Coerce(field, node);
        if (error is not null)
        {
            return new Refused(path, error);
        }

        parsed.Add((field, value!));
        if (field.Path == PoisonWaitThresholdPath)
        {
            warnings.Add("poison_wait.threshold_ms is retired: it was stored, but nothing consults it. poison_wait.enabled governs the Poison Wait alert.");
        }

        return null;
    }

    private static (object? Value, string? Error) Coerce(Field field, JsonNode? node)
    {
        var path = field.Path;
        if (node is null)
        {
            return (null, $"{path} cannot be null: send a value.");
        }

        switch (field.Kind)
        {
            case Kind.Bool:
                return node is JsonValue b && b.TryGetValue<bool>(out var flag)
                    ? (flag, null)
                    : (null, $"{path} must be true or false.");

            case Kind.Int:
            case Kind.Long:
                if (node is not JsonValue n || n.GetValueKind() != JsonValueKind.Number || !n.TryGetValue<long>(out var whole))
                {
                    return (null, $"{path} must be a whole number between {field.Min.ToString(CultureInfo.InvariantCulture)} and {field.Max.ToString(CultureInfo.InvariantCulture)}.");
                }

                if (whole < field.Min || whole > field.Max)
                {
                    return (null, $"{path} must be between {field.Min.ToString(CultureInfo.InvariantCulture)} and {field.Max.ToString(CultureInfo.InvariantCulture)} (got {whole}).");
                }

                return field.Kind == Kind.Int ? ((int)whole, null) : (whole, null);

            case Kind.Double:
                if (node is not JsonValue d || d.GetValueKind() != JsonValueKind.Number || !d.TryGetValue<double>(out var real) || double.IsNaN(real))
                {
                    return (null, $"{path} must be a number between {field.Min.ToString(CultureInfo.InvariantCulture)} and {field.Max.ToString(CultureInfo.InvariantCulture)}.");
                }

                return real < field.Min || real > field.Max
                    ? (null, $"{path} must be between {field.Min.ToString(CultureInfo.InvariantCulture)} and {field.Max.ToString(CultureInfo.InvariantCulture)} (got {real.ToString(CultureInfo.InvariantCulture)}).")
                    : (real, null);

            case Kind.Text:
                if (node is not JsonValue t || t.GetValueKind() != JsonValueKind.String || !t.TryGetValue<string>(out var text))
                {
                    return (null, $"{path} must be one of: {string.Join(", ", field.Allowed!)}.");
                }

                var match = field.Allowed!.FirstOrDefault(a => string.Equals(a, text.Trim(), StringComparison.OrdinalIgnoreCase));
                return match is null
                    ? (null, $"{path} must be one of: {string.Join(", ", field.Allowed!)} (got '{text}').")
                    : (match, null);

            default:
                if (node is not JsonArray array)
                {
                    return (null, $"{path} must be an array of strings (an empty array clears it).");
                }

                var items = new List<string>();
                foreach (var item in array)
                {
                    if (item is not JsonValue v || v.GetValueKind() != JsonValueKind.String || !v.TryGetValue<string>(out var entry))
                    {
                        return (null, $"{path} must be an array of strings (an empty array clears it).");
                    }

                    items.Add(entry);
                }

                return (items, null);
        }
    }

    /// <summary>The both-or-neither rules the window applies to its three paired boxes, over the values as they would stand
    /// after this call (a lone bar is compared with the stored other one).</summary>
    private static Refused? CheckPairs(List<(Field Field, object Value)> parsed)
    {
        int Merged(string path, Func<int> stored) =>
            parsed.Any(p => p.Field.Path == path) ? (int)parsed.First(p => p.Field.Path == path).Value : stored();

        bool Sent(string a, string b) => parsed.Any(p => p.Field.Path == a || p.Field.Path == b);

        var errors = new List<string>();
        var parameter = "runnable_queue.critical_per_scheduler";
        if (Sent("runnable_queue.warning_per_scheduler", "runnable_queue.critical_per_scheduler"))
        {
            RunnableQueueBarsInput.Validate(
                Merged("runnable_queue.warning_per_scheduler", () => App.AlertRunnableQueueWarningPerScheduler).ToString(CultureInfo.CurrentCulture),
                Merged("runnable_queue.critical_per_scheduler", () => App.AlertRunnableQueueCriticalPerScheduler).ToString(CultureInfo.CurrentCulture),
                errors);
        }

        if (errors.Count == 0 && Sent("client_timeouts.warning_per_minute", "client_timeouts.critical_per_minute"))
        {
            parameter = "client_timeouts.critical_per_minute";
            ClientTimeoutsBarsInput.Validate(
                Merged("client_timeouts.warning_per_minute", () => App.AlertClientTimeoutsWarningPerMinute).ToString(CultureInfo.CurrentCulture),
                Merged("client_timeouts.critical_per_minute", () => App.AlertClientTimeoutsCriticalPerMinute).ToString(CultureInfo.CurrentCulture),
                errors);
        }

        if (errors.Count == 0 && Sent("client_timeouts.warning_multiple", "client_timeouts.critical_multiple"))
        {
            parameter = "client_timeouts.critical_multiple";
            ClientTimeoutsBarsInput.ValidateMultiples(
                Merged("client_timeouts.warning_multiple", () => App.AlertClientTimeoutsWarningMultiple).ToString(CultureInfo.CurrentCulture),
                Merged("client_timeouts.critical_multiple", () => App.AlertClientTimeoutsCriticalMultiple).ToString(CultureInfo.CurrentCulture),
                errors);
        }

        return errors.Count == 0 ? null : new Refused(parameter, string.Join(" ", errors) + " Nothing was written.");
    }

    private static void Apply(List<(Field Field, object Value)> values)
    {
        foreach (var (field, value) in values)
        {
            field.Set(value);
        }
    }

    private static JsonNode? ToNode(Field field, object value) => field.Kind switch
    {
        Kind.Bool => JsonValue.Create((bool)value),
        Kind.Int => JsonValue.Create((int)value),
        Kind.Long => JsonValue.Create((long)value),
        Kind.Double => JsonValue.Create((double)value),
        Kind.Text => JsonValue.Create((string)value),
        _ => new JsonArray(((List<string>)value).Select(s => (JsonNode?)JsonValue.Create(s)).ToArray()),
    };

    /// <summary>Puts <paramref name="value"/> at a dotted path ("cpu.mode" becomes {"cpu":{"mode":...}}).</summary>
    private static void SetPath(JsonObject target, string path, JsonNode? value)
    {
        var dot = path.IndexOf('.');
        if (dot < 0)
        {
            target[path] = value;
            return;
        }

        var head = path[..dot];
        if (target[head] is not JsonObject child)
        {
            child = new JsonObject();
            target[head] = child;
        }

        SetPath(child, path[(dot + 1)..], value);
    }
}
