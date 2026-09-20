using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpAlertTools
{
    /// <summary>SQL-process CPU only. Darling's <c>cpu_mode</c> spelling, which its own
    /// <c>update_alert_settings</c> validates against — see <c>ViewerDataService.CpuModeSql</c>.</summary>
    internal const string CpuModeSql = "sql";

    /// <summary>All non-idle CPU. Darling's <c>ViewerDataService.CpuModeTotal</c>.</summary>
    internal const string CpuModeTotal = "total";

    /// <summary>The wire-side deprecation note for <c>poison_wait.threshold_ms</c> (#3653) — Darling's
    /// <c>DarlingMcpAlertTools.PoisonWaitThresholdMsNote</c>, byte for byte; <c>McpAlertSettingsKeyTests</c>
    /// reads that declaration to hold the two equal, the way it holds the cpu.mode vocabulary.</summary>
    internal const string PoisonWaitThresholdMsNote =
        "retired by #3593 — the alert grades accumulated wait over a ten-minute window; this value is stored and reported but not consulted";

    /// <summary>The <c>analysis.uncorroborated_route_note</c> text (#3712). Darling's twin states the precedence
    /// between the knob's TWO homes there (the settings-row column since V137, over darling.json) and publishes
    /// which one decided under <c>uncorroborated_route_source</c>; Lite has ONE home, its settings file, so this
    /// note says where it is edited and there is no source key to publish (<c>McpAlertSettingsKeyTests</c> holds
    /// that omission as a decision).</summary>
    internal const string UncorroboratedRouteNote =
        "#3712: 'digest' (default) keeps a lone uncorroborated finding off email, the webhooks and the tray — it is still "
        + "recorded (Alerts tab status Digest) and shown in Recommendations with a not-paged marker; 'page' restores delivery of "
        + "every finding at or above notify_severity. A corroborated finding is delivered under either value. Edited in "
        + "Settings → Alerts ('Only notify on corroborated findings') or as analysis_uncorroborated_route in the settings file.";

    /// <summary>
    /// Lite's <see cref="CpuAlertMode"/> in Darling's wire vocabulary (#1911). Deliberately NOT
    /// <c>App.AlertCpuMode.ToString()</c>: that emits the C# enum names <c>Total</c>/<c>SqlOnly</c>, which no
    /// Darling client accepts, and it would silently start emitting a third spelling the day someone renames
    /// the enum. The mapping is the same shape as Darling's own <c>MapCpuModeToStore</c>, which is the
    /// authority this mirrors.
    /// </summary>
    internal static string CpuModeFor(CpuAlertMode mode) =>
        mode == CpuAlertMode.SqlOnly ? CpuModeSql : CpuModeTotal;

    [McpServerTool(Name = "get_alert_history"), Description("Gets recent alert history from the alert log, NEWEST FIRST. Shows what alerts fired, when, and whether email was sent successfully. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: alerts_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_alert_time / newest_returned_alert_time bound the page — under newest-first ordering the oldest stamp IS how far back this read reached. Raise limit or narrow hours_back when truncated is true; widening hours_back cannot help. BY DEFAULT THIS READ EXCLUDES DISMISSED ALERTS — rows an operator acknowledged in the Alerts History tab. Dismissal says nothing about whether the alert fired or mattered, so an incident reconstruction that ignores it can miss the very critical someone already looked at: dismissed_excluded says whether the filter applied and dismissed_excluded_count is how many rows in the window it removed, and include_dismissed = true returns them, each labelled dismissed = true. On this edition an alert that was dismissed AFTER aging into the parquet archive is removed by the archive view itself and can be neither returned nor counted here. notification_type is the delivery disposition and is the ONLY field that says why a row did not deliver: 'email'/'webhook'/'email+webhook' delivered on that channel; 'tray' is this instance's own balloon notification, which every non-muted alert gets, so it is what most rows read here and it does NOT report the email or webhook outcome; 'failed' means a channel was attempted and came back unsuccessful, with send_error carrying the first failing channel's text; 'muted' means a mute rule suppressed it; 'digest' means the analysis finding was UNCORROBORATED (one fact in its chain, no matched co-fire check) and the corroboration gate kept it off email, the webhooks and the tray (#3712) — it is persisted, shown in Recommendations with a not-paged marker, and routing_reason on the row says which components decided; 'none' is a resolution row, which no channel applies to. Do NOT split the not-delivered rows on send_error: it is null whenever a cooldown or the per-metric repeat budget suppressed a send, on 'digest' rows, and on every row written before those dispositions existed. 'throttled' and 'folded' are recorded by the headless service; on this instance the tray channel answers first, so a cooldown-suppressed or folded send is stored as 'tray'. 'undelivered' is a retained legacy value that means throttled OR folded OR failed with nothing in the row to say which. severity is the row's tier — 'critical', 'warning', 'info' or 'resolution' — and severity_source says where it came from: 'fired' when the row persisted the tier the alert actually fired at (graded alerts such as Poison Wait, Volume Free Space and Database State fire Warning OR Critical by measurement), 'metric_name' when the row carries no tier and the metric's name is the only evidence (rows written before the tier was persisted, alerts whose severity is fixed per metric, and every resolution row). Do not infer a graded alert's tier from its name: a 'Poison Wait' row with severity 'warning' fired as a warning. routing (#3712) is the corroboration gate's decision for an 'Analysis: …' finding, one step upstream of delivery: 'page' when it earned a channel (two or more facts in its chain, a matched co-fire check, or one of the two by-construction stories), 'digest' when it was a lone uncorroborated fact; routing_reason names the components. Both are null on engine alerts and on analysis rows written before the gate existed.")]
    public static async Task<string> GetAlertHistory(
        LocalDataService dataService,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 50. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        /* Appended after as_of: MCP invokes by name, and a trailing optional is the one position no existing
           positional C# caller can be re-bound by. Same convention as Darling's twin. */
        [Description("Include alerts an operator has dismissed in the Alerts History tab. Default false, which is the tab's own read. Dismissal is an acknowledgement, not a verdict — a dismissed critical still fired — so set this when reconstructing an incident rather than triaging what is still open. Each row then carries dismissed so the two populations stay distinguishable.")] bool include_dismissed = false)
    {
        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            /* #3541 A3: over-fetch by one so truncation is OBSERVED rather than inferred from count == limit.
               The cap was already the caller's here; what was missing was any way to tell a window of
               exactly `limit` alerts from a busier one, and any statement that the dismissed rows had been
               removed. Same shape as Darling's twin. */
            var rows = await dataService.GetAlertHistoryAsync(hours_back, limit + 1, asOfUtc: windowEnd, includeDismissed: include_dismissed);
            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            /* The hidden filter, measured: how many rows in this window it removed. Zero is a real answer
               (nothing was hidden). Not probed when the filter is off, because then it removed nothing by
               construction. */
            var dismissedExcludedCount = include_dismissed
                ? 0L
                : await dataService.CountDismissedAlertsAsync(hours_back, serverId: null, asOfUtc: windowEnd);

            if (page.Count == 0)
            {
                /* An empty default page over a window that DOES hold dismissed rows is not "no alerts": it is
                   "every alert here was acknowledged", and the one-sentence quiet-window answer would send
                   the caller off widening a window whose contents they were never shown. */
                return dismissedExcludedCount > 0
                    ? McpHelpers.Status(
                        "empty",
                        $"No undismissed alerts found in the specified time range, but {dismissedExcludedCount} dismissed alert(s) were excluded by the default filter. Re-run with include_dismissed = true to see them — a dismissed alert still fired.")
                    : McpHelpers.Status("empty", "No alerts found in the specified time range.");
            }

            var alerts = page.Select(r =>
            {
                var (severity, severitySource) = AlertHistoryRowSeverity.Describe(r.MetricName, r.ContextJson);
                var routing = AlertContextSerializer.TryReadRouting(r.ContextJson);
                return new
                {
                    alert_time = r.AlertTime.ToString("o"),
                    server_id = r.ServerId,
                    server_name = r.ServerName,
                    metric_name = r.MetricName,
                    current_value = r.CurrentValue,
                    threshold_value = r.ThresholdValue,
                    alert_sent = r.AlertSent,
                    notification_type = r.NotificationType,
                    send_error = r.SendError,
                    muted = r.Muted,
                    /* Per row, so a page that mixes the two populations labels each one. Always false on the
                       default read, which is a true statement about every row on it. */
                    dismissed = r.Dismissed,
                    /* #3539 A8e: the tier the alert FIRED at where the row persisted one ("fired"), else what
                       the metric NAME implies ("metric_name") — the Darling tool's twin fields, from the same
                       shared decision the Alerts History tab colours its rows by. */
                    severity,
                    severity_source = severitySource,
                    /* #3598: the Darling twin's routing provenance, read through the shared serializer for
                       parity of shape. This edition has no routes table and its deliverer never writes the
                       member, so every row here answers null — every alert goes to every configured channel. */
                    route = AlertContextSerializer.TryReadRoute(r.ContextJson) is { } route
                        ? new
                        {
                            family = route.Family,
                            route_id = route.RouteId,
                            destinations = route.Destinations.Select(d => new { channel = d.Channel, route_id = d.RouteId, source = d.Source }),
                        }
                        : null,
                    /* #3712: the corroboration gate's decision for an analysis finding — 'page' or 'digest' —
                       and the sentence naming the components it read, read off the row the way severity_source
                       is. Null on every engine alert and on analysis rows written before the gate existed. */
                    routing = routing?.Route,
                    routing_reason = routing?.Reason,
                    detail_text = r.DetailText,
                };
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                hours_back,
                /* #3541 A3: `total_alerts` is gone — it was the page count under a name that promised the
                   window. What is published is what was measured: the page, whether the window held more,
                   the span the page covers (newest-first, so the oldest stamp IS the reach), and the filter
                   that shaped the population together with how much it removed. */
                alerts_returned = page.Count,
                truncated,
                oldest_returned_alert_time = page.Min(r => r.AlertTime).ToString("o"),
                newest_returned_alert_time = page.Max(r => r.AlertTime).ToString("o"),
                order = "alert_time_desc",
                dismissed_excluded = !include_dismissed,
                dismissed_excluded_count = dismissedExcludedCount,
                alerts
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_alert_history", ex);
        }
    }

    [McpServerTool(Name = "get_alert_settings"), Description("Gets the current alert configuration this instance is running on: which alerts are enabled and their thresholds (CPU, blocking, deadlocks, poison waits, long-running queries and jobs, tempdb space, low disk, PVS, file growth, failed jobs, database state, Availability Group health, connection loss), the cooldown, the excluded databases, the deadlock/blocking delivery mode and cooldown, the scheduled-analysis cadence, and the SMTP email configuration. The two cooldowns govern different stages: top-level cooldown_minutes gates whether an alert FIRES, delivery.cooldown_minutes bounds the resulting email/Teams/Slack/PagerDuty/webhook send both per alert FINGERPRINT and, for a re-notification, per METRIC across every monitored server (the servers it holds back are named on the send that does go out, under an 'Other Servers Affected' section; a first notice is never held back, and delivery.mode PerEvent opts out of the per-metric bound). The file_growth group's rise_mb is megabytes per HOUR, averaged over file_growth.lookback_minutes — a rate, not a total for the window: 10240 means 10 GB/hr whether the lookback is 5 minutes or 24 hours, and the engine scales it to the window. poison_wait.threshold_ms is RETIRED (#3593): the Poison Wait alert grades ACCUMULATED wait over a ten-minute window, so this value is stored and reported for compatibility but consulted by nothing — poison_wait.threshold_ms_note says so beside it, and poison_wait.enabled is the live switch. analysis.uncorroborated_route (#3712) is where a notify-worthy but UNCORROBORATED finding goes — one fact in its chain and no matched co-fire check: 'digest' (the default) keeps it off email, the webhooks and the tray while it is still recorded (Alerts tab status Digest) and shown in Recommendations with a not-paged marker; 'page' restores delivery of every finding at or above notify_severity; a corroborated finding is delivered under either. analysis.uncorroborated_route_note says where it is edited. long_running_query.excluded_program_name_prefixes and long_running_query.excluded_logins are the Long-Running Query alert's OPT-OUT knob (#3653): a session whose program_name STARTS WITH an entry of the first list, or whose login_name IS an entry of the second (both case-insensitive, no wildcard grammar), is NOT EVALUATED by that alert at all — not read into the decision, not counted, not fingerprinted — the opposite of a mute rule, which silences a fire already decided. The exclusion is applied in the read ahead of long_running_query.max_results, so an excluded session never consumes a result slot, and the fired alert's card carries Excluded Count / Excluded By Program Prefix / Excluded By Login items saying how many SESSIONS each list removed on that evaluation (a session matching both counts once, under the prefix). The lists ship SEEDED from a 7-day read of one large production store, whose long-running population fell into four classes: (1) SQL Agent job steps — program_name prefix 'SQLAgent - TSQL JobStep', ~460 sessions a week across 11 jobs, medians 35–62 min — the default prefix; (2) the NT AUTHORITY\\SYSTEM and NT AUTHORITY\\NETWORK SERVICE logins — the permanent multi-day CDC-shaped background — the default logins; (3) the application's admin login — deliberately NOT a default, because it runs the job wave but also real ad-hoc long-runners, and the job-step prefix already covers its share; (4) named humans — never excluded, they are what the page is for. The seeds are DEFAULTS: an empty list here means the operator cleared it in the Settings window and that arm excludes nothing. The same nested shape Darling's get_alert_settings returns, minus its self_alerts group (the headless service's own store-volume and collection-health thresholds, which a single-instance Lite install has no equivalent for) and plus smtp, which Lite delivers itself. Read-only: Lite has no update_alert_settings, so these change in the Settings window.")]    public static Task<string> GetAlertSettings()    {
        try
        {
            var settings = new
            {
                /* alerts_enabled, not notifications_enabled: Darling reports the master switch under this
                   name, and it spells notifications_enabled something else entirely (the analysis
                   sub-object's own toggle), so the old Lite name was not merely different — it collided. */
                alerts_enabled = App.AlertsEnabled,
                notify_connection_changes = App.NotifyConnectionChanges,
                /* #2417: the connection family's two sub-settings. Darling keeps these TOP-LEVEL beside
                   their master switch rather than in a `connection` group, because the master shipped as a
                   top-level key and a group could only ever have held two of the three; the spellings are
                   its column names, which are also the keys Lite's own settings.json already uses. Both
                   statics have been wired through to the connection-alert path since #1659. */
                notify_connection_down_at_startup = App.NotifyConnectionDownAtStartup,
                connection_refire_minutes = App.ConnectionRefireMinutes,
                cpu = new
                {
                    enabled = App.AlertCpuEnabled,
                    threshold_percent = App.AlertCpuThreshold,
                    /* #1911: Lite did not report the mode at all, and adding it as the raw enum name would
                       have traded #1895's key-level mismatch for a VALUE-level one — Darling has emitted
                       "sql"/"total" here since its store schema was written, and its update_alert_settings
                       validates against exactly those two. Its vocabulary is the older public surface, so
                       Lite maps onto it rather than the other way round; an agent can now read cpu.mode from
                       either app and compare the answers. */
                    mode = McpAlertTools.CpuModeFor(App.AlertCpuMode)
                },
                blocking = new
                {
                    enabled = App.AlertBlockingEnabled,
                    /* Renamed from threshold_seconds (#1839): this gate has always been a COUNT of
                       blocked-process events — the seconds name was copied from the Dashboard, whose
                       blocking threshold really is seconds. Leaving it would now collide with the real
                       seconds threshold below. The spelling is Darling's count_threshold, not a new
                       threshold_count: Darling's get_alert_settings/update_alert_settings pair already
                       used it for this exact field, and one MCP schema across both apps is the point. */
                    count_threshold = App.AlertBlockingThreshold,
                    wait_threshold_seconds = App.AlertBlockingWaitSecondsThreshold
                },
                deadlocks = new
                {
                    enabled = App.AlertDeadlockEnabled,
                    /* Same alignment: Darling reports this as count_threshold too. */
                    count_threshold = App.AlertDeadlockThreshold
                },
                /* #2394: everything from here down to analysis was already wired end to end on Lite —
                   AppAlertEngineSettings has projected each of these statics onto the SHARED engine's
                   IAlertEngineSettings since the Phase-5 forwarding, so the alerts evaluate here exactly as
                   they do on Darling. Only the MCP surface was narrow, which meant an agent triaging a Lite
                   instance could not read whether tempdb-space, low-disk, PVS, file-growth,
                   long-running-query/job, failed-job, database-state or analysis alerting was even switched
                   on. Group and key spellings are Darling's verbatim, because one MCP schema across both SKUs
                   is the whole point of the #1839/#1911 alignments above — and note McpHelpers.JsonOptions
                   sets no naming policy, so the C# identifier IS the wire key. */
                poison_wait = new
                {
                    enabled = App.AlertPoisonWaitEnabled,
                    /* #3653 (from #3541): the key stays — a published field a client may already read — but
                       since #3593 the shared AlertEngine grades ACCUMULATED wait over a ten-minute window and
                       reads no per-wait bar (IAlertEngineSettings.PoisonWaitThresholdMs records why the member
                       survives). The note sits beside the value so an agent reading the payload learns it
                       there rather than only in a description read once. Same text as Darling's
                       DarlingMcpAlertTools.PoisonWaitThresholdMsNote, held equal by McpAlertSettingsKeyTests
                       reading that declaration — Lite cannot reference the service assembly. */
                    threshold_ms = App.AlertPoisonWaitThresholdMs,
                    threshold_ms_note = PoisonWaitThresholdMsNote
                },
                long_running_query = new
                {
                    enabled = App.AlertLongRunningQueryEnabled,
                    threshold_minutes = App.AlertLongRunningQueryThresholdMinutes,
                    max_results = App.AlertLongRunningQueryMaxResults,
                    exclude_sp_server_diagnostics = App.AlertLongRunningQueryExcludeSpServerDiagnostics,
                    exclude_wait_for = App.AlertLongRunningQueryExcludeWaitFor,
                    exclude_backups = App.AlertLongRunningQueryExcludeBackups,
                    exclude_misc_waits = App.AlertLongRunningQueryExcludeMiscWaits,
                    exclude_cdc = App.AlertLongRunningQueryExcludeCdc,
                    /* #3653 (A5, Q5): the opt-out knob, Darling's spelling (McpAlertSettingsKeyTests derives the
                       shape from Darling's source) — sessions whose program_name starts with a prefix / whose
                       login_name equals an entry are NOT EVALUATED by the alert; see the tool description. Lite's
                       two settings.json arrays, seeded with the production read's defaults. */
                    excluded_program_name_prefixes = App.AlertLongRunningQueryExcludedProgramNamePrefixes,
                    excluded_logins = App.AlertLongRunningQueryExcludedLogins
                },
                tempdb_space = new
                {
                    enabled = App.AlertTempDbSpaceEnabled,
                    threshold_percent = App.AlertTempDbSpaceThresholdPercent
                },
                low_disk = new
                {
                    enabled = App.AlertLowDiskEnabled,
                    threshold_percent = App.AlertLowDiskThresholdPercent,
                    threshold_gb = App.AlertLowDiskThresholdGb,
                    /* Darling nests the #1136 CRITICAL-tier floors INSIDE low_disk and drops the "disk"
                       prefix the statics carry, so these are critical_free_*, not disk_critical_free_*.
                       Naming them after the App members would have read as correct and been a fifth
                       key-level mismatch. */
                    critical_free_percent = App.AlertDiskCriticalFreePercent,
                    critical_free_gb = App.AlertDiskCriticalFreeGb
                },
                /* Darling reports a self_alerts group in this position — the thresholds for alerts about its
                   own STORE rather than a monitored server: store volume, collection staleness/failure
                   counts, store-job cadence, and the Retention Held tiers. Deliberately NOT emitted here.
                   AppAlertEngineSettings returns shipped constants for the members Lite has any analogue of
                   at all, because Lite has no headless store volume and no fleet collection loop to
                   self-monitor; the rest — the TimescaleDB background-job and retention-policy knobs — name
                   machinery a DuckDB store does not contain. Reporting either kind under names that read as
                   knobs would tell an agent it can tune something Lite cannot, and an admitted gap beats an
                   overstated capability. Deliberately no count of which members fall in which half: a
                   numeral here would be a frozen enumeration that the next Darling self-alert knob leaves
                   behind. */
                pvs = new
                {
                    enabled = App.AlertPvsEnabled,
                    threshold_percent = App.AlertPvsThresholdPercent,
                    floor_gb = App.AlertPvsFloorGb
                },
                file_growth = new
                {
                    enabled = App.AlertFileGrowthEnabled,
                    /* #3539 A8c: MB per HOUR averaged over lookback_minutes, like Darling's; the key keeps its
                       spelling (McpAlertSettingsKeyTests derives the shape from Darling's source) and the unit is
                       stated in the tool description above. */
                    rise_mb = App.AlertFileGrowthRiseMb,
                    volume_percent = App.AlertFileGrowthVolumePercent,
                    lookback_minutes = App.AlertFileGrowthLookbackMinutes
                },
                long_running_job = new
                {
                    enabled = App.AlertLongRunningJobEnabled,
                    multiplier = App.AlertLongRunningJobMultiplier
                },
                failed_job = new
                {
                    enabled = App.AlertFailedJobEnabled,
                    lookback_minutes = App.AlertFailedJobLookbackMinutes
                },
                database_state = new { enabled = App.AlertDatabaseStateEnabled },
                /* #2417: the Availability Group family, which Darling emitted to nobody until now and
                   Lite therefore could not mirror. Lite has evaluated all four AG conditions since #1726
                   off these same statics, so this is surface, not new alerting. Group and key spellings
                   are Darling's verbatim, including `enabled` for what the store calls notify_ag_health —
                   inside this payload `enabled` always means "this alert family is on".

                   disconnect_refire_minutes was the one member Lite could not honestly report, because
                   #1726 mirrored three of Darling's four AG knobs and Lite had no re-fire at all — not the
                   static, not the settings key, and not the edge state that could re-announce. #2426 built
                   it, so the exemption that stood here is gone and all four members compare. */
                ag = new
                {
                    enabled = App.NotifyAgHealth,
                    lag_threshold_seconds = App.AgLagAlertSeconds,
                    redo_queue_threshold_kb = App.AgRedoQueueAlertKb,
                    disconnect_refire_minutes = App.AgDisconnectRefireMinutes
                },
                cooldown_minutes = App.AlertCooldownMinutes,
                excluded_databases = App.AlertExcludedDatabases,
                delivery = new
                {
                    /* The second value-level alignment after cpu.mode — and the one place ToString() is the
                       right answer rather than a mapping. AlertNotificationMode is the SHARED enum both SKUs
                       run on, and Darling's store holds literally a.DeliveryMode.ToString(), so the two apps
                       cannot drift the way Lite's app-local CpuAlertMode could. Darling's
                       update_alert_settings validates delivery.mode against "Summary"/"PerEvent", which are
                       those same member names. */
                    mode = App.AlertDeliveryMode.ToString(),
                    per_event_max = App.AlertPerEventMaxPerCycle,
                    /* #3314. Lite runs the SAME shared throttle -- WebhookAlertService and EmailSendCore both
                       hand IncidentCooldown App.EmailCooldownMinutes through AppAlertSettings -- so the
                       channel-neutral name Darling adopted applies here verbatim. The stored spelling stays
                       email_cooldown_minutes in settings.json and the Settings window; only the wire key is
                       channel-neutral, because one number governs Teams, Slack, PagerDuty, the generic
                       webhook AND email. DISTINCT from cooldown_minutes above, which gates the engine's FIRE
                       decision rather than the post. */
                    cooldown_minutes = App.EmailCooldownMinutes
                },
                analysis = new
                {
                    enabled = App.AnalysisEnabled,
                    interval_minutes = App.AnalysisIntervalMinutes,
                    /* Darling's own note applies here too: this is the ANALYSIS section's delivery gate, and
                       is why the master switch above had to be renamed off notifications_enabled. */
                    notifications_enabled = App.AnalysisNotificationsEnabled,
                    notify_severity = App.AnalysisNotifySeverity,
                    notify_cooldown_minutes = App.AnalysisNotifyCooldownMinutes,
                    /* #3712: where a notify-worthy but UNCORROBORATED finding goes — Darling's key, Lite's own live
                       value (Settings → Alerts, or analysis_uncorroborated_route in the settings file). The note
                       key rides for shape parity with Darling; here it names the surface that edits it. Darling's
                       uncorroborated_route_source (which of its two homes decided) is deliberately NOT mirrored:
                       Lite has one home, so the key could only ever read a constant. */
                    uncorroborated_route = FindingRouting.RouteText(App.AnalysisUncorroboratedRoute),
                    uncorroborated_route_note = UncorroboratedRouteNote
                },
                /* Lite-only, and left last so the shared groups above stay in Darling's order: Lite delivers
                   its own email, where Darling manages delivery credentials outside the settings row and
                   reports no smtp group at all. The password is reported as a boolean, never a value. */
                smtp = new
                {
                    enabled = App.SmtpEnabled,
                    server = App.SmtpServer,
                    port = App.SmtpPort,
                    use_ssl = App.SmtpUseSsl,
                    username = App.SmtpUsername,
                    from_address = App.SmtpFromAddress,
                    recipients = App.SmtpRecipients,
                    password_configured = !string.IsNullOrEmpty(App.GetSmtpPassword())
                }
            };

            return Task.FromResult(JsonSerializer.Serialize(settings, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("get_alert_settings", ex));
        }
    }

    [McpServerTool(Name = "get_notification_routes"), Description(
        "Gets the alert FAMILY taxonomy (#3598) — which metric names belong to self-monitor, reports, agent-jobs " +
        "and performance — and reports that this edition has NO notification routes: every alert goes to every " +
        "channel configured in Settings. The Darling edition's twin of this tool also lists the sparse routes " +
        "layered over its central store's channel set; routes_supported is false here, so an agent asked to route " +
        "a family on this instance should say so rather than look for a route to edit. The taxonomy is the shared " +
        "one, so an alert seen in get_alert_history classifies identically on both editions.")]
    public static Task<string> GetNotificationRoutes()
    {
        try
        {
            return Task.FromResult(JsonSerializer.Serialize(new
            {
                families = AlertFamily.All.Select(family => new
                {
                    family,
                    metrics = AlertFamily.MetricFamilies
                        .Where(kv => kv.Value == family && !AlertFamily.RecoveryPairs.ContainsKey(kv.Key))
                        .Select(kv => kv.Key)
                        .OrderBy(m => m, StringComparer.Ordinal),
                    metric_prefixes = AlertFamily.PrefixFamilies.Where(p => p.Family == family).Select(p => p.Prefix + "*"),
                }),
                recoveries_route_as_their_firing = AlertFamily.RecoveryPairs.Select(kv => new { recovery = kv.Key, firing = kv.Value }),
                unclassified_metrics_route_as = AlertFamily.Performance,
                routes_supported = false,
                route_count = 0,
                routes = Array.Empty<object>(),
                note = "This edition delivers every alert to every configured channel; per-family routes are a Darling (central store) feature.",
            }, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("get_notification_routes", ex));
        }
    }

    [McpServerTool(Name = "get_mute_rules"), Description("Gets the configured alert mute rules. Mute rules suppress specific recurring alerts while still logging them.")]
    public static Task<string> GetMuteRules(
        MuteRuleService muteRuleService,
        [Description("Include only enabled rules. Default true.")] bool enabled_only = true)
    {
        try
        {
            var all = muteRuleService.GetRules();
            var rules = all;
            if (enabled_only)
                rules = rules.Where(r => r.Enabled && (r.ExpiresAtUtc == null || r.ExpiresAtUtc > DateTime.UtcNow)).ToList();

            if (rules.Count == 0)
            {
                /*
                    Same two facts as Darling's twin, in the same words. Both are true negatives -- nothing
                    is being suppressed either way -- but "no rule has ever been written" and "five rules
                    that all lapsed" are different states, and the second is a mute somebody INTENDED that
                    is no longer in force.
                */
                var configured = all.Count;
                return Task.FromResult(McpHelpers.Status(
                    "empty",
                    configured == 0
                        ? "No mute rules are configured for this store, so no alert is being suppressed anywhere — a quiet alert history is genuine rather than muted."
                        : $"No mute rules are in force right now: {configured} rule(s) exist but every one of them is disabled or expired, so nothing is being suppressed. Pass enabled_only=false to list them — this is a lapsed mute, not an absent one.",
                    new { enabled_only, configured_count = configured, excluded_by_filter = configured - rules.Count }));
            }

            var result = new
            {
                mute_rules = rules.Select(r => new
                {
                    id = r.Id,
                    enabled = r.Enabled,
                    created_at_utc = r.CreatedAtUtc.ToString("o"),
                    expires_at_utc = r.ExpiresAtUtc?.ToString("o"),
                    reason = r.Reason,
                    server_name = r.ServerName,
                    metric_name = r.MetricName,
                    database_pattern = r.DatabasePattern,
                    query_text_pattern = r.QueryTextPattern,
                    wait_type_pattern = r.WaitTypePattern,
                    job_name_pattern = r.JobNamePattern,
                    summary = r.Summary
                }).ToArray(),
                total_count = rules.Count
            };

            return Task.FromResult(JsonSerializer.Serialize(result, McpHelpers.JsonOptions));
        }
        catch (Exception ex)
        {
            return Task.FromResult(McpHelpers.FormatError("get_mute_rules", ex));
        }
    }
}
