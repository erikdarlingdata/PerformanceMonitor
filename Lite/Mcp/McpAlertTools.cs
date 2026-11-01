using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
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
    /// which one decided under a provenance key beside the route; Lite has ONE home, its settings file, so this
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

    [McpServerTool(Name = "get_alert_history"), Description("Gets alert history, NEWEST FIRST: each row FIRED; notification_type says whether it was DELIVERED, a separate question from DISMISSED (UI-acknowledged), excluded by default. THE PAGE IS BOUNDED BY limit, NOT hours_back: truncated says the window held more, and oldest/newest_returned_alert_time bound how far the page reached. An EMPTY page can mean no alerts fired, or that every alert here was dismissed: dismissed_excluded_count says which; include_dismissed = true returns them, labelled dismissed = true. A null send_error proves nothing about delivery.<<GUIDE>>Gets recent alert history from the alert log, NEWEST FIRST. Shows what alerts fired, when, and whether email was sent successfully. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: alerts_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_alert_time / newest_returned_alert_time bound the page — under newest-first ordering the oldest stamp IS how far back this read reached. Raise limit or narrow hours_back when truncated is true; widening hours_back cannot help. BY DEFAULT THIS READ EXCLUDES DISMISSED ALERTS — rows an operator acknowledged in the Alerts History tab. Dismissal says nothing about whether the alert fired or mattered, so an incident reconstruction that ignores it can miss the very critical someone already looked at: dismissed_excluded says whether the filter applied and dismissed_excluded_count is how many rows in the window it removed, and include_dismissed = true returns them, each labelled dismissed = true. On this edition an alert that was dismissed AFTER aging into the parquet archive is removed by the archive view itself and can be neither returned nor counted here. notification_type is the delivery disposition and is the ONLY field that says why a row did not deliver: 'email'/'webhook'/'email+webhook' delivered on that channel; 'tray' is this instance's own balloon notification, which every non-muted alert gets, so it is what most rows read here and it does NOT report the email or webhook outcome; 'failed' means a channel was attempted and came back unsuccessful, with send_error carrying the first failing channel's text; 'muted' means a mute rule suppressed it; 'digest' means the analysis finding was UNCORROBORATED (one fact in its chain, no matched co-fire check) and the corroboration gate kept it off email, the webhooks and the tray — it is persisted, shown in Recommendations with a not-paged marker, and routing_reason on the row says which components decided; 'none' is a resolution row, which no channel applies to. Do NOT split the not-delivered rows on send_error: it is null whenever a cooldown or the per-metric repeat budget suppressed a send, on 'digest' rows, and on every row written before those dispositions existed. 'throttled' and 'folded' are recorded by the headless service; on this instance the tray channel answers first, so a cooldown-suppressed or folded send is stored as 'tray'. 'undelivered' is a retained legacy value that means throttled OR folded OR failed with nothing in the row to say which. severity is the row's tier — 'critical', 'warning', 'info' or 'resolution' — and severity_source says where it came from: 'fired' when the row persisted the tier the alert actually fired at (graded alerts such as Poison Wait, Volume Free Space and Database State fire Warning OR Critical by measurement), 'metric_name' when the row carries no tier and the metric's name is the only evidence (rows written before the tier was persisted, alerts whose severity is fixed per metric, and every resolution row). Do not infer a graded alert's tier from its name: a 'Poison Wait' row with severity 'warning' fired as a warning. routing is the corroboration gate's decision for an 'Analysis: …' finding, one step upstream of delivery: 'page' when it earned a channel (two or more facts in its chain, a matched co-fire check, or one of the two by-construction stories), 'digest' when it was a lone uncorroborated fact; routing_reason names the components. Both are null on engine alerts and on analysis rows written before the gate existed. Dismissal is an acknowledgement, not a verdict — a dismissed critical still fired — so set this when reconstructing an incident rather than triaging what is still open. Each row then carries dismissed so the two populations stay distinguishable. Optional metric_name and family narrow the page IN THE QUERY, ahead of limit (#6074): metric_name matches the stored alert name exactly, ignoring case; family is one of self-monitor, reports, agent-jobs, performance, external (get_notification_routes lists them) and covers every alert in that family, a recovery row included. An unknown family or a name with no alerts in the window answers empty and names the filter, so a mistyped filter is distinguishable from a quiet window. include_streak = true adds streak_days to each row (#6261): how many days in a row, counting back from the day before that alert, the same metric on the same server fired within 30 minutes of its time of day, so 'this is the Nth day in a row' is streak_days + 1. A muted fire counts (it fired); a recovery row has null. streak_days is at most 7 (the look-back), and streak_at_least_days_window true means it filled the look-back, so it is a floor, not an exact count. Rows carry neither field unless include_streak is set.")]
    public static async Task<string> GetAlertHistory(
        LocalDataService dataService,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 50. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        /* Appended after as_of: MCP invokes by name, and a trailing optional is the one position no existing
           positional C# caller can be re-bound by. Same convention as Darling's twin. */
        [Description("Include alerts an operator has dismissed in the Alerts History tab. Default false, which is the tab's own read.")] bool include_dismissed = false,
        [Description("Only alerts with this metric name (e.g. 'High CPU', 'Blocking Detected'), matched exactly, ignoring case. Omit for every metric.")] string? metric_name = null,
        [Description("Alert family: self-monitor, reports, agent-jobs, performance or external. Not an investigation family such as blocking or resources: filter those with metric_name. Omit for all.")] string? family = null,
        [Description("Add streak_days to each row: days in a row (up to 7) this metric fired at this time of day before the alert, muted fires counting. Default false: rows are unchanged.")] bool include_streak = false)
    {
        var answer = await GetAlertHistoryCore(dataService, hours_back, limit, as_of, include_dismissed, metric_name, family, include_streak);
        /* #7005: the floor is the store's data floor (the wait_stats collector, which every collecting server runs every cycle: the oldest of its runs or rows any server has in the window; not the collection log, which also holds the Query Store backfill's older runs),
           not the oldest alert row: a window that holds no alert is not truncation, a window that starts before the data is. The alert log is
           archived and pruned with the collector tables (ArchiveService.ArchivableTables), so it has no earlier cutoff of its own. */
        if (McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) != null)
        {
            return answer;
        }

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);
            return await McpQueryTools.WithProbedWindowNoticeAsync(
                answer,
                () => dataService.GetFleetWindowFloorAsync(QueryWindowRelation.WaitStats, windowStart, windowEnd),
                () => dataService.GetOldestFleetWindowRowAsync(QueryWindowRelation.WaitStats),
                windowStart, windowEnd, "collector data");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_alert_history", ex);
        }
    }

    private static async Task<string> GetAlertHistoryCore(
        LocalDataService dataService,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 50. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        /* Appended after as_of: MCP invokes by name, and a trailing optional is the one position no existing
           positional C# caller can be re-bound by. Same convention as Darling's twin. */
        [Description("Include alerts an operator has dismissed in the Alerts History tab. Default false, which is the tab's own read.")] bool include_dismissed = false,
        [Description("Only alerts with this metric name (e.g. 'High CPU', 'Blocking Detected'), matched exactly, ignoring case. Omit for every metric.")] string? metric_name = null,
        [Description("Alert family: self-monitor, reports, agent-jobs, performance or external. Not an investigation family such as blocking or resources: filter those with metric_name. Omit for all.")] string? family = null,
        [Description("Add streak_days to each row: days in a row (up to 7) this metric fired at this time of day before the alert, muted fires counting. Default false: rows are unchanged.")] bool include_streak = false)
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
            /* #6074: the optional metric and family filters, applied in the SQL (ahead of the row cap). An unknown family is an
               EMPTY answer that names the filter, not an error. */
            var (metricFilter, familyFilter) = AlertFamily.NormalizeHistoryFilter(metric_name, family);
            var filterSentence = AlertFamily.DescribeHistoryFilter(metricFilter, familyFilter);

            var rows = await dataService.GetAlertHistoryAsync(hours_back, limit + 1, asOfUtc: windowEnd, includeDismissed: include_dismissed, metricName: metricFilter, family: familyFilter);
            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            /* The hidden filter, measured: how many rows in this window it removed. Zero is a real answer
               (nothing was hidden). Not probed when the filter is off, because then it removed nothing by
               construction. */
            var dismissedExcludedCount = include_dismissed
                ? 0L
                : await dataService.CountDismissedAlertsAsync(hours_back, serverId: null, asOfUtc: windowEnd, metricName: metricFilter, family: familyFilter);

            if (page.Count == 0)
            {
                /* An empty default page over a window that DOES hold dismissed rows is not "no alerts": it is
                   "every alert here was acknowledged", and the one-sentence quiet-window answer would send
                   the caller off widening a window whose contents they were never shown. */
                return dismissedExcludedCount > 0
                    ? McpHelpers.Status(
                        "empty",
                        $"No undismissed alerts found in the specified time range{filterSentence}, but {dismissedExcludedCount} dismissed {(dismissedExcludedCount == 1 ? "alert was" : "alerts were")} excluded by the default filter. Re-run with include_dismissed = true to see them — a dismissed alert still fired.")
                    : McpHelpers.Status("empty", $"No alerts found in the specified time range{filterSentence}.");
            }

            IEnumerable<object> alerts = page.Select<AlertHistoryRow, object>(r =>
            {
                var (severity, severitySource) = AlertHistoryRowSeverity.Describe(r.MetricName, r.ContextJson);
                var routing = AlertContextSerializer.TryReadRouting(r.ContextJson);
                return new
                {
                    alert_time = r.AlertTime.ToString("o"),
                    server_id = r.ServerId,
                    server_name = r.ServerName,
                    metric_name = r.MetricName,
                    /* #5654: Darling's external-alert source for the row. Lite has no external alert intake, so the key is
                       always null; it is here so both products return one row shape. */
                    external_source = (string?)null,
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
                       parity of shape. A row's route names the channels the firing resolved to: route_id is the
                       route that supplied each (null = the parent settings) and source is "Exact", "Family" or
                       "Wildcard" for a route, "Default" for the parent (#5750). #4750: each destination also says what
                       the send to it did ("delivered", "failed" or "not attempted"; null on a row written
                       before that was recorded), so a channel that failed beside one that delivered shows
                       here. The outcome word only: the failure's reason stays in send_error. A row whose
                       fan-out reached no channel (a cooldown, a fold, a mute, nothing configured) answers
                       null. */
                    route = AlertContextSerializer.TryReadRoute(r.ContextJson) is { } route
                        ? new
                        {
                            family = route.Family,
                            route_id = route.RouteId,
                            destinations = route.Destinations.Select(d => new { channel = d.Channel, route_id = d.RouteId, source = d.Source, outcome = d.Outcome }),
                        }
                        : null,
                    /* #3712: the corroboration gate's decision for an analysis finding — 'page' or 'digest' —
                       and the sentence naming the components it read, read off the row the way severity_source
                       is. Null on every engine alert and on analysis rows written before the gate existed. */
                    routing = routing?.Route,
                    routing_reason = routing?.Reason,
                    detail_text = r.DetailText,
                    /* #5642: the newest sample the alert judged and how old it was when the alert fired, read off the
                       persisted context (computed once at fire time, never from the clock now). Null on a row with
                       no sample time: written before the field existed, or from a source that has none. The shared
                       wire type, the one Darling's get_alert_history publishes, so the two SKUs cannot drift. */
                    newest_sample = AlertNewestSampleWire.FromContextJson(r.ContextJson),
                };
            }).ToList();

            /* #6261: opt-in, so a caller who never sends include_streak reads the same rows. One extra read for the whole page
               (LocalDataService.GetAlertStreaksAsync), the fields added to each serialized row. Darling's twin. */
            if (include_streak)
            {
                var streaks = await dataService.GetAlertStreaksAsync(page);
                alerts = alerts.Select((row, i) =>
                {
                    var node = JsonSerializer.SerializeToNode(row, McpHelpers.JsonOptions)!.AsObject();
                    node["streak_days"] = streaks[i];
                    node["streak_at_least_days_window"] = streaks[i] is null ? null : streaks[i] >= InvestigationAlertRecurrence.Days;
                    return (object)node;
                }).ToList();
            }

            return JsonSerializer.Serialize(new
            {
                hours_back,
                /* #3541 A3: `total_alerts` is gone — it was the page count under a name that promised the
                   window. What is published is what was measured: the page, whether the window held more,
                   the span the page covers (newest-first, so the oldest stamp IS the reach), and the filter
                   that shaped the population together with how much it removed. */
                alerts_returned = page.Count,
                truncated,
                oldest_returned_alert_time = McpHelpers.FormatEffectiveStart(page.Min(r => r.AlertTime)),
                newest_returned_alert_time = McpHelpers.FormatEffectiveStart(page.Max(r => r.AlertTime)),
                order = "alert_time_desc",
                metric_name_filter = metricFilter,
                family_filter = familyFilter,
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

    [McpServerTool(Name = "get_alert_settings"), Description("Gets the alert configuration currently in effect: enabled flags, thresholds, cooldowns, delivery mode, and cadences. cooldown_minutes gates whether an alert FIRES; delivery.cooldown_minutes separately bounds the resulting post, per alert fingerprint and, for a re-notification, per metric across every monitored server. Darling: an unseeded store answers status unavailable and darling.json's defaults still govern until it seeds. Lite always answers its live in-memory settings. An empty knob list (e.g. long_running_query's exclusions) means cleared, not a default in force.<<GUIDE>>Gets the current alert configuration this instance is running on: which alerts are enabled and their thresholds (CPU, blocking, deadlocks, poison waits, runnable queue, client timeouts, long-running queries and jobs, tempdb space, low disk, PVS, file growth, failed jobs, database state, Availability Group health, connection loss), the cooldown, the excluded databases, the deadlock/blocking delivery mode and cooldown, the scheduled-analysis cadence, and the SMTP email configuration. The two cooldowns govern different stages: top-level cooldown_minutes gates whether an alert FIRES, delivery.cooldown_minutes bounds the resulting email/Teams/Slack/PagerDuty/webhook send both per alert FINGERPRINT and, for a re-notification, per METRIC across every monitored server (the servers it holds back are named on the send that does go out, under an 'Other Servers Affected' section; a first notice is never held back, and delivery.mode PerEvent opts out of the per-metric bound). The file_growth group's rise_mb is megabytes per HOUR, averaged over file_growth.lookback_minutes — a rate, not a total for the window: 10240 means 10 GB/hr whether the lookback is 5 minutes or 24 hours, and the engine scales it to the window. poison_wait.threshold_ms is RETIRED: the Poison Wait alert grades ACCUMULATED wait over a ten-minute window, so this value is stored and reported for compatibility but consulted by nothing — poison_wait.threshold_ms_note says so beside it, and poison_wait.enabled is the live switch. runnable_queue.enabled is the Runnable Queue alert's switch, and runnable_queue.warning_per_scheduler and runnable_queue.critical_per_scheduler are its bars: runnable tasks per scheduler, averaged over the collections of the last 5 minutes when there are at least 3, otherwise over the newest 3 if all fall in the last 10 minutes (or in 4 times the gap between cpu_scheduler_stats collections when that is longer, so the Low-Impact preset's 5-minute collection still evaluates), as the engine applies them (Critical is never below Warning). client_timeouts.enabled is the Client Timeouts alert's switch (ON by default), and the alert's bars are RELATIVE to each server's own normal rate: client_timeouts.warning_per_minute and client_timeouts.critical_per_minute are the FLOORS (client attentions, a client cancelled a query or its timeout expired, per minute over the attention collector's last runs, about 3 minutes; Warning at least 1, Critical never below Warning), and client_timeouts.warning_multiple and client_timeouts.critical_multiple (defaults 3 and 10) scale the server's baseline, the p99.9 of its own 3-minute average attention rate over the last 7 days, read about once a day. The Warning bar is the higher of the Warning floor and warning_multiple x baseline, the Critical bar the higher of the Critical floor and critical_multiple x baseline, and a server with under a day of capture, or under 720 minutes with a successful collector run, has no baseline and uses the floors alone. The fired alert names the baseline, the multiple and the bar crossed. analysis.uncorroborated_route is where a notify-worthy but UNCORROBORATED finding goes — one fact in its chain and no matched co-fire check: 'digest' (the default) keeps it off email, the webhooks and the tray while it is still recorded (Alerts tab status Digest) and shown in Recommendations with a not-paged marker; 'page' restores delivery of every finding at or above notify_severity; a corroborated finding is delivered under either. analysis.uncorroborated_route_note says where it is edited. long_running_query.excluded_program_name_prefixes and long_running_query.excluded_logins are the Long-Running Query alert's OPT-OUT knob: a session whose program_name STARTS WITH an entry of the first list, or whose login_name IS an entry of the second (both case-insensitive, no wildcard grammar), is NOT EVALUATED by that alert at all — not read into the decision, not counted, not fingerprinted — the opposite of a mute rule, which silences a fire already decided. The exclusion is applied in the read ahead of long_running_query.max_results, so an excluded session never consumes a result slot, and the fired alert's card carries Excluded Count / Excluded By Program Prefix / Excluded By Login items saying how many SESSIONS each list removed on that evaluation (a session matching both counts once, under the prefix). The lists ship SEEDED from a 7-day read of one large production store, whose long-running population fell into four classes: (1) SQL Agent job steps — program_name prefix 'SQLAgent - TSQL JobStep', ~460 sessions a week across 11 jobs, medians 35–62 min — the default prefix; (2) the NT AUTHORITY\\SYSTEM and NT AUTHORITY\\NETWORK SERVICE logins — the permanent multi-day CDC-shaped background — the default logins; (3) the application's admin login — deliberately NOT a default, because it runs the job wave but also real ad-hoc long-runners, and the job-step prefix already covers its share; (4) named humans — never excluded, they are what the page is for. The seeds are DEFAULTS: an empty list here means the operator cleared it in the Settings window and that arm excludes nothing. The same nested shape Darling's get_alert_settings returns, minus its self_alerts group (the headless service's own store-volume and collection-health thresholds, which a single-instance Lite install has no equivalent for) and plus smtp, which Lite delivers itself. Read-only: Lite has no update_alert_settings, so these change in the Settings window.")]    public static Task<string> GetAlertSettings()    {
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
                /* #5641: the Runnable Queue alert's switch and its two bars, runnable tasks per scheduler
                   averaged over 5 minutes. The bars are the EFFECTIVE values (AppAlertEngineSettings clamps
                   Warning to at least 1 and Critical to at least Warning, the same RunnableQueueEvaluator.ClampBars
                   the engine reads), so an agent sees what the alert actually grades against, not a raw
                   file value the engine would have corrected. Group and key spellings follow poison_wait's
                   convention (group = the alert, snake_case keys), which is also how Darling reports them. */
                runnable_queue = new
                {
                    enabled = App.AlertRunnableQueueEnabled,
                    warning_per_scheduler = RunnableQueueEvaluator.ClampBars(
                        App.AlertRunnableQueueWarningPerScheduler, App.AlertRunnableQueueCriticalPerScheduler).Warning,
                    critical_per_scheduler = RunnableQueueEvaluator.ClampBars(
                        App.AlertRunnableQueueWarningPerScheduler, App.AlertRunnableQueueCriticalPerScheduler).Critical
                },
                /* #5640: the Client Timeouts alert's switch (ON by default) and its two bars, client attentions per
                   minute over the attention collector's last runs. EFFECTIVE values through AttentionEvaluator.ClampBars,
                   like runnable_queue above and like Darling's payload. */
                client_timeouts = new
                {
                    enabled = App.AlertClientTimeoutsEnabled,
                    warning_per_minute = AttentionEvaluator.ClampBars(
                        App.AlertClientTimeoutsWarningPerMinute, App.AlertClientTimeoutsCriticalPerMinute).Warning,
                    critical_per_minute = AttentionEvaluator.ClampBars(
                        App.AlertClientTimeoutsWarningPerMinute, App.AlertClientTimeoutsCriticalPerMinute).Critical,
                    /* #5736: the two bars above are FLOORS; the bar for a server is the higher of its floor and the multiple times its own baseline. */
                    warning_multiple = AttentionBaselineMath.ClampMultiples(
                        App.AlertClientTimeoutsWarningMultiple, App.AlertClientTimeoutsCriticalMultiple).Warning,
                    critical_multiple = AttentionBaselineMath.ClampMultiples(
                        App.AlertClientTimeoutsWarningMultiple, App.AlertClientTimeoutsCriticalMultiple).Critical
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
                excluded_volumes = App.AlertExcludedVolumes,
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
                       provenance key (which of its two homes decided the route) is deliberately NOT mirrored:
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
        "Gets the alert family taxonomy, and where each family's posts go. Zero routes is the ordinary state: every alert goes to every parent channel configured in Settings. Sparse routes are layered over that parent set; an EMPTY channel on a route INHERITS the parent's rather than silencing it; resolution is scope first (server, tag, fleet-wide), then exact metric, then family, then parent; routes report scope and orphaned. Lite and Darling report the same fields. Routing decides WHERE a post lands, never WHETHER it is sent.<<GUIDE>>Gets the alert FAMILY taxonomy — which metric names belong to self-monitor, reports, agent-jobs " +
        "and performance — and the sparse notification routes layered over the parent channel set. Lite and Darling report the same route fields. Zero routes " +
        "means every alert goes to every channel configured in Settings (the parent). A route names a family (or one " +
        "exact metric name, or * on a scoped route) and a destination per channel type; an EMPTY channel on a route " +
        "INHERITS the parent's, so a route can redirect Slack for one family and leave PagerDuty as the parent has it. " +
        "A route cannot silence a channel the parent has: empty means inherit, not off. A route may be scoped to one " +
        "server (scope_server_id) or to a tag and every tag under it (scope_tag_id); a route whose server or tag was " +
        "deleted stays, is orphaned = true, and matches nothing. Resolution order per channel: scope first (a route " +
        "for the alert's server, then one for a tag it is under, the deeper tag first, then a fleet-wide route), and " +
        "inside a scope an enabled route matching the metric EXACTLY, then an analysis finding's CATEGORY (metric_match " +
        "'Analysis: <category>', match_kind analysis_category: every finding of that category, whatever its hash), then " +
        "its FAMILY, then * (scoped routes only), " +
        "then the parent; within a level the lowest route_id with a non-empty column wins. A recovery routes as its " +
        "firing. Routing sits AFTER the cooldown: it changes where a post lands, never whether it is sent. " +
        "Destinations (webhook URLs, the PagerDuty routing key) are bearer secrets kept in Windows Credential Manager " +
        "and are NOT reported: configured_channels lists which channels each route sets, by name; smtp_recipients is " +
        "reported because an address list is not a secret. modified_at_utc is null in Lite, which does not stamp a " +
        "route's last change. Routes are authored in Settings > Manage Notification Routes; here they can be disabled " +
        "or deleted (set_notification_route_enabled / delete_notification_route, served when mcp_allow_writes is true). " +
        "agent_level says whether a matching alert opens an investigation request for an agent (null inherits from a less " +
        "specific route, 0 is off and stops the lookup, 1 investigates, within agent_max_per_hour and agent_max_per_day); it " +
        "never changes a destination. agent_requests_last_hour and agent_requests_last_day count the requests the route " +
        "opened, and are null (agent_use_read false) when they could not be read. A route with agent_level 1 is changed only in " +
        "Settings: the two write tools refuse it.")]
    public static async Task<string> GetNotificationRoutes(
        LocalDataService dataService,
        ServerManager serverManager,
        IInvestigationRequestStore investigationStore)
    {
        /* #5820: the host registers the one investigation store (McpHostService), so the tool takes it directly. */
        try
        {
            return await ReadNotificationRoutesAsync(
                CreateRouteStore(dataService, serverManager),
                serverManager.GetAllServers(),
                await dataService.GetServerTagsAsync(),
                investigationStore);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_notification_routes", ex);
        }
    }

    /// <summary>The store the route tools use, built over the service's own database without the host registering one (#5750).</summary>
    private static DuckDbNotificationRouteStore CreateRouteStore(LocalDataService dataService, ServerManager serverManager) =>
        new(dataService.Database, serverManager.GetKnownServerIds, RouteSecretAccess.CredentialManager());

    /// <summary>The get_notification_routes payload. Same top-level and per-route field names as Darling's twin (a test pins
    /// them equal). Never carries a URL, key or host: only the channel names. Internal to the tests so they can hand it a store
    /// that keeps its secrets in memory.</summary>
    internal static async Task<string> ReadNotificationRoutesAsync(
        DuckDbNotificationRouteStore store,
        IReadOnlyCollection<ServerConnection> servers,
        IReadOnlyCollection<ServerTag> tags,
        IInvestigationRequestStore requests)
    {
        var routes = await store.ListAsync();
        var agentUse = await ReadAgentUseAsync(requests);

        var serverNames = ServerNames(servers);
        var tagNames = tags.ToDictionary(t => t.Id, t => t.Name);

        return JsonSerializer.Serialize(new
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
            resolution_order = new[] { "server-scoped route", "tag-scoped route (deeper tag first)", "fleet-wide route", "parent channel (Settings > Notifications)" },
            resolution_within_a_scope = new[] { "exact metric", "family", "* (scoped routes only)" },
            route_count = routes.Count,
            enabled_route_count = routes.Count(r => r.Enabled),
            routes = routes.Select(r => RoutePayload(r, serverNames, tagNames, agentUse)),
            /* #5820: false when the investigation request counts could not be read (the store's read failed); agent_requests_* are
               then null. */
            agent_use_read = agentUse is not null,
        }, McpHelpers.JsonOptions);
    }

    /// <summary>Each route's investigation requests in the last hour and day (#5820), or null when the store's read fails: the
    /// route list is still answered, with the use unknown.</summary>
    private static async Task<IReadOnlyDictionary<int, (int LastHour, int LastDay)>?> ReadAgentUseAsync(IInvestigationRequestStore requests)
    {
        try
        {
            return await requests.RouteUseAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    internal static object RoutePayload(
        NotificationRoute r, IReadOnlyDictionary<int, string> serverNames, IReadOnlyDictionary<int, string> tagNames,
        IReadOnlyDictionary<int, (int LastHour, int LastDay)>? agentUse = null)
    {
        /* Channel names, in Darling's order. A channel saved but unreadable (off) is still configured. */
        var channels = new List<string>();
        foreach (var channel in new[] { NotificationRouter.TeamsChannel, NotificationRouter.SlackChannel, NotificationRouter.GenericChannel, NotificationRouter.PagerDutyChannel })
        {
            if (r.HasWebhookDestinationFor(channel) || r.IsOffFor(channel))
            {
                channels.Add(channel);
            }
        }

        if (r.HasEmailDestination)
        {
            channels.Add(NotificationRouter.EmailChannel);
        }

        return new
        {
            route_id = r.RouteId,
            metric_match = r.MetricMatch,
            match_kind = NotificationRouteKinds.MatchKind(r.MetricMatch),
            family = NotificationRouteKinds.FamilyOf(r.MetricMatch),
            configured_channels = channels,
            smtp_recipients = r.SmtpRecipients,
            enabled = r.Enabled,
            modified_at_utc = (DateTime?)null,
            scope_server_id = r.ScopeServerId,
            scope_server_name = r.ScopeServerId is { } sid && serverNames.TryGetValue(sid, out var sn) ? sn : null,
            scope_tag_id = r.ScopeTagId,
            scope_tag_name = r.ScopeTagId is { } tid && tagNames.TryGetValue(tid, out var tn) ? tn : null,
            /* Null for a fleet-wide route and for a scoped route whose scope could not be read (scope_unreadable tells the two apart). */
            expanded_server_count = r.IsScoped && !r.ScopeUnexpanded ? r.ExpandedServerIds?.Count ?? 0 : (int?)null,
            orphaned = r.Orphaned,
            scope_unreadable = r.ScopeUnexpanded,
            /* #5820: the agent channel, the same five settings as Darling. agent_level null inherits, 0 is off and stops the lookup,
               1 opens investigation requests; the limits apply only where the level is 1. */
            agent_level = r.AgentLevel,
            agent_max_per_hour = r.AgentMaxPerHour,
            agent_max_per_day = r.AgentMaxPerDay,
            agent_lease_minutes = r.AgentLeaseMinutes,
            agent_expiry_minutes = r.AgentExpiryMinutes,
            agent_requests_last_hour = agentUse is null ? (int?)null : agentUse.TryGetValue(r.RouteId, out var hourUse) ? hourUse.LastHour : 0,
            agent_requests_last_day = agentUse is null ? (int?)null : agentUse.TryGetValue(r.RouteId, out var dayUse) ? dayUse.LastDay : 0,
        };
    }

    /// <summary>The refusal for an agent route (#5820): a route whose agent_level is 1 or more decides which alerts open
    /// investigation requests, and the agent tools are served over this same listener, so it is changed only by a person in Settings.
    /// Null for any other route, including agent_level 0: that is the route's explicit Off (null inherits, 0 is off, 1 investigates),
    /// so it opens no investigation and is an ordinary route to enable, disable or delete.</summary>
    internal static string? AgentRouteRefusal(NotificationRoute route, string tool) =>
        route.AgentLevel is null or < 1
            ? null
            : McpHelpers.Status("refused",
                $"Route {route.RouteId} is an agent route (agent_level {route.AgentLevel}): {tool} does not change it over MCP. " +
                "Change it in Settings > Manage Notification Routes.");

    [McpServerTool(Name = "set_notification_route_enabled"), Description(
        "Enables or disables a notification route by route_id, WITHOUT deleting it. Changes shared alert configuration the service delivers on: a disabled route is skipped at resolution as if it did not exist, so its alerts fall to the next level (family route, then parent channels). Reversible: re-enabling restores it exactly. Returns updated (route as stored), unchanged (already that value, safe to retry), or not_found. Cannot author or re-point a route; that is Settings-window only.<<GUIDE>>" +
        "Enables or disables a notification route by its route_id (from get_notification_routes) WITHOUT deleting it. " +
        "A disabled route is skipped at resolution exactly as if it did not exist, so the alerts it matched fall to " +
        "the next level (a family route, then the parent channels) on the next firing. Returns {status:\"updated\", " +
        "route:{...}} with the route AS STORED, {status:\"unchanged\", route:{...}} when it already holds that value " +
        "(a retry is safe; both carry previous {enabled} and undo, the call that sets the previous value back), or {status:\"not_found\"}. Lite only: {status:\"unavailable\"} while an earlier database " +
        "reset's saved-settings restore is pending (it finishes at the next Lite start). Served only when " +
        "mcp_allow_writes is true. This tool cannot author or re-point a route: destinations are bearer secrets and " +
        "are set only in Settings > Manage Notification Routes.")]
    public static async Task<string> SetNotificationRouteEnabled(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The route_id to enable or disable (from get_notification_routes).")] int route_id,
        [Description("true to put the route back in force, false to skip it at resolution while keeping it.")] bool enabled)
    {
        try
        {
            return await SetRouteEnabledAsync(
                CreateRouteStore(dataService, serverManager),
                serverManager.GetAllServers(),
                await dataService.GetServerTagsAsync(),
                route_id,
                enabled);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("set_notification_route_enabled", ex);
        }
    }

    internal static async Task<string> SetRouteEnabledAsync(
        DuckDbNotificationRouteStore store,
        IReadOnlyCollection<ServerConnection> servers,
        IReadOnlyCollection<ServerTag> tags,
        int route_id,
        bool enabled)
    {
        try
        {
            var existing = await store.GetAsync(route_id);
            if (existing is null)
            {
                return McpHelpers.Status("not_found", $"No notification route with route_id {route_id}.");
            }

            if (AgentRouteRefusal(existing, "set_notification_route_enabled") is { } agentRefusal)
            {
                return agentRefusal;
            }

            if (existing.Enabled == enabled)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "unchanged",
                    route = RoutePayload(existing, ServerNames(servers), tags.ToDictionary(t => t.Id, t => t.Name)),
                    previous = new { enabled = existing.Enabled },
                    undo = UndoCall("set_notification_route_enabled", new { route_id, enabled = existing.Enabled }),
                }, McpHelpers.JsonOptions);
            }

            if (!await store.SetEnabledAsync(route_id, enabled))
            {
                return McpHelpers.Status("not_found", $"No notification route with route_id {route_id}.");
            }

            /* Re-read AFTER the write, so the reported row is the stored one. */
            var stored = await store.GetAsync(route_id);
            return JsonSerializer.Serialize(new
            {
                status = "updated",
                route = stored is null ? null : RoutePayload(stored, ServerNames(servers), tags.ToDictionary(t => t.Id, t => t.Name)),
                previous = new { enabled = existing.Enabled },
                undo = UndoCall("set_notification_route_enabled", new { route_id, enabled = existing.Enabled }),
            }, McpHelpers.JsonOptions);
        }
        catch (PendingRestoreException ex)
        {
            return McpHelpers.Status("unavailable", ex.Message);
        }
    }

    [McpServerTool(Name = "delete_notification_route"), Description(
        "Deletes a notification route by its route_id (from get_notification_routes), and its saved URLs and key from Windows Credential Manager. Permanent: the alerts it matched fall to the next level (a family route, then the parent channels) on the next firing. " +
        "Returns {status:\"deleted\", route_id, previous (the route as it was), undo: null, undo_note} (Lite has no tool that creates a route, so no call restores it), {status:\"not_found\"}, or {status:\"unavailable\"} while an earlier database reset's saved-settings restore is pending (it finishes at the next Lite start). " +
        "Prefer set_notification_route_enabled for a temporary change - a deleted route's destinations are gone and only Settings > Manage Notification Routes can author them again. Served only when mcp_allow_writes is true.")]
    public static async Task<string> DeleteNotificationRoute(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The route_id to delete (from get_notification_routes).")] int route_id)
    {
        try
        {
            return await DeleteRouteAsync(
                CreateRouteStore(dataService, serverManager), route_id, serverManager.GetAllServers(), await dataService.GetServerTagsAsync());
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_notification_route", ex);
        }
    }

    internal static async Task<string> DeleteRouteAsync(
        DuckDbNotificationRouteStore store,
        int route_id,
        IReadOnlyCollection<ServerConnection>? servers = null,
        IReadOnlyCollection<ServerTag>? tags = null)
    {
        try
        {
            var existing = await store.GetAsync(route_id);
            if (existing is not null && AgentRouteRefusal(existing, "delete_notification_route") is { } agentRefusal)
            {
                return agentRefusal;
            }

            if (!await store.DeleteAsync(route_id))
            {
                return McpHelpers.Status("not_found", $"No notification route with route_id {route_id}.");
            }

            /* #6403: the row as it was (no destination is in it; they are secrets). Lite has no tool that creates a route, so no
               single call restores it: undo is null and the note says where the route is authored again. */
            return JsonSerializer.Serialize(new
            {
                status = "deleted",
                route_id,
                previous = existing is null
                    ? null
                    : RoutePayload(
                        existing,
                        servers is null ? new Dictionary<int, string>() : ServerNames(servers),
                        tags is null ? new Dictionary<int, string>() : tags.ToDictionary(t => t.Id, t => t.Name)),
                undo = (object?)null,
                undo_note = "No tool call restores a deleted route: Lite cannot author a route over MCP because its destinations are secrets that cannot be read back. Author it again in Settings > Manage Notification Routes.",
            }, McpHelpers.JsonOptions);
        }
        catch (PendingRestoreException ex)
        {
            return McpHelpers.Status("unavailable", ex.Message);
        }
    }

    /// <summary>The one shape of an undo call (#6403): <c>{tool, arguments}</c>, the same as Darling's write tools.</summary>
    private static object UndoCall(string tool, object arguments) => new { tool, arguments };

    /// <summary>A Lite server's id is the hash of its storage name, the id the alert path and the tag map use.</summary>
    private static Dictionary<int, string> ServerNames(IReadOnlyCollection<ServerConnection> servers)
    {
        var names = new Dictionary<int, string>();
        foreach (var s in servers)
        {
            var id = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s));
            names[id] = ServerManager.NameForMessage(s);
        }

        return names;
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
                        : $"No mute rules are in force right now: {configured} {(configured == 1 ? "rule exists but it is" : "rules exist but every one of them is")} disabled or expired, so nothing is being suppressed. Pass enabled_only=false to list them — this is a lapsed mute, not an absent one.",
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
                    /* #5654: always null in Lite (no external alert intake); the key keeps one rule shape across products. */
                    external_source = (string?)null,
                    database_pattern = r.DatabasePattern,
                    query_text_pattern = r.QueryTextPattern,
                    wait_type_pattern = r.WaitTypePattern,
                    job_name_pattern = r.JobNamePattern,
                    client_app_pattern = r.ClientAppPattern,
                    match_summary = r.Summary
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
