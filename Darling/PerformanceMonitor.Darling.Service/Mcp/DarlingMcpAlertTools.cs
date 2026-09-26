/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The alerts MCP tools — get_alert_history, get_alert_settings, get_mute_rules plus the alert-TUNING
/// write tools update_alert_settings / create_mute_rule / update_mute_rule / delete_mute_rule / set_mute_rule_enabled — served over Darling's Postgres
/// store. The reads are the same names Lite (and the Dashboard) expose; the writes are Darling-only (the central
/// alert store the fleet shares, which Lite's single-instance DuckDB has no twin for). This is the FLEET
/// edition's biggest MCP win: an agent triaging N servers can now see what fired, whether delivery failed, the
/// current thresholds, and which servers are suppressed vs healthy-quiet — and now TUNE that alerting
/// conversationally. The reads are STORED reads (no live monitored-server hit): get_alert_history /
/// get_alert_settings read <c>config_alert_log</c> / <c>config_alert_settings</c> through
/// <see cref="DarlingAlertReader"/>, and get_mute_rules reads through the SAME service-side
/// <see cref="PgMuteRuleStore"/> the delivery paths honor.
///
/// <para>get_alert_history adds a fleet dimension Lite's single-store tool lacks: an optional
/// <c>server_name</c> — omit it for the whole fleet (the viewer's all-servers Alert History default, with each
/// row carrying its server), or name a server to scope to it. get_alert_settings reports the single global
/// alert-settings row the service hot-swaps in (the viewer's Settings-window desired state); SMTP/webhook
/// delivery credentials are managed separately and are not exposed here (the least-privilege mcp role cannot
/// read the secret columns anyway) — configure them in the standalone PerformanceMonitor Darling Viewer app's
/// Settings window (Notifications section), pointed at this store's connection string. The Viewer's Postgres
/// connection is not hardcoded to localhost, so it can configure a remote headless box exactly as well as a
/// local one.</para>
///
/// <para><b>Writes route through the EXISTING authority, never a parallel copy.</b> update_alert_settings is a
/// PARTIAL update of the single <c>config_alert_settings</c> row (id=1) — the caller reads via get_alert_settings,
/// changes fields, and sends only those back in the SAME nested shape; every provided field is validated against
/// the SAME ranges/enums the Viewer's Settings window enforces (SettingsWindow.BuildAlertRowFromControls) BEFORE
/// any write, and only the provided columns are written by a targeted parameterized UPDATE. The write self-bumps
/// <c>config_service.config_version</c> via the existing config-table trigger, so the running service hot-reloads
/// within one sweep (the tool never writes <c>config_version</c> itself). SMTP/webhook credential columns are out
/// of scope — the mcp role cannot read or write them. create_mute_rule / update_mute_rule / delete_mute_rule /
/// set_mute_rule_enabled reuse the SAME <see cref="PgMuteRuleStore"/> get_mute_rules reads through, generating
/// the rule id the same way the Viewer's mute-create path does (a fresh GUID on a new
/// <see cref="MuteRule"/>). set_mute_rule_enabled is the headless twin of the Viewer's mute-rule checkbox: it
/// writes <c>enabled</c> and nothing else, so a rule can leave force and return without losing the id, the
/// reason or the creation date the Stale Mute Rules self-alert (#3306) ages it from. update_mute_rule is the
/// headless twin of the Viewer's Edit dialog on the same terms: a PARTIAL edit merged onto the stored rule and
/// written through the store's own <see cref="IMuteRuleStore.UpdateAsync"/>, whose SET list does not name
/// <c>created_at_utc</c> — so no edit, however sweeping, can reset the clock #3306 ages the rule from, which
/// delete-and-re-create does on every field the enable flag cannot express.</para>
///
/// <para><b>Security.</b> These write tools connect (like every MCP tool) as the least-privilege <c>mcp</c> role,
/// granted (see <see cref="DarlingManagedRoles"/>) INSERT/UPDATE/DELETE on <c>config.config_mute_rules</c>, UPDATE
/// on the singleton <c>config.config_alert_settings</c>, and the beacon columns of <c>config.config_service</c>
/// (so the settings write's self-bump trigger can fire) — and nothing else, so a token-holder can tune alerting
/// but still cannot reach the <c>config_command</c> service-credential pivot or the carved secret columns.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpAlertTools
{
    [McpServerTool(Name = "get_alert_history"), Description("Gets alert history, NEWEST FIRST: each row FIRED; notification_type says whether it was DELIVERED, a separate question from DISMISSED (UI-acknowledged), excluded by default. THE PAGE IS BOUNDED BY limit, NOT hours_back: truncated says the window held more, and oldest/newest_returned_alert_time bound how far the page reached. An EMPTY page can mean no alerts fired, or that every alert here was dismissed: dismissed_excluded_count says which; include_dismissed = true returns them, labelled dismissed = true. A null send_error proves nothing about delivery.<<GUIDE>>Gets recent alert history from the alert log, NEWEST FIRST: what alerts fired, when, for which server, the current vs threshold value, whether email/webhook delivery succeeded, and whether the alert was muted. Omit server_name to see the whole fleet (each row names its server); pass one to scope to a single server. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: alerts_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_alert_time / newest_returned_alert_time bound the page — under newest-first ordering the oldest stamp IS how far back this read reached, so on a noisy fleet a 24-hour request at the default limit may cover minutes. Raise limit or narrow hours_back when truncated is true; widening hours_back cannot help. BY DEFAULT THIS READ EXCLUDES DISMISSED ALERTS — rows an operator acknowledged in the Viewer's Alert History grid. Dismissal says nothing about whether the alert fired or mattered, so an incident reconstruction that ignores it can miss the very critical someone already looked at: dismissed_excluded says whether the filter applied and dismissed_excluded_count is how many rows in the window it removed, and include_dismissed = true returns them, each labelled dismissed = true. notification_type is the delivery disposition and is the ONLY field that says why a row did not deliver: 'email'/'webhook'/'email+webhook' delivered on that channel; 'throttled' means the delivery cooldown was still inside this alert's window so nothing was attempted (the throttle working, not a fault); 'folded' means a repeat was rolled onto another server's post for the same metric and is named there under 'Other Servers Affected', so it WAS reported; 'failed' means a channel was attempted and came back unsuccessful, with send_error carrying the first failing channel's text; 'unconfigured' means no email or webhook channel is set up; 'muted' means a mute rule suppressed it; 'digest' means the analysis finding was UNCORROBORATED (one fact in its chain, no matched co-fire check) and the corroboration gate routed it to the daily Analysis Singles Digest and the web/MCP surfaces instead of a paging channel — nothing was attempted, nothing was suppressed, and routing_reason on the row says which components decided; 'none' is a resolution row, which no channel applies to. Do NOT split the not-delivered rows on send_error: it is null on 'throttled', 'folded' and 'digest' rows and on every row written before those values existed, so a null error is not evidence of a working cooldown. 'undelivered' is a retained legacy value that means throttled OR folded OR failed with nothing in the row to say which — count those rows separately rather than attributing them. severity is the row's tier — 'critical', 'warning', 'info' or 'resolution' — and severity_source says where it came from: 'fired' when the row persisted the tier the alert actually fired at (graded alerts such as Poison Wait, Volume Free Space and Database State fire Warning OR Critical by measurement), 'metric_name' when the row carries no tier and the metric's name is the only evidence (rows written before the tier was persisted, alerts whose severity is fixed per metric, and every resolution row). Do not infer a graded alert's tier from its name: a 'Poison Wait' row with severity 'warning' fired as a warning. route says WHERE the posts went: the alert's family (self-monitor / reports / agent-jobs / performance), the notification route that matched it (route_id null = the parent channels in Settings > Notifications answered everything) and each delivered channel with the route that supplied its destination — see get_notification_routes for the taxonomy and the routes. route is null on rows written before routing existed and on rows that never reached a delivery decision (throttled, folded, muted, unconfigured, digest, resolutions), so a null route is not evidence of a routing fault. routing is a DIFFERENT question from route: for an 'Analysis: …' finding it is the corroboration gate's decision one step UPSTREAM of the fan-out — 'page' when the finding earned a channel (two or more facts in its chain, or a matched co-fire check, or one of the two by-construction stories) and 'digest' when it was a lone uncorroborated fact — with routing_reason naming the components the gate read, so 'why didn't this page' is answered by the row. Both are null on every engine alert and on analysis rows written before the gate existed. To see what the digest carried, read the rows with notification_type 'digest' in the window: each is a persisted finding with its full detail_text and context, findable in get_analysis_findings by the story hash in its metric_name. Dismissal is an acknowledgement, not a verdict — a dismissed critical still fired — so set this when reconstructing an incident rather than triaging what is still open. Each row then carries dismissed so the two populations stay distinguishable.")]
    public static async Task<string> GetAlertHistory(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Omit to return alerts across all servers (the fleet default).")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows to return, newest first. Default 50. This is what bounds the page — read truncated to know whether the window held more.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        /* Appended after as_of for the reason get_collection_log's filters are: MCP invokes by name, the
           /api/read dispatch passes as_of by name, and a trailing optional is the one position no existing
           positional C# caller can be re-bound by. */
        [Description("Include alerts an operator has dismissed in the Viewer. Default false, which is the Alert History grid's own read.")] bool include_dismissed = false,
        CancellationToken cancellationToken = default)
    {
        var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (hoursError != null) return hoursError;
        var limitError = McpHelpers.ValidateTop(limit);
        if (limitError != null) return limitError;

        /* Optional server scope: a named server resolves + scopes; omitted = the whole fleet (the viewer's
           all-servers Alert History default). Unlike the other tools this does NOT force a single server. */
        int? serverId = null;
        var scope = "(all servers)";
        if (!string.IsNullOrWhiteSpace(server_name))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
            if (error != null) return error;
            serverId = resolved.ServerId;
            scope = resolved.ServerName;
        }

        try
        {
            var since = windowEnd.AddHours(-hours_back);

            /* #3541 A3: over-fetch by one so truncation is OBSERVED rather than inferred from count == limit,
               the pattern get_collection_log and get_query_heatmap use. The cap was already the caller's
               here; what was missing was any way to tell a window of exactly `limit` alerts from a busier
               one, and any statement that the dismissed rows had been removed. */
            var rows = await DarlingAlertReader.GetAlertHistoryPageAsync(postgres, since, windowEnd, serverId, limit + 1, include_dismissed, cancellationToken);
            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            /* The hidden filter, measured: how many rows in this window and scope it removed. Zero is a real
               answer (nothing was hidden) and is what a caller who never sends include_dismissed most needs
               to see beside a clean-looking page. Not probed when the filter is off, because then it removed
               nothing by construction. */
            var dismissedExcludedCount = include_dismissed
                ? 0L
                : await DarlingAlertReader.CountDismissedAlertsAsync(postgres, since, windowEnd, serverId, cancellationToken);

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
                       the metric NAME implies ("metric_name") — the same two arms both Alert History grids
                       colour rows by, so a caller reading "Poison Wait" here sees the Warning it fired at
                       rather than the red the name used to earn every row. The source is published because
                       the two are not equal evidence; see AlertHistoryRowSeverity.Describe. */
                    severity,
                    severity_source = severitySource,
                    /* #3598 (design point 3): WHERE this firing's posts went — the alert's family, the route
                       that matched it (null = the parent channels answered everything) and each DELIVERED
                       channel with the route that supplied its destination. Null on rows written before
                       routes existed and on rows that never reached resolution (throttled, folded, muted,
                       unconfigured, resolutions): a row that consulted no destination records none. */
                    route = RouteHistoryPayload(AlertContextSerializer.TryReadRoute(r.ContextJson)),
                    /* #3712: the corroboration gate's decision for an analysis finding — 'page' or 'digest' —
                       and the one sentence naming the components it read, read off the row the way
                       severity_source is. Null on every engine alert (the gate does not apply) and on every
                       analysis row written before the gate existed; a null here is not evidence of anything. */
                    routing = routing?.Route,
                    routing_reason = routing?.Reason,
                    detail_text = r.DetailText,
                };
            });

            return JsonSerializer.Serialize(new
            {
                server = scope,
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_alert_history", ex);
        }
    }

    [McpServerTool(Name = "get_alert_settings"), Description("Gets the alert configuration currently in effect: enabled flags, thresholds, cooldowns, delivery mode, and cadences. cooldown_minutes gates whether an alert FIRES; delivery.cooldown_minutes separately bounds the resulting post, per alert fingerprint and, for a re-notification, per metric across every monitored server. Darling: an unseeded store answers status unavailable and darling.json's defaults still govern until it seeds. Lite always answers its live in-memory settings. An empty knob list (e.g. long_running_query's exclusions) means cleared, not a default in force.<<GUIDE>>Gets the current alert configuration the service is using: which alerts are enabled and their thresholds (CPU, blocking, deadlocks, poison waits, long-running queries/jobs, tempdb, low disk, failed jobs, database state, Availability Group health, connection loss), the cooldown, excluded databases, the deadlock/blocking delivery mode and cooldown, the scheduled-analysis cadence, and the fleet-sweep cadence. TWO different cooldowns are reported and they govern different stages: top-level cooldown_minutes gates whether the alert engine FIRES at all, while delivery.cooldown_minutes bounds the resulting Slack/Teams/PagerDuty/webhook/email post twice over: once per alert FINGERPRINT, and once per METRIC across the whole fleet for a RE-notification. The second bound is why one fault on forty servers does not cost forty posts an hour; the servers it holds back are named on the post that does go out, under an 'Other Servers Affected' section. A first notice is never held back by either bound, and PerEvent delivery mode opts out of the per-metric one. A channel going quiet with alerts still in get_alert_history is delivery.cooldown_minutes, not cooldown_minutes. The self_alerts group holds the thresholds for alerts about the MONITOR STORE itself rather than a monitored server — those arrive with Server: 'Monitor Store' by default, or with the store's own peers.storeName label when the operator set that file-only field (a multi-store estate names each store on its own self-alerts), so an alert naming either spelling is tuned here and nowhere else, including Retention Held's warn/critical ratios. Mute rules match the alert row's server spelling, so on a store with storeName set, scope self-alert mutes to that label, not to 'Monitor Store'. The health_bands group is NOT an alert: its two tiers decide what band a server's card, the worst-first ranking and get_fleet_overview's counts read, in deadlocks per HOUR normalised over whatever window was asked for — so the same pair means the same condition on a 1-hour read and a 24-hour one. Tuning deadlocks.count_threshold does not move the band and tuning health_bands does not move the alert. The file_growth group's rise_mb is megabytes per HOUR, averaged over file_growth.lookback_minutes — a rate, not a total for the window: 10240 means 10 GB/hr whether the lookback is 5 minutes or 24 hours, and the engine scales it to the window (a 5-minute lookback asks for 853 MB inside it, a 24-hour one for 240 GB). The fleet_sweep group is NOT an alert family either, and its cadence is a SECOND cadence, separate from the scheduled-analysis one: fleet_sweep.enabled turns the scheduled whole-fleet sweep report on or off, and fleet_sweep.interval_minutes (15–1440, default 60 — hourly) is how often it runs. The alerts_enabled master switch deliberately does not govern sweep production, only delivery: sweeps keep running under alerts_enabled: false — that is when they carry the would-have-paged ledger — so muting the fleet does not blind the report surface. Separately, deadlocks.pg_count_threshold and blocking.pg_count_threshold are the PostgreSQL versions of those two alerts' count gates, reported inside those same groups, and they are deliberately NOT the same numbers as the count_threshold beside them: the two engines count with different instruments (captured deadlock graphs versus deadlocks parsed from the server log; engine-recorded blocked-process reports versus a periodic SAMPLE of pg_stat_activity), and while both engines' cards now band deadlocks through the same health_bands tiers, a PostgreSQL server still has no blocking band to calibrate against. The enabled switch in each group governs BOTH engines; the two thresholds do not move each other. On a store with no PostgreSQL targets both PostgreSQL keys are inert. poison_wait.threshold_ms is RETIRED: since the Poison Wait alert grades ACCUMULATED wait over a ten-minute window, this value is stored and reported for compatibility but consulted by nothing; poison_wait.threshold_ms_note says so beside it on every read, and update_alert_settings accepts it with a warning rather than refusing a round-tripped payload. poison_wait.enabled is the live switch. analysis.uncorroborated_route is where a notify-worthy but UNCORROBORATED finding goes — one fact in its chain and no matched co-fire check: 'digest' (the default) keeps it off every paging channel and puts it in the daily Analysis Singles Digest and the web/MCP surfaces, 'page' restores the earlier paging of every notify-worthy finding; a corroborated finding pages under either. It is the EFFECTIVE route, never null, resolved from the knob's two homes with the STORE winning: the settings row's analysis_uncorroborated_route column (V137) when it holds a route, else darling.json's analysis.uncorroboratedRoute (read once at service start), else the shipped 'digest'. analysis.uncorroborated_route_source says which home decided — 'store', 'file' or 'default' — and analysis.uncorroborated_route_note restates the precedence beside it; a store column left NULL (every store the morning after the V137 upgrade, and every fresh seed) reads 'file'. Set it with update_alert_settings or the Viewer's Settings window (Notifications > Automated Analysis): live in the running service within one collection sweep, consulted on the next scheduled-analysis delivery, no restart; send null through update_alert_settings to clear the column and hand the decision back to the file. long_running_query.excluded_program_name_prefixes and long_running_query.excluded_logins are the Long-Running Query alert's OPT-OUT knob: a session whose program_name STARTS WITH an entry of the first list, or whose login_name IS an entry of the second (both case-insensitive, no wildcard grammar), is NOT EVALUATED by that alert at all — not read into the decision, not counted, not fingerprinted — which is the opposite of a mute rule (a mute silences a fire already decided; an exclusion means the session never reaches the decision, and excluding a paging program resolves the open incident). The exclusion is applied in the read ahead of long_running_query.max_results, so an excluded session never consumes a result slot, and the fired alert's card carries Excluded Count / Excluded By Program Prefix / Excluded By Login items saying how many SESSIONS each list removed on that evaluation (a session matching both counts once, under the prefix). The lists ship SEEDED from a 7-day read of one large production store, whose long-running population fell into four classes: (1) SQL Agent job steps — program_name prefix 'SQLAgent - TSQL JobStep', ~460 sessions a week across 11 jobs, medians 35–62 min — the default prefix; (2) the NT AUTHORITY\\SYSTEM and NT AUTHORITY\\NETWORK SERVICE logins — the permanent multi-day CDC-shaped background — the default logins; (3) the application's admin login — deliberately NOT a default, because it runs the job wave but also real ad-hoc long-runners, and the job-step prefix already covers its share; (4) named humans — never excluded, they are what the page is for. The seeds are DEFAULTS: an empty list here means the operator cleared it and that arm excludes nothing. SMTP/webhook delivery credentials are managed separately and are not reported here — configure them in the standalone Darling Viewer app's Settings window (Notifications section), which connects to this store (including remotely, not just localhost) rather than requiring desktop access to this specific box.")]    public static async Task<string> GetAlertSettings(        NpgsqlDataSource postgres,
        CancellationToken cancellationToken = default)
    {
        try
        {
            /* ONE snapshot across both config tables. The delivery cooldown is the single value on
               config_notification rather than config_alert_settings, so reporting the configuration takes two
               SELECTs -- and two independent reads could straddle a concurrent update_alert_settings commit
               and report a mix of pre- and post-update state, which is the very thing the write path takes a
               transaction to avoid producing. See DarlingAlertReader.GetAlertConfigurationAsync. */
            var (s, deliveryCooldown) = await DarlingAlertReader.GetAlertConfigurationAsync(postgres, cancellationToken);
            if (s is null)
                return McpHelpers.Status(
                    "unavailable",
                    "No alert-settings row is present in the store yet. The service seeds it on startup (or the Viewer's Settings window writes it); until then the service runs on its darling.json defaults.");

            /* Absent means the notification row is unseeded, which the service seeds in the SAME pass as the
               settings row -- so it is the same unseeded control plane the arm above reports, and reporting
               the shipped 15 instead would state a number nobody wrote. */
            if (deliveryCooldown is null)
                return McpHelpers.Status(
                    "unavailable",
                    "No notification row is present in the store yet, so the delivery cooldown cannot be reported. The service seeds it alongside the alert-settings row on startup (or the Viewer's Settings window writes it); until then the service runs on its darling.json defaults.");

            return JsonSerializer.Serialize(BuildAlertSettingsPayload(s, deliveryCooldown.Value), McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_alert_settings", ex);
        }
    }

    /// <summary>The <c>analysis.uncorroborated_route_note</c> text (#3712): the knob has TWO homes since V137, so
    /// the reader is told the precedence, how to move the decision between them, and where the EFFECTIVE
    /// decision is visible per finding. Restates <see cref="DarlingAlertSettings.ResolveUncorroboratedRoute"/>
    /// in prose; the machine-readable half is <c>uncorroborated_route_source</c> beside it.</summary>
    internal const string UncorroboratedRouteNote =
        "#3712: the EFFECTIVE route, resolved store-over-file. config_alert_settings.analysis_uncorroborated_route (V137; "
        + "set it with update_alert_settings or the Viewer's Settings window (Notifications > Automated Analysis), live in the running service within one "
        + "collection sweep, no restart) wins when it holds 'digest' or 'page'; NULL there defers to darling.json's "
        + "analysis.uncorroboratedRoute, read once at service start; a file value that is neither spelling defers to the "
        + "shipped 'digest'. uncorroborated_route_source names the home that decided: 'store', 'file' or 'default'. Send "
        + "uncorroborated_route: null through update_alert_settings to clear the store column and let the file govern again. "
        + "A corroborated finding pages under either value. The decision the running service made for each finding is on "
        + "its get_alert_history row as routing / routing_reason.";

    /// <summary>
    /// The wire-side deprecation note for <c>poison_wait.threshold_ms</c> (#3653): emitted beside the value by
    /// <see cref="BuildAlertSettingsPayload"/>, returned under <c>warnings</c> by <c>update_alert_settings</c>
    /// when a caller sets the value, and pinned word-for-word on both by <c>DarlingMcpAlertToolsTests</c>. One
    /// constant so the two surfaces cannot say different things about the same retired knob. Lite's
    /// <c>McpAlertTools</c> carries the same text by hand (Lite.Tests cannot reference this assembly) and its
    /// test reads this declaration to hold the two equal.
    /// </summary>
    internal const string PoisonWaitThresholdMsNote =
        "retired by #3593 — the alert grades accumulated wait over a ten-minute window; this value is stored and reported but not consulted";

    /// <summary>The #3712 route knob resolved for the read surface: the row's store half (raw, off
    /// <see cref="DarlingAlertReader.AlertSettingsReadRow.AnalysisUncorroboratedRoute"/>) beside the file half
    /// this process published (<see cref="DarlingFileLevelAlertSettings.UncorroboratedRoute"/>), through the ONE
    /// resolver the engine seam uses, so <c>get_alert_settings</c> and <c>DarlingAlertSettings</c> cannot read one
    /// row two ways. The publish is the file value's only route into a static tool over the store; where nothing
    /// has published (a harness) the file half is null and the resolution says <c>default</c>, which is what
    /// such a process would apply.</summary>
    private static UncorroboratedRouteResolution UncorroboratedRoute(DarlingAlertReader.AlertSettingsReadRow s) =>
        DarlingAlertSettings.ResolveUncorroboratedRoute(s.AnalysisUncorroboratedRoute, DarlingFileLevelAlertSettings.UncorroboratedRoute);

    /// <summary>The nested JSON shape get_alert_settings returns AND update_alert_settings echoes back — the same
    /// field names update_alert_settings accepts on the way in, so a read → modify → write round-trips.
    ///
    /// <para>That last clause is an INVARIANT, not a habit: #2417 found it broken in both directions at once
    /// (six columns read and emitted to nobody, one key emitted the writer refused). It is now asserted as a
    /// set equality between this payload's keys and the columns <c>AlertSettingsSelectSql</c> reads —
    /// <c>EveryColumnRead_IsEmittedByThePayload_AndAcceptedByTheWriter</c>. Adding a key here without the
    /// matching arm in <see cref="BuildAlertSettingsUpdate"/> (or the reverse) fails that test rather than
    /// shipping.</para>
    ///
    /// <para>That set equality is held PER TABLE (#3314). Every key here but one maps to a
    /// <c>config_alert_settings</c> column; <c>delivery.cooldown_minutes</c> maps to
    /// <c>config_notification.email_cooldown_minutes</c>, so it is passed in separately rather than read off
    /// <paramref name="s"/> — a single set equality across both planes would compare a union against one
    /// table's SELECT list and be satisfiable by drift on either side.</para></summary>
    private static object BuildAlertSettingsPayload(
        DarlingAlertReader.AlertSettingsReadRow s, int deliveryCooldownMinutes) => new
    {
        alerts_enabled = s.Enabled,
        notify_connection_changes = s.NotifyConnectionChanges,
        /* #2417: the connection family's two sub-settings, kept TOP-LEVEL beside their master switch
           rather than folded into a `connection` group. The master shipped as a top-level key and Lite
           emits it there too, so a group could only ever hold two of the three -- splitting one family
           across two levels of the document is worse for a reader than two extra top-level keys. The
           spellings are the store's own column names, which are also the keys Lite already reads out of
           settings.json (App.LoadAlertSettings), so Lite's payload can adopt them verbatim. */
        notify_connection_down_at_startup = s.NotifyConnectionDownAtStartup,
        connection_refire_minutes = s.ConnectionRefireMinutes,
        cpu = new { enabled = s.CpuEnabled, threshold_percent = s.CpuThresholdPercent, mode = s.CpuMode },
        blocking = new
        {
            enabled = s.BlockingEnabled,
            count_threshold = s.BlockingCountThreshold,
            /* #1839: the second gate — total blocked wait in the latest snapshot (0 = off). */
            wait_threshold_seconds = s.BlockingWaitSecondsThreshold,
            /* #3444 (V122): the PostgreSQL threshold, reported INSIDE this group rather than under a
               postgres_alerts section of its own. The two engines' figures belong side by side because
               that is the only placement where an operator reading "blocking" sees that there are two of
               them — the same argument FleetDeadlockRateThresholdSql makes about not putting a knob in a
               second table somebody has to know to look in. `enabled` above governs BOTH engines. */
            pg_count_threshold = s.PgBlockingCountThreshold
        },
        deadlocks = new
        {
            enabled = s.DeadlockEnabled,
            count_threshold = s.DeadlockCountThreshold,
            /* #3444 (V122): the PostgreSQL threshold — see the blocking group above for why it sits here
               rather than in a section of its own, and the V122 rung for why it is not this group's
               count_threshold. `enabled` governs both engines. */
            pg_count_threshold = s.PgDeadlockCountThreshold
        },
        /* #3368 (V120): the per-server HEALTH BAND tiers, reported as their own group rather than folded
           into the alert group above — the distinction is the point. `deadlocks` governs whether an alert is
           DELIVERED; these two decide what colour a server's card, the worst-first "needs attention"
           ranking and get_fleet_overview's band counts read. Nesting a band tier under the alert would
           invite tuning one and expecting the other to move. */
        health_bands = new
        {
            deadlock_warn_per_hour = s.DeadlockWarnPerHour,
            deadlock_critical_per_hour = s.DeadlockCriticalPerHour
        },
        poison_wait = new
        {
            enabled = s.PoisonWaitEnabled,
            /* #3653 (from #3541): the key STAYS — it is a published field a client may read or hand back, and
               removing it would be a contract break for a value that costs nothing to carry — but since #3593
               nothing consults it. The alert grades ACCUMULATED wait over PoisonWaitEvaluator.WindowMinutes
               (IAlertEngineSettings.PoisonWaitThresholdMs records why the member survives on the contract);
               this was the avg-ms-per-wait bar the retired shape judged one collector row against. The note
               sits BESIDE the value, on the wire, because the description is the only other place an agent
               could learn it and a description is read once. The writer accepts the note back untouched (a
               read → modify → write round-trip is this payload's invariant) and warns when the value itself
               is set. */
            threshold_ms = s.PoisonWaitThresholdMs,
            threshold_ms_note = PoisonWaitThresholdMsNote
        },
        long_running_query = new
        {
            enabled = s.LongRunningQueryEnabled,
            threshold_minutes = s.LongRunningQueryThresholdMinutes,
            max_results = s.LongRunningQueryMaxResults,
            exclude_sp_server_diagnostics = s.LongRunningQueryExcludeSpServerDiagnostics,
            exclude_wait_for = s.LongRunningQueryExcludeWaitFor,
            exclude_backups = s.LongRunningQueryExcludeBackups,
            exclude_misc_waits = s.LongRunningQueryExcludeMiscWaits,
            exclude_cdc = s.LongRunningQueryExcludeCdc,
            /* #3653 (A5, Q5): the opt-out knob -- sessions whose program_name starts with a prefix / whose login_name
               equals an entry are NOT EVALUATED by the alert (applied in the read, ahead of its row cap), which is
               the opposite of a mute rule. Case-insensitive, no wildcard grammar. Reported as the arrays the store
               holds (seeded by the V135 rung's DEFAULT) so a round-trip through update_alert_settings is
               byte-identical. */
            excluded_program_name_prefixes = s.LongRunningQueryExcludedProgramNamePrefixes,
            excluded_logins = s.LongRunningQueryExcludedLogins
        },
        tempdb_space = new { enabled = s.TempDbSpaceEnabled, threshold_percent = s.TempDbSpaceThresholdPercent },
        low_disk = new
        {
            enabled = s.LowDiskEnabled,
            threshold_percent = s.LowDiskThresholdPercent,
            threshold_gb = s.LowDiskThresholdGb,
            /* #2107: the CRITICAL severity tier's floors (#1136) — previously compile-time. */
            critical_free_percent = s.DiskCriticalFreePercent,
            critical_free_gb = s.DiskCriticalFreeGb
        },
        /* #2107: the monitor's own self-alerts (store volume, collection health) — previously
           compile-time constants. */
        self_alerts = new
        {
            disk_free_warn_percent = s.SelfDiskFreeWarnPercent,
            /* #3528 (V126): the percent's GB floor — pressure requires BOTH the percent above breached
               AND free space below this many GB (0 removes the floor), so a large store volume at a low
               percent stops paging CRITICAL. The pvs.floor_gb composition, not low_disk's OR pair. */
            disk_free_warn_gb = s.SelfDiskFreeWarnGb,
            collection_stale_minutes = s.CollectionStaleMinutes,
            collection_failure_threshold = s.CollectionFailureThreshold,
            /* #2136: the Store Job Over Cadence warning percent (Critical is fixed at 100). */
            store_job_cadence_warn_percent = s.StoreJobCadenceWarnPercent,
            /* #3297 (V119): the Retention Held tiers — how many times its own configured horizon a retention
               policy held by the rollup-coverage gate must be holding before it warns, and before it goes
               critical. Reported HERE beside the other store self-alerts because "Monitor Store" is the
               subject: #3296's operator received an hourly CRITICAL named Retention Held against Monitor
               Store and could find nothing in Settings or on this surface that matched either word, because
               these two numbers were compile-time constants. UNLIKE its cadence neighbour the critical tier
               is a knob rather than a fixed 100 — a hold has no structural ceiling the way a job outrunning
               its own cadence does. */
            retention_hold_warn_ratio = s.RetentionHoldWarnRatio,
            retention_hold_critical_ratio = s.RetentionHoldCriticalRatio
        },
        pvs = new { enabled = s.PvsEnabled, threshold_percent = s.PvsThresholdPercent, floor_gb = s.PvsFloorGb },
        /* #2391: #2349's knobs reached 3.5.0 with the store plane only, so an alert that ships OFF could
           be enabled only by UPDATEing config_alert_settings by hand. Reported by @gotqn. */
        file_growth = new
        {
            enabled = s.FileGrowthEnabled,
            /* #3539 A8c: MB per HOUR, averaged over lookback_minutes — a rate, not the in-window delta the key's
               spelling suggests. The key keeps its name (a rename breaks every client that reads or writes it,
               and Lite's McpAlertSettingsKeyTests derive its shape from this source); the unit is stated in
               both tool descriptions, which is where an agent reads it. */
            rise_mb = s.FileGrowthRiseMb,
            volume_percent = s.FileGrowthVolumePercent,
            lookback_minutes = s.FileGrowthLookbackMinutes
        },
        long_running_job = new { enabled = s.LongRunningJobEnabled, multiplier = s.LongRunningJobMultiplier },
        failed_job = new { enabled = s.FailedJobEnabled, lookback_minutes = s.FailedJobLookbackMinutes },
        database_state = new { enabled = s.DatabaseStateEnabled },
        /* #2417: the AG family, read out of the store since V35/V37 and emitted to nobody until now -- so
           an agent asked why nothing alerted when a replica fell behind could not see the lag threshold,
           could not see whether AG notification was on at all, and had no way to tell "configured not to
           alert" from "failed to alert". Grouped like every other alert family, and the master switch is
           named `enabled` for the same reason cpu/blocking/pvs/database_state are: inside this payload
           `enabled` always means "this alert family is on", and notify_ag_health IS that switch rather
           than a second opt-in behind one. The thresholds take the house <what>_threshold_<unit> spelling
           (blocking.wait_threshold_seconds, poison_wait.threshold_ms, low_disk.threshold_gb) instead of
           transliterating the columns' older _alert_<unit> suffix, which appears nowhere else on the wire. */
        ag = new
        {
            enabled = s.NotifyAgHealth,
            lag_threshold_seconds = s.AgLagAlertSeconds,
            redo_queue_threshold_kb = s.AgRedoQueueAlertKb,
            disconnect_refire_minutes = s.AgDisconnectRefireMinutes
        },
        cooldown_minutes = s.CooldownMinutes,
        excluded_databases = s.ExcludedDatabases,
        delivery = new
        {
            mode = s.DeliveryMode,
            per_event_max = s.PerEventMax,
            /* #3314: the per-fingerprint DELIVERY cooldown -- stored as
               config_notification.email_cooldown_minutes and, until now, the one alert-engine number no MCP
               tool reported and none could write, so a headless Slack-only deployment could reach the sole
               throttle on its channel volume only through the WPF Settings window. Reported HERE, beside
               mode and per_event_max, because volume is what it governs and that is where someone tuning
               volume looks -- not under a channel name for a channel they may not have configured.
               DISTINCT from the top-level cooldown_minutes, which gates AlertEngine's FIRE decision: two
               stages, two numbers, and only one of them used to be visible. */
            cooldown_minutes = deliveryCooldownMinutes
        },
        analysis = new
        {
            enabled = s.AnalysisEnabled,
            interval_minutes = s.AnalysisIntervalMinutes,
            notifications_enabled = s.AnalysisNotificationsEnabled,
            notify_severity = s.AnalysisNotifySeverity,
            /* #2107: was a hardcoded 360 in Darling while Lite passed a configured value through. */
            notify_cooldown_minutes = s.AnalysisNotifyCooldownMinutes,
            /* #3712: where an UNCORROBORATED finding goes — 'digest' (shipped) or 'page' — as the EFFECTIVE route
               the service applies, resolved by the SAME function the engine seam uses
               (DarlingAlertSettings.ResolveUncorroboratedRoute) over the knob's two homes: the row's
               analysis_uncorroborated_route column (V137, read raw off the row above) wins when it holds a route;
               NULL defers to darling.json's analysis.uncorroboratedRoute, read off the ambient publish both
               config-loading hosts install (DarlingFileLevelAlertSettings — the file is not in the store, and
               the alert tools are static methods over the store); neither defers to the shipped digest. Never
               null: a resolution always ends somewhere, and _source says where — 'store', 'file', or 'default'.
               An unpublished harness reads 'default' + 'digest', which is a true statement about what that
               process would apply, where the pre-V137 null claimed only that nothing had been published. */
            uncorroborated_route = UncorroboratedRoute(s).RouteText,
            uncorroborated_route_source = UncorroboratedRoute(s).Source,
            uncorroborated_route_note = UncorroboratedRouteNote
        },
        /* #3466 (V124): the fleet sweep's own switch and cadence — the scheduled whole-fleet report,
           NOT an alert family. The alert master switch deliberately does not govern it (sweeps under
           alerts_enabled: false carry the would-have-paged ledger, which is the muted-mode contract's
           whole point), so it gets its own group rather than a member of one the master switch covers.
           Darling-only: Lite has no fleet to sweep, and McpAlertSettingsKeyTests records the omission. */
        fleet_sweep = new
        {
            enabled = s.FleetSweepEnabled,
            interval_minutes = s.FleetSweepIntervalMinutes
        }
    };

    [McpServerTool(Name = "get_notification_routes"), Description(
        "Gets the alert family taxonomy, and where each family's posts go. Zero routes is the ordinary state: every alert goes to every parent channel configured in Settings. Darling: sparse routes layered over that parent set; an EMPTY channel on a route INHERITS the parent's rather than silencing it; resolution is exact metric, then family, then parent. Lite: routes_supported is false, so no routes exist to edit. Routing decides WHERE a post lands, never WHETHER it is sent.<<GUIDE>>Gets WHERE each alert family is delivered: the closed family taxonomy — which metric names belong to " +
        "self-monitor, reports, agent-jobs and performance — and the sparse notification routes layered over the " +
        "parent channel set. Every store starts with ZERO routes, which means every alert goes to every channel " +
        "configured in the Viewer's Settings > Notifications (the parent). A route names a family (or one exact metric " +
        "name) and a destination per channel type; an EMPTY channel on a route INHERITS the parent's, so a route " +
        "can redirect Slack for one family and leave PagerDuty as the parent has it, or ENABLE a channel the parent " +
        "does not have (a paging key on the performance route alone is how 'only pages page' is spelled). A route " +
        "cannot silence a channel the parent has: empty means inherit, not off. Resolution order per channel: an " +
        "enabled route matching the metric EXACTLY, then one matching its FAMILY, then the parent; within a level the " +
        "lowest route_id with a non-empty column wins. A recovery routes as its firing (Server Restored as Server " +
        "Unreachable), so a channel that saw the fire sees the clear. Routing sits AFTER the cooldown: it changes " +
        "where a post lands, never whether it is sent. Destination values (webhook URLs, the PagerDuty routing key) " +
        "are bearer secrets and are NOT reported — configured_channels lists which channels each route sets by " +
        "name; smtp_recipients is reported because an address list is not a secret. get_alert_history rows carry a " +
        "route field naming the family, the matched route_id and the delivered channels, so a misrouted post can " +
        "be traced. Routes are authored in the Viewer's Settings window; here they can be disabled or deleted " +
        "(set_notification_route_enabled / delete_notification_route).")]
    public static async Task<string> GetNotificationRoutes(
        NpgsqlDataSource postgres,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var routes = await new PgNotificationRouteStore(postgres).LoadSummariesAsync(cancellationToken);

            return JsonSerializer.Serialize(new
            {
                /* The taxonomy first: the vocabulary a route's metric_match is written in, with every metric name
                   each family owns, so a caller can tell which family an alert it saw in get_alert_history belongs
                   to without a second tool. Recoveries are listed with the firing they route as. */
                families = AlertFamily.All.Select(family => new
                {
                    family,
                    description = FamilyDescription(family),
                    metrics = AlertFamily.MetricFamilies
                        .Where(kv => kv.Value == family && !AlertFamily.RecoveryPairs.ContainsKey(kv.Key))
                        .Select(kv => kv.Key)
                        .OrderBy(m => m, StringComparer.Ordinal),
                    metric_prefixes = AlertFamily.PrefixFamilies
                        .Where(p => p.Family == family)
                        .Select(p => p.Prefix + "*"),
                }),
                recoveries_route_as_their_firing = AlertFamily.RecoveryPairs.Select(kv => new { recovery = kv.Key, firing = kv.Value }),
                unclassified_metrics_route_as = AlertFamily.Performance,
                resolution_order = new[] { "exact metric route", "family route", "parent channel (Settings > Notifications)" },
                route_count = routes.Count,
                enabled_route_count = routes.Count(r => r.Enabled),
                /* Zero routes is the ordinary state and the caller must be able to tell it from a failed read: an
                   empty array here with route_count 0 IS the answer "every alert goes to every parent channel". */
                routes = routes.Select(r => new
                {
                    route_id = r.RouteId,
                    metric_match = r.MetricMatch,
                    match_kind = r.Family is null ? "exact_metric" : "family",
                    family = r.Family ?? AlertFamily.Of(r.MetricMatch),
                    configured_channels = r.ConfiguredChannels,
                    smtp_recipients = r.EmailRecipients,
                    enabled = r.Enabled,
                    modified_at_utc = r.ModifiedAtUtc,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_notification_routes", ex);
        }
    }

    /// <summary>The <c>route</c> member of a get_alert_history row (#3598): the persisted provenance, re-spelled
    /// in the tool's snake_case, or null when the row carries none.</summary>
    internal static object? RouteHistoryPayload(AlertRouteDto? route) => route is null ? null : new
    {
        family = route.Family,
        route_id = route.RouteId,
        destinations = route.Destinations.Select(d => new { channel = d.Channel, route_id = d.RouteId, source = d.Source }),
    };

    /// <summary>The one-line audience each family names, for the taxonomy the read tool publishes.</summary>
    internal static string FamilyDescription(string family) => family switch
    {
        AlertFamily.SelfMonitor => "Alerts about the monitoring tool itself: its store, collectors, jobs and certificates.",
        AlertFamily.Reports => "Scheduled prose (the collector-cost digest, the fleet sweep rollup) — to read, never to be paged by.",
        AlertFamily.AgentJobs => "SQL Server Agent: failed and anomalously long jobs, and the Agent service being down.",
        AlertFamily.Performance => "A monitored server's health: blocking, deadlocks, CPU, long-running queries, space, availability groups, connection loss, custom rules and analysis findings. Also the fall-through for any metric the taxonomy does not name.",
        _ => "",
    };

    [McpServerTool(Name = "set_notification_route_enabled"), Description(
        "Enables or disables a notification route by route_id, WITHOUT deleting it. Changes shared alert configuration the service delivers on: a disabled route is skipped at resolution as if it did not exist, so its alerts fall to the next level (family route, then parent channels). Reversible: re-enabling restores it exactly. Returns updated (route as stored), unchanged (already that value, safe to retry), or not_found. Cannot author or re-point a route; that is Settings-window only." +
        "<<GUIDE>>" +
        "Enables or disables a notification route by its route_id (from get_notification_routes) WITHOUT deleting it. " +
        "A disabled route is skipped at resolution exactly as if it did not exist, so the alerts it matched fall to " +
        "the next level (a family route, then the parent channels) on the next firing — the reversible form of " +
        "'send this family to the parent channels for now'. Returns {status:\"updated\", route:{...}} with the route AS " +
        "STORED, {status:\"unchanged\", route:{...}} when it already holds that value (a retry is safe and costs no " +
        "write), or {status:\"not_found\"}. The running service picks the change up on its next collection sweep, " +
        "when the write's config_version bump makes it reload its channel configuration. This tool cannot author or " +
        "re-point a route: destinations are bearer secrets and are set only in the Viewer's Settings window.")]
    public static async Task<string> SetNotificationRouteEnabled(
        NpgsqlDataSource postgres,
        [Description("The route_id to enable or disable (from get_notification_routes).")] int route_id,
        [Description("true to put the route back in force, false to skip it at resolution while keeping it.")] bool enabled)
    {
        try
        {
            var store = new PgNotificationRouteStore(postgres);
            var existing = (await store.LoadSummariesAsync()).FirstOrDefault(r => r.RouteId == route_id);
            if (existing is null)
            {
                return Outcome("not_found", $"No notification route with route_id {route_id}.");
            }

            if (existing.Enabled == enabled)
            {
                return JsonSerializer.Serialize(new { status = "unchanged", route = RoutePayload(existing) }, McpHelpers.JsonOptions);
            }

            await store.SetEnabledAsync(route_id, enabled);

            /* Re-read AFTER the write, so the reported row is the stored one (set_mute_rule_enabled's rule). */
            var stored = (await store.LoadSummariesAsync()).FirstOrDefault(r => r.RouteId == route_id);
            return JsonSerializer.Serialize(new { status = "updated", route = stored is null ? null : RoutePayload(stored) }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("set_notification_route_enabled", ex);
        }
    }

    [McpServerTool(Name = "delete_notification_route"), Description(
        "Deletes a notification route by its route_id (from get_notification_routes). Permanent: the alerts it " +
        "matched fall to the next level (a family route, then the parent channels) on the next firing. Returns " +
        "{status:\"deleted\", route_id} or {status:\"not_found\"}. Prefer set_notification_route_enabled for a " +
        "temporary change — a deleted route's destinations are gone and only the Viewer's Settings window can author " +
        "them again. The running service stops honoring the route on its next collection sweep.")]
    public static async Task<string> DeleteNotificationRoute(
        NpgsqlDataSource postgres,
        [Description("The route_id to delete (from get_notification_routes).")] int route_id)
    {
        try
        {
            var deleted = await new PgNotificationRouteStore(postgres).DeleteAsync(route_id);
            return deleted == 0
                ? Outcome("not_found", $"No notification route with route_id {route_id}.")
                : JsonSerializer.Serialize(new { status = "deleted", route_id }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_notification_route", ex);
        }
    }

    private static object RoutePayload(PgNotificationRouteStore.RouteSummary r) => new
    {
        route_id = r.RouteId,
        metric_match = r.MetricMatch,
        match_kind = r.Family is null ? "exact_metric" : "family",
        family = r.Family ?? AlertFamily.Of(r.MetricMatch),
        configured_channels = r.ConfiguredChannels,
        smtp_recipients = r.EmailRecipients,
        enabled = r.Enabled,
        modified_at_utc = r.ModifiedAtUtc,
    };

    [McpServerTool(Name = "get_mute_rules"), Description("Gets the configured alert mute rules. Mute rules suppress specific recurring alerts (by server, metric, database, query text, wait type, or job name) while still logging them — so an agent can tell a genuinely healthy-quiet server from one whose alerts are being suppressed.")]
    public static async Task<string> GetMuteRules(
        NpgsqlDataSource postgres,
        [Description("Include only enabled, non-expired rules. Default true.")] bool enabled_only = true,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var all = await new PgMuteRuleStore(postgres).LoadAllAsync(cancellationToken);
            var rules = all.AsEnumerable();
            if (enabled_only)
                rules = rules.Where(r => r.Enabled && (r.ExpiresAtUtc == null || r.ExpiresAtUtc > DateTime.UtcNow));

            var list = rules.ToList();

            if (list.Count == 0)
            {
                /*
                    This tool exists so an agent can tell a genuinely healthy-quiet server from one whose
                    alerts are being suppressed, and an empty array answered that question with silence.
                    Both kinds of empty are true negatives -- nothing is being suppressed either way, which
                    is why both are "empty" rather than one being "unavailable" -- but they are not the same
                    fact. No rule has ever been written is a different state from five rules that all lapsed,
                    and the second one is a mute somebody INTENDED that is no longer in force.
                */
                var configured = all.Count;
                return McpHelpers.Status(
                    "empty",
                    configured == 0
                        ? "No mute rules are configured for this store, so no alert is being suppressed anywhere — a quiet alert history is genuine rather than muted."
                        : $"No mute rules are in force right now: {configured} rule(s) exist but every one of them is disabled or expired, so nothing is being suppressed. Pass enabled_only=false to list them — this is a lapsed mute, not an absent one.",
                    new { enabled_only, configured_count = configured, excluded_by_filter = configured - list.Count });
            }

            return JsonSerializer.Serialize(new
            {
                total_count = list.Count,
                mute_rules = list.Select(BuildMuteRulePayload)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_mute_rules", ex);
        }
    }

    /// <summary>The per-rule shape get_mute_rules returns AND create_mute_rule echoes back for the stored rule.</summary>
    private static object BuildMuteRulePayload(MuteRule r) => new
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
    };

    [McpServerTool(Name = "update_alert_settings"), Description(
        "PARTIAL update of the single global alert-settings row the service delivers on. Call " +
        "get_alert_settings FIRST; send back only the changed fields, in the same nested shape; " +
        "omitted fields stay unchanged. An invalid value or unknown field writes NOTHING; returns " +
        "status invalid. poison_wait.threshold_ms is RETIRED: accepted with a warnings entry but " +
        "never changes when the Poison Wait alert fires; poison_wait.enabled governs it. " +
        "health_bands and fleet_sweep are NOT alert families: health_bands only sets card bands " +
        "(delivers nothing); alerts_enabled does not govern fleet_sweep." +
        "<<GUIDE>>" +
        "Tunes the alert engine's configuration — a PARTIAL update of the single global alert-settings row. Call " +
        "get_alert_settings FIRST, change only the fields you want, and pass THOSE fields back here as JSON in the " +
        "SAME nested shape get_alert_settings returns (e.g. {\"cpu\":{\"threshold_percent\":90},\"cooldown_minutes\":10}); " +
        "every setting you do NOT include is left unchanged. Each provided field is validated against the SAME rules " +
        "the Viewer's Settings window enforces — thresholds in range, cpu.mode 'sql'|'total', delivery.mode " +
        "'Summary'|'PerEvent', counts within their bounds; an out-of-range value or an unknown field returns " +
        "{status:\"invalid\", ...} and writes NOTHING. On success the running service hot-reloads the change within " +
        "one collection sweep. TWO cooldowns are writable and they are different stages: cooldown_minutes gates " +
        "the engine's FIRE decision, delivery.cooldown_minutes bounds the resulting " +
        "Slack/Teams/PagerDuty/webhook/email post both per alert FINGERPRINT and, for a re-notification, " +
        "per METRIC across the whole fleet — so one fault on forty servers does not cost forty posts an " +
        "hour, and the servers that bound holds back are named on the post that does go out. A first " +
        "notice is never held back by either bound, and delivery.mode PerEvent opts out of the per-metric " +
        "one. Its stored name, email_cooldown_minutes, is also accepted as a top-level alias, but send " +
        "only one of the two spellings. An alert whose Server reads 'Monitor Store' — or the store's own " +
        "peers.storeName label, when that file-only field is set — " +
        "is about the monitor's own store and is tuned under self_alerts — including Retention Held, whose " +
        "self_alerts.retention_hold_warn_ratio and self_alerts.retention_hold_critical_ratio are how many times " +
        "its configured horizon a held retention tier must be holding to warn and to go critical. Both accept " +
        "2.0 upward only: healthy whole-chunk granularity reaches 1.4x on a store that is working correctly, so " +
        "a lower threshold fires on nothing being wrong. Setting critical BELOW warn is accepted and means every " +
        "fire is Critical. The health_bands group is NOT an alert and does not deliver anything: " +
        "health_bands.deadlock_warn_per_hour and health_bands.deadlock_critical_per_hour are the per-server " +
        "HEALTH BAND tiers in deadlocks per HOUR, normalised over the window each surface asked for, and they " +
        "decide what colour a card reads and how get_fleet_overview counts bands. Both accept 1.0 upward: one " +
        "per hour is the tightest setting that is still a rate, so no value here can restore the 'any deadlock " +
        "is Critical' reading these tiers replaced. Setting critical BELOW warn is accepted and means every " +
        "banded rate is Critical. " +
        "file_growth.rise_mb is megabytes per HOUR averaged over file_growth.lookback_minutes (a rate — the same 10240 is 10 GB/hr on any lookback; the engine scales it to the window), so shortening the lookback does not tighten the rise gate and lengthening it does not loosen it; only the rate does. " +
        "Two keys govern the PostgreSQL versions of the two count alerts and are NOT the same numbers as their SQL Server neighbours: deadlocks.pg_count_threshold and blocking.pg_count_threshold, both accepting 1 upward. They sit inside those groups rather than a section of their own so both engines' figures are visible together, but tuning deadlocks.count_threshold does NOT move the PostgreSQL gate and tuning deadlocks.pg_count_threshold does NOT move the SQL Server one. The enabled switch in each group DOES govern both engines. They are separate because the two engines count with different instruments - SQL Server's figure is captured deadlock graphs, the PostgreSQL one is deadlocks parsed from the server log (get_pg_deadlocks) - and an operator tuning one should not silently move the other. Both engines' fleet cards now band deadlocks through the SAME health_bands.deadlock_warn_per_hour tiers (the PostgreSQL card differences the server's own pg_stat_database.deadlocks counter over the window), so the move - raising a fire gate to meet the band's Warning bar so a page and an amber dot describe the same server - is available on either knob. On the blocking side the SQL Server count is engine-recorded blocked-process reports and the PostgreSQL one is distinct root blockers in a periodic SAMPLE of pg_stat_activity. Both PostgreSQL keys are ignored on a store with no PostgreSQL targets. " +
        "The fleet_sweep group is NOT an alert family and the alerts_enabled master switch does not govern it: " +
        "fleet_sweep.enabled turns the scheduled whole-fleet sweep report on or off, and " +
        "fleet_sweep.interval_minutes (15\u20131440, default 60) is its cadence. Sweeps deliberately keep running " +
        "under alerts_enabled: false — that is when they carry the would-have-paged ledger — so muting the " +
        "fleet does not blind the report surface. " +
        "For silencing ONE recurring signature for a " +
        "long stretch, use create_mute_rule instead of a long delivery cooldown: a mute is scoped, expires, is " +
        "listed by get_mute_rules, and still logs the alert, where the cooldown is global to every fingerprint " +
        "on every server and no tool reports what it suppressed. SMTP/webhook delivery credentials are managed " +
        "separately and cannot be set here " +
        "— configure them in the standalone Darling Viewer app's Settings window (Notifications section), which " +
        "connects to this store (including remotely, not just localhost) rather than requiring desktop access to " +
        "this specific box. " +
        "poison_wait.threshold_ms is RETIRED: the Poison Wait alert grades accumulated wait over a ten-minute window, " +
        "so this value is accepted and stored for round-trip compatibility but consulted by nothing — setting it returns " +
        "status updated with a warnings entry saying so, and does not change when the alert fires. poison_wait.enabled still governs the alert. " +
        "analysis.uncorroborated_route is WRITABLE since V137: 'digest' or 'page' (case-insensitive; stored lower-case) writes " +
        "config_alert_settings.analysis_uncorroborated_route, which the running service reads OVER darling.json's analysis.uncorroboratedRoute " +
        "from its next collection sweep and consults on the next scheduled-analysis delivery — no restart, no file edit; null CLEARS the column " +
        "so the file governs again (get_alert_settings then reports uncorroborated_route_source 'file'); any other value is refused and nothing " +
        "is written. Handing a whole get_alert_settings payload back writes the EFFECTIVE route it reported into the store, so a value the file " +
        "had been supplying becomes the store's from then on — the response carries a warnings entry when the echoed uncorroborated_route_source " +
        "was 'file' or 'default', and uncorroborated_route_source / uncorroborated_route_note themselves are read-only companions, accepted and " +
        "ignored on the way in. A corroborated finding pages under either route. " +
        "long_running_query.excluded_program_name_prefixes and long_running_query.excluded_logins are the two arrays of the " +
        "Long-Running Query alert's OPT-OUT knob: a session whose program_name STARTS WITH a prefix in the first, or whose login_name " +
        "IS an entry of the second (both case-insensitive, no wildcard grammar — a * is a character), is NOT EVALUATED by that " +
        "alert — never read into the decision, never counted, never fingerprinted — which is not a mute (a mute silences a fire " +
        "already decided; use create_mute_rule for that). Entries are trimmed, blanks and case-insensitive duplicates dropped. " +
        "Pass the FULL list each time — it replaces, like excluded_databases; an EMPTY array clears that arm and is honoured (the " +
        "shipped values are DEFAULTS from a 7-day production read — the SQL Agent job-step prefix and the two NT AUTHORITY service " +
        "logins; the application's admin login is deliberately not one, because it also runs real ad-hoc long-runners). The fired " +
        "card's Excluded Count item, split by arm, shows the knob working. " +
        "Returns {status:\"updated\", updated_fields:[...], warnings:[...], settings:{...}} with the full new settings (warnings is " +
        "empty unless a stored-but-unconsulted field was sent, or a round-trip moved the route knob's source from the file to the store), or " +
        "{status:\"unavailable\"} when the settings row has not been seeded yet.")]
    public static async Task<string> UpdateAlertSettings(
        NpgsqlDataSource postgres,
        [Description("A JSON object with ONLY the alert-settings fields to change, in the nested shape get_alert_settings returns (e.g. {\"cpu\":{\"threshold_percent\":90},\"blocking\":{\"enabled\":false},\"cooldown_minutes\":10}).")] string settings_json)
    {
        try
        {
            JsonNode? root;
            try
            {
                root = JsonNode.Parse(settings_json);
            }
            catch (JsonException ex)
            {
                return Outcome("invalid", $"settings_json is not valid JSON: {ex.Message}");
            }

            if (root is not JsonObject body)
            {
                return Outcome("invalid", "settings_json must be a JSON object of the fields to change (see get_alert_settings for the shape).");
            }

            var (updates, error, warnings) = BuildAlertSettingsUpdate(body);
            if (error != null)
            {
                return Outcome("invalid", error);
            }

            if (updates.Count == 0)
            {
                return Outcome("invalid", "No known alert-settings fields were provided. Send fields in the shape get_alert_settings returns (e.g. {\"cpu\":{\"threshold_percent\":90}}).");
            }

            /* Only the provided columns are written. Column names are this method's compile-time constants (never
               the caller's input), so interpolating them into the SET list is injection-safe; every VALUE is a
               bound parameter. The single-row config_version self-bump is left to the config-table trigger.

               ONE STATEMENT PER TABLE, in ONE transaction (#3314): the delivery cooldown lives on
               config_notification while every other knob is on config_alert_settings, and a partial update is
               the worst outcome available here -- the caller is told "updated" and the re-read below reports a
               merged state that half-landed, with no indication which half. The statement order is FIXED
               rather than the grouping's hash order, so two concurrent tools can never take the two singleton
               rows in opposite orders. Both tables carry a bump trigger, so either statement alone is enough
               to make the service reload.

               NEITHER statement touches modified_at, matching what this tool has always done to
               config_alert_settings. Both Viewer upserts do bump it, and the asymmetry is deliberate: nothing
               reads modified_at -- it is in no viewer projection, no payload and no decision -- and bumping it
               on config_notification would mean granting mcp UPDATE on a SECOND column of a table holding
               bearer secrets, to maintain a value with no reader. */
            await using var connection = await postgres.OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();

            foreach (var table in WritableTables)
            {
                var forTable = updates.Where(u => string.Equals(u.Table, table, StringComparison.Ordinal)).ToList();
                if (forTable.Count == 0)
                {
                    continue;
                }

                var setClause = string.Join(", ", forTable.Select((u, i) => $"{u.Column} = ${i + 1}"));
                /* Constructed two-arg on the store connection with the transaction ASSIGNED, not passed as a
                   third ctor argument. McpReadCommandTimeoutTests classifies every command on this surface as
                   addressing the store or a monitored TARGET, and the three-arg
                   new NpgsqlCommand(sql, connection, transaction) form is the monitored-target shape (the
                   HypoPG experiment's, bounded by a server-side SET LOCAL rather than by McpCommandDeadlines).
                   Widening that allowlist to admit a transaction would have let a real target command take a
                   store bound, so the store command takes the shape the guard already recognises. The SQL is
                   hoisted to a local for the same reason: the recognised form's first argument is an
                   identifier. */
                var sql = $"UPDATE {table} SET {setClause} WHERE id = 1";
                await using var command = new NpgsqlCommand(sql, connection) { Transaction = transaction };
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                foreach (var target in forTable)
                {
                    command.Parameters.Add(target.Param);
                }

                if (await command.ExecuteNonQueryAsync() == 0)
                {
                    /* Rolled back by the transaction's disposal on the early return -- nothing this call
                       provided is left applied, so the reported failure and the store agree. */
                    return Outcome("unavailable",
                        $"No {table} row is present in the store yet (id = 1). The service seeds it on startup (or the Viewer's Settings window writes it); until then there is nothing to update.");
                }
            }

            await transaction.CommitAsync();

            /* Re-read so the caller sees the authoritative merged state — the write fired the config-table trigger
               that self-bumps config_version, so the running service reloads this within one sweep. ONE snapshot
               across both tables, for the reason get_alert_settings uses one: "authoritative merged state" is
               a claim a pair of independent reads cannot keep. */
            var (reread, rereadCooldown) = await DarlingAlertReader.GetAlertConfigurationAsync(postgres);
            return JsonSerializer.Serialize(new
            {
                status = "updated",
                /* Bare column names, unqualified, even though two tables are now in play: this array is a
                   consumer API and qualifying the existing entries would redefine every one of them. No
                   writable column name appears on both tables, so a bare name is still unambiguous --
                   asserted, not assumed, by WritableColumnNames_DoNotCollideAcrossTheTwoTables. */
                updated_fields = updates.Select(u => u.Column).ToArray(),
                /* #3653: always present, usually empty — a caller branching on it should not have to probe
                   for the key. Non-empty only when a field was written that nothing consults (today:
                   poison_wait.threshold_ms); the field IS in updated_fields, because it was updated. */
                warnings = warnings.ToArray(),
                settings = reread is null || rereadCooldown is null ? null : BuildAlertSettingsPayload(reread, rereadCooldown.Value)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_alert_settings", ex);
        }
    }

    [McpServerTool(Name = "create_mute_rule"), Description(
        "Creates an alert mute rule: matching alerts are still logged, just not delivered. Changes shared alert configuration the service delivers on. Provide any combination of scope/pattern fields; a field left out does not narrow the rule, so a rule with NO fields set matches and mutes EVERY alert across the whole fleet. No confirm step: the rule takes effect on the running service's next collection sweep. Returns the stored rule with its generated id, for delete_mute_rule or set_mute_rule_enabled." +
        "<<GUIDE>>" +
        "Creates an alert mute rule that suppresses matching alerts (they are still logged, just not delivered) — " +
        "the same rules get_mute_rules lists and the Viewer's Manage Mute Rules surface writes. Provide any " +
        "combination of the scope/pattern fields; a field left out does not narrow the rule, so a rule with NO " +
        "fields set matches — and mutes — EVERY alert (a whole-fleet silence). server_name and metric_name match " +
        "the alert's exact values (see get_alert_history / get_alert_settings for the names in use); the *_pattern " +
        "fields are case-insensitive substring matches. expires_at is an optional ISO-8601 UTC timestamp after " +
        "which the rule stops applying; omit it for a permanent rule. Returns the stored rule, including its " +
        "generated id (for delete_mute_rule). The running service applies the rule on its next collection " +
        "sweep, when the write's config_version bump makes it reload its mute cache — so a matching alert " +
        "already mid-flight can still be delivered once.")]
    public static async Task<string> CreateMuteRule(
        NpgsqlDataSource postgres,
        [Description("Scope the rule to this server (its display name, as get_alert_history reports). Omit for all servers.")] string? server_name = null,
        [Description("Scope to this alert metric (e.g. 'High CPU', 'Blocking Detected', 'Deadlocks Detected'). Omit for all metrics.")] string? metric_name = null,
        [Description("Case-insensitive substring the alert's database name must contain. Omit for any database.")] string? database_pattern = null,
        [Description("Case-insensitive substring the alert's query text must contain. Omit for any query.")] string? query_text_pattern = null,
        [Description("Case-insensitive substring the alert's wait type must contain. Omit for any wait type.")] string? wait_type_pattern = null,
        [Description("Case-insensitive substring the alert's job name must contain. Omit for any job.")] string? job_name_pattern = null,
        [Description("Optional human-readable reason, shown in the mute-rule list.")] string? reason = null,
        [Description("Optional ISO-8601 UTC expiry (e.g. 2026-08-01T00:00:00Z); after this the rule no longer mutes. Omit for a permanent rule.")] string? expires_at = null)
    {
        try
        {
            DateTime? expiresAtUtc = null;
            if (!string.IsNullOrWhiteSpace(expires_at))
            {
                if (!DateTime.TryParse(expires_at, CultureInfo.InvariantCulture,
                        DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
                {
                    return Outcome("invalid", $"expires_at '{expires_at}' is not a valid ISO-8601 timestamp (e.g. 2026-08-01T00:00:00Z).");
                }

                expiresAtUtc = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
            }

            /* A new MuteRule defaults Id to a fresh GUID — the SAME id-generation the Viewer's mute-create path
               uses (MuteRuleEditDialog builds a `new MuteRule()`), persisted through the SAME PgMuteRuleStore. */
            var rule = new MuteRule
            {
                Enabled = true,
                CreatedAtUtc = DateTime.UtcNow,
                ExpiresAtUtc = expiresAtUtc,
                Reason = Trimmed(reason),
                ServerName = Trimmed(server_name),
                MetricName = Trimmed(metric_name),
                DatabasePattern = Trimmed(database_pattern),
                QueryTextPattern = Trimmed(query_text_pattern),
                WaitTypePattern = Trimmed(wait_type_pattern),
                JobNamePattern = Trimmed(job_name_pattern)
            };

            await new PgMuteRuleStore(postgres).InsertAsync(rule);
            return JsonSerializer.Serialize(new { status = "created", mute_rule = BuildMuteRulePayload(rule) }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_mute_rule", ex);
        }
    }

    /// <summary>
    /// The web dashboard's create half (#3450) over the <see cref="IMuteRuleStore"/> seam — create_mute_rule's
    /// semantics with the fields arriving as ONE JSON object (the web request body) instead of MCP's discrete
    /// parameters. The body is parsed by <see cref="BuildMuteRuleUpdate"/> — the SAME whitelist-and-first-error
    /// authority the update verb runs — so the two surfaces cannot disagree about a field name, a bad expiry, or
    /// which keys are refused (<c>id</c> / <c>created_at_utc</c> / <c>enabled</c> / <c>summary</c> get the same
    /// pointed messages here; a new rule is born enabled, and the flag verb owns the flag afterwards). Two
    /// consequences of that sharing are deliberate rather than incidental: a BLANK string is refused (the MCP
    /// create folds whitespace to null because omission is available there; on a JSON body <c>""</c> is far more
    /// likely a mistake than a value), and an explicit JSON null is accepted as the no-op it is (every field
    /// starts null). An EMPTY object is legal, exactly as a create_mute_rule call with no arguments is — a rule
    /// with no constraining fields mutes EVERY alert, and that warning belongs on the surface's description, not
    /// in a refusal its MCP twin does not make.
    ///
    /// <para>The new rule is born ENABLED with a fresh GUID id and <c>created_at_utc</c> = now — the same
    /// <see cref="MuteRule"/> initializer defaults the Viewer's dialog and the MCP tool rely on. The reported
    /// rule is <b>re-read from the store after the insert</b>, the discipline every mute verb follows: the
    /// <c>created_at_utc</c> on the wire — the #3306 clock — is the value the store HOLDS, not a restatement of
    /// the value this method computed, which is the only form in which the two can disagree and be seen to.</para>
    /// </summary>
    internal static async Task<string> CreateMuteRuleCore(IMuteRuleStore store, string fieldsJson)
    {
        try
        {
            JsonNode? root;
            try
            {
                root = JsonNode.Parse(fieldsJson);
            }
            catch (JsonException ex)
            {
                return Outcome("invalid", $"the request body is not valid JSON: {ex.Message}");
            }

            if (root is not JsonObject body)
            {
                return Outcome("invalid",
                    "the request body must be a JSON object of mute-rule fields (see get_mute_rules for the shape). An empty object creates a rule that mutes EVERY alert.");
            }

            var (changes, error) = BuildMuteRuleUpdate(body);
            if (error != null)
            {
                return Outcome("invalid", error);
            }

            /* A new MuteRule defaults Id to a fresh GUID and Enabled to true — the SAME id-generation and
               born-enabled default the Viewer's create dialog and the MCP create tool use. The parsed appliers
               write the caller's fields over the blank rule; a null-clear applier is a no-op on a field that
               starts null, which is what lets create and update share one parser without sharing a merge base. */
            var rule = new MuteRule { CreatedAtUtc = DateTime.UtcNow };
            foreach (var change in changes)
            {
                change.Apply(rule);
            }

            await store.InsertAsync(rule);

            var stored = await FindRuleAsync(store, rule.Id);
            if (stored is null)
            {
                /* The insert landed and the rule is gone: a concurrent delete (or the expiry purge, on a rule
                   created already-expired) took the row between the write and the read back. Both facts are
                   named because 'not_found' alone would read as a call that wrote nothing. */
                return Outcome("not_found",
                    $"Mute rule '{rule.Id}' was created but is no longer in the store — it was deleted concurrently.");
            }

            return JsonSerializer.Serialize(
                new { status = "created", mute_rule = BuildMuteRulePayload(stored) },
                McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_mute_rule", ex);
        }
    }

    [McpServerTool(Name = "delete_mute_rule"), Description(
        "Deletes an alert mute rule by its id (from get_mute_rules or create_mute_rule). Returns " +
        "{status:\"deleted\", rule_id} on success, or {status:\"not_found\"} when no rule has that id. Permanent. " +
        "The running service stops honoring the rule on its next collection sweep, when the delete's " +
        "config_version bump makes it reload its mute cache.")]
    public static Task<string> DeleteMuteRule(
        NpgsqlDataSource postgres,
        [Description("The id of the mute rule to delete (from get_mute_rules or create_mute_rule).")] string rule_id) =>
        DeleteMuteRuleCore(new PgMuteRuleStore(postgres), rule_id);

    /// <summary>
    /// delete_mute_rule's body over the <see cref="IMuteRuleStore"/> seam <see cref="PgMuteRuleStore"/>
    /// implements — hoisted verbatim (#3450) so the web dashboard's DELETE endpoint runs the SAME decisions on
    /// its own pool, exactly as the enable and update verbs already share theirs. The existence check runs
    /// through the SAME store read get_mute_rules uses, so 'deleted' vs 'not_found' is honest
    /// (<see cref="IMuteRuleStore.DeleteAsync"/> is a bare DELETE that reports no row count).
    /// </summary>
    internal static async Task<string> DeleteMuteRuleCore(IMuteRuleStore store, string ruleId)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(ruleId))
            {
                return Outcome("invalid", "rule_id is required.");
            }

            var rules = await store.LoadAllAsync();
            if (!rules.Any(r => string.Equals(r.Id, ruleId, StringComparison.Ordinal)))
            {
                return Outcome("not_found", $"No mute rule with id '{ruleId}'.");
            }

            await store.DeleteAsync(ruleId);
            return JsonSerializer.Serialize(new { status = "deleted", rule_id = ruleId }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_mute_rule", ex);
        }
    }

    [McpServerTool(Name = "set_mute_rule_enabled"), Description(
        "Enables or disables an existing mute rule by id, WITHOUT deleting it. Changes shared alert configuration the service delivers on. A disabled rule suppresses nothing but keeps its id, scope, reason and creation date, so silencing and restoring later is one reversible change, unlike delete+create, which loses those and resets the Stale Mute Rules age clock. Returns updated (rule as stored), unchanged (already that value, safe to retry), or not_found." +
        "<<GUIDE>>" +
        "Enables or disables an existing alert mute rule by its id (from get_mute_rules or create_mute_rule) " +
        "WITHOUT deleting it. A disabled rule suppresses nothing while keeping its id, its scope, its reason " +
        "and its creation date, so silencing a signal for a window and restoring it afterwards is one " +
        "reversible change to one rule. USE THIS rather than delete_mute_rule followed by create_mute_rule " +
        "for a temporary or reversible change, for two reasons. A re-created rule is a NEW rule: it carries a " +
        "new id (anything citing the old one now points at nothing) and a new creation date, and the Stale " +
        "Mute Rules self-alert ages a rule from its creation date — so a rule re-created every few days never " +
        "becomes stale and is never reported, while a disabled-and-re-enabled one is reported on the age it " +
        "actually has. And between the delete taking effect and the create taking effect, every alert the " +
        "rule was suppressing is delivered. NEITHER direction changes created_at_utc. Returns " +
        "{status:\"updated\", mute_rule:{...}} with the rule AS STORED, {status:\"unchanged\", mute_rule:{...}} " +
        "when the rule already holds that value (so a retry is safe and costs no write), or " +
        "{status:\"not_found\"} when no rule has that id. The running service applies the change on its next " +
        "collection sweep, when the write's config_version bump makes it reload its mute cache — so a " +
        "matching alert already mid-flight can still be delivered once after a disable, and one can still be " +
        "suppressed once after an enable.")]
    public static Task<string> SetMuteRuleEnabled(
        NpgsqlDataSource postgres,
        [Description("The id of the mute rule to enable or disable (from get_mute_rules or create_mute_rule).")] string rule_id,
        [Description("true to put the rule back in force, false to stop it suppressing while keeping the rule.")] bool enabled) =>
        SetMuteRuleEnabledCore(new PgMuteRuleStore(postgres), rule_id, enabled);

    /// <summary>
    /// set_mute_rule_enabled's body over the <see cref="IMuteRuleStore"/> seam
    /// <see cref="PgMuteRuleStore"/> implements, so the tool's decisions are reachable without a store.
    ///
    /// <para><b>The flag is the only field written.</b> It goes through
    /// <see cref="IMuteRuleStore.SetEnabledAsync"/> — a narrow UPDATE of <c>enabled</c> alone.
    /// <c>created_at_utc</c> is when the rule was AUTHORED, and the Stale Mute Rules self-alert (#3306) ages
    /// a rule from it, so a rule taken out of force and put back is reported on the age it actually has. A
    /// path that rebuilt the row on the way through would hand a disable/re-enable cycle the same week of
    /// invisibility a delete-and-re-create buys — more cheaply, keeping the id, and with nothing changing
    /// for a reader to notice.</para>
    ///
    /// <para><b>The reported rule is re-read from the store AFTER the write</b>, not the one this method
    /// already held. <c>created_at_utc</c> on the wire is therefore the stored value rather than a
    /// restatement of a value read before the write, which is the only form in which the caller's copy can
    /// disagree with the store's and be seen to.</para>
    ///
    /// <para><b>Setting the flag to the value it already holds writes nothing</b> and reports
    /// <c>unchanged</c>: the call is safe to retry, and a caller can tell a change it made from a state it
    /// found. The existence read that decides <c>not_found</c> is what carries the current flag, so this
    /// costs no extra round trip.</para>
    /// </summary>
    internal static async Task<string> SetMuteRuleEnabledCore(IMuteRuleStore store, string ruleId, bool enabled)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(ruleId))
            {
                return Outcome("invalid", "rule_id is required.");
            }

            /* Existence check through the SAME store get_mute_rules reads, so 'updated' vs 'not_found' is
               honest (SetEnabledAsync is a bare UPDATE that reports no row count). */
            var existing = await FindRuleAsync(store, ruleId);
            if (existing is null)
            {
                return Outcome("not_found", $"No mute rule with id '{ruleId}'.");
            }

            if (existing.Enabled == enabled)
            {
                return JsonSerializer.Serialize(
                    new { status = "unchanged", mute_rule = BuildMuteRulePayload(existing) },
                    McpHelpers.JsonOptions);
            }

            await store.SetEnabledAsync(ruleId, enabled);

            var stored = await FindRuleAsync(store, ruleId);
            if (stored is null)
            {
                /* The flag landed and the rule is gone, so the state to report is the absence: a concurrent
                   delete_mute_rule (or an expiry purge) took the row between the write and the read back.
                   Both facts are named because 'not_found' alone would read as a call that wrote nothing. */
                return Outcome("not_found",
                    $"Mute rule '{ruleId}' was set enabled={(enabled ? "true" : "false")} but is no longer in the store — it was deleted concurrently.");
            }

            return JsonSerializer.Serialize(
                new { status = "updated", mute_rule = BuildMuteRulePayload(stored) },
                McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("set_mute_rule_enabled", ex);
        }
    }

    [McpServerTool(Name = "update_mute_rule"), Description(
        "Edits an existing alert mute rule IN PLACE by its id — changes shared alert configuration. PARTIAL " +
        "update: send only the fields to change, in the same shape get_mute_rules returns; a field you do NOT " +
        "send stays exactly as stored, and an EXPLICIT JSON null CLEARS that field. enabled is NOT editable " +
        "here — use set_mute_rule_enabled instead. created_at_utc never moves, whatever changes. Clearing " +
        "scope fields WIDENS the rule — one left with none mutes EVERY alert. Returns updated, unchanged " +
        "(nothing to write), not_found, or invalid (nothing written)." +
        "<<GUIDE>>" +
        "Edits an existing alert mute rule IN PLACE by its id (from get_mute_rules or create_mute_rule) — the " +
        "changes the enabled flag cannot express: narrowing or correcting a pattern, rewording a reason, adding " +
        "an expires_at_utc to a rule that should stop being permanent, or clearing one so it stays. A PARTIAL " +
        "update: pass ONLY the fields to change as JSON in the SAME shape get_mute_rules returns (e.g. " +
        "{\"reason\":\"root cause found\",\"expires_at_utc\":\"2026-08-01T00:00:00Z\"}); a field you do NOT " +
        "send is left exactly as stored, and an EXPLICIT JSON null clears a field — the same clearing the " +
        "Viewer's edit dialog performs by blanking it — so {\"expires_at_utc\":null} makes a rule permanent and " +
        "{\"job_name_pattern\":null} stops constraining that dimension. Editable fields: server_name, " +
        "metric_name, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, reason, " +
        "expires_at_utc (create_mute_rule's expires_at spelling is accepted as a write-only alias; send only " +
        "one). enabled is NOT editable here — use set_mute_rule_enabled, the dedicated reversible verb. USE THIS " +
        "rather than delete_mute_rule followed by create_mute_rule: a re-created rule is a NEW rule with a new " +
        "id and a new creation date, and the Stale Mute Rules self-alert ages a rule from its creation date — so " +
        "a rule re-created on every edit never becomes stale and is never reported, while an edited one is " +
        "reported on the age it actually has. Editing NEVER moves created_at_utc, whatever fields change; that " +
        "preservation is the reason this verb exists. Two cautions: clearing scope fields WIDENS the rule — one " +
        "with no constraining fields left mutes EVERY alert (a whole-fleet silence) — and an edited server_name " +
        "must match the alert rows' spelling EXACTLY as get_alert_history reports it (the monitor's own " +
        "self-alert family and the engine alerts can spell the same server differently — display-short vs " +
        "registry name — so copy the spelling off the alert rows being muted rather than retyping it), or the " +
        "rule stays in force while matching nothing. Returns {status:\"updated\", updated_fields:[...], " +
        "mute_rule:{...}} with the rule AS STORED (re-read after the write), {status:\"unchanged\", " +
        "mute_rule:{...}} when every provided field already holds that value (so a retry is safe and costs no " +
        "write), {status:\"not_found\"} when no rule has that id, or {status:\"invalid\", ...} on a bad field " +
        "or value with NOTHING written. The running service applies the edit on its next collection sweep, when " +
        "the write's config_version bump makes it reload its mute cache.")]
    public static Task<string> UpdateMuteRule(
        NpgsqlDataSource postgres,
        [Description("The id of the mute rule to edit (from get_mute_rules or create_mute_rule).")] string rule_id,
        [Description("A JSON object with ONLY the mute-rule fields to change, in the shape get_mute_rules returns (e.g. {\"reason\":\"root cause found\"}). An explicit null clears a field; a field not sent does not change.")] string changes_json) =>
        UpdateMuteRuleCore(new PgMuteRuleStore(postgres), rule_id, changes_json);

    /// <summary>
    /// update_mute_rule's body over the <see cref="IMuteRuleStore"/> seam <see cref="PgMuteRuleStore"/>
    /// implements, so the tool's decisions are reachable without a store.
    ///
    /// <para><b>The partial update is a merge onto the STORED rule, written through the store's own
    /// <see cref="IMuteRuleStore.UpdateAsync"/>.</b> That statement is a full-row write of every editable
    /// column — the same shape the Viewer's Edit dialog saves through — so a field the caller did not send is
    /// restated at the value just read, and an explicit null is a VALUE (every editable column is nullable;
    /// the dialog clears one by blanking it). Following the store's own semantics is what makes clearing
    /// expressible at all: without it, removing an expires_at — making a rule permanent again — would need the
    /// delete-and-re-create this verb exists to end.</para>
    ///
    /// <para><b><c>created_at_utc</c> is structurally out of reach.</b> UpdateAsync's SET list does not name
    /// it (pinned by <c>TheShippedMuteRuleUpdates_NeverSetTheCreationDate</c>), so no edit resets the clock
    /// the Stale Mute Rules self-alert (#3306) ages a rule from. That preservation is the verb's reason to
    /// exist: every field edit used to require delete/recreate, and a rule re-created on every edit never
    /// becomes stale and is never reported.</para>
    ///
    /// <para><b><c>enabled</c> is carried, never edited.</b> The merged row restates the flag it read —
    /// UpdateAsync writes the whole editable row, so a concurrent set_mute_rule_enabled landing between this
    /// read and this write is overwritten, the same window the Viewer's Edit dialog has always had. Refusing
    /// the field here keeps flag changes on the narrow verb, whose UPDATE touches <c>enabled</c> alone.</para>
    ///
    /// <para><b>An edit that changes nothing writes nothing</b> and reports <c>unchanged</c> — a retry is
    /// safe, and a caller can tell a change it made from a state it found. The comparison is ORDINAL: a
    /// case-only edit IS an edit (the stored text changes, even though matching is case-insensitive), so the
    /// caller's spelling is honored rather than second-guessed. And <b>the reported rule is re-read from the
    /// store AFTER the write</b>; a rule deleted in that window reports the absence, naming the write that
    /// landed, rather than folding the race into a failure.</para>
    /// </summary>
    internal static async Task<string> UpdateMuteRuleCore(IMuteRuleStore store, string ruleId, string changesJson)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(ruleId))
            {
                return Outcome("invalid", "rule_id is required.");
            }

            /* Validation runs BEFORE persistence, mirroring update_alert_settings: a bad field or value
               returns 'invalid' without the store ever being read or written. */
            JsonNode? root;
            try
            {
                root = JsonNode.Parse(changesJson);
            }
            catch (JsonException ex)
            {
                return Outcome("invalid", $"changes_json is not valid JSON: {ex.Message}");
            }

            if (root is not JsonObject body)
            {
                return Outcome("invalid", "changes_json must be a JSON object of the mute-rule fields to change (see get_mute_rules for the shape).");
            }

            var (changes, error) = BuildMuteRuleUpdate(body);
            if (error != null)
            {
                return Outcome("invalid", error);
            }

            if (changes.Count == 0)
            {
                return Outcome("invalid", "No editable mute-rule fields were provided. Send only the fields to change (e.g. {\"reason\":\"...\"}); pass null to clear one.");
            }

            /* Existence check through the SAME store get_mute_rules reads, so 'updated' vs 'not_found' is
               honest (UpdateAsync is a bare UPDATE that reports no row count) — and the row it returns is
               the merge base the partial semantics need. */
            var existing = await FindRuleAsync(store, ruleId);
            if (existing is null)
            {
                return Outcome("not_found", $"No mute rule with id '{ruleId}'.");
            }

            var merged = existing.Clone();
            foreach (var change in changes)
            {
                change.Apply(merged);
            }

            if (SameEditableFields(existing, merged))
            {
                return JsonSerializer.Serialize(
                    new { status = "unchanged", mute_rule = BuildMuteRulePayload(existing) },
                    McpHelpers.JsonOptions);
            }

            await store.UpdateAsync(merged);

            var stored = await FindRuleAsync(store, ruleId);
            if (stored is null)
            {
                /* The edit landed and the rule is gone, so the state to report is the absence: a concurrent
                   delete_mute_rule (or an expiry purge) took the row between the write and the read back.
                   Both facts are named because 'not_found' alone would read as a call that wrote nothing. */
                return Outcome("not_found",
                    $"Mute rule '{ruleId}' was updated but is no longer in the store — it was deleted concurrently.");
            }

            return JsonSerializer.Serialize(new
            {
                status = "updated",
                /* The canonical wire names the call took charge of (the expires_at alias reports as
                   expires_at_utc) — what was SENT, not what differed: the write restates the whole editable
                   row either way, and 'unchanged' above is the no-difference answer. */
                updated_fields = changes.Select(c => c.Field).ToArray(),
                mute_rule = BuildMuteRulePayload(stored)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_mute_rule", ex);
        }
    }

    /// <summary>One validated field of a partial mute-rule update: the CANONICAL wire name it lands under
    /// (the expires_at alias normalizes to expires_at_utc, which is also what makes sending both spellings
    /// detectable as a duplicate) and the applier that writes it onto the merged rule.</summary>
    private sealed record MuteRuleFieldChange(string Field, Action<MuteRule> Apply);

    /// <summary>
    /// Parses a PARTIAL mute-rule update — the per-rule JSON shape get_mute_rules RETURNS, carrying only the
    /// fields to change — into appliers over a <see cref="MuteRule"/> clone. Returns a non-null error — and
    /// the caller writes nothing — on the FIRST bad field, mirroring <see cref="BuildAlertSettingsUpdate"/>'s
    /// whitelist-and-first-error discipline.
    ///
    /// <para>An explicit JSON null is the ONE spelling of "clear". A blank string is refused rather than
    /// treated as a second one: create_mute_rule folds whitespace to null because omission is available
    /// there, but on a partial update "" is far more likely a caller who meant to clear (or meant nothing)
    /// than a value, and two spellings of clear would make one of them always accidental.</para>
    ///
    /// <para>The four read-payload keys that are NOT editable each get a pointed refusal rather than the
    /// generic unknown-field text, because feeding a get_mute_rules row back with edits is the natural
    /// mistake: <c>id</c> is the rule's identity, <c>created_at_utc</c> is the #3306 clock this verb exists
    /// to preserve, <c>enabled</c> belongs to set_mute_rule_enabled, and <c>summary</c> is derived, not
    /// stored.</para>
    /// </summary>
    private static (List<MuteRuleFieldChange> Changes, string? Error) BuildMuteRuleUpdate(JsonObject body)
    {
        var changes = new List<MuteRuleFieldChange>();
        string? error = null;

        void AddText(string field, JsonNode? node, Action<MuteRule, string?> set)
        {
            if (error != null) return;
            if (node is null)
            {
                changes.Add(new MuteRuleFieldChange(field, r => set(r, null)));
            }
            else if (node is JsonValue v && v.TryGetValue<string>(out var s))
            {
                var trimmed = s.Trim();
                if (trimmed.Length == 0)
                    error = $"'{field}' is blank. Pass null to clear the field, or a non-blank value to set it.";
                else
                    changes.Add(new MuteRuleFieldChange(field, r => set(r, trimmed)));
            }
            else
            {
                error = $"'{field}' must be a string, or null to clear it.";
            }
        }

        /* `spelling` is the key the caller sent (for the error text); the recorded Field is always the
           canonical expires_at_utc, so both spellings in one body surface as a duplicate below. */
        void AddExpiry(string spelling, JsonNode? node)
        {
            if (error != null) return;
            if (node is null)
            {
                changes.Add(new MuteRuleFieldChange("expires_at_utc", r => r.ExpiresAtUtc = null));
            }
            else if (node is JsonValue v && v.TryGetValue<string>(out var s)
                && DateTime.TryParse(s, CultureInfo.InvariantCulture,
                    DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
            {
                var utc = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
                changes.Add(new MuteRuleFieldChange("expires_at_utc", r => r.ExpiresAtUtc = utc));
            }
            else
            {
                error = $"'{spelling}' must be an ISO-8601 UTC timestamp (e.g. 2026-08-01T00:00:00Z), or null for a permanent rule.";
            }
        }

        foreach (var prop in body)
        {
            if (error != null) break;
            switch (prop.Key)
            {
                case "server_name": AddText("server_name", prop.Value, (r, v) => r.ServerName = v); break;
                case "metric_name": AddText("metric_name", prop.Value, (r, v) => r.MetricName = v); break;
                case "database_pattern": AddText("database_pattern", prop.Value, (r, v) => r.DatabasePattern = v); break;
                case "query_text_pattern": AddText("query_text_pattern", prop.Value, (r, v) => r.QueryTextPattern = v); break;
                case "wait_type_pattern": AddText("wait_type_pattern", prop.Value, (r, v) => r.WaitTypePattern = v); break;
                case "job_name_pattern": AddText("job_name_pattern", prop.Value, (r, v) => r.JobNamePattern = v); break;
                case "reason": AddText("reason", prop.Value, (r, v) => r.Reason = v); break;
                case "expires_at_utc": AddExpiry("expires_at_utc", prop.Value); break;
                /* create_mute_rule's parameter spelling, accepted as a write-only alias so a caller moving
                   from create to update does not trip on the payload suffix — the same courtesy the
                   settings tool extends to email_cooldown_minutes, and like there the canonical name is the
                   only one the read emits. */
                case "expires_at": AddExpiry("expires_at", prop.Value); break;
                case "enabled":
                    error = "'enabled' is not editable here — use set_mute_rule_enabled, which writes the flag alone.";
                    break;
                case "id":
                    error = "'id' is the rule's identity and cannot be edited. Pass the rule to edit as rule_id; send only the fields to change.";
                    break;
                case "created_at_utc":
                    error = "'created_at_utc' never moves — it is the creation date the Stale Mute Rules self-alert ages a rule from, and preserving it is the reason this verb exists.";
                    break;
                case "summary":
                    error = "'summary' is derived from the scope fields and is not stored — edit the fields it summarizes instead.";
                    break;
                default:
                    error = $"Unknown field '{prop.Key}'. Editable fields: server_name, metric_name, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, reason, expires_at_utc.";
                    break;
            }
        }

        /* Two accepted keys claiming ONE field — expires_at and expires_at_utc are the only pair that can.
           Refused for the reason the settings tool refuses its alias pair: letting one silently win would
           tell the caller both applied. */
        if (error == null && changes.GroupBy(c => c.Field, StringComparer.Ordinal).Any(g => g.Count() > 1))
        {
            error = "'expires_at' and 'expires_at_utc' are two names for the same field; send only one of them.";
        }

        return (changes, error);
    }

    /// <summary>Whether two rules agree on every field update_mute_rule can move — the 'unchanged'
    /// comparison. ORDINAL on the text fields (see the core's doc for why a case-only edit counts);
    /// <c>Enabled</c> and <c>CreatedAtUtc</c> are excluded because the verb cannot move them, so including
    /// them could only ever mask a difference the caller did not ask about.</summary>
    private static bool SameEditableFields(MuteRule a, MuteRule b) =>
        string.Equals(a.ServerName, b.ServerName, StringComparison.Ordinal)
        && string.Equals(a.MetricName, b.MetricName, StringComparison.Ordinal)
        && string.Equals(a.DatabasePattern, b.DatabasePattern, StringComparison.Ordinal)
        && string.Equals(a.QueryTextPattern, b.QueryTextPattern, StringComparison.Ordinal)
        && string.Equals(a.WaitTypePattern, b.WaitTypePattern, StringComparison.Ordinal)
        && string.Equals(a.JobNamePattern, b.JobNamePattern, StringComparison.Ordinal)
        && string.Equals(a.Reason, b.Reason, StringComparison.Ordinal)
        && a.ExpiresAtUtc == b.ExpiresAtUtc;

    /// <summary>One rule by id, or null — an ordinal id match over the store's full read, which is the only
    /// read <see cref="IMuteRuleStore"/> offers.</summary>
    private static async Task<MuteRule?> FindRuleAsync(IMuteRuleStore store, string ruleId) =>
        (await store.LoadAllAsync())
            .FirstOrDefault(r => r is not null && string.Equals(r.Id, ruleId, StringComparison.Ordinal));

    /// <summary>The singleton config rows update_alert_settings writes, in the order it writes them — see
    /// the statement-order note at the write itself. Also the read side's table set: the settings row is
    /// <see cref="DarlingAlertReader.AlertSettingsSelectSql"/>'s source and the notification row is
    /// <see cref="DarlingAlertReader.DeliveryCooldownSelectSql"/>'s.</summary>
    internal static readonly string[] WritableTables = { AlertSettingsTable, NotificationTable };

    internal const string AlertSettingsTable = "config_alert_settings";
    internal const string NotificationTable = "config_notification";

    /// <summary>The delivery cooldown's stored column name — the wire alias as well as the column, which is
    /// the whole point of keeping it: <c>delivery.cooldown_minutes</c> is what the tool reports, and this is
    /// what every existing config file, Settings window and hand-written UPDATE already calls it.</summary>
    internal const string DeliveryCooldownColumn = "email_cooldown_minutes";

    private static string? Trimmed(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    /// <summary>A small {status, message} envelope for a non-data write outcome (invalid / not_found / unavailable)
    /// — the same shape <see cref="DarlingMcpCustomViewTools"/> uses, so an MCP client can branch on the outcome
    /// kind. A successful update/create/delete returns its own data-bearing shape, not this.</summary>
    private static string Outcome(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);

    /// <summary>
    /// Parses a PARTIAL alert-settings update — the nested JSON shape get_alert_settings RETURNS, carrying only the
    /// fields to change — into an ordered list of (column, bound parameter) pairs, validating each provided field
    /// against the SAME ranges/enums the Viewer's Settings window enforces
    /// (<c>SettingsWindow.BuildAlertRowFromControls</c>). Returns a non-null <c>Error</c> — and the caller writes
    /// nothing — on the FIRST bad value or unknown field (top-level or nested). Column names are this method's
    /// compile-time constants (never the input), so interpolating them into the UPDATE's SET list is injection-safe.
    ///
    /// <para><c>Warnings</c> (#3653) is the third outcome: a field that IS written but that the caller should
    /// know is inert. Today that is <c>poison_wait.threshold_ms</c> alone — stored, reported, consulted by
    /// nothing since #3593. Refusing it would break the round-trip this tool's description tells the caller
    /// to perform (hand the whole read payload back); silently storing it would let an operator believe they
    /// had tuned an alert. Accept, store, and say so, on the success envelope where the caller is looking.</para>
    /// </summary>
    private static (List<UpdateTarget> Updates, string? Error, List<string> Warnings) BuildAlertSettingsUpdate(JsonObject body)
    {
        var updates = new List<UpdateTarget>();
        var warnings = new List<string>();
        string? error = null;

        void AddBool(string column, JsonNode? node, string field)
        {
            if (error != null) return;
            if (node is JsonValue v && v.TryGetValue<bool>(out var b))
                updates.Add(new UpdateTarget(AlertSettingsTable, column, field, new NpgsqlParameter<bool> { TypedValue = b }));
            else
                error = $"'{field}' must be true or false.";
        }

        /* `table` defaults to the settings row because all but one column lives there; the delivery
           cooldown passes NotificationTable. Routing rather than a second parser so every field still
           validates through one set of adders and one first-error rule. */
        void AddInt(string column, JsonNode? node, string field, int min, int max, string table = AlertSettingsTable)
        {
            if (error != null) return;
            if (node is JsonValue v && v.TryGetValue<int>(out var i))
            {
                if (i < min || i > max)
                    error = $"'{field}' must be an integer between {min} and {max}.";
                else
                    updates.Add(new UpdateTarget(table, column, field, new NpgsqlParameter<int> { TypedValue = i }));
            }
            else
            {
                error = $"'{field}' must be an integer between {min} and {max}.";
            }
        }

        /* ag_redo_queue_alert_kb is the one bigint on this row, and DarlingAlertSettings clamps it in
           long arithmetic. Binding it through AddInt would work today only because the ceiling happens to
           fit in an int; typing it here means a raised ceiling stays writable instead of silently
           rejecting every value above int.MaxValue as "not an integer". */
        void AddLong(string column, JsonNode? node, string field, long min, long max)
        {
            if (error != null) return;
            if (node is JsonValue v && v.TryGetValue<long>(out var l))
            {
                if (l < min || l > max)
                    error = $"'{field}' must be an integer between {min} and {max}.";
                else
                    updates.Add(new UpdateTarget(AlertSettingsTable, column, field, new NpgsqlParameter<long> { TypedValue = l }));
            }
            else
            {
                error = $"'{field}' must be an integer between {min} and {max}.";
            }
        }

        void AddDouble(string column, JsonNode? node, string field, double min, double max)
        {
            if (error != null) return;
            if (node is JsonValue v && v.TryGetValue<double>(out var d))
            {
                if (d < min || d > max)
                    error = $"'{field}' must be a number between {min.ToString("0.0", CultureInfo.InvariantCulture)} and {max.ToString("0.0", CultureInfo.InvariantCulture)}.";
                else
                    updates.Add(new UpdateTarget(AlertSettingsTable, column, field, new NpgsqlParameter<double> { TypedValue = d }));
            }
            else
            {
                error = $"'{field}' must be a number between {min.ToString("0.0", CultureInfo.InvariantCulture)} and {max.ToString("0.0", CultureInfo.InvariantCulture)}.";
            }
        }

        void AddEnum(string column, JsonNode? node, string field, params string[] allowed)
        {
            if (error != null) return;
            if (node is JsonValue v && v.TryGetValue<string>(out var s))
            {
                var match = allowed.FirstOrDefault(a => string.Equals(a, s, StringComparison.OrdinalIgnoreCase));
                if (match == null)
                    error = $"'{field}' must be one of: {string.Join(", ", allowed)}.";
                else
                    updates.Add(new UpdateTarget(AlertSettingsTable, column, field, new NpgsqlParameter<string> { TypedValue = match }));
            }
            else
            {
                error = $"'{field}' must be one of: {string.Join(", ", allowed)}.";
            }
        }

        void AddStringArray(string column, JsonNode? node, string field, Func<IEnumerable<string>, IReadOnlyList<string>>? normalize = null)
        {
            if (error != null) return;
            if (node is JsonArray arr)
            {
                var array = new List<string>();
                foreach (var el in arr)
                {
                    if (el is JsonValue ev && ev.TryGetValue<string>(out var s))
                    {
                        array.Add(s);
                    }
                    else
                    {
                        error = $"'{field}' must be an array of strings.";
                        return;
                    }
                }

                /* #3653 (A5, Q5): an optional per-field normaliser, so a list whose meaning has a stated canonical
                   form (the Long-Running Query opt-out patterns) is stored in that form; excluded_databases keeps
                   its raw write, as it always has. */
                var stored = normalize is null ? array.ToArray() : normalize(array).ToArray();
                updates.Add(new UpdateTarget(AlertSettingsTable, column, field,
                    new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = stored }));
            }
            else
            {
                error = $"'{field}' must be an array of strings.";
            }
        }

        /* #3712 (V137): the route knob's TRI-STATE column, the one nullable text on this row. 'digest' / 'page'
           (case-insensitive) is stored in the canonical lower-case wire spelling (FindingRouting.RouteText) --
           the form the V137 CHECK admits and the form every reader compares against, so a 'Page' typed here
           is not a value no reader wrote. An explicit JSON null is a VALUE, not an omission: it writes SQL
           NULL, which is "hand the decision back to darling.json's analysis.uncorroboratedRoute" -- the third
           state, and the only way to reach it once a route has been stored. Anything else is refused by the
           tool's ordinary invalid arm (400-class on the web surface, #3739) with the three accepted spellings
           named: a route has no clamp, so there is no "nearest valid value" to store instead, and the CHECK
           would have refused it at the store anyway -- better to say so here than to surface a 23514. */
        void AddNullableRoute(string column, JsonNode? node, string field)
        {
            if (error != null) return;
            if (node is null)
            {
                updates.Add(new UpdateTarget(AlertSettingsTable, column, field,
                    new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value }));
                return;
            }

            if (node is JsonValue v && v.TryGetValue<string>(out var text) && FindingRouting.TryParseRoute(text) is { } route)
            {
                updates.Add(new UpdateTarget(AlertSettingsTable, column, field, new NpgsqlParameter<string> { TypedValue = FindingRouting.RouteText(route) }));
                return;
            }

            error = $"'{field}' must be '{FindingRouting.DigestText}', '{FindingRouting.PageText}', or null (null clears the store column so darling.json's analysis.uncorroboratedRoute governs again).";
        }

        /* #3712: the route knob's provenance echo. get_alert_settings emits the EFFECTIVE route under
           analysis.uncorroborated_route and WHICH home decided it under analysis.uncorroborated_route_source, and
           this tool's own description tells a caller to hand the whole read payload back -- so a round-trip
           that changed some other knob also hands back a route the FILE had been supplying, and writing it
           makes the store govern from then on. The value does not change; the provenance does, and a later
           edit to darling.json would then do nothing. That is stated on the success envelope where the caller
           is looking, ONLY when the echoed source says the file or the default had been deciding -- a partial
           update naming the route alone is a deliberate write and gets no such note. The two locals are read
           after the loop, because the echo may arrive before or after the route in document order. */
        string? routeSourceEcho = null;
        var routeWritten = false;

        /* Descends one nested group (e.g. "cpu": {...}); an unknown key inside it is rejected like an unknown
           top-level field, so the whole write is refused rather than silently ignoring a typo'd field. */
        void Group(JsonNode? node, string group, Action<string, JsonNode?> handleKey)
        {
            if (error != null) return;
            if (node is not JsonObject obj)
            {
                error = $"'{group}' must be an object.";
                return;
            }

            foreach (var kv in obj)
            {
                if (error != null) return;
                handleKey(kv.Key, kv.Value);
            }
        }

        foreach (var prop in body)
        {
            if (error != null) break;
            switch (prop.Key)
            {
                case "alerts_enabled": AddBool("enabled", prop.Value, "alerts_enabled"); break;
                case "notify_connection_changes": AddBool("notify_connection_changes", prop.Value, "notify_connection_changes"); break;
                /* #2417: bounds mirror DarlingAlertSettings' clamps EXACTLY -- Clamp(0, 1440) on the
                   refire, nothing to clamp on the at-startup opt-in. Zero is IN range because 0 is the
                   shipped configuration (one alert per outage, no re-fire), not an invalid one. */
                case "notify_connection_down_at_startup": AddBool("notify_connection_down_at_startup", prop.Value, "notify_connection_down_at_startup"); break;
                case "connection_refire_minutes": AddInt("connection_refire_minutes", prop.Value, "connection_refire_minutes", 0, 1440); break;
                case "cooldown_minutes": AddInt("cooldown_minutes", prop.Value, "cooldown_minutes", 1, 120); break;
                /* #3314: the STORED column name, accepted as an alias for delivery.cooldown_minutes so an
                   existing darling.json key, a hand-written UPDATE, or Lite's settings.json spelling keeps
                   working against the tool. Not emitted by get_alert_settings -- two wire keys for one
                   column is its own bug for a client, so the alias is write-only and the canonical name is
                   the only one that round-trips. Sending both in one body is refused below rather than
                   letting one silently win. */
                case DeliveryCooldownColumn: AddInt(DeliveryCooldownColumn, prop.Value, DeliveryCooldownColumn, 1, 120, NotificationTable); break;
                case "excluded_databases": AddStringArray("excluded_databases", prop.Value, "excluded_databases"); break;

                case "cpu":
                    Group(prop.Value, "cpu", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("cpu_enabled", n, "cpu.enabled"); break;
                            case "threshold_percent": AddInt("cpu_threshold_percent", n, "cpu.threshold_percent", 1, 100); break;
                            case "mode": AddEnum("cpu_mode", n, "cpu.mode", "sql", "total"); break;
                            default: error = $"Unknown field 'cpu.{k}'."; break;
                        }
                    });
                    break;

                case "blocking":
                    Group(prop.Value, "blocking", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("blocking_enabled", n, "blocking.enabled"); break;
                            /* #3528: the floor is the named constant rather than the literal 1 it always
                               was, because the read side now clamps to it — the same structural parity the
                               pg twin below has held since V122. */
                            case "count_threshold": AddInt("blocking_count_threshold", n, "blocking.count_threshold", PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue); break;
                            /* #2417: get_alert_settings has emitted this key since #1839 and the writer
                               never took it, so handing a whole read payload back -- the round trip this
                               tool's own description tells the caller to perform -- was rejected with
                               "Unknown field 'blocking.wait_threshold_seconds'", naming a field the caller
                               did not choose to send. The bound is the engine's Math.Max(0, ...), so 0
                               keeps disabling the second gate rather than becoming invalid. */
                            case "wait_threshold_seconds": AddInt("blocking_wait_seconds_threshold", n, "blocking.wait_threshold_seconds", 0, int.MaxValue); break;
                            /* #3444 (V122): the PostgreSQL count gate. The floor is the SAME named
                               constant DarlingAlertSettings clamps to, not a retyped 1, so this writer
                               cannot ACCEPT a value the read-side clamp then rewrites. The twin above
                               holds the identical bound-and-clamp pair since #3528. */
                            case "pg_count_threshold": AddInt("pg_blocking_count_threshold", n, "blocking.pg_count_threshold", PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue); break;
                            default: error = $"Unknown field 'blocking.{k}'."; break;
                        }
                    });
                    break;

                case "deadlocks":
                    Group(prop.Value, "deadlocks", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("deadlock_enabled", n, "deadlocks.enabled"); break;
                            /* #3528: the named constant for its blocking sibling's reason. */
                            case "count_threshold": AddInt("deadlock_count_threshold", n, "deadlocks.count_threshold", PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue); break;
                            /* #3444 (V122): the PostgreSQL count gate — same bound sourcing as its
                               blocking sibling. */
                            case "pg_count_threshold": AddInt("pg_deadlock_count_threshold", n, "deadlocks.pg_count_threshold", PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue); break;
                            default: error = $"Unknown field 'deadlocks.{k}'."; break;
                        }
                    });
                    break;

                /* #3368 (V120): the deadlock HEALTH BAND tiers. Bounds are the SAME named constants
                   DeadlockRateThresholds clamps to, not a retyped pair, so update_alert_settings cannot
                   ACCEPT a value the clamp then rewrites — the "setting did not stick" failure. The floor is
                   what keeps the knob a rate: see the constants for why one per hour is the tightest
                   threshold that cannot reach a bare-count band. */
                case "health_bands":
                    Group(prop.Value, "health_bands", (k, n) =>
                    {
                        switch (k)
                        {
                            case "deadlock_warn_per_hour":
                                AddDouble("deadlock_warn_per_hour", n, "health_bands.deadlock_warn_per_hour",
                                    ServerHealthThresholds.DeadlockRatePerHourFloor,
                                    ServerHealthThresholds.DeadlockRatePerHourCeiling);
                                break;
                            case "deadlock_critical_per_hour":
                                AddDouble("deadlock_critical_per_hour", n, "health_bands.deadlock_critical_per_hour",
                                    ServerHealthThresholds.DeadlockRatePerHourFloor,
                                    ServerHealthThresholds.DeadlockRatePerHourCeiling);
                                break;
                            default: error = $"Unknown field 'health_bands.{k}'."; break;
                        }
                    });
                    break;

                case "poison_wait":
                    Group(prop.Value, "poison_wait", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("poison_wait_enabled", n, "poison_wait.enabled"); break;
                            /* #3653: accepted and stored under the same bounds it always had, then WARNED about
                               — nothing has read this column since #3593 (see PoisonWaitThresholdMsNote). The
                               bound check still runs first, so a value that was never valid is still refused as
                               invalid rather than stored with a warning. */
                            case "threshold_ms":
                                AddInt("poison_wait_threshold_ms", n, "poison_wait.threshold_ms", 1, int.MaxValue);
                                if (error is null)
                                {
                                    warnings.Add("poison_wait.threshold_ms was stored but is " + PoisonWaitThresholdMsNote + ".");
                                }
                                break;
                            /* #3653: the note get_alert_settings emits beside threshold_ms, handed back by a
                               caller who round-tripped the whole payload. Not a setting, so no column and no
                               warning — it is our own text coming home, and refusing it would fail the
                               read → write invariant EveryColumnRead_IsEmittedByThePayload_AndAcceptedByTheWriter
                               holds. */
                            case "threshold_ms_note": break;
                            default: error = $"Unknown field 'poison_wait.{k}'."; break;
                        }
                    });
                    break;

                case "long_running_query":
                    Group(prop.Value, "long_running_query", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("long_running_query_enabled", n, "long_running_query.enabled"); break;
                            case "threshold_minutes": AddInt("long_running_query_threshold_minutes", n, "long_running_query.threshold_minutes", 1, int.MaxValue); break;
                            case "max_results": AddInt("long_running_query_max_results", n, "long_running_query.max_results", 1, 1000); break;
                            case "exclude_sp_server_diagnostics": AddBool("long_running_query_exclude_sp_server_diagnostics", n, "long_running_query.exclude_sp_server_diagnostics"); break;
                            case "exclude_wait_for": AddBool("long_running_query_exclude_wait_for", n, "long_running_query.exclude_wait_for"); break;
                            case "exclude_backups": AddBool("long_running_query_exclude_backups", n, "long_running_query.exclude_backups"); break;
                            case "exclude_misc_waits": AddBool("long_running_query_exclude_misc_waits", n, "long_running_query.exclude_misc_waits"); break;
                            case "exclude_cdc": AddBool("long_running_query_exclude_cdc", n, "long_running_query.exclude_cdc"); break;
                            /* #3653 (A5, Q5): normalised through the shared rule on the way in (trim, blanks dropped,
                               case-insensitive dedupe) -- the same treatment the Viewer's Settings
                               window and the service's seed give the lists, so what the store holds is what the
                               engine applies and what the next get_alert_settings reports. */
                            case "excluded_program_name_prefixes": AddStringArray("long_running_query_excluded_program_name_prefixes", n, "long_running_query.excluded_program_name_prefixes", LongRunningQueryExclusions.Normalize); break;
                            case "excluded_logins": AddStringArray("long_running_query_excluded_logins", n, "long_running_query.excluded_logins", LongRunningQueryExclusions.Normalize); break;
                            default: error = $"Unknown field 'long_running_query.{k}'."; break;
                        }
                    });
                    break;

                case "tempdb_space":
                    Group(prop.Value, "tempdb_space", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("tempdb_space_enabled", n, "tempdb_space.enabled"); break;
                            case "threshold_percent": AddInt("tempdb_space_threshold_percent", n, "tempdb_space.threshold_percent", 1, 100); break;
                            default: error = $"Unknown field 'tempdb_space.{k}'."; break;
                        }
                    });
                    break;

                case "low_disk":
                    Group(prop.Value, "low_disk", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("low_disk_enabled", n, "low_disk.enabled"); break;
                            case "threshold_percent": AddInt("low_disk_threshold_percent", n, "low_disk.threshold_percent", 0, 100); break;
                            case "threshold_gb": AddInt("low_disk_threshold_gb", n, "low_disk.threshold_gb", 0, int.MaxValue); break;
                            /* #2107: the CRITICAL tier floors, clamped like the warning thresholds. */
                            case "critical_free_percent": AddInt("disk_critical_free_percent", n, "low_disk.critical_free_percent", 0, 100); break;
                            case "critical_free_gb": AddInt("disk_critical_free_gb", n, "low_disk.critical_free_gb", 0, int.MaxValue); break;
                            default: error = $"Unknown field 'low_disk.{k}'."; break;
                        }
                    });
                    break;

                case "self_alerts":
                    /* #2107: the monitor's own store-volume and collection-health thresholds. The
                       clamps match DarlingAlertSettings' read-side clamps, so a value stored here is
                       the value the sweep uses. */
                    Group(prop.Value, "self_alerts", (k, n) =>
                    {
                        switch (k)
                        {
                            case "disk_free_warn_percent": AddInt("self_disk_free_warn_percent", n, "self_alerts.disk_free_warn_percent", 0, 100); break;
                            /* #3528: bound mirrors DarlingAlertSettings' Math.Max(0, ...) — 0 is IN range
                               because it removes the floor (the pvs.floor_gb reading), not nonsense. */
                            case "disk_free_warn_gb": AddInt("self_disk_free_warn_gb", n, "self_alerts.disk_free_warn_gb", 0, int.MaxValue); break;
                            case "collection_stale_minutes": AddInt("collection_stale_minutes", n, "self_alerts.collection_stale_minutes", 5, 1440); break;
                            case "collection_failure_threshold": AddInt("collection_failure_threshold", n, "self_alerts.collection_failure_threshold", 1, 1000); break;
                            case "store_job_cadence_warn_percent": AddInt("store_job_cadence_warn_percent", n, "self_alerts.store_job_cadence_warn_percent", 5, 100); break;
                            /* #3297: bounds are the SAME named constants DarlingAlertSettings clamps to, not
                               a retyped pair -- the file-growth note above gives the reason, and here the
                               constants make it structural rather than a matching literal. The floor is the
                               shipped warning default, so these knobs RAISE the tiers and cannot lower them:
                               healthy chunk granularity reaches 1.4x measured on production, so anything at
                               or below ~1.5x fires on a store that is working correctly, and the band above
                               that up to 2.0x is margin nobody has measured. See the constants for the rest,
                               including why the ceiling exists rather than leaving the knob open. */
                            case "retention_hold_warn_ratio":
                                AddDouble("retention_hold_warn_ratio", n, "self_alerts.retention_hold_warn_ratio",
                                    TimescaleSupport.RetentionHoldRatioFloor, TimescaleSupport.RetentionHoldRatioCeiling);
                                break;
                            case "retention_hold_critical_ratio":
                                AddDouble("retention_hold_critical_ratio", n, "self_alerts.retention_hold_critical_ratio",
                                    TimescaleSupport.RetentionHoldRatioFloor, TimescaleSupport.RetentionHoldRatioCeiling);
                                break;
                            default: error = $"Unknown field 'self_alerts.{k}'."; break;
                        }
                    });
                    break;

                case "pvs":
                    Group(prop.Value, "pvs", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("pvs_enabled", n, "pvs.enabled"); break;
                            case "threshold_percent": AddInt("pvs_threshold_percent", n, "pvs.threshold_percent", 0, 100); break;
                            case "floor_gb": AddInt("pvs_floor_gb", n, "pvs.floor_gb", 0, int.MaxValue); break;
                            default: error = $"Unknown field 'pvs.{k}'."; break;
                        }
                    });
                    break;

                /* #2391: bounds mirror DarlingAlertSettings' clamps EXACTLY — Max(0) on the rise,
                   [0,100] on the volume percent, [5,1440] on the lookback. If these drift apart the tool
                   accepts a value the engine then silently rewrites, which reads as the setting not
                   sticking. Zero on either gate disables that gate rather than being invalid (#2349),
                   which is why the rise floor is 0 and not 1. rise_mb is MB per HOUR (#3539 A8c); the
                   column takes the same integer it always did, and the engine scales it to the window. */
                case "file_growth":
                    Group(prop.Value, "file_growth", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("file_growth_enabled", n, "file_growth.enabled"); break;
                            case "rise_mb": AddInt("file_growth_rise_mb", n, "file_growth.rise_mb", 0, int.MaxValue); break;
                            case "volume_percent": AddInt("file_growth_volume_percent", n, "file_growth.volume_percent", 0, 100); break;
                            case "lookback_minutes": AddInt("file_growth_lookback_minutes", n, "file_growth.lookback_minutes", 5, 1440); break;
                            default: error = $"Unknown field 'file_growth.{k}'."; break;
                        }
                    });
                    break;

                case "long_running_job":
                    Group(prop.Value, "long_running_job", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("long_running_job_enabled", n, "long_running_job.enabled"); break;
                            case "multiplier": AddInt("long_running_job_multiplier", n, "long_running_job.multiplier", 2, 20); break;
                            default: error = $"Unknown field 'long_running_job.{k}'."; break;
                        }
                    });
                    break;

                case "failed_job":
                    Group(prop.Value, "failed_job", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("failed_job_enabled", n, "failed_job.enabled"); break;
                            case "lookback_minutes": AddInt("failed_job_lookback_minutes", n, "failed_job.lookback_minutes", 1, 1440); break;
                            default: error = $"Unknown field 'failed_job.{k}'."; break;
                        }
                    });
                    break;

                case "database_state":
                    Group(prop.Value, "database_state", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("database_state_enabled", n, "database_state.enabled"); break;
                            default: error = $"Unknown field 'database_state.{k}'."; break;
                        }
                    });
                    break;

                /* #2417: bounds mirror DarlingAlertSettings' clamps EXACTLY -- Clamp(lag, 0, 86400),
                   Clamp(redo, 0L, 1073741824L), Clamp(refire, 0, 1440). Zero is IN range on all three
                   and means OFF for that gate (the redo queue SHIPS at 0, because a healthy queue size
                   is workload-specific), so a floor of 1 would remove the shipped configuration. */
                case "ag":
                    Group(prop.Value, "ag", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("notify_ag_health", n, "ag.enabled"); break;
                            case "lag_threshold_seconds": AddInt("ag_lag_alert_seconds", n, "ag.lag_threshold_seconds", 0, 86400); break;
                            case "redo_queue_threshold_kb": AddLong("ag_redo_queue_alert_kb", n, "ag.redo_queue_threshold_kb", 0L, 1073741824L); break;
                            case "disconnect_refire_minutes": AddInt("ag_disconnect_refire_minutes", n, "ag.disconnect_refire_minutes", 0, 1440); break;
                            default: error = $"Unknown field 'ag.{k}'."; break;
                        }
                    });
                    break;

                case "delivery":
                    Group(prop.Value, "delivery", (k, n) =>
                    {
                        switch (k)
                        {
                            case "mode": AddEnum("delivery_mode", n, "delivery.mode", "Summary", "PerEvent"); break;
                            case "per_event_max": AddInt("per_event_max", n, "delivery.per_event_max", 1, 100); break;
                            /* #3314. The bound is DarlingAlertSettings' Clamp(..., 1, 120) EXACTLY -- the
                               same parity the file-growth and AG bounds hold, and for the same reason: a
                               wider bound here would ACCEPT a value the engine then silently rewrites on
                               read, which presents to the operator as the setting not sticking. Raising the
                               ceiling is therefore an engine change across both SKUs, not a bound edit; see
                               the PR for why long suppression belongs to create_mute_rule instead. */
                            case "cooldown_minutes": AddInt(DeliveryCooldownColumn, n, "delivery.cooldown_minutes", 1, 120, NotificationTable); break;
                            default: error = $"Unknown field 'delivery.{k}'."; break;
                        }
                    });
                    break;

                case "analysis":
                    Group(prop.Value, "analysis", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("analysis_enabled", n, "analysis.enabled"); break;
                            case "interval_minutes": AddInt("analysis_interval_minutes", n, "analysis.interval_minutes", 5, 360); break;
                            case "notifications_enabled": AddBool("analysis_notifications_enabled", n, "analysis.notifications_enabled"); break;
                            case "notify_severity": AddDouble("analysis_notify_severity", n, "analysis.notify_severity", 0.0, 2.0); break;
                            /* #2107: the clamp matches the shared engine's documented [30, 10080]. */
                            case "notify_cooldown_minutes": AddInt("analysis_notify_cooldown_minutes", n, "analysis.notify_cooldown_minutes", 30, 10080); break;
                            /* #3712 (V137): the route knob's store half -- 'digest' / 'page' write the column, null clears
                               it back to "the file governs". Before the rung this arm refused a change by name with the
                               file key; the store column is now the writable home and the file is what NULL defers to. */
                            case "uncorroborated_route":
                                AddNullableRoute("analysis_uncorroborated_route", n, "analysis.uncorroborated_route");
                                routeWritten = error is null;
                                break;
                            /* Read-only companions coming home from a get_alert_settings round-trip: the note is the
                               tool's own text; the source is the reader's provenance label, remembered so the
                               post-loop warning can say when a write moved the decision off the file. Neither claims
                               a column and neither is validated -- our own text is not the caller's input. */
                            case "uncorroborated_route_source":
                                routeSourceEcho = n is JsonValue sv && sv.TryGetValue<string>(out var sourceText) ? sourceText : null;
                                break;
                            case "uncorroborated_route_note": break;
                            default: error = $"Unknown field 'analysis.{k}'."; break;
                        }
                    });
                    break;

                /* #3466 (V124): bounds are FleetSweepCadence's named constants — the same figures the
                   worker clamps to on read and the Viewer's save gate enforces — so no value this tool
                   accepts is a value another surface then silently rewrites, the "setting did not
                   stick" parity every knob group above holds. The bounds' own reasoning (a sweep span
                   must hold enough samples to band on; a sweep rarer than daily starves the lane-4
                   rollup) lives on the constants. */
                case "fleet_sweep":
                    Group(prop.Value, "fleet_sweep", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("fleet_sweep_enabled", n, "fleet_sweep.enabled"); break;
                            case "interval_minutes":
                                AddInt("fleet_sweep_interval_minutes", n, "fleet_sweep.interval_minutes",
                                    FleetSweepCadence.IntervalMinutesFloor, FleetSweepCadence.IntervalMinutesCeiling);
                                break;
                            default: error = $"Unknown field 'fleet_sweep.{k}'."; break;
                        }
                    });
                    break;

                default:
                    error = $"Unknown field '{prop.Key}'. Send fields in the nested shape get_alert_settings returns.";
                    break;
            }
        }

        /* #3712: the provenance note described above the locals -- the route was written AND the payload's own
           echo said the file (or the shipped default) had been deciding, so this write moved the decision into
           the store. Not an error (the write is exactly what the caller sent) and not a refusal (the round-trip
           this tool's description prescribes must not fail on a field the caller did not choose); a sentence
           on the success envelope saying what moved and how to move it back. */
        if (error == null && routeWritten
            && (string.Equals(routeSourceEcho, DarlingAlertSettings.RouteSourceFile, StringComparison.Ordinal)
                || string.Equals(routeSourceEcho, DarlingAlertSettings.RouteSourceDefault, StringComparison.Ordinal)))
        {
            warnings.Add("analysis.uncorroborated_route was written to the store, which now governs it; the payload's uncorroborated_route_source said '"
                + routeSourceEcho + "' had been deciding, so darling.json's analysis.uncorroboratedRoute no longer applies until the column is cleared "
                + "(send analysis.uncorroborated_route: null).");
        }

        /* Two accepted keys claiming ONE column. The only pair that can do this today is
           delivery.cooldown_minutes and its email_cooldown_minutes alias, and both landing in one SET list
           is a Postgres error (multiple assignments to the same column) -- so refusing it here names the two
           spellings instead of surfacing a dialect message, and the caller learns which key to drop. Checked
           over (table, column) because the two planes are written by separate statements. */
        if (error == null)
        {
            var clash = updates
                .GroupBy(u => (u.Table, u.Column))
                .FirstOrDefault(g => g.Count() > 1);
            if (clash != null)
            {
                error = $"'{string.Join("' and '", clash.Select(u => u.Field))}' are two names for the same setting "
                    + $"({clash.Key.Table}.{clash.Key.Column}); send only one of them.";
            }
        }

        return (updates, error, warnings);
    }

    /// <summary>One validated field of a partial update: the TABLE it writes, the column, the wire field
    /// name it arrived under (for the two-names-one-column message), and the bound parameter.
    ///
    /// <para>The table is carried per field because <c>update_alert_settings</c> spans two config tables
    /// (#3314): all but one column is on the singleton <c>config_alert_settings</c> row, and the delivery
    /// cooldown is on the singleton <c>config_notification</c> row. Carrying it beats inferring it from the
    /// column name — an inference that would be correct today and silently wrong the first time a second
    /// notification knob arrives.</para></summary>
    private sealed record UpdateTarget(string Table, string Column, string Field, NpgsqlParameter Param);
}
