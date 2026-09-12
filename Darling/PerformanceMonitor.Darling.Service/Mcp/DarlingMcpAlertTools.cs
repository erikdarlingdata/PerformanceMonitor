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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The alerts MCP tools — get_alert_history, get_alert_settings, get_mute_rules plus the three alert-TUNING
/// write tools update_alert_settings / create_mute_rule / delete_mute_rule — served over Darling's Postgres
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
/// of scope — the mcp role cannot read or write them. create_mute_rule / delete_mute_rule reuse the SAME
/// <see cref="PgMuteRuleStore"/> get_mute_rules reads through, generating the rule id the same way the Viewer's
/// mute-create path does (a fresh GUID on a new <see cref="MuteRule"/>).</para>
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
    [McpServerTool(Name = "get_alert_history"), Description("Gets recent alert history from the alert log: what alerts fired, when, for which server, the current vs threshold value, whether email/webhook delivery succeeded, and whether the alert was muted. Omit server_name to see the whole fleet (each row names its server); pass one to scope to a single server.")]
    public static async Task<string> GetAlertHistory(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Omit to return alerts across all servers (the fleet default).")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
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
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
            if (error != null) return error;
            serverId = resolved.ServerId;
            scope = resolved.ServerName;
        }

        try
        {
            var since = windowEnd.AddHours(-hours_back);
            var rows = await DarlingAlertReader.GetAlertHistoryAsync(postgres, since, windowEnd, serverId, limit);
            if (rows.Count == 0)
                return McpHelpers.Status("empty", "No alerts found in the specified time range.");

            var alerts = rows.Select(r => new
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
                detail_text = r.DetailText
            });

            return JsonSerializer.Serialize(new
            {
                server = scope,
                hours_back,
                total_alerts = rows.Count,
                alerts
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_alert_history", ex);
        }
    }

    [McpServerTool(Name = "get_alert_settings"), Description("Gets the current alert configuration the service is using: which alerts are enabled and their thresholds (CPU, blocking, deadlocks, poison waits, long-running queries/jobs, tempdb, low disk, failed jobs, database state, Availability Group health, connection loss), the cooldown, excluded databases, the deadlock/blocking delivery mode and cooldown, and the scheduled-analysis cadence. TWO different cooldowns are reported and they govern different stages: top-level cooldown_minutes gates whether the alert engine FIRES at all, while delivery.cooldown_minutes is the per-alert-fingerprint throttle on the resulting Slack/Teams/PagerDuty/webhook/email post. A channel going quiet with alerts still in get_alert_history is delivery.cooldown_minutes, not cooldown_minutes. The self_alerts group holds the thresholds for alerts about the MONITOR STORE itself rather than a monitored server — those arrive with Server: 'Monitor Store', so an alert naming that is tuned here and nowhere else, including Retention Held's warn/critical ratios. SMTP/webhook delivery credentials are managed separately and are not reported here — configure them in the standalone Darling Viewer app's Settings window (Notifications section), which connects to this store (including remotely, not just localhost) rather than requiring desktop access to this specific box.")]
    public static async Task<string> GetAlertSettings(
        NpgsqlDataSource postgres)
    {
        try
        {
            /* ONE snapshot across both config tables. The delivery cooldown is the single value on
               config_notification rather than config_alert_settings, so reporting the configuration takes two
               SELECTs -- and two independent reads could straddle a concurrent update_alert_settings commit
               and report a mix of pre- and post-update state, which is the very thing the write path takes a
               transaction to avoid producing. See DarlingAlertReader.GetAlertConfigurationAsync. */
            var (s, deliveryCooldown) = await DarlingAlertReader.GetAlertConfigurationAsync(postgres);
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
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_alert_settings", ex);
        }
    }

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
            wait_threshold_seconds = s.BlockingWaitSecondsThreshold
        },
        deadlocks = new { enabled = s.DeadlockEnabled, count_threshold = s.DeadlockCountThreshold },
        poison_wait = new { enabled = s.PoisonWaitEnabled, threshold_ms = s.PoisonWaitThresholdMs },
        long_running_query = new
        {
            enabled = s.LongRunningQueryEnabled,
            threshold_minutes = s.LongRunningQueryThresholdMinutes,
            max_results = s.LongRunningQueryMaxResults,
            exclude_sp_server_diagnostics = s.LongRunningQueryExcludeSpServerDiagnostics,
            exclude_wait_for = s.LongRunningQueryExcludeWaitFor,
            exclude_backups = s.LongRunningQueryExcludeBackups,
            exclude_misc_waits = s.LongRunningQueryExcludeMiscWaits,
            exclude_cdc = s.LongRunningQueryExcludeCdc
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
            notify_cooldown_minutes = s.AnalysisNotifyCooldownMinutes
        }
    };

    [McpServerTool(Name = "get_mute_rules"), Description("Gets the configured alert mute rules. Mute rules suppress specific recurring alerts (by server, metric, database, query text, wait type, or job name) while still logging them — so an agent can tell a genuinely healthy-quiet server from one whose alerts are being suppressed.")]
    public static async Task<string> GetMuteRules(
        NpgsqlDataSource postgres,
        [Description("Include only enabled, non-expired rules. Default true.")] bool enabled_only = true)
    {
        try
        {
            var all = await new PgMuteRuleStore(postgres).LoadAllAsync();
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
        catch (Exception ex)
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
        "Tunes the alert engine's configuration — a PARTIAL update of the single global alert-settings row. Call " +
        "get_alert_settings FIRST, change only the fields you want, and pass THOSE fields back here as JSON in the " +
        "SAME nested shape get_alert_settings returns (e.g. {\"cpu\":{\"threshold_percent\":90},\"cooldown_minutes\":10}); " +
        "every setting you do NOT include is left unchanged. Each provided field is validated against the SAME rules " +
        "the Viewer's Settings window enforces — thresholds in range, cpu.mode 'sql'|'total', delivery.mode " +
        "'Summary'|'PerEvent', counts within their bounds; an out-of-range value or an unknown field returns " +
        "{status:\"invalid\", ...} and writes NOTHING. On success the running service hot-reloads the change within " +
        "one collection sweep. TWO cooldowns are writable and they are different stages: cooldown_minutes gates " +
        "the engine's FIRE decision, delivery.cooldown_minutes throttles the per-fingerprint post to " +
        "Slack/Teams/PagerDuty/webhook/email (its stored name, email_cooldown_minutes, is also accepted as a " +
        "top-level alias, but send only one of the two spellings). An alert whose Server reads 'Monitor Store' " +
        "is about the monitor's own store and is tuned under self_alerts — including Retention Held, whose " +
        "self_alerts.retention_hold_warn_ratio and self_alerts.retention_hold_critical_ratio are how many times " +
        "its configured horizon a held retention tier must be holding to warn and to go critical. Both accept " +
        "2.0 upward only: healthy whole-chunk granularity reaches 1.4x on a store that is working correctly, so " +
        "a lower threshold fires on nothing being wrong. Setting critical BELOW warn is accepted and means every " +
        "fire is Critical. For silencing ONE recurring signature for a " +
        "long stretch, use create_mute_rule instead of a long delivery cooldown: a mute is scoped, expires, is " +
        "listed by get_mute_rules, and still logs the alert, where the cooldown is global to every fingerprint " +
        "on every server and no tool reports what it suppressed. SMTP/webhook delivery credentials are managed " +
        "separately and cannot be set here " +
        "— configure them in the standalone Darling Viewer app's Settings window (Notifications section), which " +
        "connects to this store (including remotely, not just localhost) rather than requiring desktop access to " +
        "this specific box. " +
        "Returns {status:\"updated\", updated_fields:[...], settings:{...}} with the full new settings, or " +
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

            var (updates, error) = BuildAlertSettingsUpdate(body);
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
                settings = reread is null || rereadCooldown is null ? null : BuildAlertSettingsPayload(reread, rereadCooldown.Value)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_alert_settings", ex);
        }
    }

    [McpServerTool(Name = "create_mute_rule"), Description(
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

    [McpServerTool(Name = "delete_mute_rule"), Description(
        "Deletes an alert mute rule by its id (from get_mute_rules or create_mute_rule). Returns " +
        "{status:\"deleted\", rule_id} on success, or {status:\"not_found\"} when no rule has that id. Permanent. " +
        "The running service stops honoring the rule on its next collection sweep, when the delete's " +
        "config_version bump makes it reload its mute cache.")]
    public static async Task<string> DeleteMuteRule(
        NpgsqlDataSource postgres,
        [Description("The id of the mute rule to delete (from get_mute_rules or create_mute_rule).")] string rule_id)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(rule_id))
            {
                return Outcome("invalid", "rule_id is required.");
            }

            /* Existence check through the SAME store get_mute_rules reads, so 'deleted' vs 'not_found' is honest
               (PgMuteRuleStore.DeleteAsync is a bare DELETE that reports no row count). */
            var store = new PgMuteRuleStore(postgres);
            var rules = await store.LoadAllAsync();
            if (!rules.Any(r => string.Equals(r.Id, rule_id, StringComparison.Ordinal)))
            {
                return Outcome("not_found", $"No mute rule with id '{rule_id}'.");
            }

            await store.DeleteAsync(rule_id);
            return JsonSerializer.Serialize(new { status = "deleted", rule_id }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_mute_rule", ex);
        }
    }

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
    /// </summary>
    private static (List<UpdateTarget> Updates, string? Error) BuildAlertSettingsUpdate(JsonObject body)
    {
        var updates = new List<UpdateTarget>();
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

        void AddStringArray(string column, JsonNode? node, string field)
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

                updates.Add(new UpdateTarget(AlertSettingsTable, column, field,
                    new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = array.ToArray() }));
            }
            else
            {
                error = $"'{field}' must be an array of strings.";
            }
        }

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
                            case "count_threshold": AddInt("blocking_count_threshold", n, "blocking.count_threshold", 1, int.MaxValue); break;
                            /* #2417: get_alert_settings has emitted this key since #1839 and the writer
                               never took it, so handing a whole read payload back -- the round trip this
                               tool's own description tells the caller to perform -- was rejected with
                               "Unknown field 'blocking.wait_threshold_seconds'", naming a field the caller
                               did not choose to send. The bound is the engine's Math.Max(0, ...), so 0
                               keeps disabling the second gate rather than becoming invalid. */
                            case "wait_threshold_seconds": AddInt("blocking_wait_seconds_threshold", n, "blocking.wait_threshold_seconds", 0, int.MaxValue); break;
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
                            case "count_threshold": AddInt("deadlock_count_threshold", n, "deadlocks.count_threshold", 1, int.MaxValue); break;
                            default: error = $"Unknown field 'deadlocks.{k}'."; break;
                        }
                    });
                    break;

                case "poison_wait":
                    Group(prop.Value, "poison_wait", (k, n) =>
                    {
                        switch (k)
                        {
                            case "enabled": AddBool("poison_wait_enabled", n, "poison_wait.enabled"); break;
                            case "threshold_ms": AddInt("poison_wait_threshold_ms", n, "poison_wait.threshold_ms", 1, int.MaxValue); break;
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
                   which is why the rise floor is 0 and not 1. */
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
                            default: error = $"Unknown field 'analysis.{k}'."; break;
                        }
                    });
                    break;

                default:
                    error = $"Unknown field '{prop.Key}'. Send fields in the nested shape get_alert_settings returns.";
                    break;
            }
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

        return (updates, error);
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
