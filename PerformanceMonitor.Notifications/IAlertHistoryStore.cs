/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Persistence boundary for the alert/notification history that the alert
/// services write to and read their cooldown seeds from. Implemented per-app:
///   Lite      → DuckDB <c>config_alert_log</c> (real async I/O).
///   Dashboard → in-memory <c>List&lt;AlertLogEntry&gt;</c> persisted to
///               <c>alert_history.json</c> (the read scan is wrapped in
///               <see cref="Task.FromResult{TResult}(TResult)"/>).
/// All reads are async so the shared service body is identical across apps.
/// The Dashboard-only history-management surface (GetAlertHistory / Hide*) is
/// NOT on this interface — it lives on the Dashboard JSON store impl.
/// </summary>
public interface IAlertHistoryStore
{
    /// <summary>
    /// Persists one alert event. The record carries BOTH display strings and
    /// optional numerics so each impl persists its native shape without loss:
    ///   Lite  → INSERT into config_alert_log (numeric current/threshold → DOUBLE cols)
    ///   Dash  → new AlertLogEntry{...} added to in-memory list + trim to 1000
    /// The alert timestamp is stamped <see cref="DateTime.UtcNow"/> at record time.
    /// </summary>
    Task RecordAlertAsync(AlertHistoryRecord record);

    /// <summary>
    /// MAX(alert_time) filtered to a *successful email send* — seeds the email
    /// cooldown across restart (#981). Lite: notification_type IN
    /// ('email','email+webhook') AND send_error IS NULL. Dash: NotificationType
    /// == "email" AND SendError empty.
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown), the result is
    /// additionally restricted to rows whose persisted <c>ContextJson</c> carries that #1140 dedup
    /// fingerprint, so the seed reconstructs the per-incident last-sent time. Null = the metric-level
    /// seed (the pre-#1154 behavior, used by the non-fingerprinted fallback).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null);

    /// <summary>
    /// MAX(alert_time) filtered to a *successful webhook send* — seeds the webhook
    /// cooldown across restart so a Teams/Slack alert delivered shortly before a restart
    /// is not re-posted afterward (#1145, mirroring the email seed #981). The
    /// notification_type already implies the webhook delivered (it's only written on a
    /// successful post), and send_error tracks the EMAIL channel, so it is NOT filtered on.
    /// Lite: notification_type IN ('webhook','email+webhook'). Dash: NotificationType == "webhook".
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown), the result is
    /// additionally restricted to rows whose persisted <c>ContextJson</c> carries that #1140 dedup
    /// fingerprint. Null = the metric-level seed (the pre-#1154 behavior).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null);

    /// <summary>
    /// MAX(alert_time) UNFILTERED (any channel/result) — seeds the analysis
    /// per-finding cooldown across restart. Stamped unconditionally upstream.
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown, reused by #2716 to
    /// seed Darling's Postgres Tier-0-predictor cooldowns), the result is additionally restricted to rows
    /// whose persisted <c>ContextJson</c> carries that #1140 dedup fingerprint. Null = the metric-level
    /// seed (the pre-#1154 behavior, used by the non-fingerprinted fallback).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null);
}

/// <summary>
/// One alert event to persist. Carries both the display strings (Dashboard
/// persists these verbatim) and the optional resolved numerics (Lite persists
/// these into DOUBLE columns, falling back to parsing the display text).
/// </summary>
public sealed record AlertHistoryRecord(
    string  ServerId, string ServerName, string MetricName,
    string  CurrentValueText, string ThresholdValueText,    // Dashboard persists these
    double? NumericCurrentValue, double? NumericThresholdValue, // Lite persists these
    bool    AlertSent, string NotificationType, string? SendError,
    bool    Muted, string? DetailText, string? ContextJson);
