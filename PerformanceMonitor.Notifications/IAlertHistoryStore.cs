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
    /// The send_error clause is why <see cref="AlertDelivery.FromFanout"/> keeps a failed webhook's error
    /// off a row that delivered: an error borrowed from a sibling channel would take a successful email out
    /// of this seed and re-send it after a restart.
    /// </para>
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
    /// successful post), so send_error is NOT filtered on. It can hold a webhook channel's error
    /// (<see cref="AlertDelivery.ChannelFailed"/>), but only on a row that delivered nothing, and those
    /// rows carry neither of the notification_type values below.
    /// Lite: notification_type IN ('webhook','email+webhook'). Dash: NotificationType == "webhook".
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown), the result is
    /// additionally restricted to rows whose persisted <c>ContextJson</c> carries that #1140 dedup
    /// fingerprint. Null = the metric-level seed (the pre-#1154 behavior).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null);

    /// <summary>
    /// MAX(alert_time) over every row that was a PAGING candidate (any channel/result, muted or not) — the
    /// #2716 seed of Darling's Postgres Tier-0-predictor cooldowns (called with <paramref name="dedupKey"/>).
    /// Those live cooldowns are stamped UNCONDITIONALLY, even when muted or undelivered (a muted alert still
    /// consumes its cooldown, mirroring AlertEngine), so "any result" is the persisted equivalent THERE.
    /// <para>#3916: this is no longer the analysis seed. The analysis #2054 hold must be EARNED by a delivery,
    /// and a row that reached no one (a month-old undelivered <c>tray</c> row held one production story silent
    /// for 35 days) must not arm it — the analysis seed moved to <see cref="GetLastDeliveredPageUtcAsync"/>.
    /// Do not add <c>alert_sent</c> here: it would break the Tier-0 family's seed/stamp agreement.</para>
    /// <para>#3712: rows the corroboration gate routed to the digest (<c>notification_type =
    /// <see cref="AlertDelivery.ChannelDigest"/></c>) are EXCLUDED. A digest entry is not a page, and a
    /// seed that read one would hold the page a story earns when it later gains corroboration as a repeat
    /// of a page that never happened — design point 2 of #3712 makes that escalation a NEW firing. Before
    /// #3712 this read was unfiltered, and every row it could see was a paging candidate, so the exclusion
    /// changes nothing about the rows that existed then.</para>
    /// <para>
    /// When <paramref name="dedupKey"/> is non-null (#1154 per-fingerprint cooldown, reused by #2716 to
    /// seed Darling's Postgres Tier-0-predictor cooldowns), the result is additionally restricted to rows
    /// whose persisted <c>ContextJson</c> carries that #1140 dedup fingerprint. Null = the metric-level
    /// seed (the pre-#1154 behavior, used by the non-fingerprinted fallback).
    /// </para>
    /// </summary>
    Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null);

    /// <summary>
    /// MAX(alert_time) over the rows that were a DELIVERED page — <c>alert_sent</c> true, minus the #3712
    /// digest exclusion — for (serverId, metricName). Seeds the analysis per-finding #2054 hold across a
    /// restart (#3916): a hold must be earned by a delivery, so a row that reached no one (no channel
    /// configured, every channel failed, muted) seeds nothing and the story is heard on its next firing.
    /// <para>The digest exclusion is redundant by construction (a digest row is never Sent) and is kept so
    /// the exclusion stays one symbol and stays pinned.</para>
    /// <para>Dashboard's JSON store answers with ANY row: every Dashboard analysis page raises the tray
    /// balloon through the shared service's sink, so every page row there is a delivered page.</para>
    /// </summary>
    Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName);
}

/// <summary>
/// One alert event to persist. Carries both the display strings (Dashboard
/// persists these verbatim) and the optional resolved numerics (Lite persists
/// these into DOUBLE columns, falling back to parsing the display text).
///
/// <para>The delivery disposition arrives as one <see cref="AlertDelivery"/> rather than as a loose
/// <c>bool</c> + two strings. A producer cannot then state a send outcome without stating the channel it
/// belongs to, cannot transpose the two strings past the compiler, and — because
/// <see cref="AlertDelivery"/> has no public constructor — cannot hand-write a delivered row at all. The
/// three column-shaped members below are projections for the store writers, which are unchanged.</para>
/// </summary>
public sealed record AlertHistoryRecord(
    string  ServerId, string ServerName, string MetricName,
    string  CurrentValueText, string ThresholdValueText,    // Dashboard persists these
    double? NumericCurrentValue, double? NumericThresholdValue, // Lite persists these
    AlertDelivery Delivery,
    bool    Muted, string? DetailText, string? ContextJson)
{
    /// <summary>The <c>alert_sent</c> column. A delivery measurement on every row.</summary>
    public bool AlertSent => Delivery.Sent;

    /// <summary>The <c>notification_type</c> column — which channel the row is about.</summary>
    public string NotificationType => Delivery.Channel;

    /// <summary>The <c>send_error</c> column.</summary>
    public string? SendError => Delivery.SendError;
}
