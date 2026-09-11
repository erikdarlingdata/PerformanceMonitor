/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One fired alert as the Phase-5 shared engine emits it — everything a host needs to record it to
/// alert history and send it out (tray/email/webhook). The member shape mirrors the argument list
/// both apps already pass to <c>EmailAlertService.TrySendAlertEmailAsync</c> / <c>RecordAlert</c>,
/// so the per-app deliverers are thin adapters.
/// </summary>
/// <param name="ServerKey">The engine's stable identity for the server (see <see cref="IAlertStateStore"/>).</param>
/// <param name="ServerName">Display name rendered in notifications.</param>
/// <param name="MetricName">The alert metric name (e.g. "High CPU", "Deadlocks Detected") — the key mute rules, severity, and history filtering match on.</param>
/// <param name="CurrentValue">Human-readable current value (e.g. "3 deadlock(s) in the last hour").</param>
/// <param name="ThresholdValue">Human-readable threshold it breached (e.g. "1", "80%").</param>
/// <param name="Context">Structured detail context (built by <see cref="AlertContextBuilders"/>), or null when no detail was resolvable.</param>
/// <param name="DetailText">Flat plain-text rendering of <paramref name="Context"/> (the engine emits <see cref="AlertContextBuilders.ContextToDetailText"/> output), or null.</param>
/// <param name="NumericCurrentValue">Numeric current value for history charting/thresholding, when the metric has one.</param>
/// <param name="NumericThresholdValue">Numeric threshold twin of <paramref name="NumericCurrentValue"/>.</param>
/// <param name="Muted">True when a mute rule matched — the host records the alert but must not toast/send it.</param>
/// <param name="Severity">Runtime severity override (e.g. low-disk grading WARNING vs CRITICAL, #1136), or null to use the per-metric severity map.</param>
/// <param name="ShortMessage">
/// One-line human-readable summary of the fired condition WITHOUT the server-name prefix —
/// verbatim the per-metric tray-toast body Lite's pre-forwarding loop composed (e.g.
/// "Total CPU at 92% (threshold: 80%)", "Session #55 running 45m — SELECT ..."). Composed by the
/// engine because some bodies need per-row data (worst session id / query preview / job minutes)
/// that the other display fields don't carry; interactive hosts render it as
/// <c>$"{ServerName}: {ShortMessage}"</c>, headless hosts may log or ignore it.
/// </param>
/// <param name="DisplayName">
/// Human-facing name rendered in notification titles/subjects INSTEAD of <paramref name="MetricName"/>,
/// or null/empty for built-in alerts — which keep rendering <paramref name="MetricName"/> unchanged
/// (byte-identical to before this field existed). Custom alert rules key history / mute / cooldown on the
/// immutable <paramref name="MetricName"/> (<c>"Custom:&lt;id&gt;"</c>, rename-safe) but set this to the
/// rule's (newline-stripped, length-capped) name so a human sees the name, not <c>Custom:42</c>. Every
/// render site falls back to <paramref name="MetricName"/> whenever this is null or empty; the metric name
/// stays the severity / cooldown / dedup key everywhere.
/// </param>
public sealed record AlertOutcome(
    string ServerKey,
    string ServerName,
    string MetricName,
    string CurrentValue,
    string ThresholdValue,
    AlertContext? Context,
    string? DetailText,
    double? NumericCurrentValue,
    double? NumericThresholdValue,
    bool Muted,
    AlertSeverityLevel? Severity,
    string? ShortMessage = null,
    string? DisplayName = null);

/// <summary>
/// The record-and-send seam for the Phase-5 shared alert engine: the engine evaluates conditions
/// and calls this once per fired alert; everything channel- and store-specific happens behind it.
/// One of the three engine seams (with <see cref="IAlertEngineSettings"/> and
/// <see cref="IAlertStateStore"/>), consumed by the headless Darling alert engine first; Lite
/// forwards later; Dashboard convergence is a separately-decided migration.
/// <para>
/// History-row SEMANTICS deliberately live behind this seam, per app: Lite writes ONE combined
/// history row per fired alert (including muted ones, flagged muted) as a side effect of its send
/// path; the Dashboard records explicitly (its <c>RecordAlert</c>), can write MULTIPLE rows per
/// condition, and also records "Cleared/Resolved" rows when a condition recovers. The engine does
/// not know or care — it reports outcomes; the deliverer decides what a history row is. Delivery
/// mode fan-out (#1141 Summary vs Per-event splitting) is also an implementation concern here, not
/// an engine one.
/// </para>
/// </summary>
public interface IAlertDeliverer
{
    /// <summary>
    /// Records and (unless <see cref="AlertOutcome.Muted"/>) sends one fired alert. Implementations
    /// must not throw for channel failures — a dead SMTP server must not abort the engine's sweep.
    /// </summary>
    Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default);
}
