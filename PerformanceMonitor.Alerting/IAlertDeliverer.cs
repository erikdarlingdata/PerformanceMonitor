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
/// <param name="DetailText">
/// The alert's flat plain-text detail, or null. For an engine alert this is a rendering of
/// <paramref name="Context"/> (<see cref="AlertContextBuilders.ContextToDetailText"/>), but it is NOT only
/// that: a self-alert fires with <c>Context: null</c> and carries independent prose here — what happened,
/// what it costs, and the operator action that clears it — which is the ONLY place that text exists. Every
/// delivery channel renders it (#3297), suppressed on the engine case so the same content is not printed
/// twice.
/// </param>
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

    /// <summary>
    /// <see cref="DeliverAsync"/>, and then SAYS what the channels did (#3580): the
    /// <see cref="AlertDelivery"/> the deliverer recorded on the alert's history row, or <c>null</c> when it
    /// has no single answer to give.
    ///
    /// <para><b>Why a second method rather than a return value on the first.</b> The never-throws contract
    /// above is deliberate and stays: a channel fault is the deliverer's to record, not the engine's to
    /// handle, and every condition-class alert wants exactly that. The two DOCUMENT-class self-alerts (the
    /// collector-cost digest and the fleet-sweep rollup) are the exception, because their once-a-day gate
    /// has to answer "was one delivered today" and not "did this process fire one today" — on the v3.8.0
    /// install night three service restarts re-announced both documents on every store, six re-posts among
    /// ~23 channel posts, while the one pair whose delivery had genuinely FAILED was correctly re-attempted
    /// after the restart. Distinguishing those two cases needs the disposition at the fire site, and
    /// nothing else on the engine's side does; so the report rides a separate method that the two askers
    /// call and every other caller ignores.</para>
    ///
    /// <para><b>Required, not defaulted — CONTRIBUTING's Two-Store Parity rule, which names this interface.</b>
    /// A default body here would have compiled, and would have left Lite's deliverer and fourteen test fakes
    /// quietly inheriting an answer nobody wrote down. So every implementer states its answer: Darling's
    /// deliverer reports the disposition its history row was written with; Lite's, whose send seam returns
    /// no disposition and which hosts neither daily document, returns <c>null</c> by hand and says why; each
    /// fake does the same. <c>null</c> means "unreported", never "failed" — the askers treat it the way every
    /// fire before #3580 was treated, as delivered — and a deliverer that KNOWS a send failed reports
    /// <see cref="AlertDelivery.ChannelFailed"/>, the one disposition the askers withhold their
    /// delivered-today stamp on.</para>
    /// </summary>
    Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default);
}
