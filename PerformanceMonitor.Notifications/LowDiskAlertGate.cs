/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Decides whether a low-disk ("Volume Free Space") alert should (re-)fire for a server, so a
/// standing breach does not re-notify — and re-record an alert-history row — every cooldown.
/// <para>
/// A full volume is a sustained condition (disk does not free itself), so a plain per-cooldown
/// level check fires the alert every <c>AlertCooldownMinutes</c> for as long as the breach lasts.
/// Besides the repeated tray/email, that wrote a fresh alert-history row every cycle, which made
/// Alert-History "Dismiss" feel broken: each dismissed row was immediately replaced by an
/// identical, newer one. This mirrors the failed-job watermark (<c>_lastAlertedFailedJobTime</c>)
/// and the rolling-count edge trigger (<c>RollingCountAlertGate</c>, #1091): notify on a NEW or
/// WORSENING breach, stay quiet at an unchanged level. Shared by Lite and Dashboard so the rule
/// cannot drift between the two apps.
/// </para>
/// </summary>
public static class LowDiskAlertGate
{
    /// <summary>
    /// Minimum drop, in free-space percentage points below the last-alerted level, required to
    /// re-alert. Keeps normal free-space jitter (logs / tempdb growing and shrinking a fraction
    /// of a percent) from re-tripping the alert while still catching a genuine decline.
    /// </summary>
    public const double DefaultWorseningMarginPercent = 1.0;

    /// <summary>
    /// Free-space percentage at or below which a breach is "critically low" (#1136) — a second,
    /// lower tier beneath the user-configured fire threshold. Below this the database can no longer
    /// grow data/log files, so transactions fail and the database can go into recovery/suspect; that
    /// warrants CRITICAL, not the WARNING the normal breach renders.
    /// </summary>
    public const double CriticalFreePercent = 3.0;

    /// <summary>
    /// Free-space GB at or below which a breach is "critically low" regardless of percentage — a
    /// large volume sitting at a few GB free has no room for a single autogrow. See
    /// <see cref="CriticalFreePercent"/>.
    /// </summary>
    public const double CriticalFreeGb = 2.0;

    /// <summary>
    /// True when the worst breached volume is critically low on EITHER dimension (mirrors the OR
    /// semantics of the breach test itself): free space at/below <see cref="CriticalFreePercent"/>
    /// or at/below <see cref="CriticalFreeGb"/>. Drives the CRITICAL severity tier (#1136). Shared
    /// by Lite and Dashboard so the two apps grade low-disk identically.
    /// </summary>
    public static bool IsCriticallyLow(double freePercent, double freeGb) =>
        IsCriticallyLow(freePercent, freeGb, CriticalFreePercent, CriticalFreeGb);

    /// <summary>#2107: the configurable form — both apps pass their settings' critical floors; the
    /// parameterless overload keeps the shipped constants for callers with no settings in reach
    /// (and for the tests pinning the defaults).</summary>
    public static bool IsCriticallyLow(double freePercent, double freeGb, double criticalFreePercent, double criticalFreeGb) =>
        freePercent <= criticalFreePercent || freeGb <= criticalFreeGb;

    /// <summary>
    /// Returns true when a low-disk alert should fire this cycle.
    /// </summary>
    /// <param name="currentWorstFreePercent">
    /// Free-space percentage of the worst (lowest-free) breached volume this cycle.
    /// </param>
    /// <param name="lastAlertedFreePercent">
    /// Free percentage captured when the alert last fired for this server, or <c>null</c> when
    /// there is no active low-disk alert (a fresh breach — the caller clears its watermark when
    /// the condition resolves).
    /// </param>
    /// <param name="worseningMarginPercent">
    /// Required worsening, in percentage points; defaults to <see cref="DefaultWorseningMarginPercent"/>.
    /// </param>
    public static bool ShouldAlert(
        double currentWorstFreePercent,
        double? lastAlertedFreePercent,
        double worseningMarginPercent = DefaultWorseningMarginPercent)
    {
        /* Fresh breach (no active alert) always notifies. */
        if (lastAlertedFreePercent is null)
        {
            return true;
        }

        /* Otherwise only when free space has fallen at least the margin below the last-alerted
           level — i.e. the breach got meaningfully worse. */
        return currentWorstFreePercent <= lastAlertedFreePercent.Value - worseningMarginPercent;
    }
}
