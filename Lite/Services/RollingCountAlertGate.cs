/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Services
{
    /// <summary>
    /// Edge-trigger gate for the overview alert engine's rolling-window count alerts —
    /// blocking and deadlocks (#1091).
    ///
    /// Those counts are rolling 1-hour totals: a single blocked-process report or deadlock
    /// keeps the count at/above the threshold for the whole hour it lingers in the window. A
    /// plain level check (<c>count &gt;= threshold</c>) therefore re-fires the SAME event every
    /// cooldown until it ages out — the bug reported in #1091, where two deadlocks produced an
    /// alert every five minutes for an hour.
    ///
    /// This gate is edge-triggered instead. It fires only when the current count has climbed
    /// above the watermark recorded at the last fired alert (a genuinely new event). The
    /// watermark:
    /// <list type="bullet">
    /// <item>decays down to the current count as older events age out of the window, so a later
    ///   rise is still recognised as new;</item>
    /// <item>resets to zero when the count falls below the threshold (the window emptied), so the
    ///   next event re-alerts; and</item>
    /// <item>advances ONLY when an alert actually fires — never while the cooldown or a
    ///   suppression is gating the notification — so an event that arrives during the cooldown is
    ///   reported on the next eligible check instead of being silently swallowed.</item>
    /// </list>
    ///
    /// Pure function: no clock, no I/O. The caller resolves <paramref name="cooldownElapsed"/>
    /// and <paramref name="suppressed"/> and persists <see cref="Decision.Watermark"/>.
    /// </summary>
    public static class RollingCountAlertGate
    {
        /// <param name="Fire">True when the caller should raise the alert this cycle (tray/email).</param>
        /// <param name="Active">
        /// True while the count is at/above the threshold. Drives the "Cleared" notification on the
        /// active→inactive transition; independent of whether a notification fired this cycle.
        /// </param>
        /// <param name="Watermark">The watermark to persist for the next evaluation.</param>
        public readonly record struct Decision(bool Fire, bool Active, int Watermark);

        /// <summary>
        /// Evaluates the edge-trigger gate for one alert check.
        /// </summary>
        /// <param name="currentCount">Current rolling-window count (raw or database-filtered).</param>
        /// <param name="threshold">Alert threshold; the alert is "active" at or above this count.</param>
        /// <param name="watermark">Watermark persisted from the previous evaluation (0 if none).</param>
        /// <param name="cooldownElapsed">True if the cooldown since the last fired alert has elapsed.</param>
        /// <param name="suppressed">True if popups/emails are suppressed for this server (acknowledged/silenced).</param>
        public static Decision Evaluate(
            int currentCount,
            int threshold,
            int watermark,
            bool cooldownElapsed,
            bool suppressed)
        {
            // Below threshold: the window has emptied (or never filled). Not active; reset the
            // watermark so the next event re-alerts from a clean slate.
            if (currentCount < threshold)
            {
                return new Decision(Fire: false, Active: false, Watermark: 0);
            }

            // Decay the watermark down to the current count as older events age out of the
            // window, so a subsequent rise is still recognised as a new event.
            if (watermark > currentCount)
            {
                watermark = currentCount;
            }

            bool hasNew = currentCount > watermark;
            bool fire = hasNew && cooldownElapsed && !suppressed;

            // Advance the watermark only when we actually fire; otherwise a new event arriving
            // during the cooldown would be consumed when the cooldown later elapses.
            if (fire)
            {
                watermark = currentCount;
            }

            return new Decision(Fire: fire, Active: true, Watermark: watermark);
        }
    }
}
