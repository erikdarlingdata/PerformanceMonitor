/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Decides when the Dashboard alert engine should fire the "Deadlocks Cleared" notification
    /// (#1091).
    ///
    /// Deadlock detection is edge-triggered off a delta against a cumulative perfmon counter, so
    /// the very next check after a deadlock sees a zero delta and the alert is no longer
    /// "exceeded". Clearing on that transition makes the alert flap: a "Deadlocks Cleared" balloon
    /// fires one check interval (~60s) after every "Deadlock Detected".
    ///
    /// Instead, we keep the alert active and clear only once a deadlock-quiet window has elapsed
    /// since the last NEW deadlock — matching Lite, whose rolling 1-hour count drains about an hour
    /// after the last deadlock. Each new deadlock resets the window.
    /// </summary>
    public static class DeadlockAlertClearPolicy
    {
        /// <summary>
        /// True when the "Deadlocks Cleared" notification should fire this check.
        /// </summary>
        /// <param name="wasActive">Whether a deadlock alert is currently active for the server.</param>
        /// <param name="lastDeadlockActivity">Time of the last new deadlock, or null if not tracked.</param>
        /// <param name="now">Current evaluation time.</param>
        /// <param name="quietWindow">How long the alert stays active after the last new deadlock.</param>
        public static bool ShouldClear(bool wasActive, DateTime? lastDeadlockActivity, DateTime now, TimeSpan quietWindow)
        {
            if (!wasActive)
            {
                return false;
            }

            // No recorded activity (e.g. state seeded without a timestamp): nothing holding it
            // active, so clearing is safe.
            if (lastDeadlockActivity is null)
            {
                return true;
            }

            return (now - lastDeadlockActivity.Value) >= quietWindow;
        }
    }
}
