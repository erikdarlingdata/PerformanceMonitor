/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one definition of "has this standing condition gone unannounced long enough to say it again" — the
/// #1659 / #1674 re-fire window, shared by <see cref="ConnectionAlertPolicy"/> and
/// <see cref="AgAlertPolicy"/>.
///
/// <para>Three lines, and extracted anyway because it had already been written twice and #2426 was about to
/// write it a third time. The two rules worth having in one place are the ones that are easy to get subtly
/// wrong from memory: a non-positive interval means OFF rather than "every sweep", and a NULL last-alert
/// stamp means DUE NOW rather than "never announced, so stay quiet". The second is the one that carries a
/// standing outage across a restart — an app that re-baselines its edge state on launch has no stamp for a
/// condition that was already true, and treating that as not-due would silence exactly the week-long outage
/// re-firing exists to keep announcing.</para>
///
/// <para>Pure: the caller passes its own clock and owns the stamp. The stamp belongs on DELIVERY, never on
/// the decision — an alert a master switch or an acknowledgement suppressed must not consume the window it
/// was never announced in.</para>
/// </summary>
public static class AlertRefireWindow
{
    /// <param name="interval">The configured re-fire interval. Null or non-positive = off.</param>
    /// <param name="lastAlertUtc">When this condition was last ANNOUNCED, or null if it never has been.</param>
    /// <param name="nowUtc">The caller's clock, injected so the decision pins under test.</param>
    public static bool IsDue(TimeSpan? interval, DateTime? lastAlertUtc, DateTime nowUtc) =>
        interval is TimeSpan window
        && window > TimeSpan.Zero
        && (lastAlertUtc is not DateTime last || nowUtc - last >= window);
}
