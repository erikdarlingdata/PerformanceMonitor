// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The shared half of <see cref="IAlertHistoryStore.GetLastFiredTierAsync"/> (#5715 part B): the failed-send
/// predicate both stores spell in SQL, and the one place that turns "newest fire row" and "newest resolution row"
/// into the tier the engine seeds, so the DuckDB and PostgreSQL stores cannot answer the tie question differently.
/// </summary>
public static class AlertHistoryEscalationSeed
{
    /// <summary>
    /// SQL that keeps a row unless EVERY attempted channel failed: <c>alert_sent</c> false with a non-null
    /// <c>send_error</c>, exactly the engine's <c>FailedSendBackoff.EveryChannelFailed</c> test. In process, such a fire
    /// puts the tier back (<c>TierEscalation.Restore</c>), so the history must not seed it. A row with no send error
    /// and no delivery (unconfigured, throttled, muted, folded) was not a failed send and stays: the engine recorded its
    /// tier. A legacy <c>undelivered</c> row (a webhook that came back unsuccessful, with a null error) cannot be told
    /// apart from those and is counted. That is harmless: only a Warning seeds (the engine drops any other tier), so such a row can
    /// only make the next Critical an escalation, delivered sooner, never held.
    /// </summary>
    public const string KeepUnlessEveryChannelFailedFilter = "\nAND   (alert_sent OR send_error IS NULL)";

    /// <summary>SQL that drops muted rows, for a family whose engine fire site does not record a muted fire's tier.</summary>
    public const string ExcludeMutedFilter = "\nAND   NOT muted";

    /// <summary>
    /// Truncates <paramref name="value"/> to the microsecond both stores keep (a .NET tick is 100 ns and is not stored).
    /// Both window bounds go through it, so a bound and a stored row compare at one precision and the tie rule
    /// (<see cref="Resolve"/>) is decided by stored instants alone.
    /// </summary>
    public static DateTime TruncateToMicroseconds(DateTime value) => new(value.Ticks - (value.Ticks % 10), value.Kind);

    /// <summary>A fire is open unless a resolution row is as new as it or newer (a tie closes it). The one tie rule, shared by
    /// the tier seed above and <see cref="IAlertHistoryStore.GetOpenFireAsync"/> in both stores.</summary>
    public static bool IsOpen(DateTime fireAtUtc, DateTime? resolvedAtUtc) => resolvedAtUtc is not { } resolved || resolved < fireAtUtc;

    /// <summary>
    /// The tier to seed: the tier of the newest fire row (<paramref name="fireAtUtc"/>, read from its
    /// <paramref name="contextJson"/> the way <c>AlertHistoryRowSeverity.FiredAt</c> does), or null when there is no
    /// fire, the fire carries no tier, or a resolution row is as new as the fire or newer. At an exact tie the
    /// resolution wins: the incident closed no later than the fire, and a seed that revived it would bypass a cooldown
    /// for an incident that is over. Both values come back from the same column type, so equal stored instants compare
    /// equal, microsecond for microsecond.
    /// <para>A fire row can carry no tier: Volume Free Space persists one only when the fire is Critical, so its Warning
    /// rows have none. <paramref name="tierWhenRowCarriesNone"/> is what such a row means, the engine's family table
    /// passing Warning for that one family and null for every other (a tierless Deadlocks or Poison Wait row seeds
    /// nothing; the metric name no longer decides it).</para>
    /// </summary>
    public static AlertSeverityLevel? Resolve(
        DateTime? fireAtUtc, string? contextJson, DateTime? resolvedAtUtc, AlertSeverityLevel? tierWhenRowCarriesNone = null)
    {
        if (fireAtUtc is not { } fired)
        {
            return null;
        }

        if (!IsOpen(fired, resolvedAtUtc))
        {
            return null;
        }

        return AlertContextSerializer.TryReadSeverity(contextJson) ?? tierWhenRowCarriesNone;
    }
}
