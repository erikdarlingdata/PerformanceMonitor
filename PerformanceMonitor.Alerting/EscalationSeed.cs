// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One question the engine asks the alert history once per server key per process (#5715 part B): what tier did the
/// newest fire of <see cref="MetricName"/> reach at or after <see cref="SinceUtc"/>, unless a
/// <see cref="ResolutionTitle"/> row closed it since.
/// </summary>
/// <param name="ServerKey">The engine's server key, which is the history row's server id.</param>
/// <param name="MetricName">The fire's metric name ("High CPU").</param>
/// <param name="ResolutionTitle">The resolution row's metric name ("CPU Resolved"): resolution rows are written under
/// their title, not under the fire's metric name.</param>
/// <param name="SinceUtc">The start of the family's cooldown window. Older than this the bypass changes nothing.</param>
/// <param name="CountMuted">False for the one family whose fire site does not record a muted fire's tier.</param>
public sealed record EscalationSeedQuery(
    string ServerKey, string MetricName, string ResolutionTitle, DateTime SinceUtc, bool CountMuted);

/// <summary>
/// The resolution titles of the six alerts that opt into <see cref="TierEscalation"/>, as constants so the engine's
/// <c>AlertResolution</c> sites and the history read below name the same string (a resolution row is stored under its
/// title), and the host-side reader that answers an <see cref="EscalationSeedQuery"/> from an
/// <see cref="IAlertHistoryStore"/>, so Lite and Darling wire it identically.
/// </summary>
public static class EscalationSeed
{
    public const string CpuResolvedTitle = "CPU Resolved";
    public const string DeadlocksClearedTitle = "Deadlocks Cleared";
    public const string PoisonWaitsClearedTitle = "Poison Waits Cleared";
    public const string RunnableQueueClearedTitle = "Runnable Queue Cleared";
    public const string ClientTimeoutsClearedTitle = "Client Timeouts Cleared";
    public const string VolumeFreeSpaceResolvedTitle = "Volume Free Space Resolved";

    /// <summary>
    /// Answers an <see cref="EscalationSeedQuery"/> from <paramref name="store"/>. The store's read throws on a failure,
    /// and this lets it: the engine counts that as a read failure and retries it on the next sweep.
    /// </summary>
    public static Func<EscalationSeedQuery, CancellationToken, Task<AlertSeverityLevel?>> ReaderFor(IAlertHistoryStore store)
    {
        if (store is null)
        {
            throw new ArgumentNullException(nameof(store));
        }

        /* A fire row with no persisted tier means what the history grids make of it (AlertHistoryRowSeverity's name arm):
           Volume Free Space writes a tier only for a Critical fire, so its tierless row is a Warning. */
        return (query, _) => store.GetLastFiredTierAsync(
            query.ServerKey, query.MetricName, query.ResolutionTitle, query.SinceUtc, query.CountMuted,
            PerformanceMonitor.Common.AlertMetricClassifier.IsCritical(query.MetricName)
                ? AlertSeverityLevel.Critical
                : AlertSeverityLevel.Warning);
    }
}
