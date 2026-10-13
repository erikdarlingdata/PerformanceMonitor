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
/// newest fire of <see cref="MetricName"/> reach in [<see cref="SinceUtc"/>, <see cref="UntilUtc"/>), unless a
/// <see cref="ResolutionTitle"/> row closed it since.
/// </summary>
/// <param name="ServerKey">The engine's server key, which is the history row's server id.</param>
/// <param name="MetricName">The fire's metric name ("High CPU").</param>
/// <param name="ResolutionTitle">The resolution row's metric name ("CPU Resolved"): resolution rows are written under
/// their title, not under the fire's metric name.</param>
/// <param name="SinceUtc">The start of the family's cooldown window. Older than this the bypass changes nothing.</param>
/// <param name="UntilUtc">EXCLUSIVE upper bound: the engine's start instant. A row at or after it was written by this
/// process, whose in-memory tier already covers it (round 1 of #5715).</param>
/// <param name="CountMuted">False for the one family whose fire site does not record a muted fire's tier.</param>
/// <param name="TierlessRowIsWarning">True for the one family whose Warning fire rows are tierless by design (Volume Free
/// Space persists a tier only for a Critical fire): a tierless row there means Warning. For every other family a
/// tierless row seeds nothing; the metric NAME no longer decides it (round 1 of #5715).</param>
public sealed record EscalationSeedQuery(
    string ServerKey, string MetricName, string ResolutionTitle, DateTime SinceUtc, DateTime UntilUtc, bool CountMuted,
    bool TierlessRowIsWarning);

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
    /// <para>The answer is the row's own tier, whatever it is. Only a Warning ever seeds, and the ENGINE drops anything
    /// else (<c>AlertEngine.EnsureEscalationSeededAsync</c>), so a reader that hands back a Critical cannot cancel the
    /// next incident's escalation.</para>
    /// </summary>
    public static Func<EscalationSeedQuery, CancellationToken, Task<AlertSeverityLevel?>> ReaderFor(IAlertHistoryStore store)
    {
        if (store is null)
        {
            throw new ArgumentNullException(nameof(store));
        }

        /* A fire row with no persisted tier seeds nothing, except in the family whose Warning rows are tierless by design
           (Volume Free Space writes a tier only for a Critical fire): the engine's family table says which, not the metric
           name. The name classifier called a tierless Deadlocks or Poison Wait row Critical (round 1 of #5715). */
        return (query, _) => store.GetLastFiredTierAsync(
            query.ServerKey, query.MetricName, query.ResolutionTitle, query.SinceUtc, query.CountMuted,
            query.TierlessRowIsWarning ? AlertSeverityLevel.Warning : null,
            query.UntilUtc);
    }
}
