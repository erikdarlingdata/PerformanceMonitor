using System;
using System.Globalization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Where the store's history for an analysis source starts, and what retention it is held to (#8910). The
/// window-empty answer uses it to tell a window that retention removed from a window that collection missed: the first
/// ends before <see cref="OldestRowUtc"/> and sits past <see cref="RetentionHorizonUtc"/>, the second sits inside the span.
/// One type for both products, so the two say it the same way.
/// </summary>
/// <param name="OldestRowUtc">The time of the oldest row the store holds for the source.</param>
/// <param name="RetentionHorizonUtc">The instant retention keeps rows back to (now less the retention in force).</param>
/// <param name="Source">The collector table the oldest row came from (wait_stats, or pg_database_stats on a PostgreSQL target).</param>
/// <param name="RetentionDescription">The retention in force, in words (for example "30 days of retention for wait_stats").</param>
public sealed record RetentionBoundary(DateTime OldestRowUtc, DateTime RetentionHorizonUtc, string Source, string RetentionDescription)
{
    /// <summary>
    /// How far past the retention horizon the oldest row may sit and still be put down to retention. A purge removes
    /// whole chunks or whole files, so the oldest row is rarely exactly at the horizon. A store whose first row is much
    /// newer than the horizon (a server monitored for five days, asked about ten days ago) never held the window, and
    /// retention did not remove it.
    /// </summary>
    public static readonly TimeSpan HorizonSlack = TimeSpan.FromDays(1);

    /// <summary>
    /// True when a window ending at <paramref name="windowEndUtc"/> ended before the oldest row the store keeps AND that row
    /// sits at the retention horizon, so the store held rows for it once and the purge removed them.
    /// </summary>
    public bool RemovedWindow(DateTime windowEndUtc) =>
        windowEndUtc < OldestRowUtc && OldestRowUtc <= RetentionHorizonUtc + HorizonSlack;

    /// <summary>The sentence that says retention removed the window.</summary>
    public string Describe() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"This window ends before the oldest row this store keeps ({OldestRowUtc:yyyy-MM-dd HH:mm} UTC, {RetentionDescription}). Retention removed it, and collection did not break.");
}
