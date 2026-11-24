using System;
using System.Globalization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Where the store's history for an analysis source starts, and what retention it is held to (#8910). The
/// window-empty answer uses it to tell a window that retention removed from a window that collection missed: the first
/// ends before <see cref="OldestRowUtc"/> and the store's purge has been running (the oldest row sits where a purge leaves
/// it), the second sits inside the span or was never held. One type for both products, so the two say it the same way.
/// </summary>
/// <param name="OldestRowUtc">The time of the oldest row the store holds for the source.</param>
/// <param name="RetentionHorizonUtc">The instant retention keeps rows back to (now less the retention in force).</param>
/// <param name="LatestOldestRowUtc">The latest instant the oldest surviving row can sit at after a purge, set by each product
/// from its own purge rule: the horizon plus a slack for a chunk purge (Darling), the first month start at or after the cutoff
/// plus a slack for the archive's whole-month file purge (Lite). An oldest row later than this means the store never held the
/// window, so retention did not remove it.</param>
/// <param name="EarliestOldestRowUtc">The earliest instant the oldest surviving row can sit at while the purge is running:
/// the horizon less what the purge leaves behind (its granularity and one purge interval). An oldest row earlier than this
/// means the purge has stalled, so the window before the first monitored time is not one retention removed either.</param>
/// <param name="Source">The collector table the oldest row came from (wait_stats, or pg_database_stats on a PostgreSQL target).</param>
/// <param name="RetentionDescription">The retention in force, in words (for example "30 days of retention for wait_stats").</param>
public sealed record RetentionBoundary(
    DateTime OldestRowUtc,
    DateTime RetentionHorizonUtc,
    DateTime LatestOldestRowUtc,
    DateTime EarliestOldestRowUtc,
    string Source,
    string RetentionDescription)
{
    /// <summary>
    /// The slack a product adds around the instant its purge rule gives. A purge removes whole chunks or whole files, so the
    /// oldest row is rarely exactly there, and a collector that was down for a few hours moves the first row later. A store
    /// whose first row is much newer than the retention horizon (a server monitored for five days, asked about ten days ago)
    /// never held the window, and retention did not remove it.
    /// </summary>
    public static readonly TimeSpan HorizonSlack = TimeSpan.FromDays(1);

    /// <summary>
    /// True when a window ending at <paramref name="windowEndUtc"/> ended before the oldest row the store keeps AND that row
    /// sits where a running purge leaves the oldest row (no later than <see cref="LatestOldestRowUtc"/>, no earlier than
    /// <see cref="EarliestOldestRowUtc"/>), so the store held rows for it once and the purge removed them. A purge that has
    /// stalled leaves the oldest row far before the horizon: the store may never have held the window, so it is not claimed.
    /// </summary>
    public bool RemovedWindow(DateTime windowEndUtc) =>
        windowEndUtc < OldestRowUtc && OldestRowUtc <= LatestOldestRowUtc && OldestRowUtc >= EarliestOldestRowUtc;

    /// <summary>The sentence that says retention removed the window.</summary>
    public string Describe() =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"This window ends before the oldest row this store keeps ({OldestRowUtc:yyyy-MM-dd HH:mm} UTC, {RetentionDescription}). Retention removed it, and collection did not break.");
}
