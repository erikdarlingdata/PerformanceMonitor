using System;
using System.Globalization;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What to tell the user, and what to write to the log, when a store write gave up waiting for the process-wide
/// lock (#5650). Kept out of the WPF handlers so the wording can be tested.
///
/// <para>The old dialog said "archival or maintenance in progress" for every timeout, whatever held the lock. This
/// says only what the code knows: the database was busy and the action did not happen. It names a holder only when
/// the lock registry recorded one (<see cref="StoreLockTimeoutException.Holders"/>), and calls it the startup repair
/// only when that is the holder's name.</para>
/// </summary>
internal static class StoreBusyReport
{
    /// <summary>
    /// The dialog for a refused write. <paramref name="actionDone"/> is the clause about what did not happen,
    /// for example "nothing was dismissed".
    /// </summary>
    internal static string DialogText(TimeoutException ex, string actionDone = "nothing was dismissed")
    {
        var cause = ex is StoreLockTimeoutException typed && typed.Holders.Count > 0
            ? $" It was in use by {FriendlyName(typed.Holders[0])}."
            : string.Empty;
        return $"The local database was busy, so {actionDone}.{cause}\n\nPlease try again in a few minutes.";
    }

    /// <summary>
    /// The one sentence every user-started write other than Dismiss shows when the lock wait ran out: a status bar
    /// line or a message box body. <paramref name="doing"/> names the action ("renaming the tag"), and
    /// <paramref name="outcome"/> says what the user can rely on, "nothing was changed" unless a caller knows that
    /// part of a multi-step write had already gone through. The holder is named under the same rule as
    /// <see cref="DialogText"/>.
    /// </summary>
    internal static string Text(TimeoutException ex, string doing, string outcome = "nothing was changed")
    {
        var cause = ex is StoreLockTimeoutException typed && typed.Holders.Count > 0
            ? $" It was in use by {FriendlyName(typed.Holders[0])}."
            : string.Empty;
        return $"The local database was busy while {doing}, so {outcome}.{cause} Please try again in a few minutes.";
    }

    /// <summary>
    /// The shared catch body for those writes: one Warning line to the log (like Dismiss), and the text to show.
    /// A timeout is an expected outcome of colliding with archival or a collector write, not a fault, so it is
    /// Warn and not Error.
    /// </summary>
    internal static string Report(string logArea, string logAction, TimeoutException ex, string doing, string outcome = "nothing was changed")
    {
        AppLogger.Warn(logArea, LogLine(logAction, ex, outcome));
        return Text(ex, doing, outcome);
    }

    /// <summary>
    /// What a surface shows while a user write waits for the lock, so the wait is not mistaken for a hang. The
    /// 30 s is the shipped <see cref="LocalDataService.UserWriteLockBudget"/>.
    /// </summary>
    internal static string WaitingText(string doing)
    {
        var seconds = (int)LocalDataService.UserWriteLockBudget.TotalSeconds;
        return string.Create(CultureInfo.InvariantCulture,
            $"{char.ToUpperInvariant(doing[0])}{doing[1..]}... waiting up to {seconds} seconds if the local database is busy.");
    }

    /// <summary>
    /// One Warning line for the log: the action, how long it waited, who held the lock, and the same
    /// <paramref name="outcome"/> the user was told, so a write that had saved part of its work does not log "nothing was changed".
    /// </summary>
    internal static string LogLine(string action, TimeoutException ex, string outcome = "nothing was changed")
    {
        if (ex is not StoreLockTimeoutException typed)
        {
            return $"{action}: gave up waiting for the local database write lock (holder unknown); {outcome}. {ex.Message}";
        }

        var who = typed.Holders.Count == 0 ? "holder unknown" : StoreLockTimeoutException.FormatHolders(typed.Holders);
        return string.Create(CultureInfo.InvariantCulture,
            $"{action}: gave up after waiting {typed.Waited.TotalSeconds:0.#} s for the local database write lock; {outcome}. {who}");
    }

    /// <summary>Plain wording for the callers that are well known; any other holder is shown by its code name.</summary>
    private static string FriendlyName(StoreLockHolder holder)
    {
        var held = string.Create(CultureInfo.InvariantCulture, $"{holder.Held.TotalSeconds:0} seconds");
        if (holder.Name.StartsWith("QueryStoreSliceRepairService.", StringComparison.Ordinal))
        {
            return $"the startup Query Store repair (for {held})";
        }

        if (holder.Name.StartsWith("ArchiveService.", StringComparison.Ordinal))
        {
            return $"the archive job ({holder.Name}, for {held})";
        }

        return $"{holder.Name} (for {held})";
    }
}
