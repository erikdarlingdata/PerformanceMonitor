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

    /// <summary>One Warning line for the log: the action, how long it waited, and who held the lock.</summary>
    internal static string LogLine(string action, TimeoutException ex)
    {
        if (ex is not StoreLockTimeoutException typed)
        {
            return $"{action}: gave up waiting for the local database write lock (holder unknown). {ex.Message}";
        }

        var who = typed.Holders.Count == 0 ? "holder unknown" : StoreLockTimeoutException.FormatHolders(typed.Holders);
        return string.Create(CultureInfo.InvariantCulture,
            $"{action}: gave up after waiting {typed.Waited.TotalSeconds:0.#} s for the local database write lock; nothing was changed. {who}");
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
