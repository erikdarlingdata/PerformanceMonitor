using System;
using System.Globalization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The max server memory and MAXDOP sentences the configuration audit and the max-memory fact card share, so the
/// Darling tool, the Lite tool and <see cref="FactAdvice"/> state one figure and one basis (#7400). Before this, three
/// copies computed <c>total - max(4096, 10% of total)</c> with no floor, so a server under 4 GB suggested a negative cap.
/// </summary>
public static class ConfigAuditGuidance
{
    /// <summary>The OS reserve floor in MB: 4 GB.</summary>
    public const double OsReserveFloorMb = 4096;

    /// <summary>
    /// What to leave for the OS and everything else on the box: the larger of 4,096 MB and 10% of RAM, but never more
    /// than half of RAM, so a small server still gets a positive suggestion. Unchanged at 8 GB and above, where half of
    /// RAM is already at least 4,096 MB. Zero when the RAM is unknown.
    /// </summary>
    public static double OsReserveMb(double totalMb)
    {
        if (totalMb <= 0)
            return 0;
        return Math.Min(Math.Max(OsReserveFloorMb, totalMb * 0.10), totalMb / 2);
    }

    /// <summary>Total RAM minus <see cref="OsReserveMb"/>, in whole MB (truncated); never negative. Zero when the RAM is unknown.</summary>
    public static int SuggestedMaxMemoryMb(double totalMb) =>
        totalMb <= 0 ? 0 : (int)(totalMb - OsReserveMb(totalMb));

    /// <summary>The rule in words, for a server whose RAM was not collected.</summary>
    public const string ReserveRule =
        "the larger of 4 GB and 10% of RAM, but never more than half of RAM";

    /// <summary>One max server memory audit row: the suggested value, its status and the sentence.</summary>
    public readonly record struct MaxMemoryAudit(int Suggested, string Status, string Message);

    /// <summary>
    /// Grades max server memory against physical RAM (<paramref name="totalMb"/>, 0 when unknown). Same statuses and
    /// thresholds the audit always used, with the suggestion from <see cref="SuggestedMaxMemoryMb"/> and the reserve the
    /// text states being the one the suggestion used. A server already at or under the suggestion cannot reach the
    /// "reduce" branch: that needs more than 95% of RAM, and the suggestion is at most half of it.
    /// </summary>
    public static MaxMemoryAudit AuditMaxMemory(int maxMemory, double totalMb)
    {
        var inv = CultureInfo.CurrentCulture;
        if (maxMemory == FactRemediation.UnconfiguredMaxMemoryMb)
        {
            if (totalMb > 0)
            {
                var reserve = OsReserveMb(totalMb);
                var suggested = SuggestedMaxMemoryMb(totalMb);
                return new MaxMemoryAudit(suggested, "warning",
                    $"Max server memory is at the default (unlimited). SQL Server will consume all available RAM, " +
                    $"starving the OS and other processes. With {totalMb.ToString("N0", inv)} MB physical RAM, set max server memory to " +
                    $"~{suggested.ToString("N0", inv)} MB (leaving {reserve.ToString("N0", inv)} MB for the OS).");
            }
            return new MaxMemoryAudit(maxMemory, "warning",
                "Max server memory is at the default (unlimited). SQL Server will consume all available RAM. " +
                $"Set this to total physical memory minus the OS reserve ({ReserveRule}) to leave room for the OS.");
        }

        if (totalMb <= 0)
            return new MaxMemoryAudit(maxMemory, "ok", $"Max server memory is set to {maxMemory.ToString("N0", inv)} MB.");

        var ratio = maxMemory / totalMb;
        var cap = SuggestedMaxMemoryMb(totalMb);
        if (ratio > 0.95)
        {
            return new MaxMemoryAudit(cap, "review",
                $"Max server memory ({maxMemory.ToString("N0", inv)} MB) is {ratio.ToString("P0", inv)} of physical RAM ({totalMb.ToString("N0", inv)} MB). " +
                $"Consider reducing to ~{cap.ToString("N0", inv)} MB to leave room for the OS.");
        }
        if (ratio < 0.50 && totalMb > 8192)
        {
            return new MaxMemoryAudit(cap, "review",
                $"Max server memory ({maxMemory.ToString("N0", inv)} MB) is only {ratio.ToString("P0", inv)} of physical RAM ({totalMb.ToString("N0", inv)} MB). " +
                $"SQL Server may be under-utilizing available memory. Consider raising to ~{cap.ToString("N0", inv)} MB unless other " +
                "applications need the remaining RAM.");
        }
        return new MaxMemoryAudit(maxMemory, "ok",
            $"Max server memory ({maxMemory.ToString("N0", inv)} MB) looks reasonable for {totalMb.ToString("N0", inv)} MB physical RAM.");
    }

    /// <summary>What the recommended MAXDOP was taken from, as the audit words it: "this database's vCores" or "this server's cores-per-socket".</summary>
    public static string MaxdopSource(FactRemediation.MaxdopBasis basis) =>
        basis.FromVcores ? "this database's vCores" : "this server's cores-per-socket";

    /// <summary>The MAXDOP 0 warning: unlimited parallelism, and the figure to start with and where it came from.</summary>
    public static string MaxdopUnlimitedMessage(long recommended, FactRemediation.MaxdopBasis basis) =>
        $"MAXDOP is 0 (unlimited). This lets one query fan out across all schedulers, " +
        $"leading to CXPACKET waits and thread exhaustion under load. Microsoft's guidance is " +
        $"topology-based: keep MAXDOP at or under the logical processors in a single NUMA node, capped at 8. " +
        $"Start with {recommended} ({MaxdopSource(basis)}, capped at 8) and adjust to the workload.";

    /// <summary>The MAXDOP above-guidance review note. It names the figure it used (<see cref="MaxdopSource"/>), the same as <see cref="MaxdopUnlimitedMessage"/>.</summary>
    public static string MaxdopAboveMessage(int maxdop, long recommended, FactRemediation.MaxdopBasis basis) =>
        $"MAXDOP {maxdop} is above the topology-based guidance of {recommended} " +
        $"({MaxdopSource(basis)}, capped at 8). Review whether queries here genuinely " +
        $"benefit from the higher degree, or lower it to {recommended}.";
}
