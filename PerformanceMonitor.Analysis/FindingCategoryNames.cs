using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The plain name a person reads for an analysis finding's category (#7486). A finding's category is the source of its root fact
/// (<see cref="FactScorer.KnownSources"/>), an internal word such as <c>bad_actor</c> that reads like a security event next to "One
/// expensive query dominated the server's work this window". The raw value stays in stored rows, the MCP reply, mute and alert-route
/// matching (<c>Analysis: &lt;category&gt;</c>) and CSV export, because tools and rules read it. Only text a person reads goes through
/// <see cref="DisplayName"/>. The web viewer keeps the same map in <c>wwwroot/js/finding-categories.js</c>, and a test holds the two equal.
/// </summary>
public static class FindingCategoryNames
{
    private static readonly Dictionary<string, string> Names = new(StringComparer.Ordinal)
    {
        ["absolution"] = "No problem found",
        ["anomaly"] = "Unusual activity",
        ["bad_actor"] = "Expensive query",
        ["blocking"] = "Blocking",
        ["config"] = "Server settings",
        ["coverage"] = "Data coverage",
        ["cpu"] = "CPU",
        ["database_config"] = "Database settings",
        ["disk"] = "Disk space",
        ["io"] = "Disk I/O",
        ["jobs"] = "Agent jobs",
        ["memory"] = "Memory",
        ["queries"] = "Queries",
        ["sessions"] = "Sessions",
        ["tempdb"] = "TempDB",
        ["waits"] = "Waits",
        [PgTargetSources.BloatSource] = "PostgreSQL table bloat",
        [PgTargetSources.BlockingSource] = "PostgreSQL blocking",
        [PgTargetSources.BufferSource] = "PostgreSQL buffer cache",
        [PgTargetSources.ConfigSource] = "PostgreSQL settings",
        [PgTargetSources.CpuSource] = "PostgreSQL CPU",
        [PgTargetSources.DatabaseSource] = "PostgreSQL databases",
        [PgTargetSources.GrowthSource] = "PostgreSQL growth",
        [PgTargetSources.IoSource] = "PostgreSQL disk I/O",
        [PgTargetSources.KernelSource] = "PostgreSQL host",
        [PgTargetSources.MemorySource] = "PostgreSQL memory",
        [PgTargetSources.PlansSource] = "PostgreSQL plans",
        [PgTargetSources.PostureSource] = "PostgreSQL durability settings",
        [PgTargetSources.QueriesSource] = "PostgreSQL queries",
        [PgTargetSources.ReplicationSource] = "PostgreSQL replication",
        [PgTargetSources.SessionsSource] = "PostgreSQL sessions",
        [PgTargetSources.TempSource] = "PostgreSQL temp files",
        [PgTargetSources.VacuumSource] = "PostgreSQL vacuum",
        [PgTargetSources.WaitsSource] = "PostgreSQL waits",
        [PgTargetSources.WriteSource] = "PostgreSQL writes",
    };

    /// <summary>Every raw category with a display name, for a test that holds a copy of the map equal.</summary>
    public static IReadOnlyDictionary<string, string> All => Names;

    /// <summary>
    /// The display name for a category. A category the map does not know shows as it is, so a new source is never a blank cell,
    /// and a missing one stays empty.
    /// </summary>
    public static string DisplayName(string? category)
    {
        if (string.IsNullOrEmpty(category))
        {
            return category ?? string.Empty;
        }

        return Names.TryGetValue(category, out var name) ? name : category;
    }
}
