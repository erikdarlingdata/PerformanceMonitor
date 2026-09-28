using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #4677: the one place the pg_stat_statements eviction caveat is worded, so the MCP tool, the web dashboard (which
/// renders the MCP's <c>evictions.note</c>) and the Darling Viewer cannot drift apart. Null passes means unknown,
/// never 0: 0 means the counter was read and no pass happened.
/// </summary>
public static class PgStatementEvictionNote
{
    /// <summary>
    /// The caveat for a window: the unknown sentence when the counter was never observed, null when it was read and
    /// no pass happened, otherwise the passes sentence (with the current max when present).
    /// </summary>
    public static string? Build(DarlingPgStatementReader.PgEvictionInfo? info)
    {
        if (info is null || !info.Known || info.EvictionPasses is null)
        {
            return "Eviction count unknown for this target (pg_stat_statements_info is absent: PostgreSQL before 14, "
                 + "or the extension is below 1.9; ALTER EXTENSION pg_stat_statements UPDATE adds it).";
        }

        var passes = info.EvictionPasses.Value;
        if (passes <= 0)
        {
            return null;
        }

        var current = info.MaxEntries is { } max ? ", currently " + max.ToString(CultureInfo.InvariantCulture) : "";
        return "pg_stat_statements evicted entries " + passes.ToString(CultureInfo.InvariantCulture)
             + " time(s) in this window (each pass drops about 5% of pg_stat_statements.max" + current
             + "), so a rarely-run statement may be missing and a statement re-admitted after an eviction counts only from then: "
             + "the totals here can under-report. Raising pg_stat_statements.max (a postmaster setting, restart required) reduces this.";
    }
}
