namespace PerformanceMonitor.Exporter;

/// <summary>
/// One row destined for a content dimension table (`plan_shape` or `query_text`), tagged with
/// which raw table it came from. The key is a caller-supplied string built from the row's own
/// key columns (e.g. "server||planhash" or "server||db||queryhash") so the merge logic below
/// never needs to know the concrete key shape.
/// </summary>
public sealed record OriginRow(string Key, string Origin, object?[] Values);

/// <summary>The name recorded in a dimension row's `origin` column.</summary>
public static class DimensionOrigin
{
    public const string QueryStats = "query_stats";
    public const string QueryStore = "query_store";
}

/// <summary>
/// The values `--dimension-origin` accepts. `Both` is the default and preserves today's behavior (one call reads and
/// merges both key sources). `QueryStats` and `QueryStore` each read only their own source, for the split hourly
/// calls: the fresh call reads query_stats only, the Query Store call reads Query Store only, so each big table is
/// still read once per window.
/// </summary>
public static class DimensionOrigins
{
    public const string QueryStats = "query_stats";
    public const string QueryStore = "query_store";
    public const string Both = "both";

    public static readonly string[] All = [QueryStats, QueryStore, Both];

    /// <summary>
    /// The file-name `part` suffix a dimension/feature file gets for this origin
    /// (OutputPaths.ParquetFilePath's `part` parameter). Only the Query Store-only origin
    /// gets a suffix (`"qs"`); `query_stats` and `both` stay unsuffixed, because a `both`
    /// run and a `query_stats`-only run never both write the same window (a manual `both`
    /// run is not paired with a same-window `query_stats` run).
    /// </summary>
    public static string? PartFor(string dimensionOrigin) => dimensionOrigin == QueryStore ? "qs" : null;
}

/// <summary>
/// Pure merge logic for unioning a dimension table's rows from two sources (query_stats and
/// Query Store) into the single file the run writes. Kept as a standalone pure function so it
/// can be unit tested without a database: when both sources have the same key, the query_stats
/// row wins, because query_stats is the older, more heavily relied-on source and its content
/// batching is unchanged from part B.
/// </summary>
public static class DimensionKeyMerge
{
    public static List<OriginRow> Merge(IEnumerable<OriginRow> queryStatsRows, IEnumerable<OriginRow> queryStoreRows)
    {
        var byKey = new Dictionary<string, OriginRow>();

        foreach (var row in queryStatsRows)
        {
            byKey[row.Key] = row;
        }

        foreach (var row in queryStoreRows)
        {
            if (!byKey.ContainsKey(row.Key))
            {
                byKey[row.Key] = row;
            }
        }

        return [.. byKey.Values];
    }
}
