namespace PerformanceMonitor.Exporter;

/// <summary>
/// The time column and the dimension keys of the core tables, so the exporter can write
/// `time_column`/`kind`/`key_columns` for every core manifest entry. A loader reads these from the manifest and needs
/// no hard-coded map of its own. The same facts are in the file contract.
/// </summary>
public static class CoreTableMetadata
{
    /// <summary>The time column of each table that does not use <see cref="DefaultTimeColumn"/>.</summary>
    public static readonly IReadOnlyDictionary<string, string> LoaderTimeColumn = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        [HourlyTables.QueryStatsHourly] = "hour",
        [HourlyTables.ProcedureStatsHourly] = "hour",
        [QueryStoreTables.QueryStoreStats] = "interval_start_time_utc",
    };

    /// <summary>The fallback time column for any table not in <see cref="LoaderTimeColumn"/>.</summary>
    public const string DefaultTimeColumn = "collection_time";

    /// <summary>The key columns of each dimension table, in key order.</summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyList<string>> LoaderDimensionKeys = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
    {
        [DimensionTables.PlanShape] = ["source_store", "server_name", "query_plan_hash"],
        [DimensionTables.QueryText] = ["source_store", "server_name", "database_name", "query_hash"],
        [PlanFeatureTables.PlanFeatures] = ["source_store", "plan_digest", "statement_ordinal", "feature_version"],
        [PlanFeatureTables.PlanWarnings] = ["source_store", "plan_digest", "statement_ordinal", "feature_version", "warning_seq"],
        [PlanFeatureTables.PlanMissingIndexes] = ["source_store", "plan_digest", "statement_ordinal", "feature_version", "index_seq"],
        [DimensionTables.ProcedurePlan] = ["source_store", "server_name", "database_name", "schema_name", "object_name", "shape_hash"],
    };

    /// <summary>True for every table the loader treats as a dimension (insert-if-absent by key)</summary>
    public static bool IsDimension(string table) => LoaderDimensionKeys.ContainsKey(table);

    /// <summary>`dimension` for a table in <see cref="LoaderDimensionKeys"/>, `window` otherwise.</summary>
    public static string Kind(string table) => IsDimension(table) ? "dimension" : "window";

    /// <summary>The manifest `time_column` value for a core table: <see cref="SqlBuilder.TimeColumn"/>'s value, which already matches the loader's map/fallback for every window table.</summary>
    public static string TimeColumn(string table) => SqlBuilder.TimeColumn(table);

    /// <summary>The manifest `key_columns` value for a core table: the loader's dimension key tuple, or null for a window table.</summary>
    public static IReadOnlyList<string>? KeyColumns(string table) =>
        LoaderDimensionKeys.TryGetValue(table, out var keys) ? keys : null;

    /// <summary>
    /// Returns a copy of <paramref name="entry"/> with <see cref="ManifestTableEntry.TimeColumn"/>,
    /// <see cref="ManifestTableEntry.Kind"/> and <see cref="ManifestTableEntry.KeyColumns"/> filled in from this
    /// class's maps. <see cref="ManifestTableEntry"/> is not a record (it carries too many optional, per-exporter
    /// fields for a `with` expression to stay readable), so every field is copied explicitly here.
    /// </summary>
    public static ManifestTableEntry WithMetadata(ManifestTableEntry entry) => new()
    {
        Table = entry.Table,
        OutputFile = entry.OutputFile,
        RowCount = entry.RowCount,
        ByteSize = entry.ByteSize,
        Sha256 = entry.Sha256,
        Columns = entry.Columns,
        StoreQueryElapsedMs = entry.StoreQueryElapsedMs,
        WriteElapsedMs = entry.WriteElapsedMs,
        MissingContentCount = entry.MissingContentCount,
        KnownKeysSkipped = entry.KnownKeysSkipped,
        FeatureDigestsParsed = entry.FeatureDigestsParsed,
        FeatureDigestsSkipped = entry.FeatureDigestsSkipped,
        OversizedPlanRows = entry.OversizedPlanRows,
        ProcedurePlanReuse = entry.ProcedurePlanReuse,
        QueryStoreSource = entry.QueryStoreSource,
        BelowThresholdDeferred = entry.BelowThresholdDeferred,
        DeferredNowFetched = entry.DeferredNowFetched,
        Timings = entry.Timings,
        TimeColumn = TimeColumn(entry.Table),
        Kind = Kind(entry.Table),
        KeyColumns = KeyColumns(entry.Table) is { } keys ? [.. keys] : null,
    };
}
