namespace PerformanceMonitor.Exporter;

/// <summary>Builds the on-disk layout for exported Parquet files and the run manifest.</summary>
public static class OutputPaths
{
    private const string FileNameTimestampFormat = "yyyyMMddTHHmmZ";

    /// <summary>Formats a UTC instant the way it appears in file and manifest names.</summary>
    public static string FormatTimestamp(DateTime utc) => utc.ToString(FileNameTimestampFormat, System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// The Parquet file path for one table's export, overwritten on re-run of the same
    /// window: `&lt;out&gt;/&lt;store&gt;/&lt;table&gt;/&lt;table&gt;__&lt;store&gt;__&lt;from&gt;__&lt;to&gt;.parquet`. When
    /// <paramref name="part"/> is given, it is appended before the extension
    /// (`...__&lt;to&gt;__&lt;part&gt;.parquet`), so two producers writing the same manifest `table` name
    /// in one run (e.g. plan_shape and procedure_plan both writing `plan_features`) do not collide on
    /// the same file.
    /// </summary>
    public static string ParquetFilePath(string outDir, string store, string table, DateTime fromUtc, DateTime toUtc, string? part = null)
    {
        var suffix = part is null ? string.Empty : $"__{part}";
        var fileName = $"{table}__{store}__{FormatTimestamp(fromUtc)}__{FormatTimestamp(toUtc)}{suffix}.parquet";
        return Path.Combine(outDir, store, table, fileName);
    }

    /// <summary>The path of the run's manifest file, written last, after every table has succeeded.</summary>
    public static string ManifestFilePath(string outDir, string store, DateTime fromUtc, DateTime toUtc) =>
        ManifestFilePath(outDir, store, fromUtc, toUtc, ExportOptions.AllTables);

    /// <summary>
    /// The manifest path for a run of <paramref name="tables"/>. A run of every table (with the default
    /// `--dimension-origin both`) is named <c>store__from__to.json</c>. A run of a subset, or a run whose
    /// `--dimension-origin` is not `both`, adds <c>__t</c> and the first 8 hex digits of the SHA-256 of the
    /// sorted table list plus the origin, so a subset run never replaces the full run's manifest for the same
    /// window (the landing area is the only copy of a window until it is loaded), a run of the same table set
    /// under a different origin never collides with it either (the hourly fresh and Query Store calls can
    /// request an overlapping or identical table set, e.g. `plan_shape,query_text`, under different origins),
    /// and re-running the same subset/origin replaces its own.
    /// </summary>
    public static string ManifestFilePath(string outDir, string store, DateTime fromUtc, DateTime toUtc, IReadOnlyCollection<string> tables, string? dimensionOrigin = null)
    {
        var baseName = $"{store}__{FormatTimestamp(fromUtc)}__{FormatTimestamp(toUtc)}";
        var isFullRun = tables.Count == ExportOptions.AllTables.Length && ExportOptions.AllTables.All(tables.Contains)
            && (dimensionOrigin is null || dimensionOrigin == DimensionOrigins.Both);
        var fileName = isFullRun ? $"{baseName}.json" : $"{baseName}__t{TableSetTag(tables, dimensionOrigin)}.json";
        return Path.Combine(outDir, store, "_manifests", fileName);
    }

    /// <summary>
    /// First 8 lowercase hex digits of the SHA-256 of the sorted, comma-joined table names, plus the
    /// dimension origin (when given and not the default `both`), so two calls with the same table set but a
    /// different origin never share a manifest name.
    /// </summary>
    public static string TableSetTag(IEnumerable<string> tables, string? dimensionOrigin = null)
    {
        var tag = dimensionOrigin is null || dimensionOrigin == DimensionOrigins.Both ? string.Empty : $"|origin={dimensionOrigin}";
        var joined = string.Join(",", tables.Distinct(StringComparer.Ordinal).OrderBy(t => t, StringComparer.Ordinal)) + tag;
        var hash = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(joined));
        return Convert.ToHexStringLower(hash)[..8];
    }

    /// <summary>
    /// The Parquet file path relative to the store's output directory, as recorded in the
    /// manifest (so the manifest is portable across the parent --out directory). This is a
    /// portable manifest/S3 path, not a local file-system path, so it is always joined with
    /// '/' even when the exporter runs on Windows (Path.Combine would emit '\' there, and a
    /// backslash in an S3 key is wrong).
    /// </summary>
    public static string RelativeParquetPath(string store, string table, DateTime fromUtc, DateTime toUtc, string? part = null)
    {
        var suffix = part is null ? string.Empty : $"__{part}";
        var fileName = $"{table}__{store}__{FormatTimestamp(fromUtc)}__{FormatTimestamp(toUtc)}{suffix}.parquet";
        return $"{table}/{fileName}";
    }
}
