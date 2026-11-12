namespace PerformanceMonitor.Exporter.Upload;

/// <summary>
/// Builds the keys an export uploads to. Every key is joined with '/' only, never <c>Path.Combine</c>: a key is a
/// string with a fixed separator, not a local file-system path, and on Windows <c>Path.Combine</c> would emit a
/// backslash that S3 treats as a literal character in the key rather than a separator.
/// </summary>
/// <param name="Prefix">Leading key segment. May be empty (the segment is then omitted); a trailing '/' is stripped.</param>
/// <param name="HiveStoreFolder">
/// When true the store folder is <c>store=&lt;store&gt;</c> instead of <c>&lt;store&gt;</c>. Off by default, which
/// gives the keys the exporter has always produced.
/// </param>
public sealed record UploadLayout(string Prefix, bool HiveStoreFolder = false)
{
    /// <summary>
    /// The key for one table's Parquet file: <c>&lt;prefix&gt;/&lt;store&gt;/&lt;relativePath&gt;</c>, where relativePath is
    /// the manifest's portable, always-forward-slash <c>OutputPaths.RelativeParquetPath</c> value.
    /// </summary>
    public string ParquetKey(string store, string relativeParquetPath) => Join(StoreSegment(store), relativeParquetPath);

    /// <summary>The key for a run manifest: <c>&lt;prefix&gt;/&lt;store&gt;/_manifests/&lt;fileName&gt;</c>.</summary>
    public string ManifestKey(string store, string manifestFileName) => Join(StoreSegment(store), "_manifests", manifestFileName);

    private string StoreSegment(string store) => HiveStoreFolder ? "store=" + store : store;

    private string Join(params string[] tail)
    {
        var trimmed = (Prefix ?? "").TrimEnd('/');
        var parts = new List<string>();
        if (trimmed.Length > 0)
        {
            parts.Add(trimmed);
        }
        parts.AddRange(tail);
        return string.Join('/', parts);
    }
}
