namespace PerformanceMonitorLite.Database;

/// <summary>
/// #7812: what <c>DuckDbInitializer.MigrateViaParquetAsync</c> leaves behind when the rebuilt database starts without the
/// old data (the export failed, or the Parquet import failed). The initializer sets it only then: a rebuild whose import
/// worked records nothing. <paramref name="ExportFolder"/> is set only when the export worked and the import did not.
/// </summary>
internal sealed record DatabaseRebuildOutcome(string BackupPath, string? ExportFolder)
{
    /// <summary>The plain-words notice shown once after start.</summary>
    public string Describe()
    {
        var text = $"Lite could not read its old database and started with an empty one. The old file is kept at {BackupPath}.";
        if (!string.IsNullOrEmpty(ExportFolder))
            text += $" A copy of its data in Parquet files is kept in {ExportFolder}.";
        return text;
    }
}
