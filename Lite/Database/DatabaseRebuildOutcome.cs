namespace PerformanceMonitorLite.Database;

/// <summary>
/// #7812: what <c>DuckDbInitializer.MigrateViaParquetAsync</c> leaves behind when the rebuilt database starts without all of
/// the old data (the export failed, or the Parquet import failed, which can leave part of it loaded). The initializer sets
/// it only then: a rebuild whose import worked records nothing. <paramref name="ExportFolder"/> is set only when the export
/// worked and the import did not.
/// </summary>
internal sealed record DatabaseRebuildOutcome(string BackupPath, string? ExportFolder)
{
    /// <summary>The notice title.</summary>
    public const string NoticeTitle = "Lite started without all of its old data";

    /// <summary>The message the window shows when starting fails: the failure text, then, when a rebuild had already moved
    /// the old file aside, where it went, so that is not left in the log alone.</summary>
    public static string StartupFailureMessage(string error, string logFile, DatabaseRebuildOutcome? outcome)
    {
        var text = $"Failed to initialize the application:\n\n{error}\n\nSee the log for details: {logFile}. If the database is locked, close any other copy of Lite and start it again.";
        if (outcome is not null)
            text += $"\n\nBefore it failed, Lite rebuilt its database. {outcome.Describe()}";
        return text;
    }

    /// <summary>The plain-words notice shown once after start.</summary>
    public string Describe()
    {
        var text = $"Lite could not read its old database and started without all of its old data. The old file is kept at {BackupPath}.";
        if (!string.IsNullOrEmpty(ExportFolder))
            text += $" A copy of its data in Parquet files is kept in {ExportFolder}.";
        return text;
    }
}
