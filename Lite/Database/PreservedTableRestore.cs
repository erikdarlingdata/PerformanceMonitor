using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// The restore marker and the conflict-ignoring restore of the configuration and state tables that survive a
/// database reset. The in-process reset and the startup recovery both go through this class, so a restore that
/// is repeated after a crash behaves the same as the first one.
/// </summary>
internal static class PreservedTableRestore
{
    /// <summary>
    /// Lives at the top of the archive folder. Line 1 is the preserve directory's name (relative to the archive
    /// folder); each later line is one table whose rows are in <c>{directory}/{table}.parquet</c>.
    /// </summary>
    internal const string RestoreMarkerFileName = "archive_restore_pending.txt";

    /// <summary>
    /// Prefix of the directory under the archive folder that holds the preserved parquet files. Archive scans read
    /// the folder's top level only, so a subdirectory is never taken for archive data.
    /// </summary>
    internal const string PreserveDirectoryPrefix = "pm_preserve_";

    /// <summary>
    /// The marker's in-progress name. It must not end in <c>.tmp</c>: the archive recovery deletes top-level
    /// <c>*.tmp</c> files.
    /// </summary>
    internal const string RestoreMarkerWritingFileName = RestoreMarkerFileName + ".writing";

    /// <summary>The table whose rows have no primary key, so a conflict-ignoring insert cannot skip duplicates.</summary>
    private const string KeylessTable = "dismissed_archive_alerts";

    /// <summary>
    /// Writes the marker atomically: the full content goes to a side file first, then a rename replaces the
    /// marker, so a reader sees no marker or a complete one, never a partial one.
    /// </summary>
    internal static void WriteMarker(string archivePath, string dirName, IEnumerable<string> tables)
    {
        var writingPath = Path.Combine(archivePath, RestoreMarkerWritingFileName);
        var markerPath = Path.Combine(archivePath, RestoreMarkerFileName);
        File.WriteAllLines(writingPath, new[] { dirName }.Concat(tables));
        File.Move(writingPath, markerPath, overwrite: true);
    }

    /// <summary>
    /// Reads the marker. False when there is none, or when it names no directory.
    /// </summary>
    internal static bool TryReadMarker(string archivePath, out string dirName, out IReadOnlyList<string> tables)
    {
        dirName = string.Empty;
        tables = Array.Empty<string>();
        var markerPath = Path.Combine(archivePath, RestoreMarkerFileName);
        if (!File.Exists(markerPath)) return false;

        var lines = File.ReadAllLines(markerPath)
            .Select(l => l.Trim())
            .Where(l => l.Length > 0)
            .ToList();
        if (lines.Count == 0) return false;

        dirName = lines[0];
        tables = lines.Skip(1).ToList();
        return true;
    }

    /// <summary>
    /// Puts one preserved table's rows back. Conflict-ignoring on purpose: a restore repeated after a crash must
    /// not duplicate rows, and "restore only if the table is empty" is wrong after an in-process failure followed
    /// by live writes, because the table is then non-empty and every preserved row would be dropped. A row that is
    /// already there (the live, newer one) wins.
    /// </summary>
    internal static async Task RestoreTableAsync(DuckDBConnection connection, string table, string parquetPath)
    {
        var escaped = DuckDbInitializer.EscapeSqlPath(parquetPath);
        using var cmd = connection.CreateCommand();
        if (table == KeylessTable)
        {
            /* No primary key: skip a row whose natural key (the one its writers use) is already present. */
            cmd.CommandText = $@"INSERT INTO {KeylessTable} BY NAME
SELECT p.* FROM read_parquet('{escaped}') AS p
WHERE NOT EXISTS (
    SELECT 1 FROM {KeylessTable} AS d
    WHERE d.alert_time = p.alert_time AND d.server_id = p.server_id AND d.metric_name = p.metric_name)";
        }
        else
        {
            cmd.CommandText = $"INSERT OR IGNORE INTO {table} BY NAME SELECT * FROM read_parquet('{escaped}')";
        }
        await cmd.ExecuteNonQueryAsync();
    }
}
