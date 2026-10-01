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
    /// folder. An optional <c>identity:</c> line carries the database file's identity at the time of the copy,
    /// and each <c>promoted:</c> line names an archive file the reset promoted. Every other line is one table
    /// whose rows are in <c>{directory}/{table}.parquet</c>. A marker with no identity line is the legacy
    /// format.
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
    /// The reset's export marker, at the top of the archive folder: it names the archive files a reset wrote
    /// before it empties the database. Its presence means the reset has not started.
    /// </summary>
    internal const string ResetExportMarkerFileName = "archive_reset_pending.txt";

    /// <summary>
    /// The configuration tables a user edits by hand. While a restore of one of them is pending the live table is
    /// empty or partial, so a user-initiated write to it is refused: it would collide with the rows the next
    /// start puts back.
    /// </summary>
    internal static readonly string[] UserChoiceTables =
        ["config_mute_rules", "dismissed_archive_alerts", "analysis_muted", "server_tags", "server_tag_map"];

    /// <summary>Prefix of the marker line that carries the database file's identity.</summary>
    internal const string IdentityLinePrefix = "identity:";

    /// <summary>Prefix of a marker line that names one promoted archive file.</summary>
    internal const string PromotedLinePrefix = "promoted:";

    /// <summary>
    /// Writes the marker atomically: the full content goes to a side file first, then a rename replaces the
    /// marker, so a reader sees no marker or a complete one, never a partial one.
    /// </summary>
    internal static void WriteMarker(string archivePath, string dirName, IEnumerable<string> tables,
        string identity, IEnumerable<string> promotedNames)
    {
        var writingPath = Path.Combine(archivePath, RestoreMarkerWritingFileName);
        var markerPath = Path.Combine(archivePath, RestoreMarkerFileName);
        var lines = new[] { dirName, IdentityLinePrefix + identity }
            .Concat(promotedNames.Select(n => PromotedLinePrefix + n))
            .Concat(tables);
        /* Flushed to disk before the rename: the rename is atomic against a kill, and the flush keeps a power
           loss from leaving a renamed marker with no content. */
        using (var stream = new FileStream(writingPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            using (var writer = new StreamWriter(stream, new System.Text.UTF8Encoding(false), 1024, leaveOpen: true))
            {
                foreach (var line in lines) writer.WriteLine(line);
            }
            stream.Flush(flushToDisk: true);
        }
        File.Move(writingPath, markerPath, overwrite: true);
    }

    /// <summary>
    /// Reads the marker. False when there is none, or when it names no directory. <paramref name="identity"/> is
    /// null for the legacy format (no identity line).
    /// </summary>
    internal static bool TryReadMarker(string archivePath, out string dirName, out IReadOnlyList<string> tables,
        out string? identity, out IReadOnlyList<string> promotedNames)
    {
        dirName = string.Empty;
        tables = Array.Empty<string>();
        identity = null;
        promotedNames = Array.Empty<string>();
        var markerPath = Path.Combine(archivePath, RestoreMarkerFileName);
        if (!File.Exists(markerPath)) return false;

        var lines = File.ReadAllLines(markerPath)
            .Select(l => l.Trim())
            .Where(l => l.Length > 0)
            .ToList();
        if (lines.Count == 0) return false;

        dirName = lines[0];
        var tableLines = new List<string>();
        var promoted = new List<string>();
        foreach (var line in lines.Skip(1))
        {
            if (line.StartsWith(IdentityLinePrefix, StringComparison.Ordinal))
                identity = line.Substring(IdentityLinePrefix.Length).Trim();
            else if (line.StartsWith(PromotedLinePrefix, StringComparison.Ordinal))
                promoted.Add(line.Substring(PromotedLinePrefix.Length).Trim());
            else
                tableLines.Add(line);
        }
        if (string.IsNullOrEmpty(identity)) identity = null;
        tables = tableLines;
        promotedNames = promoted;
        return true;
    }

    /// <summary>
    /// True when a readable restore marker in <paramref name="archivePath"/> lists <paramref name="table"/>.
    /// </summary>
    internal static bool IsRestorePendingFor(string archivePath, string table)
    {
        try
        {
            return TryReadMarker(archivePath, out _, out var tables, out _, out _)
                && tables.Contains(table, StringComparer.Ordinal);
        }
        catch (IOException)
        {
            return false;
        }
    }

    /// <summary>
    /// Refuses a user-initiated write to <paramref name="table"/> while its restore is pending. Called after the
    /// write lock is taken, because the marker changes only under that lock.
    /// </summary>
    internal static void ThrowIfRestorePending(string archivePath, string table)
    {
        if (!IsRestorePendingFor(archivePath, table)) return;
        var markerPath = Path.Combine(archivePath, RestoreMarkerFileName);
        throw new PendingRestoreException(
            "This setting can't be changed until Lite restarts: an earlier database reset couldn't put your saved settings back, and the next start finishes it. "
            + $"If that keeps failing, deleting {markerPath} discards those saved settings and unlocks them.");
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

/// <summary>A user-initiated write was refused because a restore of that table's saved rows is pending.</summary>
internal sealed class PendingRestoreException(string message) : InvalidOperationException(message);
