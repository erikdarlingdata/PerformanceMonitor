/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>One Lite start (#9425).</summary>
/// <param name="StartedUtc">When the start finished opening the store.</param>
/// <param name="Build">The informational version with any "+hash" removed, so two builds of one version compare equal.</param>
/// <param name="FullVersion">The informational version exactly as the assembly carries it, hash included.</param>
/// <param name="SchemaBefore">The schema version stamped in the store before this start's migrations. 0 for a store that did not exist yet.</param>
/// <param name="SchemaAfter">The schema version stamped after them.</param>
/// <param name="MigrationMs">Milliseconds the migration steps took. 0 when none ran.</param>
/// <param name="ArchiveFiles">Parquet files in the archive folder when the start began.</param>
/// <param name="PreviousBuild">The build of the start before this one. Null for the first row.</param>
/// <param name="LastSeenUtc">The last time this run was seen alive, filled in by the NEXT start. Null on the newest row, and on an older row
/// when nothing newer than its own start was found.</param>
public sealed record BuildStartEntry(
    DateTime StartedUtc,
    string Build,
    string FullVersion,
    int SchemaBefore,
    int SchemaAfter,
    long MigrationMs,
    int ArchiveFiles,
    string? PreviousBuild = null,
    DateTime? LastSeenUtc = null);

/// <summary>
/// Lite's build history (#9425): one row per start, so a change in timings or in the archive can be lined up with the build that
/// brought it. Kept in <c>build-history.json</c> at the data root beside <c>store-health-history.json</c>.
///
/// <para><b>Why a file and not a table.</b> The same reasons as <see cref="StoreHousekeepingHistory"/> (#8788). The size reset
/// deletes <c>monitor.duckdb</c> and builds a fresh one, and a table would need a place in the reset's fixed list of restored
/// tables. A file beside the store is not touched by the reset, needs no schema version and no archive, and a start that cannot
/// open the store can still be read from it.</para>
///
/// <para><b>Last seen.</b> Lite writes no stop time on a clean exit. The next start fills the previous row's
/// <see cref="BuildStartEntry.LastSeenUtc"/> with the newest of the last housekeeping entry and the last collection log row
/// (see <see cref="RecordStartAsync"/>).</para>
///
/// <para>Best effort: a read of a missing or damaged file is an empty history, and a write that fails is logged at Debug and
/// dropped. It must never fail the start it describes.</para>
/// </summary>
public sealed class BuildHistory
{
    public const string FileName = "build-history.json";

    /// <summary>The rows kept, newest first. At one start a day this is most of a year, and each row is a few hundred bytes.</summary>
    public const int MaxEntries = 200;

    private const string LogSource = "BuildHistory";

    /* One lock for every instance over the process: the start and a reader each hold their own object over the file. */
    private static readonly object s_fileLock = new();

    private static readonly JsonSerializerOptions s_options = new() { WriteIndented = true };

    private readonly string _filePath;

    public BuildHistory(string filePath)
    {
        _filePath = filePath;
    }

    /// <summary>The history file that belongs to a store: beside the database file, in the data root.</summary>
    public static string PathFor(string databasePath) =>
        Path.Combine(Path.GetDirectoryName(databasePath) ?? ".", FileName);

    public string FilePath => _filePath;

    /// <summary>The running build: the informational version without any "+hash", and the version as the assembly carries it.</summary>
    public static (string Build, string FullVersion) CurrentBuild()
    {
        var assembly = typeof(BuildHistory).Assembly;
        var full = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (string.IsNullOrWhiteSpace(full))
        {
            full = assembly.GetName().Version?.ToString() ?? "unknown";
        }

        return (StripHash(full), full);
    }

    /// <summary>The version up to the first "+", the SemVer build-metadata mark.</summary>
    public static string StripHash(string version)
    {
        var plus = version.IndexOf('+', StringComparison.Ordinal);
        return plus >= 0 ? version[..plus] : version;
    }

    /// <summary>
    /// Adds this start at the head, names the build before it, closes the previous row with <paramref name="previousLastSeenUtc"/>,
    /// and trims to <see cref="MaxEntries"/>. Never throws.
    /// </summary>
    public void Record(BuildStartEntry entry, DateTime? previousLastSeenUtc)
    {
        try
        {
            lock (s_fileLock)
            {
                var entries = ReadUnlocked();
                if (entries.Count > 0)
                {
                    var previous = entries[0];
                    entry = entry with { PreviousBuild = previous.Build };

                    /* A time from before the previous start cannot be its last sighting (a clock set back, or a collection log row
                       that predates it), so it is left unknown rather than shown as a run that ended before it began. */
                    var seen = previousLastSeenUtc is { } t && t >= previous.StartedUtc ? t : (DateTime?)null;
                    entries[0] = previous with { LastSeenUtc = seen };
                }

                entries.Insert(0, entry);
                if (entries.Count > MaxEntries)
                {
                    entries.RemoveRange(MaxEntries, entries.Count - MaxEntries);
                }

                var temp = _filePath + ".tmp";
                File.WriteAllText(temp, JsonSerializer.Serialize(entries, s_options));
                File.Move(temp, _filePath, overwrite: true);
            }
        }
        catch (Exception ex)
        {
            AppLogger.Debug(LogSource, $"Could not record the build history row: {ex.Message}");
        }
    }

    /// <summary>The history, newest first. Empty when the file is missing or cannot be read. It reads the file only: no store, no archive.</summary>
    public IReadOnlyList<BuildStartEntry> Read()
    {
        lock (s_fileLock)
        {
            return ReadUnlocked();
        }
    }

    private List<BuildStartEntry> ReadUnlocked()
    {
        try
        {
            if (!File.Exists(_filePath))
            {
                return [];
            }

            var entries = JsonSerializer.Deserialize<List<BuildStartEntry>>(File.ReadAllText(_filePath), s_options);
            return entries?.Where(e => e is not null && !string.IsNullOrEmpty(e.Build)).ToList() ?? [];
        }
        catch (Exception ex)
        {
            AppLogger.Debug(LogSource, $"Could not read the build history, so it is treated as empty: {ex.Message}");
            return [];
        }
    }

    /// <summary>
    /// Records this start: the build, the schema versions and migration time the store's initializer measured, the archive
    /// folder's file count, and the previous run's last sighting. The sighting is the newest of the latest housekeeping entry and
    /// the latest collection log row, whichever the previous run wrote last. Call it once the store is open and before collection
    /// starts. Never throws, so a failure here never fails the start. A store that is busy leaves the collection source out.
    /// </summary>
    public static async Task RecordStartAsync(
        DuckDbInitializer duckDb, BuildHistory history, StoreHousekeepingHistory housekeeping, string build, string fullVersion, DateTime nowUtc)
    {
        try
        {
            var archiveFiles = StoreHealthReader.ReadArchiveFolder(duckDb.ArchivePath).FileCount;
            var lastSeen = housekeeping.Read().Select(e => (DateTime?)e.TimeUtc).Max();
            var lastCollection = await Task.Run(() => ReadLastCollectionUtc(duckDb));
            if (lastCollection is { } c && (lastSeen is null || c > lastSeen))
            {
                lastSeen = c;
            }

            history.Record(
                new BuildStartEntry(
                    nowUtc, build, fullVersion, duckDb.StartupSchemaBefore ?? 0, duckDb.StartupSchemaAfter, duckDb.StartupMigrationMs, archiveFiles),
                lastSeen);
        }
        catch (Exception ex)
        {
            AppLogger.Debug(LogSource, $"Could not record the build history row: {ex.Message}");
        }
    }

    private static DateTime? ReadLastCollectionUtc(DuckDbInitializer duckDb)
    {
        try
        {
            using var readLock = duckDb.TryAcquireReadLock(TimeSpan.FromSeconds(2));
            if (readLock is null)
            {
                return null;
            }

            using var connection = duckDb.CreateConnection();
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT max(collection_time) FROM collection_log";
            return cmd.ExecuteScalar() is DateTime t ? DateTime.SpecifyKind(t, DateTimeKind.Utc) : null;
        }
        catch (Exception ex)
        {
            AppLogger.Debug(LogSource, $"Could not read the last collection time: {ex.Message}");
            return null;
        }
    }

    /// <summary>The BUILD HISTORY section of <c>get_store_health</c>: newest first, at most <paramref name="max"/> rows.</summary>
    public static string Render(IReadOnlyList<BuildStartEntry> entries, int max = 20)
    {
        var ci = CultureInfo.InvariantCulture;
        var sb = new StringBuilder();
        sb.AppendLine("BUILD HISTORY (newest first, one row per Lite start)");
        if (entries.Count == 0)
        {
            sb.AppendLine("  Nothing recorded yet.");
        }

        var newest = true;
        foreach (var e in entries.Take(max))
        {
            sb.Append(ci, $"  {e.StartedUtc:yyyy-MM-dd HH:mm} UTC  build {e.Build}  schema v{e.SchemaBefore} to v{e.SchemaAfter}");
            sb.Append(ci, $", migrations {e.MigrationMs:N0} ms, {e.ArchiveFiles:N0} archive files at start");
            if (e.PreviousBuild is { Length: > 0 } before && before != e.Build)
            {
                sb.Append(ci, $", previous build {before}");
            }

            if (e.LastSeenUtc is { } seen)
            {
                sb.Append(ci, $". Last seen {seen:yyyy-MM-dd HH:mm} UTC");
            }
            else
            {
                /* The newest row is the run that is answering this read. An older row without a time had nothing newer than its start on record. */
                sb.Append(newest ? ". This run is still going" : ". Last seen not known");
            }

            newest = false;
            sb.AppendLine();
        }

        if (entries.Count > max)
        {
            sb.AppendLine(ci, $"  ... and {entries.Count - max:N0} older starts.");
        }

        return sb.ToString().TrimEnd();
    }
}
