/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// The #1912 Query Store slice repair's own record of what it did (#9352): the completion marker and the attempt
/// history, kept in <c>query-store-slice-repair.json</c> at the data root beside the store.
///
/// <para><b>Why a file and not the store's tables.</b> The repair is meant to run once per install. Its marker used to
/// live in <c>monitor.duckdb</c>, and the size reset deletes that file and builds a fresh one
/// (<c>ArchiveService.ArchiveAllAndResetAsync</c>), so the marker went with the old store. The next launch found no marker
/// and no attempts and surveyed the whole store and every archive file again. This is the same shape as
/// <see cref="StoreHousekeepingHistory"/> (#8788): a file beside the store is not touched by the reset, and the reset's
/// fixed list of restored tables (<c>ArchiveService.PreservedConfigTables</c>) stays as it is.</para>
///
/// <para>Best effort, like that history: a read of a missing or damaged file is an empty state, and a write that fails is
/// logged and dropped. The in-store tables are still written beside it, so a failed write leaves the repair exactly as
/// protected as it was before this file existed, until the next reset.</para>
/// </summary>
public sealed class QueryStoreSliceRepairState
{
    public const string FileName = "query-store-slice-repair.json";

    private const string LogSource = "QueryStoreSliceRepair";

    /* One lock for every instance over the same process: the service and a test each hold their own object over the file. */
    private static readonly object s_fileLock = new();

    private static readonly JsonSerializerOptions s_options = new() { WriteIndented = true };

    private readonly string _filePath;

    public QueryStoreSliceRepairState(string filePath)
    {
        _filePath = filePath;
    }

    /// <summary>The state file that belongs to a store: beside the database file, in the data root.</summary>
    public static string PathFor(string databasePath) =>
        Path.Combine(Path.GetDirectoryName(databasePath) ?? ".", FileName);

    public string FilePath => _filePath;

    /// <summary>One recorded attempt: when it started and the shape of the work it saw.</summary>
    public sealed class AttemptEntry
    {
        public DateTime AttemptedAtUtc { get; set; }
        public long HotGroups { get; set; }
        public long ArchiveFiles { get; set; }
        public long UnreadableFiles { get; set; }
    }

    /// <summary>The whole file. <see cref="CompletedAtUtc"/> is null until a fully successful pass (or an in-store marker) is recorded.</summary>
    public sealed class Snapshot
    {
        public DateTime? CompletedAtUtc { get; set; }
        public long RowsRemoved { get; set; }
        public List<AttemptEntry> Attempts { get; set; } = [];
    }

    /// <summary>The recorded state. Empty when the file is missing or cannot be read.</summary>
    public Snapshot Read()
    {
        lock (s_fileLock)
        {
            return ReadUnlocked();
        }
    }

    /// <summary>Records the completed repair. Never throws.</summary>
    public void MarkCompleted(long rowsRemoved)
    {
        Update(snapshot =>
        {
            snapshot.CompletedAtUtc = DateTime.UtcNow;
            snapshot.RowsRemoved = rowsRemoved;
        });
    }

    /// <summary>Records one attempt, before the repair it describes runs. Never throws.</summary>
    public void AddAttempt(long hotGroups, long archiveFiles, long unreadableFiles)
    {
        Update(snapshot => snapshot.Attempts.Add(new AttemptEntry
        {
            AttemptedAtUtc = DateTime.UtcNow,
            HotGroups = hotGroups,
            ArchiveFiles = archiveFiles,
            UnreadableFiles = unreadableFiles,
        }));
    }

    /// <summary>Brings the file's attempts up to <paramref name="count"/> when a store carries more of them than the file does
    /// (a store whose attempts were recorded before this file existed). The added entries carry the last attempt's shape. Never throws.</summary>
    public void SeedAttempts(long count, long hotGroups, long archiveFiles, long unreadableFiles)
    {
        Update(snapshot =>
        {
            while (snapshot.Attempts.Count < count)
            {
                snapshot.Attempts.Add(new AttemptEntry
                {
                    AttemptedAtUtc = DateTime.UtcNow,
                    HotGroups = hotGroups,
                    ArchiveFiles = archiveFiles,
                    UnreadableFiles = unreadableFiles,
                });
            }
        });
    }

    private void Update(Action<Snapshot> change)
    {
        try
        {
            lock (s_fileLock)
            {
                var snapshot = ReadUnlocked();
                change(snapshot);

                var temp = _filePath + ".tmp";
                File.WriteAllText(temp, JsonSerializer.Serialize(snapshot, s_options));
                File.Move(temp, _filePath, overwrite: true);
            }
        }
        catch (Exception ex)
        {
            AppLogger.Warn(LogSource, $"Could not record the Query Store repair state: {ex.Message}");
        }
    }

    private Snapshot ReadUnlocked()
    {
        try
        {
            if (!File.Exists(_filePath))
            {
                return new Snapshot();
            }

            var snapshot = JsonSerializer.Deserialize<Snapshot>(File.ReadAllText(_filePath), s_options) ?? new Snapshot();
            snapshot.Attempts = (snapshot.Attempts ?? []).Where(a => a is not null).OrderBy(a => a.AttemptedAtUtc).ToList();
            return snapshot;
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException or NotSupportedException)
        {
            AppLogger.Warn(LogSource, $"Could not read the Query Store repair state, treating it as empty: {ex.Message}");
            return new Snapshot();
        }
    }
}
