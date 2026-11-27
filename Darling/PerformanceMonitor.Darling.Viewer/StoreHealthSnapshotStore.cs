/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #9364: the last tables and indexes rows the Viewer read from one store, as the file the Store Health tab shows on its first open in a new
/// process. Each part is null until a live read of it has been saved. A part carries the time the store answered it.
/// </summary>
internal sealed record StoreHealthSnapshot(
    int Version,
    IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow>? Tables,
    DateTime? TablesReadAtUtc,
    IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexHealthRow>? Indexes,
    IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexCoverageRow>? Coverage,
    string? CoverageLeftOutNote,
    DateTime? IndexesReadAtUtc);

/// <summary>
/// #9364: the small file beside the Viewer's other per-user files that holds one store's last Store Health rows. A first open in a new process
/// sizes every relation of the store, which takes 5 to 11 seconds on a large one, while the Viewer's in-process cache (#8963) only helps the
/// second open. After every live read the tab saves its rows here, and the next process shows them at once, with their age, while the live
/// read runs. The file is keyed by the store's host, port and database (never the password) and written with a temporary file and a move, so a
/// crash mid-write leaves the old file. A file that is absent, from another version, or cannot be read is the same as no file: the tab then
/// behaves as it did before. A write that fails is a log line, never an error shown to the person using the Viewer.
/// </summary>
internal sealed class StoreHealthSnapshotStore
{
    internal const int CurrentVersion = 1;

    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = false };

    private readonly string _path;
    private readonly object _gate = new();

    public StoreHealthSnapshotStore(string path) => _path = path;

    /// <summary>The path of the file this store reads and writes.</summary>
    public string FilePath => _path;

    /// <summary>The store's file: <c>%APPDATA%\PerformanceMonitorDarling\store-health-{hash of the store identity}.json</c>, next to the Viewer's other
    /// per-user files. <paramref name="storeIdentity"/> is <see cref="ViewerDataService.StoreConnectionIdentity"/>: host, port and database.</summary>
    public static StoreHealthSnapshotStore ForStore(string storeIdentity)
    {
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(storeIdentity)), 0, 8).ToLowerInvariant();
        var directory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "PerformanceMonitorDarling");
        return new StoreHealthSnapshotStore(Path.Combine(directory, $"store-health-{hash}.json"));
    }

    /// <summary>The saved rows, or null when there is no file, it cannot be read, it is from another version, or it holds no rows to show.</summary>
    public StoreHealthSnapshot? Load()
    {
        lock (_gate)
        {
            return LoadCore();
        }
    }

    /// <summary>Saves a tables read, keeping the saved indexes. A read no newer than the saved tables is not written again.</summary>
    public void SaveTables(IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow> rows, DateTime readAtUtc)
    {
        lock (_gate)
        {
            var current = LoadCore();
            if (current?.TablesReadAtUtc is { } saved && saved >= readAtUtc)
            {
                return;
            }

            Write((current ?? Empty()) with { Tables = rows, TablesReadAtUtc = readAtUtc });
        }
    }

    /// <summary>Saves an indexes read, keeping the saved tables. A read no newer than the saved indexes is not written again.</summary>
    public void SaveIndexes(StoreIndexHealth read)
    {
        if (read.ReadAtUtc is not { } readAtUtc)
        {
            return;
        }

        lock (_gate)
        {
            var current = LoadCore();
            if (current?.IndexesReadAtUtc is { } saved && saved >= readAtUtc)
            {
                return;
            }

            Write((current ?? Empty()) with
            {
                Indexes = read.Indexes,
                Coverage = read.Coverage,
                CoverageLeftOutNote = read.CoverageLeftOutNote,
                IndexesReadAtUtc = readAtUtc,
            });
        }
    }

    private static StoreHealthSnapshot Empty() => new(CurrentVersion, null, null, null, null, null, null);

    private StoreHealthSnapshot? LoadCore()
    {
        try
        {
            if (!File.Exists(_path))
            {
                return null;
            }

            var snapshot = JsonSerializer.Deserialize<StoreHealthSnapshot>(File.ReadAllBytes(_path), s_json);
            if (snapshot is null || snapshot.Version != CurrentVersion)
            {
                return null;
            }

            var tables = snapshot.Tables is { Count: > 0 } t && t.All(r => r is not null) && snapshot.TablesReadAtUtc is not null;
            var indexes = snapshot.Indexes is { Count: > 0 } i && i.All(r => r is not null) && snapshot.IndexesReadAtUtc is not null
                && (snapshot.Coverage?.All(r => r is not null) ?? true);
            if (!tables && !indexes)
            {
                return null;
            }

            return snapshot with
            {
                Tables = tables ? snapshot.Tables : null,
                TablesReadAtUtc = tables ? snapshot.TablesReadAtUtc : null,
                Indexes = indexes ? snapshot.Indexes : null,
                Coverage = indexes ? snapshot.Coverage ?? [] : null,
                CoverageLeftOutNote = indexes ? snapshot.CoverageLeftOutNote : null,
                IndexesReadAtUtc = indexes ? snapshot.IndexesReadAtUtc : null,
            };
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or NotSupportedException or ArgumentException)
        {
            ViewerLogger.Debug("StoreHealth", $"The saved store health rows could not be read ({ex.GetType().Name}); the tab reads the store.");
            return null;
        }
    }

    private void Write(StoreHealthSnapshot snapshot)
    {
        var temp = _path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_path)!);
            File.WriteAllBytes(temp, JsonSerializer.SerializeToUtf8Bytes(snapshot, s_json));
            File.Move(temp, _path, overwrite: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException or ArgumentException)
        {
            ViewerLogger.Warn("StoreHealth", $"The store health rows could not be saved for the next open ({ex.GetType().Name}).");
            try
            {
                File.Delete(temp);
            }
            catch (Exception cleanup) when (cleanup is IOException or UnauthorizedAccessException)
            {
                /* The temporary file stays; the next write uses another name and the store never reads it. */
            }
        }
    }
}
