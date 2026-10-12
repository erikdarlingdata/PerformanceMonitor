/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Free space on the volume that holds Lite's own data folder (#5377). Lite had no check of its own volume:
/// the FinOps free-space code is about monitored servers. A compaction merge writes its output next to its
/// inputs and a CHECKPOINT can grow the database file, and on a volume with a couple of GiB free both fail
/// part way with an I/O error that names neither the cause nor the numbers.
/// </summary>
internal static class DataVolumeSpace
{
    /// <summary>
    /// Room kept free on top of a compaction's merged output: DuckDB's spill folder, the swap journal and
    /// whatever else writes to the volume while the merge runs.
    /// </summary>
    internal const long CompactionHeadroomBytes = 64L * 1024 * 1024;

    /// <summary>
    /// What a compaction pass leaves free for the main database (#5377): the larger of the flat
    /// <see cref="CompactionHeadroomBytes"/> and the database file's size. While the merges run the database
    /// keeps writing (its WAL grows up to the checkpoint threshold, a CHECKPOINT can grow the file by about its
    /// own size), so merged output may not take the volume down to the flat headroom. A missing or unreadable
    /// file counts as 0.
    /// </summary>
    internal static long CompactionReserveBytes(string databasePath)
    {
        long databaseBytes = 0;
        try
        {
            databaseBytes = File.Exists(databasePath) ? new FileInfo(databasePath).Length : 0;
        }
        catch (Exception)
        {
            /* unreadable: the flat headroom still applies */
        }

        return Math.Max(CompactionHeadroomBytes, databaseBytes);
    }

    /// <summary>
    /// Bytes available to this user on the volume holding <paramref name="path"/>, or null when they cannot
    /// be read (a network share has no drive letter, a removed drive, no permission). Null means "unknown",
    /// and callers treat unknown as enough, so a share is never warned about or blocked on a guess.
    /// Asks Windows about the folder itself (#5377): the drive root answers for the root volume, and a data
    /// folder on a mounted volume or a junction lives on another one. The drive root is the fallback.
    /// </summary>
    internal static long? GetAvailableFreeBytes(string path) =>
        GetAvailableFreeBytes(path, GetFolderFreeBytes, GetDriveRootFreeBytes);

    /// <summary>The same read with both sources passed in, so the fallback and unknown paths are testable without a mounted volume.</summary>
    internal static long? GetAvailableFreeBytes(
        string path, Func<string, long?> folderReader, Func<string, long?> driveRootReader)
    {
        try
        {
            var full = Path.GetFullPath(path);
            return folderReader(full) ?? driveRootReader(full);
        }
        catch (Exception)
        {
            return null;
        }
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetDiskFreeSpaceExW(
        string lpDirectoryName, out ulong lpFreeBytesAvailableToCaller, out ulong lpTotalNumberOfBytes, out ulong lpTotalNumberOfFreeBytes);

    /// <summary>Free bytes for the folder's own volume, walking up to the nearest existing parent; null when the call fails.</summary>
    private static long? GetFolderFreeBytes(string fullPath)
    {
        try
        {
            var folder = fullPath;
            while (!string.IsNullOrEmpty(folder) && !Directory.Exists(folder))
            {
                folder = Path.GetDirectoryName(folder);
            }

            if (string.IsNullOrEmpty(folder))
            {
                return null;
            }

            return GetDiskFreeSpaceExW(folder, out var available, out _, out _) && available <= long.MaxValue
                ? (long)available
                : null;
        }
        catch (Exception)
        {
            /* no kernel32 export (not Windows), or the call threw: fall back to the drive root */
            return null;
        }
    }

    private static long? GetDriveRootFreeBytes(string fullPath)
    {
        var root = Path.GetPathRoot(fullPath);
        if (string.IsNullOrEmpty(root))
        {
            return null;
        }

        var drive = new DriveInfo(root);
        return drive.IsReady ? drive.AvailableFreeSpace : null;
    }

    /// <summary>
    /// Logs one warning, and returns true, when <paramref name="folder"/>'s volume has less free space than
    /// <paramref name="neededBytes"/>. Names the folder, the bytes free and the bytes needed, and what needs
    /// them. Returns false, logging nothing, when there is enough or the free space is unknown.
    /// </summary>
    internal static bool WarnIfLow(
        ILogger? logger, string folder, long neededBytes, string reason, Func<string, long?> availableFreeBytes)
    {
        if (!IsLow(folder, neededBytes, availableFreeBytes, out var free))
        {
            return false;
        }

        logger?.LogWarning(
            "Low disk space for the Lite data folder {Folder}: {FreeBytes} bytes free ({Free}), {NeededBytes} bytes needed ({Needed}) for {Reason}. " +
            "Free space on that volume, or the archive cannot compact and the database cannot checkpoint, and writes can fail part way",
            folder, free, FormatBytes(free), neededBytes, FormatBytes(neededBytes), reason);
        return true;
    }

    /// <summary>
    /// True, with the bytes free, when <paramref name="folder"/>'s volume has less than
    /// <paramref name="neededBytes"/>. False when there is enough or the free space is unknown.
    /// </summary>
    internal static bool IsLow(string folder, long neededBytes, Func<string, long?> availableFreeBytes, out long free)
    {
        free = 0;
        if (neededBytes <= 0 || availableFreeBytes(folder) is not long observed || observed >= neededBytes)
        {
            return false;
        }

        free = observed;
        return true;
    }

    internal static string FormatBytes(long bytes)
    {
        const double Kib = 1024.0;
        const double Mib = Kib * 1024;
        const double Gib = Mib * 1024;
        return bytes switch
        {
            >= (long)Gib => (bytes / Gib).ToString("F2", CultureInfo.InvariantCulture) + " GiB",
            >= (long)Mib => (bytes / Mib).ToString("F1", CultureInfo.InvariantCulture) + " MiB",
            _ => (bytes / Kib).ToString("F0", CultureInfo.InvariantCulture) + " KiB"
        };
    }
}
