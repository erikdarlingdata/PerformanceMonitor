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
    /// Bytes available to this user on the volume holding <paramref name="path"/>, or null when they cannot
    /// be read (a network share has no drive letter, a removed drive, no permission). Null means "unknown",
    /// and callers treat unknown as enough, so a share is never warned about or blocked on a guess.
    /// </summary>
    internal static long? GetAvailableFreeBytes(string path)
    {
        try
        {
            var root = Path.GetPathRoot(Path.GetFullPath(path));
            if (string.IsNullOrEmpty(root))
            {
                return null;
            }

            var drive = new DriveInfo(root);
            return drive.IsReady ? drive.AvailableFreeSpace : null;
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>
    /// Logs one warning, and returns true, when <paramref name="folder"/>'s volume has less free space than
    /// <paramref name="neededBytes"/>. Names the folder, the bytes free and the bytes needed, and what needs
    /// them. Returns false, logging nothing, when there is enough or the free space is unknown.
    /// </summary>
    internal static bool WarnIfLow(
        ILogger? logger, string folder, long neededBytes, string reason, Func<string, long?> availableFreeBytes)
    {
        if (neededBytes <= 0)
        {
            return false;
        }

        if (availableFreeBytes(folder) is not long free || free >= neededBytes)
        {
            return false;
        }

        logger?.LogWarning(
            "Low disk space for the Lite data folder {Folder}: {FreeBytes} bytes free ({Free}), {NeededBytes} bytes needed ({Needed}) for {Reason}. " +
            "Free space on that volume, or the archive cannot compact and the database cannot checkpoint, and writes can fail part way",
            folder, free, FormatBytes(free), neededBytes, FormatBytes(neededBytes), reason);
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
