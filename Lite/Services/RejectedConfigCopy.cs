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
using System.Linq;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9761, #9772: keeps a config file Lite could not read (servers.json, profiles.json, collection_schedule.json) before anything
/// can write over it, so a hand edit with one wrong character is never lost.
/// </summary>
internal static class RejectedConfigCopy
{
    /// <summary>
    /// Copies a config file that could not be read to <c>&lt;file&gt;.rejected-&lt;UTC stamp&gt;</c> beside it and returns that path.
    /// A copy is never overwritten, and a rejected file that already has a byte-identical copy beside it keeps that copy instead
    /// of adding another, so starting Lite again on a file the user has not fixed yet adds nothing. When no copy can be made the
    /// result says so instead of a path.
    /// </summary>
    internal static string Keep(string filePath)
    {
        try
        {
            var bytes = File.ReadAllBytes(filePath);
            var directory = Path.GetDirectoryName(filePath)!;
            foreach (var kept in Directory.EnumerateFiles(directory, Path.GetFileName(filePath) + ".rejected-*"))
            {
                if (File.ReadAllBytes(kept).AsSpan().SequenceEqual(bytes))
                {
                    return kept;
                }
            }

            var copy = filePath + ".rejected-" + DateTime.UtcNow.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
            File.Copy(filePath, copy, overwrite: false);
            return copy;
        }
        catch (Exception ex)
        {
            return $"nowhere (no copy could be made: {ex.GetType().Name})";
        }
    }
}
