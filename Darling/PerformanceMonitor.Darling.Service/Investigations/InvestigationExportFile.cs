/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.IO;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>The files an investigation export is written through (#6236): a partial file only the current user can read, from the
/// moment it exists.</summary>
internal static class InvestigationExportFile
{
    /// <summary>
    /// Creates <paramref name="path"/>, which must not exist, for writing. <see cref="FileMode.CreateNew"/> fails on anything already
    /// there (a file or a link) instead of writing through it. On Windows the file carries the hardened ACL the credential files
    /// carry from its first moment (<see cref="DarlingFileSecurity.CreateHardenedFile"/>: the operators who may read darling.json);
    /// elsewhere its mode is owner read and write (<see cref="DarlingManagedRoles.OwnerOnlyFile"/>).
    /// </summary>
    internal static FileStream CreateOwnerOnly(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return DarlingFileSecurity.CreateHardenedFile(path, allowInteractiveRead: true);
        }

        return new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            UnixCreateMode = DarlingManagedRoles.OwnerOnlyFile,
        });
    }

    /// <summary>Deletes <paramref name="path"/> when it is there; a file that cannot be removed is left (the next export names its own).</summary>
    internal static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Nothing more to do: the export already failed or finished. */
        }
    }
}
