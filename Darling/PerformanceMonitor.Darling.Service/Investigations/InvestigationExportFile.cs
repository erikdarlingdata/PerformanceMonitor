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

    /// <summary>Who can read a file <see cref="CreateOwnerOnly"/> makes, in words for the command's success line and the README.</summary>
    internal static string ReadableBy => OperatingSystem.IsWindows()
        ? "you, the administrators and the service's operator group (the same people who can read darling.json)"
        : "the current user only";

    /// <summary>
    /// Creates the web download's work file in <paramref name="folder"/> (the service's own data folder, never the system temp folder),
    /// opened ONCE for write and read: the cases are written, the handle is rewound and the same handle is sent, so nothing reopens
    /// the file by name. On Windows the system deletes it when the handle closes, even after a crash
    /// (<see cref="FileOptions.DeleteOnClose"/>); elsewhere the name is removed right after the create, so no name is left to find.
    /// Same hardened ACL or owner-only mode as <see cref="CreateOwnerOnly"/>. #6236.
    /// </summary>
    internal static FileStream CreateScratch(string folder)
    {
        Directory.CreateDirectory(folder);
        var path = Path.Combine(folder, "darling-export-" + Guid.NewGuid().ToString("N") + ".partial");
        if (OperatingSystem.IsWindows())
        {
            return DarlingFileSecurity.CreateHardenedFile(
                path, allowInteractiveRead: true,
                System.Security.AccessControl.FileSystemRights.Read | System.Security.AccessControl.FileSystemRights.Write
                | System.Security.AccessControl.FileSystemRights.Delete,
                FileOptions.DeleteOnClose | FileOptions.Asynchronous);
        }

        var stream = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.ReadWrite,
            Share = FileShare.None,
            Options = FileOptions.Asynchronous,
            UnixCreateMode = DarlingManagedRoles.OwnerOnlyFile,
        });
        try
        {
            File.Delete(path);
        }
        catch
        {
            stream.Dispose();
            throw;
        }

        return stream;
    }

    /// <summary>Deletes <paramref name="path"/> when it is there. False when it is still there afterwards (a lock, a permission), so
    /// the caller can say where it is.</summary>
    internal static bool TryDelete(string path)
    {
        try
        {
            File.Delete(path);
            return !File.Exists(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }
}
