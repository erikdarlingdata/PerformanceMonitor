/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The local group whose members can read darling.json and the store's admin and viewer credentials without
/// elevation, and the message the Viewer shows when a read of one of those files is refused.
///
/// <para>The Viewer does not reference the service project, so the group name is repeated here. It mirrors
/// <c>DarlingFileSecurity.OperatorGroupName</c> (<c>DarlingOperatorGroup.Name</c>) in the service project and
/// must change with it: the service grants that group and install-darling.ps1 creates it, so a different name
/// here would send the person to a group nothing grants.</para>
/// </summary>
internal static class ViewerOperatorGroup
{
    public const string Name = "PerformanceMonitor Darling Operators";

    /// <summary>
    /// What a person sees when the file at <paramref name="path"/> is restricted and their account is not allowed
    /// to read it. A group membership is put into a logon session when it starts, so an account added to the group
    /// has to sign out and back in before it can read the file.
    /// </summary>
    public static string AccessDeniedMessage(string path) =>
        $"{path} is restricted, and this account is not allowed to read it. Ask an administrator to add your " +
        $"account to the local group \"{Name}\" (installed by install-darling.ps1), then sign out and back in " +
        "so the new group membership takes effect.";

    /// <summary>
    /// The exception a refused read is rethrown as: the message above, then the original exception's text, so the
    /// cause is not lost. Still an <see cref="UnauthorizedAccessException"/>, so a caller that catches that type
    /// keeps working.
    /// </summary>
    public static UnauthorizedAccessException AccessDenied(string path, UnauthorizedAccessException original) =>
        new($"{AccessDeniedMessage(path)} ({original.Message})", original);

    /// <summary>
    /// Whether <paramref name="path"/> exists but this account is not allowed to open it. <see cref="File.Exists"/>
    /// cannot say: it returns false for a file the account may not reach, such as one in a folder it may not
    /// traverse, so a restricted file would be reported as a missing one. Opening it tells the two apart.
    /// </summary>
    public static bool IsRefused(string path)
    {
        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return true;
        }
        catch (IOException)
        {
            // Not there, or in use in a way that is not a permission problem: the caller's own message applies.
            return false;
        }
    }
}
