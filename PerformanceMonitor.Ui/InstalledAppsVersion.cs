/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security;
using Microsoft.Win32;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The version Windows shows for a Setup.exe install under Settings &gt; Apps &gt; Installed apps: the
/// <c>DisplayVersion</c> value of the install's entry beneath the current user's Uninstall key, named after the
/// package id. The Setup.exe and the updater write major.minor.patch there (#8430), so every nightly of one release
/// reads the same as the release itself. The app writes its full installed version over that value each time it
/// starts, prerelease tag included, which is the version the About window shows.
/// </summary>
public static class InstalledAppsVersion
{
    /// <summary>The current user's key that holds one entry per Setup.exe install.</summary>
    public const string UninstallKeyPath = @"Software\Microsoft\Windows\CurrentVersion\Uninstall";

    private const string DisplayVersion = "DisplayVersion";

    /// <summary>
    /// Stamps the running install's entry in the current user's Uninstall key. A registry that refuses the read or
    /// the write leaves the entry as it is: the value is a display line, and the app starts without it.
    /// </summary>
    public static void StampCurrentUser(string? packageId, string? installedVersion, bool isPortable)
    {
        try
        {
            using var uninstall = Registry.CurrentUser.OpenSubKey(UninstallKeyPath);
            if (uninstall is not null)
            {
                Stamp(uninstall, packageId, installedVersion, isPortable);
            }
        }
        catch (Exception ex) when (ex is SecurityException or UnauthorizedAccessException or IOException)
        {
        }
    }

    /// <summary>
    /// Sets <c>DisplayVersion</c> of <paramref name="packageId"/>'s entry under <paramref name="uninstallRoot"/> to
    /// <paramref name="installedVersion"/>, and returns whether it wrote. It writes nothing when the entry already
    /// shows that version, which is every stable release. It writes nothing for a copy that was not installed (the
    /// zip and the viewer folder beside the service have no package id and no installed version), for a portable
    /// copy (it shares its package id with a Setup.exe install, and the entry describes that install), or when the
    /// entry does not exist. It never creates an entry.
    /// </summary>
    public static bool Stamp(RegistryKey uninstallRoot, string? packageId, string? installedVersion, bool isPortable)
    {
        ArgumentNullException.ThrowIfNull(uninstallRoot);

        if (isPortable || string.IsNullOrEmpty(packageId) || string.IsNullOrEmpty(installedVersion))
        {
            return false;
        }

        using var entry = uninstallRoot.OpenSubKey(packageId, writable: true);
        if (entry is null)
        {
            return false;
        }

        if (entry.GetValue(DisplayVersion) is string shown && string.Equals(shown, installedVersion, StringComparison.Ordinal))
        {
            return false;
        }

        entry.SetValue(DisplayVersion, installedVersion, RegistryValueKind.String);
        return true;
    }
}
