/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Win32;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The version Windows shows for a Setup.exe install under Settings &gt; Apps &gt; Installed apps: the
/// <c>DisplayVersion</c> value of the install's entry beneath the current user's Uninstall key, named after the
/// package id. The Setup.exe and the updater write major.minor.patch there, and the app leaves it as they wrote it.
/// </summary>
public static class InstalledAppsVersion
{
    /// <summary>Leaves <paramref name="packageId"/>'s entry under <paramref name="uninstallRoot"/> as it is, and
    /// returns false: nothing was written.</summary>
    public static bool Stamp(RegistryKey uninstallRoot, string? packageId, string? installedVersion, bool isPortable)
    {
        _ = uninstallRoot;
        _ = packageId;
        _ = installedVersion;
        _ = isPortable;
        return false;
    }
}
