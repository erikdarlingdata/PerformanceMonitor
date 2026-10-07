/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What Import Settings copies from a previous install's folder into this one, as named lists so a test can read
/// them. The list is short on purpose and <see cref="InstallIdStore.FileName"/> is never on it (#4961): the
/// previous install may still run, and two installs that shared an id would share Extended Events session names
/// and drop each other's sessions. Servers and credential profiles are not in these lists; they are imported
/// through <see cref="ServerManager"/> and <see cref="ProfileManager"/>.
/// </summary>
internal static class SettingsImport
{
    /// <summary>Files copied from the previous install's <c>config</c> folder into this install's.</summary>
    internal static readonly string[] ConfigFiles = ["settings.json", "collection_schedule.json", "ignored_wait_types.json"];

    /// <summary>Files copied from the previous install's data root into this install's.</summary>
    internal static readonly string[] RootFiles = ["alert_state.json"];

    /// <summary>
    /// Copies each listed file the previous install has and this one does not, and returns how many it copied. A
    /// file this install already has is kept as it is.
    /// </summary>
    internal static int CopyMissing(string oldConfigDir, string oldRootDir, string newConfigDir, string newRootDir) =>
        CopyEach(ConfigFiles, oldConfigDir, newConfigDir) + CopyEach(RootFiles, oldRootDir, newRootDir);

    private static int CopyEach(string[] fileNames, string fromDir, string toDir)
    {
        var copied = 0;
        foreach (var fileName in fileNames)
        {
            var source = Path.Combine(fromDir, fileName);
            var target = Path.Combine(toDir, fileName);

            if (File.Exists(source) && !File.Exists(target))
            {
                File.Copy(source, target);
                copied++;
            }
        }

        return copied;
    }
}
