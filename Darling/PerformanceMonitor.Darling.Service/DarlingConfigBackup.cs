/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5649: the ONE spelling of which files beside <c>darling.json</c> count as its backups. The service's
/// start-up hardening (<c>DarlingWorker.TryHardenConfigBackups</c>) and the CLI's permission check both take
/// EVERY <c>darling.json.bak-*</c> file, not only the <c>darling.json.bak-yyyyMMdd-HHmmss</c> names the config
/// verbs write, because a backup an operator named by hand holds the same secrets. The install and upgrade
/// scripts spell the same prefix once (<c>BackupPrefix</c> in <c>Get-DarlingServiceWrittenNames</c>) and test it with
/// an ordinal, case-insensitive <c>StartsWith</c>; a test pins that spelling to <see cref="Prefix"/> so the scripts'
/// pre-lock check and this hardening cannot drift apart. The search pattern handed to a directory listing only
/// narrows the list: <see cref="IsBackupName"/> decides, so the two sides cannot read a name differently under
/// another culture.
/// </summary>
internal static class DarlingConfigBackup
{
    /// <summary>The tail every backup name carries after the config file's own name.</summary>
    public const string Tail = ".bak-";

    /// <summary>The tail every backup name carries after the config file's own name, as a search pattern.</summary>
    public const string GlobSuffix = Tail + "*";

    /// <summary>The start of every backup name of the default <c>darling.json</c>.</summary>
    public const string Prefix = "darling.json" + Tail;

    /// <summary>The name the config verbs give the backup they write beside <paramref name="configPath"/>: the config path, then
    /// <see cref="Tail"/>, then <paramref name="stamp"/>, and <c>-N</c> after the stamp when <paramref name="suffix"/> is 2 or more
    /// (the stamp has one-second resolution, so a second edit in the same second takes the next free number).
    /// The writer builds its name here, so <see cref="Tail"/> is the one spelling for both the name that is written and the
    /// names <see cref="IsBackupName"/> reads back.</summary>
    public static string BackupPath(string configPath, string stamp, int suffix = 1) =>
        suffix <= 1 ? configPath + Tail + stamp : configPath + Tail + stamp + "-" + suffix.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Whether <paramref name="fileName"/> is a backup of the config file named <paramref name="configFileName"/>:
    /// it starts with that name plus <see cref="Tail"/>, compared ordinally and ignoring case, as Windows compares names.
    /// A name ending in a dot or a space is never a backup: Windows folds that tail away, so the name resolves to a
    /// DIFFERENT file, and the install and upgrade scripts' <c>Test-DarlingServiceWrittenFileName</c> refuses it the same way.</summary>
    public static bool IsBackupName(string configFileName, string fileName) =>
        !fileName.EndsWith('.') && !fileName.EndsWith(' ') &&
        fileName.StartsWith(configFileName + Tail, StringComparison.OrdinalIgnoreCase);

    /// <summary>The backups of the config file named <paramref name="configFileName"/> directly in
    /// <paramref name="directory"/>: the directory listing's own pattern narrows the list, and <see cref="IsBackupName"/>
    /// decides. Throws what the listing throws.</summary>
    public static string[] ListBackups(string directory, string configFileName) =>
        Directory.GetFiles(directory, configFileName + GlobSuffix)
            .Where(file => IsBackupName(configFileName, Path.GetFileName(file)))
            .ToArray();
}
