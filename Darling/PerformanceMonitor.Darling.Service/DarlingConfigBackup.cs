/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5649: the ONE spelling of which files beside <c>darling.json</c> count as its backups. The service's
/// start-up hardening (<c>DarlingWorker.TryHardenConfigBackups</c>) and the CLI's permission check both take
/// EVERY <c>darling.json.bak-*</c> file, not only the <c>darling.json.bak-yyyyMMdd-HHmmss</c> names the config
/// verbs write, because a backup an operator named by hand holds the same secrets. The install and upgrade
/// scripts spell the same glob once (<c>BackupGlob</c> in <c>Get-DarlingServiceWrittenNames</c>), and a test pins
/// that spelling to <see cref="Glob"/> so the scripts' pre-lock check and this hardening cannot drift apart.
/// </summary>
internal static class DarlingConfigBackup
{
    /// <summary>The tail every backup name carries after the config file's own name, as a search pattern.</summary>
    public const string GlobSuffix = ".bak-*";

    /// <summary>The search pattern for every backup of the default <c>darling.json</c>.</summary>
    public const string Glob = "darling.json" + GlobSuffix;
}
