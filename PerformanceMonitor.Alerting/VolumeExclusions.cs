/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The fleet-wide excluded-volumes list of the Volume Free Space alert (#6098): mount points the operator has
/// decided are not worth an alert (a scratch volume that sits full by design, a read-only media volume, a
/// system volume on a host the team does not run). The match rule lives here once so both products, the
/// store seed and the Settings windows read one definition.
///
/// <para><b>Match rule.</b> The whole mount point, compared case-insensitively, and tolerant of a trailing
/// <c>\</c> or <c>/</c> on either side: <c>S:</c>, <c>S:\</c> and <c>s:\</c> are one volume, and so are
/// <c>/var/opt/mssql</c> and <c>/var/opt/mssql/</c>. It is never a prefix match: excluding <c>D:\</c> must not
/// swallow <c>D:\Data</c>, a different mount. A root mount (<c>/</c>) keeps its slash, so it matches only
/// itself.</para>
///
/// <para>Excluded volumes are removed BEFORE the breach test, so the worst-volume pick, the severity tier and
/// the alert's context all come from the volumes that remain; a server whose only low volumes are excluded
/// takes the ordinary resolved path.</para>
/// </summary>
public static class VolumeExclusions
{
    /// <summary>The comparison form of a mount point: trimmed, trailing separators dropped, a bare root kept
    /// as <c>/</c>. Null for a blank entry, which matches nothing.</summary>
    public static string? Key(string? mountPoint)
    {
        var trimmed = mountPoint?.Trim() ?? "";
        if (trimmed.Length == 0)
        {
            return null;
        }

        var stripped = trimmed.TrimEnd('\\', '/');
        return stripped.Length == 0 ? "/" : stripped;
    }

    /// <summary>Trims, drops blanks and de-duplicates by <see cref="Key"/> (so <c>S:</c> and <c>S:\</c> keep
    /// the first one typed), keeping first-seen order. Applied on the way into a store or a settings file so
    /// text typed in a Settings window and a JSON array arriving through MCP mean the same thing.</summary>
    public static IReadOnlyList<string> Normalize(IEnumerable<string>? raw)
    {
        if (raw is null)
        {
            return Array.Empty<string>();
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();
        foreach (var entry in raw)
        {
            var key = Key(entry);
            if (key is not null && seen.Add(key))
            {
                result.Add(entry!.Trim());
            }
        }

        return result;
    }

    /// <summary>True when <paramref name="mountPoint"/> is named by <paramref name="excluded"/>.</summary>
    public static bool IsExcluded(string? mountPoint, IReadOnlyList<string>? excluded)
    {
        if (excluded is null || excluded.Count == 0)
        {
            return false;
        }

        var key = Key(mountPoint);
        if (key is null)
        {
            return false;
        }

        foreach (var entry in excluded)
        {
            if (string.Equals(Key(entry), key, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The volumes that remain after the excluded ones are dropped. Returns the input list itself
    /// when nothing is excluded.</summary>
    public static List<VolumeFreeSpaceInfo> Filter(List<VolumeFreeSpaceInfo> volumes, IReadOnlyList<string>? excluded)
    {
        if (excluded is null || excluded.Count == 0)
        {
            return volumes;
        }

        return volumes.Where(v => !IsExcluded(v.MountPoint, excluded)).ToList();
    }
}
