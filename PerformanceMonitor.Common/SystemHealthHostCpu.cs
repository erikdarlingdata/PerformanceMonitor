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

namespace PerformanceMonitor.Common;

/// <summary>
/// Whether the system_health SYSTEM component's <c>systemCpuUtilization</c> can be believed for a server (#7416 item 1).
/// On SQL Server on Linux the scheduler monitor's <c>SystemIdle</c> reports 0 on some builds, so the host figure derived
/// from it reads 100 on every row while the CPU tab on the same server reads 0 to 1 percent. The CPU collector already
/// stores NULL for the host share in that case. This is the same rule for the system_health read, and it decides from
/// what the server IS, not from the value, so a real 100 percent on a Windows host still draws.
/// <para>The only stored host platform is <c>server_properties.host_os_version</c>: <c>host_distribution</c> from
/// <c>sys.dm_os_host_info</c> ("Windows" on Windows, the distribution name on Linux), or the <c>@@VERSION</c> suffix on
/// a build without that view ("Windows Server ..." or "Linux (...)"). A blank value says nothing about the platform, so
/// the figure is kept.</para>
/// </summary>
public static class SystemHealthHostCpu
{
    /// <summary>The reason shown wherever the System CPU series is left out.</summary>
    public const string LinuxNote =
        "System CPU is not shown for this server. SQL Server on Linux reports the host's idle CPU as 0 on some builds, "
        + "so system_cpu_utilization would read 100% whatever the host is doing. SQL CPU is unaffected.";

    /// <summary>True when the stored host platform names something other than Windows (SQL Server on Linux).</summary>
    public static bool IsHostCpuUnreported(string? hostOsVersion)
    {
        var os = hostOsVersion?.Trim();
        if (string.IsNullOrEmpty(os))
            return false;

        return !os.StartsWith("Windows", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The reason to publish beside the read, or null when the figure can be trusted (or the platform is unknown).</summary>
    public static string? NoteFor(string? hostOsVersion) => IsHostCpuUnreported(hostOsVersion) ? LinuxNote : null;

    /// <summary>The snapshots with <c>SystemCpuUtilization</c> cleared when <paramref name="hostOsVersion"/> says the figure
    /// cannot be known. The list is returned unchanged for a platform that can report it.</summary>
    public static List<SystemHealthRecord> Apply(List<SystemHealthRecord> rows, string? hostOsVersion)
    {
        if (!IsHostCpuUnreported(hostOsVersion))
            return rows;

        return rows.Select(r => r with { SystemCpuUtilization = null }).ToList();
    }
}
