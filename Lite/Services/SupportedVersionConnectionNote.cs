/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9099: the line the Add and Edit server dialog's Test result adds when the server it reached is older than the versions this product
/// supports. The probe's major version and edition (the shared detection query) decide, through the same rule
/// (<see cref="CollectorEngineCapability.IsBelowSupportedVersion"/>) the collectors' gates and the not-collected sentences use, so the
/// dialog, the panels and the MCP tools agree on which servers are below the floor.
/// </summary>
internal static class SupportedVersionConnectionNote
{
    /// <summary>
    /// The sentence that names the version the test found and the supported versions, or null when the server is at or above the floor, is
    /// an Azure SQL Database or Managed Instance (whose major is not a product year), or the probe read no version (0).
    /// </summary>
    internal static string? For(int sqlMajorVersion, int engineEdition)
    {
        if (!CollectorEngineCapability.IsBelowSupportedVersion(sqlMajorVersion, engineEdition))
        {
            return null;
        }

        var running = MonitoredEngineVersion.DescribeEngineVersion(null, sqlMajorVersion, null, engineEdition);
        return $"This server runs {running}, which is older than the versions Performance Monitor supports ({CollectorEngineCapability.SupportedVersionsDescription}). " +
               "You can still add it, but the collectors that need a newer version do not run on it.";
    }
}
