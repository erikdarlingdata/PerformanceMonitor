/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>
/// The figures the Server Inventory derives from the collected server properties and metrics: the
/// Standard-edition license warning, and the Azure SQL Database hardware blanks and note. The health band is
/// <see cref="FinOpsUtilizationFigures.HealthBand"/>.
/// </summary>
public static class FinOpsInventoryFigures
{
    /// <summary>
    /// The license-limit warning for Standard edition (over 24 CPUs or 128 GB of memory), or null. The memory is the
    /// stored figure; where the engine edition's hardware columns are the host's it is blanked first, so no RAM warning
    /// can come from a host's memory.
    /// </summary>
    public static string? LicenseWarning(string? edition, int engineEdition, int cpuCount, long storedPhysicalMemoryMb)
    {
        var physicalMemoryMb = PhysicalMemoryMb(engineEdition, storedPhysicalMemoryMb);
        if (edition is null || !edition.Contains("Standard", StringComparison.OrdinalIgnoreCase)) return null;
        var warnings = new List<string>();
        if (cpuCount > 24) warnings.Add($"CPU: {cpuCount} cores (Standard limited to 24)");
        if (physicalMemoryMb is long ramMb && ramMb > 131072) warnings.Add($"RAM: {ramMb / 1024}GB (Standard limited to 128GB)");
        return warnings.Count > 0 ? string.Join("; ", warnings) : null;
    }

    /// <summary>The stored physical memory, or null when the engine edition's hardware columns are the host's.</summary>
    public static long? PhysicalMemoryMb(int engineEdition, long stored) =>
        ServerHardwareScope.HardwareIsTheHosts(engineEdition) ? null : stored;

    /// <summary>The stored socket count, or null when the engine edition's hardware columns are the host's.</summary>
    public static int? SocketCount(int engineEdition, int? stored) =>
        ServerHardwareScope.HardwareIsTheHosts(engineEdition) ? null : stored;

    /// <summary>The stored cores per socket, or null when the engine edition's hardware columns are the host's.</summary>
    public static int? CoresPerSocket(int engineEdition, int? stored) =>
        ServerHardwareScope.HardwareIsTheHosts(engineEdition) ? null : stored;

    /// <summary>The stored hardware note, else the host-scoped note when the hardware columns are the host's, else null.</summary>
    public static string? HardwareNote(int engineEdition, string? stored) =>
        stored ?? (ServerHardwareScope.HardwareIsTheHosts(engineEdition) ? ServerHardwareScope.InventoryHardwareNote : null);
}
