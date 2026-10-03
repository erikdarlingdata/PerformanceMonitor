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
/// The figures the Server Inventory derives from the collected server properties and metrics: the health score, the
/// Standard-edition license warning, and the Azure SQL Database hardware blanks and note. The health band is
/// <see cref="FinOpsUtilizationFigures.HealthBand"/>.
/// </summary>
public static class FinOpsInventoryFigures
{
    /// <summary>The 0-100 health score for a server's average CPU; a null average leaves the CPU term out.</summary>
    public static int HealthScore(decimal? avgCpuPct)
    {
        /* A server with no CPU sample in the window has a null average: its CPU term is left out, because scoring it
           as 0% CPU would hand it a full 100 made from nothing. */
        int? cpuScore = avgCpuPct is decimal avgCpu ? FinOpsHealthCalculator.CpuScore(avgCpu) : null;
        var memScore = 80;
        var storScore = FinOpsHealthCalculator.StorageScore(50);
        return FinOpsHealthCalculator.Overall(cpuScore, memScore, storScore);
    }

    /// <summary>The license-limit warning for Standard edition (over 24 CPUs or 128 GB of memory), or null.</summary>
    public static string? LicenseWarning(string? edition, int cpuCount, long physicalMemoryMb)
    {
        if (edition is null || !edition.Contains("Standard", StringComparison.OrdinalIgnoreCase)) return null;
        var warnings = new List<string>();
        if (cpuCount > 24) warnings.Add($"CPU: {cpuCount} cores (Standard limited to 24)");
        if (physicalMemoryMb > 131072) warnings.Add($"RAM: {physicalMemoryMb / 1024}GB (Standard limited to 128GB)");
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
