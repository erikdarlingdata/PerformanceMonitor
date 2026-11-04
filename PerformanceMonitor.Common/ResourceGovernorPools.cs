/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// Names for the two resource pools every SQL Server instance has (#7416). The memory grant charts label a line by pool, and "Pool 1" and "Pool 2"
/// left a reader to know that 1 is the internal pool and 2 the default pool. Any other pool keeps its number.
/// </summary>
public static class ResourceGovernorPools
{
    public static string Name(int poolId) => poolId switch
    {
        1 => "Internal pool",
        2 => "Default pool",
        _ => "Pool " + poolId.ToString(CultureInfo.InvariantCulture),
    };
}
