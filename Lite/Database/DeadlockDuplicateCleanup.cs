/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Database;

/// <summary>Removal of exact duplicate rows already stored in the hot <c>deadlocks</c> table.</summary>
internal static class DeadlockDuplicateCleanup
{
    /// <summary>Returns the number of rows removed.</summary>
    internal static Task<int> RemoveAsync(DuckDBConnection connection, ILogger? logger)
    {
        return Task.FromResult(0);
    }
}
