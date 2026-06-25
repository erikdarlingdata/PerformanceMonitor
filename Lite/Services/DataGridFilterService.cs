/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Provides operator-based filtering for DataGrid column filters. Forwards to the shared
/// <see cref="ColumnFilterMatcher"/> so Lite and Dashboard share one matching implementation.
/// </summary>
public static class DataGridFilterService
{
    /// <summary>
    /// Checks if an item matches a ColumnFilterState using operator-based filtering.
    /// </summary>
    public static bool MatchesFilter(object item, ColumnFilterState filter) =>
        ColumnFilterMatcher.MatchesFilter(item, filter);
}
