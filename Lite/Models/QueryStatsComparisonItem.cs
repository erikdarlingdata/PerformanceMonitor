/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Models;

public class QueryStatsComparisonItem : ComparisonItemBase
{
    public string QueryHash { get; set; } = "";
    public string QueryText { get; set; } = "";
}
