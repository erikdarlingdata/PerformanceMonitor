/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorDashboard.Models
{
    /// <summary>
    /// Represents aggregated duration data for a collection time point.
    /// Used by Performance Trends duration charts.
    /// </summary>
    public class DurationTrendItem
    {
        public DateTime CollectionTime { get; set; }
        public double AvgDurationMs { get; set; }
    }
}
