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
    public class CollectionLogEntry
    {
        public long LogId { get; set; }
        public DateTime CollectionTime { get; set; }
        public string CollectorName { get; set; } = string.Empty;
        public string CollectionStatus { get; set; } = string.Empty;
        public int RowsCollected { get; set; }
        public int DurationMs { get; set; }
        public string? ErrorMessage { get; set; }
    }
}
