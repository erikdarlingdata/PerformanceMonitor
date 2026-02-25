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
    public class FileIoLatencyTimeSeriesItem
    {
        public DateTime CollectionTime { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public string FileName { get; set; } = string.Empty;
        public string FileType { get; set; } = string.Empty;
        public decimal ReadLatencyMs { get; set; }
        public decimal WriteLatencyMs { get; set; }
        public long ReadCount { get; set; }
        public long WriteCount { get; set; }
        public decimal ReadQueuedLatencyMs { get; set; }
        public decimal WriteQueuedLatencyMs { get; set; }
        public decimal ReadThroughputMbPerSec { get; set; }
        public decimal WriteThroughputMbPerSec { get; set; }
    }
}
