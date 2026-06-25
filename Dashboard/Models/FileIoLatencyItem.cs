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
    public class FileIoLatencyItem
    {
        public string DatabaseName { get; set; } = string.Empty;
        public string FileType { get; set; } = string.Empty;
        public string FileName { get; set; } = string.Empty;
        public decimal AvgReadLatencyMs { get; set; }
        public decimal AvgWriteLatencyMs { get; set; }
        public long ReadsLast15Min { get; set; }
        public long WritesLast15Min { get; set; }
        public string LatencyIssue { get; set; } = string.Empty;
        public string Recommendation { get; set; } = string.Empty;
        public DateTime LastSeen { get; set; }
    }
}
