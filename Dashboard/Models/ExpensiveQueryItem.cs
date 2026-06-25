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
    public class ExpensiveQueryItem
    {
        public string Source { get; set; } = string.Empty;
        public string DatabaseName { get; set; } = string.Empty;
        public string ObjectIdentifier { get; set; } = string.Empty;
        public string? ObjectName { get; set; }
        public long ExecutionCount { get; set; }
        public decimal TotalWorkerTimeSec { get; set; }
        public decimal AvgWorkerTimeMs { get; set; }
        public decimal TotalElapsedTimeSec { get; set; }
        public decimal AvgElapsedTimeMs { get; set; }
        public long TotalLogicalReads { get; set; }
        public long AvgLogicalReads { get; set; }
        public long TotalLogicalWrites { get; set; }
        public long AvgLogicalWrites { get; set; }
        public long TotalPhysicalReads { get; set; }
        public long AvgPhysicalReads { get; set; }
        public decimal? MaxGrantMb { get; set; }
        public string QueryTextSample { get; set; } = string.Empty;
        public string? QueryPlanXml { get; set; }
        public DateTime? FirstExecutionTime { get; set; }
        public DateTime? LastExecutionTime { get; set; }
    }
}
