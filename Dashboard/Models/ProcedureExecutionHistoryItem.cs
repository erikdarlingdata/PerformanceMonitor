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
    public class ProcedureExecutionHistoryItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime ServerStartTime { get; set; }
        public string ObjectType { get; set; } = string.Empty;
        public string? TypeDesc { get; set; }
        public DateTime CachedTime { get; set; }
        public DateTime LastExecutionTime { get; set; }

        // Cumulative values
        public long ExecutionCount { get; set; }
        public long IntervalExecutions { get; set; }
        public long TotalWorkerTime { get; set; }
        public long MinWorkerTime { get; set; }
        public long MaxWorkerTime { get; set; }
        public long TotalElapsedTime { get; set; }
        public long MinElapsedTime { get; set; }
        public long MaxElapsedTime { get; set; }
        public long TotalLogicalReads { get; set; }
        public long MinLogicalReads { get; set; }
        public long MaxLogicalReads { get; set; }
        public long TotalPhysicalReads { get; set; }
        public long MinPhysicalReads { get; set; }
        public long MaxPhysicalReads { get; set; }
        public long TotalLogicalWrites { get; set; }
        public long MinLogicalWrites { get; set; }
        public long MaxLogicalWrites { get; set; }
        public long? TotalSpills { get; set; }
        public long? MinSpills { get; set; }
        public long? MaxSpills { get; set; }

        // Query identifiers (hex strings from binary columns)
        public string? SqlHandle { get; set; }
        public string? PlanHandle { get; set; }

        // Delta values
        public long? ExecutionCountDelta { get; set; }
        public long? TotalWorkerTimeDelta { get; set; }
        public long? TotalElapsedTimeDelta { get; set; }
        public long? TotalLogicalReadsDelta { get; set; }
        public long? TotalPhysicalReadsDelta { get; set; }
        public long? TotalLogicalWritesDelta { get; set; }
        public int? SampleIntervalSeconds { get; set; }

        // Query plan
        public string? QueryPlanXml { get; set; }

        // Computed/display helpers
        public double? AvgWorkerTimeMs => ExecutionCount > 0 ? (double)TotalWorkerTime / ExecutionCount / 1000.0 : null;
        public double? AvgElapsedTimeMs => ExecutionCount > 0 ? (double)TotalElapsedTime / ExecutionCount / 1000.0 : null;
        public double? AvgLogicalReads => ExecutionCount > 0 ? (double)TotalLogicalReads / ExecutionCount : null;
        public double? AvgPhysicalReads => ExecutionCount > 0 ? (double)TotalPhysicalReads / ExecutionCount : null;
        public double? AvgLogicalWrites => ExecutionCount > 0 ? (double)TotalLogicalWrites / ExecutionCount : null;
        public double? AvgSpills => ExecutionCount > 0 && TotalSpills.HasValue ? (double)TotalSpills.Value / ExecutionCount : null;

        // Worker time = CPU time in SQL Server (microseconds to ms)
        public double MinWorkerTimeMs => MinWorkerTime / 1000.0;
        public double MaxWorkerTimeMs => MaxWorkerTime / 1000.0;
        public double MinElapsedTimeMs => MinElapsedTime / 1000.0;
        public double MaxElapsedTimeMs => MaxElapsedTime / 1000.0;
        public double TotalWorkerTimeMs => TotalWorkerTime / 1000.0;
        public double TotalElapsedTimeMs => TotalElapsedTime / 1000.0;

        // Delta display helpers (ms)
        public double? TotalWorkerTimeDeltaMs => TotalWorkerTimeDelta.HasValue ? TotalWorkerTimeDelta.Value / 1000.0 : null;
        public double? TotalElapsedTimeDeltaMs => TotalElapsedTimeDelta.HasValue ? TotalElapsedTimeDelta.Value / 1000.0 : null;
    }
}
