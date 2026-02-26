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
    public class QueryStatsHistoryItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime ServerStartTime { get; set; }
        public string ObjectType { get; set; } = string.Empty;
        public DateTime CreationTime { get; set; }
        public DateTime LastExecutionTime { get; set; }

        // Execution count
        public long ExecutionCount { get; set; }
        public long? ExecutionCountDelta { get; set; }
        public long IntervalExecutions { get; set; }

        // Worker time (CPU) - microseconds in database
        public long TotalWorkerTime { get; set; }
        public long MinWorkerTime { get; set; }
        public long MaxWorkerTime { get; set; }
        public long? TotalWorkerTimeDelta { get; set; }

        // Elapsed time - microseconds in database
        public long TotalElapsedTime { get; set; }
        public long MinElapsedTime { get; set; }
        public long MaxElapsedTime { get; set; }
        public long? TotalElapsedTimeDelta { get; set; }

        // Logical reads
        public long TotalLogicalReads { get; set; }
        public long? TotalLogicalReadsDelta { get; set; }

        // Physical reads
        public long TotalPhysicalReads { get; set; }
        public long MinPhysicalReads { get; set; }
        public long MaxPhysicalReads { get; set; }
        public long? TotalPhysicalReadsDelta { get; set; }

        // Logical writes
        public long TotalLogicalWrites { get; set; }
        public long? TotalLogicalWritesDelta { get; set; }

        // CLR time
        public long TotalClrTime { get; set; }

        // Rows
        public long TotalRows { get; set; }
        public long MinRows { get; set; }
        public long MaxRows { get; set; }

        // Parallelism
        public short MinDop { get; set; }
        public short MaxDop { get; set; }

        // Memory grants (KB)
        public long MinGrantKb { get; set; }
        public long MaxGrantKb { get; set; }
        public long MinUsedGrantKb { get; set; }
        public long MaxUsedGrantKb { get; set; }
        public long MinIdealGrantKb { get; set; }
        public long MaxIdealGrantKb { get; set; }

        // Thread usage
        public int MinReservedThreads { get; set; }
        public int MaxReservedThreads { get; set; }
        public int MinUsedThreads { get; set; }
        public int MaxUsedThreads { get; set; }

        // Spills
        public long TotalSpills { get; set; }
        public long MinSpills { get; set; }
        public long MaxSpills { get; set; }

        // Query identifiers (hex strings from binary columns)
        public string? SqlHandle { get; set; }
        public string? PlanHandle { get; set; }
        public string? QueryHash { get; set; }
        public string? QueryPlanHash { get; set; }

        // Sample interval
        public int? SampleIntervalSeconds { get; set; }

        // Query plan
        public string? QueryPlanXml { get; set; }

        // Display helpers - convert microseconds to milliseconds
        public double TotalWorkerTimeMs => TotalWorkerTime / 1000.0;
        public double MinWorkerTimeMs => MinWorkerTime / 1000.0;
        public double MaxWorkerTimeMs => MaxWorkerTime / 1000.0;
        public double? TotalWorkerTimeDeltaMs => TotalWorkerTimeDelta / 1000.0;
        public double? AvgWorkerTimeMs => ExecutionCount > 0 ? TotalWorkerTime / (double)ExecutionCount / 1000.0 : null;

        public double TotalElapsedTimeMs => TotalElapsedTime / 1000.0;
        public double MinElapsedTimeMs => MinElapsedTime / 1000.0;
        public double MaxElapsedTimeMs => MaxElapsedTime / 1000.0;
        public double? TotalElapsedTimeDeltaMs => TotalElapsedTimeDelta / 1000.0;
        public double? AvgElapsedTimeMs => ExecutionCount > 0 ? TotalElapsedTime / (double)ExecutionCount / 1000.0 : null;

        public double? AvgLogicalReads => ExecutionCount > 0 ? TotalLogicalReads / (double)ExecutionCount : null;
        public double? AvgPhysicalReads => ExecutionCount > 0 ? TotalPhysicalReads / (double)ExecutionCount : null;
        public double? AvgLogicalWrites => ExecutionCount > 0 ? TotalLogicalWrites / (double)ExecutionCount : null;
        public double? AvgRows => ExecutionCount > 0 ? TotalRows / (double)ExecutionCount : null;

        // Memory in MB
        public double MaxGrantMb => MaxGrantKb / 1024.0;
        public double MaxUsedGrantMb => MaxUsedGrantKb / 1024.0;
        public double MaxIdealGrantMb => MaxIdealGrantKb / 1024.0;
    }
}
