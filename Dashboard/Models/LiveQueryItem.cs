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
    public class LiveQueryItem
    {
        public DateTime SnapshotTime { get; set; }
        public int SessionId { get; set; }
        public string? DatabaseName { get; set; }
        public string ElapsedTimeFormatted { get; set; } = string.Empty;
        public string? QueryText { get; set; }
        public string? QueryPlan { get; set; }
        public string? LiveQueryPlan { get; set; }
        public string? Status { get; set; }
        public int BlockingSessionId { get; set; }
        public string? WaitType { get; set; }
        public long WaitTimeMs { get; set; }
        public string? WaitResource { get; set; }
        public long CpuTimeMs { get; set; }
        public long TotalElapsedTimeMs { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public long LogicalReads { get; set; }
        public decimal GrantedQueryMemoryGb { get; set; }
        public string? TransactionIsolationLevel { get; set; }
        public int Dop { get; set; }
        public int ParallelWorkerCount { get; set; }
        public string? LoginName { get; set; }
        public string? HostName { get; set; }
        public string? ProgramName { get; set; }
        public int OpenTransactionCount { get; set; }
        public decimal PercentComplete { get; set; }

        public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlan);
        public bool HasLiveQueryPlan => !string.IsNullOrEmpty(LiveQueryPlan);
    }
}
