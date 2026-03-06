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
    public class QuerySnapshotItem
    {
        public DateTime CollectionTime { get; set; }
        public string Duration { get; set; } = string.Empty;
        public short SessionId { get; set; }
        public string? Status { get; set; }
        public string? WaitInfo { get; set; }
        public short? BlockingSessionId { get; set; }
        public short? BlockedSessionCount { get; set; }
        public string? DatabaseName { get; set; }
        public string? LoginName { get; set; }
        public string? HostName { get; set; }
        public string? ProgramName { get; set; }
        public string? SqlText { get; set; }
        public string? SqlCommand { get; set; }
        public long? Cpu { get; set; }
        public long? Reads { get; set; }
        public long? Writes { get; set; }
        public long? PhysicalReads { get; set; }
        public long? ContextSwitches { get; set; }
        public decimal? UsedMemoryMb { get; set; }
        public decimal? TempdbCurrentMb { get; set; }
        public decimal? TempdbAllocations { get; set; }
        public string? TranLogWrites { get; set; }
        public short? OpenTranCount { get; set; }
        public decimal? PercentComplete { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? TranStartTime { get; set; }
        public short? RequestId { get; set; }
        public string? AdditionalInfo { get; set; }
        public string? Locks { get; set; }
        public string? QueryPlan { get; set; }

        // Property alias for XAML binding compatibility
        public string? QueryText => SqlText;
    }
}
