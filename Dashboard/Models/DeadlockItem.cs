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
    public class DeadlockItem
    {
        public long DeadlockId { get; set; }
        public DateTime CollectionTime { get; set; }
        public string ServerName { get; set; } = string.Empty;
        public string DeadlockType { get; set; } = string.Empty;
        public DateTime? EventDate { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public short? Spid { get; set; }
        public string DeadlockGroup { get; set; } = string.Empty;
        public string Query { get; set; } = string.Empty;
        public string ObjectNames { get; set; } = string.Empty;
        public string IsolationLevel { get; set; } = string.Empty;
        public string OwnerMode { get; set; } = string.Empty;
        public string WaiterMode { get; set; } = string.Empty;
        public string LockMode { get; set; } = string.Empty;
        public long? TransactionCount { get; set; }
        public string ClientOption1 { get; set; } = string.Empty;
        public string ClientOption2 { get; set; } = string.Empty;
        public string LoginName { get; set; } = string.Empty;
        public string HostName { get; set; } = string.Empty;
        public string ClientApp { get; set; } = string.Empty;
        public long? WaitTime { get; set; }
        public string WaitResource { get; set; } = string.Empty;
        public short? Priority { get; set; }
        public long? LogUsed { get; set; }
        public DateTime? LastTranStarted { get; set; }
        public DateTime? LastBatchStarted { get; set; }
        public DateTime? LastBatchCompleted { get; set; }
        public string TransactionName { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
        public string OwnerWaiterType { get; set; } = string.Empty;
        public string OwnerActivity { get; set; } = string.Empty;
        public string OwnerWaiterActivity { get; set; } = string.Empty;
        public string OwnerMerging { get; set; } = string.Empty;
        public string OwnerSpilling { get; set; } = string.Empty;
        public string OwnerWaitingToClose { get; set; } = string.Empty;
        public string WaiterWaiterType { get; set; } = string.Empty;
        public string WaiterOwnerActivity { get; set; } = string.Empty;
        public string WaiterWaiterActivity { get; set; } = string.Empty;
        public string WaiterMerging { get; set; } = string.Empty;
        public string WaiterSpilling { get; set; } = string.Empty;
        public string WaiterWaitingToClose { get; set; } = string.Empty;
        public string DeadlockGraph { get; set; } = string.Empty;

        public double? WaitTimeSec => WaitTime.HasValue ? WaitTime.Value / 1000.0 : null;
    }
}
