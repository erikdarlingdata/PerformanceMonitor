// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserMemoryConditionItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public string LastNotification { get; set; } = string.Empty;
        public long? OutOfMemoryExceptions { get; set; }
        public bool? IsAnyPoolOutOfMemory { get; set; }
        public long? ProcessOutOfMemoryPeriod { get; set; }
        public string Name { get; set; } = string.Empty;
        public long? AvailablePhysicalMemoryGb { get; set; }
        public long? AvailableVirtualMemoryGb { get; set; }
        public long? AvailablePagingFileGb { get; set; }
        public long? WorkingSetGb { get; set; }
        public long? PercentOfCommittedMemoryInWs { get; set; }
        public long? PageFaults { get; set; }
        public long? SystemPhysicalMemoryHigh { get; set; }
        public long? SystemPhysicalMemoryLow { get; set; }
        public long? ProcessPhysicalMemoryLow { get; set; }
        public long? ProcessVirtualMemoryLow { get; set; }
        public long? VmReservedGb { get; set; }
        public long? VmCommittedGb { get; set; }
        public long? TargetCommittedGb { get; set; }
        public long? CurrentCommittedGb { get; set; }
    }
}
