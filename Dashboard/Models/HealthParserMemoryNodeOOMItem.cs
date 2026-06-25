// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserMemoryNodeOOMItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public long? NodeId { get; set; }
        public long? MemoryNodeId { get; set; }
        public long? MemoryUtilizationPct { get; set; }
        public long? TotalPhysicalMemoryKb { get; set; }
        public long? AvailablePhysicalMemoryKb { get; set; }
        public long? TotalPageFileKb { get; set; }
        public long? AvailablePageFileKb { get; set; }
        public long? TotalVirtualAddressSpaceKb { get; set; }
        public long? AvailableVirtualAddressSpaceKb { get; set; }
        public long? TargetKb { get; set; }
        public long? ReservedKb { get; set; }
        public long? CommittedKb { get; set; }
        public decimal? SharedCommittedKb { get; set; }
        public long? AweKb { get; set; }
        public long? PagesKb { get; set; }
        public string FailureType { get; set; } = string.Empty;
        public int? FailureValue { get; set; }
        public int? Resources { get; set; }
        public string FactorText { get; set; } = string.Empty;
        public int? FactorValue { get; set; }
        public int? LastError { get; set; }
        public int? PoolMetadataId { get; set; }
        public string IsProcessInJob { get; set; } = string.Empty;
        public string IsSystemPhysicalMemoryHigh { get; set; } = string.Empty;
        public string IsSystemPhysicalMemoryLow { get; set; } = string.Empty;
        public string IsProcessPhysicalMemoryLow { get; set; } = string.Empty;
        public string IsProcessVirtualMemoryLow { get; set; } = string.Empty;
    }
}
