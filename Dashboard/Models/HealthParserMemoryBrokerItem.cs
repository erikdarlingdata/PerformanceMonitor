// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserMemoryBrokerItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public long? BrokerId { get; set; }
        public long? PoolMetadataId { get; set; }
        public long? DeltaTime { get; set; }
        public long? MemoryRatio { get; set; }
        public long? NewTarget { get; set; }
        public long? Overall { get; set; }
        public long? Rate { get; set; }
        public long? CurrentlyPredicated { get; set; }
        public long? CurrentlyAllocated { get; set; }
        public long? PreviouslyAllocated { get; set; }
        public string Broker { get; set; } = string.Empty;
        public string Notification { get; set; } = string.Empty;
    }
}
