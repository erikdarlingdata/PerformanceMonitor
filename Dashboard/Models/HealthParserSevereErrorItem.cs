// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class HealthParserSevereErrorItem
    {
        public long Id { get; set; }
        public DateTime CollectionTime { get; set; }
        public DateTime? EventTime { get; set; }
        public int? ErrorNumber { get; set; }
        public int? Severity { get; set; }
        public int? State { get; set; }
        public string Message { get; set; } = string.Empty;
        public string DatabaseName { get; set; } = string.Empty;
        public int? DatabaseId { get; set; }
    }
}
