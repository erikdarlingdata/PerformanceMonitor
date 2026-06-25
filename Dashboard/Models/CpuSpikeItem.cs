// Copyright (c) 2025 Darling Data, LLC
// Licensed under the MIT License

using System;

namespace PerformanceMonitorDashboard.Models
{
    public class CpuSpikeItem
    {
        public DateTime EventTime { get; set; }
        public int SqlServerCpu { get; set; }
        public int OtherProcessCpu { get; set; }
        public int TotalCpu { get; set; }
        public string Severity { get; set; } = string.Empty;
    }
}
