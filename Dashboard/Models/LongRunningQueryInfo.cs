/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

namespace PerformanceMonitorDashboard.Models
{
    public class LongRunningQueryInfo
    {
        public int SessionId { get; set; }
        public string DatabaseName { get; set; } = "";
        public string QueryText { get; set; } = "";
        public string ProgramName { get; set; } = "";
        public long ElapsedSeconds { get; set; }
        public long CpuTimeMs { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public string? WaitType { get; set; }
        public int? BlockingSessionId { get; set; }
    }
}
