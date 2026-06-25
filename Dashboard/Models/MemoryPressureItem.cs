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
    public class MemoryPressureItem
    {
        public DateTime CollectionTime { get; set; }
        public int ActiveGrants { get; set; }
        public int QueriesWaiting { get; set; }
        public decimal AvailableMemoryMb { get; set; }
        public decimal GrantedMemoryMb { get; set; }
        public decimal UsedMemoryMb { get; set; }
        public decimal MemoryUtilizationPercent { get; set; }
        public int TimeoutErrors { get; set; }
        public int ForcedGrants { get; set; }
        public string PressureLevel { get; set; } = string.Empty;
        public string Recommendation { get; set; } = string.Empty;
    }
}
