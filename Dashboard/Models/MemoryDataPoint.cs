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
    public class MemoryDataPoint
    {
        public DateTime CollectionTime { get; set; }
        public decimal BufferPoolMb { get; set; }
        public decimal PlanCacheMb { get; set; }
        public decimal PhysicalMemoryInUseMb { get; set; }
        public decimal AvailablePhysicalMemoryMb { get; set; }
        public int MemoryUtilizationPercentage { get; set; }
        public decimal TotalMemoryMb { get; set; }
        public decimal GrantedMemoryMb { get; set; }
    }
}
