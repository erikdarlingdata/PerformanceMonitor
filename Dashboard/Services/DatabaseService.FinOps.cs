/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// FinOps Tab data access — see sub-partials:
    ///   - DatabaseService.FinOps.Inventory.cs       (server-level utilization, properties, metrics)
    ///   - DatabaseService.FinOps.Workload.cs        (database/application resource usage, top consumers)
    ///   - DatabaseService.FinOps.Storage.cs         (database sizes, growth, idle DBs, tempdb)
    ///   - DatabaseService.FinOps.Queries.cs         (waits, expensive/high-impact queries, memory grants, provisioning trend)
    ///   - DatabaseService.FinOps.IndexAnalysis.cs   (sp_IndexCleanup integration)
    ///   - DatabaseService.FinOps.Recommendations.cs (FinOps recommendation engine)
    ///   - DatabaseService.FinOps.Models.cs          (FinOps DTO/model types)
    /// </summary>
    public partial class DatabaseService
    {
    }
}
