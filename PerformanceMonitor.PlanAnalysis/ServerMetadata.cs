/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Server-level facts an analyzer rule can use alongside a parsed plan (#4530/#4535). Ported from
/// erikdarlingdata/PerformanceStudio dev (85492a1) <c>src/PlanViewer.Core/Models/ServerMetadata.cs:5-37</c>,
/// the top-level properties and the two computed flags only. PS's <c>Database</c>/<c>DatabaseMetadata</c>/
/// <c>ScopedConfigItem</c> are left out: only PS's App UI reads those, and no analyzer rule does.
/// This step ports the model only; nothing in the analyzer reads it yet.
/// </summary>
public class ServerMetadata
{
    // Instance
    public string? ServerName { get; set; }
    public string? ProductVersion { get; set; }
    public string? ProductLevel { get; set; }
    public string? Edition { get; set; }
    public bool IsAzure { get; set; }

    // Hardware
    public int CpuCount { get; set; }
    public long PhysicalMemoryMB { get; set; }

    // Instance settings
    public int MaxDop { get; set; }
    public int CostThresholdForParallelism { get; set; }
    public long MaxServerMemoryMB { get; set; }

    /// <summary>
    /// Whether sys.database_scoped_configurations is available (SQL 2016+ or Azure).
    /// </summary>
    public bool SupportsScopedConfigs =>
        IsAzure || (int.TryParse(ProductVersion?.Split('.')[0], out var major) && major >= 13);

    /// <summary>
    /// Whether sys.query_store_wait_stats is available (SQL 2017+ or Azure).
    /// </summary>
    public bool SupportsQueryStoreWaitStats =>
        IsAzure || (int.TryParse(ProductVersion?.Split('.')[0], out var major) && major >= 14);
}
