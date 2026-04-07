/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Models;

public class ProcedureStatsComparisonItem : ComparisonItemBase
{
    public string SchemaName { get; set; } = "";
    public string ObjectName { get; set; } = "";
    public string FullName => string.IsNullOrEmpty(SchemaName) ? ObjectName : $"{SchemaName}.{ObjectName}";
}
