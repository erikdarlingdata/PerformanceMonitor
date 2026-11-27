/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#8798: the column_stats collector's registration: its catalog shape, its schedule default and its detached run.</summary>
public sealed class ColumnStatsCollectorTests
{
    [Fact]
    public void TheCollector_IsDarlingOnly_APlainTable_Hourly_OffByDefault_AndInTheCatalog()
    {
        var c = ColumnStatsCollector.Instance;

        Assert.Equal("column_stats", c.Name);
        Assert.Equal("column_stats_runs", c.TargetTable);
        Assert.True(c.StaysPlainTable);
        Assert.True(c.IsDarlingOnly);
        Assert.Equal(CollectorTargetEngine.SqlServer, c.TargetEngine);
        Assert.True(c.AppliesTo(new CollectorTargetInfo()));
        Assert.True(c.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));

        Assert.Equal(new CollectorScheduleDefaults.Entry(60, 30, false), CollectorScheduleDefaults.All["column_stats"]);

        var names = CollectorCatalog.All.Select(d => d.Name).ToList();
        Assert.Equal(names.IndexOf("schema_objects") + 1, names.IndexOf("column_stats"));

        Assert.True(SweepPressureReport.IsDetachedByName("column_stats"));
    }

    [Fact]
    public void ThePayload_HasTheSeventeenColumnsTheRunTableStores_InOrder()
    {
        Assert.Equal(
            new[]
            {
                "database_name", "history_key", "kind", "checked", "read", "deleted", "skipped_filtered", "skipped_memory_optimized",
                "skipped_xml_spatial_columnstore", "skipped_never_updated", "skipped_locked", "skipped_over_cap", "errors", "sample_error",
                "refused", "complete", "note",
            },
            ColumnStatsCollector.Instance.PayloadColumns.Select(p => p.Name).ToArray());
    }
}
