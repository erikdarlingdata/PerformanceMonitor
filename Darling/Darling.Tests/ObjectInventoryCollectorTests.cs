/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8784: the shape of <see cref="ObjectInventoryCollector"/> (the Darling-only daily list of procedures, functions and views) and of the
/// answering-instance column added to <see cref="IndexObjectStatsCollector"/>. No database.
/// </summary>
public sealed class ObjectInventoryCollectorTests
{
    [Fact]
    public void TheInventory_IsDarlingOnly_APlainTable_Daily_AndInTheCatalog()
    {
        var collector = ObjectInventoryCollector.Instance;
        Assert.True(collector.IsDarlingOnly);
        Assert.True(collector.StaysPlainTable);
        Assert.Contains(collector, CollectorCatalog.All);
        Assert.Equal(1440, CollectorScheduleDefaults.All["object_inventory"].FrequencyMinutes);
    }

    [Fact]
    public void TheInventory_ReadsUserModulesAndViews_Only()
    {
        var body = ObjectInventoryCollector.BuildPayloadBody();
        Assert.Contains("o.is_ms_shipped = 0", body, StringComparison.Ordinal);
        foreach (var type in new[] { "N'P'", "N'PC'", "N'FN'", "N'IF'", "N'TF'", "N'FS'", "N'FT'", "N'V'" })
        {
            Assert.Contains(type, body, StringComparison.Ordinal);
        }

        /* Tables are index_object_stats' job; listing them here would duplicate every table row. */
        Assert.DoesNotContain("N'U'", body, StringComparison.Ordinal);
        Assert.Contains("OPTION(RECOMPILE)", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheInventory_EnumeratesTheSameDatabasesAsTheIndexStats()
    {
        Assert.Contains("IndexObjectStatsCollector.Instance.BuildEnumerationQuery(context)", System.IO.File.ReadAllText(SourcePath("PerformanceMonitor.Collectors", "ObjectInventoryCollector.cs")), StringComparison.Ordinal);
    }

    [Fact]
    public void ThePerItemQuery_QuoteDoublesTheBody_AndEscapesTheDatabaseName()
    {
        var query = ObjectInventoryCollector.Instance.BuildPerItemQuery(
            "odd]name",
            new CollectorContext { ServerId = 1, ServerName = "s", CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator(), Target = new CollectorTargetInfo { SqlMajorVersion = 16 } });
        Assert.Contains("EXECUTE [odd]]name].sys.sp_executesql", query.Text, StringComparison.Ordinal);
        Assert.Contains("o.type IN (N''P''", query.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAnsweringInstance_AndTheMemoryOptimizedFlag_AreTheLastIndexStatsColumns_ReadFromTheLastOrdinals()
    {
        var columns = IndexObjectStatsCollector.Instance.PayloadColumns;
        Assert.Equal("is_memory_optimized", columns[^1].Name);
        Assert.Equal(CollectorColumnType.Boolean, columns[^1].Type);
        Assert.Equal("answering_server_name", columns[^2].Name);
        Assert.Equal(CollectorColumnType.Varchar, columns[^2].Type);
        Assert.Equal("is_indexed_view", columns[^3].Name);

        var body = IndexObjectStatsCollector.BuildPerDatabaseStatsBody(supportsOptimizeForSequentialKey: true);
        var select = body[body.LastIndexOf("sqlserver_start_time = (SELECT", StringComparison.Ordinal)..];
        Assert.True(
            select.IndexOf("is_indexed_view =", StringComparison.Ordinal) < select.IndexOf("answering_server_name = @@SERVERNAME", StringComparison.Ordinal),
            "answering_server_name must follow is_indexed_view in the final SELECT, so every earlier ordinal stays where it is read.");
        Assert.True(
            select.IndexOf("answering_server_name = @@SERVERNAME", StringComparison.Ordinal) < select.IndexOf("is_memory_optimized =", StringComparison.Ordinal),
            "is_memory_optimized must follow answering_server_name in the final SELECT.");
    }

    private static string SourcePath(params string[] parts)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.Path.Combine(new[] { dir! }.Concat(parts).ToArray());
    }
}
