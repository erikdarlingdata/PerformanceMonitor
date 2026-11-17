/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8943: the Viewer's PostgreSQL Storage tab does not call an uncollected panel "the healthy answer". The table bloat and index usage
/// collectors are gated off on a read replica, so a server with no run of either gets the writer-only sentence, and a primary whose
/// collectors ran but found nothing over the floor keeps the healthy one.
/// </summary>
public sealed class ViewerPostgresStorageWriterOnlyTests
{
    private static ViewerDataService.PostgresCollectorLogFacts Ran(string name, long runs, long rows = 0) =>
        new(name, DateTime.UtcNow, DateTime.UtcNow, runs, 0, rows, "SUCCESS", null);

    [Fact]
    public void AReplica_WithNoRunOfEitherCollector_GetsTheWriterOnlySentence_ForBothPanels()
    {
        /* A replica writes no collection_log row for a collector it never runs, so the read returns no facts at all. */
        var (table, index) = ViewerServerTab.StorageEmptyTexts(new List<ViewerDataService.PostgresCollectorLogFacts>(), offline: false);

        foreach (var text in new[] { table, index })
        {
            Assert.Contains("collected on the writer only", text, StringComparison.Ordinal);
            Assert.Contains("gated off on a read replica", text, StringComparison.Ordinal);
            Assert.DoesNotContain("healthy answer", text, StringComparison.Ordinal);
        }

        Assert.Contains("table bloat", table, StringComparison.Ordinal);
        Assert.Contains("index usage", index, StringComparison.Ordinal);
    }

    [Fact]
    public void APrimary_WhoseCollectorsRanAndFoundNothingOverTheFloor_KeepsTheHealthySentences()
    {
        var runs = new List<ViewerDataService.PostgresCollectorLogFacts> { Ran("pg_table_bloat_stats", 6), Ran("pg_index_usage_stats", 6) };

        var (table, index) = ViewerServerTab.StorageEmptyTexts(runs, offline: false);

        Assert.Equal(ViewerServerTab.TableBloatHealthyEmptyText, table);
        Assert.Equal(ViewerServerTab.IndexUsageHealthyEmptyText, index);
        Assert.Contains("healthy answer", table, StringComparison.Ordinal);
        Assert.Contains("healthy answer", index, StringComparison.Ordinal);
    }

    [Fact]
    public void EachPanel_IsJudgedOnItsOwnCollector()
    {
        var runs = new List<ViewerDataService.PostgresCollectorLogFacts> { Ran("pg_table_bloat_stats", 6), Ran("pg_index_usage_stats", 0) };

        var (table, index) = ViewerServerTab.StorageEmptyTexts(runs, offline: false);

        Assert.Equal(ViewerServerTab.TableBloatHealthyEmptyText, table);
        Assert.Contains("collected on the writer only", index, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailedRead_MakesNoWriterOnlyClaim()
    {
        var (table, index) = ViewerServerTab.StorageEmptyTexts(null, offline: false);

        Assert.Equal(ViewerServerTab.TableBloatHealthyEmptyText, table);
        Assert.Equal(ViewerServerTab.IndexUsageHealthyEmptyText, index);
    }

    [Fact]
    public void AnOfflineServer_MakesNoWriterOnlyClaim()
    {
        /* Nothing arrives from an offline server, so "no run" says nothing about replicas. */
        var (table, index) = ViewerServerTab.StorageEmptyTexts(new List<ViewerDataService.PostgresCollectorLogFacts>(), offline: true);

        Assert.DoesNotContain("writer only", table, StringComparison.Ordinal);
        Assert.DoesNotContain("writer only", index, StringComparison.Ordinal);
    }
}
