/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4966: the PostgreSQL latest-state grids (Extensions, Server Config, Buffer Usage, Replication Slots, Index Bloat,
/// Predicate Stats) show their snapshot time as a column, in the display zone, to the second. The reader rows carry naive
/// UTC; the display row words it and keeps the instant beside it for sorting. The XAML side (column present, sortable) is
/// pinned in <see cref="ViewerGridTimeColumnSortMemberTests"/>.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerPgSnapshotGridTimeTests
{
    private static readonly DateTime Utc = new(2026, 7, 4, 15, 30, 45, DateTimeKind.Unspecified);

    private static void InZone(Action body)
    {
        var mode = ViewerTimeHelper.CurrentDisplayMode;
        var clock = ViewerTimeHelper.ActiveServerClock;
        var culture = CultureInfo.CurrentCulture;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            ViewerTimeHelper.ActiveServerClock = ServerClock.Resolve("Eastern Standard Time", -300);
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            body();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            ViewerTimeHelper.ActiveServerClock = clock;
            CultureInfo.CurrentCulture = culture;
        }
    }

    private const string Expected = "2026-07-04 11:30:45";

    [Fact]
    public void ExtensionCaptureTime_IsTheDisplayZone_ToTheSecond_AndKeepsTheUtcInstant()
    {
        InZone(() =>
        {
            var row = PgDisplay.Extension(new DarlingPgExtensionAvailabilityReader.PgExtensionRow(
                "appdb", "pg_stat_statements", "installed", "1.10", "1.10", true, "c", Utc));
            Assert.Equal(Expected, row.CaptureTime);
            Assert.Equal(Utc, row.CaptureTimeUtc);
            Assert.Equal("pg_stat_statements", row.ExtensionName);
        });
    }

    [Fact]
    public void ServerConfigCollectionTime_IsTheDisplayZone_ToTheSecond_AndKeepsTheUtcInstant()
    {
        InZone(() =>
        {
            var row = PgDisplay.ServerConfig(new DarlingPgServerConfigReader.PgConfigRow(
                "work_mem", "64MB", "kB", "c", "user", "configuration file", "4096", "4096", null, 0, false, "d", false, Utc));
            Assert.Equal(Expected, row.CollectionTime);
            Assert.Equal(Utc, row.CollectionTimeUtc);
            Assert.Equal("work_mem", row.Name);
        });
    }

    [Fact]
    public void BufferUsageCaptureTime_IsTheDisplayZone_ToTheSecond_AndKeepsTheUtcInstant()
    {
        InZone(() =>
        {
            var row = PgDisplay.BufferUsage(new DarlingPgBufferUsageReader.PgBufferUsageRow(
                "appdb", "t", "r", 10, 1, 2.5, 100, 50, 10.0, 10.0, Utc));
            Assert.Equal(Expected, row.CaptureTime);
            Assert.Equal(Utc, row.CaptureTimeUtc);
            Assert.Equal(100, row.PoolBuffersTotal);
        });
    }

    [Fact]
    public void PredicateStatCaptureTime_IsTheDisplayZone_ToTheSecond_AndKeepsTheReaderRowForTheContextMenu()
    {
        InZone(() =>
        {
            var source = new DarlingPgPredicateStatsReader.PgPredicateStatRow(
                "appdb", "public", "t", "c", "=", 7, 1, 2, 1, 50.0, 1.0, 1.0, Utc);
            var row = PgDisplay.PredicateStat(source);
            Assert.Equal(Expected, row.CaptureTime);
            Assert.Equal(Utc, row.CaptureTimeUtc);
            Assert.Same(source, row.Source);
        });
    }

    [Fact]
    public void SlotMeasuredAt_IsTheDisplayZone_ToTheSecond_AndKeepsTheUtcInstant()
    {
        InZone(() =>
        {
            var row = PgDisplay.Slot(new DarlingPgSlotReader.PgSlotRow(
                "s1", Utc, "logical", "pgoutput", "appdb", true, "reserved", 1, 1, 1, 1, null, null, false, 1, Utc));
            Assert.Equal(Expected, row.MeasuredAt);
            Assert.Equal(Utc, row.MeasuredAtUtc);
        });
    }

    [Fact]
    public void SnapshotTime_IsEmptyForAMissingTime_AndIndexBloatAndSlotsUseIt()
    {
        InZone(() =>
        {
            Assert.Equal(string.Empty, PgDisplay.SnapshotTime(null));
            Assert.Equal(Expected, PgDisplay.SnapshotTime(Utc));
        });

        var display = ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerPostgresDisplay.cs").ReplaceLineEndings("\n");
        Assert.Contains("MeasuredAt = SnapshotTime(row.MeasuredAt),\n        MeasuredAtUtc = row.MeasuredAt,\n        /* An invalidated", display, StringComparison.Ordinal);
        Assert.Contains("EstimatedAt = SnapshotTime(row.EstimatedAt),", display, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFourLoadersBindTheDisplayRows_AndTheMenuReadsTheSourceRow()
    {
        var tab = ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Postgres.cs").ReplaceLineEndings("\n");
        Assert.Contains("PgExtensionsGrid.ItemsSource = rows.Select(PgDisplay.Extension).ToList();", tab, StringComparison.Ordinal);
        Assert.Contains("PgServerConfigGrid.ItemsSource = chosen.Select(PgDisplay.ServerConfig).ToList();", tab, StringComparison.Ordinal);
        Assert.Contains("PgBufferUsageGrid.ItemsSource = rows.Select(PgDisplay.BufferUsage).ToList();", tab, StringComparison.Ordinal);
        Assert.Contains("PgPredicateStatsGrid.ItemsSource = rows.Select(PgDisplay.PredicateStat).ToList();", tab, StringComparison.Ordinal);

        var menu = ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.HypotheticalIndex.cs").ReplaceLineEndings("\n");
        Assert.Contains("PgPredicateStatsGrid.SelectedItem is not PgDisplay.PredicateStatRow selected", menu, StringComparison.Ordinal);
        Assert.Contains("var row = selected.Source;", menu, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("PgExtensionsGrid")]
    [InlineData("PgServerConfigGrid")]
    [InlineData("PgBufferUsageGrid")]
    [InlineData("PgPredicateStatsGrid")]
    public void TheSnapshotTimeColumnIsHeadedCollected(string grid)
    {
        var xaml = ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml").ReplaceLineEndings("\n");
        var start = xaml.IndexOf("x:Name=\"" + grid + "\"", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = xaml.IndexOf("</DataGrid>", start, StringComparison.Ordinal);
        var block = xaml[start..end];
        Assert.Contains("Header=\"Collected\"", block, StringComparison.Ordinal);
        Assert.DoesNotContain("Header=\"Captured\"", block, StringComparison.Ordinal);
    }
}
