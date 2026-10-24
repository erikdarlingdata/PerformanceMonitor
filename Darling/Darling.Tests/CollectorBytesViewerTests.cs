/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6629 part B: the Viewer shows the bytes a collector run received and sent. A figure reads in 1024-based units, a run or collector
/// with none reads "not measured" (never 0 B), the columns are read only from a store at V219 or later, and the grids hide them below it.
/// </summary>
public sealed class CollectorBytesViewerTests
{
    [Fact]
    public void ABytesFigure_ReadsInUnits_AndNoFigureReadsNotMeasured_NeverZero()
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        try
        {
            Assert.Equal("not measured", CollectorBytesText.Format(null));
            Assert.Equal("0 B", CollectorBytesText.Format(0));
            Assert.Equal("900 B", CollectorBytesText.Format(900));
            Assert.Equal("1.5 KB", CollectorBytesText.Format(1536));
            Assert.Equal("2.0 MB", CollectorBytesText.Format(2L * 1024 * 1024));
            Assert.Equal("3.5 GB", CollectorBytesText.Format((long)(3.5 * 1024 * 1024 * 1024)));
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    [Fact]
    public void ACollectionLogRow_ShowsItsBytes_AndNotMeasuredWhenTheRunHadNone()
    {
        var measured = new CollectionLogRow { BytesReceived = 4096, BytesSent = 512 };
        var unmeasured = new CollectionLogRow();

        Assert.Equal(CollectorBytesText.Format(4096), measured.BytesReceivedFormatted);
        Assert.Equal(CollectorBytesText.Format(512), measured.BytesSentFormatted);
        Assert.Equal("not measured", unmeasured.BytesReceivedFormatted);
        Assert.Equal("not measured", unmeasured.BytesSentFormatted);
    }

    [Fact]
    public void BytesPerDay_SpreadTheWindowSums_AndLeaveACollectorWithNoSumsNotMeasured()
    {
        var measured = new CollectorHealthRow { CollectorName = "wait_stats" };
        var unmeasured = new CollectorHealthRow { CollectorName = "pg_database_stats" };
        var sums = new Dictionary<string, (long Received, long Sent)> { ["wait_stats"] = (7000, 700) };

        ViewerDataService.ApplyBytesPerDay([measured, unmeasured], sums, 7);

        Assert.Equal(1000, measured.BytesReceivedPerDay);
        Assert.Equal(100, measured.BytesSentPerDay);
        Assert.Null(unmeasured.BytesReceivedPerDay);
        Assert.Null(unmeasured.BytesSentPerDay);
        Assert.Equal("not measured", unmeasured.BytesReceivedPerDayFormatted);
        Assert.Equal("not measured", unmeasured.BytesSentPerDayFormatted);
    }

    [Theory]
    [InlineData(218, false)]
    [InlineData(219, true)]
    [InlineData(220, true)]
    [InlineData(null, true)]
    public void TheByteColumns_AreReadOnlyFromAStoreAtV219OrLater_AndAnUnansweredProbeFailsOpen(int? probedVersion, bool offered) =>
        Assert.Equal(offered, ViewerDataService.CollectionBytesAvailable(probedVersion));

    [Fact]
    public void TheByteColumns_AreAddedAfterReadSource_OnBothLogStatements_AndOnlyThere()
    {
        foreach (var sql in new[] { ViewerDataService.RecentCollectionLogSql, ViewerDataService.CollectionLogByCollectorSql })
        {
            var gated = ViewerDataService.CollectionLogWithBytesSql(sql);
            Assert.Contains("read_source, bytes_received, bytes_sent", gated, StringComparison.Ordinal);
            Assert.DoesNotContain("bytes_received", sql, StringComparison.Ordinal);
            Assert.Equal(sql.Length + ", bytes_received, bytes_sent".Length, gated.Length);
        }

        Assert.Throws<InvalidOperationException>(() => ViewerDataService.CollectionLogWithBytesSql("SELECT 1"));
    }

    [Fact]
    public void AGrid_HidesItsByteColumns_BelowV219_AndShowsThemAtIt()
    {
        StaTestThread.Run(() =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false };
            var duration = new DataGridTextColumn { SortMemberPath = "DurationMs" };
            var received = new DataGridTextColumn { SortMemberPath = "BytesReceived" };
            var sent = new DataGridTextColumn { SortMemberPath = "BytesSentPerDay" };
            grid.Columns.Add(duration);
            grid.Columns.Add(received);
            grid.Columns.Add(sent);

            CollectorBytesText.ShowColumns(grid, false);
            Assert.Equal(Visibility.Visible, duration.Visibility);
            Assert.Equal(Visibility.Collapsed, received.Visibility);
            Assert.Equal(Visibility.Collapsed, sent.Visibility);

            CollectorBytesText.ShowColumns(grid, true);
            Assert.Equal(Visibility.Visible, received.Visibility);
            Assert.Equal(Visibility.Visible, sent.Visibility);
            return Task.CompletedTask;
        });
    }
}
