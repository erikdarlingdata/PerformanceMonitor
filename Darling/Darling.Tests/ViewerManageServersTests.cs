/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Manage Servers grid's new "Last Collected" column projection (<see cref="ManagedServerListItem"/>):
/// the SERVICE's newest collection time (the MAX(collection_time) freshness signal the sidebar dots reuse) is
/// rendered in the viewer's timestamp-display mode, and a server the service has not collected yet reads
/// "Never" rather than a bogus epoch. The read itself (<see cref="ViewerDataService.GetServerFreshnessAsync"/>)
/// is a single documented query already exercised by the sidebar; the CELL logic is what this change adds, and
/// it is pure (given the display mode), so it is unit-testable without a live Postgres.
/// </summary>
/* Serialized: these classes flip the process-wide ViewerTimeHelper.CurrentDisplayMode static (each
   restores in finally, but xUnit runs CLASSES in parallel — two flippers racing corrupts the mode a
   third class reads). One shared collection serializes them. */
[Collection("viewer-time-statics")]
public sealed class ViewerManageServersColumnTests
{
    private static ManagedServerListItem Item(DateTime? lastCollectedUtc) =>
        new(new MonitoredServerRow { ServerId = 1, Host = "SQL2022", Name = "SQL2022" }, isFavorite: false)
        {
            LastCollectedUtc = lastCollectedUtc,
        };

    /// <summary>#7343: the Added column is a fleet-wide value. Server mode on a row whose own clock is UTC+05:30 still reads UTC (labelled), Local reads this machine.</summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void CreatedDisplay_FollowsTheDisplayMode_AsAFleetWideValue(TimeDisplayMode mode)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            var created = new DateTime(2026, 7, 1, 14, 30, 0, DateTimeKind.Utc);
            var item = new ManagedServerListItem(new MonitoredServerRow { ServerId = 1, Host = "SQL2022", Name = "SQL2022", CreatedAt = created }, isFavorite: false)
            {
                Clock = ServerClock.FixedOffset(330),
            };

            var expected = mode == TimeDisplayMode.LocalTime
                ? TimeZoneInfo.ConvertTimeFromUtc(created, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm", System.Globalization.CultureInfo.InvariantCulture) + " "
                : "2026-07-01 14:30 UTC";
            Assert.StartsWith(expected, item.CreatedDisplay, StringComparison.Ordinal);
            Assert.Equal(created, item.CreatedAtUtc);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }

    [Fact]
    public void LastCollectedDisplay_ReadsNever_WhenTheServiceHasNotCollectedYet()
    {
        Assert.Equal("Never", Item(lastCollectedUtc: null).LastCollectedDisplay);
    }

    [Fact]
    public void LastCollectedDisplay_FormatsTheNewestCollectionTime_InTheDisplayMode()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            // UTC mode is the identity conversion, so the naive-UTC collection time renders verbatim.
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var lastCollected = new DateTime(2026, 7, 1, 14, 30, 0, DateTimeKind.Unspecified);

            Assert.Equal("2026-07-01 14:30", Item(lastCollected).LastCollectedDisplay);
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }
}
