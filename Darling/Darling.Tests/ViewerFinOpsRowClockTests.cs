/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the FinOps Server Inventory rows (one per server) and Application Connections rows read their times on THEIR
/// server's clock. They went through the active server tab's clock, but Server Inventory lists every server at once and
/// the FinOps tab lists the server it was opened for, so in Server mode a row could read another server's hour. Each row
/// now carries the clock its loader stamped (the server's collected clock, else the viewer machine's offset, the rule every
/// list row uses) and words its text on it, with the UTC offset in the repeated autumn hour.
///
/// <para>US Eastern and India (+05:30 all year) are the servers, so a clock that differs from this machine's is always
/// among them; the autumn change is 2026-11-01 at 06:00 UTC, so 05:30Z is the first 01:30 (-04:00) and 06:30Z the second
/// (-05:00) on the Eastern one.</para>
/// </summary>
/* Flips the process-wide display mode and active server clock (each restored in Dispose); one shared collection
   serializes it with every other class that does. */
[Collection("viewer-time-statics")]
public sealed class ViewerFinOpsRowClockTests : IDisposable
{
    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    private static readonly ServerClock India = ServerClock.Resolve("India Standard Time", 330);

    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public ViewerFinOpsRowClockTests()
    {
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
    }

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
        CultureInfo.CurrentCulture = _savedCulture;
    }

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static ServerPropertyRow Inventory(DateTime? asOfUtc, DateTime? collectedUtc, ServerClock? clock) => new()
    {
        ServerId = 1,
        ServerName = "SQL1",
        InventoryAsOfUtc = asOfUtc,
        LastCollectedUtc = collectedUtc,
        Clock = clock,
    };

    private static ApplicationConnectionRow Connections(DateTime firstUtc, DateTime lastUtc, ServerClock? clock) => new()
    {
        ApplicationName = "app",
        FirstSeenUtc = firstUtc,
        LastSeenUtc = lastUtc,
        Clock = clock,
    };

    [Fact]
    public void Inventory_ARowReadsItsOwnServersWallTime_NotTheActiveTabs()
    {
        ViewerTimeHelper.ActiveServerClock = India;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        /* 14:30Z is 10:30 in US Eastern (the row's server) and 20:00 in India (the active tab's). */
        var row = Inventory(Utc(2026, 7, 1, 14, 30), Utc(2026, 7, 1, 14, 30), Eastern);
        Assert.Equal("2026-07-01 10:30", row.InventoryAsOfText);
        Assert.Equal("2026-07-01 10:30", row.LastCollectedText);

        ViewerTimeHelper.ActiveServerClock = Eastern;
        var indian = Inventory(Utc(2026, 7, 1, 14, 30), Utc(2026, 7, 1, 14, 30), India);
        Assert.Equal("2026-07-01 20:00", indian.InventoryAsOfText);
        Assert.Equal("2026-07-01 20:00", indian.LastCollectedText);
    }

    [Fact]
    public void Inventory_InTheRepeatedHour_ReadsItsOffsetInServerMode_AndTheStoredInstantInUtcMode()
    {
        ViewerTimeHelper.ActiveServerClock = India;
        var first = Inventory(Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 5, 30), Eastern);
        var second = Inventory(Utc(2026, 11, 1, 6, 30), Utc(2026, 11, 1, 6, 30), Eastern);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30 -04:00", first.InventoryAsOfText);
        Assert.Equal("2026-11-01 01:30 -05:00", second.InventoryAsOfText);
        Assert.Equal("2026-11-01 01:30 -04:00", first.LastCollectedText);
        Assert.Equal("2026-11-01 01:30 -05:00", second.LastCollectedText);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-01 05:30", first.InventoryAsOfText);
        Assert.Equal("2026-11-01 06:30", second.LastCollectedText);
    }

    [Fact]
    public void Inventory_AServerWithNoSnapshotOrCollection_ShowsNothing_AndARowWithoutAClockTakesTheActiveOne()
    {
        ViewerTimeHelper.ActiveServerClock = Eastern;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        var none = Inventory(null, null, Eastern);
        Assert.Equal("", none.InventoryAsOfText);
        Assert.Equal("", none.LastCollectedText);

        var unstamped = Inventory(Utc(2026, 11, 1, 6, 30), Utc(2026, 11, 1, 6, 30), clock: null);
        Assert.Equal("2026-11-01 01:30 -05:00", unstamped.InventoryAsOfText);
        Assert.Equal("2026-11-01 01:30 -05:00", unstamped.LastCollectedText);
    }

    [Fact]
    public void AppConnections_ARowReadsItsOwnServersWallTime_NotTheActiveTabs()
    {
        ViewerTimeHelper.ActiveServerClock = India;
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        var row = Connections(Utc(2026, 7, 1, 14, 30), Utc(2026, 7, 1, 15, 30), Eastern);
        Assert.Equal("2026-07-01 10:30", row.FirstSeenText);
        Assert.Equal("2026-07-01 11:30", row.LastSeenText);
    }

    [Fact]
    public void AppConnections_InTheRepeatedHour_ReadsItsOffsetInServerMode_AndTheStoredInstantInUtcMode()
    {
        ViewerTimeHelper.ActiveServerClock = India;
        var row = Connections(Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 6, 30), Eastern);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30 -04:00", row.FirstSeenText);
        Assert.Equal("2026-11-01 01:30 -05:00", row.LastSeenText);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-01 05:30", row.FirstSeenText);
        Assert.Equal("2026-11-01 06:30", row.LastSeenText);
    }

    /// <summary>
    /// Both loaders read the clocks and stamp each row with its server's, and the sort values (the "Local" DateTimes) are
    /// converted on that same clock, so a column sorts in the order its text reads. Neither goes through the active
    /// server's clock any more (<c>ForDisplay</c>).
    /// </summary>
    [Fact]
    public void TheLoaders_StampEachRowsClock_AndConvertTheSortValuesOnIt()
    {
        var workload = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.ViewerSource("ViewerDataService.FinOps.Workload.cs", ThisFile()));
        var connections = ViewerTypedRangeTests.MemberText(workload, "GetApplicationConnectionsAsync");
        Assert.Contains("GetServerClocksAsync(serverId, cancellationToken)", connections);
        Assert.Contains("ClockForServerOrMachine(", connections);
        Assert.Contains("Clock = clock", connections);
        Assert.Contains("ConvertToDisplay(reader.GetDateTime(18), ViewerTimeHelper.CurrentDisplayMode, clock)", connections);
        Assert.Contains("ConvertToDisplay(reader.GetDateTime(19), ViewerTimeHelper.CurrentDisplayMode, clock)", connections);
        Assert.DoesNotContain("ForDisplay(", connections);

        var inventorySource = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.ViewerSource("ViewerDataService.FinOps.Inventory.cs", ThisFile()));
        var inventory = ViewerTypedRangeTests.MemberText(inventorySource, "GetServerInventoryAsync");
        Assert.Contains("GetServerClocksAsync(null, cancellationToken)", inventory);
        Assert.Contains("ClockForServerOrMachine(clocks, serverId, TimeZoneInfo.Local, nowUtc)", inventory);
        Assert.Contains("Clock = clock", inventory);
        Assert.Contains("ConvertToDisplay(reader.GetDateTime(13), ViewerTimeHelper.CurrentDisplayMode, clock)", inventory);
        Assert.Contains("ConvertToDisplay(reader.GetDateTime(19), ViewerTimeHelper.CurrentDisplayMode, clock)", inventory);
        Assert.DoesNotContain("ForDisplay(", inventory);
    }

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;
}
