/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7691: a server with several memory nodes stores one record per node for the same event. The desktop chart counts an event
/// once: one notification twice at one instant, or 1 ms apart in one second, is one event; two notifications at one second are two.
/// </summary>
public sealed class MemoryPressureEventCountTests
{
    private static MemoryPressureEventRow Row(DateTime at, string notification) => new()
    {
        SampleTime = at,
        MemoryNotification = notification,
        MemoryIndicatorsProcess = 2,
        MemoryIndicatorsSystem = 0
    };

    [Fact]
    public void RecordsOfOneEventOnSeveralNodes_AreOneEvent_AndTwoNotificationsAreTwo()
    {
        var at = new DateTime(2026, 9, 12, 10, 0, 5, DateTimeKind.Utc);
        var rows = new[]
        {
            Row(at, "RESOURCE_MEMPHYSICAL_LOW"), Row(at, "RESOURCE_MEMPHYSICAL_LOW"),
            Row(at.AddHours(1), "RESOURCE_MEMPHYSICAL_LOW"), Row(at.AddHours(1), "RESOURCE_MEM_STEADY"),
            Row(at.AddHours(2), "RESOURCE_MEMPHYSICAL_LOW"), Row(at.AddHours(2).AddMilliseconds(1), "RESOURCE_MEMPHYSICAL_LOW"),
        };

        var events = MemoryPressureEventCount.OnePerEvent(rows);

        Assert.Equal(4, events.Count);
        Assert.Equal(new[] { at, at.AddHours(1), at.AddHours(1), at.AddHours(2) }, events.ConvertAll(e => e.SampleTime));
    }
}
