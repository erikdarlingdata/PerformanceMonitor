/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The panel notes say how often a collector runs from its effective cadence, not from the shipped default: a schedule override
/// changes the words. Pure: no window, no store.
/// </summary>
public sealed class ViewerCadenceTextTests
{
    [Theory]
    [InlineData(60, "hourly", "hour")]
    [InlineData(1, "every minute", "minute")]
    [InlineData(5, "every 5 minutes", "5 minutes")]
    [InlineData(30, "every 30 minutes", "30 minutes")]
    [InlineData(120, "every 2 hours", "2 hours")]
    [InlineData(1440, "daily", "day")]
    [InlineData(4320, "every 3 days", "3 days")]
    [InlineData(90, "every 90 minutes", "90 minutes")]
    public void TheNoteNamesTheEffectiveCadence(int minutes, string every, string span)
    {
        Assert.Equal(every, ViewerCadenceText.Every(minutes));
        Assert.Equal(span, ViewerCadenceText.Span(minutes));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ANonRecurringCadence_IsNotStatedAsANumber(int minutes)
    {
        Assert.Equal("on its schedule", ViewerCadenceText.Every(minutes));
        Assert.Equal("collection interval", ViewerCadenceText.Span(minutes));
    }

    [Fact]
    public void AFleetOverrideOfThirtyMinutes_ChangesTheConfigChangesNoteFromHourly()
    {
        /* The same resolution the Viewer's GetEffectiveCollectorMinutesAsync runs: pg_server_config ships at 60, an override wins. */
        var shipped = CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes("pg_server_config", 1, Array.Empty<ScheduleOverride>());
        var overridden = CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(
            "pg_server_config", 1, new[] { new ScheduleOverride(null, "pg_server_config", 30, null, null) });

        Assert.Equal("hourly", ViewerCadenceText.Every(shipped!.Value));
        Assert.Equal("every 30 minutes", ViewerCadenceText.Every(overridden!.Value));
    }
}
