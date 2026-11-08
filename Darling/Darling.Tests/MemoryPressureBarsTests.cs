/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8109: the Memory Pressure Events chart (Lite's Memory tab and the Viewer's twin) bins pressure samples by hour and draws two
/// stacks per hour, SQL Server and Operating System, each stacked medium under severe. A bar's position is its centre, so an hour's
/// stacks have to be placed off the hour's midpoint, not off its starting tick: then every stack lies inside its own hour, the
/// first hour is not cut by the axis start, and the newest, partial hour is drawn between its tick and now. The bar maths is
/// <c>PerformanceMonitor.Ui.MemoryPressureBars</c>, reached by name so these tests build without it and fail at their asserts.
/// </summary>
public sealed class MemoryPressureBarsTests
{
    private static readonly DateTime Hour14 = new(2026, 11, 3, 14, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Hour16 = Hour14.AddHours(2);

    [Fact]
    public void AnHoursStacks_LieInsideTheHour_SqlServerLeftOfTheMidpoint_OperatingSystemRight()
    {
        var hours = Place(TimeZoneInfo.Utc,
            (Hour14.AddMinutes(5), 2, 0),
            (Hour14.AddMinutes(20), 2, 2),
            (Hour14.AddMinutes(40), 3, 3));

        var hour = Assert.Single(hours);
        Assert.Equal(Hour14, (DateTime)hour.HourStartUtc);
        Assert.Equal(2, (int)hour.SqlMedium);
        Assert.Equal(1, (int)hour.SqlSevere);
        Assert.Equal(1, (int)hour.OsMedium);
        Assert.Equal(1, (int)hour.OsSevere);

        double half = BarSize() / 2;
        double start = Hour14.ToOADate();
        double end = Hour14.AddHours(1).ToOADate();
        double midpoint = start + (end - start) / 2;
        double sql = (double)hour.SqlStackPosition;
        double os = (double)hour.OsStackPosition;

        Assert.True(sql - half >= start, $"the SQL Server stack starts at {At(sql - half)}, before the hour's 14:00 tick");
        Assert.True(sql + half <= midpoint, $"the SQL Server stack ends at {At(sql + half)}, past the hour's midpoint");
        Assert.True(os - half >= midpoint, $"the Operating System stack starts at {At(os - half)}, before the hour's midpoint");
        Assert.True(os + half <= end, $"the Operating System stack ends at {At(os + half)}, past the next hour's tick");
    }

    [Fact]
    public void TheFirstHour_IsNotClippedByTheAxisStart()
    {
        /* The window starts at 14:00, the hour the first samples fall in. */
        double xMin = Hour14.ToOADate();
        var hour = Assert.Single(Place(TimeZoneInfo.Utc, (Hour14.AddMinutes(5), 2, 0), (Hour14.AddMinutes(20), 2, 0)));

        double half = BarSize() / 2;
        double leftEdge = (double)hour.SqlStackPosition - half;
        Assert.True(leftEdge >= xMin, $"the first hour's SQL Server stack starts at {At(leftEdge)}, before the window's 14:00 start, so the axis cuts it");
    }

    [Fact]
    public void TheNewestPartialHour_IsDrawnAfterItsOwnTick_BeforeNow()
    {
        var now = Hour16.AddMinutes(42);
        var hour = Assert.Single(Place(TimeZoneInfo.Utc, (Hour16.AddMinutes(10), 2, 0), (Hour16.AddMinutes(30), 3, 0)));

        Assert.Equal(Hour16, (DateTime)hour.HourStartUtc);
        double half = BarSize() / 2;
        double sql = (double)hour.SqlStackPosition;
        Assert.True(sql - half >= Hour16.ToOADate(), $"the newest hour's SQL Server stack starts at {At(sql - half)}, left of its 16:00 tick, so the stretch from the tick to now reads as empty");
        Assert.True(sql + half <= now.ToOADate(), $"the newest hour's SQL Server stack ends at {At(sql + half)}, past now ({At(now.ToOADate())})");
    }

    [Fact]
    public void Samples_AreBinnedByHour_OldestFirst_OnlyHoursWithPressure()
    {
        var hours = Place(TimeZoneInfo.Utc,
            (Hour16.AddMinutes(1), 0, 2),
            (Hour14.AddMinutes(59), 2, 0),
            (Hour16.AddMinutes(59), 0, 3),
            (Hour14.AddMinutes(0), 3, 0));

        Assert.Equal(2, hours.Count);
        Assert.Equal(Hour14, (DateTime)hours[0].HourStartUtc);
        Assert.Equal((1, 1, 0, 0), ((int)hours[0].SqlMedium, (int)hours[0].SqlSevere, (int)hours[0].OsMedium, (int)hours[0].OsSevere));
        Assert.Equal(Hour16, (DateTime)hours[1].HourStartUtc);
        Assert.Equal((0, 0, 1, 1), ((int)hours[1].SqlMedium, (int)hours[1].SqlSevere, (int)hours[1].OsMedium, (int)hours[1].OsSevere));
    }

    [Fact]
    public void AHalfHourDisplayZone_BinsOnItsOwnHourEdges_WhereTheTicksAre()
    {
        /* +05:30: 14:05 UTC is 19:35 there, in the hour that began at 19:00, which is 13:30 UTC; 14:40 UTC is 20:10, the hour from 14:30 UTC. */
        var halfHour = TimeZoneInfo.CreateCustomTimeZone("half-hour-zone", TimeSpan.FromMinutes(330), "half-hour-zone", "half-hour-zone");
        var hours = Place(halfHour, (Hour14.AddMinutes(5), 2, 0), (Hour14.AddMinutes(40), 2, 0));

        Assert.Equal(2, hours.Count);
        Assert.Equal(Hour14.AddMinutes(-30), (DateTime)hours[0].HourStartUtc);
        Assert.Equal(Hour14.AddMinutes(30), (DateTime)hours[1].HourStartUtc);

        double half = BarSize() / 2;
        double tick = Hour14.AddMinutes(-30).ToOADate();
        double nextTick = Hour14.AddMinutes(30).ToOADate();
        double sql = (double)hours[0].SqlStackPosition;
        double os = (double)hours[0].OsStackPosition;
        Assert.True(sql - half >= tick && os + half <= nextTick, $"the 13:30 hour's stacks span {At(sql - half)} to {At(os + half)}, outside the zone's hour");
    }

    private static Type Bars()
    {
        var type = typeof(ChartAxisStart).Assembly.GetType("PerformanceMonitor.Ui.MemoryPressureBars");
        Assert.True(type is not null, "PerformanceMonitor.Ui has no MemoryPressureBars: the Memory Pressure Events chart's bar maths is inline in the two renderers, with each stack centred on the hour's starting tick.");
        return type!;
    }

    private static double BarSize()
    {
        var field = Bars().GetField("BarSize", BindingFlags.Public | BindingFlags.Static);
        Assert.True(field is not null, "MemoryPressureBars has no public BarSize.");
        return (double)field!.GetValue(null)!;
    }

    /// <summary>The hours the chart draws for these samples, as the helper's own records, read dynamically.</summary>
    private static List<dynamic> Place(TimeZoneInfo zone, params (DateTime SampleTimeUtc, int ProcessIndicator, int SystemIndicator)[] samples)
    {
        var place = Bars().GetMethod("Place", BindingFlags.Public | BindingFlags.Static);
        Assert.True(place is not null, "MemoryPressureBars has no public static Place(samples, zone).");
        var hours = (IEnumerable)place!.Invoke(null, new object[] { samples.AsEnumerable(), zone })!;
        return hours.Cast<dynamic>().ToList();
    }

    private static string At(double oaDate) => DateTime.FromOADate(oaDate).ToString("HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
}
