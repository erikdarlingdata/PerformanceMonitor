/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8133: the All Servers fleet cut with a guaranteed minimum per server (<see cref="LocalDataService.ApplyJobHistoryShare"/>). A busy
/// server and a quiet one: under the cap every row comes back; over it each server keeps its share (the cap over the servers present,
/// rounded up) before the newest of the rest fill the room; with the share off the page is the newest runs of the whole fleet.
/// </summary>
public sealed class JobHistoryShareTests
{
    private const int Busy = 1;
    private const int Quiet = 2;
    private static readonly DateTime Start = new(2026, 3, 10, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>Six busy runs on the hour from 12:00 back to 07:00, two quiet runs on the half hour, so no two runs tie.</summary>
    private static List<JobHistoryRow> Fleet()
    {
        var rows = new List<JobHistoryRow>();
        for (var i = 0; i < 6; i++)
        {
            rows.Add(new JobHistoryRow { ServerId = Busy, InstanceId = 100 + i, RunDateTimeUtc = Start.AddHours(12 - i), RunDateTime = Start.AddHours(12 - i) });
        }

        rows.Add(new JobHistoryRow { ServerId = Quiet, InstanceId = 200, RunDateTimeUtc = Start.AddHours(8.5), RunDateTime = Start.AddHours(8.5) });
        rows.Add(new JobHistoryRow { ServerId = Quiet, InstanceId = 201, RunDateTimeUtc = Start.AddHours(7.5), RunDateTime = Start.AddHours(7.5) });
        return rows;
    }

    private static List<JobHistoryRow> Shared(int cap) =>
        LocalDataService.ApplyJobHistoryShare(LocalDataService.ApplyJobHistoryWindow(Fleet(), Start, int.MaxValue), cap);

    [Fact]
    public void UnderTheCap_TheWholeFleetComesBack()
    {
        var page = Shared(10);
        Assert.Equal(8, page.Count);
        Assert.Equal(6, page.Count(r => r.ServerId == Busy));
        Assert.Equal(2, page.Count(r => r.ServerId == Quiet));
    }

    [Fact]
    public void OverTheCap_EachServerKeepsItsShare_ThenTheNewestOfTheRestFillTheRoom()
    {
        /* Cap four over two servers: a share of two each, the two newest of both. */
        var four = Shared(4);
        Assert.Equal(new[] { 100L, 101L }, four.Where(r => r.ServerId == Busy).Select(r => r.InstanceId).ToArray());
        Assert.Equal(new[] { 200L, 201L }, four.Where(r => r.ServerId == Quiet).Select(r => r.InstanceId).ToArray());

        /* Cap five: a share of three, the quiet server has two, so the busy server's third newest run takes the last place. */
        var five = Shared(5);
        Assert.Equal(5, five.Count);
        Assert.Equal(new[] { 100L, 101L, 102L }, five.Where(r => r.ServerId == Busy).Select(r => r.InstanceId).ToArray());
        Assert.Equal(2, five.Count(r => r.ServerId == Quiet));
    }

    /// <summary>The share decides which runs are on the page; the page itself reads newest-first across servers, like every other read.</summary>
    [Fact]
    public void OverTheCap_ThePageIsStillNewestFirstAcrossServers()
    {
        /* Six busy runs and one quiet run at 08:30, cap five: the share is three, so the busy server's 09:00 run fills the last place,
           and it belongs above the quiet 08:30 run on the page. */
        var rows = Fleet().Where(r => r.ServerId == Busy || r.InstanceId == 200).ToList();
        var page = LocalDataService.ApplyJobHistoryShare(LocalDataService.ApplyJobHistoryWindow(rows, Start, int.MaxValue), 5);
        Assert.Equal(new[] { 100L, 101L, 102L, 103L, 200L }, page.Select(r => r.InstanceId).ToArray());
        Assert.Equal(page.OrderByDescending(r => r.RunDateTimeUtc).Select(r => r.InstanceId), page.Select(r => r.InstanceId));
    }

    [Fact]
    public void WithTheShareOff_ThePageIsTheNewestOfTheWholeFleet()
    {
        var page = LocalDataService.ApplyJobHistoryWindow(Fleet(), Start, 4);
        Assert.Equal(4, page.Count);
        Assert.All(page, r => Assert.Equal(Busy, r.ServerId));
    }
}
