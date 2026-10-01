/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitorLite.Helpers;
using Xunit;

namespace Lite.Tests;

public sealed class PickerRefreshCoalescerTests
{
    private static (PickerRefreshCoalescer C, Queue<Action> Posted, int[] Runs) Make()
    {
        var posted = new Queue<Action>();
        var runs = new int[1];
        return (new PickerRefreshCoalescer(posted.Enqueue, () => runs[0]++), posted, runs);
    }

    [Fact]
    public void A_burst_of_requests_posts_one_callback()
    {
        var (c, posted, runs) = Make();

        c.Request();
        c.Request();
        c.Request();

        Assert.Single(posted);
        Assert.Equal(0, runs[0]);
    }

    [Fact]
    public void The_callback_runs_the_refresh_once_and_clears_the_guard()
    {
        var (c, posted, runs) = Make();
        c.Request();
        c.Request();

        posted.Dequeue()();

        Assert.Equal(1, runs[0]);
        Assert.Empty(posted);

        c.Request();
        Assert.Single(posted);
    }

    [Fact]
    public void A_request_made_during_the_refresh_posts_a_follow_up()
    {
        var posted = new Queue<Action>();
        PickerRefreshCoalescer? c = null;
        var runs = 0;
        c = new PickerRefreshCoalescer(posted.Enqueue, () => { runs++; if (runs == 1) c!.Request(); });
        c.Request();

        posted.Dequeue()();

        Assert.Single(posted);
    }
}
