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

        posted.Dequeue()();

        Assert.Equal(2, runs);
        Assert.Empty(posted);
    }

    [Fact]
    public void A_throwing_post_does_not_wedge_the_guard()
    {
        var fail = true;
        var posted = new Queue<Action>();
        var c = new PickerRefreshCoalescer(a => { if (fail) throw new InvalidOperationException("post failed"); posted.Enqueue(a); }, () => { });

        Assert.Throws<InvalidOperationException>(() => c.Request());

        fail = false;
        c.Request();
        Assert.Single(posted);
    }

    private static (PickerRefreshCoalescer C, Queue<Action> Posted, int[] Runs, string[] Sig) MakeGated()
    {
        var posted = new Queue<Action>();
        var runs = new int[1];
        var sig = new[] { "a" };
        return (new PickerRefreshCoalescer(posted.Enqueue, () => runs[0]++, () => sig[0]), posted, runs, sig);
    }

    [Fact]
    public void An_unchanged_signature_does_not_run_the_refresh()
    {
        var (c, posted, runs, _) = MakeGated();
        c.Request();
        posted.Dequeue()();
        c.Request();
        posted.Dequeue()();

        Assert.Equal(1, runs[0]);
    }

    [Fact]
    public void A_changed_signature_runs_once_and_is_recorded()
    {
        var (c, posted, runs, sig) = MakeGated();
        c.Request();
        posted.Dequeue()();
        sig[0] = "a\u001fb";
        c.Request();
        posted.Dequeue()();
        c.Request();
        posted.Dequeue()();

        Assert.Equal(2, runs[0]);
    }

    [Fact]
    public void Two_posts_with_the_same_signature_run_one_refresh()
    {
        var (c, posted, runs, _) = MakeGated();
        c.Request();
        posted.Dequeue()();
        c.Request();
        c.Request();
        posted.Dequeue()();

        Assert.Equal(1, runs[0]);
    }

    [Fact]
    public void An_invalidated_gate_applies_even_an_old_signature()
    {
        var (c, posted, runs, _) = MakeGated();
        c.Request();
        posted.Dequeue()();
        c.Invalidate();
        c.Request();
        posted.Dequeue()();

        Assert.Equal(2, runs[0]);
    }

    [Fact]
    public void The_signature_ignores_selection_order()
    {
        Assert.Equal(PickerRefreshCoalescer.SignatureOf(new[] { "b", "a" }), PickerRefreshCoalescer.SignatureOf(new[] { "a", "b" }));
    }
}
