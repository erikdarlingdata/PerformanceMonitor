/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: a server tab opened while a fleet tab's load is running used to leave a request in the shell's one slot and wait for that load to
/// finish (7.9 s of one field first open). The shell now keeps one lane for the fleet tabs and one for the server tabs.
/// </summary>
public sealed class ViewerRefreshLanesTests
{
    [Fact]
    public async Task AServerTabLoad_StartsWhileAFleetTabLoadIsStillRunning()
    {
        var lanes = new ViewerRefreshLanes();
        var serverSelected = false;
        var fleetRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fleetStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverStarted = 0;

        var fleet = lanes.RunAsync(() => serverSelected, async () =>
        {
            fleetStarted.SetResult();
            await fleetRelease.Task;
        });
        await fleetStarted.Task;

        /* The reader clicks a server tab while the Overview read is still out. */
        serverSelected = true;
        var server = lanes.RunAsync(() => serverSelected, () =>
        {
            Interlocked.Increment(ref serverStarted);
            return Task.CompletedTask;
        });

        var finishedFirst = await Task.WhenAny(server, Task.Delay(5000));
        Assert.Same(server, finishedFirst);
        Assert.Equal(1, serverStarted);
        Assert.False(fleet.IsCompleted);

        fleetRelease.SetResult();
        await fleet;
    }

    [Fact]
    public async Task AFleetTabLoad_StartsWhileAServerTabLoadIsStillRunning()
    {
        var lanes = new ViewerRefreshLanes();
        var serverSelected = true;
        var serverRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fleetLoads = 0;

        var server = lanes.RunAsync(() => serverSelected, async () =>
        {
            serverStarted.SetResult();
            await serverRelease.Task;
        });
        await serverStarted.Task;

        serverSelected = false;
        var fleet = lanes.RunAsync(() => serverSelected, () =>
        {
            Interlocked.Increment(ref fleetLoads);
            return Task.CompletedTask;
        });

        Assert.Same(fleet, await Task.WhenAny(fleet, Task.Delay(5000)));
        Assert.Equal(1, fleetLoads);

        serverRelease.SetResult();
        await server;
    }

    [Fact]
    public async Task ATriggerInTheSameLaneMidLoad_ReloadsOnceMoreInsteadOfBeingDropped()
    {
        var lanes = new ViewerRefreshLanes();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loads = 0;

        var first = lanes.RunAsync(() => false, async () =>
        {
            if (Interlocked.Increment(ref loads) == 1)
            {
                started.SetResult();
                await release.Task;
            }
        });
        await started.Task;

        /* A second fleet trigger bounces: it returns at once and starts no load of its own. */
        await lanes.RunAsync(() => false, () =>
        {
            Interlocked.Increment(ref loads);
            return Task.CompletedTask;
        });
        Assert.Equal(1, loads);

        release.SetResult();
        await first;
        Assert.Equal(2, loads);
    }

    [Fact]
    public async Task ARequestWhoseTabIsNoLongerOnScreen_DoesNotLoadThatTabInTheWrongLane()
    {
        var lanes = new ViewerRefreshLanes();
        var serverSelected = true;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loads = 0;

        var first = lanes.RunAsync(() => serverSelected, async () =>
        {
            Interlocked.Increment(ref loads);
            started.SetResult();
            await release.Task;
        });
        await started.Task;

        /* A second server-tab trigger queues a request, then the reader moves to a fleet tab before the load ends. */
        await lanes.RunAsync(() => serverSelected, () => Task.CompletedTask);
        serverSelected = false;
        release.SetResult();
        await first;

        Assert.Equal(1, loads);
    }
}
