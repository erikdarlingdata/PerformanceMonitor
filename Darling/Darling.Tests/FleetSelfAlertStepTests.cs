/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet-level self-alerts (store disk pressure, stale mute rules, failing notification channels, the TLS certificates
/// and the rest) are evaluated from the collection launch loop, and a self-alert that posts to a webhook can wait on the
/// destination's pacing or on the hold an HTTP 429 left (#8244). The launch loop must not: a wait there delays every
/// collection body's launch. So the evaluations run as one tracked step the loop starts and does not await, one at a time,
/// as the store-maintenance tick does. A tick that finds the previous step still running starts no second one, and the
/// step's own failure never reaches the loop.
/// </summary>
public sealed class FleetSelfAlertStepTests
{
    [Fact]
    public async Task TheLaunchLoop_DoesNotWaitOnAFleetSelfAlertStep_AndRunsOneAtATime()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var ct = TestContext.Current.CancellationToken;
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = 0;

        /* The step sits inside a send that is waiting on its destination: the launch call comes back at once. */
        Assert.True(TryStart(worker, _ => { started++; return held.Task; }, ct));
        Assert.Equal(1, started);

        /* The next tick finds the step still running and starts no second one, and waits on nothing. */
        Assert.False(TryStart(worker, _ => { started++; return Task.CompletedTask; }, ct));
        Assert.Equal(1, started);

        held.SetResult();
        await Step(worker);

        /* The tick after the step ends starts the next one. */
        Assert.True(TryStart(worker, _ => { started++; return Task.CompletedTask; }, ct));
        Assert.Equal(2, started);
        await Step(worker);
    }

    [Fact]
    public async Task AStepThatThrows_EndsQuietly_AndTheNextTickStartsAnother()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var ct = TestContext.Current.CancellationToken;

        Assert.True(TryStart(worker, _ => throw new InvalidOperationException("the evaluator failed"), ct));
        await Step(worker);

        Assert.True(TryStart(worker, _ => Task.CompletedTask, ct));
        await Step(worker);
    }

    private static bool TryStart(DarlingWorker worker, Func<CancellationToken, Task> step, CancellationToken ct) =>
        worker.TryStartFleetSelfAlertStep(step, ct);

    private static Task Step(DarlingWorker worker)
    {
        var step = worker.FleetSelfAlertStepForTests;
        Assert.NotNull(step);
        return step;
    }
}
