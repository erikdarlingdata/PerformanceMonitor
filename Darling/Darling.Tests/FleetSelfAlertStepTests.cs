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
using System.Reflection;
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

    /// <summary>
    /// The step reads the server list off the launch loop, and the loop's reload adds and removes servers on every pass.
    /// The step walks a copy taken under the list's lock, so a reload while the step is inside its custom-rule read changes
    /// nothing the step is reading: the server that was removed is still in the step's set, the one that was added is not,
    /// and a server that is still monitored is never read as gone.
    /// </summary>
    [Fact]
    public async Task AReloadDuringAStep_ChangesNothingTheStepReads()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var ct = TestContext.Current.CancellationToken;
        var alpha = Server("alpha");
        var beta = Server("beta");
        var servers = new List<DarlingWorker.ServerLoopState> { alpha, beta };

        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        List<string>? before = null;
        List<string>? after = null;
        SetCustomAlertHealth(worker, async (fleet, _) =>
        {
            before = Names(fleet);
            await held.Task;
            after = Names(fleet);
        });

        var fleet = Snapshot(worker, servers);
        Assert.True(TryStart(worker, token => RunCustomAlertHealth(worker, fleet, token), ct));

        /* The reload, while the step is inside its read: beta leaves, gamma arrives. */
        Reconcile(worker, servers, new[] { alpha.Config, Server("gamma").Config });
        Assert.Equal(new[] { "alpha", "gamma" }, Names(servers));
        Assert.True(beta.Retired);

        held.SetResult();
        await Step(worker);

        Assert.Equal(new[] { "alpha", "beta" }, before);
        Assert.Equal(new[] { "alpha", "beta" }, after);
    }

    /// <summary>A server named <paramref name="name"/>; its id derives from the name, so distinct names are distinct servers.</summary>
    private static DarlingWorker.ServerLoopState Server(string name)
    {
        var config = new MonitoredServer { Name = name, Host = name + ".invalid" };
        return new DarlingWorker.ServerLoopState { Config = config };
    }

    private static List<string> Names(IEnumerable<DarlingWorker.ServerLoopState> fleet) =>
        fleet.Select(s => s.Config.Name).ToList();

    private static bool TryStart(DarlingWorker worker, Func<CancellationToken, Task> step, CancellationToken ct) =>
        worker.TryStartFleetSelfAlertStep(step, ct);

    /* The snapshot, the custom-rule half of the step and its seam are reached by name, so this class states what is missing
       when the step still walks the live list rather than failing to compile against it. */
    private const string Missing =
        "DarlingWorker has no {0}, so the fleet self-alert step walks the live server list off the launch loop while the loop's reload changes it.";

    private static object Member(string name, Func<Type, object?> find)
    {
        var member = find(typeof(DarlingWorker));
        if (member is null)
        {
            Assert.Fail(string.Format(System.Globalization.CultureInfo.InvariantCulture, Missing, name));
        }

        return member;
    }

    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    private static IReadOnlyList<DarlingWorker.ServerLoopState> Snapshot(DarlingWorker worker, List<DarlingWorker.ServerLoopState> servers)
    {
        var method = (MethodInfo)Member("SnapshotFleet", t => t.GetMethod("SnapshotFleet", Any));
        return (IReadOnlyList<DarlingWorker.ServerLoopState>)method.Invoke(worker, new object[] { servers })!;
    }

    private static void SetCustomAlertHealth(DarlingWorker worker, Func<IReadOnlyList<DarlingWorker.ServerLoopState>, CancellationToken, Task> evaluate)
    {
        var property = (PropertyInfo)Member("CustomAlertHealthOverride", t => t.GetProperty("CustomAlertHealthOverride", Any));
        property.SetValue(worker, evaluate);
    }

    private static Task RunCustomAlertHealth(DarlingWorker worker, IReadOnlyList<DarlingWorker.ServerLoopState> fleet, CancellationToken ct)
    {
        var method = (MethodInfo)Member("RunCustomAlertHealthAsync", t => t.GetMethod("RunCustomAlertHealthAsync", Any));
        return (Task)method.Invoke(worker, new object[] { fleet, ct })!;
    }

    private static void Reconcile(DarlingWorker worker, List<DarlingWorker.ServerLoopState> servers, IReadOnlyList<MonitoredServer> desired)
    {
        var method = (MethodInfo)Member("ReconcileServers", t => t.GetMethod("ReconcileServers", Any));
        method.Invoke(worker, new object[] { servers, desired });
    }

    private static Task Step(DarlingWorker worker)
    {
        var step = worker.FleetSelfAlertStepForTests;
        Assert.NotNull(step);
        return step;
    }
}
