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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9270: a <c>database_config</c> capture that holds a database still restoring or recovering is taken again in minutes, and the
/// change watch never compares such a capture. These cases drive the worker's on-load run and its scheduled pass with a stand-in for
/// the collector run and for the read of the capture, with no store.
/// </summary>
public sealed class TransitionalRecaptureTests
{
    private const string Collector = "database_config";
    private const int Daily = 1440;

    private static DarlingWorker MakeWorker() =>
        new(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());

    private static DarlingWorker.ServerLoopState MakeServer()
    {
        var config = new MonitoredServer { Name = "transitional-test", Host = "transitional-test" };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = "Server=tcp:transitional-test,1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo(),
            StorageName = "transitional-test",
            ServerId = 0,
        };
        return new DarlingWorker.ServerLoopState { Config = config, Runtime = runtime };
    }

    private static async Task<DarlingWorker.RunOutcome> RunAtConnectAsync(DarlingWorker worker, DarlingWorker.ServerLoopState server)
    {
        var outcome = new DarlingWorker.RunOutcome();
        var effective = StoreConfigProvider.ResolveSchedule(Collector, 0, worker.ScheduleOverridesForTest);
        await worker.RunOnLoadAsync(server, null!, Collector, 0, effective, TestContext.Current.CancellationToken, outcome);
        return outcome;
    }

    [Fact]
    public async Task ACaptureWithARestoringDatabase_MakesTheCollectorDueInAboutTenMinutes()
    {
        var worker = MakeWorker();
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(5);
        worker.RecaptureDueOverride = _ => true;
        var server = MakeServer();
        server.NextDue[Collector] = DateTime.UtcNow.AddMinutes(Daily);

        var outcome = await RunAtConnectAsync(worker, server);
        worker.ApplyTransitionalRecapture(server, Collector, outcome.RecaptureEarly);

        Assert.True(outcome.RecaptureEarly);
        var due = server.NextDue[Collector];
        Assert.InRange(due, DateTime.UtcNow.AddMinutes(9), DateTime.UtcNow.AddMinutes(11));
    }

    [Fact]
    public async Task ASettledCapture_LeavesTheNormalDueTimeAlone()
    {
        var worker = MakeWorker();
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(5);
        worker.RecaptureDueOverride = _ => false;
        var server = MakeServer();
        var normal = DateTime.UtcNow.AddMinutes(Daily);
        server.NextDue[Collector] = normal;

        var outcome = await RunAtConnectAsync(worker, server);
        worker.ApplyTransitionalRecapture(server, Collector, outcome.RecaptureEarly);

        Assert.False(outcome.RecaptureEarly);
        Assert.Equal(normal, server.NextDue[Collector]);
    }

    [Fact]
    public async Task AFailedRun_LeavesTheNormalDueTimeAlone()
    {
        var worker = MakeWorker();
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(-1);
        worker.RecaptureDueOverride = _ => true;
        var server = MakeServer();
        var normal = DateTime.UtcNow.AddMinutes(Daily);
        server.NextDue[Collector] = normal;

        var outcome = await RunAtConnectAsync(worker, server);
        worker.ApplyTransitionalRecapture(server, Collector, outcome.RecaptureEarly);

        Assert.Null(outcome.RecaptureEarly);
        Assert.Equal(normal, server.NextDue[Collector]);
    }

    private static Dictionary<string, DateTime> JustRan() => new(StringComparer.Ordinal) { [Collector] = DateTime.UtcNow };

    /// <summary>
    /// The at-connect pass of one collector (<see cref="DarlingWorker.SeedCollectorAtConnectAsync"/>): the run, the seed of its daily
    /// recapture, then the early recapture. The apply has to come after the seed or the next capture is a day out; this fails if the apply
    /// is moved above the seed or dropped.
    /// </summary>
    [Fact]
    public async Task AtConnect_ATransitionalCapture_LeavesTheCollectorDueInAboutTenMinutes_AfterTheSeed()
    {
        var worker = MakeWorker();
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(5);
        worker.RecaptureDueOverride = _ => true;
        var server = MakeServer();
        var effective = StoreConfigProvider.ResolveSchedule(Collector, 0, worker.ScheduleOverridesForTest);

        await worker.SeedCollectorAtConnectAsync(server, null!, Collector, 0, effective, JustRan(), DateTime.UtcNow, TestContext.Current.CancellationToken);

        Assert.InRange(server.NextDue[Collector], DateTime.UtcNow.AddMinutes(9), DateTime.UtcNow.AddMinutes(11));
    }

    [Fact]
    public async Task AtConnect_ASettledCapture_LeavesTheSeededDailyDueTime()
    {
        var worker = MakeWorker();
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(5);
        worker.RecaptureDueOverride = _ => false;
        var server = MakeServer();
        var effective = StoreConfigProvider.ResolveSchedule(Collector, 0, worker.ScheduleOverridesForTest);

        await worker.SeedCollectorAtConnectAsync(server, null!, Collector, 0, effective, JustRan(), DateTime.UtcNow, TestContext.Current.CancellationToken);

        Assert.True(server.NextDue[Collector] > DateTime.UtcNow.AddHours(20));
    }

    [Fact]
    public void ACollectorWithARunTime_KeepsItsTimeOfDay()
    {
        var worker = MakeWorker();
        var server = MakeServer();
        var normal = DateTime.UtcNow.AddHours(12);
        server.NextDue[Collector] = normal;
        server.RunTimeSlots[Collector] = new DarlingWorker.RunTimeSlot(120, Daily, server.Clock.Id, null);

        worker.ApplyTransitionalRecapture(server, Collector, true);

        Assert.Equal(normal, server.NextDue[Collector]);
    }

    [Fact]
    public void ACollectorThatIsNoLongerScheduled_IsNotScheduledAgain()
    {
        var worker = MakeWorker();
        var server = MakeServer();

        worker.ApplyTransitionalRecapture(server, Collector, true);

        Assert.False(server.NextDue.ContainsKey(Collector));
    }

    [Fact]
    public async Task TheScheduledRun_OfACaptureWithARestoringDatabase_IsRetakenInAboutTenMinutes()
    {
        var worker = MakeWorker();
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(5);
        worker.RecaptureDueOverride = _ => true;
        var server = MakeServer();
        server.NextDue[Collector] = DateTime.UtcNow.AddMinutes(-5);

        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);
        await Task.WhenAll(worker.InFlightDetachedRuns);

        Assert.InRange(server.NextDue[Collector], DateTime.UtcNow.AddMinutes(9), DateTime.UtcNow.AddMinutes(11));
    }

    [Fact]
    public async Task TheScheduledRun_OfASettledCapture_WaitsForTheNextDay()
    {
        var worker = MakeWorker();
        worker.RunOneBodyOverride = (_, _, _, _) => Task.FromResult(5);
        worker.RecaptureDueOverride = _ => false;
        var server = MakeServer();
        server.NextDue[Collector] = DateTime.UtcNow.AddMinutes(-5);

        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);
        await Task.WhenAll(worker.InFlightDetachedRuns);

        Assert.True(server.NextDue[Collector] > DateTime.UtcNow.AddHours(20));
    }
}
