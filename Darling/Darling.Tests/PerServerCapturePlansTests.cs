// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// #6771: a server's own <c>capture_plans</c> decides whether plan XML is fetched for it; NULL follows the service-wide setting.
/// </summary>
public sealed class PerServerCapturePlansTests
{
    private const string UnusedStore = "Host=127.0.0.1;Port=1;Database=example;Username=example;Password=example";

    private static DarlingCollectorRunner Runner(bool fleet) =>
        new(NpgsqlDataSource.Create(UnusedStore), new CollectorDeltaCalculator(), logger: null,
            capturePlans: () => fleet, procedureStatsDeferredPlanFetch: () => "on", procedureStatsPlanCycleInterval: () => 1);

    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store")]
    [InlineData("procedure_stats")]
    public void FleetOn_AServerSetOff_FetchesNoPlans_AServerLeftNullDoes(string collector)
    {
        var runner = Runner(fleet: true);

        /* Server 8 sits on a cadence cycle that captures for procedure_stats (the first call of the instance). */
        Assert.False(runner.ShouldCapturePlanXmlFor(collector, serverId: 8, serverCapturePlans: false));
        Assert.True(runner.ShouldCapturePlanXmlFor(collector, serverId: 9, serverCapturePlans: null));
        Assert.True(runner.ShouldCapturePlanXmlFor(collector, serverId: 10, serverCapturePlans: true));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("query_store")]
    [InlineData("procedure_stats")]
    public void FleetOff_AServerSetOn_FetchesPlans_AServerLeftNullDoesNot(string collector)
    {
        var runner = Runner(fleet: false);

        Assert.True(runner.ShouldCapturePlanXmlFor(collector, serverId: 8, serverCapturePlans: true));
        Assert.False(runner.ShouldCapturePlanXmlFor(collector, serverId: 9, serverCapturePlans: null));
        Assert.False(runner.ShouldCapturePlanXmlFor(collector, serverId: 10, serverCapturePlans: false));
    }

    [Fact]
    public void TheProcedureStatsPlanFetchAndTheDeferredQueryStatsFetch_FollowTheServersOwnSetting()
    {
        var target = new CollectorTargetInfo();
        var fleetOn = Runner(fleet: true);
        var fleetOff = Runner(fleet: false);

        Assert.Equal(ProcedureStatsPlanFetchMode.Off, fleetOn.ProcedureStatsPlanFetchModeFor("procedure_stats", target, false));
        Assert.Equal(ProcedureStatsPlanFetchMode.On, fleetOn.ProcedureStatsPlanFetchModeFor("procedure_stats", target, null));
        Assert.Equal(ProcedureStatsPlanFetchMode.On, fleetOff.ProcedureStatsPlanFetchModeFor("procedure_stats", target, true));
        Assert.Equal(ProcedureStatsPlanFetchMode.Off, fleetOff.ProcedureStatsPlanFetchModeFor("procedure_stats", target, null));

        /* The identity columns that serve the module plan fetch are not sent for a server that captures no plans. */
        var offServer = fleetOn.StampPlanFetchFlags("procedure_stats", capturePlanXml: false, target, serverCapturePlans: false);
        Assert.False(offServer.DeferPlanXmlFetch);
        Assert.False(offServer.PlanIdentityColumns);
        var onServer = fleetOff.StampPlanFetchFlags("procedure_stats", capturePlanXml: true, target, serverCapturePlans: true);
        Assert.True(onServer.DeferPlanXmlFetch);
        Assert.True(onServer.PlanIdentityColumns);

        Assert.False(fleetOn.ShouldDeferPlanFetchFor("procedure_stats", capturePlanXml: false, target, serverCapturePlans: false));
    }

    [Fact]
    public void TheServiceWideSettingIsStillReadLive_ForAServerWithNoSetting()
    {
        var fleet = true;
        var runner = new DarlingCollectorRunner(
            NpgsqlDataSource.Create(UnusedStore), new CollectorDeltaCalculator(), logger: null, capturePlans: () => fleet);

        Assert.True(runner.CapturePlansFor(null));
        fleet = false;
        Assert.False(runner.CapturePlansFor(null));
        Assert.True(runner.CapturePlansFor(true));
    }

    private static ServerRuntime RuntimeFor(bool? capturePlans) => new()
    {
        Config = new MonitoredServer { Name = "s", Host = "s.invalid", StoredServerId = 7, CapturePlans = capturePlans },
        CapturePlans = capturePlans,
        ConnectionString = "Server=s.invalid;Integrated Security=true",
        Target = new CollectorTargetInfo(),
        StorageName = "s.invalid",
        ServerId = 7,
    };

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(true, null, true)]
    [InlineData(false, true, true)]
    [InlineData(false, null, false)]
    public async Task TheQueryStoreBackfill_FetchesPlansForTheServerPerItsOwnSetting(bool fleet, bool? server, bool expected)
    {
        await using var postgres = NpgsqlDataSource.Create(UnusedStore);
        var backfill = new QueryStoreBackfill(
            postgres, new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator(), capturePlans: () => fleet),
            new CollectorDeltaCalculator(), logger: null);

        Assert.Equal(expected, backfill.CapturePlanXmlFor(RuntimeFor(server)));
    }

    [Fact]
    public void AnEditedSetting_ReachesTheConnectedRuntime_WithoutAReconnect()
    {
        var loopState = typeof(DarlingWorker).GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;
        var held = new MonitoredServer { Name = "s", Host = "s.invalid", StoredServerId = 7, CapturePlans = null };
        var state = ServerConnectBackoffTests.NewLoopState(loopState, held);
        var runtime = RuntimeFor(null);
        loopState.GetProperty("Runtime")!.SetValue(state, runtime);
        loopState.GetProperty("LastKnownRuntime")!.SetValue(state, runtime);

        var worker = (DarlingWorker)System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(DarlingWorker));
        typeof(DarlingWorker).GetField("_logger", BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, NullLogger<DarlingWorker>.Instance);
        var list = Activator.CreateInstance(typeof(List<>).MakeGenericType(loopState))!;
        list.GetType().GetMethod("Add")!.Invoke(list, new[] { state });
        var reconcile = typeof(DarlingWorker).GetMethod("ReconcileServers", BindingFlags.NonPublic | BindingFlags.Instance)!;

        var edited = new MonitoredServer { Name = "s", Host = "s.invalid", StoredServerId = 7, CapturePlans = false };
        reconcile.Invoke(worker, new object[] { list, new List<MonitoredServer> { edited } });

        Assert.Same(runtime, loopState.GetProperty("Runtime")!.GetValue(state));
        Assert.False(runtime.CapturePlans);

        var cleared = new MonitoredServer { Name = "s", Host = "s.invalid", StoredServerId = 7, CapturePlans = null };
        reconcile.Invoke(worker, new object[] { list, new List<MonitoredServer> { cleared } });

        Assert.Same(runtime, loopState.GetProperty("Runtime")!.GetValue(state));
        Assert.Null(runtime.CapturePlans);
    }

    [Fact]
    public async Task TheRegistryRead_CarriesNullTrueAndFalseToTheServerModel()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        var ids = new (string Name, int Id, string Value)[] { ("cp-null", 6771001, "NULL"), ("cp-true", 6771002, "TRUE"), ("cp-false", 6771003, "FALSE") };
        foreach (var (name, id, value) in ids)
        {
            await using var insert = owner.CreateCommand(
                "INSERT INTO config_monitored_servers (server_id, name, host, auth, capture_plans) " +
                $"VALUES ({id}, '{name}', '{name}.example.test', 'windows', {value})");
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using var connection = await owner.OpenConnectionAsync(ct);
        var servers = await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct);

        Assert.Null(servers.Single(s => s.Name == "cp-null").CapturePlans);
        Assert.True(servers.Single(s => s.Name == "cp-true").CapturePlans);
        Assert.False(servers.Single(s => s.Name == "cp-false").CapturePlans);
    }

    [Theory]
    [InlineData(null, "service")]
    [InlineData(true, "on")]
    [InlineData(false, "off")]
    public void TheViewerDialogsThreeChoices_RoundTripThroughTheStoredValue(bool? stored, string tag)
    {
        Assert.Equal(tag, AddServerDialog.CapturePlansTagOf(stored));
        Assert.Equal(stored, AddServerDialog.CapturePlansOfTag(tag));
        Assert.Null(AddServerDialog.CapturePlansOfTag(null));
    }
}
