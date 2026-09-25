/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4238: the fleet-level Availability Groups tab used to tear down and rebuild every card and every
/// per-database <c>DataGrid</c> on each 30 s refresh (<c>AgCards.ItemsSource = groups</c>, a brand new list of
/// brand new <see cref="AgTopologyCard"/> instances every time) with no virtualization — measured on a production
/// topology (42 AGs, 84 replicas, 398 database rows) at 10,353 realized elements and a 2,548 ms UI-thread stall
/// every tick. The fix keeps one <c>ObservableCollection&lt;AgTopologyCard&gt;</c>, reconciled in place by AG
/// identity (<see cref="AgTopology.CardKey"/>), and virtualizes the card list. These pins run the same
/// <see cref="AvailabilityGroupsTab.Render"/> entry point the shell's refresh timer drives, so they exercise the
/// real code path rather than a copy of it.
/// </summary>
public sealed class AvailabilityGroupsTabRefreshTests
{
    [Fact]
    public void Render_SameRowsAcrossTwoCalls_KeepsTheSameCardInstances()
    {
        /* The pin the ruling asks for: two refreshes over identical rows must not hand the ItemsControl a new
           object per card. Before the fix, Render assigns a brand new List<AgTopologyCard> (brand new instances)
           every call, so this fails on the pre-#4238 code — proven once, below. */
        OnStaThread(() =>
        {
            var tab = new AvailabilityGroupsTab();

            tab.Render(BuildTopology(agCount: 5, replicasPerAg: 2, dbRowsPerAg: 3));
            var first = CardsOf(tab);

            tab.Render(BuildTopology(agCount: 5, replicasPerAg: 2, dbRowsPerAg: 3));
            var second = CardsOf(tab);

            Assert.Equal(first.Count, second.Count);
            for (var i = 0; i < first.Count; i++)
            {
                Assert.Same(first[i], second[i]);
            }
        });
    }

    [Fact]
    public void Render_ChangedValue_UpdatesInPlace_SameInstanceNewValue()
    {
        OnStaThread(() =>
        {
            var tab = new AvailabilityGroupsTab();

            tab.Render(AgTopology.BuildCards(
                new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY") },
                Array.Empty<AgTopologyDatabaseRow>()));
            var before = Assert.Single(CardsOf(tab));
            Assert.Equal(HealthSeverity.Healthy, before.Severity);

            /* A fresh read, same AG identity, now unhealthy — simulates the next 30 s tick after a real change. */
            tab.Render(AgTopology.BuildCards(
                new[] { Replica(1, "NODE1", "AG1", "NODE1", "PRIMARY", syncHealth: "NOT_HEALTHY") },
                Array.Empty<AgTopologyDatabaseRow>()));
            var after = Assert.Single(CardsOf(tab));

            Assert.Same(before, after);
            Assert.Equal(HealthSeverity.Critical, after.Severity);
        });
    }

    [Fact]
    public void Render_AddedOrRemovedAg_AddsOrRemovesExactlyOneCard()
    {
        OnStaThread(() =>
        {
            var tab = new AvailabilityGroupsTab();

            tab.Render(BuildTopology(agCount: 5, replicasPerAg: 1, dbRowsPerAg: 1));
            Assert.Equal(5, CardsOf(tab).Count);
            var survivors = CardsOf(tab);

            var withExtra = new List<AgTopologyCard>(BuildTopology(agCount: 5, replicasPerAg: 1, dbRowsPerAg: 1))
            {
                Assert.Single(AgTopology.BuildCards(
                    new[] { Replica(99, "NODE99", "AG_EXTRA", "NODE99", "PRIMARY") },
                    Array.Empty<AgTopologyDatabaseRow>())),
            };
            tab.Render(withExtra);
            Assert.Equal(6, CardsOf(tab).Count);
            /* The five surviving AGs kept their card instances; only the new one is a new container. */
            foreach (var card in survivors)
            {
                Assert.Contains(CardsOf(tab), c => ReferenceEquals(c, card));
            }

            tab.Render(BuildTopology(agCount: 5, replicasPerAg: 1, dbRowsPerAg: 1));
            Assert.Equal(5, CardsOf(tab).Count);
        });
    }

    [Fact]
    public void AgCards_UsesAVirtualizingPanel()
    {
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AvailabilityGroupsTab.xaml");

        Assert.Contains("VirtualizingStackPanel", xaml, StringComparison.Ordinal);
        Assert.Contains("VirtualizingPanel.IsVirtualizing=\"True\"", xaml, StringComparison.Ordinal);
        Assert.Contains("VirtualizationMode=\"Recycling\"", xaml, StringComparison.Ordinal);
        Assert.Contains("ScrollUnit=\"Pixel\"", xaml, StringComparison.Ordinal);
        Assert.Contains("CanContentScroll=\"True\"", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void Render_UnchangedRows_StaysUnderTheHundredMillisecondBudget()
    {
        /* The production shape from #4238: 42 AGs, 84 replicas, 398 database rows. */
        OnStaThread(() =>
        {
            var tab = new AvailabilityGroupsTab();
            var topology = BuildTopology(agCount: 42, replicasPerAg: 2, dbRowsPerAg: 9, extraDbRowsOnFirst: 20);

            tab.Render(topology); // first render always does full work; only the SECOND is timed.

            var sw = Stopwatch.StartNew();
            tab.Render(BuildTopology(agCount: 42, replicasPerAg: 2, dbRowsPerAg: 9, extraDbRowsOnFirst: 20));
            sw.Stop();

            Assert.True(sw.Elapsed.TotalMilliseconds < 100,
                $"unchanged-rows refresh took {sw.Elapsed.TotalMilliseconds:N1} ms, budget is 100 ms (#4238)");
        });
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    private static List<AgTopologyCard> CardsOf(AvailabilityGroupsTab tab) =>
        ((IEnumerable<AgTopologyCard>)tab.AgCards.ItemsSource).ToList();

    private static List<AgTopologyCard> BuildTopology(int agCount, int replicasPerAg, int dbRowsPerAg, int extraDbRowsOnFirst = 0)
    {
        var replicas = new List<AgTopologyReplicaRow>();
        var databases = new List<AgTopologyDatabaseRow>();

        for (var ag = 0; ag < agCount; ag++)
        {
            var agName = $"AG{ag}";
            for (var r = 0; r < replicasPerAg; r++)
            {
                replicas.Add(Replica(ag, $"NODE{ag}", agName, $"NODE{ag}_{r}", r == 0 ? "PRIMARY" : "SECONDARY"));
            }

            var rows = dbRowsPerAg + (ag == 0 ? extraDbRowsOnFirst : 0);
            for (var d = 0; d < rows; d++)
            {
                databases.Add(Database(ag, $"NODE{ag}", agName, $"DB{d}", $"NODE{ag}_0"));
            }
        }

        return AgTopology.BuildCards(replicas, databases);
    }

    private static AgTopologyReplicaRow Replica(
        int serverId, string serverName, string agName, string replicaName, string role, string syncHealth = "HEALTHY") =>
        new()
        {
            ServerId = serverId,
            ServerName = serverName,
            CollectionTime = new DateTime(2026, 9, 25, 12, 0, 0),
            AgName = agName,
            ReplicaServerName = replicaName,
            RoleDesc = role,
            OperationalStateDesc = "ONLINE",
            ConnectedStateDesc = "CONNECTED",
            RecoveryHealthDesc = "ONLINE",
            SynchronizationHealthDesc = syncHealth,
            AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
            FailoverModeDesc = "AUTOMATIC",
        };

    private static AgTopologyDatabaseRow Database(int serverId, string serverName, string agName, string db, string replicaName) =>
        new()
        {
            ServerId = serverId,
            ServerName = serverName,
            CollectionTime = new DateTime(2026, 9, 25, 12, 0, 0),
            AgName = agName,
            DatabaseName = db,
            ReplicaServerName = replicaName,
            SynchronizationStateDesc = "SYNCHRONIZED",
            AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
            IsSuspended = false,
            SecondaryLagSeconds = 0,
            LogSendQueueKb = 0,
            RedoQueueKb = 0,
            LogSendRateKbPerSec = 0,
            RedoRateKbPerSec = 0,
        };

    /// <summary>WPF objects require STA; same shape as RawWindowFloorViewerPortTests / Lite.Tests' MainWindowAccessKeyTests.</summary>
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
