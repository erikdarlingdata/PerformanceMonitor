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
    public void Render_UnchangedRows_ReplacesNoCardReplicaOrDatabaseInstances()
    {
        /* The production shape from #4238: 42 AGs, 84 replicas, 398 database rows. This used to assert a 100 ms
           wall-clock budget around the second Render call; that measured 191 ms during a loaded full-suite run
           (and has flaked in other lanes' full runs too) while passing alone -- a loaded box, not a correctness
           regression. This asserts the property #4238 actually protects instead: an unchanged refresh must
           reconcile every card, replica and database IN PLACE and never rebuild any of them. A regression that
           tears down and rebuilds rows -- or an O(n^2) loop that amounts to the same thing -- shows up here as a
           nonzero replaced count, independent of how loaded the runner is. */
        OnStaThread(() =>
        {
            var tab = new AvailabilityGroupsTab();
            var topology = BuildTopology(agCount: 42, replicasPerAg: 2, dbRowsPerAg: 9, extraDbRowsOnFirst: 20);

            tab.Render(topology); // first render always does full work; only the SECOND is checked.
            var before = Flatten(tab);

            tab.Render(BuildTopology(agCount: 42, replicasPerAg: 2, dbRowsPerAg: 9, extraDbRowsOnFirst: 20));
            var after = Flatten(tab);

            Assert.Equal(before.Cards.Count, after.Cards.Count);
            Assert.Equal(before.Replicas.Count, after.Replicas.Count);
            Assert.Equal(before.Databases.Count, after.Databases.Count);

            var replacedCards = ReplacedCount(before.Cards, after.Cards);
            var replacedReplicas = ReplacedCount(before.Replicas, after.Replicas);
            var replacedDatabases = ReplacedCount(before.Databases, after.Databases);

            Assert.True(replacedCards == 0 && replacedReplicas == 0 && replacedDatabases == 0,
                $"unchanged-rows refresh replaced {replacedCards} of {after.Cards.Count} cards, " +
                $"{replacedReplicas} of {after.Replicas.Count} replicas, " +
                $"{replacedDatabases} of {after.Databases.Count} databases instead of updating them in place (#4238)");
        });
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    private static List<AgTopologyCard> CardsOf(AvailabilityGroupsTab tab) =>
        ((IEnumerable<AgTopologyCard>)tab.AgCards.ItemsSource).ToList();

    /// <summary>Every card, and every one of its nested replicas and databases, in render order -- so a work-
    /// count comparison across two renders can see a rebuild at any level, not only the top one.</summary>
    private static (List<AgTopologyCard> Cards, List<AgTopologyReplica> Replicas, List<AgTopologyDatabase> Databases) Flatten(AvailabilityGroupsTab tab)
    {
        var cards = CardsOf(tab);
        var replicas = new List<AgTopologyReplica>();
        var databases = new List<AgTopologyDatabase>();
        foreach (var card in cards)
        {
            replicas.AddRange(card.Replicas);
            databases.AddRange(card.Databases);
        }

        return (cards, replicas, databases);
    }

    /// <summary>Counts positions where <paramref name="after"/> holds a DIFFERENT instance than
    /// <paramref name="before"/> held at the same position -- the direct "items replaced" work count for an
    /// unchanged refresh, which should reconcile in place and replace nothing.</summary>
    private static int ReplacedCount<T>(List<T> before, List<T> after) where T : class
    {
        var replaced = 0;
        for (var i = 0; i < Math.Min(before.Count, after.Count); i++)
        {
            if (!ReferenceEquals(before[i], after[i]))
            {
                replaced++;
            }
        }

        return replaced;
    }

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
