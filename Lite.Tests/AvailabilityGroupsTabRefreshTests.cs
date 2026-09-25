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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's twin of Darling.Tests' <c>AvailabilityGroupsTabRefreshTests</c> (#4238): Lite's AG tab had the exact
/// same defect — <c>AgCards.ItemsSource = cards</c>, a brand new list of brand new <see cref="AgTopologyCard"/>
/// instances on every refresh, no virtualization. The fix is the same shared mechanism
/// (<see cref="AgTopology.Reconcile{TItem,TKey}"/> / <see cref="AgTopology.ComputeDigest"/>), so this file pins
/// only what is specific to Lite's control: the same-instance guarantee through its own <c>Render</c> entry
/// point, and its own XAML's virtualizing panel. The shared reconcile/update-in-place/digest behavior itself is
/// pinned once, in Darling.Tests, so the two suites are not pinning the identical BCL code twice.
/// </summary>
public sealed class AvailabilityGroupsTabRefreshTests
{
    [Fact]
    public void Render_SameRowsAcrossTwoCalls_KeepsTheSameCardInstances()
    {
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
    public void AgCards_UsesAVirtualizingPanel()
    {
        var xaml = ReadRepoFile("Lite/Controls/AvailabilityGroupsTab.xaml");

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

            tab.Render(topology);

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
                replicas.Add(new AgTopologyReplicaRow
                {
                    ServerId = ag,
                    ServerName = $"NODE{ag}",
                    CollectionTime = new DateTime(2026, 9, 25, 12, 0, 0),
                    AgName = agName,
                    ReplicaServerName = $"NODE{ag}_{r}",
                    RoleDesc = r == 0 ? "PRIMARY" : "SECONDARY",
                    OperationalStateDesc = "ONLINE",
                    ConnectedStateDesc = "CONNECTED",
                    RecoveryHealthDesc = "ONLINE",
                    SynchronizationHealthDesc = "HEALTHY",
                    AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
                    FailoverModeDesc = "AUTOMATIC",
                });
            }

            var rows = dbRowsPerAg + (ag == 0 ? extraDbRowsOnFirst : 0);
            for (var d = 0; d < rows; d++)
            {
                databases.Add(new AgTopologyDatabaseRow
                {
                    ServerId = ag,
                    ServerName = $"NODE{ag}",
                    CollectionTime = new DateTime(2026, 9, 25, 12, 0, 0),
                    AgName = agName,
                    DatabaseName = $"DB{d}",
                    ReplicaServerName = $"NODE{ag}_0",
                    SynchronizationStateDesc = "SYNCHRONIZED",
                    AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
                    IsSuspended = false,
                    SecondaryLagSeconds = 0,
                    LogSendQueueKb = 0,
                    RedoQueueKb = 0,
                    LogSendRateKbPerSec = 0,
                    RedoRateKbPerSec = 0,
                });
            }
        }

        return AgTopology.BuildCards(replicas, databases);
    }

    /// <summary>WPF objects require STA; same shape as Darling.Tests' RawWindowFloorViewerPortTests.</summary>
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

    /* Locate the repo from this file — the DarlingLockTimeoutYieldTests idiom; no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
