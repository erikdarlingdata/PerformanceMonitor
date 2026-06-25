/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Tests for the shared <see cref="DeadlockGraphParser"/>, driven primarily by REAL deadlock graphs
/// captured from sql2025 (HammerDB TPC-C) plus synthetic fixtures for the cases that workload never
/// produced (parallel self-edge, Azure wrapper, nameless resource, cross-product). See
/// <c>Fixtures/Deadlocks/README.md</c> for provenance and the asserted cycle structure of each sample.
/// Tested once in Common (not duplicated per app). No database, no WPF.
/// </summary>
public class DeadlockGraphParserTests
{
    // ── fixtures ──

    internal static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "Deadlocks", name));

    /// <summary>The edge set expressed as (waiter spid -> owner spid), for cycle-structure assertions.</summary>
    private static HashSet<(int Waiter, int Owner)> SpidEdges(DeadlockGraphModel m)
    {
        var bySpid = m.Processes.ToDictionary(p => p.Id, p => p.Spid, StringComparer.Ordinal);
        return m.Edges.Select(e => (bySpid[e.WaiterProcessId], bySpid[e.OwnerProcessId])).ToHashSet();
    }

    private static int[] Spids(DeadlockGraphModel m) => m.Processes.Select(p => p.Spid).OrderBy(s => s).ToArray();
    private static int[] VictimSpids(DeadlockGraphModel m) =>
        m.Processes.Where(p => p.IsVictim).Select(p => p.Spid).OrderBy(s => s).ToArray();

    // ── real: 2-process ──

    [Fact]
    public void Real2Proc_ParsesProcessesVictimAndTwoCycle()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_2proc_real_sql2025.xml"));

        Assert.Equal(new[] { 85, 103 }, Spids(m));
        Assert.Equal(new[] { 103 }, VictimSpids(m));
        Assert.False(m.IsParallel);

        // 103 -> 85 (waits on new_order), 85 -> 103 (waits on orders): a 2-cycle.
        Assert.Equal(new HashSet<(int, int)> { (103, 85), (85, 103) }, SpidEdges(m));

        // Resource label leads with the friendly kind + the real object name.
        var bySpid = m.Processes.ToDictionary(p => p.Id, p => p.Spid, StringComparer.Ordinal);
        var edge103 = m.Edges.Single(e => bySpid[e.WaiterProcessId] == 103);
        Assert.Equal("PAGE hammerdb_tpcc.dbo.new_order", edge103.ResourceLabel);
        Assert.Equal("pagelock", edge103.ResourceKind);
        Assert.Equal("X", edge103.RequestMode);
        Assert.Equal("X", edge103.OwnerMode);
    }

    [Fact]
    public void Real2Proc_LeadsWithStatementAndContext_NotNumericDbId()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_2proc_real_sql2025.xml"));
        var victim = m.Processes.Single(p => p.Spid == 103);

        Assert.False(string.IsNullOrWhiteSpace(victim.SqlText));   // the differentiator
        Assert.Equal("hammerdb_tpcc", victim.DatabaseName);        // currentdbname, NOT the numeric "7"
        Assert.Equal("hammerdb_tpcc.dbo.neword", victim.ProcName); // first real exec-stack frame
        Assert.Equal("sa", victim.LoginName);
        Assert.Equal("X", victim.LockMode);
        Assert.True(victim.WaitTimeMs > 0);
    }

    // ── real: 5-process (the common case = a bundle of cycles) ──

    [Fact]
    public void Real5Proc_IsThreeCyclePlusTwoCycle_WithOneVictimEach()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_5proc_real_sql2025.xml"));

        Assert.Equal(new[] { 80, 88, 97, 103, 108 }, Spids(m));
        Assert.Equal(new[] { 88, 108 }, VictimSpids(m));   // multi-victim: one per cycle
        Assert.False(m.IsParallel);

        // 3-cycle {80->103->108->80} + 2-cycle {88->97->88}.
        Assert.Equal(new HashSet<(int, int)>
        {
            (80, 103), (103, 108), (108, 80),
            (88, 97), (97, 88)
        }, SpidEdges(m));
    }

    // ── real: 8-process, multi-victim ──

    [Fact]
    public void Real8Proc_IsFourTwoCycles_WithFourVictims()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_8proc_multivictim_real_sql2025.xml"));

        Assert.Equal(new[] { 105, 107, 114, 122, 123, 125, 135, 139 }, Spids(m));
        Assert.Equal(new[] { 107, 114, 122, 135 }, VictimSpids(m));   // four victims, one per 2-cycle
        Assert.False(m.IsParallel);

        Assert.Equal(new HashSet<(int, int)>
        {
            (105, 122), (122, 105),   // cycle 1
            (107, 123), (123, 107),   // cycle 2
            (114, 125), (125, 114),   // cycle 3
            (135, 139), (139, 135),   // cycle 4
        }, SpidEdges(m));
        Assert.Equal(8, m.Edges.Count);
    }

    // ── parallel: exchangeEvent self-edge ──

    [Fact]
    public void ParallelDeadlock_IsFlagged_AndKeepsSelfEdge()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_parallel_selfedge_synthetic.xml"));

        Assert.True(m.IsParallel);
        Assert.Contains(m.Edges, e => e.IsSelfEdge);                 // owner == waiter
        Assert.Contains(m.Edges, e => e.ResourceKind == "exchangeEvent");
        Assert.All(m.Edges.Where(e => e.ResourceKind == "exchangeEvent"),
            e => Assert.Equal("Parallelism", e.ResourceLabel));      // no objectname -> friendly fallback

        // Two ECIDs of the same spid stay distinct nodes (keyed by process id, not spid).
        Assert.Equal(2, m.Processes.Count);
        Assert.All(m.Processes, p => Assert.Equal(55, p.Spid));
        Assert.Equal(new[] { 1, 2 }, m.Processes.Select(p => p.Ecid).OrderBy(e => e).ToArray());
    }

    // ── cloud: Azure database_xml_deadlock_report wrapper + nameless resource ──

    [Fact]
    public void AzureWrappedReport_IsParsed_WrapperIsTransparent()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_azure_database_report_synthetic.xml"));

        // The <event name="database_xml_deadlock_report"> wrapper is stripped transparently.
        Assert.Equal(2, m.Processes.Count);
        Assert.Equal(new[] { 121 }, VictimSpids(m));
        Assert.Equal("salesdb", m.Processes.First().DatabaseName);
        Assert.NotNull(m.EventTime);    // timestamp read off the event wrapper
    }

    [Fact]
    public void NamelessResource_FallsBackToKindPlusId()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_azure_database_report_synthetic.xml"));

        // keylocks carry no objectname (Azure) -> "KEY hobt <hobtid>".
        Assert.All(m.Edges, e => Assert.StartsWith("KEY hobt ", e.ResourceLabel));
    }

    // ── cross-product: one resource, many owners/waiters ──

    [Fact]
    public void CrossProductResource_EmitsOwnerByWaiterEdges()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_crossproduct_synthetic.xml"));

        // One objectlock with 2 owners (201,202) and 2 waiters (203,204) => 2x2 = 4 edges.
        Assert.Equal(4, m.Edges.Count);
        Assert.Equal(new HashSet<(int, int)>
        {
            (203, 201), (203, 202), (204, 201), (204, 202)
        }, SpidEdges(m));
        Assert.All(m.Edges, e => Assert.Equal("OBJECT StackOverflow.dbo.Votes", e.ResourceLabel));
        Assert.Equal(new[] { 203, 204 }, VictimSpids(m));
    }

    // ── single true N-cycle (synthetic; this workload only bundles small cycles) ──

    [Fact]
    public void SingleFiveCycle_FormsOneRing()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_single5cycle_synthetic.xml"));

        Assert.Equal(new[] { 301, 302, 303, 304, 305 }, Spids(m));
        Assert.Equal(new[] { 305 }, VictimSpids(m));
        Assert.Equal(new HashSet<(int, int)>
        {
            (301, 302), (302, 303), (303, 304), (304, 305), (305, 301)
        }, SpidEdges(m));
    }

    // ── contended object (resolved straight off the deadlock resource-list) ──

    [Fact]
    public void ContentiousObject_MatchesEachNodesOutgoingEdgeLabel()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_2proc_real_sql2025.xml"));

        // A process's contended object is the resolved label of the resource it waits on (its outgoing edge).
        foreach (var node in m.Processes)
        {
            var waitsOn = m.Edges.First(e => e.WaiterProcessId == node.Id && !e.IsSelfEdge);
            Assert.Equal(waitsOn.ResourceLabel, node.ContentiousObject);
            Assert.False(string.IsNullOrEmpty(node.ContentiousObject));
        }

        // Concretely: the victim contends for the page it was blocked on — no DMV lookup, no raw "7:1:..." id.
        var bySpid = m.Processes.ToDictionary(p => p.Spid);
        Assert.Equal("PAGE hammerdb_tpcc.dbo.new_order", bySpid[103].ContentiousObject);
    }

    [Fact]
    public void ContentiousObject_CrossProduct_IsTheSharedObject()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_crossproduct_synthetic.xml"));
        var bySpid = m.Processes.ToDictionary(p => p.Spid);

        // Both waiters on the single objectlock surface the same resolved object.
        Assert.Equal("OBJECT StackOverflow.dbo.Votes", bySpid[203].ContentiousObject);
        Assert.Equal("OBJECT StackOverflow.dbo.Votes", bySpid[204].ContentiousObject);
    }

    [Fact]
    public void ContentiousObject_Empty_WhenProcessWaitsOnlyOnAParallelSelfEdge()
    {
        var m = DeadlockGraphParser.Parse(LoadFixture("deadlock_parallel_selfedge_synthetic.xml"));

        // Parallel threads wait on the exchange itself (a self-edge) — there is no contended OBJECT to show.
        Assert.All(m.Processes, p => Assert.Equal(string.Empty, p.ContentiousObject));
    }

    // ── robustness ──

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("<deadlock>")]                          // truncated / not well-formed
    [InlineData("<notadeadlock><foo/></notadeadlock>")] // well-formed but no deadlock element
    [InlineData("complete garbage not xml at all")]
    public void MalformedOrEmptyInput_ReturnsEmptyModel(string? xml)
    {
        var m = DeadlockGraphParser.Parse(xml);
        Assert.True(m.IsEmpty);
        Assert.Empty(m.Processes);
        Assert.Empty(m.Edges);
    }

    [Fact]
    public void EventWrappedOnPremXml_IsAlsoAccepted()
    {
        // The raw collect.deadlock_xml form is the full <event name="xml_deadlock_report"> wrapper; the
        // parser must handle it as well as the bare <deadlock> the apps store. Build one inline.
        var bare = LoadFixture("deadlock_2proc_real_sql2025.xml");
        var wrapped =
            "<event name=\"xml_deadlock_report\" package=\"sqlserver\" timestamp=\"2026-06-21T13:51:02.662Z\">" +
            "<data name=\"xml_report\"><type name=\"xml\" package=\"package0\"/><value>" + bare +
            "</value></data></event>";

        var m = DeadlockGraphParser.Parse(wrapped);
        Assert.Equal(new[] { 85, 103 }, Spids(m));
        Assert.Equal(new[] { 103 }, VictimSpids(m));
        Assert.NotNull(m.EventTime);
    }
}
