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
using PerformanceMonitor.Common;
using Xunit;
using static PerformanceMonitor.Common.DeadlockGraphProcessParser;

namespace Lite.Tests;

/// <summary>
/// Behavioral pin for the shared <see cref="DeadlockGraphProcessParser"/> — the sp_BlitzLock-style per-process
/// deadlock-graph walk that used to be duplicated byte-for-byte as <c>DeadlockProcessDetail.ParseFromRows</c>
/// in Lite (<c>LocalDataService.Blocking.cs</c>) and the Darling viewer (<c>ViewerDataService.Deadlock.cs</c>).
/// Drives the parser directly over the WPF-free base <see cref="DeadlockProcessInfo"/>, proving the collapsed
/// walk still resolves victims, per-process owner/waiter modes, contended objects, proc names, the
/// regular-vs-parallel classification, the victim-plan threading, and the malformed-XML fallback. Pure — no
/// database, no WPF.
/// </summary>
public class DeadlockGraphProcessParserTests
{
    /// <summary>A known 2-process graph: process1 (spid 55) is the victim/waiter, process2 (spid 60) the owner.</summary>
    private const string TwoProcessDeadlockXml = """
        <deadlock>
          <victim-list>
            <victimProcess id="process1" />
          </victim-list>
          <process-list>
            <process id="process1" spid="55" currentdbname="AppDb" waitresource="KEY: 5:72" waittime="1500" lockMode="X" isolationlevel="read committed (2)" logused="200" trancount="1" clientapp="TestApp" hostname="HOST1" loginname="sa" status="suspended" transactionname="user_transaction" priority="0" lasttranstarted="2026-07-01T10:00:00.000">
              <executionStack>
                <frame procname="AppDb.dbo.usp_Update" line="1">UPDATE dbo.t SET c = 1</frame>
              </executionStack>
              <inputbuf>UPDATE dbo.t SET c = 1</inputbuf>
            </process>
            <process id="process2" spid="60" currentdbname="AppDb" waitresource="KEY: 5:73" waittime="1200" lockMode="X" isolationlevel="read committed (2)" logused="300" trancount="1" clientapp="TestApp2" hostname="HOST2" loginname="sa" status="suspended" transactionname="user_transaction" priority="0">
              <inputbuf>UPDATE dbo.t SET c = 2</inputbuf>
            </process>
          </process-list>
          <resource-list>
            <keylock hobtid="72" dbid="5" objectname="AppDb.dbo.t" indexname="pk_t" mode="X">
              <owner-list>
                <owner id="process2" mode="X" />
              </owner-list>
              <waiter-list>
                <waiter id="process1" mode="X" requestType="wait" />
              </waiter-list>
            </keylock>
          </resource-list>
        </deadlock>
        """;

    /// <summary>A minimal parallel graph: the resource-list carries an exchangeEvent, so it must classify Parallel.</summary>
    private const string ParallelDeadlockXml = """
        <deadlock>
          <victim-list>
            <victimProcess id="p1" />
          </victim-list>
          <process-list>
            <process id="p1" spid="70" currentdbname="Db2" waittime="500" />
            <process id="p2" spid="70" currentdbname="Db2" waittime="500" />
          </process-list>
          <resource-list>
            <exchangeEvent id="Pipe1" WaitType="e_waitPipeGetRow">
              <owner-list><owner id="p2" /></owner-list>
              <waiter-list><waiter id="p1" /></waiter-list>
            </exchangeEvent>
          </resource-list>
        </deadlock>
        """;

    private static readonly DateTime DeadlockTime = new(2026, 7, 1, 10, 0, 5, DateTimeKind.Unspecified);

    [Fact]
    public void Parse_TwoProcessGraph_ResolvesPerProcessFields()
    {
        var input = new[]
        {
            new DeadlockGraphInput(TwoProcessDeadlockXml, DeadlockTime, VictimSqlText: null, VictimQueryPlanXml: null),
        };

        var details = DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(input);

        Assert.Equal(2, details.Count);
        Assert.All(details, d => Assert.Equal("Regular", d.DeadlockType));
        Assert.All(details, d => Assert.True(d.HasDeadlockXml));
        Assert.All(details, d => Assert.Equal(DeadlockTime, d.DeadlockTime));

        var victim = details.Single(d => d.Spid == 55);
        Assert.True(victim.IsVictim);
        Assert.Equal("process1", victim.ProcessId);
        Assert.Equal("AppDb", victim.DatabaseName);
        Assert.Equal("UPDATE dbo.t SET c = 1", victim.SqlText);
        Assert.Equal("KEY: 5:72", victim.WaitResource);
        Assert.Equal(1500, victim.WaitTime);
        Assert.Equal("X", victim.LockMode);
        Assert.Equal("read committed (2)", victim.IsolationLevel);
        Assert.Equal(200, victim.LogUsed);
        Assert.Equal(1, victim.TransactionCount);
        Assert.Equal("TestApp", victim.ClientApp);
        Assert.Equal("HOST1", victim.HostName);
        Assert.Equal("sa", victim.LoginName);
        Assert.Equal("suspended", victim.Status);
        Assert.Equal("user_transaction", victim.TransactionName);
        Assert.Equal("AppDb.dbo.usp_Update", victim.ProcName);
        Assert.Equal("X", victim.WaiterMode);              // process1 waits on the keylock
        Assert.Equal("AppDb.dbo.t", victim.ObjectNames);
        Assert.Equal(new DateTime(2026, 7, 1, 10, 0, 0), victim.LastTranStarted);

        var owner = details.Single(d => d.Spid == 60);
        Assert.False(owner.IsVictim);
        Assert.Equal("process2", owner.ProcessId);
        Assert.Equal("X", owner.OwnerMode);               // process2 owns the keylock
        Assert.Equal("AppDb.dbo.t", owner.ObjectNames);
        Assert.Equal("", owner.WaiterMode);
    }

    [Fact]
    public void Parse_ThreadsVictimQueryPlanOntoEveryProcessRow()
    {
        var input = new[]
        {
            new DeadlockGraphInput(TwoProcessDeadlockXml, DeadlockTime, VictimSqlText: null,
                VictimQueryPlanXml: "<ShowPlanXML>victim</ShowPlanXML>"),
        };

        var details = DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(input);

        Assert.Equal(2, details.Count);
        Assert.All(details, d => Assert.Equal("<ShowPlanXML>victim</ShowPlanXML>", d.VictimQueryPlanXml));
    }

    [Fact]
    public void Parse_NullVictimQueryPlan_LeavesItNull()
    {
        var input = new[]
        {
            new DeadlockGraphInput(TwoProcessDeadlockXml, DeadlockTime, VictimSqlText: null, VictimQueryPlanXml: null),
        };

        var details = DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(input);

        Assert.NotEmpty(details);
        Assert.All(details, d => Assert.Null(d.VictimQueryPlanXml));
    }

    [Fact]
    public void Parse_ExchangeEventGraph_ClassifiesParallel()
    {
        var input = new[]
        {
            new DeadlockGraphInput(ParallelDeadlockXml, DeadlockTime, VictimSqlText: null, VictimQueryPlanXml: null),
        };

        var details = DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(input);

        Assert.Equal(2, details.Count);
        Assert.All(details, d => Assert.Equal("Parallel", d.DeadlockType));
    }

    [Fact]
    public void Parse_MalformedXml_YieldsSingleVictimFallbackRow()
    {
        var input = new[]
        {
            new DeadlockGraphInput("<deadlock", DeadlockTime, VictimSqlText: "UPDATE t SET c = 9",
                VictimQueryPlanXml: "<ShowPlanXML>fallback</ShowPlanXML>"),
        };

        var details = DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(input);

        var only = Assert.Single(details);
        Assert.True(only.IsVictim);
        Assert.Equal("UPDATE t SET c = 9", only.SqlText);
        Assert.True(only.HasDeadlockXml);
        Assert.Equal(DeadlockTime, only.DeadlockTime);
        Assert.Equal("<ShowPlanXML>fallback</ShowPlanXML>", only.VictimQueryPlanXml);
    }

    [Fact]
    public void Parse_MalformedXml_NullVictimSql_FallsBackToEmptyString()
    {
        var input = new[]
        {
            new DeadlockGraphInput("<deadlock", DeadlockTime, VictimSqlText: null, VictimQueryPlanXml: null),
        };

        var only = Assert.Single(DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(input));
        Assert.Equal("", only.SqlText);   // null VictimSqlText coalesces to "" (the base's non-null default)
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Parse_EmptyGraphXml_IsSkipped(string? xml)
    {
        var input = new[]
        {
            new DeadlockGraphInput(xml, DeadlockTime, VictimSqlText: null, VictimQueryPlanXml: null),
        };

        Assert.Empty(DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(input));
    }

    /* ---------------- TryParseGraph: the same walk, one graph, malformed reported rather than substituted
       (#3442). Parse's fallback row is what a grid needs and what a text renderer cannot use, so the two
       outcomes have to stay distinguishable at the seam. ---------------- */

    [Fact]
    public void TryParseGraph_GoodXml_ReturnsTheSameProcessesParseDoes()
    {
        var row = new DeadlockGraphInput(TwoProcessDeadlockXml, DeadlockTime, VictimSqlText: null, VictimQueryPlanXml: null);

        Assert.True(DeadlockGraphProcessParser.TryParseGraph<DeadlockProcessInfo>(row, out var processes));
        Assert.Equal(
            DeadlockGraphProcessParser.Parse<DeadlockProcessInfo>(new[] { row }).Select(d => d.ProcessId).ToList(),
            processes.Select(d => d.ProcessId).ToList());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("<deadlock")]
    public void TryParseGraph_AbsentOrMalformed_IsFalseWithNoProcesses(string? xml)
    {
        var row = new DeadlockGraphInput(xml, DeadlockTime, VictimSqlText: "UPDATE t SET c = 9", VictimQueryPlanXml: null);

        Assert.False(DeadlockGraphProcessParser.TryParseGraph<DeadlockProcessInfo>(row, out var processes));

        /* Empty rather than null, and NOT the victim-only fallback row: a caller rendering text would
           otherwise publish the fallback's default spid as a session id. */
        Assert.NotNull(processes);
        Assert.Empty(processes);
    }
}
