/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6205: the Properties panel beside the deadlock graph shows each process's execution-stack sql_handles, so the
/// plan can be found in the cache. The shared parse turns the graph XML into those frames (stack order, the
/// all-zero dynamic-SQL handle dropped); the desktop panel text and the web payload both read them from there.
/// </summary>
public sealed class DeadlockSqlHandleTests
{
    private const string H1 = "0x0300050075E8F72EBD8D9E0099AF000001000000000000000000000000000000000000000000000000000000";
    private const string H2 = "0x03000500AAAAAAAAAAAAAAAAAAAAAA0001000000000000000000000000000000000000000000000000000000";
    private static readonly string Zero = "0x" + new string('0', 84);

    private static string Graph(string stack1, string stack2 = "") => $"""
        <deadlock>
          <victim-list><victimProcess id="process1" /></victim-list>
          <process-list>
            <process id="process1" taskpriority="0" logused="0" waitresource="KEY: 5:1" waittime="10" status="suspended" spid="51" ecid="0" priority="0" currentdbname="AppDb" lockMode="X">
              <executionStack>{stack1}</executionStack>
              <inputbuf>UPDATE dbo.t SET a = 1</inputbuf>
            </process>
            <process id="process2" taskpriority="0" logused="0" waitresource="KEY: 5:2" waittime="10" status="suspended" spid="52" ecid="0" priority="0" currentdbname="AppDb" lockMode="U">
              <executionStack>{stack2}</executionStack>
              <inputbuf>UPDATE dbo.t SET a = 2</inputbuf>
            </process>
          </process-list>
          <resource-list>
            <keylock hobtid="1" dbid="5" objectname="AppDb.dbo.t" indexname="pk" id="lock1" mode="X" associatedObjectId="1">
              <owner-list><owner id="process2" mode="X" /></owner-list>
              <waiter-list><waiter id="process1" mode="U" requestType="wait" /></waiter-list>
            </keylock>
            <keylock hobtid="1" dbid="5" objectname="AppDb.dbo.t" indexname="pk" id="lock2" mode="X" associatedObjectId="1">
              <owner-list><owner id="process1" mode="X" /></owner-list>
              <waiter-list><waiter id="process2" mode="U" requestType="wait" /></waiter-list>
            </keylock>
          </resource-list>
        </deadlock>
        """;

    private static DeadlockProcessNode Node(DeadlockGraphModel m, string id) => m.Processes.Single(p => p.Id == id);

    [Fact]
    public void Frames_AreKeptInStackOrder_WithTheirStatementRange()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame procname="adhoc" line="1" stmtstart="20" stmtend="140" sqlhandle="{H1}">UPDATE</frame><frame procname="dbo.Outer" line="9" stmtstart="300" stmtend="-1" sqlhandle="{H2}">EXEC</frame>"""));

        var frames = Node(m, "process1").StackFrames;

        Assert.Equal(new[] { new DeadlockStackFrame(H1, 20, 140), new DeadlockStackFrame(H2, 300, -1) }, frames);
    }

    [Fact]
    public void TheAllZeroHandle_AndFramesWithoutAHandle_AreSkipped()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame procname="adhoc" sqlhandle="{Zero}">x</frame><frame procname="adhoc" sqlhandle="0x00">x</frame><frame procname="adhoc">no handle</frame><frame sqlhandle="">blank</frame><frame procname="p" stmtstart="4" sqlhandle="{H1.ToLowerInvariant()}">y</frame>"""));

        var frames = Node(m, "process1").StackFrames;

        var only = Assert.Single(frames);
        Assert.Equal(H1.ToLowerInvariant(), only.SqlHandle);
        Assert.Equal(4, only.StmtStart);
        Assert.Equal(-1, only.StmtEnd);   // no stmtend in the frame: to the end of the batch
    }

    [Fact]
    public void AFrameThatRepeatsAnEarlierOne_IsListedOnce()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame sqlhandle="{H1}" stmtstart="2" stmtend="9">a</frame><frame sqlhandle="{H1}" stmtstart="2" stmtend="9">a</frame><frame sqlhandle="{H1}" stmtstart="30" stmtend="90">b</frame>"""));

        Assert.Equal(2, Node(m, "process1").StackFrames.Count);
    }

    [Fact]
    public void AProcessWithNoExecutionStackOrNoUsableHandle_HasNoFrames()
    {
        var noHandles = DeadlockGraphParser.Parse(Graph($"""<frame sqlhandle="{Zero}">x</frame>""", ""));
        Assert.Empty(Node(noHandles, "process1").StackFrames);
        Assert.Empty(Node(noHandles, "process2").StackFrames);

        const string noStack = """
            <deadlock><victim-list><victimProcess id="process1" /></victim-list>
            <process-list><process id="process1" spid="51" ecid="0" /><process id="process2" spid="52" ecid="0" /></process-list>
            <resource-list></resource-list></deadlock>
            """;
        var m = DeadlockGraphParser.Parse(noStack);
        Assert.All(m.Processes, p => Assert.Empty(p.StackFrames));
    }

    [Fact]
    public void ThePanelText_IsOneHandlePerLineInStackOrder_AndNullWhenThereAreNone()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame sqlhandle="{H1}" stmtstart="2" stmtend="9">a</frame><frame sqlhandle="{H1}" stmtstart="30" stmtend="90">b</frame><frame sqlhandle="{H2}">c</frame>""",
            $"""<frame sqlhandle="{Zero}">z</frame>"""));

        // The same handle at two statement ranges is one line: the text is for finding the plan, not the statement.
        Assert.Equal(H1 + Environment.NewLine + H2, DeadlockGraphControl.SqlHandlesToCopy(Node(m, "process1")));
        Assert.Null(DeadlockGraphControl.SqlHandlesToCopy(Node(m, "process2")));
        Assert.Null(DeadlockGraphControl.SqlHandlesToCopy(null));
    }

    [Fact]
    public void TheWebPayload_CarriesTheHandlesInStackOrder_AndOmitsThemWhenThereAreNone()
    {
        var graph = DarlingWebDeadlockGraph.BuildGraph(Graph(
            $"""<frame sqlhandle="{H1}" stmtstart="2" stmtend="9">a</frame><frame sqlhandle="{H2}">c</frame>""",
            $"""<frame sqlhandle="{Zero}">z</frame>"""));
        Assert.NotNull(graph);

        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(graph));
        var processes = doc.RootElement.GetProperty("processes").EnumerateArray().ToDictionary(p => p.GetProperty("id").GetString()!);

        Assert.Equal(new[] { H1, H2 }, processes["process1"].GetProperty("sql_handles").EnumerateArray().Select(h => h.GetString()).ToArray());
        Assert.False(processes["process2"].TryGetProperty("sql_handles", out _));
    }
}
