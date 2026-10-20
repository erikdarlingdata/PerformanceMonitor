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
    private const string H3 = "0x03000500BBBBBBBBBBBBBBBBBBBBBB0001000000000000000000000000000000000000000000000000000000";
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
    public void EveryNonZeroFrame_IsKeptInStackOrder_WithItsOwnStatementRangeProcedureAndLine()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame procname="adhoc" line="1" stmtstart="20" stmtend="140" sqlhandle="{H1}">UPDATE</frame><frame procname="dbo.Inner" line="9" stmtstart="300" stmtend="-1" sqlhandle="{H2}">EXEC</frame><frame procname="dbo.Outer" line="3" stmtstart="50" stmtend="90" sqlhandle="{H3}">EXEC</frame>"""));

        var frames = Node(m, "process1").StackFrames;

        Assert.Equal(new[]
        {
            new DeadlockStackFrame(1, H1, 20, 140, "adhoc", 1),
            new DeadlockStackFrame(2, H2, 300, -1, "dbo.Inner", 9),
            new DeadlockStackFrame(3, H3, 50, 90, "dbo.Outer", 3),
        }, frames);
    }

    [Fact]
    public void ZeroFramesAreSkipped_ButStillCountInThePosition()
    {
        // Frame 1 is dynamic SQL (zero handle), so the first listed frame is frame 2, a caller, not the running statement.
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame procname="adhoc" sqlhandle="{Zero}">x</frame><frame procname="dbo.P" line="4" stmtstart="8" stmtend="60" sqlhandle="{H1}">y</frame><frame procname="adhoc" sqlhandle="{Zero}">z</frame><frame procname="dbo.Q" sqlhandle="{H2}">w</frame>"""));

        var frames = Node(m, "process1").StackFrames;

        Assert.Equal(new[] { 2, 4 }, frames.Select(f => f.Position).ToArray());
        Assert.Equal(new[] { H1, H2 }, frames.Select(f => f.SqlHandle).ToArray());
        Assert.StartsWith("2 (caller)  " + H1, frames[0].Describe(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("0x00")]
    [InlineData("0x0000000000000000")]
    [InlineData("0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000")]
    [InlineData("0X0000")]
    [InlineData("0x")]
    public void AHandleOfOnlyZeroDigits_IsSkipped_WhateverItsLength(string zero)
    {
        var m = DeadlockGraphParser.Parse(Graph($"""<frame sqlhandle="{zero}">x</frame><frame sqlhandle="{H1}">y</frame>"""));

        var only = Assert.Single(Node(m, "process1").StackFrames);
        Assert.Equal(H1, only.SqlHandle);
        Assert.Equal(2, only.Position);
    }

    [Fact]
    public void AFrameWithoutAHandle_IsSkipped_AndAHandleWithOneNonZeroDigitIsKept()
    {
        const string almostZero = "0x0000000000000000000000000000000000000001";
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame procname="adhoc">no handle</frame><frame sqlhandle="">blank</frame><frame sqlhandle="{almostZero}">kept</frame>"""));

        var only = Assert.Single(Node(m, "process1").StackFrames);
        Assert.Equal(almostZero, only.SqlHandle);
        Assert.Equal(3, only.Position);
    }

    [Fact]
    public void AMissingStatementStartReadsAsZero_AndAMissingStatementEndAsMinusOne()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame sqlhandle="{H1}">a</frame><frame sqlhandle="{H2}" stmtstart="12">b</frame><frame sqlhandle="{H3}" stmtend="77">c</frame><frame sqlhandle="{H1}" stmtstart="x" stmtend="">d</frame>"""));

        var frames = Node(m, "process1").StackFrames;

        Assert.Equal(new[] { (0, -1), (12, -1), (0, 77), (0, -1) }, frames.Select(f => (f.StmtStart, f.StmtEnd)).ToArray());
        Assert.All(frames, f => { Assert.Equal("", f.ProcName); Assert.Null(f.Line); });
    }

    [Fact]
    public void ASecondFrameWithTheSameHandleAndAnotherRange_IsAFrameOfItsOwn()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame sqlhandle="{H1}" stmtstart="2" stmtend="9">a</frame><frame sqlhandle="{H1}" stmtstart="30" stmtend="90">b</frame>"""));

        Assert.Equal(new[] { (2, 9), (30, 90) }, Node(m, "process1").StackFrames.Select(f => (f.StmtStart, f.StmtEnd)).ToArray());
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
    public void ThePanelText_IsOneLinePerFrameInStackOrder_AndNullWhenThereAreNone()
    {
        var m = DeadlockGraphParser.Parse(Graph(
            $"""<frame procname="dbo.P" line="7" stmtstart="2" stmtend="9" sqlhandle="{H1}">a</frame><frame stmtstart="30" sqlhandle="{H2}">c</frame>""",
            $"""<frame sqlhandle="{Zero}">z</frame>"""));

        var expected = $"1 (running)  {H1}  stmtstart 2, stmtend 9  dbo.P line 7"
            + Environment.NewLine + $"2 (caller)  {H2}  stmtstart 30, stmtend -1";
        Assert.Equal(expected, DeadlockGraphControl.SqlHandlesToCopy(Node(m, "process1")));
        Assert.Null(DeadlockGraphControl.SqlHandlesToCopy(Node(m, "process2")));
        Assert.Null(DeadlockGraphControl.SqlHandlesToCopy(null));
    }

    [Fact]
    public void TheWebPayload_CarriesTheFramesInStackOrder_AndOmitsThemWhenThereAreNone()
    {
        var graph = DarlingWebDeadlockGraph.BuildGraph(Graph(
            $"""<frame procname="dbo.P" line="7" stmtstart="2" stmtend="9" sqlhandle="{H1}">a</frame><frame sqlhandle="{H2}">c</frame>""",
            $"""<frame sqlhandle="{Zero}">z</frame>"""));
        Assert.NotNull(graph);

        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(graph));
        var processes = doc.RootElement.GetProperty("processes").EnumerateArray().ToDictionary(p => p.GetProperty("id").GetString()!);

        var frames = processes["process1"].GetProperty("sql_frames").EnumerateArray().ToArray();
        Assert.Equal(new[] { H1, H2 }, frames.Select(f => f.GetProperty("sql_handle").GetString()).ToArray());
        Assert.Equal(new[] { 1, 2 }, frames.Select(f => f.GetProperty("position").GetInt32()).ToArray());
        Assert.Equal(new[] { (2, 9), (0, -1) }, frames.Select(f => (f.GetProperty("stmt_start").GetInt32(), f.GetProperty("stmt_end").GetInt32())).ToArray());
        Assert.Equal("dbo.P", frames[0].GetProperty("proc_name").GetString());
        Assert.Equal(7, frames[0].GetProperty("line").GetInt32());
        Assert.False(frames[1].TryGetProperty("proc_name", out _));
        Assert.False(frames[1].TryGetProperty("line", out _));
        Assert.False(processes["process2"].TryGetProperty("sql_frames", out _));
    }
}
