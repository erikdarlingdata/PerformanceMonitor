/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Controls;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6205: Lite's plan lookups (View Plan on a deadlock process or a blocked-process side) read the sql_handle,
/// stmtstart and stmtend a graph or report carries. A handle whose hex digits are all zeros, of any length, is the
/// "no persistent handle" sentinel and is skipped, the same test the deadlock pane uses.
/// </summary>
public sealed class PlanLookupZeroHandleTests
{
    public static TheoryData<string> ZeroHandles => new()
    {
        "0x00",
        "0x0000000000000000",
        "0X0000",
        "0x" + new string('0', 84),
        "0x" + new string('0', 120),
    };

    [Theory]
    [MemberData(nameof(ZeroHandles))]
    public void DeadlockProcessFrames_SkipAZeroHandle_OfAnyLength_OnTheProcessAndOnAFrame(string zero)
    {
        var graph = $@"<deadlock><process-list>
    <process id='p1' sqlhandle='{zero}' stmtstart='5' stmtend='9'>
      <executionStack><frame sqlhandle='{zero}'/><frame sqlhandle='0x0200EEEE' stmtstart='3' stmtend='8'/></executionStack>
    </process>
    <process id='p2' sqlhandle='{zero}'><executionStack><frame sqlhandle='{zero}'/></executionStack></process>
  </process-list></deadlock>";

        var kept = Assert.Single(ServerTab.ExtractDeadlockProcessFrames(graph, "p1"));
        Assert.Equal(("0x0200EEEE", 3, 8), kept);
        Assert.Empty(ServerTab.ExtractDeadlockProcessFrames(graph, "p2"));
    }

    [Fact]
    public void DeadlockProcessFrames_KeepAProcessLevelHandle_BeforeTheFrames()
    {
        var graph = @"<deadlock><process-list>
    <process id='p1' sqlhandle='0x0200AAAA' stmtstart='40' stmtend='120'>
      <executionStack><frame sqlhandle='0x0200BBBB'/></executionStack>
    </process>
  </process-list></deadlock>";

        var frames = ServerTab.ExtractDeadlockProcessFrames(graph, "p1");

        Assert.Equal(new[] { ("0x0200AAAA", 40, 120), ("0x0200BBBB", 0, -1) }, frames);
    }

    [Theory]
    [MemberData(nameof(ZeroHandles))]
    public void BlockedProcessFrames_SkipAZeroHandle_OfAnyLength_OnBothSides(string zero)
    {
        var report = $@"<blocked-process-report>
  <blocked-process><process id='a'><executionStack><frame sqlhandle='{zero}'/><frame sqlhandle='0x0200CCCC' stmtstart='1' stmtend='2'/></executionStack></process></blocked-process>
  <blocking-process><process id='b'><executionStack><frame sqlhandle='{zero}'/></executionStack></process></blocking-process>
</blocked-process-report>";

        var kept = Assert.Single(ServerTab.ExtractBlockedProcessFrames(report, blockingSide: false));
        Assert.Equal(("0x0200CCCC", 1, 2), kept);
        Assert.Empty(ServerTab.ExtractBlockedProcessFrames(report, blockingSide: true));
    }
}
