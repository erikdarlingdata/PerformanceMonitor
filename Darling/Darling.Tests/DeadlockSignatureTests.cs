/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>#6005, D12: the signature is a pure function of the graph, blind to line numbers and offsets.</summary>
public sealed class DeadlockSignatureTests
{
    internal static string Graph(string proc = "dbo.usp_pay", int line = 12, string mode = "X", string table = "db.dbo.Orders", string handle = "0x01") => $@"
<deadlock><victim-list><victimProcess id=""p1""/></victim-list>
<process-list>
  <process id=""p1"" currentdbname=""db"" spid=""55""><executionStack><frame procname=""{proc}"" line=""{line}"" stmtstart=""{line * 10}"" sqlhandle=""{handle}"">x</frame></executionStack></process>
  <process id=""p2"" currentdbname=""db"" spid=""61""><executionStack><frame procname=""dbo.usp_ship"" line=""{line + 3}"" sqlhandle=""{handle}"">y</frame></executionStack></process>
</process-list>
<resource-list>
  <keylock objectname=""{table}"" mode=""{mode}""><owner-list><owner id=""p1"" mode=""{mode}""/></owner-list><waiter-list><waiter id=""p2"" mode=""S""/></waiter-list></keylock>
</resource-list></deadlock>";

    [Fact]
    public void AProcedureEdit_ThatMovesLineNumbers_KeepsTheSignature()
    {
        var before = DeadlockSignature.FromGraphXml(Graph(line: 12, handle: "0x01"));
        var after = DeadlockSignature.FromGraphXml(Graph(line: 148, handle: "0xFF"));

        Assert.NotNull(before);
        Assert.Equal(before, after);
        Assert.NotEqual(before, DeadlockSignature.FromGraphXml(Graph(proc: "dbo.usp_refund")));
        Assert.NotEqual(before, DeadlockSignature.FromGraphXml(Graph(mode: "U")));
        Assert.NotEqual(before, DeadlockSignature.FromGraphXml(Graph(table: "db.dbo.Invoices")));
    }

    [Fact]
    public void TheAgentGate_OpensForANewSignature_OrACountOverThreeTimesTheDailyAverage_AndNoOther()
    {
        Assert.True(DeadlockAgentGate.Opens(isNew: true, count24Hours: 1, dailyAverage: 0));
        Assert.False(DeadlockAgentGate.Opens(isNew: false, count24Hours: 3, dailyAverage: 1));
        Assert.True(DeadlockAgentGate.Opens(isNew: false, count24Hours: 3.5, dailyAverage: 1));
        Assert.False(DeadlockAgentGate.Opens(isNew: false, count24Hours: 50, dailyAverage: 0));
    }
}
