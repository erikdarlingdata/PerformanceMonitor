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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5320, Lite's side: Lite builds its blocking, deadlock and long-running-query alert bodies through the same
/// shared <see cref="AlertContextBuilders"/> Darling does, so one pin per builder holds the order here too. A
/// statement judged WHOLE and then cut shows the withheld marker; a cut that came first would show the value in the
/// statement's first 300 characters, because the text that names it is past the cut.
/// </summary>
public sealed class AlertStatementCutOrderLiteTests
{
    private const string Value = "Hunter2Secret5320";

    private static readonly string Statement =
        $"SELECT '{Value}' AS v" + string.Concat(Enumerable.Range(0, 60).Select(i => $", c{i:D4} AS a{i:D4}"))
        + " INTO #t; CREATE LOGIN leaker WITH PASSWORD = 'x';";

    private static readonly List<string> NoExclusions = new();

    private static void AssertWithheld(AlertContext? context)
    {
        var text = AlertContextBuilders.ContextToDetailText(context) ?? "";
        Assert.DoesNotContain(Value, text, StringComparison.Ordinal);
        Assert.Contains(SensitiveStatements.PlaceholderText, text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFixtureStatement_IsNamedWhole_AndItsFirstThreeHundredCharactersAreNot()
    {
        Assert.True(SensitiveStatements.Names(Statement));
        Assert.False(SensitiveStatements.Names(Statement[..300]));
        Assert.Contains(Value, Statement[..300], StringComparison.Ordinal);
    }

    [Fact]
    public void BlockingContext_ShowsTheMarker()
    {
        var rows = new List<BlockedProcessAlertRow>
        {
            new() { DatabaseName = "AppDb", WaitTimeMs = 9000, ContentiousObject = "AppDb.dbo.Ledger", BlockedSqlText = Statement, BlockingSqlText = Statement },
        };

        AssertWithheld(AlertContextBuilders.BuildBlockingContext("SQL2022", rows, NoExclusions));
    }

    [Fact]
    public void DeadlockContext_ShowsTheMarker_ForAVictimStatementAndAPartyStatement()
    {
        var graph = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list>"
            + $"<process id=\"process1\" spid=\"51\" currentdbname=\"AppDb\" lockMode=\"X\"><inputbuf>{Statement}</inputbuf></process>"
            + "<process id=\"process2\" spid=\"52\" currentdbname=\"AppDb\" lockMode=\"U\"><inputbuf>SELECT 2</inputbuf></process>"
            + "</process-list><resource-list><keylock objectname=\"AppDb.dbo.Ledger\" indexname=\"IX\">"
            + "<owner id=\"process2\" mode=\"U\"/><waiter id=\"process1\" mode=\"X\"/></keylock></resource-list></deadlock>";
        var rows = new List<DeadlockAlertRow>
        {
            new() { VictimProcessId = "process1", VictimSqlText = Statement, DeadlockGraphXml = graph },
        };

        AssertWithheld(AlertContextBuilders.BuildDeadlockContext("SQL2022", rows, NoExclusions));
    }

    [Fact]
    public void LongRunningQueryContext_ShowsTheMarker()
    {
        var queries = new List<LongRunningQueryInfo>
        {
            new() { SessionId = 61, DatabaseName = "AppDb", QueryText = Statement, ElapsedSeconds = 900 },
        };

        AssertWithheld(AlertContextBuilders.BuildLongRunningQueryContext("SQL2022", queries));
    }
}
