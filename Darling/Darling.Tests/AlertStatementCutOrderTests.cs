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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320: an alert's statement text is judged WHOLE and then cut, never cut and then judged. The shared alert
/// builders (<see cref="AlertContextBuilders"/>, <see cref="DeadlockAlertRow"/>, both apps) cut a statement to a
/// one-line preview for the alert body. A judge that sees the cut preview misses a value whose naming text (here a
/// <c>CREATE LOGIN ... PASSWORD</c> tail) lies past the cut, so every statement here carries a value in its first
/// part and its naming text well past 300 characters. Each pin fails when the cut comes first.
/// </summary>
public sealed class AlertStatementCutOrderTests
{
    private const string Value = "Hunter2Secret5320";

    /// <summary>A value in the first characters, 600 characters of column list, then the text that names it.</summary>
    private static readonly string Statement =
        $"SELECT '{Value}' AS v" + string.Concat(Enumerable.Range(0, 60).Select(i => $", c{i:D4} AS a{i:D4}"))
        + " INTO #t; CREATE LOGIN leaker WITH PASSWORD = 'x';";

    private static readonly List<string> NoExclusions = new();

    private static string Flat(AlertContext? context) =>
        AlertContextBuilders.ContextToDetailText(context) ?? "";

    private static void AssertWithheld(string text)
    {
        Assert.DoesNotContain(Value, text, StringComparison.Ordinal);
        Assert.Contains(SensitiveStatements.PlaceholderText, text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFixtureStatement_IsNamedWhole_AndItsFirstThreeHundredCharactersAreNot()
    {
        Assert.True(Statement.IndexOf("CREATE LOGIN", StringComparison.Ordinal) > 600);
        Assert.True(SensitiveStatements.Names(Statement));
        Assert.False(SensitiveStatements.Names(Statement[..300]));
        Assert.Contains(Value, Statement[..300], StringComparison.Ordinal);
    }

    [Fact]
    public void TruncateStatement_JudgesTheWholeStatement_BeforeTheCut()
    {
        Assert.Equal(SensitiveStatements.PlaceholderText, AlertContextBuilders.TruncateStatement(Statement));
        Assert.Equal(SensitiveStatements.PlaceholderText, AlertContextBuilders.TruncateStatement(Statement, 80));
    }

    [Fact]
    public void TruncateStatement_CutsAClearStatementTheWayTruncateTextDoes()
    {
        var clear = "SELECT a,\r\n b FROM t " + new string('x', 400);

        Assert.Equal(AlertContextBuilders.TruncateText(clear), AlertContextBuilders.TruncateStatement(clear));
        Assert.Equal(AlertContextBuilders.TruncateText(clear, 40), AlertContextBuilders.TruncateStatement(clear, 40));
        Assert.Equal("", AlertContextBuilders.TruncateStatement(""));
    }

    [Fact]
    public void BlockingContext_ShowsTheMarker_ForABlockedAndABlockingStatementWhoseNamingTextIsPastTheCut()
    {
        var rows = new List<BlockedProcessAlertRow>
        {
            new()
            {
                DatabaseName = "AppDb", WaitTimeMs = 9000, ContentiousObject = "AppDb.dbo.Ledger",
                BlockedSqlText = Statement, BlockingSqlText = Statement,
            },
        };

        var context = AlertContextBuilders.BuildBlockingContext("SQL2022", rows, NoExclusions);

        AssertWithheld(Flat(context));
    }

    [Fact]
    public void BlockingIncident_CarriesTheMarkerInItsDetailFields()
    {
        var rows = new List<BlockedProcessAlertRow>
        {
            new()
            {
                DatabaseName = "AppDb", WaitTimeMs = 9000, ContentiousObject = "AppDb.dbo.Ledger",
                BlockedSqlText = Statement, BlockingSqlText = Statement,
            },
        };

        var incident = Assert.Single(AlertContextBuilders.BlockingIncidents("SQL2022", rows, NoExclusions));
        var fields = Assert.IsAssignableFrom<IReadOnlyList<AlertIncidentField>>(incident.DetailFields);
        var query = fields.Where(f => f.Label.EndsWith("Query", StringComparison.Ordinal)).ToList();

        Assert.Equal(2, query.Count);
        Assert.All(query, f => Assert.Equal(SensitiveStatements.PlaceholderText, f.Value));
    }

    [Fact]
    public void BlockingIncident_KeepsItsIdentityOnTheRawText_SoAWithheldStatementDoesNotChangeTheKey()
    {
        /* No contentious object: identity falls back to database + the literal-stripped statement pair. */
        var rows = new List<BlockedProcessAlertRow>
        {
            new() { DatabaseName = "AppDb", WaitTimeMs = 9000, BlockedSqlText = Statement, BlockingSqlText = Statement + " " },
            new() { DatabaseName = "AppDb", WaitTimeMs = 8000, BlockedSqlText = Statement, BlockingSqlText = Statement + " " },
        };

        var incidents = AlertContextBuilders.BlockingIncidents("SQL2022", rows, NoExclusions);

        Assert.Single(incidents);
    }

    private const string StandaloneVictimGraph = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list>"
        + "<process-list><process id=\"process1\" spid=\"51\" currentdbname=\"AppDb\"><inputbuf>x</inputbuf></process></process-list>"
        + "<resource-list></resource-list></deadlock>";

    private static string FingerprintedGraph(string inputbuf) =>
        "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list>"
        + $"<process id=\"process1\" spid=\"51\" currentdbname=\"AppDb\" lockMode=\"X\"><inputbuf>{System.Security.SecurityElement.Escape(inputbuf)}</inputbuf></process>"
        + "<process id=\"process2\" spid=\"52\" currentdbname=\"AppDb\" lockMode=\"U\"><inputbuf>SELECT 2</inputbuf></process>"
        + "</process-list><resource-list><keylock objectname=\"AppDb.dbo.Ledger\" indexname=\"IX\">"
        + "<owner id=\"process2\" mode=\"U\"/><waiter id=\"process1\" mode=\"X\"/></keylock></resource-list></deadlock>";

    [Fact]
    public void DeadlockContext_ShowsTheMarker_ForAVictimStatementWithNoParseableObjects()
    {
        var rows = new List<DeadlockAlertRow>
        {
            new() { VictimProcessId = "process1", VictimSqlText = Statement, DeadlockGraphXml = StandaloneVictimGraph },
        };

        var context = AlertContextBuilders.BuildDeadlockContext("SQL2022", rows, NoExclusions);

        AssertWithheld(Flat(context));
    }

    [Fact]
    public void DeadlockContext_ShowsTheMarker_ForTheFingerprintedVictimAndEachPartyStatement()
    {
        var rows = new List<DeadlockAlertRow>
        {
            new() { VictimProcessId = "process1", VictimSqlText = Statement, DeadlockGraphXml = FingerprintedGraph(Statement) },
        };

        var context = AlertContextBuilders.BuildDeadlockContext("SQL2022", rows, NoExclusions);
        var text = Flat(context);

        AssertWithheld(text);
        Assert.Contains("SELECT 2", text, StringComparison.Ordinal);
    }

    [Fact]
    public void LongRunningQueryContext_ShowsTheMarker_ForAStatementWhoseNamingTextIsPastTheCut()
    {
        var queries = new List<LongRunningQueryInfo>
        {
            new() { SessionId = 61, DatabaseName = "AppDb", QueryText = Statement, ElapsedSeconds = 900 },
        };

        var context = AlertContextBuilders.BuildLongRunningQueryContext("SQL2022", queries);

        AssertWithheld(Flat(context));
    }

    /// <summary>
    /// The toast preview of the long-running-query alert is cut to 80 characters inside the engine's sweep, which
    /// no pure seam reaches; this holds it to the judge-then-cut helper by its source.
    /// </summary>
    [Fact]
    public void TheEnginesLongRunningQueryPreview_UsesTheJudgeThenCutHelper()
    {
        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Alerting", "AlertEngine.cs");

        Assert.Contains("AlertContextBuilders.TruncateStatement(worst.QueryText, 80)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AlertContextBuilders.TruncateText(", source, StringComparison.Ordinal);
    }

    /* The two PostgreSQL alert texts cut a statement the store already holds judged: a deadlock victim the log
       parser took out of a report it had redacted whole, and a blocking root query PgBlockingCollector judges on the
       target (PgSensitiveStatementFilterTests). These two pins read the chain through the parser. */
    [Fact]
    public void PgDeadlockAlert_NeverCarriesAValueOfAVictimWhoseNamingTextIsPastTheCut()
    {
        var at = new DateTime(2026, 10, 6, 12, 0, 0, 100);
        var prefix = at.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture) + " UTC [3301] ";
        var report = prefix + "ERROR:  deadlock detected\n"
            + prefix + "DETAIL:  Process 3301 waits for ShareLock on transaction 5501; blocked by process 3302.\n"
            + "\tProcess 3302 waits for ShareLock on transaction 5500; blocked by process 3301.\n"
            + $"\tProcess 3301: SELECT '{Value}' AS v" + string.Concat(Enumerable.Range(0, 60).Select(i => $", c{i:D4}"))
            + " INTO t; ALTER ROLE leaker PASSWORD 'x'\n"
            + "\tProcess 3302: SELECT 2\n"
            + prefix + "HINT:  See server log for query details.\n";

        var parsed = Assert.Single(PgDeadlockLogParser.Extract(report));
        var incident = DarlingWorker.BuildPgDeadlockIncident(new DarlingPgDeadlockReader.PgDeadlockRow(
            parsed.OccurredAtUtc, parsed.VictimPid, parsed.ParticipantCount, parsed.DeadlockHash,
            parsed.LockModes, parsed.Resources, parsed.VictimStatement, 1));

        Assert.DoesNotContain(Value, string.Join("|", incident.InvolvedObjects), StringComparison.Ordinal);
        Assert.DoesNotContain(Value, parsed.GraphText, StringComparison.Ordinal);
    }
}
