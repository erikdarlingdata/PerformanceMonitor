/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4058 r3: <see cref="PgLogEntryProvenance"/>'s two signals in isolation, over hand-built
/// <see cref="PgLogEntry"/> values — no parser, no csvlog text, just the two fields each method reads.
/// </summary>
public sealed class PgLogEntryProvenanceTests
{
    private static PgLogEntry Entry(string? context = null, string? location = null) => new(
        TimestampText: "2026-09-24 00:00:00.000",
        ZoneText: "UTC",
        OccurredAtUtc: new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Utc),
        Pid: 1,
        PrefixRest: string.Empty,
        Severity: "ERROR",
        Message: "deadlock detected",
        Detail: "irrelevant to this check",
        Hint: null,
        Statement: null,
        Context: context,
        UserName: null,
        DatabaseName: null,
        SqlState: null,
        RawText: string.Empty,
        Location: location);

    /* ---- RaisedByPlpgsql -------------------------------------------------------------------------------- */

    [Fact]
    public void RaisedByPlpgsql_TrueWhenTheFirstContextLineEndsAtRaise()
    {
        Assert.True(PgLogEntryProvenance.RaisedByPlpgsql(
            Entry(context: "PL/pgSQL function forge_deadlock() line 3 at RAISE")));
    }

    [Fact]
    public void RaisedByPlpgsql_FalseWhenAtRaiseIsOnlyOnALaterLine()
    {
        var context = "SQL statement \"select forge_deadlock()\""
            + "\nPL/pgSQL function outer_caller() line 1 at RAISE";

        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: context)));
    }

    [Fact]
    public void RaisedByPlpgsql_TrueWhenTheFirstLineIsCrlfTerminated()
    {
        Assert.True(PgLogEntryProvenance.RaisedByPlpgsql(
            Entry(context: "PL/pgSQL function forge_deadlock() line 3 at RAISE\r\nSQL statement \"x\"")));
    }

    [Fact]
    public void RaisedByPlpgsql_FalseWhenContextIsNull()
    {
        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: null)));
    }

    [Fact]
    public void RaisedByPlpgsql_FalseWhenContextIsEmpty()
    {
        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: string.Empty)));
    }

    /* ---- ReportedByOther --------------------------------------------------------------------------------- */

    [Fact]
    public void ReportedByOther_FalseWhenLocationIsEmpty()
    {
        Assert.False(PgLogEntryProvenance.ReportedByOther(Entry(location: string.Empty), "DeadLockReport"));
    }

    [Fact]
    public void ReportedByOther_FalseWhenLocationStartsWithTheExpectedFunction()
    {
        Assert.False(PgLogEntryProvenance.ReportedByOther(
            Entry(location: "DeadLockReport, deadlock.c:1"), "DeadLockReport"));
    }

    [Fact]
    public void ReportedByOther_TrueWhenLocationNamesExecStmtRaiseInstead()
    {
        Assert.True(PgLogEntryProvenance.ReportedByOther(
            Entry(location: "exec_stmt_raise, pl_exec.c:1"), "DeadLockReport"));
    }

    /* ---- #4058 review round 1: the innermost-frame parse ---------------------------------------------- */

    private const string Prefix = "PL/pgSQL function ";

    [Fact]
    public void ADoBlockRaiseFrame_IsRaiseShaped()
    {
        Assert.True(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: Prefix + "inline_code_block line 2 at RAISE")));
    }

    [Fact]
    public void AFunctionNamedInlineCodeBlock_IsParsedAsASignature_NotAsADoBlock()
    {
        /* A client can CREATE FUNCTION inline_code_block(...): its frame has an argument list, and its RAISE must
           still be caught rather than fail open on the DO-block branch. */
        Assert.True(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: Prefix + "inline_code_block(integer) line 3 at RAISE")));
    }

    [Fact]
    public void AQuotedFunctionNameHoldingANewline_StillEndsItsFrameAtRaise()
    {
        var name = "\"forge" + "\n" + "d\"";
        Assert.True(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: Prefix + name + "() line 3 at RAISE")));
    }

    [Fact]
    public void AQuotedNameImitatingAnSqlStatementFrame_IsStillReadToItsRealRaiseKind()
    {
        var name = "\"x() line 1 at SQL statement" + "\n" + "\"";
        Assert.True(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: Prefix + name + "() line 3 at RAISE")));
    }

    [Fact]
    public void AQuotedTypeArgumentContainingAParen_IsParsedToTheRealCloseParen()
    {
        Assert.True(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: Prefix + "f(\"odd)type\") line 3 at RAISE")));
    }

    [Fact]
    public void ARealDeadlockInsideAFunction_WhoseInnermostFrameIsAnSqlStatement_IsKept()
    {
        var context = "SQL statement \"UPDATE t SET v = 1 WHERE id = 2\"" + "\n" + Prefix + "upd(integer,integer) line 3 at SQL statement";
        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: context)));
    }

    [Fact]
    public void ARealDeadlockInARaisesSubquery_WhoseInnermostFrameIsAnSqlExpression_IsKept()
    {
        var context = "SQL expression \"(SELECT v FROM t WHERE id = 1 FOR UPDATE)\"" + "\n" + Prefix + "f() line 3 at RAISE";
        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: context)));
    }

    [Fact]
    public void AFunctionFrameAtAnotherStatementKind_IsNotRaiseShaped()
    {
        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: Prefix + "f() line 3 at SQL statement")));
        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: Prefix + "f() line 3 at RAISE statement")));
    }

    [Theory]
    [InlineData("PL/pgSQL function")]
    [InlineData("PL/pgSQL function f(")]
    [InlineData("PL/pgSQL function f() line x at RAISE")]
    [InlineData("PL/pgSQL function f() at RAISE")]
    [InlineData("garbage with no frame")]
    public void AnUnparseableContext_FailsOpen(string context)
    {
        Assert.False(PgLogEntryProvenance.RaisedByPlpgsql(Entry(context: context)));
    }
}
