/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#5632 step 12: the parts of the session kill that need no server and no store. Behaviour only.</summary>
public sealed class KillSessionTests
{
    private const string Name = "kill_session";

    private static KillSessionFacts Facts(long? bytes = 0) =>
        new(57, new DateTime(2026, 10, 19, 8, 0, 0), "app_login", "host-a", "an app", "sleeping", 1, new DateTime(2026, 10, 19, 8, 5, 0), bytes);

    [Theory]
    [InlineData("kill_not_built_on_platform", "kill_not_built_on_platform")]
    [InlineData("own_session", "own_session")]
    [InlineData("session_gone", "session_gone")]
    [InlineData("session_reused", "session_reused")]
    [InlineData("not_user_session", "not_user_session")]
    [InlineData("kill_permission_denied", "kill_permission_denied")]
    public void EveryRefusalOfTheBatch_IsARefusalWithItsOwnReason_AndNeverSaysItWrote(string outcome, string reason)
    {
        foreach (var dryRun in new[] { true, false })
        {
            var result = SqlServerRemediationExecutor.KillAnswer(Name, dryRun, 57, outcome, Facts(), null, null);
            Assert.Equal(RemediationOutcome.Refused, result.Outcome);
            Assert.Equal(reason, result.Reason);
            Assert.False(result.Wrote);
            Assert.Null(result.Rollback);
        }
    }

    [Fact]
    public void OnlyTheKilledOutcome_SaysWrote_AndADryOkNever_DoesEvenOnARealCall()
    {
        var dry = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "dry_ok", Facts(), null, null);
        Assert.True(dry is { Outcome: RemediationOutcome.Done, Wrote: false, Rollback: null });

        var killed = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(), null, null);
        Assert.True(killed is { Outcome: RemediationOutcome.Done, Wrote: true });

        /* A real call whose batch answered "dry_ok" did not kill: it must not read as a write. */
        var confused = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "dry_ok", Facts(), null, null);
        Assert.False(confused.Wrote);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("something_else")]
    public void AnOutcomeTheBatchDoesNotName_IsAFailure_NeverAKill(string? outcome)
    {
        var result = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, outcome, Facts(), null, null);
        Assert.Equal(RemediationOutcome.Failed, result.Outcome);
        Assert.False(result.Wrote);
    }

    [Fact]
    public void AfterAKill_TheRollbackReadSaysWhetherARollbackRuns_AndItsPercent()
    {
        var running = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(4096), "KILLED/ROLLBACK", 42.5);
        Assert.Equal(new KillRollback(true, 42.5), running.Rollback);

        var none = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(0), null, null);
        Assert.Equal(new KillRollback(false, null), none.Rollback);

        /* An ordinary command's percent is not a rollback's. */
        var other = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(0), "SELECT", 12);
        Assert.Equal(new KillRollback(false, null), other.Rollback);
    }

    [Fact]
    public void TheSessionFacts_RideOnTheResult_SoTheJournalCanKeepThem()
    {
        var facts = Facts(120);
        var result = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "dry_ok", facts, null, null);
        Assert.Equal(facts, result.Session);
    }

    [Theory]
    [InlineData("2026-10-19T12:34:56.5400000", 2026, 10, 19, 12, 34, 56, 540)]
    [InlineData("2026-10-19T12:34:56.5400000Z", 2026, 10, 19, 12, 34, 56, 540)]
    [InlineData("2026-10-19T12:34:56Z", 2026, 10, 19, 12, 34, 56, 0)]
    [InlineData("2026-10-19T12:34:56.003", 2026, 10, 19, 12, 34, 56, 3)]
    public void TheLoginTimeGetBlockingReturns_IsReadAsUtc_WithItsFractionKept(string text, int y, int mo, int d, int h, int mi, int s, int ms)
    {
        Assert.True(DarlingMcpPlanForceTools.TryReadLoginTime(text, out var utc));
        Assert.Equal(new DateTime(y, mo, d, h, mi, s, ms), utc);
        Assert.Equal(DateTimeKind.Unspecified, utc.Kind);
    }

    [Fact]
    public void ALoginTimeWithAnOffset_IsMovedToUtc_NotReadAsLocal()
    {
        Assert.True(DarlingMcpPlanForceTools.TryReadLoginTime("2026-10-19T12:00:00+02:00", out var utc));
        Assert.Equal(new DateTime(2026, 10, 19, 10, 0, 0), utc);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("yesterday")]
    [InlineData("10/19/2026 12:00")]
    [InlineData("2026-10-19")]
    [InlineData("2026-13-45T00:00:00Z")]
    public void ALoginTimeThatIsNotATime_IsNotRead(string? text)
    {
        Assert.False(DarlingMcpPlanForceTools.TryReadLoginTime(text, out _));
    }

    [Fact]
    public void ALogByteCount_AboveZero_WarnsOfARollback_AndZeroOrNoneDoesNot()
    {
        var warned = Assert.Single(DarlingMcpPlanForceTools.KillWarnings(Facts(120)));
        Assert.Contains("rolls that work back", warned, StringComparison.Ordinal);
        Assert.Contains("120 bytes", warned, StringComparison.Ordinal);
        Assert.Empty(DarlingMcpPlanForceTools.KillWarnings(Facts(0)));
        Assert.Empty(DarlingMcpPlanForceTools.KillWarnings(Facts(null)));
        Assert.Empty(DarlingMcpPlanForceTools.KillWarnings(null));
    }

    [Fact]
    public void ASessionKill_HasNoUndo_WhateverItsResultAndAge()
    {
        var called = $$"""{"kind":"{{AgentChangeKinds.SessionKill}}","tool":"kill_session"}""";
        var change = new AgentChangeRow(1, DateTime.UtcNow, null, 7, "agent-a", "why", called, AgentChangeResult.Done, null, null, null, null);
        Assert.Equal(AgentChangeKinds.SessionKill, change.Kind);
        Assert.False(change.CanTryUndo(DateTime.UtcNow));
    }
}
