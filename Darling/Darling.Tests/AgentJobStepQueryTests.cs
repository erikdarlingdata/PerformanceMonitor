/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #3497's identity mechanics: the <c>program_name</c> parse, the GUID byte order, and the
/// one-round-trip msdb lookup's SQL shape. The byte-order pin is the load-bearing one — a wrong
/// conversion here does not error, it resolves NOTHING (the lookup matches no job and every card
/// degrades to the unresolved form), which is exactly the failure mode a test has to catch because
/// production never would.
/// </summary>
public sealed class AgentJobStepQueryTests
{
    /* ---------------- the parse and the byte order ---------------- */

    /// <summary>msdb job_id AB6D9F63-3B01-4E15-9F34-B0A0F0B355A2 as SQL Agent spells it: the
    /// uniqueidentifier's binary(16) hex, Data1–Data3 little-endian, Data4 in order.</summary>
    private const string StandardForm = "SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2 : Step 3)";

    [Fact]
    public void TryParse_TheStandardForm_RecoversTheJobId_WithSqlServersByteOrder()
    {
        /* THE ENDIANNESS PIN. The hex is CONVERT(binary(16), job_id) — SQL Server's GUID storage
           layout — and .NET's Guid(byte[]) constructor reads the same layout, so the parse does NO
           byte swapping. Verified against the engine's own conversion:
             SELECT CONVERT(binary(16), CONVERT(uniqueidentifier, 'AB6D9F63-3B01-4E15-9F34-B0A0F0B355A2'))
           yields 0x639F6DAB013B154E9F34B0A0F0B355A2 — note Data1 (AB6D9F63 → 639F6DAB), Data2
           (3B01 → 013B) and Data3 (4E15 → 154E) reversed, the last eight bytes in order. A parse via
           new Guid(hexString) instead would yield 639F6DAB-013B-154E-9F34-B0A0F0B355A2 — a guid that
           exists nowhere in msdb and silently resolves nothing. */
        Assert.True(AgentJobStepQuery.TryParseProgramName(StandardForm, out var key));
        Assert.Equal(Guid.Parse("AB6D9F63-3B01-4E15-9F34-B0A0F0B355A2"), key.JobId);
        Assert.Equal(3, key.StepId);
    }

    [Fact]
    public void ToProgramNameHex_RoundTripsTheParse_ByteForByte()
    {
        /* The unresolved card form spells the job id back in program_name's own hex so an operator can
           eyeball-match the two lines — that only works if the round trip is exact. */
        Assert.True(AgentJobStepQuery.TryParseProgramName(StandardForm, out var key));
        Assert.Equal("639F6DAB013B154E9F34B0A0F0B355A2", AgentJobStepQuery.ToProgramNameHex(key.JobId));
    }

    [Fact]
    public void TryParse_AcceptsLowercaseHex()
    {
        /* The hex DIGITS are value, not identity: Convert.FromHexString reads either case, and refusing
           lowercase would fail a spelling the engine itself considers equal. The PREFIX stays
           case-sensitive (see the next pin) because it is a machine-emitted marker, not a value. */
        Assert.True(AgentJobStepQuery.TryParseProgramName(
            "SQLAgent - TSQL JobStep (Job 0x639f6dab013b154e9f34b0a0f0b355a2 : Step 12)", out var key));
        Assert.Equal(Guid.Parse("AB6D9F63-3B01-4E15-9F34-B0A0F0B355A2"), key.JobId);
        Assert.Equal(12, key.StepId);
    }

    [Theory]
    /* Not the job-step form at all. */
    [InlineData(null)]
    [InlineData("")]
    [InlineData("HammerDB")]
    [InlineData("SQLAgent - Generic Refresher")]
    [InlineData("SQLAgent - Job invocation engine")]
    /* A human-cased lookalike: the prefix is machine-emitted and matched Ordinal on purpose. */
    [InlineData("sqlagent - tsql jobstep (job 0x639F6DAB013B154E9F34B0A0F0B355A2 : Step 3)")]
    /* Hex too short, hex too long, non-hex characters. */
    [InlineData("SQLAgent - TSQL JobStep (Job 0x639F6DAB : Step 3)")]
    [InlineData("SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2FF : Step 3)")]
    [InlineData("SQLAgent - TSQL JobStep (Job 0xZZ9F6DAB013B154E9F34B0A0F0B355A2 : Step 3)")]
    /* Step suffix missing, malformed, signed, or not closed. */
    [InlineData("SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2)")]
    [InlineData("SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2 : Step )")]
    [InlineData("SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2 : Step -1)")]
    [InlineData("SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2 : Step 3")]
    [InlineData("SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2 : Step 3) extra")]
    public void TryParse_MalformedForms_DegradeToNoAnnotation(string? programName)
    {
        /* Every malformed arm answers false — no annotation — rather than a guess: a wrong parse would
           put a wrong job name on a card whose whole value is being trustworthy. */
        Assert.False(AgentJobStepQuery.TryParseProgramName(programName, out _));
    }

    /* ---------------- the lookup SQL ---------------- */

    [Fact]
    public void BuildSql_NamesTheTwoMsdbTables_AndBindsEveryPair()
    {
        /* One round trip for a card's whole display budget: INNER JOIN sysjobs (the deleted-job arm —
           no row means the unresolved form, honestly), LEFT JOIN sysjobsteps (a missing step still
           names the JOB). The permission arm rides these exact two tables' SELECT grants, which is what
           the hosts' log lines tell the operator to grant. */
        var sql = AgentJobStepQuery.BuildSql(3);

        Assert.Contains("JOIN msdb.dbo.sysjobs AS j", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN msdb.dbo.sysjobsteps AS js", sql, StringComparison.Ordinal);
        for (int i = 0; i < 3; i++)
        {
            Assert.Contains($"(@job_id{i}, @step_id{i})", sql, StringComparison.Ordinal);
            Assert.Equal($"@job_id{i}", AgentJobStepQuery.JobIdParameter(i));
            Assert.Equal($"@step_id{i}", AgentJobStepQuery.StepIdParameter(i));
        }

        /* The display cap bounds the pair list, so a fourth pair has no business being spelled. */
        Assert.DoesNotContain("@job_id3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSql_RefusesAnEmptyPairList()
    {
        /* Zero pairs is a caller bug, not a query: the engine only resolves when it parsed at least one
           key, and VALUES () is not SQL. Loud beats a malformed statement the server rejects cryptically. */
        Assert.Throws<ArgumentOutOfRangeException>(() => AgentJobStepQuery.BuildSql(0));
    }
}
