/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4346: <c>PgPlanForceActionStore.SanitizeDetailForAudit</c> is the read-side gate that keeps
/// pre-#4326 rows' raw exception text out of <c>get_plan_force_actions</c> (and any other reader of
/// <c>GetRecentActionsAsync</c>). There is no build marker on the row, so the gate is a positive match
/// against the one line #4326 made the only safe shape for a <c>state_unavailable</c> evidence line;
/// anything else on that line is legacy and gets replaced.
/// </summary>
public sealed class PlanForceActionAuditRedactionTests
{
    [Fact]
    public void PostFixLine_PassesThrough_TypeOnly()
    {
        var detail = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    [Fact]
    public void PostFixLine_PassesThrough_WithSqlState()
    {
        var detail = "state_unavailable: the forcing and automatic-plan-correction state read failed (PostgresException, SQLSTATE 57014; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    [Fact]
    public void PostFixLine_PassesThrough_MissingSchemaNote()
    {
        var detail = "state_unavailable: the forcing and automatic-plan-correction state read failed (PostgresException, SQLSTATE 42P01; a table or column the read needs is missing, logged only at Debug level) \u2014 an unattended force cannot proceed on an unknown engine state";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    /// <summary>The pin: a pre-#4326 row's raw exception text (host, credential, relation names — anything
    /// an exception message can carry) never reaches the caller. This is the exact shape
    /// <c>PgPlanForceActionStore.TryGetTargetStatesAsync</c>'s old catch wrote before #4326.</summary>
    [Fact]
    public void LegacyLine_WithRawExceptionText_IsRedacted()
    {
        var legacy = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException: 28P01: password authentication failed for user \"darling_ro\" at host db-primary-02.internal)";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(legacy);

        Assert.DoesNotContain("darling_ro", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
        Assert.DoesNotContain("password authentication failed", sanitized);
        Assert.Equal(
            "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state",
            sanitized);
    }

    [Fact]
    public void OtherBlockerLines_AreNeverTouched()
    {
        var detail = "apc_owns_it: query_store_stats: plan 7 is_forced_plan = true\nstate_unavailable: garbage (Exception: whatever leaked)";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(detail)!;
        var lines = sanitized.Split('\n');

        Assert.Equal("apc_owns_it: query_store_stats: plan 7 is_forced_plan = true", lines[0]);
        Assert.DoesNotContain("whatever leaked", lines[1]);
    }

    [Fact]
    public void NonStateUnavailableDetail_IsUnchanged()
    {
        var detail = "no write path in this build (#2138 phase 1 is detection, evidence and dry-run only)";
        Assert.Equal(detail, PgPlanForceActionStore.SanitizeDetailForAudit(detail));
    }

    [Fact]
    public void NullDetail_StaysNull()
    {
        Assert.Null(PgPlanForceActionStore.SanitizeDetailForAudit(null));
    }

    /// <summary>pin (a) from the #4363 anchoring round: a legacy line that STARTS with the safe prefix but
    /// carries exception text after the fixed sentence — the shape a naive prefix check (rather than a full
    /// anchor) would have let through.</summary>
    [Fact]
    public void LegacyLine_SafePrefixWithTrailingExceptionText_IsRedacted()
    {
        var legacy = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) extra: password authentication failed for user \"darling_ro\" at host db-primary-02.internal \u2014 an unattended force cannot proceed on an unknown engine state";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(legacy)!;

        Assert.DoesNotContain("darling_ro", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
        Assert.DoesNotContain("password authentication failed", sanitized);
        Assert.Equal(
            "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state",
            sanitized);
    }

    /// <summary>pin (b): the safe shape followed by a newline and exception text — an embedded newline in
    /// a legacy exception message must not let the second line escape as if it were a sibling blocker's
    /// evidence line.</summary>
    [Fact]
    public void LegacyLine_SafeShapeFollowedByNewlineAndExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\nleaked: password=hunter2 host=db-primary-02.internal";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
        Assert.Equal(
            "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state",
            sanitized);
    }

    /// <summary>pin (c), safe half: a CRLF-terminated sibling blocker line ahead of the safe shape must not
    /// stop the safe shape from passing — the anchor treats the position right after '\n' as the line start
    /// regardless of a preceding '\r'.</summary>
    [Fact]
    public void CrlfPrecededSafeLine_StillPassesThrough()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var detail = "apc_owns_it: query_store_stats: plan 7 is_forced_plan = true\r\n" + safeLine;
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(detail)!;

        Assert.Equal(detail, sanitized);
    }

    /// <summary>pin (c), unsafe half: a CRLF-joined legacy line still gets redacted.</summary>
    [Fact]
    public void CrlfSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\r\nleaked: password=hunter2 host=db-primary-02.internal";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
        Assert.DoesNotContain("db-primary-02", sanitized);
    }

    /// <summary>A lone '\r' between the safe shape and smuggled text is not a line boundary to .NET's
    /// '^'/'\z' under <c>Multiline</c>, so the smuggled text stays inside the redacted block rather than
    /// surviving as if it were a new line.</summary>
    [Fact]
    public void LoneCrSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\rleaked: password=hunter2";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
    }

    /// <summary>The Unicode LINE SEPARATOR (U+2028) is not a line boundary to .NET's '^'/'\z' either, so
    /// smuggled text after one stays inside the redacted block.</summary>
    [Fact]
    public void UnicodeLineSeparatorSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\u2028leaked: password=hunter2";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
    }

    /// <summary>Same as the LINE SEPARATOR pin, for the Unicode PARAGRAPH SEPARATOR (U+2029).</summary>
    [Fact]
    public void UnicodeParagraphSeparatorSmuggledExceptionText_IsRedacted()
    {
        var safeLine = "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";
        var smuggled = safeLine + "\u2029leaked: password=hunter2";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(smuggled)!;

        Assert.DoesNotContain("hunter2", sanitized);
    }

    /// <summary>A sibling blocker's line on the same multi-line detail still survives redaction of an
    /// adjacent, unsafe state_unavailable block — the block match must not eat past the next blocker's
    /// prefix.</summary>
    [Fact]
    public void SiblingBlockerLine_SurvivesEvenWithAdjacentRedaction()
    {
        var detail = "state_unavailable: garbage (Exception: whatever leaked)\napc_owns_it: query_store_stats: plan 7 is_forced_plan = true";
        var sanitized = PgPlanForceActionStore.SanitizeDetailForAudit(detail)!;
        var lines = sanitized.Split('\n');

        Assert.DoesNotContain("whatever leaked", sanitized);
        Assert.Equal("apc_owns_it: query_store_stats: plan 7 is_forced_plan = true", lines[1]);
    }
}
