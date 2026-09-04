/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2918 — the compose <c>statement_timeout</c> reaching the live roles on a control-plane reload instead of
/// only on a service restart.
///
/// <para><b>Why it needed its own path at all.</b> Every other <c>config_service</c> knob goes live by
/// landing in the held <see cref="DarlingConfig"/>, because something reads the held value on its next use.
/// This one does not live in a query — it lives on the ROLES, as
/// <c>ALTER ROLE viewer/mcp SET statement_timeout</c>, written by startup provisioning. #2917 fixed the held
/// config's truthfulness and deliberately changed nothing about delivery; a reload still observed the new
/// value and left the roles on whatever the last start wrote.</para>
///
/// <para><b>The invariant that matters most here is no-drift.</b> Two code paths now write the same pair of
/// statements, and if they disagreed nothing would fail — both would run, and the live ceiling would depend
/// on which path touched the roles last. So there is exactly ONE renderer, and the test below asserts the
/// provisioning batch contains that renderer's own output rather than a retyped copy of it.</para>
/// </summary>
public sealed class ComposeStatementTimeoutReloadTests
{
    /* ---------------- the no-drift invariant ---------------- */

    /// <summary>
    /// The startup batch embeds the SHARED renderer's exact output. Spliced from the real artifact, not
    /// reimplemented: a retyped copy would prove the transcription works, which is not the claim.
    /// </summary>
    [Theory]
    [InlineData(15)]
    [InlineData(120)]
    [InlineData(600)]
    public void TheProvisioningBatch_EmbedsTheSharedRenderer_Verbatim(int seconds)
    {
        var shared = DarlingManagedRoles.BuildComposeStatementTimeoutSql(seconds);
        var batch = DarlingManagedRoles.BuildProvisioningSql(
            "AdminPassword01", "ViewerPassword02", "McpPassword03", seconds);

        Assert.Contains(shared, batch, StringComparison.Ordinal);

        /* And the batch carries the pair exactly once — an embed that also left the old inline copy behind
           would still satisfy Contains, while writing the ceiling twice. */
        Assert.Equal(2, Regex.Matches(batch, @"SET statement_timeout = '").Count);
    }

    /// <summary>
    /// Both compose identities, never just one: the mcp role gets viewer's whole read surface, so an
    /// unbounded mcp is an unbounded network-reachable compose surface over a raw, no-rollup store.
    /// </summary>
    [Fact]
    public void TheRenderer_BoundsBothComposeRoles_AndNotAdmin()
    {
        var sql = DarlingManagedRoles.BuildComposeStatementTimeoutSql(42);

        Assert.Contains("ALTER ROLE viewer SET statement_timeout = '42s';", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER ROLE mcp    SET statement_timeout = '42s';", sql, StringComparison.Ordinal);

        /* admin is the Settings writer (small config writes) and is deliberately unbounded — bounding it
           would put a ceiling on the path an operator uses to RAISE the ceiling. */
        Assert.DoesNotContain("ALTER ROLE admin", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The renderer clamps independently of the store read. It is public and both callers reach it with an
    /// operator-supplied number, and 0 would not mean "no timeout" to a reader — it means NO CEILING to
    /// PostgreSQL, which is the single outcome the backstop exists to prevent.
    /// </summary>
    [Theory]
    [InlineData(0, "15s")]
    [InlineData(-1, "15s")]
    [InlineData(1, "5s")]
    [InlineData(5, "5s")]
    [InlineData(600, "600s")]
    [InlineData(99999, "600s")]
    public void TheRenderer_ClampsOnItsOwn(int seconds, string expected)
    {
        Assert.Contains(
            $"SET statement_timeout = '{expected}'",
            DarlingManagedRoles.BuildComposeStatementTimeoutSql(seconds), StringComparison.Ordinal);
    }

    /* ---------------- the reload decision (pure) ---------------- */

    /// <summary>
    /// The gate fires on a real change and nothing else. A <c>config_version</c> bump happens on ANY
    /// <c>config_service</c> or schedule write, so re-asserting unconditionally would put a catalog write on
    /// reloads that have nothing to do with this knob.
    /// </summary>
    [Theory]
    // store, applied, managed, windows, expected
    [InlineData(120, 15, true, true, true)]    // operator raised it -> apply
    [InlineData(15, 120, true, true, true)]    // and lowering it is the case the backstop exists for
    [InlineData(15, 15, true, true, false)]    // unchanged -> no catalog write
    [InlineData(120, -1, true, true, true)]    // baseline unknown -> apply
    [InlineData(120, 15, false, true, false)]  // BYO: roles are the operator's, named by them
    [InlineData(120, 15, true, false, false)]  // off-Windows: provisioning never created these roles
    [InlineData(15, 15, false, false, false)]
    public void TheReloadGate_FiresOnlyOnARealChange_InManagedModeOnWindows(
        int store, int applied, bool managed, bool windows, bool expected)
    {
        Assert.Equal(
            expected,
            DarlingManagedRoles.ShouldReassertComposeStatementTimeout(store, applied, managed, windows));
    }

    /// <summary>
    /// <b>The baseline must advance only on success.</b> Recording the attempt rather than the write would
    /// make one transient failure permanent: the gate would see store == applied forever and never retry, so
    /// the roles would sit on the old ceiling with the held config claiming otherwise — the exact
    /// store-says-one-thing-roles-say-another split #2917 just closed, reintroduced one layer down.
    /// </summary>
    [Fact]
    public void AFailedReassert_LeavesTheBaselineBehind_SoTheNextReloadRetries()
    {
        /* Models the call site's success/failure handling; ReassertComposeStatementTimeoutAsync returns the
           bool this stands in for, and is non-throwing by contract. */
        var applied = 15;
        const int stored = 120;

        static bool Attempt(bool succeeds) => succeeds;

        /* Reload 1 — the ALTER ROLE fails (store unreachable mid-reload, say). */
        Assert.True(DarlingManagedRoles.ShouldReassertComposeStatementTimeout(stored, applied, true, true));
        if (Attempt(succeeds: false))
        {
            applied = stored;
        }

        Assert.Equal(15, applied);

        /* Reload 2 — still eligible, because the failure was not recorded as applied. */
        Assert.True(DarlingManagedRoles.ShouldReassertComposeStatementTimeout(stored, applied, true, true));
        if (Attempt(succeeds: true))
        {
            applied = stored;
        }

        Assert.Equal(120, applied);

        /* Reload 3 — now it settles, and stops paying the catalog write. */
        Assert.False(DarlingManagedRoles.ShouldReassertComposeStatementTimeout(stored, applied, true, true));
    }
}
