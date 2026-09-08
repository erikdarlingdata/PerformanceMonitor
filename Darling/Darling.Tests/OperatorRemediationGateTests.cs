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
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2138 phase-1 arming gate. Four contracts, each of which the surface exists to keep:
/// <list type="number">
/// <item>a server with no remediation credential gets NO surface — not a disabled one;</item>
/// <item>the gate reads the existing <c>structured_remediation</c> verdict and asks no evidence
/// question of its own, so the PSP never-auto-force contract is inherited rather than re-implemented;</item>
/// <item>a disagreement between the verdict's two halves REFUSES;</item>
/// <item>evict-first degrading always carries a NAMED reason — never silently.</item>
/// </list>
///
/// <para>Targets are built as real <see cref="ForcePlanTarget"/>s and projected through
/// <c>FactRemediation.BuildStructuredRemediation</c> wherever the subject is eligibility, so these cases
/// travel the same path an MCP consumer's verdict does. A hand-built
/// <see cref="StructuredForcePlanTarget"/> appears only where the subject IS a hand-built object.</para>
/// </summary>
public sealed class OperatorRemediationGateTests
{
    private static StructuredForcePlanTarget Verdict(bool psp = false, string? replicaRole = null)
    {
        var target = new ForcePlanTarget(
            Database: "orders",
            QueryId: 42,
            PlanId: 7,
            BestPlanHash: "0x1111111111111111",
            LatestPlanHash: "0x2222222222222222",
            LatestCpuPerExecUs: 50000,
            BestCpuPerExecUs: 5000,
            RegressionFactor: 10.0,
            ReplicaRole: replicaRole,
            ParameterSensitivityCoFired: psp);

        var action = new RemediationAction(
            FactKey: "PLAN_REGRESSION",
            Action: "force",
            Targets: new List<ForcePlanTarget> { target });

        var structured = FactRemediation.BuildStructuredRemediation(action);
        Assert.NotNull(structured);
        return structured!.ForcePlanTargets.Single();
    }

    private static readonly EvictCapability FullyCapable = new(StatementSupported: true, true);

    /* ---------------- 1. no credential, no surface ---------------- */

    [Fact]
    public void AServerWithNoRemediationCredential_GetsNoSurface()
    {
        Assert.Null(OperatorRemediationGate.SurfaceFor(
            Verdict(), remediationCredentialConfigured: false, FullyCapable));
    }

    /// <summary>
    /// The discriminating half of the test above. Without this, the null could come from anything in the
    /// target — the assertion would pass against a gate that never returns a surface at all.
    /// </summary>
    [Fact]
    public void TheSameTargetWithACredential_DoesGetASurface()
    {
        var surface = OperatorRemediationGate.SurfaceFor(
            Verdict(), remediationCredentialConfigured: true, FullyCapable);

        Assert.NotNull(surface);
        Assert.True(surface!.EvictFirstOffered);
        Assert.Null(surface.EvictUnavailableReason);
    }

    /// <summary>
    /// The absence is expressed by the RETURN TYPE, and that is the mechanism rather than a convention:
    /// a nullable return has nothing for a view to bind a command to, while an object carrying
    /// <c>Enabled = false</c> is one <c>IsEnabled</c> binding away from a greyed-out button with a
    /// tooltip. Read through <see cref="NullabilityInfoContext"/> so a change to a non-nullable return —
    /// the shape that would make a disabled control the easy thing to write — fails here.
    /// </summary>
    [Fact]
    public void TheSurfaceIsExpressedAsANullableReturn_NotAnEnabledFlag()
    {
        var method = typeof(OperatorRemediationGate).GetMethod(nameof(OperatorRemediationGate.SurfaceFor));
        Assert.NotNull(method);

        var nullability = new NullabilityInfoContext().Create(method!.ReturnParameter);
        Assert.Equal(NullabilityState.Nullable, nullability.ReadState);

        /* And the surface type itself must carry no enabled/disabled member. The nullable return is the
           whole mechanism; a bool beside it would give a view a second, contradictable answer. */
        var offenders = typeof(OperatorRemediationSurface)
            .GetProperties()
            .Select(p => p.Name)
            .Where(n =>
                n.Contains("Enabled", StringComparison.OrdinalIgnoreCase) ||
                n.Contains("Disabled", StringComparison.OrdinalIgnoreCase) ||
                n.Contains("Visible", StringComparison.OrdinalIgnoreCase))
            /* EvictFirstOffered is about which LEVER, not whether the surface exists, so it is not an
               offender — matched by name so this stays a name test rather than a judgement call. */
            .Where(n => !string.Equals(n, nameof(OperatorRemediationSurface.EvictFirstOffered), StringComparison.Ordinal))
            .ToList();

        Assert.Empty(offenders);
    }

    /* ---------------- 2. the verdict object is the gate ---------------- */

    /// <summary>
    /// The PSP never-auto-force contract, arriving through the projection rather than re-derived: the
    /// gate never looks at <c>ParameterSensitivityCoFired</c>, it looks at <c>Eligible</c>/<c>Blockers</c>,
    /// and the projection is what turns the flag into those.
    /// </summary>
    [Fact]
    public void AParameterSensitiveTarget_GetsNoSurface_EvenFullyArmed()
    {
        var verdict = Verdict(psp: true);

        /* Proof the case is the one intended — a blocker-free verdict would make the assertion below
           pass for the wrong reason. */
        Assert.False(verdict.Eligible);
        Assert.Contains(verdict.Blockers, b => b == "parameter_sensitivity_cofired");

        Assert.Null(OperatorRemediationGate.SurfaceFor(
            verdict, remediationCredentialConfigured: true, FullyCapable));
    }

    [Fact]
    public void ASecondaryReplicaTarget_GetsNoSurface_EvenFullyArmed()
    {
        var verdict = Verdict(replicaRole: "Secondary");

        Assert.False(verdict.Eligible);
        Assert.Contains(verdict.Blockers, b => b == "secondary_replica_evidence");

        Assert.Null(OperatorRemediationGate.SurfaceFor(
            verdict, remediationCredentialConfigured: true, FullyCapable));
    }

    /// <summary>
    /// The gate must not have an opinion of its own. Every blocker
    /// <c>FactRemediation.ForcePlanBlockers</c> can produce has to suppress the surface, including ones
    /// added after this test was written — so the cases are derived from the projection's own output over
    /// the evidence shapes that produce blockers, not from a list retyped here.
    /// </summary>
    [Fact]
    public void EveryBlockerTheProjectionCanProduce_SuppressesTheSurface()
    {
        var blocking = new[]
        {
            Verdict(psp: true),
            Verdict(replicaRole: "Secondary"),
            Verdict(replicaRole: "Geo Secondary"),
            Verdict(psp: true, replicaRole: "Secondary"),
        };

        /* Positive control: the shapes really do produce blockers, so an empty-blockers projection
           cannot make this pass by making every case eligible. */
        Assert.Empty(blocking.Where(v => v.Blockers.Count == 0));

        foreach (var verdict in blocking)
        {
            Assert.Null(OperatorRemediationGate.SurfaceFor(
                verdict, remediationCredentialConfigured: true, FullyCapable));
        }
    }

    /* ---------------- 3. disagreement refuses ---------------- */

    /// <summary>
    /// A hand-built or deserialized verdict whose two halves disagree must refuse, in BOTH directions.
    /// The projection can never emit either shape (it defines <c>Eligible</c> as
    /// <c>blockers.Count == 0</c>), which is exactly why the gate reading only one of them would look
    /// correct forever while being one wire format away from arming a blocked target.
    /// </summary>
    [Theory]
    [InlineData(true, "parameter_sensitivity_cofired")]
    [InlineData(false, null)]
    public void AVerdictWhoseHalvesDisagree_GetsNoSurface(bool eligible, string? blocker)
    {
        var blockers = blocker is null ? Array.Empty<string>() : new[] { blocker };
        var handBuilt = new StructuredForcePlanTarget(
            "orders", 42, 7, "0x2222222222222222", "0x1111111111111111", null,
            Eligible: eligible,
            Blockers: blockers,
            Evidence: new StructuredForcePlanEvidence(10.0, 50000, 5000, blocker is not null),
            ForceSql: "force",
            UnforceSql: "unforce",
            VerifySql: "verify");

        Assert.Null(OperatorRemediationGate.SurfaceFor(
            handBuilt, remediationCredentialConfigured: true, FullyCapable));
    }

    /* ---------------- 4. the degrade always names a reason ---------------- */

    [Fact]
    public void AnEnginePlatformWithoutTheStatement_DegradesToForceOnly_WithThatNamedReason()
    {
        var surface = OperatorRemediationGate.SurfaceFor(
            Verdict(),
            remediationCredentialConfigured: true,
            new EvictCapability(StatementSupported: false, CredentialHoldsAlterServerState: true));

        Assert.NotNull(surface);
        Assert.False(surface!.EvictFirstOffered);
        Assert.Equal(EvictDegradeReasons.UnsupportedOnPlatform, surface.EvictUnavailableReason);
    }

    [Fact]
    public void ACredentialWithoutAlterServerState_DegradesToForceOnly_WithThatNamedReason()
    {
        var surface = OperatorRemediationGate.SurfaceFor(
            Verdict(),
            remediationCredentialConfigured: true,
            new EvictCapability(StatementSupported: true, CredentialHoldsAlterServerState: false));

        Assert.NotNull(surface);
        Assert.False(surface!.EvictFirstOffered);
        Assert.Equal(EvictDegradeReasons.PermissionDenied, surface.EvictUnavailableReason);
    }

    [Fact]
    public void AnUnprobedCapability_DegradesWithItsOwnReason_RatherThanGuessing()
    {
        var surface = OperatorRemediationGate.SurfaceFor(
            Verdict(), remediationCredentialConfigured: true, EvictCapability.Unprobed);

        Assert.NotNull(surface);
        Assert.False(surface!.EvictFirstOffered);
        Assert.Equal(EvictDegradeReasons.CapabilityUnknown, surface.EvictUnavailableReason);
    }

    /// <summary>
    /// On a platform with no statement, the answer must NOT be the permission reason — that one sends an
    /// operator to ask a cloud provider for a grant that would change nothing. Pinned separately from the
    /// arms above because it is a precedence claim, and precedence is what a later reordering breaks.
    /// </summary>
    [Fact]
    public void PlatformOutranksPermission_SoNoOneIsSentToAskForAGrantThatCannotHelp()
    {
        Assert.Equal(
            EvictDegradeReasons.UnsupportedOnPlatform,
            OperatorRemediationGate.EvictUnavailableReason(
                new EvictCapability(StatementSupported: false, CredentialHoldsAlterServerState: false)));

        Assert.Equal(
            EvictDegradeReasons.UnsupportedOnPlatform,
            OperatorRemediationGate.EvictUnavailableReason(
                new EvictCapability(StatementSupported: false, CredentialHoldsAlterServerState: null)));
    }

    /// <summary>
    /// THE never-silently contract, over every capability shape there is: a surface that does not offer
    /// evict-first always carries a reason, and one that does never carries a stale one. Exhaustive rather
    /// than case-by-case because the failure mode is a shape nobody enumerated — a new
    /// <see cref="EvictCapability"/> field would add shapes here and the null-reason arm would catch the
    /// one that fell through.
    /// </summary>
    [Fact]
    public void NoCapabilityShapeCanDegradeWithoutANamedReason()
    {
        var shapes =
            from supported in new[] { true, false }
            from granted in new bool?[] { true, false, null }
            select new EvictCapability(supported, granted);

        var checked_ = 0;
        foreach (var shape in shapes)
        {
            var surface = OperatorRemediationGate.SurfaceFor(
                Verdict(), remediationCredentialConfigured: true, shape);
            Assert.NotNull(surface);
            checked_++;

            if (surface!.EvictFirstOffered)
            {
                Assert.Null(surface.EvictUnavailableReason);
            }
            else
            {
                Assert.False(
                    string.IsNullOrWhiteSpace(surface.EvictUnavailableReason),
                    $"evict-first is unavailable for {shape} with no named reason — the degrade must " +
                    "never be silent");
            }
        }

        /* The loop really ran over every shape: 2 x 3. A comprehension that produced nothing would
           otherwise pass this test by asserting about no cases. */
        Assert.Equal(6, checked_);
    }

    /// <summary>
    /// Exactly one capability shape offers evict-first. Stated as a count so a change that quietly widens
    /// the grant — say, treating an unprobed capability as permitted — fails here rather than showing up
    /// as an unexpected <c>DBCC FREEPROCCACHE</c> attempt against a server.
    /// </summary>
    [Fact]
    public void ExactlyOneCapabilityShapeOffersEvictFirst()
    {
        var offering =
            (from supported in new[] { true, false }
             from granted in new bool?[] { true, false, null }
             let capability = new EvictCapability(supported, granted)
             where OperatorRemediationGate.EvictUnavailableReason(capability) is null
             select capability).ToList();

        var only = Assert.Single(offering);
        Assert.True(only.StatementSupported);
        Assert.True(only.CredentialHoldsAlterServerState == true);
    }

    /// <summary>
    /// The capability probe is a SELECT. It runs as the remediation credential against a production
    /// server, so the one thing it must never be is a statement that changes something — asserted against
    /// the shipped constant rather than a copy, and by naming the statements it must not contain rather
    /// than by matching a shape a rewrite would slip past.
    /// </summary>
    [Fact]
    public void TheCapabilityProbeIsReadOnly()
    {
        var sql = OperatorRemediationGate.AlterServerStateProbeSql;

        Assert.StartsWith("SELECT ", sql, StringComparison.Ordinal);

        /* Quoted literals are stripped, then the scan is by WORD BOUNDARY rather than substring — and both
           refinements were forced by this test failing on the real statement, twice, for reasons that were
           the scan's fault rather than the SQL's. First the permission NAME 'ALTER SERVER STATE' matched
           as a DDL keyword; then the output alias has_alter_server_state did, because a substring scan
           cannot tell an identifier from a statement. A scan with either flaw has to be silenced to ship,
           and a silenced scan guards nothing. */
        var withoutLiterals = Regex.Replace(sql, "'[^']*'", "''", RegexOptions.CultureInvariant);
        Assert.Contains("''", withoutLiterals, StringComparison.Ordinal);

        foreach (var forbidden in new[]
                 { "DBCC", "FREEPROCCACHE", "EXEC", "EXECUTE", "ALTER", "UPDATE", "DELETE", "INSERT", "DROP", "MERGE", "TRUNCATE" })
        {
            Assert.DoesNotMatch(
                new Regex($@"\b{forbidden}\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
                withoutLiterals);
        }

        /* Positive control for the scan itself: it must fire on a statement that really does write, or
           the loop above is asserting nothing about anything. Underscore-joined and quoted forms stay
           clear, which is exactly the discrimination the two failures above were about. */
        var writeShape = "SELECT 1; DBCC FREEPROCCACHE(0x00);";
        Assert.Matches(new Regex(@"\bDBCC\b", RegexOptions.IgnoreCase), writeShape);
        Assert.DoesNotMatch(new Regex(@"\bALTER\b", RegexOptions.IgnoreCase), "SELECT has_alter_server_state = 1;");

        /* One statement. A trailing terminator is house style; a second one would let a read-only-looking
           probe carry anything after it. */
        Assert.Equal(1, sql.Count(c => c == ';'));
        Assert.EndsWith(";", sql.TrimEnd(), StringComparison.Ordinal);

        /* It asks the server-scope question, not a database-scope one: has_perms_by_name's first two
           arguments must both be NULL or it answers about the current database instead, which would
           report a grant an eviction cannot use. */
        Assert.Contains("has_perms_by_name(NULL, NULL, 'ALTER SERVER STATE')", sql, StringComparison.Ordinal);

        /* House style: every output column aliased, so the answer is not called has_perms_by_name. */
        Assert.Contains("has_alter_server_state =", sql, StringComparison.Ordinal);
    }
}
