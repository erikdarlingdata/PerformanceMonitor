/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Whether a server's remediation credential can drive the targeted plan-cache eviction, and the NAMED
/// reason when it cannot (#2138 phase 1).
///
/// <para>Two independent facts have to hold, and they fail for different reasons an operator would fix
/// differently, so they are separate members rather than one boolean. The engine either has the statement
/// or does not (nothing an operator can grant changes that); the credential either holds
/// <c>ALTER SERVER STATE</c> or does not (a grant fixes that one). Collapsing them would tell an operator
/// to ask for a permission that does not exist on their platform.</para>
///
/// <para><b>Neither fact is predicted from a per-platform table.</b> <see cref="StatementSupported"/> is
/// answered by the engine edition the registration upsert already stamps, and
/// <see cref="CredentialHoldsAlterServerState"/> by a read-only <c>has_perms_by_name</c> probe run AS the
/// remediation credential. A table of "platforms that allow plan-handle FREEPROCCACHE" would be a claim
/// about a managed service's grant set, which is the vendor's to change without telling us, and it would
/// go stale in the direction that keeps passing.</para>
/// </summary>
/// <param name="StatementSupported">The engine edition has <c>DBCC FREEPROCCACHE</c> at all.</param>
/// <param name="CredentialHoldsAlterServerState">The probe said this credential may run it. Null means the
/// probe has not run yet — deliberately distinct from false, because "not asked" and "asked and denied"
/// send an operator to different places.</param>
public sealed record EvictCapability(bool StatementSupported, bool? CredentialHoldsAlterServerState)
{
    /// <summary>Nothing known yet — the shape before any probe has run.</summary>
    public static EvictCapability Unprobed { get; } = new(true, null);
}

/// <summary>
/// The named reasons evict-first is unavailable. Consumer-API strings like the alert fact names and
/// <see cref="ForcePlanBotPolicy"/>'s reasons: add new ones freely, never redefine an existing one.
/// </summary>
public static class EvictDegradeReasons
{
    /// <summary><c>DBCC FREEPROCCACHE</c> is not a statement this engine edition has (Azure SQL Database).
    /// Not a permission problem and no grant fixes it.</summary>
    public const string UnsupportedOnPlatform = "evict_unsupported_on_platform";

    /// <summary>The remediation credential lacks <c>ALTER SERVER STATE</c>. A grant fixes this one, which is
    /// why it is not the same reason as <see cref="UnsupportedOnPlatform"/>.</summary>
    public const string PermissionDenied = "evict_permission_denied";

    /// <summary>The capability probe has not run yet, so evict-first is not offered — refusing to guess
    /// rather than attempting a write to find out.</summary>
    public const string CapabilityUnknown = "evict_capability_unknown";
}

/// <summary>
/// What an operator may do to ONE force-plan target on ONE server. Deliberately a value that is
/// <b>absent</b> (null from <see cref="OperatorRemediationGate.SurfaceFor"/>) rather than a value carrying
/// a disabled flag — see that method's remarks.
/// </summary>
/// <param name="EvictFirstOffered">The evict-then-observe path is available. When false,
/// <see cref="EvictUnavailableReason"/> names why and the force is offered on its own.</param>
/// <param name="EvictUnavailableReason">One of <see cref="EvictDegradeReasons"/>, or null when evict-first
/// IS offered. Never null-with-EvictFirstOffered-false: the degrade always carries its reason, which is
/// what stops it happening silently.</param>
public sealed record OperatorRemediationSurface(bool EvictFirstOffered, string? EvictUnavailableReason);

/// <summary>
/// The #2138 phase-1 arming gate: does an operator get an action surface for this force-plan target, and
/// if so, which levers.
///
/// <para><b>The gate EXECUTES the verdict object; it does not re-derive one.</b> Eligibility comes from
/// <see cref="StructuredForcePlanTarget.Eligible"/> and <see cref="StructuredForcePlanTarget.Blockers"/> —
/// the fields <c>FactRemediation.BuildStructuredRemediation</c> fills from
/// <c>FactRemediation.ForcePlanBlockers</c>, which is the same output an MCP consumer reads. Nothing here
/// looks at <c>ParameterSensitivityCoFired</c>, <c>ReplicaRole</c> or any other evidence field to reach a
/// verdict of its own. If a future change makes this file ask an evidence question, that is the second
/// policy path the whole design exists to avoid.</para>
///
/// <para><b>Why the return is nullable rather than an "enabled" flag.</b> The design's requirement is that
/// a server with no remediation credential has NO phase-1 surface — not a greyed-out button with a tooltip.
/// A record carrying <c>Enabled = false</c> invites exactly that button, because a binding to a present
/// object is the easy thing to write. Absence is unrenderable: a view binding
/// <c>Visibility</c> to null-ness cannot accidentally draw a disabled control, and a view-model with a
/// null surface has nothing to bind a command to. The nullability is the mechanism, not a convention.</para>
///
/// <para>Pure and static, no clock and no I/O, matching <see cref="ForcePlanBotPolicy"/> — both SKUs' view
/// models call this and neither can hold a different opinion.</para>
/// </summary>
public static class OperatorRemediationGate
{
    /// <summary>
    /// The surface for one target, or <b>null</b> for no surface at all.
    /// </summary>
    /// <param name="target">The verdict object, as an MCP consumer would read it.</param>
    /// <param name="remediationCredentialConfigured">This server has an opt-in remediation credential.
    /// False for every server until an operator enters one; there is no default and no fallback to the
    /// monitoring credential, which stays read-only forever.</param>
    /// <param name="evict">What the eviction lever can do here — see <see cref="EvictCapability"/>.</param>
    public static OperatorRemediationSurface? SurfaceFor(
        StructuredForcePlanTarget target,
        bool remediationCredentialConfigured,
        EvictCapability evict)
    {
        if (target is null)
        {
            return null;
        }

        /* No credential, no surface. First check on purpose: an operator who has not armed this server
           should not be able to tell an eligible target from an ineligible one through the presence of a
           control, because that is how a surface starts existing "just to explain itself". */
        if (!remediationCredentialConfigured)
        {
            return null;
        }

        /* Both halves of the verdict, and disagreement REFUSES. Eligible is defined as
           blockers.Count == 0 where the projection is built, so on any object that projection produced the
           two agree and the second read is free. It is not free on an object assembled anywhere else —
           a hand-built or deserialized target with Eligible true and a blocker listed would arm on the
           flag alone. Reading both means the only way to get a surface is for both to say so, and the
           direction a mismatch fails in is "no surface", which is the safe one. */
        if (!target.Eligible)
        {
            return null;
        }

        if (target.Blockers is { Count: > 0 })
        {
            return null;
        }

        var reason = EvictUnavailableReason(evict);
        return reason is null
            ? new OperatorRemediationSurface(EvictFirstOffered: true, EvictUnavailableReason: null)
            : new OperatorRemediationSurface(EvictFirstOffered: false, EvictUnavailableReason: reason);
    }

    /// <summary>
    /// Why evict-first is unavailable, or null when it is available. Split out so the degrade path can be
    /// exercised without an eligible target to hang it on.
    ///
    /// <para>Precedence is platform-then-permission, and it matters: on an engine that has no
    /// <c>DBCC FREEPROCCACHE</c> the permission question is meaningless, and reporting
    /// <see cref="EvictDegradeReasons.PermissionDenied"/> there would send an operator to ask a cloud
    /// provider for a grant that would change nothing.</para>
    /// </summary>
    public static string? EvictUnavailableReason(EvictCapability evict)
    {
        if (evict is null)
        {
            return EvictDegradeReasons.CapabilityUnknown;
        }

        if (!evict.StatementSupported)
        {
            return EvictDegradeReasons.UnsupportedOnPlatform;
        }

        return evict.CredentialHoldsAlterServerState switch
        {
            null => EvictDegradeReasons.CapabilityUnknown,
            false => EvictDegradeReasons.PermissionDenied,
            true => null,
        };
    }

    /// <summary>
    /// The read-only probe behind <see cref="EvictCapability.CredentialHoldsAlterServerState"/>, run AS the
    /// remediation credential against the target server.
    ///
    /// <para><c>has_perms_by_name(NULL, NULL, ...)</c> is the server-scope form, and it answers for the
    /// EFFECTIVE permissions of the login executing it — which is the only question that matters, because
    /// the grant can arrive through a server role, through <c>CONTROL SERVER</c>, or directly, and an
    /// operator on a managed platform generally cannot tell which they were given. Asking the server beats
    /// enumerating the routes.</para>
    ///
    /// <para>It is a SELECT. Running it costs nothing, changes nothing, and is safe against a server whose
    /// remediation credential turns out to be wrong — which is why the capability is probed rather than
    /// discovered by attempting the eviction and reading the error.</para>
    /// </summary>
    public const string AlterServerStateProbeSql =
        "SELECT has_alter_server_state = has_perms_by_name(NULL, NULL, 'ALTER SERVER STATE');";
}
