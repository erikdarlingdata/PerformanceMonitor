/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The proof that the server-action gate passed for ONE action on ONE target (#5632, step 6). Every server write in
/// <c>PerformanceMonitor.Darling.Remediation</c> takes one as its first argument, so there is no way to call a write without it.
/// <para>The constructor is <c>internal</c>: outside this assembly the only way to get a permit is
/// <see cref="InvestigationRequestStore.AuthorizeServerActionAsync"/>, which builds one only when
/// <see cref="InvestigationRequestStore.CheckServerActionAsync"/> returned no refusal. A permit names what it was issued for, and a write
/// refuses a permit issued for anything else: another action, another server, another database, another object.</para>
/// <para><see cref="Id"/> is the object acted on, written one way per action so the gate's caller and the write agree: the
/// <c>plan_id</c> in decimal for <c>force_plan</c> (force and unforce), the plan handle as <c>0x</c> hex for <c>evict_plan</c>, and the
/// session id in decimal for <c>kill_session</c>.</para>
/// </summary>
public sealed class ServerActionPermit
{
    internal ServerActionPermit(ServerActionCall call)
    {
        ArgumentNullException.ThrowIfNull(call);
        ServerAction = call.ServerAction;
        ServerId = call.Target.ServerId;
        ServerName = call.Target.ServerName;
        Id = call.Target.Id;
        Database = call.Target.Database;
        RequestId = call.Action.RequestId;
        AgentId = call.Action.AgentId;
    }

    /// <summary>A permit for a person's undo (#5632, step 9): issued by <see cref="AgentChangeUndo"/> after the two server-side conditions
    /// (the server's switch and its remediation login) and nothing else. <paramref name="actor"/> is the person (<c>person:&lt;login&gt;</c>) and
    /// <paramref name="requestId"/> is the request of the change being undone.</summary>
    internal ServerActionPermit(string serverAction, int serverId, string serverName, string? id, string? database, Guid requestId, string actor)
    {
        ServerAction = serverAction;
        ServerId = serverId;
        ServerName = serverName;
        Id = id;
        Database = database;
        RequestId = requestId;
        AgentId = actor;
    }

    /// <summary>One of <see cref="ServerActionNames"/>.</summary>
    public string ServerAction { get; }

    public int ServerId { get; }

    public string ServerName { get; }

    /// <summary>The object acted on, in the form the class remarks give for this action. Null when the gate was asked with no object.</summary>
    public string? Id { get; }

    public string? Database { get; }

    public Guid RequestId { get; }

    public string AgentId { get; }
}

/// <summary>The gate's answer as a value: exactly one of <see cref="Permit"/> and <see cref="Refusal"/> is set. <see cref="Approval"/> is set
/// with the permit (#7109): whether the action needs a person's approval, which the tool answers with a proposal instead of a write.</summary>
public sealed record ServerActionDecision(ServerActionPermit? Permit, AgentActionRefusal? Refusal, ServerActionApproval? Approval = null)
{
    /// <summary>True when the gate passed and the action needs a person's approval.</summary>
    public bool NeedsApproval => Permit is not null && Approval is { Needed: true };
}

/// <summary>The approval outcome of a passed gate (#7109): <see cref="Needed"/> as <see cref="ActionApprovalRules.ForRequest"/> decides it,
/// <see cref="SinglePerson"/> for the approver check, and the request's claim number, which a proposal stores.</summary>
public sealed record ServerActionApproval(bool Needed, bool SinglePerson, int ClaimNo);

public sealed partial class InvestigationRequestStore
{
    /// <summary>
    /// <see cref="CheckServerActionAsync"/>, answering with a <see cref="ServerActionPermit"/> when every condition holds. The check, the
    /// journaling of a refusal and the order of the conditions are that method's, unchanged: this adds the permit and nothing else.
    /// </summary>
    public async System.Threading.Tasks.Task<ServerActionDecision> AuthorizeServerActionAsync(ServerActionCall call, ServerActionHooks hooks,
        System.Threading.CancellationToken cancellationToken = default)
    {
        var check = await CheckServerActionCoreAsync(call, hooks, cancellationToken);
        return check.Refusal is null
            ? new ServerActionDecision(new ServerActionPermit(call), null, check.Approval)
            : new ServerActionDecision(null, check.Refusal);
    }
}
