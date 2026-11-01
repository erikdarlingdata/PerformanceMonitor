/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Who presses a proposal (#7109): a mapped person (the person and the identity they signed in with) or an administrator path (the shared web
/// token, the Viewer) with no person. An agent token never maps to a person, so no agent is ever an approver.
/// </summary>
public sealed record ProposalApprover
{
    private ProposalApprover(long? personId, long? identityId, string surface)
    {
        PersonId = personId;
        IdentityId = identityId;
        Surface = surface;
    }

    /// <summary>A mapped person, pressing through <paramref name="surface"/> (web, slack) with the identity <paramref name="identityId"/>.</summary>
    public static ProposalApprover Person(long personId, long identityId, string surface) =>
        new(personId, identityId, string.IsNullOrWhiteSpace(surface) ? throw new ArgumentException("A press names its surface.", nameof(surface)) : surface);

    /// <summary>An administrator path with no person: the shared web token or the Viewer.</summary>
    public static ProposalApprover Administrator(string surface) =>
        new(null, null, string.IsNullOrWhiteSpace(surface) ? throw new ArgumentException("A press names its surface.", nameof(surface)) : surface);

    public long? PersonId { get; }

    public long? IdentityId { get; }

    public string Surface { get; }

    public bool IsPerson => PersonId is not null;

    /// <summary>The value stored in <c>first_press_by</c> and <c>decided_by</c>: <c>person:&lt;id&gt;</c>, or <c>admin:&lt;surface&gt;</c>. The person id,
    /// not the display name, so a rename between the presses does not make a second person.</summary>
    public string Key => PersonId is { } id ? "person:" + id.ToString(CultureInfo.InvariantCulture) : "admin:" + Surface;

    /// <summary>The approver as journal detail: the key, the surface, and the person and identity when there is one.</summary>
    public JsonObject ToJson() => new()
    {
        ["by"] = Key,
        ["surface"] = Surface,
        ["person_id"] = PersonId,
        ["identity_id"] = IdentityId,
    };
}

/// <summary>
/// The proof that a confirming press passed (#7109): built only by <see cref="InvestigationRequestStore.ApproveProposalForRunAsync"/>,
/// in the transaction that moved the proposal from pending to approved, so outside this assembly there is no way to get one.
/// <see cref="InvestigationRequestStore.RunApprovedActionAsync"/> takes it, and <see cref="Permit"/> is what the tool's write step takes.
/// </summary>
public sealed class ApprovedActionPermit
{
    internal ApprovedActionPermit(ActionProposalRow proposal, ServerActionPermit permit, ProposalApprover approver, long calledId)
    {
        Proposal = proposal;
        Permit = permit;
        Approver = approver;
        CalledId = calledId;
    }

    public ActionProposalRow Proposal { get; }

    public ServerActionPermit Permit { get; }

    public ProposalApprover Approver { get; }

    /// <summary>The <c>action_called</c> journal row the approval wrote.</summary>
    public long CalledId { get; }

    /// <summary>Set once <see cref="InvestigationRequestStore.RunApprovedActionAsync"/> has run with this permit: a permit runs once.</summary>
    internal bool Used { get; set; }
}

/// <summary>The caller's part of an approval check: the live routes and the server's credential classification, as <see cref="ServerActionHooks"/>.</summary>
public sealed class ApprovedActionHooks
{
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> LoadRoutes { get; init; }

    public required Func<int, CancellationToken, Task<ServerCredentialState>> ReadCredential { get; init; }
}

/// <summary>The answer to a press check: the proposal and a permit for the press's dry run, or a refusal.</summary>
public sealed record ProposalPressCheck(ActionProposalRow? Proposal, ServerActionPermit? Permit, AgentActionRefusal? Refusal);

/// <summary>The answer to the confirming press's first transaction: the permit, or a refusal (the proposal stays pending).</summary>
public sealed record ProposalApproval(ApprovedActionPermit? Permit, AgentActionRefusal? Refusal);

/// <summary>What the tool's write step did: <see cref="ProposalStates"/> run, failed or unknown, with the after image or the error.</summary>
public sealed record ApprovedWriteOutcome(string State, string Message, string? AfterJson = null);

/// <summary>The approval path's end: the proposal's final state and what to tell the person.</summary>
public sealed record ApprovedRunResult(string State, string Message, string? Code, long CalledId, string? AfterJson);

public sealed partial class InvestigationRequestStore
{
    /// <summary>Codes of a press refusal that is the proposal's own, not a shared condition.</summary>
    public static class ProposalPressCodes
    {
        public const string NotFound = "proposal_not_found";
        public const string NotPending = "proposal_not_pending";
        public const string Expired = "proposal_expired";
        public const string NoFirstPress = "no_first_press";
        public const string OtherApprover = "other_approver";
        public const string NotApprover = "not_approver";
        public const string AdministratorNeedsSinglePerson = "administrator_needs_single_person";
        public const string AskerCannotApprove = "asker_cannot_approve";
        public const string AlreadyRun = "permit_used";
    }

    /// <summary>
    /// The journal event that names a person who asked in a request's thread (#6980 child (c), not written by any code yet). Its
    /// <c>detail.person_id</c> is the asker. Open rule 1 reads it, so the request's askers cannot approve on a two-person route.
    /// </summary>
    public const string AskerJournalEvent = "person_message";

    /// <summary>
    /// The approver check for a person (#7109), in the press's transaction: enabled (the row held <c>FOR SHARE</c>, so a disable waits for the
    /// press), the identity used is theirs, a grant of <c>approve</c> with the action's kind scoped to the fleet, the server or one of its tags,
    /// and whether they asked in the request.
    /// </summary>
    internal const string ApproverCheckSql = @"
SELECT p.enabled,
       EXISTS (SELECT 1 FROM config.person_identity AS i WHERE i.identity_id = $2 AND i.person_id = p.person_id),
       EXISTS (
           SELECT 1
           FROM config.person_grant AS g
           WHERE g.person_id = p.person_id
             AND g.capability = 'approve'
             AND $4 = ANY (g.kinds)
             AND (   (g.scope_server_id IS NULL AND g.scope_tag_id IS NULL)
                  OR g.scope_server_id = $3
                  OR g.scope_tag_id IN (SELECT m.tag_id FROM config.server_tag_map AS m WHERE m.server_id = $3))),
       EXISTS (
           SELECT 1
           FROM config.investigation_journal AS j
           WHERE j.request_id = $5 AND j.event = 'person_message' AND j.detail->>'person_id' = p.person_id::text)
FROM config.person AS p
WHERE p.person_id = $1
FOR SHARE OF p";

    /// <summary>The server action a proposal's tool runs (the grant's kind). Unforce is <c>force_plan</c>.</summary>
    public static string ServerActionForTool(string tool) => tool switch
    {
        "force_query_store_plan" or "unforce_query_store_plan" => ServerActionNames.ForcePlan,
        "evict_cached_plan" => ServerActionNames.EvictPlan,
        "kill_session" => ServerActionNames.KillSession,
        "recompile_module" => ServerActionNames.RecompileModule,
        "create_plan_guide" => ServerActionNames.CreatePlanGuide,
        "drop_plan_guide" => ServerActionNames.DropPlanGuide,
        _ => throw new ArgumentOutOfRangeException(nameof(tool), tool, "Not a Level 3 tool."),
    };

    /// <summary>The journal kind of a proposal's tool, as the agent's own call journals it.</summary>
    internal static string ChangeKindForTool(string tool) => tool switch
    {
        "force_query_store_plan" => AgentChangeKinds.PlanForce,
        "unforce_query_store_plan" => AgentChangeKinds.PlanUnforce,
        "evict_cached_plan" => AgentChangeKinds.PlanEvict,
        "kill_session" => AgentChangeKinds.SessionKill,
        "recompile_module" => AgentChangeKinds.ModuleRecompile,
        "create_plan_guide" => AgentChangeKinds.PlanGuideCreate,
        "drop_plan_guide" => AgentChangeKinds.PlanGuideDrop,
        _ => throw new ArgumentOutOfRangeException(nameof(tool), tool, "Not a Level 3 tool."),
    };

    /// <summary>
    /// The object a proposal's permit names, read from the stored target the way the agent's call writes the permit's id: the pid or session id
    /// in decimal for a kill, the plan id for force and unforce, the plan handle for evict. The database is the target's own.
    /// </summary>
    private static (string? Id, string? Database) PermitTargetOf(ActionProposalRow proposal)
    {
        var target = JsonNode.Parse(proposal.TargetJson)?.AsObject() ?? new JsonObject();
        static string? Text(JsonNode? node) => node is null ? null : node.GetValueKind() == System.Text.Json.JsonValueKind.String
            ? node.GetValue<string>() : node.ToJsonString();
        var id = proposal.Tool switch
        {
            "kill_session" => Text(target["pid"] ?? target["session_id"]),
            "force_query_store_plan" or "unforce_query_store_plan" => Text(target["plan_id"]),
            "evict_cached_plan" => Text(target["plan_handle"]),
            _ => null,
        };
        return (id, Text(target["database"]));
    }

    private async Task<ActionProposalRow?> ReadProposalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid proposalId, bool forUpdate,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, ProposalSelect + " WHERE proposal_id = $1" + (forUpdate ? " FOR UPDATE" : ""), P(proposalId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadProposal(reader) : null;
    }

    /// <summary>
    /// The checks of a press (#7109), in one transaction: the proposal pending and before its expiry, the shared conditions over the live
    /// routes (an approved proposal stands in for the approval setting, the claim is not asked for), and the approver (the grant and open
    /// rule 1). Null when every check holds, with the request's server name.
    /// </summary>
    private async Task<(AgentActionRefusal? Refusal, ActionProposalRow? Proposal, string? ServerName)> PressChecksAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, Guid proposalId, ProposalApprover approver, ApprovedActionHooks hooks, bool forUpdate, DateTime now,
        CancellationToken cancellationToken)
    {
        var proposal = await ReadProposalAsync(connection, transaction, proposalId, forUpdate, cancellationToken);
        if (proposal is null)
            return (new AgentActionRefusal(ProposalPressCodes.NotFound, "There is no proposal with that id."), null, null);
        if (proposal.State != "pending")
            return (new AgentActionRefusal(ProposalPressCodes.NotPending,
                $"This proposal is {proposal.State}, not waiting for a person. Nothing was run."), proposal, null);
        if (proposal.ExpiresAt <= now)
            return (new AgentActionRefusal(ProposalPressCodes.Expired,
                "This proposal's time ran out. Nothing was run. A new alert or a new request can propose it again."), proposal, null);

        var request = await ReadAgentActionRequestAsync(connection, transaction, proposal.RequestId, cancellationToken);
        if (request is null)
            return (new AgentActionRefusal(ProposalPressCodes.NotFound, "This proposal's request is gone."), proposal, null);

        var serverAction = ServerActionForTool(proposal.Tool);
        var (id, database) = PermitTargetOf(proposal);
        var target = new AgentActionTarget(proposal.ServerId, request.ServerName, id, database);
        var conditions = await SharedActionConditionsAsync(connection, transaction, request, serverAction, target, hooks.LoadRoutes,
            hooks.ReadCredential, lockRow: false, cancellationToken);
        if (conditions.Refusal is { } refusal)
            return (refusal, proposal, request.ServerName);

        var approverRefusal = await ApproverRefusalAsync(connection, transaction, approver, conditions.Approval!.SinglePerson, proposal.ServerId,
            serverAction, proposal.RequestId, cancellationToken);
        return (approverRefusal, proposal, request.ServerName);
    }

    /// <summary>
    /// The approver's grant and open rule 1 (#7109, design line 170). On a route without single-person approval the approver is a mapped
    /// person who did not ask in the request, and an administrator path cannot approve. With single-person approval an asker can approve, and
    /// so can an administrator path. A person is always enabled, signed in with an identity of their own, and holds <c>approve</c> with the
    /// action's kind for the server.
    /// </summary>
    private async Task<AgentActionRefusal?> ApproverRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, ProposalApprover approver,
        bool singlePerson, int serverId, string serverAction, Guid requestId, CancellationToken cancellationToken)
    {
        if (!approver.IsPerson)
            return singlePerson ? null : new AgentActionRefusal(ProposalPressCodes.AdministratorNeedsSinglePerson,
                "This request's route needs a second person to approve, so an administrator path with no person cannot approve it. A person mapped to a sign-in, with an approve grant for this server, approves it.");

        await using var command = Command(connection, transaction, ApproverCheckSql, P(approver.PersonId!.Value), P(approver.IdentityId!.Value),
            P(serverId), PText(serverAction), P(requestId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken) || !reader.GetBoolean(0) || !reader.GetBoolean(1) || !reader.GetBoolean(2))
            return new AgentActionRefusal(ProposalPressCodes.NotApprover,
                $"You cannot approve {serverAction} on this server: approving needs an enabled person, signed in with their own identity, with an approve grant for {serverAction} on this server.");
        if (!singlePerson && reader.GetBoolean(3))
            return new AgentActionRefusal(ProposalPressCodes.AskerCannotApprove,
                "You asked in this request, and its route needs a second person to approve. Another person with an approve grant approves it.");
        return null;
    }

    /// <summary>
    /// The checks of a press before its dry run (#7109): the proposal pending and before its expiry, the shared conditions, and the approver.
    /// Writes nothing. On a pass it answers the proposal and a permit for the tool's prepare step and dry run, against the stored server.
    /// </summary>
    public async Task<ProposalPressCheck> CheckProposalPressAsync(Guid proposalId, ProposalApprover approver, ApprovedActionHooks hooks,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(approver);
        ArgumentNullException.ThrowIfNull(hooks);
        await using var connection = await AgentActionSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var (refusal, proposal, serverName) = await PressChecksAsync(connection, transaction, proposalId, approver, hooks, forUpdate: false, Now(),
            cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        if (refusal is not null)
            return new ProposalPressCheck(proposal, null, refusal);
        var (id, database) = PermitTargetOf(proposal!);
        return new ProposalPressCheck(proposal, new ServerActionPermit(ServerActionForTool(proposal!.Tool), proposal.ServerId, serverName!, id, database,
            proposal.RequestId, approver.Key), null);
    }

    /// <summary>
    /// The confirming press's first transaction (#7109): the shared conditions, the approver's grant and open rule 1, the proposal's state, its
    /// expiry and the person of the first press, then pending to approved and <c>action_called</c> (the agent as the actor, the approver in the
    /// detail). The permit is built here and nowhere else. A refusal changes nothing: the proposal stays pending and the answer says why.
    /// </summary>
    public async Task<ProposalApproval> ApproveProposalForRunAsync(Guid proposalId, ProposalApprover approver, ApprovedActionHooks hooks,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(approver);
        ArgumentNullException.ThrowIfNull(hooks);
        var now = Now();
        await using var connection = await AgentActionSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var (refusal, proposal, serverName) = await PressChecksAsync(connection, transaction, proposalId, approver, hooks, forUpdate: true, now,
            cancellationToken);
        if (refusal is null && proposal!.FirstPressBy is null)
            refusal = new AgentActionRefusal(ProposalPressCodes.NoFirstPress,
                "Nobody has pressed this proposal yet. The first press runs a fresh dry run and shows its facts. Confirm after that.");
        else if (refusal is null && !string.Equals(proposal!.FirstPressBy, approver.Key, StringComparison.Ordinal))
            refusal = new AgentActionRefusal(ProposalPressCodes.OtherApprover,
                "Another person made the first press. The same person makes both presses: press first, read the facts, then confirm.");
        if (refusal is not null)
        {
            await transaction.RollbackAsync(cancellationToken);
            return new ProposalApproval(null, refusal);
        }

        var detail = new JsonObject { ["approver"] = approver.ToJson() };
        if (!await ApproveProposalAsync(connection, transaction, proposalId, approver.Key, detail, cancellationToken))
        {
            await transaction.RollbackAsync(cancellationToken);
            return new ProposalApproval(null, new AgentActionRefusal(ProposalPressCodes.NotPending, "This proposal is no longer waiting for a person. Nothing was run."));
        }

        var (id, database) = PermitTargetOf(proposal!);
        var called = new JsonObject
        {
            ["tool"] = proposal!.Tool,
            ["kind"] = ChangeKindForTool(proposal.Tool),
            ["target"] = new JsonObject { ["id"] = id, ["server_id"] = proposal.ServerId, ["database"] = database },
            ["claim_no"] = proposal.ClaimNo,
            ["proposal_id"] = proposal.ProposalId.ToString(),
            ["approved_by"] = approver.ToJson(),
            ["before"] = JsonNode.Parse(proposal.BindingJson),
        };
        var calledId = await JournalReturningAsync(connection, transaction, now, proposal.RequestId, proposal.ServerId, "action_called",
            "agent:" + proposal.Agent, proposal.Reason, called.ToJsonString(), cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        var permit = new ServerActionPermit(ServerActionForTool(proposal.Tool), proposal.ServerId, serverName!, id, database, proposal.RequestId,
            proposal.Agent);
        return new ProposalApproval(new ApprovedActionPermit(proposal, permit, approver, calledId), null);
    }

    /// <summary>
    /// The confirming press's second transaction (#7109), as the agent's write runs: it takes the server's switch lock and reads the service
    /// row <c>FOR SHARE</c>, runs the shared conditions again, and then <paramref name="write"/>, the tool's write step, under one deadline. A
    /// refusal here marks the proposal <c>refused</c> with the reason and journals <c>action_failed</c>. The write's outcome ends the proposal
    /// <c>run</c>, <c>failed</c> or <c>unknown</c>. A permit runs once.
    /// </summary>
    public async Task<ApprovedRunResult> RunApprovedActionAsync(ApprovedActionPermit permit, ApprovedActionHooks hooks,
        Func<CancellationToken, Task<ApprovedWriteOutcome>> write, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(permit);
        ArgumentNullException.ThrowIfNull(hooks);
        ArgumentNullException.ThrowIfNull(write);
        if (permit.Used)
            return new ApprovedRunResult("refused", "This approval already ran.", ProposalPressCodes.AlreadyRun, permit.CalledId, null);
        permit.Used = true;

        var proposal = permit.Proposal;
        var actor = permit.Approver.Key;
        var agent = "agent:" + proposal.Agent;
        var serverAction = ServerActionForTool(proposal.Tool);
        var now = Now();

        await using var connection = await AgentActionSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await ServerActionSwitchLock.AcquireSharedAsync(connection, transaction, proposal.ServerId, serverAction, _commandTimeoutSeconds, cancellationToken);
        var request = await ReadAgentActionRequestAsync(connection, transaction, proposal.RequestId, cancellationToken);
        var refusal = request is null
            ? new AgentActionRefusal(ProposalPressCodes.NotFound, "This proposal's request is gone.")
            : (await SharedActionConditionsAsync(connection, transaction, request,
                serverAction, new AgentActionTarget(permit.Permit.ServerId, permit.Permit.ServerName, permit.Permit.Id, permit.Permit.Database),
                hooks.LoadRoutes, hooks.ReadCredential, lockRow: true, cancellationToken)).Refusal;
        if (refusal is not null)
        {
            await JournalReturningAsync(connection, transaction, now, proposal.RequestId, proposal.ServerId, "action_failed", agent, refusal.Reason,
                new JsonObject { ["called_id"] = permit.CalledId, ["error"] = refusal.Reason }.ToJsonString(), cancellationToken);
            await FinishProposalAsync(connection, transaction, proposal.ProposalId, "refused", actor,
                new JsonObject { ["code"] = refusal.Reason, ["message"] = refusal.Message }, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new ApprovedRunResult("refused", refusal.Message, refusal.Reason, permit.CalledId, null);
        }

        ApprovedWriteOutcome outcome;
        using (var remoteWork = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
        {
            remoteWork.CancelAfter(RemoteWorkDeadline);
            try
            {
                outcome = await write(remoteWork.Token);
            }
            catch (OperationCanceledException) when (remoteWork.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                outcome = new ApprovedWriteOutcome("unknown", $"The monitored server did not finish within {AgentActionRemoteWorkSeconds} seconds. "
                    + "Whether the change was made is unknown: check the server before trying again.");
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                outcome = new ApprovedWriteOutcome("failed", ex.Message);
            }
        }

        /* A lost reply leaves action_called with no result row, which reads "result unknown", as the agent's write does. */
        var writeNow = Now();
        if (outcome.State == "run")
            await JournalReturningAsync(connection, transaction, writeNow, proposal.RequestId, proposal.ServerId, "action_done", agent, null,
                new JsonObject { ["called_id"] = permit.CalledId, ["after"] = outcome.AfterJson is null ? null : JsonNode.Parse(outcome.AfterJson) }.ToJsonString(),
                cancellationToken);
        else if (outcome.State == "failed")
            await JournalReturningAsync(connection, transaction, writeNow, proposal.RequestId, proposal.ServerId, "action_failed", agent, "error",
                new JsonObject { ["called_id"] = permit.CalledId, ["error"] = outcome.Message }.ToJsonString(), cancellationToken);
        await FinishProposalAsync(connection, transaction, proposal.ProposalId, outcome.State, actor,
            new JsonObject { ["message"] = outcome.Message, ["called_id"] = permit.CalledId }, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new ApprovedRunResult(outcome.State, outcome.Message, null, permit.CalledId, outcome.AfterJson);
    }
}
