/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One server action a caller asks the gate about (#5632, step 5): the tool, the action (one of <see cref="ServerActionNames"/>),
/// the validated <c>agent_action</c> and the server it would run on.</summary>
public sealed record ServerActionCall(string Tool, string ServerAction, AgentAction Action, AgentActionTarget Target, string? TokenName = null);

/// <summary>The caller's part of a server-action check (#5632, step 5): the live routes (as <see cref="AgentActionHooks.LoadRoutes"/>) and
/// the server's credential classification. The credential read is the caller's because the <c>remediation_encrypted_password</c> column is
/// secret to the <c>viewer</c> and <c>mcp</c> roles: the caller runs it as the store owner, through
/// <see cref="InvestigationRequestStore.ReadServerCredentialStateAsync"/>. It never resolves the secret.</summary>
public sealed class ServerActionHooks
{
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> LoadRoutes { get; init; }

    public required Func<int, CancellationToken, Task<ServerCredentialState>> ReadCredential { get; init; }
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>One switch row: no row is off. The switch table has no writer in this slice.</summary>
    internal const string ServerActionSwitchSql =
        "SELECT enabled FROM config.config_server_actions WHERE server_id = $1 AND action = $2";

    /// <summary>The two slots the credential check reads. The secret is classified by its prefix and never returned or resolved.</summary>
    internal const string ServerCredentialSql =
        "SELECT remediation_username, remediation_encrypted_password FROM config.config_monitored_servers WHERE server_id = $1";

    /// <summary>The level a request must hold for a server action (#5632, step 5).</summary>
    internal const int ServerActionLevel = 3;

    /// <summary>
    /// Classifies a server's remediation slots (#5240, #5632 step 5). Missing: no login name or no secret slot. Reference: the slot
    /// starts with <c>env:</c> or <c>file:</c>, the same prefixes <c>DarlingSecretSource.IsReference</c> and the store's own trigger
    /// accept. StoredPassword: anything else. The text is never resolved or returned.
    /// </summary>
    public static ServerCredentialState ClassifyCredential(string? username, string? stored)
    {
        if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(stored))
            return ServerCredentialState.Missing;
        return stored.StartsWith("env:", StringComparison.Ordinal) || stored.StartsWith("file:", StringComparison.Ordinal)
            ? ServerCredentialState.Reference
            : ServerCredentialState.StoredPassword;
    }

    /// <summary>Reads a server's remediation slots on <paramref name="source"/> (the store owner's connection: the secret column is not
    /// readable by the viewer or mcp roles) and classifies them. A server with no row is <see cref="ServerCredentialState.Missing"/>.</summary>
    public async Task<ServerCredentialState> ReadServerCredentialStateAsync(NpgsqlDataSource source, int serverId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        await using var connection = await source.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, ServerCredentialSql, P(serverId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return ServerCredentialState.Missing;
        return ClassifyCredential(reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    /// <summary>
    /// The one gate for a server action (#5632, step 5). It answers whether ONE action may run on ONE server for ONE request, and
    /// writes nothing but a journal row on a refusal. Every condition is required, checked in this order, and each refusal has its
    /// own reason: 1 <c>actions_off</c> (<c>config_service.agent_actions_enabled</c>), then the claim (<c>claim_not_live</c>) and
    /// <c>practice</c>, 2 <c>not_level_3</c> (the stamped level, and every metric re-resolved over the live routes; the lowest wins;
    /// a Level 2 request is told Level 2 changes only the monitoring store), <c>other_server</c>, 3 <c>server_action_off</c> (that
    /// server's row for that action is present and enabled; no row is off), 4 <c>no_remediation_credential</c> or
    /// <c>credential_not_reference</c> (a login name and an <c>env:</c> or <c>file:</c> reference; a stored password is refused).
    /// <c>mcp.allowWrites</c> is not an input and no flag skips a check. A refusal is journaled <c>refused</c>, at most once per
    /// request, verified token, tool, code and hour, as the Level 2 gate journals it. <b>Nothing calls this yet:</b> steps 7, 10 and 12 will.
    /// Null means every condition holds; the secret was not read or resolved.
    /// </summary>
    public async Task<AgentActionRefusal?> CheckServerActionAsync(ServerActionCall call, ServerActionHooks hooks,
        CancellationToken cancellationToken = default) =>
        (await CheckServerActionCoreAsync(call, hooks, cancellationToken)).Refusal;

    /// <summary>
    /// <see cref="CheckServerActionAsync"/>, also answering whether the action needs a person's approval (#7109). The approval is an outcome,
    /// never a refusal here: it is set only when every condition holds, so the tool can run its prepare step and a dry run and store a
    /// proposal. Only the write's re-check refuses an action that needs approval.
    /// </summary>
    private async Task<ServerActionCheck> CheckServerActionCoreAsync(ServerActionCall call, ServerActionHooks hooks,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(call);
        ArgumentNullException.ThrowIfNull(hooks);
        var action = call.Action;
        var actor = "agent:" + action.AgentId;
        var now = Now();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var request = await ReadAgentActionRequestAsync(connection, transaction, action.RequestId, cancellationToken);
        var (refusal, approval) = await ServerActionChecksAsync(connection, transaction, request, call, hooks, now, cancellationToken);
        if (refusal is not null)
            await JournalRefusedAsync(connection, transaction, now, request is null ? null : action.RequestId,
                request?.ServerId, actor, call.TokenName, call.Tool, refusal.Reason, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return refusal is not null
            ? new ServerActionCheck(refusal, null)
            : new ServerActionCheck(null, new ServerActionApproval(approval!.Needed, approval.SinglePerson, request!.ClaimCount));
    }

    /// <summary>The gate's answer inside the store: a refusal, or the approval outcome of an action that may proceed.</summary>
    private sealed record ServerActionCheck(AgentActionRefusal? Refusal, ServerActionApproval? Approval);

    private async Task<(AgentActionRefusal? Refusal, ActionApprovalState? Approval)> ServerActionChecksAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        AgentActionRequest? request, ServerActionCall call, ServerActionHooks hooks, DateTime now, CancellationToken cancellationToken)
    {
        if (!System.Linq.Enumerable.Contains(ServerActionNames.All, call.ServerAction, StringComparer.Ordinal))
            return (new AgentActionRefusal(AgentActionCodes.UnknownServerAction,
                "That is not a server action. The actions are force_plan, evict_plan, kill_session, recompile_module, create_plan_guide and drop_plan_guide."), null);

        /* 1: the service-wide switch, the one read the write-time re-check uses (without the lock, here). It stays ahead of the claim, so
           the gate answers actions_off first, as it always has. The shared conditions read it again: a cheap read of one row. */
        if (await ActionsOffRefusalAsync(connection, transaction, lockRow: false, cancellationToken) is { } off)
            return (off, null);

        /* 2: the claim, the agent's own condition. 3 and 4: every other condition, in the one method the approval path runs too (#7109).
           The approval is read there, over the same routes, and reported, not refused. */
        var conditions = await ServerActionConditionsAsync(connection, transaction, request, call.ServerAction, call.Action, call.Target,
            hooks.LoadRoutes, hooks.ReadCredential, lockRow: false, now, practice: true, cancellationToken);
        return conditions.Refusal is { } refusal ? (refusal, null) : (null, conditions.Approval);
    }

    /// <summary>
    /// The agent's conditions of a server action that can change between the gate and the write (#5632): the claim (and, in the gate, the
    /// practice flag), then <see cref="SharedActionConditionsAsync"/>. The refusal codes and words are the same in both places. The approval
    /// (#7109) is read there: the gate reports it, and the write (<paramref name="refuseNeedsApproval"/>) refuses it, so a route a person set to
    /// <c>required</c> after the gate refuses the agent's write, and a future tool that reaches the write cannot skip it.
    /// </summary>
    private async Task<AgentActionRefusal?> ServerActionConditionsRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        AgentActionRequest? request, string serverAction, AgentAction action, AgentActionTarget target,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> loadRoutes, DateTime now, bool practice,
        bool refuseNeedsApproval, CancellationToken cancellationToken)
    {
        var conditions = await ServerActionConditionsAsync(connection, transaction, request, serverAction, action, target, loadRoutes, null,
            lockRow: true, now, practice, cancellationToken);
        if (conditions.Refusal is not null)
            return conditions.Refusal;
        if (refuseNeedsApproval && conditions.Approval!.Needed)
            return new AgentActionRefusal(AgentActionCodes.ApprovalRequired, ApprovalRequiredMessage);
        return null;
    }

    private const string ApprovalRequiredMessage =
        "This action needs a person's approval, so the agent's own write does not run it. A person may have set this request's route to require approval after the check. Nothing was run. Call the tool again with dry_run false to propose it for approval.";

    /// <summary>The agent's path: the claim check, which only this path has, then the conditions both paths share.</summary>
    private async Task<(AgentActionRefusal? Refusal, ActionApprovalState? Approval)> ServerActionConditionsAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, AgentActionRequest? request, string serverAction, AgentAction action, AgentActionTarget target,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> loadRoutes,
        Func<int, CancellationToken, Task<ServerCredentialState>>? readCredential, bool lockRow, DateTime now, bool practice,
        CancellationToken cancellationToken)
    {
        if (ClaimOrPracticeRefusal(request, action, now, practice) is { } claimRefusal)
            return (claimRefusal, null);
        return await SharedActionConditionsAsync(connection, transaction, request!, serverAction, target, loadRoutes, readCredential, lockRow,
            cancellationToken);
    }

    /// <summary>
    /// Every condition of a server action but the claim, in ONE method the agent's path and the approval path both run (#7109), so the two
    /// cannot drift: the service switch (held <c>FOR SHARE</c> when <paramref name="lockRow"/>), the request not closed (and for
    /// <c>kill_session</c> on an alert request, its alert not resolved), Level 3 over the live routes (the lowest level among the request's
    /// metrics wins) with the approval setting over the same routes, the request's own server, this server's switch for this action (no row is
    /// off) and, when <paramref name="readCredential"/> is given, the remediation login. The approval is returned, never refused here: the
    /// agent's write refuses it, and on the approval path an approved proposal satisfies it.
    /// </summary>
    private async Task<(AgentActionRefusal? Refusal, ActionApprovalState? Approval)> SharedActionConditionsAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, AgentActionRequest live, string serverAction, AgentActionTarget target,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> loadRoutes,
        Func<int, CancellationToken, Task<ServerCredentialState>>? readCredential, bool lockRow, CancellationToken cancellationToken)
    {
        if (await ActionsOffRefusalAsync(connection, transaction, lockRow, cancellationToken) is { } actionsOff)
            return (actionsOff, null);

        if (live.State == "closed")
            return (new AgentActionRefusal(AgentActionCodes.RequestClosed,
                "A person closed this request, so no action runs under it. Nothing was run."), null);

        if (serverAction == ServerActionNames.KillSession && live.TriggerKind == InvestigationTriggerKinds.Alert)
        {
            await using var resolved = Command(connection, transaction, RequestResolvedSql, P(live.RequestId));
            if (await resolved.ExecuteScalarAsync(cancellationToken) is true)
                return (new AgentActionRefusal(AgentActionCodes.AlertResolved,
                    "This request's alert has resolved, so the session it showed is no longer the problem. Nothing was run."), null);
        }

        var routes = await ResolveRequestRoutesAsync(connection, transaction, live, loadRoutes, ServerActionLevel, cancellationToken);
        var level = routes.Level;
        if (level == 2)
            return (new AgentActionRefusal(AgentActionCodes.NotLevel3,
                "Level 2 changes only the monitoring store. A server action needs a Level 3 route. Report what you would change instead."), null);
        if (level < ServerActionLevel)
            return (new AgentActionRefusal(AgentActionCodes.NotLevel3,
                "This request is not Level 3: its routes do not let an agent act on a monitored server. Report what you would change instead."), null);

        if (target.ServerId != live.ServerId)
            return (new AgentActionRefusal(AgentActionCodes.OtherServer,
                $"This action targets {target.ServerName}; the request is for {live.ServerName}. An action runs only on the request's server."), null);

        var off = await ServerActionSwitchRefusalAsync(connection, transaction, live.ServerId, live.ServerName, serverAction, person: false,
            cancellationToken);
        if (off is not null)
            return (off, null);

        if (readCredential is not null
            && await CredentialRefusalAsync(live.ServerId, live.ServerName, readCredential, person: false, cancellationToken) is { } credential)
            return (credential, null);

        return (null, routes.Approval);
    }

    /// <summary>The resolve sweep's mark on a request (#7109): a <c>resolved</c> journal row, as the proposal sweep's kill void reads it.</summary>
    internal const string RequestResolvedSql =
        "SELECT EXISTS (SELECT 1 FROM config.investigation_journal WHERE request_id = $1 AND event = 'resolved')";

    /// <summary>The server action a gated kind runs, null for a kind that only changes the monitoring store. The write's re-check keys on it,
    /// so a call site cannot forget to name its action.</summary>
    internal static string? ServerActionForKind(string kind) => kind switch
    {
        AgentChangeKinds.PlanForce or AgentChangeKinds.PlanUnforce => ServerActionNames.ForcePlan,
        AgentChangeKinds.PlanEvict => ServerActionNames.EvictPlan,
        AgentChangeKinds.SessionKill => ServerActionNames.KillSession,
        AgentChangeKinds.ModuleRecompile => ServerActionNames.RecompileModule,
        AgentChangeKinds.PlanGuideCreate => ServerActionNames.CreatePlanGuide,
        AgentChangeKinds.PlanGuideDrop => ServerActionNames.DropPlanGuide,
        _ => null,
    };

    /// <summary>
    /// The two server-side conditions of the gate, shared by an agent's action and a person's undo (#5632, step 9): 3 this server's switch for this
    /// action (no row means off), 4 the remediation login, a reference only. The secret is classified by its prefix and never resolved here.
    /// <paramref name="person"/> only changes the words: a person is told how to turn the switch on, or to remove the force in SQL Server.
    /// </summary>
    internal async Task<AgentActionRefusal?> SwitchAndCredentialRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        int serverId, string serverName, string serverAction, Func<int, CancellationToken, Task<ServerCredentialState>> readCredential, bool person,
        CancellationToken cancellationToken)
    {
        if (await ServerActionSwitchRefusalAsync(connection, transaction, serverId, serverName, serverAction, person, cancellationToken) is { } off)
            return off;

        return await CredentialRefusalAsync(serverId, serverName, readCredential, person, cancellationToken);
    }

    /// <summary>Condition 3: this server's switch for this action. No row is off. A person is told how to turn it on.</summary>
    private async Task<AgentActionRefusal?> ServerActionSwitchRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        int serverId, string serverName, string serverAction, bool person, CancellationToken cancellationToken)
    {
        bool on;
        await using (var command = Command(connection, transaction, ServerActionSwitchSql, P(serverId), PText(serverAction)))
        {
            on = await command.ExecuteScalarAsync(cancellationToken) is true;
        }

        if (!on)
            return new AgentActionRefusal(AgentActionCodes.SwitchOff, person
                ? $"{serverAction} is turned off for {serverName}, so Darling will not change that server. To allow it, set enabled to true for that server and action in config.config_server_actions. Or remove the plan force in SQL Server yourself."
                : $"{serverAction} is turned off for {serverName}. Every server action starts off until a person turns it on for that server.");

        return null;
    }

    /// <summary>Condition 4: the remediation login, a reference only.</summary>
    private static async Task<AgentActionRefusal?> CredentialRefusalAsync(int serverId, string serverName,
        Func<int, CancellationToken, Task<ServerCredentialState>> readCredential, bool person, CancellationToken cancellationToken)
    {
        return await readCredential(serverId, cancellationToken) switch
        {
            ServerCredentialState.Reference => null,
            ServerCredentialState.StoredPassword => new AgentActionRefusal(AgentActionCodes.CredentialNotReference, person
                ? $"{serverName}'s remediation password is stored, not given as an env: or file: reference. Server actions use references only. Set a reference, or remove the plan force in SQL Server yourself."
                : $"{serverName}'s remediation password is stored, not given as an env: or file: reference. Server actions use references only."),
            _ => new AgentActionRefusal(AgentActionCodes.NoCredential, person
                ? $"{serverName} has no remediation login (a login name and an env: or file: reference). Set one, or remove the plan force in SQL Server yourself."
                : $"{serverName} has no remediation login (a login name and an env: or file: reference). Report what you would change instead."),
        };
    }

    private static NpgsqlParameter<string> PText(string value) =>
        new NpgsqlParameter<string> { TypedValue = value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text };
}
