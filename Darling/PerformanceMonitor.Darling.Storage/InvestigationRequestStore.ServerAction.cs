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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One server action a caller asks the gate about (#5632, step 5): the tool, the action (one of <see cref="ServerActionNames"/>),
/// the validated <c>agent_action</c> and the server it would run on.</summary>
public sealed record ServerActionCall(string Tool, string ServerAction, AgentAction Action, AgentActionTarget Target);

/// <summary>The caller's part of a server-action check (#5632, step 5): the live routes (as <see cref="AgentActionHooks.LoadLevels"/>) and
/// the server's credential classification. The credential read is the caller's because the <c>remediation_encrypted_password</c> column is
/// secret to the <c>viewer</c> and <c>mcp</c> roles: the caller runs it as the store owner, through
/// <see cref="InvestigationRequestStore.ReadServerCredentialStateAsync"/>. It never resolves the secret.</summary>
public sealed class ServerActionHooks
{
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, int?>>> LoadLevels { get; init; }

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
    /// request, tool, code and hour, as the Level 2 gate journals it. <b>Nothing calls this yet:</b> steps 7, 10 and 12 will.
    /// Null means every condition holds; the secret was not read or resolved.
    /// </summary>
    public async Task<AgentActionRefusal?> CheckServerActionAsync(ServerActionCall call, ServerActionHooks hooks,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(call);
        ArgumentNullException.ThrowIfNull(hooks);
        var action = call.Action;
        var actor = "agent:" + action.AgentId;
        var now = Now();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var request = await ReadAgentActionRequestAsync(connection, transaction, action.RequestId, cancellationToken);
        var refusal = await ServerActionChecksAsync(connection, transaction, request, call, hooks, now, cancellationToken);
        if (refusal is not null)
            await JournalRefusedAsync(connection, transaction, now, request is null ? null : action.RequestId,
                request?.ServerId, actor, call.Tool, refusal.Reason, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return refusal;
    }

    private async Task<AgentActionRefusal?> ServerActionChecksAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        AgentActionRequest? request, ServerActionCall call, ServerActionHooks hooks, DateTime now, CancellationToken cancellationToken)
    {
        if (!System.Linq.Enumerable.Contains(ServerActionNames.All, call.ServerAction, StringComparer.Ordinal))
            return new AgentActionRefusal(AgentActionCodes.UnknownServerAction,
                "That is not a server action. The actions are force_plan, evict_plan and kill_session.");

        /* 1: the service-wide switch. A store with no row reads as on, its default, as the Level 2 gate reads it. */
        bool enabled;
        await using (var command = Command(connection, transaction, AgentActionsEnabledSql))
        {
            enabled = await command.ExecuteScalarAsync(cancellationToken) is not bool flag || flag;
        }

        if (!enabled)
            return new AgentActionRefusal(AgentActionCodes.ActionsOff,
                "Agent changes are turned off for this service (config_service.agent_actions_enabled). Report what you would change instead.");

        if (ClaimOrPracticeRefusal(request, call.Action, now, practice: true) is { } claimRefusal)
            return claimRefusal;
        var live = request!;

        /* 2: Level 3. The request's routes are read once, and the lowest level among its metrics wins. */
        var level = await ResolveRequestLevelAsync(connection, transaction, live, hooks.LoadLevels, ServerActionLevel, cancellationToken);
        if (level == 2)
            return new AgentActionRefusal(AgentActionCodes.NotLevel3,
                "Level 2 changes only the monitoring store. A server action needs a Level 3 route. Report what you would change instead.");
        if (level < ServerActionLevel)
            return new AgentActionRefusal(AgentActionCodes.NotLevel3,
                "This request is not Level 3: its routes do not let an agent act on a monitored server. Report what you would change instead.");

        if (call.Target.ServerId != live.ServerId)
            return new AgentActionRefusal(AgentActionCodes.OtherServer,
                $"This action targets {call.Target.ServerName}; the request is for {live.ServerName}. An action runs only on the request's server.");

        /* 3 and 4: this server's switch and its remediation login, the two server-side conditions. */
        return await SwitchAndCredentialRefusalAsync(connection, transaction, live.ServerId, live.ServerName, call.ServerAction, hooks.ReadCredential,
            person: false, cancellationToken);
    }

    /// <summary>
    /// The two server-side conditions of the gate, shared by an agent's action and a person's undo (#5632, step 9): 3 this server's switch for this
    /// action (no row means off), 4 the remediation login, a reference only. The secret is classified by its prefix and never resolved here.
    /// <paramref name="person"/> only changes the words: a person is told how to turn the switch on, or to remove the force in SQL Server.
    /// </summary>
    internal async Task<AgentActionRefusal?> SwitchAndCredentialRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        int serverId, string serverName, string serverAction, Func<int, CancellationToken, Task<ServerCredentialState>> readCredential, bool person,
        CancellationToken cancellationToken)
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
